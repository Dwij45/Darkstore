package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"darkstore/internal/fulfilment"
	"darkstore/internal/inventory"
)

type fakeDispatcher struct {
	mu  sync.Mutex
	ids []string
	err error
}

func (f *fakeDispatcher) Submit(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.ids = append(f.ids, id)
	return nil
}

func newTestServer(t *testing.T, ttl time.Duration, stock map[string]int) (*Server, *inventory.Store, *fakeDispatcher) {
	t.Helper()
	inv := inventory.NewStore(ttl)
	for sku, n := range stock {
		if err := inv.AddStock(sku, n); err != nil {
			t.Fatal(err)
		}
	}
	d := &fakeDispatcher{}
	return New(inv, d, fulfilment.NewTracker()), inv, d
}

func do(s *Server, method, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func decodeOrder(t *testing.T, rec *httptest.ResponseRecorder) orderResponse {
	t.Helper()
	var o orderResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &o); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return o
}

const milkx2 = `{"user_id":"u1","items":[{"sku":"milk","qty":2}]}`

func TestCheckoutRequiresIdempotencyKey(t *testing.T) {
	s, _, _ := newTestServer(t, time.Minute, map[string]int{"milk": 10})
	if rec := do(s, "POST", "/checkout", "", milkx2); rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestCheckoutValidatesBody(t *testing.T) {
	s, _, _ := newTestServer(t, time.Minute, map[string]int{"milk": 10})
	cases := []struct {
		name, body string
		want       int
	}{
		{"not json", `{`, http.StatusBadRequest},
		{"unknown field", `{"user_id":"u1","items":[],"coupon":"x"}`, http.StatusBadRequest},
		{"missing user", `{"items":[{"sku":"milk","qty":1}]}`, http.StatusBadRequest},
		{"unknown sku", `{"user_id":"u1","items":[{"sku":"caviar","qty":1}]}`, http.StatusUnprocessableEntity},
		{"zero qty", `{"user_id":"u1","items":[{"sku":"milk","qty":0}]}`, http.StatusUnprocessableEntity},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := do(s, "POST", "/checkout", fmt.Sprintf("k%d", i), c.body)
			if rec.Code != c.want {
				t.Errorf("status = %d, want %d (body %s)", rec.Code, c.want, rec.Body)
			}
		})
	}
}

func TestRetriedCheckoutCreatesOneOrder(t *testing.T) {
	s, inv, _ := newTestServer(t, time.Minute, map[string]int{"milk": 10})

	first := do(s, "POST", "/checkout", "retry-1", milkx2)
	// Same request with different whitespace and field order.
	second := do(s, "POST", "/checkout", "retry-1", `{ "items":[{"qty":2,"sku":"milk"}], "user_id":"u1" }`)

	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Fatalf("status = %d, %d; want 201, 201", first.Code, second.Code)
	}
	if a, b := decodeOrder(t, first).OrderID, decodeOrder(t, second).OrderID; a != b {
		t.Errorf("order ids differ: %s vs %s", a, b)
	}
	if second.Header().Get("Idempotent-Replayed") != "true" {
		t.Error("second response missing Idempotent-Replayed header")
	}
	if n, _ := inv.Available("milk"); n != 8 {
		t.Errorf("available = %d, want 8 (stock taken once)", n)
	}
}

// 50 copies of the same request arrive at once (a client retrying aggressively).
func TestConcurrentDuplicateCheckoutsCreateOneOrder(t *testing.T) {
	s, inv, _ := newTestServer(t, time.Minute, map[string]int{"milk": 10})

	const n = 50
	ids := make([]string, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := do(s, "POST", "/checkout", "dup", milkx2)
			if rec.Code != http.StatusCreated {
				t.Errorf("status = %d, want 201", rec.Code)
				return
			}
			ids[i] = decodeOrder(t, rec).OrderID
		}()
	}
	close(start)
	wg.Wait()

	for _, id := range ids[1:] {
		if id != ids[0] {
			t.Fatalf("got different order ids %s and %s", ids[0], id)
		}
	}
	if avail, _ := inv.Available("milk"); avail != 8 {
		t.Errorf("available = %d, want 8", avail)
	}
}

func TestKeyReusedWithDifferentBodyIsRejected(t *testing.T) {
	s, _, _ := newTestServer(t, time.Minute, map[string]int{"milk": 10})
	do(s, "POST", "/checkout", "k", milkx2)
	rec := do(s, "POST", "/checkout", "k", `{"user_id":"u1","items":[{"sku":"milk","qty":5}]}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", rec.Code)
	}
}

func TestSameKeyFromDifferentUsersIsIndependent(t *testing.T) {
	s, inv, _ := newTestServer(t, time.Minute, map[string]int{"milk": 10})
	a := do(s, "POST", "/checkout", "k", `{"user_id":"alice","items":[{"sku":"milk","qty":1}]}`)
	b := do(s, "POST", "/checkout", "k", `{"user_id":"bob","items":[{"sku":"milk","qty":1}]}`)
	if a.Code != http.StatusCreated || b.Code != http.StatusCreated {
		t.Fatalf("status = %d, %d; want 201, 201", a.Code, b.Code)
	}
	if n, _ := inv.Available("milk"); n != 8 {
		t.Errorf("available = %d, want 8", n)
	}
}

// The no-oversell guarantee holds end to end through HTTP.
func TestConcurrentCheckoutsOverHTTPNeverOversell(t *testing.T) {
	const stock, buyers = 50, 1000
	s, inv, _ := newTestServer(t, time.Minute, map[string]int{"milk": stock})

	var created, conflict atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range buyers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			body := fmt.Sprintf(`{"user_id":"u%d","items":[{"sku":"milk","qty":1}]}`, i)
			switch rec := do(s, "POST", "/checkout", fmt.Sprintf("key-%d", i), body); rec.Code {
			case http.StatusCreated:
				created.Add(1)
			case http.StatusConflict:
				conflict.Add(1)
			default:
				t.Errorf("status = %d, body %s", rec.Code, rec.Body)
			}
		}()
	}
	close(start)
	wg.Wait()

	if created.Load() != stock || conflict.Load() != buyers-stock {
		t.Errorf("201s = %d, 409s = %d; want %d, %d", created.Load(), conflict.Load(), stock, buyers-stock)
	}
	if n, _ := inv.Available("milk"); n != 0 {
		t.Errorf("available = %d, want 0", n)
	}
}

func TestConfirmDispatchesOrder(t *testing.T) {
	s, _, d := newTestServer(t, time.Minute, map[string]int{"milk": 10})
	id := decodeOrder(t, do(s, "POST", "/checkout", "k", milkx2)).OrderID

	rec := do(s, "POST", "/orders/"+id+"/confirm", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	o := decodeOrder(t, rec)
	if o.Status != inventory.StatusConfirmed {
		t.Errorf("status = %s, want confirmed", o.Status)
	}
	if o.Fulfilment == nil || o.Fulfilment.Stage != fulfilment.StageQueued {
		t.Errorf("fulfilment = %+v, want stage queued", o.Fulfilment)
	}
	if len(d.ids) != 1 || d.ids[0] != id {
		t.Errorf("dispatched = %v, want [%s]", d.ids, id)
	}
	if rec := do(s, "POST", "/orders/"+id+"/confirm", "", ""); rec.Code != http.StatusConflict {
		t.Errorf("second confirm status = %d, want 409", rec.Code)
	}
}

func TestConfirmReportsUnavailableQueue(t *testing.T) {
	s, _, d := newTestServer(t, time.Minute, map[string]int{"milk": 10})
	d.err = context.DeadlineExceeded
	id := decodeOrder(t, do(s, "POST", "/checkout", "k", milkx2)).OrderID
	if rec := do(s, "POST", "/orders/"+id+"/confirm", "", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if o := decodeOrder(t, do(s, "GET", "/orders/"+id, "", "")); o.Fulfilment != nil {
		t.Errorf("fulfilment = %+v, want none (order was never queued)", o.Fulfilment)
	}
}

func TestConfirmAfterExpiryIsGone(t *testing.T) {
	s, inv, _ := newTestServer(t, 10*time.Millisecond, map[string]int{"milk": 10})
	id := decodeOrder(t, do(s, "POST", "/checkout", "k", milkx2)).OrderID
	time.Sleep(20 * time.Millisecond)

	if rec := do(s, "POST", "/orders/"+id+"/confirm", "", ""); rec.Code != http.StatusGone {
		t.Errorf("status = %d, want 410", rec.Code)
	}
	if n, _ := inv.Available("milk"); n != 10 {
		t.Errorf("available = %d, want 10", n)
	}
}

func TestCancelReturnsStock(t *testing.T) {
	s, inv, _ := newTestServer(t, time.Minute, map[string]int{"milk": 10})
	id := decodeOrder(t, do(s, "POST", "/checkout", "k", milkx2)).OrderID

	rec := do(s, "POST", "/orders/"+id+"/cancel", "", "")
	if rec.Code != http.StatusOK || decodeOrder(t, rec).Status != inventory.StatusReleased {
		t.Fatalf("status = %d body %s, want 200 released", rec.Code, rec.Body)
	}
	if n, _ := inv.Available("milk"); n != 10 {
		t.Errorf("available = %d, want 10", n)
	}
}

func TestGetUnknownOrderIsNotFound(t *testing.T) {
	s, _, _ := newTestServer(t, time.Minute, nil)
	if rec := do(s, "GET", "/orders/nope", "", ""); rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}
