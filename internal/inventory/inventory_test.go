package inventory

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 1,000 buyers race for 50 units: exactly 50 must win and stock must end at 0.
func TestReserveNeverOversells(t *testing.T) {
	const stock, buyers = 50, 1000
	s := NewStore(time.Minute)
	if err := s.AddStock("milk", stock); err != nil {
		t.Fatal(err)
	}

	var won, lost atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range buyers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.Reserve([]Item{{SKU: "milk", Qty: 1}})
			switch {
			case err == nil:
				won.Add(1)
			case errors.Is(err, ErrOutOfStock):
				lost.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if won.Load() != stock {
		t.Errorf("successful reservations = %d, want %d", won.Load(), stock)
	}
	if lost.Load() != buyers-stock {
		t.Errorf("rejected reservations = %d, want %d", lost.Load(), buyers-stock)
	}
	if n, _ := s.Available("milk"); n != 0 {
		t.Errorf("available = %d, want 0", n)
	}
}

func TestReserveIsAllOrNothing(t *testing.T) {
	s := NewStore(time.Minute)
	s.AddStock("milk", 5)
	s.AddStock("bread", 1)

	_, err := s.Reserve([]Item{{SKU: "milk", Qty: 2}, {SKU: "bread", Qty: 2}})
	var oos *OutOfStockError
	if !errors.As(err, &oos) || oos.SKU != "bread" {
		t.Fatalf("err = %v, want out of stock for bread", err)
	}
	if n, _ := s.Available("milk"); n != 5 {
		t.Errorf("milk available = %d, want 5 (nothing should be held)", n)
	}
}

func TestReserveMergesDuplicateLines(t *testing.T) {
	s := NewStore(time.Minute)
	s.AddStock("milk", 3)
	_, err := s.Reserve([]Item{{SKU: "milk", Qty: 2}, {SKU: "milk", Qty: 2}})
	if !errors.Is(err, ErrOutOfStock) {
		t.Fatalf("err = %v, want ErrOutOfStock for 4 > 3", err)
	}
}

func TestReserveRejectsBadInput(t *testing.T) {
	s := NewStore(time.Minute)
	s.AddStock("milk", 3)
	cases := []struct {
		name  string
		items []Item
		want  error
	}{
		{"empty", nil, ErrEmptyOrder},
		{"zero qty", []Item{{SKU: "milk", Qty: 0}}, ErrInvalidQty},
		{"unknown sku", []Item{{SKU: "caviar", Qty: 1}}, ErrUnknownSKU},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := s.Reserve(c.items); !errors.Is(err, c.want) {
				t.Errorf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestExpiredReservationReturnsStock(t *testing.T) {
	s := NewStore(time.Minute)
	now := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return now }
	s.AddStock("milk", 10)

	r, err := s.Reserve([]Item{{SKU: "milk", Qty: 3}})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)

	if n := s.ExpireDue(); n != 1 {
		t.Errorf("expired = %d, want 1", n)
	}
	if n, _ := s.Available("milk"); n != 10 {
		t.Errorf("available = %d, want 10", n)
	}
	if _, err := s.Confirm(r.ID); !errors.Is(err, ErrExpired) {
		t.Errorf("confirm err = %v, want ErrExpired", err)
	}
}

// Confirm must not accept a late payment just because the janitor has not run yet.
func TestConfirmAfterDeadlineExpiresImmediately(t *testing.T) {
	s := NewStore(time.Minute)
	now := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return now }
	s.AddStock("milk", 10)

	r, _ := s.Reserve([]Item{{SKU: "milk", Qty: 4}})
	now = now.Add(2 * time.Minute)

	if _, err := s.Confirm(r.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("err = %v, want ErrExpired", err)
	}
	if n, _ := s.Available("milk"); n != 10 {
		t.Errorf("available = %d, want 10", n)
	}
}

func TestReleaseReturnsStockOnce(t *testing.T) {
	s := NewStore(time.Minute)
	s.AddStock("milk", 10)
	r, _ := s.Reserve([]Item{{SKU: "milk", Qty: 4}})

	if _, err := s.Release(r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Release(r.ID); !errors.Is(err, ErrNotPending) {
		t.Errorf("second release err = %v, want ErrNotPending", err)
	}
	if n, _ := s.Available("milk"); n != 10 {
		t.Errorf("available = %d, want 10", n)
	}
}

func TestConfirmedStockStaysSold(t *testing.T) {
	s := NewStore(time.Minute)
	s.AddStock("milk", 10)
	r, _ := s.Reserve([]Item{{SKU: "milk", Qty: 4}})
	if _, err := s.Confirm(r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Release(r.ID); !errors.Is(err, ErrNotPending) {
		t.Errorf("release after confirm err = %v, want ErrNotPending", err)
	}
	if n, _ := s.Available("milk"); n != 6 {
		t.Errorf("available = %d, want 6", n)
	}
}
