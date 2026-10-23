package pgstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"darkstore/internal/inventory"
)

// These tests need a real PostgreSQL:
//
//	docker compose up -d
//	TEST_DATABASE_URL=postgres://darkstore:darkstore@localhost:5433/darkstore go test ./internal/pgstore
//
// Each test gets its own schema, so tests are isolated and can run in parallel.
// Without TEST_DATABASE_URL they are skipped.

var schemaSeq atomic.Int64

func newStore(t *testing.T, ttl time.Duration, stock map[string]int) (*Store, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL tests")
	}
	ctx := context.Background()
	name := fmt.Sprintf("t_%d_%d", time.Now().UnixNano(), schemaSeq.Add(1))

	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		t.Fatal(err)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		admin.Exec(ctx, "DROP SCHEMA "+name+" CASCADE")
		admin.Close()
	})

	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	s := New(pool, ttl)
	if err := s.Seed(ctx, stock); err != nil {
		t.Fatal(err)
	}
	return s, pool
}

func available(t *testing.T, s *Store, sku string) int {
	t.Helper()
	stock, err := s.Stock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return stock[sku]
}

// 1,000 buyers race for 50 units: exactly 50 must win and stock must end at 0.
func TestReserveNeverOversells(t *testing.T) {
	const stock, buyers = 50, 1000
	s, _ := newStore(t, time.Minute, map[string]int{"milk": stock})

	var won, lost atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range buyers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.Reserve(context.Background(), []inventory.Item{{SKU: "milk", Qty: 1}})
			switch {
			case err == nil:
				won.Add(1)
			case errors.Is(err, inventory.ErrOutOfStock):
				lost.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if won.Load() != stock || lost.Load() != buyers-stock {
		t.Errorf("won %d, lost %d; want %d, %d", won.Load(), lost.Load(), stock, buyers-stock)
	}
	if n := available(t, s, "milk"); n != 0 {
		t.Errorf("available = %d, want 0", n)
	}
}

// Carts that overlap in opposite SKU order are the classic deadlock. Sorted
// lock order means they only wait for each other.
func TestOpposingCartsDoNotDeadlock(t *testing.T) {
	s, _ := newStore(t, time.Minute, map[string]int{"a": 1_000_000, "b": 1_000_000})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, items := range [][]inventory.Item{
		{{SKU: "a", Qty: 1}, {SKU: "b", Qty: 1}},
		{{SKU: "b", Qty: 1}, {SKU: "a", Qty: 1}},
	} {
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 50 {
					if _, err := s.Reserve(ctx, items); err != nil {
						t.Errorf("reserve: %v", err)
						return
					}
				}
			}()
		}
	}
	wg.Wait()

	// 2 cart shapes x 8 goroutines x 50 = 800 carts, each taking one of a and one of b.
	if a, b := available(t, s, "a"), available(t, s, "b"); a != 1_000_000-800 || b != 1_000_000-800 {
		t.Errorf("a=%d b=%d, want both %d", a, b, 1_000_000-800)
	}
}

func TestReserveIsAllOrNothing(t *testing.T) {
	s, _ := newStore(t, time.Minute, map[string]int{"milk": 5, "bread": 1})

	_, err := s.Reserve(context.Background(), []inventory.Item{{SKU: "milk", Qty: 2}, {SKU: "bread", Qty: 2}})
	var oos *inventory.OutOfStockError
	if !errors.As(err, &oos) || oos.SKU != "bread" {
		t.Fatalf("err = %v, want out of stock for bread", err)
	}
	if n := available(t, s, "milk"); n != 5 {
		t.Errorf("milk available = %d, want 5 (nothing should be held)", n)
	}
}

func TestReserveMergesDuplicateLines(t *testing.T) {
	s, _ := newStore(t, time.Minute, map[string]int{"milk": 3})
	_, err := s.Reserve(context.Background(), []inventory.Item{{SKU: "milk", Qty: 2}, {SKU: "milk", Qty: 2}})
	if !errors.Is(err, inventory.ErrOutOfStock) {
		t.Fatalf("err = %v, want ErrOutOfStock for 4 > 3", err)
	}
}

func TestReserveRejectsBadInput(t *testing.T) {
	s, _ := newStore(t, time.Minute, map[string]int{"milk": 3})
	cases := []struct {
		name  string
		items []inventory.Item
		want  error
	}{
		{"empty", nil, inventory.ErrEmptyOrder},
		{"zero qty", []inventory.Item{{SKU: "milk", Qty: 0}}, inventory.ErrInvalidQty},
		{"unknown sku", []inventory.Item{{SKU: "caviar", Qty: 1}}, inventory.ErrUnknownSKU},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := s.Reserve(context.Background(), c.items); !errors.Is(err, c.want) {
				t.Errorf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestExpiredReservationReturnsStock(t *testing.T) {
	s, _ := newStore(t, time.Minute, map[string]int{"milk": 10})
	now := time.Now()
	s.now = func() time.Time { return now }

	r, err := s.Reserve(context.Background(), []inventory.Item{{SKU: "milk", Qty: 3}})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)

	if n, err := s.ExpireDue(context.Background()); err != nil || n != 1 {
		t.Errorf("ExpireDue = %d, %v; want 1, nil", n, err)
	}
	if n := available(t, s, "milk"); n != 10 {
		t.Errorf("available = %d, want 10", n)
	}
	if _, err := s.Confirm(context.Background(), r.ID); !errors.Is(err, inventory.ErrExpired) {
		t.Errorf("confirm err = %v, want ErrExpired", err)
	}
}

// Confirm must not accept a late payment just because the janitor has not run yet.
func TestConfirmAfterDeadlineExpiresImmediately(t *testing.T) {
	s, _ := newStore(t, time.Minute, map[string]int{"milk": 10})
	now := time.Now()
	s.now = func() time.Time { return now }

	r, _ := s.Reserve(context.Background(), []inventory.Item{{SKU: "milk", Qty: 4}})
	now = now.Add(2 * time.Minute)

	if _, err := s.Confirm(context.Background(), r.ID); !errors.Is(err, inventory.ErrExpired) {
		t.Fatalf("err = %v, want ErrExpired", err)
	}
	if n := available(t, s, "milk"); n != 10 {
		t.Errorf("available = %d, want 10 (the expiry must be committed)", n)
	}
	got, _ := s.Get(context.Background(), r.ID)
	if got.Status != inventory.StatusExpired {
		t.Errorf("status = %s, want expired", got.Status)
	}
}

func TestReleaseReturnsStockOnce(t *testing.T) {
	s, _ := newStore(t, time.Minute, map[string]int{"milk": 10})
	r, _ := s.Reserve(context.Background(), []inventory.Item{{SKU: "milk", Qty: 4}})

	if _, err := s.Release(context.Background(), r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Release(context.Background(), r.ID); !errors.Is(err, inventory.ErrNotPending) {
		t.Errorf("second release err = %v, want ErrNotPending", err)
	}
	if n := available(t, s, "milk"); n != 10 {
		t.Errorf("available = %d, want 10", n)
	}
}

func TestConfirmedStockStaysSold(t *testing.T) {
	s, _ := newStore(t, time.Minute, map[string]int{"milk": 10})
	r, _ := s.Reserve(context.Background(), []inventory.Item{{SKU: "milk", Qty: 4}})
	if _, err := s.Confirm(context.Background(), r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Release(context.Background(), r.ID); !errors.Is(err, inventory.ErrNotPending) {
		t.Errorf("release after confirm err = %v, want ErrNotPending", err)
	}
	if n := available(t, s, "milk"); n != 6 {
		t.Errorf("available = %d, want 6", n)
	}
}

// A confirm racing a cancel must settle the reservation exactly once.
func TestConfirmAndReleaseRaceSettleOnce(t *testing.T) {
	for range 20 {
		s, _ := newStore(t, time.Minute, map[string]int{"milk": 10})
		r, _ := s.Reserve(context.Background(), []inventory.Item{{SKU: "milk", Qty: 4}})

		var ok atomic.Int64
		var wg sync.WaitGroup
		for _, fn := range []func(context.Context, string) (inventory.Reservation, error){s.Confirm, s.Release} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := fn(context.Background(), r.ID); err == nil {
					ok.Add(1)
				} else if !errors.Is(err, inventory.ErrNotPending) {
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
		wg.Wait()

		if ok.Load() != 1 {
			t.Fatalf("%d calls succeeded, want exactly 1", ok.Load())
		}
		got, _ := s.Get(context.Background(), r.ID)
		want := 10 // released
		if got.Status == inventory.StatusConfirmed {
			want = 6
		}
		if n := available(t, s, "milk"); n != want {
			t.Fatalf("status %s: available = %d, want %d", got.Status, n, want)
		}
	}
}

// Seeding on every start must not add stock again.
func TestSeedDoesNotInflateExistingStock(t *testing.T) {
	s, _ := newStore(t, time.Minute, map[string]int{"milk": 10})
	ctx := context.Background()
	if _, err := s.Reserve(ctx, []inventory.Item{{SKU: "milk", Qty: 4}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Seed(ctx, map[string]int{"milk": 10, "tea": 5}); err != nil {
		t.Fatal(err)
	}
	if n := available(t, s, "milk"); n != 6 {
		t.Errorf("milk = %d, want 6 (re-seeding must not reset it)", n)
	}
	if n := available(t, s, "tea"); n != 5 {
		t.Errorf("tea = %d, want 5 (new SKUs are added)", n)
	}
}

// The point of a database: a new Store over the same data (a restart) still
// knows the reservation and can settle it.
func TestReservationSurvivesRestart(t *testing.T) {
	s, pool := newStore(t, time.Minute, map[string]int{"milk": 10})
	r, err := s.Reserve(context.Background(), []inventory.Item{{SKU: "milk", Qty: 3}})
	if err != nil {
		t.Fatal(err)
	}

	restarted := New(pool, time.Minute)
	got, err := restarted.Get(context.Background(), r.ID)
	if err != nil || got.Status != inventory.StatusPending || len(got.Items) != 1 || got.Items[0].Qty != 3 {
		t.Fatalf("Get after restart = %+v, %v", got, err)
	}
	if _, err := restarted.Confirm(context.Background(), r.ID); err != nil {
		t.Errorf("confirm after restart: %v", err)
	}
}

func TestDatabaseRefusesNegativeStock(t *testing.T) {
	s, pool := newStore(t, time.Minute, map[string]int{"milk": 1})
	_, err := pool.Exec(context.Background(), `UPDATE stock SET available = available - 2 WHERE sku = 'milk'`)
	if err == nil {
		t.Fatal("expected the CHECK constraint to reject negative stock")
	}
	if n := available(t, s, "milk"); n != 1 {
		t.Errorf("available = %d, want 1", n)
	}
}

func TestGetUnknownReservation(t *testing.T) {
	s, _ := newStore(t, time.Minute, nil)
	if _, err := s.Get(context.Background(), "nope"); !errors.Is(err, inventory.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}
