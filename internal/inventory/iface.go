package inventory

import (
	"context"
	"time"
)

// Inventory is what the API needs from a stock backend. Store implements it
// in memory (through AsInventory); internal/pgstore implements it on PostgreSQL.
// Both must behave the same: errors are the package's Err* values, and a
// reservation either holds every line of a cart or none.
type Inventory interface {
	// Seed makes sure the given SKUs exist with at least this quantity on a
	// first run. It must not add stock again when called on an already
	// populated backend, so restarts do not inflate inventory.
	Seed(ctx context.Context, stock map[string]int) error
	Stock(ctx context.Context) (map[string]int, error)
	Reserve(ctx context.Context, items []Item) (Reservation, error)
	Get(ctx context.Context, id string) (Reservation, error)
	Confirm(ctx context.Context, id string) (Reservation, error)
	Release(ctx context.Context, id string) (Reservation, error)
	// RunJanitor expires overdue reservations every interval until ctx ends.
	RunJanitor(ctx context.Context, interval time.Duration)
}

// NewID returns a random reservation ID shared by every backend.
func NewID() (string, error) { return newID() }

// AsInventory adapts the in-memory Store to the Inventory interface. The
// context is ignored: in-memory calls cannot block on I/O.
func AsInventory(s *Store) Inventory { return memory{s} }

type memory struct{ s *Store }

func (m memory) Seed(_ context.Context, stock map[string]int) error {
	for sku, qty := range stock {
		if err := m.s.AddStock(sku, qty); err != nil {
			return err
		}
	}
	return nil
}

func (m memory) Stock(context.Context) (map[string]int, error) { return m.s.Stock(), nil }

func (m memory) Reserve(_ context.Context, items []Item) (Reservation, error) {
	return m.s.Reserve(items)
}

func (m memory) Get(_ context.Context, id string) (Reservation, error)     { return m.s.Get(id) }
func (m memory) Confirm(_ context.Context, id string) (Reservation, error) { return m.s.Confirm(id) }
func (m memory) Release(_ context.Context, id string) (Reservation, error) { return m.s.Release(id) }

func (m memory) RunJanitor(ctx context.Context, interval time.Duration) {
	m.s.RunJanitor(ctx, interval)
}
