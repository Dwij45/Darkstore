// Package pgstore keeps stock and reservations in PostgreSQL.
//
// Overselling is prevented by row locks: Reserve takes SELECT ... FOR UPDATE on
// every SKU in the cart, in sorted SKU order, so concurrent checkouts for the
// same SKU queue behind each other and carts that overlap in opposite order
// cannot deadlock. A CHECK (available >= 0) constraint backs this up.
package pgstore

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"darkstore/internal/inventory"
)

//go:embed schema.sql
var schema string

// Migrate creates the tables if they do not exist.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

type Store struct {
	pool *pgxpool.Pool
	ttl  time.Duration
	now  func() time.Time

	// OnError, if set, receives errors from the background janitor.
	OnError func(error)
}

var _ inventory.Inventory = (*Store)(nil)

func New(pool *pgxpool.Pool, ttl time.Duration) *Store {
	return &Store{pool: pool, ttl: ttl, now: time.Now}
}

// querier is the read side shared by *pgxpool.Pool and pgx.Tx.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op once committed
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Seed adds a SKU only if it does not exist yet, so restarting the service
// against a populated database never inflates stock.
func (s *Store) Seed(ctx context.Context, stock map[string]int) error {
	for sku, qty := range stock {
		if qty <= 0 {
			return inventory.ErrInvalidQty
		}
		if _, err := s.pool.Exec(ctx,
			`INSERT INTO stock (sku, available) VALUES ($1, $2) ON CONFLICT (sku) DO NOTHING`,
			sku, qty); err != nil {
			return err
		}
	}
	return nil
}

// AddStock increases a SKU's quantity, creating it if needed (restocking).
func (s *Store) AddStock(ctx context.Context, sku string, qty int) error {
	if qty <= 0 {
		return inventory.ErrInvalidQty
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO stock (sku, available) VALUES ($1, $2)
		 ON CONFLICT (sku) DO UPDATE SET available = stock.available + EXCLUDED.available`,
		sku, qty)
	return err
}

func (s *Store) Stock(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT sku, available FROM stock`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int)
	for rows.Next() {
		var sku string
		var n int
		if err := rows.Scan(&sku, &n); err != nil {
			return nil, err
		}
		out[sku] = n
	}
	return out, rows.Err()
}

// Reserve holds stock for every item or for none of them. Duplicate lines for
// one SKU are merged, and the returned items are sorted by SKU.
func (s *Store) Reserve(ctx context.Context, items []inventory.Item) (inventory.Reservation, error) {
	if len(items) == 0 {
		return inventory.Reservation{}, inventory.ErrEmptyOrder
	}
	want := make(map[string]int, len(items))
	for _, it := range items {
		if it.Qty <= 0 {
			return inventory.Reservation{}, inventory.ErrInvalidQty
		}
		want[it.SKU] += it.Qty
	}
	skus := make([]string, 0, len(want))
	for sku := range want {
		skus = append(skus, sku)
	}
	sort.Strings(skus)

	id, err := inventory.NewID()
	if err != nil {
		return inventory.Reservation{}, err
	}
	now := s.now()
	res := inventory.Reservation{
		ID:        id,
		Status:    inventory.StatusPending,
		CreatedAt: now,
		ExpiresAt: now.Add(s.ttl),
	}
	for _, sku := range skus {
		res.Items = append(res.Items, inventory.Item{SKU: sku, Qty: want[sku]})
	}

	err = s.inTx(ctx, func(tx pgx.Tx) error {
		// Lock in sorted order. Two carts that share SKUs always lock them in
		// the same order, so they wait for each other instead of deadlocking.
		rows, err := tx.Query(ctx,
			`SELECT sku, available FROM stock WHERE sku = ANY($1) ORDER BY sku FOR UPDATE`, skus)
		if err != nil {
			return err
		}
		have := make(map[string]int, len(skus))
		for rows.Next() {
			var sku string
			var n int
			if err := rows.Scan(&sku, &n); err != nil {
				rows.Close()
				return err
			}
			have[sku] = n
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		// Check every line before changing anything.
		for _, it := range items {
			n, ok := have[it.SKU]
			if !ok {
				return fmt.Errorf("%w: %s", inventory.ErrUnknownSKU, it.SKU)
			}
			if n < want[it.SKU] {
				return &inventory.OutOfStockError{SKU: it.SKU, Requested: want[it.SKU], Available: n}
			}
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO reservations (id, status, created_at, expires_at) VALUES ($1, 'pending', $2, $3)`,
			id, res.CreatedAt, res.ExpiresAt); err != nil {
			return err
		}
		for _, sku := range skus {
			if _, err := tx.Exec(ctx,
				`UPDATE stock SET available = available - $2 WHERE sku = $1`, sku, want[sku]); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO reservation_items (reservation_id, sku, qty) VALUES ($1, $2, $3)`,
				id, sku, want[sku]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return inventory.Reservation{}, err
	}
	return res, nil
}

func (s *Store) Get(ctx context.Context, id string) (inventory.Reservation, error) {
	return load(ctx, s.pool, id, "")
}

// load reads a reservation and its items. lock may be "FOR UPDATE" inside a transaction.
func load(ctx context.Context, q querier, id, lock string) (inventory.Reservation, error) {
	r := inventory.Reservation{ID: id}
	err := q.QueryRow(ctx,
		`SELECT status, created_at, expires_at FROM reservations WHERE id = $1 `+lock, id).
		Scan(&r.Status, &r.CreatedAt, &r.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return inventory.Reservation{}, inventory.ErrNotFound
	}
	if err != nil {
		return inventory.Reservation{}, err
	}
	rows, err := q.Query(ctx,
		`SELECT sku, qty FROM reservation_items WHERE reservation_id = $1 ORDER BY sku`, id)
	if err != nil {
		return inventory.Reservation{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var it inventory.Item
		if err := rows.Scan(&it.SKU, &it.Qty); err != nil {
			return inventory.Reservation{}, err
		}
		r.Items = append(r.Items, it)
	}
	return r, rows.Err()
}

// Confirm turns a pending reservation into a sale. A reservation past its
// deadline is expired here even if the janitor has not reached it yet.
func (s *Store) Confirm(ctx context.Context, id string) (inventory.Reservation, error) {
	return s.settle(ctx, id, inventory.StatusConfirmed)
}

// Release cancels a pending reservation and returns its stock.
func (s *Store) Release(ctx context.Context, id string) (inventory.Reservation, error) {
	return s.settle(ctx, id, inventory.StatusReleased)
}

// settle moves a pending reservation to `to` (confirmed or released) under a row lock.
func (s *Store) settle(ctx context.Context, id string, to inventory.Status) (inventory.Reservation, error) {
	var out inventory.Reservation
	var late bool
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		r, err := load(ctx, tx, id, "FOR UPDATE")
		if err != nil {
			return err
		}
		switch r.Status {
		case inventory.StatusPending:
		case inventory.StatusExpired:
			return inventory.ErrExpired
		default:
			return inventory.ErrNotPending
		}

		if !s.now().Before(r.ExpiresAt) {
			// Too late. Return the stock and commit that, then report ErrExpired.
			if err := releaseStock(ctx, tx, id, inventory.StatusExpired); err != nil {
				return err
			}
			late = true
			return nil
		}
		if to == inventory.StatusReleased {
			if err := releaseStock(ctx, tx, id, inventory.StatusReleased); err != nil {
				return err
			}
		} else if _, err := tx.Exec(ctx, `UPDATE reservations SET status = $2 WHERE id = $1`, id, to); err != nil {
			return err
		}
		r.Status = to
		out = r
		return nil
	})
	if err != nil {
		return inventory.Reservation{}, err
	}
	if late {
		return inventory.Reservation{}, inventory.ErrExpired
	}
	return out, nil
}

// releaseStock gives a reservation's stock back and marks it `to`. The
// reservation row must already be locked by the caller.
func releaseStock(ctx context.Context, tx pgx.Tx, id string, to inventory.Status) error {
	// Lock the affected stock rows in SKU order, for the same reason Reserve does.
	if _, err := tx.Exec(ctx,
		`SELECT 1 FROM stock
		 WHERE sku IN (SELECT sku FROM reservation_items WHERE reservation_id = $1)
		 ORDER BY sku FOR UPDATE`, id); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE stock s SET available = s.available + ri.qty
		 FROM reservation_items ri
		 WHERE ri.reservation_id = $1 AND ri.sku = s.sku`, id); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE reservations SET status = $2 WHERE id = $1`, id, to)
	return err
}

// ExpireDue releases overdue pending reservations (up to a batch) and reports
// how many it released. Safe to run from several instances at once: each
// reservation is re-checked under its row lock.
func (s *Store) ExpireDue(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM reservations WHERE status = 'pending' AND expires_at <= $1
		 ORDER BY expires_at LIMIT 200`, s.now())
	if err != nil {
		return 0, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	n := 0
	for _, id := range slices.Clone(ids) {
		expired, err := s.expireOne(ctx, id)
		if err != nil {
			return n, err
		}
		if expired {
			n++
		}
	}
	return n, nil
}

func (s *Store) expireOne(ctx context.Context, id string) (bool, error) {
	expired := false
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		r, err := load(ctx, tx, id, "FOR UPDATE")
		if err != nil {
			return err
		}
		if r.Status != inventory.StatusPending || s.now().Before(r.ExpiresAt) {
			return nil // someone settled or already expired it
		}
		if err := releaseStock(ctx, tx, id, inventory.StatusExpired); err != nil {
			return err
		}
		expired = true
		return nil
	})
	return expired, err
}

// RunJanitor calls ExpireDue every interval until ctx is cancelled.
func (s *Store) RunJanitor(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := s.ExpireDue(ctx); err != nil && ctx.Err() == nil && s.OnError != nil {
				s.OnError(err)
			}
		}
	}
}
