// Package inventory holds per-SKU stock for a single dark store and hands out
// time-limited reservations, so concurrent checkouts can never oversell.
//
// Stock moves through three states: available -> reserved (on checkout) ->
// confirmed (on payment). A reservation that is not confirmed before its TTL
// expires is released back to available stock.
package inventory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

var (
	ErrEmptyOrder = errors.New("order has no items")
	ErrInvalidQty = errors.New("quantity must be positive")
	ErrUnknownSKU = errors.New("unknown sku")
	ErrOutOfStock = errors.New("insufficient stock")
	ErrNotFound   = errors.New("reservation not found")
	ErrExpired    = errors.New("reservation expired")
	ErrNotPending = errors.New("reservation is not pending")
)

// OutOfStockError reports which SKU could not be satisfied. It matches
// ErrOutOfStock with errors.Is.
type OutOfStockError struct {
	SKU       string
	Requested int
	Available int
}

func (e *OutOfStockError) Error() string {
	return fmt.Sprintf("insufficient stock for %s: requested %d, available %d", e.SKU, e.Requested, e.Available)
}

func (e *OutOfStockError) Unwrap() error { return ErrOutOfStock }

type Status string

const (
	StatusPending   Status = "pending"
	StatusConfirmed Status = "confirmed"
	StatusReleased  Status = "released"
	StatusExpired   Status = "expired"
)

type Item struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

type Reservation struct {
	ID        string    `json:"id"`
	Items     []Item    `json:"items"`
	Status    Status    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Store is safe for concurrent use. A single mutex guards both stock and
// reservations, which is what makes a multi-item reserve all-or-nothing.
type Store struct {
	mu           sync.Mutex
	available    map[string]int
	reservations map[string]*Reservation
	ttl          time.Duration
	now          func() time.Time
}

func NewStore(ttl time.Duration) *Store {
	return &Store{
		available:    make(map[string]int),
		reservations: make(map[string]*Reservation),
		ttl:          ttl,
		now:          time.Now,
	}
}

func (s *Store) AddStock(sku string, qty int) error {
	if qty <= 0 {
		return ErrInvalidQty
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available[sku] += qty
	return nil
}

// Available returns the unreserved quantity of sku.
func (s *Store) Available(sku string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.available[sku]
	return n, ok
}

// Stock returns a copy of available quantities for every SKU.
func (s *Store) Stock() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.available))
	for sku, n := range s.available {
		out[sku] = n
	}
	return out
}

// Reserve holds stock for every item or for none of them.
func (s *Store) Reserve(items []Item) (Reservation, error) {
	if len(items) == 0 {
		return Reservation{}, ErrEmptyOrder
	}
	want := make(map[string]int, len(items))
	for _, it := range items {
		if it.Qty <= 0 {
			return Reservation{}, ErrInvalidQty
		}
		want[it.SKU] += it.Qty
	}
	id, err := newID()
	if err != nil {
		return Reservation{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check every line before touching stock, so a failure leaves nothing half-reserved.
	for _, it := range items {
		have, ok := s.available[it.SKU]
		if !ok {
			return Reservation{}, fmt.Errorf("%w: %s", ErrUnknownSKU, it.SKU)
		}
		if have < want[it.SKU] {
			return Reservation{}, &OutOfStockError{SKU: it.SKU, Requested: want[it.SKU], Available: have}
		}
	}
	for sku, qty := range want {
		s.available[sku] -= qty
	}

	now := s.now()
	r := &Reservation{
		ID:        id,
		Items:     slices.Clone(items),
		Status:    StatusPending,
		CreatedAt: now,
		ExpiresAt: now.Add(s.ttl),
	}
	s.reservations[id] = r
	return r.snapshot(), nil
}

// Confirm turns a pending reservation into a sale. A reservation past its
// deadline is expired here even if the janitor has not reached it yet.
func (s *Store) Confirm(id string) (Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.pendingLocked(id)
	if err != nil {
		return Reservation{}, err
	}
	r.Status = StatusConfirmed
	return r.snapshot(), nil
}

// Release cancels a pending reservation and returns its stock.
func (s *Store) Release(id string) (Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.pendingLocked(id)
	if err != nil {
		return Reservation{}, err
	}
	s.releaseLocked(r, StatusReleased)
	return r.snapshot(), nil
}

func (s *Store) Get(id string) (Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.reservations[id]
	if !ok {
		return Reservation{}, ErrNotFound
	}
	return r.snapshot(), nil
}

// ExpireDue releases every pending reservation whose TTL has passed and
// reports how many it released.
func (s *Store) ExpireDue() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	n := 0
	for _, r := range s.reservations {
		if r.Status == StatusPending && !now.Before(r.ExpiresAt) {
			s.releaseLocked(r, StatusExpired)
			n++
		}
	}
	return n
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
			s.ExpireDue()
		}
	}
}

func (s *Store) pendingLocked(id string) (*Reservation, error) {
	r, ok := s.reservations[id]
	if !ok {
		return nil, ErrNotFound
	}
	switch r.Status {
	case StatusPending:
	case StatusExpired:
		return nil, ErrExpired
	default:
		return nil, ErrNotPending
	}
	if !s.now().Before(r.ExpiresAt) {
		s.releaseLocked(r, StatusExpired)
		return nil, ErrExpired
	}
	return r, nil
}

func (s *Store) releaseLocked(r *Reservation, to Status) {
	for _, it := range r.Items {
		s.available[it.SKU] += it.Qty
	}
	r.Status = to
}

func (r *Reservation) snapshot() Reservation {
	c := *r
	c.Items = slices.Clone(r.Items)
	return c
}

func newID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
