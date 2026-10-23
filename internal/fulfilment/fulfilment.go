// Package fulfilment moves a paid order through pick, pack and delivery,
// assigning the nearest free rider. Durations are simulated.
package fulfilment

import (
	"context"
	"sync"
	"time"
)

type Stage string

const (
	StageQueued         Stage = "queued"
	StagePicking        Stage = "picking"
	StagePacked         Stage = "packed"
	StageAwaitingRider  Stage = "awaiting_rider"
	StageOutForDelivery Stage = "out_for_delivery"
	StageDelivered      Stage = "delivered"
	StageCancelled      Stage = "cancelled"
)

type Status struct {
	Stage   Stage  `json:"stage"`
	RiderID string `json:"rider_id,omitempty"`
}

// Tracker records the latest fulfilment stage of each order.
type Tracker struct {
	mu     sync.RWMutex
	orders map[string]Status
}

func NewTracker() *Tracker {
	return &Tracker{orders: make(map[string]Status)}
}

func (t *Tracker) Set(orderID string, s Status) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.orders[orderID] = s
}

func (t *Tracker) Delete(orderID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.orders, orderID)
}

func (t *Tracker) Get(orderID string) (Status, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.orders[orderID]
	return s, ok
}

type Fulfiller struct {
	Riders  *Riders
	Tracker *Tracker

	StoreX, StoreY float64

	PickTime     time.Duration
	PackTime     time.Duration
	DeliveryTime time.Duration
	// RiderRetry is how long to wait before looking again when no rider is free.
	RiderRetry time.Duration
}

// Process runs one order to completion, or marks it cancelled if ctx ends
// first. It has the signature worker.Pool expects of a handler.
func (f *Fulfiller) Process(ctx context.Context, orderID string) {
	f.Tracker.Set(orderID, Status{Stage: StagePicking})
	if !sleep(ctx, f.PickTime) {
		f.cancel(orderID)
		return
	}
	f.Tracker.Set(orderID, Status{Stage: StagePacked})
	if !sleep(ctx, f.PackTime) {
		f.cancel(orderID)
		return
	}

	rider, ok := f.waitForRider(ctx, orderID)
	if !ok {
		f.cancel(orderID)
		return
	}
	// The rider goes back to the pool whatever happens on the trip.
	defer f.Riders.Release(rider)

	f.Tracker.Set(orderID, Status{Stage: StageOutForDelivery, RiderID: rider.ID})
	if !sleep(ctx, f.DeliveryTime) {
		f.Tracker.Set(orderID, Status{Stage: StageCancelled, RiderID: rider.ID})
		return
	}
	f.Tracker.Set(orderID, Status{Stage: StageDelivered, RiderID: rider.ID})
}

func (f *Fulfiller) waitForRider(ctx context.Context, orderID string) (Rider, bool) {
	retry := f.RiderRetry
	if retry <= 0 {
		retry = 100 * time.Millisecond
	}
	for {
		if r, ok := f.Riders.AssignNearest(f.StoreX, f.StoreY); ok {
			return r, true
		}
		f.Tracker.Set(orderID, Status{Stage: StageAwaitingRider})
		if !sleep(ctx, retry) {
			return Rider{}, false
		}
	}
}

func (f *Fulfiller) cancel(orderID string) {
	f.Tracker.Set(orderID, Status{Stage: StageCancelled})
}

// sleep waits for d and reports false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
