package fulfilment

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAssignNearestPicksClosestFreeRider(t *testing.T) {
	r := NewRiders(
		Rider{ID: "far", X: 10, Y: 10},
		Rider{ID: "near", X: 1, Y: 0},
		Rider{ID: "mid", X: 3, Y: 4},
	)
	want := []string{"near", "mid", "far"}
	for _, id := range want {
		got, ok := r.AssignNearest(0, 0)
		if !ok || got.ID != id {
			t.Fatalf("got %q (ok=%v), want %q", got.ID, ok, id)
		}
	}
	if _, ok := r.AssignNearest(0, 0); ok {
		t.Error("expected no rider once all are assigned")
	}
}

// Two orders must never be given the same rider.
func TestConcurrentAssignmentNeverDoubleBooks(t *testing.T) {
	const n = 100
	rs := make([]Rider, n)
	for i := range rs {
		rs[i] = Rider{ID: fmt.Sprintf("r%03d", i), X: float64(i)}
	}
	r := NewRiders(rs...)

	var mu sync.Mutex
	seen := make(map[string]int)
	var wg sync.WaitGroup
	for range n * 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rider, ok := r.AssignNearest(0, 0); ok {
				mu.Lock()
				seen[rider.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != n {
		t.Errorf("assigned %d distinct riders, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Errorf("rider %s assigned %d times", id, c)
		}
	}
}

func TestProcessDeliversAndFreesRider(t *testing.T) {
	riders := NewRiders(Rider{ID: "r1"})
	f := &Fulfiller{Riders: riders, Tracker: NewTracker()}

	f.Process(context.Background(), "o1")

	got, _ := f.Tracker.Get("o1")
	if got.Stage != StageDelivered || got.RiderID != "r1" {
		t.Errorf("status = %+v, want delivered by r1", got)
	}
	if riders.Idle() != 1 {
		t.Errorf("idle riders = %d, want 1", riders.Idle())
	}
}

func TestProcessWaitsForARider(t *testing.T) {
	riders := NewRiders()
	f := &Fulfiller{Riders: riders, Tracker: NewTracker(), RiderRetry: 5 * time.Millisecond}

	done := make(chan struct{})
	go func() { f.Process(context.Background(), "o1"); close(done) }()

	time.Sleep(20 * time.Millisecond)
	if s, _ := f.Tracker.Get("o1"); s.Stage != StageAwaitingRider {
		t.Fatalf("stage = %s, want awaiting_rider", s.Stage)
	}
	riders.Release(Rider{ID: "late"})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("order was not delivered after a rider became free")
	}
	if s, _ := f.Tracker.Get("o1"); s.Stage != StageDelivered || s.RiderID != "late" {
		t.Errorf("status = %+v, want delivered by late", s)
	}
}

func TestProcessStopsOnCancelAndReturnsRider(t *testing.T) {
	riders := NewRiders(Rider{ID: "r1"})
	f := &Fulfiller{Riders: riders, Tracker: NewTracker(), DeliveryTime: time.Hour}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Process(ctx, "o1"); close(done) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	if s, _ := f.Tracker.Get("o1"); s.Stage != StageCancelled {
		t.Errorf("stage = %s, want cancelled", s.Stage)
	}
	if riders.Idle() != 1 {
		t.Errorf("idle riders = %d, want 1 (rider must be released)", riders.Idle())
	}
}
