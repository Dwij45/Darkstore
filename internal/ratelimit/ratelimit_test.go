package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestLimiter(rate float64, burst int) (*Limiter, *time.Time) {
	now := time.Unix(1_700_000_000, 0)
	l := New(rate, burst)
	l.now = func() time.Time { return now }
	return l, &now
}

func TestAllowsBurstThenRefuses(t *testing.T) {
	l, _ := newTestLimiter(1, 3)
	for i := range 3 {
		if ok, _ := l.Allow("u"); !ok {
			t.Fatalf("request %d refused, want allowed (burst 3)", i+1)
		}
	}
	if ok, _ := l.Allow("u"); ok {
		t.Error("4th request allowed, want refused")
	}
}

func TestRefillsOverTime(t *testing.T) {
	l, now := newTestLimiter(2, 2) // 2 tokens per second
	l.Allow("u")
	l.Allow("u")
	if ok, _ := l.Allow("u"); ok {
		t.Fatal("expected empty bucket")
	}
	*now = now.Add(500 * time.Millisecond) // +1 token
	if ok, _ := l.Allow("u"); !ok {
		t.Error("want allowed after 500ms at 2 tokens/s")
	}
	if ok, _ := l.Allow("u"); ok {
		t.Error("only one token should have been earned")
	}
}

func TestRefillIsCappedAtBurst(t *testing.T) {
	l, now := newTestLimiter(10, 2)
	l.Allow("u")
	*now = now.Add(time.Hour)
	allowed := 0
	for range 10 {
		if ok, _ := l.Allow("u"); ok {
			allowed++
		}
	}
	if allowed != 2 {
		t.Errorf("allowed = %d after a long idle, want 2 (burst cap)", allowed)
	}
}

func TestRetryAfterTellsWhenToComeBack(t *testing.T) {
	l, _ := newTestLimiter(2, 1)
	l.Allow("u")
	ok, wait := l.Allow("u")
	if ok || wait != 500*time.Millisecond {
		t.Errorf("ok=%v wait=%v, want refused with 500ms", ok, wait)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	l, _ := newTestLimiter(1, 1)
	if ok, _ := l.Allow("alice"); !ok {
		t.Fatal("alice refused")
	}
	if ok, _ := l.Allow("alice"); ok {
		t.Error("alice's second request allowed")
	}
	if ok, _ := l.Allow("bob"); !ok {
		t.Error("bob refused because of alice")
	}
}

// 1,000 simultaneous requests from one user against a burst of 50: exactly 50 pass.
func TestConcurrentRequestsNeverExceedBurst(t *testing.T) {
	l, _ := newTestLimiter(0.000001, 50) // clock is frozen, so no refill
	var allowed atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if ok, _ := l.Allow("u"); ok {
				allowed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if allowed.Load() != 50 {
		t.Errorf("allowed = %d, want 50", allowed.Load())
	}
}

func TestSweepDropsOnlyFullBuckets(t *testing.T) {
	l, now := newTestLimiter(1, 2)
	l.Allow("drained") // 1 token left
	l.Allow("drained")
	l.Allow("drained") // refused, bucket empty
	l.Allow("idle")    // 1 token left

	*now = now.Add(1500 * time.Millisecond) // idle refills to 2; drained only to 1.5
	if n := l.Sweep(); n != 1 {
		t.Errorf("swept = %d, want 1", n)
	}
	if l.Len() != 1 {
		t.Errorf("tracked keys = %d, want 1 (drained still tracked)", l.Len())
	}
	if ok, _ := l.Allow("drained"); !ok {
		t.Error("drained user should have 1.5 tokens")
	}
}

func TestZeroRateNeverRefills(t *testing.T) {
	l, now := newTestLimiter(0, 1)
	l.Allow("u")
	*now = now.Add(24 * time.Hour)
	if ok, _ := l.Allow("u"); ok {
		t.Error("allowed with a zero refill rate")
	}
}
