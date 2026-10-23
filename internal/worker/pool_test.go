package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProcessesEveryJob(t *testing.T) {
	var sum atomic.Int64
	p := New(4, 10, func(_ context.Context, n int) { sum.Add(int64(n)) })
	p.Start(context.Background())

	for i := 1; i <= 100; i++ {
		if err := p.Submit(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := sum.Load(); got != 5050 {
		t.Errorf("sum = %d, want 5050", got)
	}
}

func TestShutdownDrainsQueuedJobs(t *testing.T) {
	release := make(chan struct{})
	var done atomic.Int64
	p := New(1, 10, func(_ context.Context, _ int) {
		<-release
		done.Add(1)
	})
	p.Start(context.Background())
	for i := range 5 {
		p.Submit(context.Background(), i)
	}

	shut := make(chan error, 1)
	go func() { shut <- p.Shutdown(context.Background()) }()
	close(release)

	if err := <-shut; err != nil {
		t.Fatal(err)
	}
	if done.Load() != 5 {
		t.Errorf("completed = %d, want 5 (queued jobs must not be dropped)", done.Load())
	}
}

func TestSubmitAfterShutdownIsRejected(t *testing.T) {
	p := New(1, 1, func(context.Context, int) {})
	p.Start(context.Background())
	p.Shutdown(context.Background())
	if err := p.Submit(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Errorf("err = %v, want ErrClosed", err)
	}
}

func TestSubmitBlocksWhenQueueIsFull(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	p := New(1, 1, func(context.Context, int) { <-block })
	p.Start(context.Background())

	p.Submit(context.Background(), 1) // taken by the worker
	p.Submit(context.Background(), 2) // fills the queue

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Submit(ctx, 3); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
}

func TestShutdownTimeoutThenCancelStopsWorkers(t *testing.T) {
	workCtx, cancelWork := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	p := New(1, 1, func(ctx context.Context, _ int) {
		<-ctx.Done()
		close(stopped)
	})
	p.Start(workCtx)
	p.Submit(context.Background(), 1)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	cancelWork()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after its context was cancelled")
	}
}

// Submit and Shutdown racing must never panic with "send on closed channel".
func TestConcurrentSubmitAndShutdown(t *testing.T) {
	for range 50 {
		p := New(2, 4, func(context.Context, int) {})
		p.Start(context.Background())
		var wg sync.WaitGroup
		for i := range 20 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := p.Submit(context.Background(), i)
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
		p.Shutdown(context.Background())
		wg.Wait()
	}
}
