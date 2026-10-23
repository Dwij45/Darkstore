// Package worker is a fixed-size goroutine pool fed by a bounded channel.
package worker

import (
	"context"
	"errors"
	"sync"
)

var ErrClosed = errors.New("worker pool is shut down")

// Pool runs handle for each submitted job on one of a fixed number of
// goroutines. The bounded queue is the backpressure point: when it is full,
// Submit blocks until a worker frees a slot or the caller's context ends.
type Pool[T any] struct {
	jobs    chan T
	handle  func(context.Context, T)
	workers int
	wg      sync.WaitGroup

	// mu makes "check closed, then send" atomic with respect to Shutdown,
	// so a Submit can never send on a closed channel and panic.
	mu     sync.RWMutex
	closed bool
}

func New[T any](workers, queueSize int, handle func(context.Context, T)) *Pool[T] {
	if workers < 1 {
		workers = 1
	}
	return &Pool[T]{
		jobs:    make(chan T, queueSize),
		handle:  handle,
		workers: workers,
	}
}

// Start launches the workers. ctx is passed to every handler; cancel it to
// ask in-flight jobs to stop early.
func (p *Pool[T]) Start(ctx context.Context) {
	for range p.workers {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for job := range p.jobs {
				p.handle(ctx, job)
			}
		}()
	}
}

// Submit enqueues job, waiting for queue space until ctx is done.
func (p *Pool[T]) Submit(ctx context.Context, job T) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	select {
	case p.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown stops accepting jobs and waits for the queue to drain. If ctx ends
// first it returns ctx.Err() and the workers keep draining in the background;
// cancel the context given to Start to make them stop.
func (p *Pool[T]) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.jobs)
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
