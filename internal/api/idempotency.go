package api

import (
	"crypto/sha256"
	"errors"
	"sync"
	"time"
)

var errKeyReused = errors.New("idempotency key reused with a different request body")

// idemEntry is the stored outcome of the first request seen for a key.
// done is closed once status and body are set.
type idemEntry struct {
	fingerprint [sha256.Size]byte
	done        chan struct{}
	status      int
	body        []byte
	finishedAt  time.Time
}

// idempotencyCache replays the first response for a given key. Requests that
// arrive while the first one is still running wait for it instead of racing
// it, so a client retry can never create a second order.
//
// A finished entry is kept for ttl and then forgotten, so memory is bounded.
// An entry still in flight is never dropped. The cache is in memory; a
// multi-instance deployment would store it in Redis or Postgres.
type idempotencyCache struct {
	mu      sync.Mutex
	entries map[string]*idemEntry
	ttl     time.Duration
	now     func() time.Time
}

func newIdempotencyCache(ttl time.Duration) *idempotencyCache {
	return &idempotencyCache{
		entries: make(map[string]*idemEntry),
		ttl:     ttl,
		now:     time.Now,
	}
}

// begin returns the entry for key. owner is true when the caller is the first
// request for this key and must call finish; otherwise the caller should wait
// on entry.done and replay the stored response.
func (c *idempotencyCache) begin(key string, fp [sha256.Size]byte) (e *idemEntry, owner bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok {
		if !c.expired(e) {
			if e.fingerprint != fp {
				return nil, false, errKeyReused
			}
			return e, false, nil
		}
		delete(c.entries, key) // too old to replay: treat this as a new request
	}
	e = &idemEntry{fingerprint: fp, done: make(chan struct{})}
	c.entries[key] = e
	return e, true, nil
}

// forget removes e's entry so the next request with the same key runs afresh.
// Requests already waiting on e still receive its response.
func (c *idempotencyCache) forget(key string, e *idemEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries[key] == e {
		delete(c.entries, key)
	}
}

func (c *idempotencyCache) finish(e *idemEntry, status int, body []byte) {
	c.mu.Lock()
	e.finishedAt = c.now()
	c.mu.Unlock()
	e.status = status
	e.body = body
	close(e.done)
}

// sweep removes finished entries older than the ttl and reports how many.
func (c *idempotencyCache) sweep() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for key, e := range c.entries {
		if c.expired(e) {
			delete(c.entries, key)
			n++
		}
	}
	return n
}

func (c *idempotencyCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// expired is true only for a finished entry past its ttl. Callers hold c.mu.
func (c *idempotencyCache) expired(e *idemEntry) bool {
	if c.ttl <= 0 || e.finishedAt.IsZero() {
		return false
	}
	return c.now().Sub(e.finishedAt) >= c.ttl
}
