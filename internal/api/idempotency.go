package api

import (
	"crypto/sha256"
	"errors"
	"sync"
)

var errKeyReused = errors.New("idempotency key reused with a different request body")

// idemEntry is the stored outcome of the first request seen for a key.
// done is closed once status and body are set.
type idemEntry struct {
	fingerprint [sha256.Size]byte
	done        chan struct{}
	status      int
	body        []byte
}

// idempotencyCache replays the first response for a given key. Requests that
// arrive while the first one is still running wait for it instead of racing
// it, so a client retry can never create a second order.
//
// Entries are kept for the life of the process; a real deployment would
// expire them (e.g. after 24h) and store them in Redis or Postgres.
type idempotencyCache struct {
	mu      sync.Mutex
	entries map[string]*idemEntry
}

func newIdempotencyCache() *idempotencyCache {
	return &idempotencyCache{entries: make(map[string]*idemEntry)}
}

// begin returns the entry for key. owner is true when the caller is the first
// request for this key and must call finish; otherwise the caller should wait
// on entry.done and replay the stored response.
func (c *idempotencyCache) begin(key string, fp [sha256.Size]byte) (e *idemEntry, owner bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok {
		if e.fingerprint != fp {
			return nil, false, errKeyReused
		}
		return e, false, nil
	}
	e = &idemEntry{fingerprint: fp, done: make(chan struct{})}
	c.entries[key] = e
	return e, true, nil
}

func (c *idempotencyCache) finish(e *idemEntry, status int, body []byte) {
	e.status = status
	e.body = body
	close(e.done)
}
