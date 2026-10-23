package fulfilment

import (
	"math"
	"sync"
)

type Rider struct {
	ID string  `json:"id"`
	X  float64 `json:"x"`
	Y  float64 `json:"y"`
}

// Riders is the pool of riders waiting at or near the store.
type Riders struct {
	mu   sync.Mutex
	idle map[string]Rider
}

func NewRiders(rs ...Rider) *Riders {
	r := &Riders{idle: make(map[string]Rider, len(rs))}
	for _, rider := range rs {
		r.idle[rider.ID] = rider
	}
	return r
}

// AssignNearest removes and returns the idle rider closest to (x, y). Ties go
// to the lower ID so the choice is deterministic.
func (r *Riders) AssignNearest(x, y float64) (Rider, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var best Rider
	bestDist := math.Inf(1)
	for _, rider := range r.idle {
		d := math.Hypot(rider.X-x, rider.Y-y)
		if d < bestDist || (d == bestDist && rider.ID < best.ID) {
			best, bestDist = rider, d
		}
	}
	if math.IsInf(bestDist, 1) {
		return Rider{}, false
	}
	delete(r.idle, best.ID)
	return best, true
}

func (r *Riders) Release(rider Rider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.idle[rider.ID] = rider
}

func (r *Riders) Idle() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.idle)
}
