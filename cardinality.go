package loadtoolkit

import (
	"sync"
	"sync/atomic"

	"go.k6.io/k6/v2/metrics"
)

// cardinalityGuard counts distinct metric time series (metric + indexed tag
// set, as k6 core counts them) and trips once a budget is crossed.
//
// A script that tags samples with a per-iteration value mints a new series per
// iteration, and that memory lives in k6 core, upstream of every output. An
// output cannot free it by dropping samples; it can stop the run, which stops
// new series from being minted. The budget is per process.
type cardinalityGuard struct {
	mu    sync.Mutex
	seen  map[metrics.TimeSeries]struct{}
	limit int // <= 0 disables the guard

	// tripped is read lock-free first, so after the trip each sample costs one atomic load.
	tripped atomic.Bool
	count   atomic.Int64
}

// newCardinalityGuard returns a guard for the budget. A limit <= 0 never
// allocates and never trips.
func newCardinalityGuard(limit int) *cardinalityGuard {
	g := &cardinalityGuard{limit: limit}
	if limit > 0 {
		g.seen = make(map[metrics.TimeSeries]struct{})
	}
	return g
}

func (g *cardinalityGuard) enabled() bool {
	return g != nil && g.limit > 0
}

// observe records one series and returns true exactly once: on the call that
// first pushes the distinct count past the limit.
func (g *cardinalityGuard) observe(ts metrics.TimeSeries) bool {
	if g == nil || g.limit <= 0 || g.tripped.Load() {
		return false
	}

	g.mu.Lock()
	if _, ok := g.seen[ts]; ok {
		g.mu.Unlock()
		return false
	}
	g.seen[ts] = struct{}{}
	n := len(g.seen)
	g.mu.Unlock()

	g.count.Store(int64(n))
	if n > g.limit {
		return g.tripped.CompareAndSwap(false, true)
	}
	return false
}

// distinctCount is the number of distinct series seen; it settles near the
// limit once the guard trips.
func (g *cardinalityGuard) distinctCount() int64 {
	if g == nil {
		return 0
	}
	return g.count.Load()
}
