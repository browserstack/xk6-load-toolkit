package sink

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.k6.io/k6/v2/metrics"
)

func distinctSeriesSample(reg *metrics.Registry, id int, at time.Time) metrics.Sample {
	return mkSample(reg, "http_req_duration", map[string]string{"series": strconv.Itoa(id)}, at, 1)
}

func TestCardinalityGuard_CountsDistinctAndDedupes(t *testing.T) {
	reg := metrics.NewRegistry()
	g := newCardinalityGuard(100)
	for round := 0; round < 3; round++ {
		for i := 0; i < 5; i++ {
			if g.observe(distinctSeriesSample(reg, i, time.Unix(1700000000, 0)).TimeSeries) {
				t.Fatal("guard tripped below its limit")
			}
		}
	}
	if got := g.distinctCount(); got != 5 {
		t.Fatalf("distinctCount() = %d, want 5", got)
	}
}

func TestCardinalityGuard_TripsOnceOnCrossing(t *testing.T) {
	reg := metrics.NewRegistry()
	const limit = 10
	g := newCardinalityGuard(limit)
	trips, tripAt := 0, -1
	for i := 0; i < limit*3; i++ {
		if g.observe(distinctSeriesSample(reg, i, time.Unix(1700000000, 0)).TimeSeries) {
			trips++
			if tripAt == -1 {
				tripAt = i
			}
		}
	}
	if trips != 1 || tripAt != limit {
		t.Fatalf("trips=%d at index %d, want 1 at %d", trips, tripAt, limit)
	}
}

func TestCardinalityGuard_DisabledNeverTrips(t *testing.T) {
	reg := metrics.NewRegistry()
	for _, limit := range []int{0, -1, -100} {
		g := newCardinalityGuard(limit)
		if g.enabled() || g.seen != nil {
			t.Fatalf("guard with limit %d is enabled or allocated", limit)
		}
		for i := 0; i < 1000; i++ {
			if g.observe(distinctSeriesSample(reg, i, time.Unix(1700000000, 0)).TimeSeries) {
				t.Fatalf("disabled guard (limit %d) tripped", limit)
			}
		}
		if g.distinctCount() != 0 {
			t.Fatalf("disabled guard counted series")
		}
	}
}

func TestCardinalityGuard_ConcurrentTripsOnce(t *testing.T) {
	reg := metrics.NewRegistry()
	const limit, total = 50, 500
	g := newCardinalityGuard(limit)
	series := make([]metrics.TimeSeries, total)
	for i := range series {
		series[i] = distinctSeriesSample(reg, i, time.Unix(1700000000, 0)).TimeSeries
	}
	var trips atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < total; i++ {
				if g.observe(series[i]) {
					trips.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := trips.Load(); got != 1 {
		t.Fatalf("concurrent observe tripped %d times, want 1", got)
	}
}
