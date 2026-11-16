package loadtoolkit

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

// rollingMax replays grant timestamps against a true rolling window and returns
// the largest number of grants ever inside one. This is the authoritative
// check: a per-second bucket view mis-attributes grants either side of a
// boundary, and a correct mean hid a real breach on both k6's native --rps
// (rolling max 13 against a target of 10) and a token bucket (51 vs 50).
func rollingMax(grants []time.Time) int {
	sort.Slice(grants, func(i, j int) bool { return grants[i].Before(grants[j]) })
	max, start := 0, 0
	for end := range grants {
		for grants[end].Sub(grants[start]) >= Window {
			start++
		}
		if n := end - start + 1; n > max {
			max = n
		}
	}
	return max
}

func TestCeilingHoldsUnderConcurrentVUs(t *testing.T) {
	// One limiter, many goroutines — the arrangement a pod actually runs, since
	// every VU shares the RootModule's window.
	for _, rate := range []int{5, 50} {
		for _, vus := range []int{1, 15, 50} {
			l, err := NewSlidingWindowLimiter(rate)
			if err != nil {
				t.Fatalf("rate=%d: %v", rate, err)
			}

			var mu sync.Mutex
			var grants []time.Time
			deadline := time.Now().Add(1200 * time.Millisecond)

			var wg sync.WaitGroup
			for i := 0; i < vus; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for time.Now().Before(deadline) {
						if err := l.Acquire(context.Background()); err != nil {
							return
						}
						mu.Lock()
						grants = append(grants, time.Now())
						mu.Unlock()
					}
				}()
			}
			wg.Wait()

			if got := rollingMax(grants); got > rate {
				t.Errorf("rate=%d vus=%d: rolling-1s max %d exceeds the ceiling", rate, vus, got)
			}
		}
	}
}

func TestAcquireUnblocksOnCancellation(t *testing.T) {
	// A VU parked in Acquire when a run is aborted must wake, or the run hangs
	// at teardown instead of exiting.
	l, err := NewSlidingWindowLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Acquire(context.Background()); err != nil { // fills the window
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Acquire(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a cancellation error, got a grant")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Acquire did not return after the context was cancelled")
	}
}

func TestSeedingPacesTheFirstWindow(t *testing.T) {
	// An unseeded window would hand out all `limit` grants in milliseconds — a
	// startup spike, measured at 10 grants inside 107ms before seeding was
	// added. Seeded, the first grant is still immediate (the synthetic window is
	// already a second old) but the rest are paced.
	l, err := NewSlidingWindowLimiter(10)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := l.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if first := time.Since(start); first > 20*time.Millisecond {
		t.Errorf("first grant waited %v; seeding must not delay the start of traffic", first)
	}
	for i := 0; i < 9; i++ {
		if err := l.Acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// 10 grants at 10/s occupy ~1s once paced; an unseeded window did it in 107ms.
	if elapsed := time.Since(start); elapsed < 800*time.Millisecond {
		t.Errorf("first 10 grants took %v; the window is not seeded and burst at startup", elapsed)
	}
}

func TestAcquireNTakesOneGrantPerRequest(t *testing.T) {
	// http.batch(n) must consume n grants: a batch of 5 is 5 ClickHouse rows.
	// Counted, not timed — with a seeded window every grant is paced identically,
	// so elapsed time cannot distinguish n grants from one.
	l, err := NewSlidingWindowLimiter(50)
	if err != nil {
		t.Fatal(err)
	}
	l.EnableTrace()
	if err := l.AcquireN(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	if got := len(l.Trace()); got != 5 {
		t.Errorf("AcquireN(5) consumed %d grants, want 5", got)
	}
}

func TestAcquireNIsAtomic(t *testing.T) {
	// n grants must all land at the same instant, or a caller that fires n
	// requests together (http.batch) bursts past the ceiling on the wire while
	// the ledger still looks honest. Measured on staging before this fix: 15
	// sends in a second against a target of 10.
	l, err := NewSlidingWindowLimiter(10)
	if err != nil {
		t.Fatal(err)
	}
	l.EnableTrace()
	for i := 0; i < 4; i++ {
		if err := l.AcquireN(context.Background(), 3); err != nil {
			t.Fatal(err)
		}
	}
	grants := l.Trace()
	if len(grants) != 12 {
		t.Fatalf("got %d grants, want 12", len(grants))
	}
	// Each group of 3 shares one timestamp.
	for i := 0; i < 12; i += 3 {
		if grants[i] != grants[i+1] || grants[i] != grants[i+2] {
			t.Errorf("batch starting at %d did not share a timestamp: %v", i, grants[i:i+3])
		}
	}
	ts := make([]time.Time, len(grants))
	for i, ns := range grants {
		ts[i] = time.Unix(0, ns)
	}
	if got := rollingMax(ts); got > 10 {
		t.Errorf("rolling-1s max %d exceeds the ceiling", got)
	}
}

func TestAcquireNLargerThanRateIsChunked(t *testing.T) {
	// A batch bigger than the per-second rate cannot fit one window; it must be
	// split rather than rejected or granted all at once.
	l, err := NewSlidingWindowLimiter(3)
	if err != nil {
		t.Fatal(err)
	}
	l.EnableTrace()
	if err := l.AcquireN(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	grants := l.Trace()
	if len(grants) != 7 {
		t.Fatalf("got %d grants, want 7", len(grants))
	}
	ts := make([]time.Time, len(grants))
	for i, ns := range grants {
		ts[i] = time.Unix(0, ns)
	}
	if got := rollingMax(ts); got > 3 {
		t.Errorf("rolling-1s max %d exceeds the ceiling of 3", got)
	}
}

func TestRateBelowOneIsRejected(t *testing.T) {
	if _, err := NewSlidingWindowLimiter(0); err == nil {
		t.Error("rate 0 must be rejected; a 0-rps pod share is handled by the runner, not silently unthrottled here")
	}
}

// TestAcquireNIsNotStarvedBySingleGrants is the regression for the fairness gate.
//
// tryGrantN(n) needs n slots free simultaneously; single-grant callers only need the
// oldest slot and keep refilling it. Without the gate, a full-window batch at a busy
// rate is refused on every retry, blocks for the entire run, and unwinds with
// `context canceled` at teardown — its requests never leave the pod, while the run
// still reports as healthy. That is silent under-delivery, the failure mode this
// limiter exists to rule out.
//
// The assertion is deliberately loose on timing (a generous deadline, not a tight
// one) because the point is "completes at all", not "completes fast".
func TestAcquireNIsNotStarvedBySingleGrants(t *testing.T) {
	const rate = 10
	l, err := NewSlidingWindowLimiter(rate)
	if err != nil {
		t.Fatalf("NewSlidingWindowLimiter: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	// Hammer single grants from several VUs for the whole test, so the oldest slot is
	// always being taken the instant it frees.
	stop := make(chan struct{})
	var singles sync.WaitGroup
	for i := 0; i < 4; i++ {
		singles.Add(1)
		go func() {
			defer singles.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := l.Acquire(ctx); err != nil {
					return
				}
			}
		}()
	}

	// Give the singles time to saturate the window before the batch asks for it.
	time.Sleep(300 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- l.AcquireN(ctx, rate) }() // a full-window batch

	select {
	case err := <-done:
		close(stop)
		singles.Wait()
		if err != nil {
			t.Fatalf("AcquireN(%d) failed instead of being served: %v", rate, err)
		}
	case <-time.After(4 * time.Second):
		close(stop)
		singles.Wait()
		t.Fatalf("AcquireN(%d) was still blocked after 4s — starved by single-grant callers", rate)
	}
}
