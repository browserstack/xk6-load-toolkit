package loadtoolkit

// The request-rate limiter: at most `rate` grants in any rolling one-second
// window, never more. A sliding window was chosen over a token bucket and a
// slot scheduler, both of which overshot the ceiling when measured.

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Window is the rolling interval the ceiling applies to. One second, because
// the contract and the reported Request Rate series are both per-second.
const Window = time.Second

// SlidingWindowLimiter grants at most `limit` acquisitions per Window.
//
// It holds the timestamps of the last `limit` grants in a ring buffer; a grant
// is refused until the oldest of them has aged out of the window. Nothing is
// clamped, so unlike a token bucket the ceiling costs no accuracy.
//
// One instance is shared by every VU in the process, so all methods are safe
// for concurrent use.
type SlidingWindowLimiter struct {
	mu     sync.Mutex
	limit  int
	ring   []time.Time // len == limit; ring[next] is the oldest grant
	next   int
	seeded bool

	// trace, when non-nil, records every grant so the ceiling can be proven on a
	// real run rather than inferred from a mean. Bounded by traceCap; off by default.
	trace    []int64
	traceCap int

	// turn serialises the RIGHT TO ATTEMPT a grant, one caller at a time, so a
	// caller needing several slots cannot be starved by callers needing one.
	//
	// Without it, tryGrantN(n) needs n slots free simultaneously while single-grant
	// callers only need the oldest — and they keep refilling it. At rate=10 with
	// mixed single + http.batch(10) traffic the batch is refused on every retry,
	// blocks for the whole run, and unwinds with `context canceled` at teardown:
	// its requests never leave. Buffered to 1; the holder keeps it across its sleep,
	// so whoever is waiting is guaranteed the next free slots rather than racing for
	// them. Throughput is unaffected because the limiter, not the queue, is the
	// bottleneck — the holder sleeps exactly until its own slots free.
	turn chan struct{}

	// Injectable for tests; nil means real time.
	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

// defaultTraceCap bounds the diagnostic trace: 500k grants is ~14 minutes at
// 600 rps, and 4MB of int64s.
const defaultTraceCap = 500_000

// NewSlidingWindowLimiter returns a limiter for `limit` grants per second.
func NewSlidingWindowLimiter(limit int) (*SlidingWindowLimiter, error) {
	if limit < 1 {
		return nil, fmt.Errorf("throughput: rate must be >= 1, got %d", limit)
	}
	return &SlidingWindowLimiter{
		limit:    limit,
		ring:     make([]time.Time, limit),
		turn:     make(chan struct{}, 1),
		traceCap: defaultTraceCap,
	}, nil
}

// Rate is the configured per-second ceiling.
func (l *SlidingWindowLimiter) Rate() int { return l.limit }

// Acquire blocks until a grant is available, and returns ctx.Err() if the
// context is cancelled first. Cancellation is not optional: a VU parked here
// when a run is aborted or torn down must wake, or the run hangs instead of
// exiting.
func (l *SlidingWindowLimiter) Acquire(ctx context.Context) error {
	return l.AcquireN(ctx, 1)
}

// AcquireN takes n grants that all become effective at the SAME instant, for a
// caller that fires n requests together — http.batch(n) is n requests on the
// wire simultaneously, and n rows in ClickHouse.
//
// Taking the n grants one at a time is wrong here, and measurably so: the grants
// spread across n/rate seconds while the batch fires only once they are all in
// hand, which decouples the ledger from the wire. Measured at rate=10 with
// batches of 3: grant-side rolling max 10 (honest) while the target server saw
// up to 15 sends in a second. The contract is about requests sent, so the window
// has to be reserved for the whole batch before any of it leaves.
//
// A batch larger than the per-second rate cannot be granted atomically, so it is
// split into chunks of at most `limit`; each chunk is itself a legal burst that
// exactly fills one window.
func (l *SlidingWindowLimiter) AcquireN(ctx context.Context, n int) error {
	for n > 0 {
		chunk := n
		if chunk > l.limit {
			chunk = l.limit
		}
		// Take the turn per chunk rather than for the whole call: a batch larger than
		// the rate still makes guaranteed progress, but it releases between chunks so
		// single-grant callers are not blocked for ceil(n/limit) seconds.
		select {
		case l.turn <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		err := func() error {
			defer func() { <-l.turn }()
			for {
				wait := l.tryGrantN(chunk)
				if wait <= 0 {
					return nil
				}
				if err := l.doSleep(ctx, wait); err != nil {
					return err
				}
			}
		}()
		if err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

// AcquireUpTo is AcquireN capped at the rate, returning the grants taken.
func (l *SlidingWindowLimiter) AcquireUpTo(ctx context.Context, n int) (int, error) {
	g := min(n, l.limit)
	return g, l.AcquireN(ctx, g)
}

// Scheduled is false: this is the flat window.
func (l *SlidingWindowLimiter) Scheduled() bool { return false }

// tryGrantN records n grants at one instant and returns 0, or returns how long
// the caller must wait before retrying. The sleep deliberately happens in
// AcquireN, outside the lock, so a waiting caller does not block the others.
// Caller guarantees 1 <= n <= limit.
func (l *SlidingWindowLimiter) tryGrantN(n int) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.timeNow()

	// Seed on the FIRST request, not at construction: the limiter is built at
	// boot but traffic only starts after the VU ramp, by which time grants
	// synthesised at construction would all have aged out. An unseeded window
	// lets the first `limit` requests through in milliseconds — measured as a
	// 10-grant burst inside 107ms at 10 rps. That stays within the ceiling, but
	// it is a visible startup spike. Filling the window with grants spread across the second that
	// just elapsed makes the first real request wait its turn like any other.
	if !l.seeded {
		l.seed(now)
	}

	// The ring holds grants oldest-first from l.next, so the n-th oldest is the
	// binding one: once it has aged out, so have the n-1 before it, and all n new
	// grants fit inside this window.
	idx := (l.next + n - 1) % l.limit
	if wait := l.ring[idx].Add(Window).Sub(now); wait > 0 {
		return wait
	}
	for i := 0; i < n; i++ {
		l.record(now)
	}
	return 0
}

// seed fills the window with synthetic grants evenly spread across the second
// that just elapsed; caller holds the lock. The oldest is one window old, so it
// ages out immediately and the first real request is granted without waiting a
// full second — but the tenth is paced, not instant.
func (l *SlidingWindowLimiter) seed(now time.Time) {
	interval := Window / time.Duration(l.limit)
	for i := 0; i < l.limit; i++ {
		l.ring[i] = now.Add(-Window + time.Duration(i)*interval)
	}
	l.next = 0
	l.seeded = true
}

// record overwrites the oldest slot; caller holds the lock.
func (l *SlidingWindowLimiter) record(t time.Time) {
	l.ring[l.next] = t
	l.next++
	if l.next == l.limit {
		l.next = 0
	}
	if l.trace != nil && len(l.trace) < l.traceCap {
		l.trace = append(l.trace, t.UnixNano())
	}
}

// EnableTrace starts recording grant timestamps.
func (l *SlidingWindowLimiter) EnableTrace() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.trace == nil {
		l.trace = make([]int64, 0, 1024)
	}
}

// Trace returns a copy of the recorded grant timestamps in nanoseconds.
func (l *SlidingWindowLimiter) Trace() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]int64, len(l.trace))
	copy(out, l.trace)
	return out
}

func (l *SlidingWindowLimiter) timeNow() time.Time {
	if l.now != nil {
		return l.now()
	}
	return time.Now()
}

func (l *SlidingWindowLimiter) doSleep(ctx context.Context, d time.Duration) error {
	if l.sleep != nil {
		return l.sleep(ctx, d)
	}
	return sleepCtx(ctx, d)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	if ctx == nil {
		<-t.C
		return nil
	}
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
