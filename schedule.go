package loadtoolkit

// Ramped throughput: a time-varying ceiling. The rate follows a piecewise-linear
// schedule of per-process targets, and the contract is: for every t, requests
// granted in (t-1s, t] are at most floor(W(t)), where W(t) = A(t) - A(t-1s) and
// A is the integral of the schedule.
//
// The grant rule is "window-min": grant at t iff
// count(t-1s, t] + n <= L(t) = floor(min over s in [t, t+1s] of W(s) + eps).
// Looking one second ahead is what makes it sufficient: every grant still in
// the window at any later instant s was admitted against a bound <= W(s). It is
// identical to the exact rule wherever W is non-decreasing (holds, rising
// ramps, flat) and only more conservative on falling segments.
//
// Flat throughput never builds this type — modules.go selects it only when the
// throughput section carries a schedule — so SlidingWindowLimiter is untouched.

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

// scheduleEpsilon keeps a hold's exact-integer W from flooring to k-1 on float
// error. testdata/schedule_fixtures.json records it for other implementations.
const scheduleEpsilon = 1e-9

// scheduleMaxWait bounds a refused caller's sleep, so a growing budget is seen
// within 10ms. Only the turn holder sleeps: ~100 wake-ups/s per process at most.
const scheduleMaxWait = 10 * time.Millisecond

// Segment is one linear piece of a schedule, as carried by throughput.schedule.stages.
type Segment struct {
	FromRps     int `json:"fromRps"`
	ToRps       int `json:"toRps"`
	DurationSec int `json:"durationSec"`
}

// schedule evaluates rate / A / W / L on the schedule clock, in integer
// nanoseconds since t0. Integer time keeps a full window inside a hold exactly
// 1.0s long, so W of a hold is exactly the integer rate at any run length.
type schedule struct {
	segs    []Segment
	bNs     []int64 // breakpoints: bNs[0] = 0, bNs[len(segs)] = total
	totalNs int64
	peak    int
}

func newSchedule(segs []Segment) schedule {
	s := schedule{segs: segs, bNs: make([]int64, len(segs)+1)}
	for i, sg := range segs {
		s.bNs[i+1] = s.bNs[i] + int64(sg.DurationSec)*int64(time.Second)
		s.peak = max(s.peak, sg.FromRps, sg.ToRps)
	}
	s.totalNs = s.bNs[len(segs)]
	return s
}

// seg is the index of the segment holding ns, or -1 outside [0, total).
func (s schedule) seg(ns int64) int {
	if ns < 0 || ns >= s.totalNs {
		return -1
	}
	return sort.Search(len(s.segs), func(i int) bool { return s.bNs[i+1] > ns })
}

// rateIn is segment k's line evaluated at ns (ns may sit on either end).
func (s schedule) rateIn(k int, ns int64) float64 {
	sg := s.segs[k]
	return float64(sg.FromRps) + float64(sg.ToRps-sg.FromRps)*float64(ns-s.bNs[k])/float64(s.bNs[k+1]-s.bNs[k])
}

func (s schedule) rateAt(ns int64) float64 {
	if k := s.seg(ns); k >= 0 {
		return s.rateIn(k, ns)
	}
	return 0
}

// slopeAt is d rate / d s, in rps per second.
func (s schedule) slopeAt(ns int64) float64 {
	if k := s.seg(ns); k >= 0 {
		return float64(s.segs[k].ToRps-s.segs[k].FromRps) / float64(s.segs[k].DurationSec)
	}
	return 0
}

// integral is the schedule's request budget over [a, b].
func (s schedule) integral(a, b int64) float64 {
	a, b = max(a, 0), min(b, s.totalNs)
	sum := 0.0
	for k := range s.segs {
		lo, hi := max(a, s.bNs[k]), min(b, s.bNs[k+1])
		if hi > lo {
			sum += float64(hi-lo) / 1e9 * (s.rateIn(k, lo) + s.rateIn(k, hi)) / 2
		}
	}
	return sum
}

func (s schedule) cumulative(ns int64) float64 { return s.integral(0, ns) }

func (s schedule) window(ns int64) float64 { return s.integral(ns-int64(Window), ns) }

// limit is L(t). W is piecewise quadratic, so its minimum over [t, t+1s] is at
// a span end, a breakpoint b or b+1s inside the span, or where W' = rate(s) -
// rate(s-1s) (linear on each sub-piece) crosses zero. Only those are evaluated.
func (s schedule) limit(t int64) int {
	hi := t + int64(Window)
	cuts := []int64{t, hi}
	for _, b := range s.bNs {
		for _, c := range [2]int64{b, b + int64(Window)} {
			if c > t && c < hi {
				cuts = append(cuts, c)
			}
		}
	}
	sort.Slice(cuts, func(i, j int) bool { return cuts[i] < cuts[j] })

	mn := math.Inf(1)
	consider := func(x int64) { mn = math.Min(mn, s.window(x)) }
	for i, c := range cuts {
		consider(c)
		if i == 0 || c-cuts[i-1] < 3 {
			continue
		}
		u, m := cuts[i-1], cuts[i-1]+(c-cuts[i-1])/2
		f := s.rateAt(m) - s.rateAt(m-int64(Window))
		g := s.slopeAt(m) - s.slopeAt(m-int64(Window))
		if g != 0 {
			if r := float64(m) - f/g*1e9; r > float64(u) && r < float64(c) {
				consider(int64(r))
			}
		}
	}
	return int(math.Floor(mn + scheduleEpsilon))
}

// ScheduleLimiter enforces a schedule; one per process, shared by every VU.
type ScheduleLimiter struct {
	mu       sync.Mutex
	sched    schedule
	t0       time.Time   // the instant the schedule starts, shared by every process of a run
	ring     []time.Time // grants in the last second, oldest first
	trace    []int64
	traceCap int
	turn     chan struct{} // same fairness token as SlidingWindowLimiter

	// Per-stage delivery, excluding the VU warm-up at the start of the run.
	warmup       time.Duration
	stageGranted []int
	stageOut     io.Writer

	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

// NewScheduleLimiter builds a limiter for segs anchored at t0.
func NewScheduleLimiter(segs []Segment, t0 time.Time) (*ScheduleLimiter, error) {
	if len(segs) == 0 {
		return nil, fmt.Errorf("throughput: schedule has no segments")
	}
	s := newSchedule(segs)
	if s.peak < 1 {
		return nil, fmt.Errorf("throughput: schedule peak must be >= 1, got %d", s.peak)
	}
	return &ScheduleLimiter{
		sched:        s,
		t0:           t0,
		ring:         make([]time.Time, 0, s.peak+1),
		turn:         make(chan struct{}, 1),
		traceCap:     defaultTraceCap,
		stageGranted: make([]int, len(segs)),
		stageOut:     os.Stderr,
	}, nil
}

func (l *ScheduleLimiter) Scheduled() bool { return true }

// Rate is max(1, L(now)); log-only in schedule mode.
func (l *ScheduleLimiter) Rate() int {
	return max(1, l.sched.limit(l.timeNow().Sub(l.t0).Nanoseconds()))
}

func (l *ScheduleLimiter) Acquire(ctx context.Context) error { return l.AcquireN(ctx, 1) }

// AcquireN takes n grants, possibly across several instants. Batch callers
// must use AcquireUpTo so each wire slice matches one atomic grant.
func (l *ScheduleLimiter) AcquireN(ctx context.Context, n int) error {
	for n > 0 {
		g, err := l.AcquireUpTo(ctx, n)
		if err != nil {
			return err
		}
		n -= g
	}
	return nil
}

// AcquireUpTo blocks until at least one grant is free, then takes
// g = min(n, L(t) - count) grants at one instant and returns g.
func (l *ScheduleLimiter) AcquireUpTo(ctx context.Context, n int) (int, error) {
	if n < 1 {
		return 0, nil
	}
	select {
	case l.turn <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	defer func() { <-l.turn }()
	for {
		g, wait := l.tryGrantUpTo(n)
		if g > 0 {
			return g, nil
		}
		if err := l.doSleep(ctx, wait); err != nil {
			return 0, err
		}
	}
}

// tryGrantUpTo grants min(n, free) at now, or returns 0 and how long to wait.
func (l *ScheduleLimiter) tryGrantUpTo(n int) (int, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.timeNow()
	cutoff := now.Add(-Window)
	i := 0
	for i < len(l.ring) && !l.ring[i].After(cutoff) {
		i++
	}
	l.ring = l.ring[i:]

	if free := l.sched.limit(now.Sub(l.t0).Nanoseconds()) - len(l.ring); free >= 1 {
		g := min(n, free)
		for j := 0; j < g; j++ {
			l.record(now)
		}
		return g, 0
	}
	wait := scheduleMaxWait
	if len(l.ring) > 0 {
		wait = min(wait, l.ring[0].Add(Window).Sub(now))
	}
	return 0, wait
}

// record appends a grant; caller holds the lock.
func (l *ScheduleLimiter) record(t time.Time) {
	l.ring = append(l.ring, t)
	if l.trace != nil && len(l.trace) < l.traceCap {
		l.trace = append(l.trace, t.UnixNano())
	}
	if s := t.Sub(l.t0); s >= l.warmup {
		if k := l.sched.seg(s.Nanoseconds()); k >= 0 {
			l.stageGranted[k]++
		}
	}
}

// logStage writes segment k's delivery line: budget is A over the segment after
// the warm-up, and a hold below 95% is flagged.
func (l *ScheduleLimiter) logStage(k int) {
	l.mu.Lock()
	granted := l.stageGranted[k]
	l.mu.Unlock()

	sg := l.sched.segs[k]
	budget := l.sched.integral(max(l.sched.bNs[k], l.warmup.Nanoseconds()), l.sched.bNs[k+1])
	ratio := 1.0
	if budget > 0 {
		ratio = float64(granted) / budget
	}
	kind := "ramp"
	if sg.FromRps == sg.ToRps {
		kind = "hold"
	}
	line := fmt.Sprintf("[xk6-load-toolkit] throughput stage=%d kind=%s budget=%.1f granted=%d ratio=%.3f", k+1, kind, budget, granted, ratio)
	if kind == "hold" && ratio < 0.95 {
		line += " UNDER-TARGET: VU pool too small (maxVus) or system under test not keeping up"
	}
	fmt.Fprintln(l.stageOut, line)
}

// startStageLog logs each segment as its end passes on the local clock. The
// last segment's timer races k6's exit at the end of the run; the line is a
// diagnostic, not a delivery guarantee.
func (l *ScheduleLimiter) startStageLog() {
	for k := range l.sched.segs {
		time.AfterFunc(time.Until(l.t0.Add(time.Duration(l.sched.bNs[k+1]))), func() { l.logStage(k) })
	}
}

func (l *ScheduleLimiter) EnableTrace() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.trace == nil {
		l.trace = make([]int64, 0, 1024)
	}
}

func (l *ScheduleLimiter) Trace() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]int64, len(l.trace))
	copy(out, l.trace)
	return out
}

func (l *ScheduleLimiter) timeNow() time.Time {
	if l.now != nil {
		return l.now()
	}
	return time.Now()
}

func (l *ScheduleLimiter) doSleep(ctx context.Context, d time.Duration) error {
	if l.sleep != nil {
		return l.sleep(ctx, d)
	}
	return sleepCtx(ctx, d)
}
