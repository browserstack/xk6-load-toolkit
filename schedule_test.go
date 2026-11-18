package loadtoolkit

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// fixtureFile holds exact-rational reference numbers from
// testdata/gen_schedule_fixtures.py, so other implementations can be pinned to them.
type fixtureFile struct {
	Epsilon float64 `json:"epsilon"`
	Cases   []struct {
		Name     string    `json:"name"`
		Segments []Segment `json:"segments"`
		TotalSec int       `json:"totalSec"`
		Peak     int       `json:"peak"`
		Points   []struct {
			T    float64 `json:"t"`
			Rate float64 `json:"rate"`
			A    float64 `json:"A"`
			W    float64 `json:"W"`
			L    int     `json:"L"`
		} `json:"points"`
	} `json:"cases"`
}

func loadFixtures(t *testing.T) fixtureFile {
	t.Helper()
	raw, err := os.ReadFile("testdata/schedule_fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var f fixtureFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Cases) != 12 {
		t.Fatalf("want 12 fixture schedules, got %d", len(f.Cases))
	}
	return f
}

func near(a, b float64) bool { return math.Abs(a-b) <= 1e-9*math.Max(1, math.Abs(b)) }

func secNs(s float64) int64 { return int64(math.Round(s * 1e9)) }

// T2: rate, A, W and L agree with the reference for every fixture point.
func TestScheduleMathMatchesFixtures(t *testing.T) {
	f := loadFixtures(t)
	if f.Epsilon != scheduleEpsilon {
		t.Fatalf("fixture epsilon %v != scheduleEpsilon %v", f.Epsilon, scheduleEpsilon)
	}
	for _, c := range f.Cases {
		s := newSchedule(c.Segments)
		if s.totalNs != int64(c.TotalSec)*1e9 || s.peak != c.Peak {
			t.Errorf("%s: total/peak = %d/%d, want %ds/%d", c.Name, s.totalNs, s.peak, c.TotalSec, c.Peak)
		}
		for _, p := range c.Points {
			ns := secNs(p.T)
			if got := s.rateAt(ns); !near(got, p.Rate) {
				t.Errorf("%s t=%v: rate %v want %v", c.Name, p.T, got, p.Rate)
			}
			if got := s.cumulative(ns); !near(got, p.A) {
				t.Errorf("%s t=%v: A %v want %v", c.Name, p.T, got, p.A)
			}
			if got := s.window(ns); !near(got, p.W) {
				t.Errorf("%s t=%v: W %v want %v", c.Name, p.T, got, p.W)
			}
			if got := s.limit(ns); got != p.L {
				t.Errorf("%s t=%v: L %d want %d", c.Name, p.T, got, p.L)
			}
		}
	}
}

// fakeClock drives a limiter deterministically: sleep advances the clock.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }
func (c *fakeClock) sleep(_ context.Context, d time.Duration) error {
	c.t = c.t.Add(d)
	return nil
}

var testT0 = time.Unix(1_700_000_000, 0)

func newTestScheduleLimiter(t *testing.T, segs []Segment, clk *fakeClock) *ScheduleLimiter {
	t.Helper()
	l, err := NewScheduleLimiter(segs, testT0)
	if err != nil {
		t.Fatal(err)
	}
	l.now, l.sleep = clk.now, clk.sleep
	l.stageOut = &bytes.Buffer{}
	return l
}

// greedy hammers tryGrantUpTo every step from t0-2s to T+2s: the worst case a
// pod with unlimited VUs can produce. Returns every grant timestamp.
func greedy(l *ScheduleLimiter, clk *fakeClock, step func() time.Duration, n func() int) []time.Time {
	var grants []time.Time
	end := testT0.Add(time.Duration(l.sched.totalNs) + 2*time.Second)
	for clk.t = testT0.Add(-2 * time.Second); clk.t.Before(end); clk.t = clk.t.Add(step()) {
		for {
			g, _ := l.tryGrantUpTo(n())
			if g == 0 {
				break
			}
			for i := 0; i < g; i++ {
				grants = append(grants, clk.t)
			}
		}
	}
	return grants
}

// windowCounts returns count(g-1s, g] for every grant g (sorts in place).
func windowCounts(grants []time.Time) []int {
	sort.Slice(grants, func(i, j int) bool { return grants[i].Before(grants[j]) })
	out := make([]int, len(grants))
	start := 0
	for end := range grants {
		for !grants[start].After(grants[end].Add(-Window)) {
			start++
		}
		// grants at the same instant as `end` that follow it also count.
		last := end
		for last+1 < len(grants) && grants[last+1].Equal(grants[end]) {
			last++
		}
		out[end] = last - start + 1
	}
	return out
}

// checkContract asserts count(t-1s, t] <= floor(W(t) + slack) for EVERY t, not
// just at grant instants (where even the naive "W(now)" rule holds by
// construction — falling segments breach between grants). Grants land on a
// 1ms grid, so count is constant on [k, k+1ms): both ends of each are checked.
func checkContract(t *testing.T, label string, grants []time.Time, s schedule, slack float64) {
	t.Helper()
	sort.Slice(grants, func(i, j int) bool { return grants[i].Before(grants[j]) })
	lo, hi := 0, 0
	for k := -2000 * time.Millisecond; k < time.Duration(s.totalNs)+2*time.Second; k += time.Millisecond {
		at := testT0.Add(k)
		for hi < len(grants) && !grants[hi].After(at) {
			hi++
		}
		for lo < hi && !grants[lo].After(at.Add(-Window)) {
			lo++
		}
		n := hi - lo
		for _, ns := range [2]int64{k.Nanoseconds(), (k + time.Millisecond).Nanoseconds() - 1} {
			if lim := int(math.Floor(s.window(ns) + slack)); n > lim {
				t.Errorf("%s: %d grants in the window ending %v, budget floor(W)=%d", label, n, time.Duration(ns), lim)
				return
			}
		}
	}
}

// T1: the ceiling contract on every fixture schedule, under adversarial (1ms greedy)
// and random (jittered step, random batch size) grant timing.
func TestScheduleContractHolds(t *testing.T) {
	f := loadFixtures(t)
	rng := rand.New(rand.NewSource(5376))
	drivers := map[string][2]func() int{
		"greedy-1ms":  {func() int { return 1 }, func() int { return 1 }},
		"random":      {func() int { return 1 + rng.Intn(7) }, func() int { return 1 + rng.Intn(5) }},
		"big-batches": {func() int { return 3 }, func() int { return 50 }},
	}
	for _, c := range f.Cases {
		for name, d := range drivers {
			clk := &fakeClock{}
			l := newTestScheduleLimiter(t, c.Segments, clk)
			stepMs := d[0]
			grants := greedy(l, clk, func() time.Duration { return time.Duration(stepMs()) * time.Millisecond }, d[1])
			checkContract(t, c.Name+"/"+name, grants, l.sched, scheduleEpsilon)
			if len(grants) == 0 {
				t.Errorf("%s/%s: no grants at all", c.Name, name)
			}
		}
	}
}

// Delivery is near the budget on a hold: the rule is conservative only on
// falling segments, so a greedy pod must take ~A(T) on a hold schedule.
func TestScheduleDeliversHold(t *testing.T) {
	clk := &fakeClock{}
	l := newTestScheduleLimiter(t, []Segment{{50, 50, 60}}, clk)
	grants := greedy(l, clk, func() time.Duration { return time.Millisecond }, func() int { return 1 })
	if got, want := len(grants), 50*60; got < want-50 || got > want {
		t.Errorf("hold 50x60s granted %d, want within [%d, %d]", got, want-50, want)
	}
}

// T3: on a hold the schedule limiter is the flat sliding window: equal grant counts
// in every one-second window once the first second has passed.
func TestScheduleHoldEquivalentToFlat(t *testing.T) {
	for _, r := range []int{7, 10, 50} {
		clk := &fakeClock{}
		sl := newTestScheduleLimiter(t, []Segment{{r, r, 30}}, clk)
		fl, _ := NewSlidingWindowLimiter(r)
		fclk := &fakeClock{}
		fl.now, fl.sleep = fclk.now, fclk.sleep

		var sg, fg []time.Time
		for ms := 0; ms < 30_000; ms++ {
			ts := testT0.Add(time.Duration(ms) * time.Millisecond)
			clk.t, fclk.t = ts, ts
			for {
				if g, _ := sl.tryGrantUpTo(1); g == 0 {
					break
				}
				sg = append(sg, ts)
			}
			for fl.tryGrantN(1) == 0 {
				fg = append(fg, ts)
			}
		}
		count := func(gs []time.Time, from time.Time) int {
			n := 0
			for _, g := range gs {
				if g.After(from) && !g.After(from.Add(Window)) {
					n++
				}
			}
			return n
		}
		// Stop where the one-second lookahead leaves the hold: past T-1s the rule
		// sees W fall to 0 and is (correctly) more conservative than flat.
		for ms := 1000; ms < 28_000; ms += 37 {
			from := testT0.Add(time.Duration(ms) * time.Millisecond)
			if a, b := count(sg, from), count(fg, from); a != b {
				t.Fatalf("r=%d window after %dms: schedule %d vs flat %d", r, ms, a, b)
			}
		}
	}
}

// T4: per-value-sharded pods on a common clock compose: the merged count never
// exceeds the global floor(W + N*eps).
func TestScheduleComposesAcrossPods(t *testing.T) {
	// Global 0 -> 200 (60s) -> hold (30s) -> 7 (20s), sharded over 3 pods by
	// residual-carry with VUs 10/10/10: [0,67,67,2] [0,67,67,3] [0,66,66,2].
	pods := [][]int{{0, 67, 67, 2}, {0, 67, 67, 3}, {0, 66, 66, 2}}
	durs := []int{60, 30, 20}
	global := newSchedule(segsOf([]int{0, 200, 200, 7}, durs))
	var merged []time.Time
	for _, p := range pods {
		clk := &fakeClock{}
		l := newTestScheduleLimiter(t, segsOf(p, durs), clk)
		merged = append(merged, greedy(l, clk, func() time.Duration { return time.Millisecond }, func() int { return 1 })...)
	}
	checkContract(t, "merged", merged, global, 3*scheduleEpsilon)
}

func segsOf(vals, durs []int) []Segment {
	out := make([]Segment, len(durs))
	for i, d := range durs {
		out[i] = Segment{FromRps: vals[i], ToRps: vals[i+1], DurationSec: d}
	}
	return out
}

// T5: a batch of 50 at a budget of 7 goes out in slices no larger than the free
// budget at the instant they are granted — never as one burst.
func TestAcquireUpToSlicesBatchToBudget(t *testing.T) {
	clk := &fakeClock{t: testT0.Add(2 * time.Second)}
	l := newTestScheduleLimiter(t, []Segment{{7, 7, 60}}, clk)
	l.EnableTrace()
	total := 0
	for total < 50 {
		g, err := l.AcquireUpTo(context.Background(), 50-total)
		if err != nil {
			t.Fatal(err)
		}
		if g < 1 || g > 7 {
			t.Fatalf("slice of %d, want 1..7", g)
		}
		total += g
	}
	if total != 50 {
		t.Fatalf("granted %d, want 50", total)
	}
	var ts []time.Time
	for _, ns := range l.Trace() {
		ts = append(ts, time.Unix(0, ns))
	}
	for _, n := range windowCounts(ts) {
		if n > 7 {
			t.Fatalf("%d grants in one second at budget 7", n)
		}
	}
}

// AcquireN takes all n grants (single-request callers use n == 1).
func TestScheduleAcquireN(t *testing.T) {
	clk := &fakeClock{t: testT0.Add(2 * time.Second)}
	l := newTestScheduleLimiter(t, []Segment{{5, 5, 60}}, clk)
	l.EnableTrace()
	if err := l.AcquireN(context.Background(), 12); err != nil {
		t.Fatal(err)
	}
	if got := len(l.Trace()); got != 12 {
		t.Fatalf("granted %d, want 12", got)
	}
	if !l.Scheduled() {
		t.Fatal("Scheduled() = false")
	}
	if got := l.Rate(); got != 5 {
		t.Fatalf("Rate() = %d, want 5", got)
	}
}

// Rate is never below 1 (log-only), even where L is 0.
func TestScheduleRateFloorsAtOne(t *testing.T) {
	clk := &fakeClock{t: testT0}
	l := newTestScheduleLimiter(t, []Segment{{0, 10, 60}}, clk)
	if got := l.Rate(); got != 1 {
		t.Fatalf("Rate() at L=0 = %d, want 1", got)
	}
}

// E2: at L = 0 a parked VU wakes on cancellation instead of hanging the run.
func TestScheduleAcquireUnblocksOnCancellation(t *testing.T) {
	l, err := NewScheduleLimiter([]Segment{{5, 0, 1}}, time.Now().Add(-10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Acquire(ctx) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a cancellation error, got a grant")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire did not wake on cancellation")
	}
}

// The wait is capped at 10ms so a growing budget is noticed promptly.
func TestScheduleWaitIsCapped(t *testing.T) {
	clk := &fakeClock{t: testT0}
	l := newTestScheduleLimiter(t, []Segment{{0, 10, 60}}, clk)
	if g, wait := l.tryGrantUpTo(1); g != 0 || wait <= 0 || wait > scheduleMaxWait {
		t.Fatalf("at L=0: g=%d wait=%v, want 0 and (0, %v]", g, wait, scheduleMaxWait)
	}
}

func TestNewScheduleLimiterRejectsZeroPeak(t *testing.T) {
	if _, err := NewScheduleLimiter([]Segment{{0, 0, 10}}, testT0); err == nil {
		t.Fatal("want an error for peak 0")
	}
	if _, err := NewScheduleLimiter(nil, testT0); err == nil {
		t.Fatal("want an error for no segments")
	}
}

// RN-4: one delivery line per segment; holds under 95% carry UNDER-TARGET, and
// the warm-up is excluded from both budget and grants.
func TestStageLog(t *testing.T) {
	clk := &fakeClock{}
	l := newTestScheduleLimiter(t, []Segment{{0, 10, 10}, {10, 10, 10}}, clk)
	l.warmup = 2 * time.Second
	out := &bytes.Buffer{}
	l.stageOut = out
	// Deliver nothing until 15s, then greedily: the hold gets ~half its budget.
	for clk.t = testT0.Add(15 * time.Second); clk.t.Before(testT0.Add(20 * time.Second)); clk.t = clk.t.Add(time.Millisecond) {
		for {
			if g, _ := l.tryGrantUpTo(1); g == 0 {
				break
			}
		}
	}
	l.logStage(0)
	l.logStage(1)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %q", out.String())
	}
	// Ramp 0->10 over 10s is 50 requests; the 2s warm-up holds 2 of them: 48.
	if !strings.Contains(lines[0], "[xk6-load-toolkit] throughput stage=1 kind=ramp budget=48.0 granted=0") || strings.Contains(lines[0], "UNDER-TARGET") {
		t.Errorf("ramp line = %q", lines[0])
	}
	if !strings.Contains(lines[1], "stage=2 kind=hold budget=100.0") || !strings.Contains(lines[1], "UNDER-TARGET") ||
		!strings.Contains(lines[1], "maxVus") {
		t.Errorf("hold line = %q", lines[1])
	}
}
