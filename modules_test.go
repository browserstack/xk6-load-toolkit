package loadtoolkit

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The rate comes only from configuration: absent means off, fractional shares truncate.
func TestLimiterFromConfig(t *testing.T) {
	if l, err := limiterFromConfig(nil); l != nil || err != nil {
		t.Fatalf("absent section: l=%v err=%v", l, err)
	}
	for raw, want := range map[float64]int{50: 50, 1: 1, 1.9: 1} {
		l, err := limiterFromConfig(&ThroughputConfig{PodRPS: raw})
		if err != nil || l.Rate() != want || l.Scheduled() {
			t.Fatalf("podRps %v: l=%v err=%v, want flat rate %d", raw, l, err, want)
		}
		if l.(*SlidingWindowLimiter).traceCap != defaultTraceCap {
			t.Fatal("traceCap default not applied")
		}
	}
	l, err := limiterFromConfig(&ThroughputConfig{PodRPS: 5, TraceCap: 7})
	if err != nil || l.(*SlidingWindowLimiter).traceCap != 7 {
		t.Fatalf("traceCap override: %v %v", l, err)
	}
}

// A schedule section selects the schedule limiter, and every invariant is an
// error: a malformed schedule must never run unthrottled or at a guessed rate.
func TestScheduleFromConfig(t *testing.T) {
	nowMs := time.Now().UnixMilli()
	ms := func(d time.Duration) string { return strconv.FormatInt(nowMs+d.Milliseconds(), 10) }
	good := ScheduleConfig{
		Stages:      `[{"fromRps":0,"toRps":50,"durationSec":60},{"fromRps":50,"toRps":50,"durationSec":120}]`,
		PeakRps:     "50",
		DurationSec: "180",
		T0Ms:        ms(-2 * time.Second),
	}
	with := func(edit func(*ScheduleConfig)) *ScheduleConfig {
		c := good
		edit(&c)
		return &c
	}
	cases := []struct {
		name    string
		cfg     *ThroughputConfig
		wantErr string
	}{
		{name: "valid schedule", cfg: &ThroughputConfig{Schedule: with(func(*ScheduleConfig) {})}},
		{name: "t0 slightly in the future is fine", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.T0Ms = ms(30 * time.Second) })}},
		{name: "schedule without stages", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.Stages = "" })}, wantErr: "requires stages"},
		{name: "schedule and podRps together", cfg: &ThroughputConfig{PodRPS: 50, Schedule: with(func(*ScheduleConfig) {})}, wantErr: "podRps"},
		{name: "malformed json", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.Stages = `[{"fromRps":0,` })}, wantErr: "JSON"},
		{name: "fractional value", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) {
			c.Stages, c.PeakRps = `[{"fromRps":0,"toRps":1.5,"durationSec":180}]`, "1"
		})}, wantErr: "JSON"},
		{name: "unknown key", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.Stages = `[{"fromRps":0,"toRps":50,"durationSec":180,"x":1}]` })}, wantErr: "JSON"},
		{name: "empty array", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.Stages = `[]` })}, wantErr: "1..30"},
		{name: "31 segments", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) {
			c.Stages = "[" + strings.TrimSuffix(strings.Repeat(`{"fromRps":1,"toRps":1,"durationSec":1},`, 31), ",") + "]"
		})}, wantErr: "1..30"},
		{name: "negative rps", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.Stages = `[{"fromRps":-1,"toRps":50,"durationSec":180}]` })}, wantErr: ">= 0"},
		{name: "segment over the 24h bound", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) {
			c.Stages, c.DurationSec = `[{"fromRps":0,"toRps":50,"durationSec":86401}]`, "86401"
		})}, wantErr: "durationSec must be 1.."},
		{name: "total over the 24h bound", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) {
			c.Stages, c.DurationSec = `[{"fromRps":0,"toRps":50,"durationSec":86400},{"fromRps":50,"toRps":50,"durationSec":1}]`, "86401"
		})}, wantErr: "exceeds"},
		{name: "zero duration", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) {
			c.Stages = `[{"fromRps":0,"toRps":50,"durationSec":0},{"fromRps":50,"toRps":50,"durationSec":180}]`
		})}, wantErr: "durationSec"},
		{name: "discontinuous", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) {
			c.Stages = `[{"fromRps":0,"toRps":50,"durationSec":60},{"fromRps":40,"toRps":50,"durationSec":120}]`
		})}, wantErr: "continuous"},
		{name: "duration mismatch", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.DurationSec = "181" })}, wantErr: "durationSec="},
		{name: "duration not an integer", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.DurationSec = "180.0" })}, wantErr: "durationSec="},
		{name: "peak mismatch", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.PeakRps = "49" })}, wantErr: "peakRps="},
		{name: "oversize", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.Stages = "[" + strings.Repeat(" ", 4096) + "]" })}, wantErr: "4096"},
		{name: "t0 missing", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.T0Ms = "" })}, wantErr: "t0Ms"},
		{name: "t0 not a number", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.T0Ms = "soon" })}, wantErr: "t0Ms"},
		{name: "t0 older than the schedule", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.T0Ms = ms(-181 * time.Second) })}, wantErr: "t0Ms"},
		{name: "t0 over 60s ahead", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) { c.T0Ms = ms(61 * time.Second) })}, wantErr: "t0Ms"},
		{name: "all-zero schedule", cfg: &ThroughputConfig{Schedule: with(func(c *ScheduleConfig) {
			c.Stages, c.PeakRps = `[{"fromRps":0,"toRps":0,"durationSec":180}]`, "0"
		})}, wantErr: "peak"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, err := limiterFromConfig(tc.cfg)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one mentioning %q", err, tc.wantErr)
				}
				if l != nil {
					t.Fatal("limiter returned alongside an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if l == nil || !l.Scheduled() {
				t.Fatalf("want a schedule limiter, got %v", l)
			}
		})
	}
}

// The warm-up only shifts the stage log; a non-positive value means none.
func TestScheduleWarmup(t *testing.T) {
	base := ScheduleConfig{
		Stages: `[{"fromRps":1,"toRps":1,"durationSec":60}]`, PeakRps: "1", DurationSec: "60",
		T0Ms: strconv.FormatInt(time.Now().UnixMilli(), 10),
	}
	for warm, want := range map[float64]time.Duration{0: 0, -3: 0, 2.5: 2500 * time.Millisecond} {
		sc := base
		sc.WarmupSec = warm
		l, err := scheduleFromConfig(&sc)
		if err != nil || l.warmup != want {
			t.Fatalf("warmupSec %v: warmup=%v err=%v, want %v", warm, l.warmup, err, want)
		}
	}
}

// The flat path never yields a schedule limiter, and AcquireUpTo on it is
// AcquireN capped at the rate.
func TestFlatLimiterInterface(t *testing.T) {
	l, err := limiterFromConfig(&ThroughputConfig{PodRPS: 3})
	if err != nil || l == nil || l.Scheduled() {
		t.Fatalf("flat: l=%v err=%v", l, err)
	}
	if g, err := l.AcquireUpTo(context.Background(), 10); err != nil || g != 3 {
		t.Fatalf("AcquireUpTo(10) at rate 3 = %d, %v", g, err)
	}
}

// Schedule traces start with the anchor so a replay can rebuild W(t); flat
// traces have no header.
func TestWriteTraceHeader(t *testing.T) {
	dir := t.TempDir()
	flat, sched := dir+"/flat", dir+"/sched"
	if err := writeTrace(flat, "", []int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := writeTrace(sched, "#t0 99\n", []int64{1}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(flat); string(b) != "1\n2\n" {
		t.Errorf("flat trace = %q", b)
	}
	if b, _ := os.ReadFile(sched); string(b) != "#t0 99\n1\n" {
		t.Errorf("schedule trace = %q", b)
	}
	sl, err := NewScheduleLimiter([]Segment{{FromRps: 1, ToRps: 1, DurationSec: 1}}, time.Unix(0, 42))
	if err != nil || traceHeader(sl) != "#t0 42\n" {
		t.Fatalf("schedule header = %q (%v)", traceHeader(sl), err)
	}
	fl, _ := NewSlidingWindowLimiter(1)
	if traceHeader(fl) != "" {
		t.Fatal("flat limiter must have no trace header")
	}
}

// The schedule section is part of the strict configuration schema.
func TestParseConfig_ScheduleSection(t *testing.T) {
	cfg, err := ParseConfig([]byte(`{"version":1,"throughput":{"schedule":{"stages":"[]","durationSec":"1","peakRps":"1","t0Ms":"1","warmupSec":2}}}`))
	if err != nil || cfg.Throughput.Schedule == nil || cfg.Throughput.Schedule.WarmupSec != 2 {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
	if _, err := ParseConfig([]byte(`{"version":1,"throughput":{"schedule":{"stage":"[]"}}}`)); err == nil {
		t.Fatal("an unknown schedule field must be rejected")
	}
	if _, err := ParseConfig([]byte(`{"version":1,"throughput":{"podRps":0}}`)); err == nil {
		t.Fatal("a flat section still needs podRps >= 1")
	}
}
