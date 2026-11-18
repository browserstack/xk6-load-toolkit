package loadtoolkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.k6.io/k6/v2/js/common"
	"go.k6.io/k6/v2/js/modules"
)

const (
	// ThroughputModule is the JS import path of the request-rate limiter.
	ThroughputModule = "k6/x/load-toolkit/throughput"
	// TagFilterModule is the JS import path of the tag filter.
	TagFilterModule = "k6/x/load-toolkit/tagfilter"
)

// limiter is what the JS module needs from the flat window and the schedule.
type limiter interface {
	Acquire(context.Context) error
	AcquireN(context.Context, int) error
	AcquireUpTo(context.Context, int) (int, error)
	Rate() int
	Scheduled() bool
	EnableTrace()
	Trace() []int64
}

// ThroughputRoot is one per process; its limiter is shared by every VU, which
// is what makes the rate ceiling global to the process.
type ThroughputRoot struct {
	once    sync.Once
	limiter limiter
	traceTo string
	err     error
}

type throughputInstance struct {
	vu   modules.VU
	root *ThroughputRoot
}

// NewThroughput returns the root module.
func NewThroughput() *ThroughputRoot { return &ThroughputRoot{} }

// NewModuleInstance implements modules.Module.
func (r *ThroughputRoot) NewModuleInstance(vu modules.VU) modules.Instance {
	r.once.Do(func() {
		if configErr != nil {
			r.err = configErr
			return
		}
		r.limiter, r.err = limiterFromConfig(loadedConfig.Throughput)
		if r.err == nil && loadedConfig.Throughput != nil {
			r.traceTo = loadedConfig.Throughput.TraceFile
		}
	})
	return &throughputInstance{vu: vu, root: r}
}

// Schedule bounds.
const (
	maxStagesBytes = 4096
	maxSegments    = 30
	maxT0Lead      = 60 * time.Second
	// maxScheduleSec bounds every duration before the seconds->nanoseconds
	// conversion (int64 ns overflows at ~292 years); it is a parse backstop.
	maxScheduleSec = 24 * 60 * 60
)

// limiterFromConfig returns nil when the throughput section is absent. Every
// invariant failure is an error, never a default: _enabled() then throws and
// the run fails instead of running unthrottled or at a guessed rate.
func limiterFromConfig(cfg *ThroughputConfig) (limiter, error) {
	if cfg == nil {
		return nil, nil
	}
	if cfg.Schedule != nil && cfg.PodRPS != 0 {
		return nil, fmt.Errorf("throughput: schedule stages are set but podRps=%v is set too", cfg.PodRPS)
	}
	var (
		l      limiter
		header string
	)
	if cfg.Schedule != nil {
		sl, err := scheduleFromConfig(cfg.Schedule)
		if err != nil {
			return nil, err
		}
		if cfg.TraceCap > 0 {
			sl.traceCap = cfg.TraceCap
		}
		sl.startStageLog()
		l, header = sl, traceHeader(sl)
	} else {
		fl, err := NewSlidingWindowLimiter(int(cfg.PodRPS))
		if err != nil {
			return nil, err
		}
		if cfg.TraceCap > 0 {
			fl.traceCap = cfg.TraceCap
		}
		l = fl
	}
	if cfg.TraceFile != "" {
		l.EnableTrace()
		// Flushed periodically: an aborted run never reaches teardown.
		go func() {
			for range time.Tick(traceFlushInterval) {
				_ = writeTrace(cfg.TraceFile, header, l.Trace())
			}
		}()
	}
	return l, nil
}

// scheduleFromConfig parses and validates a schedule section.
func scheduleFromConfig(sc *ScheduleConfig) (*ScheduleLimiter, error) {
	raw := sc.Stages
	if raw == "" {
		return nil, errors.New("throughput: schedule requires stages")
	}
	if len(raw) > maxStagesBytes {
		return nil, fmt.Errorf("throughput: schedule stages are %d bytes, max %d", len(raw), maxStagesBytes)
	}
	var segs []Segment
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&segs); err != nil {
		return nil, fmt.Errorf("throughput: schedule stages are not valid JSON segments: %v", err)
	}
	if len(segs) < 1 || len(segs) > maxSegments {
		return nil, fmt.Errorf("throughput: schedule stages must have 1..%d segments, got %d", maxSegments, len(segs))
	}
	total, peak := 0, 0
	for i, sg := range segs {
		if sg.FromRps < 0 || sg.ToRps < 0 {
			return nil, fmt.Errorf("throughput: schedule segment %d: rps must be >= 0", i+1)
		}
		if sg.DurationSec < 1 || sg.DurationSec > maxScheduleSec {
			return nil, fmt.Errorf("throughput: schedule segment %d: durationSec must be 1..%d", i+1, maxScheduleSec)
		}
		if i > 0 && sg.FromRps != segs[i-1].ToRps {
			return nil, fmt.Errorf("throughput: schedule segment %d is not continuous with segment %d", i+1, i)
		}
		total += sg.DurationSec
		peak = max(peak, sg.FromRps, sg.ToRps)
	}
	if total > maxScheduleSec {
		return nil, fmt.Errorf("throughput: schedule total %ds exceeds %ds", total, maxScheduleSec)
	}
	if want, err := strconv.Atoi(sc.DurationSec); err != nil || want != total {
		return nil, fmt.Errorf("throughput: schedule durationSec=%q does not match the stages total %ds", sc.DurationSec, total)
	}
	if want, err := strconv.Atoi(sc.PeakRps); err != nil || want != peak {
		return nil, fmt.Errorf("throughput: schedule peakRps=%q does not match the stages peak %d", sc.PeakRps, peak)
	}
	t0ms, err := strconv.ParseInt(sc.T0Ms, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("throughput: schedule t0Ms=%q is not epoch milliseconds", sc.T0Ms)
	}
	t0 := time.UnixMilli(t0ms)
	if since := time.Since(t0); since > time.Duration(total)*time.Second || since < -maxT0Lead {
		return nil, fmt.Errorf("throughput: schedule t0Ms=%d is %v from now, outside the %ds schedule", t0ms, since, total)
	}
	l, err := NewScheduleLimiter(segs, t0)
	if err != nil {
		return nil, err
	}
	// Diagnostics only (stage log lines), so a non-positive value means no warm-up.
	if sc.WarmupSec > 0 && !math.IsInf(sc.WarmupSec, 0) {
		l.warmup = time.Duration(sc.WarmupSec * float64(time.Second))
	}
	return l, nil
}

// traceHeader is "#t0 <unixNano>" for a schedule trace, so a replay can rebuild
// W(t) on the same clock; flat traces have none.
func traceHeader(l limiter) string {
	if sl, ok := l.(*ScheduleLimiter); ok {
		return fmt.Sprintf("#t0 %d\n", sl.t0.UnixNano())
	}
	return ""
}

const traceFlushInterval = 2 * time.Second

func writeTrace(path, header string, grants []int64) error {
	var buf bytes.Buffer
	buf.WriteString(header)
	for _, ns := range grants {
		buf.WriteString(strconv.FormatInt(ns, 10))
		buf.WriteByte('\n')
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

// Exports implements modules.Instance.
func (m *throughputInstance) Exports() modules.Exports {
	return modules.Exports{
		Default: map[string]any{
			"_enabled":     m.enabled,
			"_rate":        m.rate,
			"_acquire":     m.acquire,
			"_acquireN":    m.acquireN,
			"_dumpTrace":   m.dumpTrace,
			"_scheduled":   m.scheduled,
			"_acquireUpTo": m.acquireUpTo,
		},
	}
}

func (m *throughputInstance) dumpTrace() int {
	if m.root.limiter == nil || m.root.traceTo == "" {
		return 0
	}
	grants := m.root.limiter.Trace()
	if err := writeTrace(m.root.traceTo, traceHeader(m.root.limiter), grants); err != nil {
		common.Throw(m.vu.Runtime(), err)
	}
	return len(grants)
}

// enabled throws on a broken configuration rather than reporting false, so a
// misconfigured run fails instead of running unthrottled.
func (m *throughputInstance) enabled() bool {
	m.throwIfMisconfigured()
	return m.root.limiter != nil
}

func (m *throughputInstance) rate() int {
	m.throwIfMisconfigured()
	if m.root.limiter == nil {
		return 0
	}
	return m.root.limiter.Rate()
}

func (m *throughputInstance) acquire() { m.acquireN(1) }

// acquireN blocks this VU until n grants are available. Each VU runs on its own
// goroutine, so a waiting VU does not stall the others.
func (m *throughputInstance) acquireN(n int) {
	m.throwIfMisconfigured()
	if m.root.limiter == nil || n < 1 {
		return
	}
	if err := m.root.limiter.AcquireN(m.vu.Context(), n); err != nil {
		// The run is stopping. Throw rather than return, or the caller would send
		// the request against a dead context and record a phantom failure.
		common.Throw(m.vu.Runtime(), err)
	}
}

func (m *throughputInstance) scheduled() bool {
	m.throwIfMisconfigured()
	return m.root.limiter != nil && m.root.limiter.Scheduled()
}

// acquireUpTo takes 1..n grants at one instant and returns how many, so the
// caller sends exactly that slice of a batch.
func (m *throughputInstance) acquireUpTo(n int) int {
	m.throwIfMisconfigured()
	if m.root.limiter == nil || n < 1 {
		return max(n, 0)
	}
	g, err := m.root.limiter.AcquireUpTo(m.vu.Context(), n)
	if err != nil {
		common.Throw(m.vu.Runtime(), err) // same unwind as acquireN
	}
	return g
}

func (m *throughputInstance) throwIfMisconfigured() {
	if m.root.err != nil {
		common.Throw(m.vu.Runtime(), m.root.err)
	}
}

// TagFilterRoot parses the rules once per process and shares them across VUs.
type TagFilterRoot struct {
	once   sync.Once
	filter *Filter
}

type tagFilterInstance struct {
	vu     modules.VU
	filter *Filter
}

// NewTagFilter returns the root module.
func NewTagFilter() *TagFilterRoot { return &TagFilterRoot{} }

// NewModuleInstance implements modules.Module.
func (r *TagFilterRoot) NewModuleInstance(vu modules.VU) modules.Instance {
	r.once.Do(func() {
		// A broken configuration fails the run through the sink; here it is a no-op filter.
		var rules *TagFilterConfig
		if configErr == nil {
			rules = loadedConfig.TagFilter
		}
		r.filter = FilterFromConfig(rules)
	})
	return &tagFilterInstance{vu: vu, filter: r.filter}
}

// Exports implements modules.Instance. Filtering happens in JS: a skipped
// request never reaches k6's transport, so it emits no metric sample.
func (m *tagFilterInstance) Exports() modules.Exports {
	return modules.Exports{
		Default: map[string]any{
			"_mode":              m.mode,
			"_shouldRunRequest":  m.shouldRunRequest,
			"_shouldRunGroup":    m.shouldRunGroup,
			"_shouldRunScenario": m.shouldRunScenario,
		},
	}
}

func (m *tagFilterInstance) mode() string {
	switch m.filter.Mode() {
	case ModeInclude:
		return "include"
	case ModeExclude:
		return "exclude"
	default:
		return "none"
	}
}

// shouldRunRequest evaluates a request's params.tags in the current group and scenario.
func (m *tagFilterInstance) shouldRunRequest(testTags []string) bool {
	groupName, scenarioName := m.currentContext()
	return m.filter.ShouldRun(testTags, groupName, scenarioName)
}

// shouldRunGroup is called before group() is entered, so the candidate name is passed in.
func (m *tagFilterInstance) shouldRunGroup(name string) bool {
	_, scenarioName := m.currentContext()
	return m.filter.ShouldRun(nil, name, scenarioName)
}

func (m *tagFilterInstance) shouldRunScenario(name string) bool {
	return m.filter.ShouldRun(nil, "", name)
}

func (m *tagFilterInstance) currentContext() (string, string) {
	state := m.vu.State()
	if state == nil || state.Tags == nil {
		return "", ""
	}
	current := state.Tags.GetCurrentValues()
	if current.Tags == nil {
		return "", ""
	}
	groupName, _ := current.Tags.Get("group")
	scenarioName, _ := current.Tags.Get("scenario")
	return groupName, scenarioName
}
