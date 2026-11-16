package loadtoolkit

import (
	"bytes"
	"os"
	"strconv"
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

// ThroughputRoot is one per process; its limiter is shared by every VU, which
// is what makes the rate ceiling global to the process.
type ThroughputRoot struct {
	once    sync.Once
	limiter *SlidingWindowLimiter
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
		r.limiter, r.traceTo, r.err = limiterFromConfig(loadedConfig.Throughput)
	})
	return &throughputInstance{vu: vu, root: r}
}

// limiterFromConfig returns nil when the throughput section is absent.
func limiterFromConfig(cfg *ThroughputConfig) (*SlidingWindowLimiter, string, error) {
	if cfg == nil {
		return nil, "", nil
	}
	l, err := NewSlidingWindowLimiter(int(cfg.PodRPS))
	if err != nil {
		return nil, "", err
	}
	if cfg.TraceCap > 0 {
		l.traceCap = cfg.TraceCap
	}
	if cfg.TraceFile != "" {
		l.EnableTrace()
		// Flushed periodically: an aborted run never reaches teardown.
		go func() {
			for range time.Tick(traceFlushInterval) {
				_ = writeTrace(cfg.TraceFile, l.Trace())
			}
		}()
	}
	return l, cfg.TraceFile, nil
}

const traceFlushInterval = 2 * time.Second

func writeTrace(path string, grants []int64) error {
	var buf bytes.Buffer
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
			"_enabled":   m.enabled,
			"_rate":      m.rate,
			"_acquire":   m.acquire,
			"_acquireN":  m.acquireN,
			"_dumpTrace": m.dumpTrace,
		},
	}
}

func (m *throughputInstance) dumpTrace() int {
	if m.root.limiter == nil || m.root.traceTo == "" {
		return 0
	}
	grants := m.root.limiter.Trace()
	if err := writeTrace(m.root.traceTo, grants); err != nil {
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
