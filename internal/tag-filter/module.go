package tagfilter

import (
	"sync"

	"go.k6.io/k6/v2/js/modules"
)

// ModuleName is the JS import path of the tag filter.
const ModuleName = "k6/x/load-toolkit/tagfilter"

// RootModule parses the rules once per process and shares them across VUs.
type RootModule struct {
	load   func() (*Config, error)
	once   sync.Once
	filter *Filter
}

type moduleInstance struct {
	vu     modules.VU
	filter *Filter
}

// NewModule returns the root module. load supplies the tagFilter section when
// the first VU is created; an error from it gives a no-op filter.
func NewModule(load func() (*Config, error)) *RootModule { return &RootModule{load: load} }

// NewModuleInstance implements modules.Module.
func (r *RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	r.once.Do(func() {
		// A broken configuration fails the run through the sink; here it is a no-op filter.
		rules, err := r.load()
		if err != nil {
			rules = nil
		}
		r.filter = FromConfig(rules)
	})
	return &moduleInstance{vu: vu, filter: r.filter}
}

// Exports implements modules.Instance. Filtering happens in JS: a skipped
// request never reaches k6's transport, so it emits no metric sample.
func (m *moduleInstance) Exports() modules.Exports {
	return modules.Exports{
		Default: map[string]any{
			"_mode":              m.mode,
			"_shouldRunRequest":  m.shouldRunRequest,
			"_shouldRunGroup":    m.shouldRunGroup,
			"_shouldRunScenario": m.shouldRunScenario,
		},
	}
}

func (m *moduleInstance) mode() string {
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
func (m *moduleInstance) shouldRunRequest(testTags []string) bool {
	groupName, scenarioName := m.currentContext()
	return m.filter.ShouldRun(testTags, groupName, scenarioName)
}

// shouldRunGroup is called before group() is entered, so the candidate name is passed in.
func (m *moduleInstance) shouldRunGroup(name string) bool {
	_, scenarioName := m.currentContext()
	return m.filter.ShouldRun(nil, name, scenarioName)
}

func (m *moduleInstance) shouldRunScenario(name string) bool {
	return m.filter.ShouldRun(nil, "", name)
}

func (m *moduleInstance) currentContext() (string, string) {
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
