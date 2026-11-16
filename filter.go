package loadtoolkit

import (
	"encoding/json"
	"strings"
)

// Mode encodes whether the configured filter is an include-list or exclude-list.
// Only one mode is active per run; include wins when both are configured.
type Mode int

const (
	ModeNone Mode = iota
	ModeInclude
	ModeExclude
)

// k6 joins nested group names with this separator. State.Tags["group"] for a
// request inside group("ABC", () => group("PQR", fn)) is "::ABC::PQR".
const groupSeparator = "::"

// rawTags is the shape of an include/exclude rule string:
//
//	{ "test": [...], "group": [...], "scenario": [...] }
type rawTags struct {
	Test     []string `json:"test"`
	Group    []string `json:"group"`
	Scenario []string `json:"scenario"`
}

// Filter is the per-VU policy that decides whether a request should run.
type Filter struct {
	mode     Mode
	test     map[string]struct{}
	scenario map[string]struct{}
	// group entries are stored as pre-split path segments so each match is a
	// segment-by-segment prefix check (no string-mangling on the hot path).
	group [][]string
}

// FilterFromConfig builds the filter from the tagFilter section. Absent or
// empty rules give ModeNone; include wins when both are set.
func FilterFromConfig(cfg *TagFilterConfig) *Filter {
	if cfg == nil {
		return &Filter{mode: ModeNone}
	}
	if strings.TrimSpace(cfg.Include) != "" {
		return parseFilter(ModeInclude, cfg.Include)
	}
	if strings.TrimSpace(cfg.Exclude) != "" {
		return parseFilter(ModeExclude, cfg.Exclude)
	}
	return &Filter{mode: ModeNone}
}

func parseFilter(mode Mode, raw string) *Filter {
	var rt rawTags
	if err := json.Unmarshal([]byte(raw), &rt); err != nil {
		// Fail open: a malformed rule must not silently filter every request.
		return &Filter{mode: ModeNone}
	}
	f := &Filter{
		mode:     mode,
		test:     toExactSet(rt.Test),
		scenario: toExactSet(rt.Scenario),
		group:    toGroupPaths(rt.Group),
	}
	if !f.hasAnyConstraint() {
		// Mode set but every list empty — equivalent to no filter.
		f.mode = ModeNone
	}
	return f
}

// toExactSet builds a string set for exact-match levels (test, scenario).
// No @ stripping, no case folding — match-as-typed.
func toExactSet(s []string) map[string]struct{} {
	out := make(map[string]struct{}, len(s))
	for _, t := range s {
		t = strings.TrimSpace(t)
		if t != "" {
			out[t] = struct{}{}
		}
	}
	return out
}

// toGroupPaths normalizes each filter entry into its segments. Trims leading
// and trailing "::" (cosmetic — `"::ABC::PQR::"` and `"ABC::PQR"` are the same
// path), collapses any consecutive separators (`A::::B` → `A::B`) so the
// runtime is forgiving even if the upstream validator missed something, and
// drops entries that end up empty.
func toGroupPaths(s []string) [][]string {
	out := make([][]string, 0, len(s))
	for _, raw := range s {
		segs := splitGroupPath(raw)
		if len(segs) > 0 {
			out = append(out, segs)
		}
	}
	return out
}

func splitGroupPath(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, groupSeparator)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			// Drop empty segments produced by leading/trailing/consecutive
			// "::". `"::ABC::PQR"` and `"ABC::::PQR"` both normalize cleanly.
			continue
		}
		out = append(out, p)
	}
	return out
}

func (f *Filter) hasAnyConstraint() bool {
	return len(f.test) > 0 || len(f.group) > 0 || len(f.scenario) > 0
}

// Mode returns the active mode.
func (f *Filter) Mode() Mode { return f.mode }

// ShouldRun applies the configured filter across the active levels:
//
//	testTags     — values from the request's `params.tags` (passed in from JS)
//	groupName    — current state.Tags["group"] — k6's "::"-joined group path
//	scenarioName — current state.Tags["scenario"] — the scenario key
//
// Match semantics:
//   - test:     exact string match between any value in testTags and any entry
//     in the configured set.
//   - scenario: exact string match between scenarioName and the set.
//   - group:    prefix match on path segments. Filter "ABC" matches a request
//     at "ABC", "ABC::PQR", "ABC::PQR::DEEP", … (all descendants).
//     Filter "ABC::PQR" matches "ABC::PQR" and below, but not "ABC".
//
// OR across active levels (any level that has a configured filter and matches
// makes the overall match true).
//
//	include mode → run if matched, skip if not
//	exclude mode → skip if matched, run if not
func (f *Filter) ShouldRun(testTags []string, groupName, scenarioName string) bool {
	if f.mode == ModeNone {
		return true
	}

	matched := false

	if len(f.test) > 0 && intersectsExact(testTags, f.test) {
		matched = true
	}
	if !matched && len(f.group) > 0 {
		if f.matchesGroup(groupName) {
			matched = true
		}
	}
	if !matched && len(f.scenario) > 0 && scenarioName != "" {
		if _, ok := f.scenario[scenarioName]; ok {
			matched = true
		}
	}

	if f.mode == ModeInclude {
		return matched
	}
	return !matched
}

func intersectsExact(values []string, set map[string]struct{}) bool {
	for _, v := range values {
		if _, ok := set[v]; ok {
			return true
		}
	}
	return false
}

// matchesGroup returns true if any configured group filter is a path prefix of
// the request's current group path. A bare filter "ABC" matches every request
// inside ABC and any descendant; "ABC::PQR" matches at-or-below ABC::PQR.
func (f *Filter) matchesGroup(groupName string) bool {
	pathSegs := splitGroupPath(groupName)
	if len(pathSegs) == 0 {
		// Top-level request — no group context, can never match a group filter.
		return false
	}
	for _, prefix := range f.group {
		if isPathPrefix(prefix, pathSegs) {
			return true
		}
	}
	return false
}

func isPathPrefix(prefix, full []string) bool {
	if len(prefix) > len(full) {
		return false
	}
	for i, p := range prefix {
		if full[i] != p {
			return false
		}
	}
	return true
}
