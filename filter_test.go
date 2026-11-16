package loadtoolkit

import (
	"reflect"
	"sort"
	"testing"
)

func mkFilter(mode Mode, test, group, scenario []string) *Filter {
	f := &Filter{
		mode:     mode,
		test:     toExactSet(test),
		group:    toGroupPaths(group),
		scenario: toExactSet(scenario),
	}
	if mode != ModeNone && !f.hasAnyConstraint() {
		f.mode = ModeNone
	}
	return f
}

func TestShouldRun_NoFilter(t *testing.T) {
	f := mkFilter(ModeNone, nil, nil, nil)
	if !f.ShouldRun([]string{"smoke"}, "::ABC", "smoke") {
		t.Fatal("ModeNone should pass all requests")
	}
	if !f.ShouldRun(nil, "", "") {
		t.Fatal("ModeNone should pass untagged requests")
	}
}

func TestShouldRun_IncludeMode_TestLevel_ExactMatch(t *testing.T) {
	f := mkFilter(ModeInclude, []string{"smoke", "api"}, nil, nil)
	cases := []struct {
		name string
		test []string
		want bool
	}{
		{"matches smoke", []string{"smoke"}, true},
		{"matches api", []string{"api"}, true},
		{"any-of (one match)", []string{"smoke", "regression"}, true},
		{"no match", []string{"regression"}, false},
		{"untagged is skipped under include", nil, false},
		{"@-prefixed value does NOT match bare filter", []string{"@smoke"}, false},
	}
	for _, c := range cases {
		got := f.ShouldRun(c.test, "", "")
		if got != c.want {
			t.Errorf("%s: ShouldRun(%v) = %v, want %v", c.name, c.test, got, c.want)
		}
	}
}

func TestShouldRun_IncludeMode_TestLevel_StrictAtSign(t *testing.T) {
	// Filter explicitly contains "@smoke" — only the literal string matches.
	f := mkFilter(ModeInclude, []string{"@smoke"}, nil, nil)
	if !f.ShouldRun([]string{"@smoke"}, "", "") {
		t.Fatal(`filter ["@smoke"] should match value "@smoke"`)
	}
	if f.ShouldRun([]string{"smoke"}, "", "") {
		t.Fatal(`filter ["@smoke"] should NOT match bare "smoke" (strict exact)`)
	}
}

func TestShouldRun_IncludeMode_GroupLevel_PrefixMatch(t *testing.T) {
	f := mkFilter(ModeInclude, nil, []string{"ABC"}, nil)
	cases := []struct {
		group string
		want  bool
	}{
		{"::ABC", true},            // direct in ABC
		{"::ABC::PQR", true},       // child PQR
		{"::ABC::PQR::DEEP", true}, // grandchild
		{"::AB", false},            // not a prefix segment
		{"::OTHER", false},
		{"::OTHER::ABC", false}, // ABC is mid-path, not a prefix
		{"", false},             // top-level request
	}
	for _, c := range cases {
		got := f.ShouldRun(nil, c.group, "")
		if got != c.want {
			t.Errorf("group=%q: ShouldRun = %v, want %v", c.group, got, c.want)
		}
	}
}

func TestShouldRun_IncludeMode_GroupLevel_NestedPrefix(t *testing.T) {
	f := mkFilter(ModeInclude, nil, []string{"ABC::PQR"}, nil)
	cases := []struct {
		group string
		want  bool
	}{
		{"::ABC::PQR", true},
		{"::ABC::PQR::DEEP", true},
		{"::ABC", false}, // parent doesn't match a deeper-than-self filter
		{"::ABC::XYZ", false},
		{"", false},
	}
	for _, c := range cases {
		got := f.ShouldRun(nil, c.group, "")
		if got != c.want {
			t.Errorf("group=%q: ShouldRun = %v, want %v", c.group, got, c.want)
		}
	}
}

func TestShouldRun_IncludeMode_GroupLevel_Normalization(t *testing.T) {
	// All four filter forms should be equivalent: "ABC", "::ABC", "ABC::",
	// "ABC::::". The runtime collapses consecutive "::" defensively.
	for _, raw := range []string{"ABC", "::ABC", "ABC::", "::ABC::", "ABC::::"} {
		f := mkFilter(ModeInclude, nil, []string{raw}, nil)
		if !f.ShouldRun(nil, "::ABC", "") {
			t.Errorf("filter %q failed to match group %q", raw, "::ABC")
		}
		if !f.ShouldRun(nil, "::ABC::PQR", "") {
			t.Errorf("filter %q failed to match descendant %q", raw, "::ABC::PQR")
		}
	}
}

func TestShouldRun_IncludeMode_ScenarioLevel_ExactMatch(t *testing.T) {
	f := mkFilter(ModeInclude, nil, nil, []string{"smoke", "regression"})
	cases := []struct {
		scenario string
		want     bool
	}{
		{"smoke", true},
		{"regression", true},
		{"@smoke", false},    // strict exact
		{"smokeFlow", false}, // not a prefix-match level
		{"e2e", false},
		{"", false},
	}
	for _, c := range cases {
		got := f.ShouldRun(nil, "", c.scenario)
		if got != c.want {
			t.Errorf("scenario=%q: ShouldRun = %v, want %v", c.scenario, got, c.want)
		}
	}
}

func TestShouldRun_IncludeMode_OrAcrossLevels(t *testing.T) {
	f := mkFilter(ModeInclude,
		[]string{"smoke"},    // test
		[]string{"checkout"}, // group (matches checkout and any descendant)
		[]string{"baseline"}, // scenario
	)
	cases := []struct {
		name     string
		test     []string
		group    string
		scenario string
		want     bool
	}{
		{"test match wins", []string{"smoke"}, "::unrelated", "other", true},
		{"group match wins", nil, "::checkout::flow", "other", true},
		{"scenario match wins", nil, "", "baseline", true},
		{"no match anywhere", []string{"flaky"}, "::unrelated", "other", false},
		{"all untagged", nil, "", "", false},
	}
	for _, c := range cases {
		got := f.ShouldRun(c.test, c.group, c.scenario)
		if got != c.want {
			t.Errorf("%s: ShouldRun = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestShouldRun_ExcludeMode_TestLevel(t *testing.T) {
	f := mkFilter(ModeExclude, []string{"flaky"}, nil, nil)
	cases := []struct {
		test []string
		want bool
	}{
		{[]string{"smoke"}, true},
		{[]string{"flaky"}, false},
		{[]string{"smoke", "flaky"}, false},
		{nil, true},
	}
	for _, c := range cases {
		got := f.ShouldRun(c.test, "", "")
		if got != c.want {
			t.Errorf("test=%v: ShouldRun = %v, want %v", c.test, got, c.want)
		}
	}
}

func TestShouldRun_ExcludeMode_GroupLevel_PrefixMatch(t *testing.T) {
	f := mkFilter(ModeExclude, nil, []string{"slow"}, nil)
	cases := []struct {
		group string
		want  bool
	}{
		{"::slow", false},       // direct in slow → excluded
		{"::slow::poll", false}, // descendant → excluded
		{"::fast::quick", true},
		{"", true}, // top-level passes (no group context)
	}
	for _, c := range cases {
		got := f.ShouldRun(nil, c.group, "")
		if got != c.want {
			t.Errorf("group=%q: ShouldRun = %v, want %v", c.group, got, c.want)
		}
	}
}

func TestShouldRun_ExcludeMode_OrAcrossLevels(t *testing.T) {
	f := mkFilter(ModeExclude, []string{"flaky"}, []string{"slow"}, []string{"chaos"})
	cases := []struct {
		name     string
		test     []string
		group    string
		scenario string
		want     bool
	}{
		{"clean", []string{"smoke"}, "::checkout", "baseline", true},
		{"flaky test → exclude", []string{"flaky"}, "", "", false},
		{"slow group → exclude", nil, "::slow::poll", "", false},
		{"chaos scenario → exclude", nil, "", "chaos", false},
		{"any-level match excludes", []string{"smoke"}, "::slow", "", false},
	}
	for _, c := range cases {
		got := f.ShouldRun(c.test, c.group, c.scenario)
		if got != c.want {
			t.Errorf("%s: ShouldRun = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestParseFilter_EmptyListsBecomeModeNone(t *testing.T) {
	f := parseFilter(ModeInclude, `{"test":[],"group":[],"scenario":[]}`)
	if f.Mode() != ModeNone {
		t.Fatalf("all-empty filter should collapse to ModeNone, got %v", f.Mode())
	}
}

func TestParseFilter_MalformedJSONFailsOpen(t *testing.T) {
	f := parseFilter(ModeInclude, `{not-json`)
	if f.Mode() != ModeNone {
		t.Fatal("malformed JSON should fail-open (ModeNone), not silently filter all requests")
	}
}

func TestFilterFromConfig_PrefersInclude(t *testing.T) {
	f := FilterFromConfig(&TagFilterConfig{Include: `{"test":["smoke"]}`, Exclude: `{"test":["flaky"]}`})
	if f.Mode() != ModeInclude {
		t.Fatalf("when both rules are set, include should win; got %v", f.Mode())
	}
}

func TestFilterFromConfig_Absent(t *testing.T) {
	for _, cfg := range []*TagFilterConfig{nil, {}, {Include: "  ", Exclude: ""}} {
		if f := FilterFromConfig(cfg); f.Mode() != ModeNone {
			t.Fatalf("absent rules should be ModeNone, got %v", f.Mode())
		}
	}
}

func TestSplitGroupPath(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"ABC", []string{"ABC"}},
		{"::ABC", []string{"ABC"}},
		{"ABC::", []string{"ABC"}},
		{"::ABC::PQR", []string{"ABC", "PQR"}},
		{"ABC::PQR::DEEP", []string{"ABC", "PQR", "DEEP"}},
		{"ABC::::PQR", []string{"ABC", "PQR"}}, // collapse consecutive "::"
		{"  ABC  ::  PQR  ", []string{"ABC", "PQR"}},
		{"", nil},
		{"::", nil},
		{"::::", nil},
	}
	for _, c := range cases {
		got := splitGroupPath(c.in)
		want := append([]string{}, c.want...)
		if c.want == nil {
			if got != nil && len(got) != 0 {
				t.Errorf("splitGroupPath(%q) = %v, want empty", c.in, got)
			}
			continue
		}
		gotCopy := append([]string{}, got...)
		sort.Strings(gotCopy)
		sort.Strings(want)
		if !reflect.DeepEqual(gotCopy, want) {
			t.Errorf("splitGroupPath(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
