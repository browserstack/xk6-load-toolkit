package loadtoolkit

import (
	"testing"

	"go.k6.io/k6/v2/metrics"
)

func groupEnabled() *metrics.SystemTagSet {
	return metrics.NewSystemTagSet(metrics.TagGroup)
}

func TestCheckFilter_InertUnlessGroupOnly(t *testing.T) {
	cases := []*TagFilterConfig{
		nil,
		{},
		{Include: `{"test":["smoke"]}`},
		{Include: `{"scenario":["s"],"group":["G"]}`},
		{Include: `{"test":["smoke"],"group":["G"]}`},
		{Include: `not json`},
		{Include: `{"group":["  "]}`},
	}
	for _, rules := range cases {
		if f := newCheckFilter(rules, groupEnabled()); f.mode() != "inert" || f.dropsCheck("", true) {
			t.Errorf("rules %+v: mode=%s, want inert", rules, f.mode())
		}
	}
	// A blank test level is no constraint, exactly as in the request filter.
	if f := newCheckFilter(&TagFilterConfig{Include: `{"test":["  "],"group":["G"]}`}, groupEnabled()); f.mode() != "include" {
		t.Errorf("blank test level: mode=%s, want include", f.mode())
	}
}

func TestCheckFilter_RequiresTheGroupSystemTag(t *testing.T) {
	rules := &TagFilterConfig{Include: `{"group":["G"]}`}
	if f := newCheckFilter(rules, nil); f.mode() != "inert" {
		t.Fatal("must stay inert when SystemTags is nil")
	}
	if f := newCheckFilter(rules, metrics.NewSystemTagSet(metrics.TagStatus)); f.mode() != "inert" {
		t.Fatal("must stay inert when the group system tag is off")
	}
}

func TestCheckFilter_IncludeMode(t *testing.T) {
	f := newCheckFilter(&TagFilterConfig{Include: `{"group":["Checkout"]}`}, groupEnabled())
	cases := map[string]bool{"::Checkout": false, "::Checkout::Pay": false, "::Browse": true, "": true, "::CheckoutX": true}
	for path, wantDrop := range cases {
		if got := f.dropsCheck(path, true); got != wantDrop {
			t.Errorf("dropsCheck(%q) = %v, want %v", path, got, wantDrop)
		}
	}
}

func TestCheckFilter_NestedEntry(t *testing.T) {
	f := newCheckFilter(&TagFilterConfig{Include: `{"group":["Checkout::Pay"]}`}, groupEnabled())
	if f.dropsCheck("::Checkout::Pay::Card", true) || !f.dropsCheck("::Checkout", true) {
		t.Fatal("nested entry must match at or below itself only")
	}
}

func TestCheckFilter_ExcludeMode(t *testing.T) {
	f := newCheckFilter(&TagFilterConfig{Exclude: `{"group":["Checkout"]}`}, groupEnabled())
	if f.mode() != "exclude" || !f.dropsCheck("::Checkout::Pay", true) || f.dropsCheck("::Browse", true) || f.dropsCheck("", true) {
		t.Fatal("exclude mode drops only checks inside the excluded group")
	}
}

func TestCheckFilter_IncludeWins(t *testing.T) {
	f := newCheckFilter(&TagFilterConfig{Include: `{"group":["A"]}`, Exclude: `{"group":["B"]}`}, groupEnabled())
	if f.mode() != "include" {
		t.Fatalf("mode = %s", f.mode())
	}
}

// An absent group key says nothing about the request, so the check is kept.
func TestCheckFilter_AbsentGroupKeyIsKept(t *testing.T) {
	f := newCheckFilter(&TagFilterConfig{Include: `{"group":["Checkout"]}`}, groupEnabled())
	if f.dropsCheck("", false) {
		t.Fatal("a check with no group key must be kept")
	}
	var nilFilter *checkFilter
	if nilFilter.dropsCheck("", true) || nilFilter.mode() != "inert" {
		t.Fatal("nil filter must be inert")
	}
}
