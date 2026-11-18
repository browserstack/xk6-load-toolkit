package tagfilter_test

import (
	"testing"

	"go.k6.io/k6/v2/metrics"

	tagfilter "github.com/browserstack/xk6-load-toolkit/internal/tag-filter"
	"github.com/browserstack/xk6-load-toolkit/internal/testutil"
)

func TestCheckFilter_InertUnlessGroupOnly(t *testing.T) {
	cases := []*tagfilter.Config{
		nil,
		{},
		{Include: `{"test":["smoke"]}`},
		{Include: `{"scenario":["s"],"group":["G"]}`},
		{Include: `{"test":["smoke"],"group":["G"]}`},
		{Include: `not json`},
		{Include: `{"group":["  "]}`},
	}
	for _, rules := range cases {
		if f := tagfilter.NewCheckFilter(rules, testutil.GroupEnabled()); f.Mode() != "inert" || f.DropsCheck("", true) {
			t.Errorf("rules %+v: mode=%s, want inert", rules, f.Mode())
		}
	}
	// A blank test level is no constraint, exactly as in the request filter.
	if f := tagfilter.NewCheckFilter(&tagfilter.Config{Include: `{"test":["  "],"group":["G"]}`}, testutil.GroupEnabled()); f.Mode() != "include" {
		t.Errorf("blank test level: mode=%s, want include", f.Mode())
	}
}

func TestCheckFilter_RequiresTheGroupSystemTag(t *testing.T) {
	rules := &tagfilter.Config{Include: `{"group":["G"]}`}
	if f := tagfilter.NewCheckFilter(rules, nil); f.Mode() != "inert" {
		t.Fatal("must stay inert when SystemTags is nil")
	}
	if f := tagfilter.NewCheckFilter(rules, metrics.NewSystemTagSet(metrics.TagStatus)); f.Mode() != "inert" {
		t.Fatal("must stay inert when the group system tag is off")
	}
}

func TestCheckFilter_IncludeMode(t *testing.T) {
	f := tagfilter.NewCheckFilter(&tagfilter.Config{Include: `{"group":["Checkout"]}`}, testutil.GroupEnabled())
	cases := map[string]bool{"::Checkout": false, "::Checkout::Pay": false, "::Browse": true, "": true, "::CheckoutX": true}
	for path, wantDrop := range cases {
		if got := f.DropsCheck(path, true); got != wantDrop {
			t.Errorf("dropsCheck(%q) = %v, want %v", path, got, wantDrop)
		}
	}
}

func TestCheckFilter_NestedEntry(t *testing.T) {
	f := tagfilter.NewCheckFilter(&tagfilter.Config{Include: `{"group":["Checkout::Pay"]}`}, testutil.GroupEnabled())
	if f.DropsCheck("::Checkout::Pay::Card", true) || !f.DropsCheck("::Checkout", true) {
		t.Fatal("nested entry must match at or below itself only")
	}
}

func TestCheckFilter_ExcludeMode(t *testing.T) {
	f := tagfilter.NewCheckFilter(&tagfilter.Config{Exclude: `{"group":["Checkout"]}`}, testutil.GroupEnabled())
	if f.Mode() != "exclude" || !f.DropsCheck("::Checkout::Pay", true) || f.DropsCheck("::Browse", true) || f.DropsCheck("", true) {
		t.Fatal("exclude mode drops only checks inside the excluded group")
	}
}

func TestCheckFilter_IncludeWins(t *testing.T) {
	f := tagfilter.NewCheckFilter(&tagfilter.Config{Include: `{"group":["A"]}`, Exclude: `{"group":["B"]}`}, testutil.GroupEnabled())
	if f.Mode() != "include" {
		t.Fatalf("mode = %s", f.Mode())
	}
}

// An absent group key says nothing about the request, so the check is kept.
func TestCheckFilter_AbsentGroupKeyIsKept(t *testing.T) {
	f := tagfilter.NewCheckFilter(&tagfilter.Config{Include: `{"group":["Checkout"]}`}, testutil.GroupEnabled())
	if f.DropsCheck("", false) {
		t.Fatal("a check with no group key must be kept")
	}
	var nilFilter *tagfilter.CheckFilter
	if nilFilter.DropsCheck("", true) || nilFilter.Mode() != "inert" {
		t.Fatal("nil filter must be inert")
	}
}
