package loadtoolkit

import "go.k6.io/k6/v2/metrics"

// checkFilter drops check samples recorded against requests the tag filter
// skipped. A skipped request hands the script an inert response, and k6 records
// the script's check against it as a real sample; without this, those checks
// would be written as assertions for requests that never left the process.
//
// It is deliberately narrow:
//   - check samples only, so iterations, vus and other untagged samples are untouched;
//   - group-only rules only: levels are OR'd and a check does not carry its
//     request's own tags, so with a test or scenario rule there is no way to
//     tell whether the request ran;
//   - only when the `group` system tag is enabled. Without it every check looks
//     ungrouped, and in include mode that would drop every real assertion.
type checkFilter struct {
	filter *Filter
}

func newCheckFilter(rules *TagFilterConfig, systemTags *metrics.SystemTagSet) *checkFilter {
	if systemTags == nil || !systemTags.Has(metrics.TagGroup) {
		return &checkFilter{}
	}
	f := FilterFromConfig(rules)
	if f.mode == ModeNone || len(f.test) > 0 || len(f.scenario) > 0 || len(f.group) == 0 {
		return &checkFilter{}
	}
	return &checkFilter{filter: f}
}

func (c *checkFilter) mode() string {
	if c == nil || c.filter == nil {
		return "inert"
	}
	if c.filter.mode == ModeExclude {
		return "exclude"
	}
	return "include"
}

// dropsCheck reports whether a check sample with this group path is fabricated.
// An absent group key says nothing about the request, so the sample is kept.
func (c *checkFilter) dropsCheck(groupPath string, hasGroup bool) bool {
	if c == nil || c.filter == nil || !hasGroup {
		return false
	}
	matched := c.filter.matchesGroup(groupPath)
	if c.filter.mode == ModeExclude {
		return matched
	}
	return !matched
}
