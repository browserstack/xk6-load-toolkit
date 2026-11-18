// Package tagfilter is the tag filter capability: include/exclude rules over
// request tags, groups and scenarios, exposed to scripts as the
// k6/x/load-toolkit/tagfilter JS module, plus the check-sample policy the sink
// applies to checks recorded against requests the filter skipped.
package tagfilter

// Config holds the raw include/exclude rule strings,
// shaped {"test":[...],"group":[...],"scenario":[...]}. Include wins.
type Config struct {
	Include string `json:"include"`
	Exclude string `json:"exclude"`
}

// Validate checks the section. Rule strings are parsed at run time and fail
// open, so nothing here can be rejected.
func (c *Config) Validate() error { return nil }
