// Package loadtoolkit is a k6 extension with three independently configured
// capabilities: an output that posts request, WebSocket and check rows as
// JSONEachRow, a request-rate limiter module and a tag filter module. All
// configuration comes from one JSON document in XK6_LOAD_TOOLKIT_CONFIG.
//
// The package exports nothing. k6 reaches the capabilities through the
// registrations below (--out load-toolkit-sink, and the k6/x/load-toolkit/* JS
// modules); each capability lives in its own package under internal/.
package loadtoolkit

import (
	"go.k6.io/k6/v2/js/modules"
	"go.k6.io/k6/v2/output"

	"github.com/browserstack/xk6-load-toolkit/internal/config"
	"github.com/browserstack/xk6-load-toolkit/internal/sink"
	tagfilter "github.com/browserstack/xk6-load-toolkit/internal/tag-filter"
	"github.com/browserstack/xk6-load-toolkit/internal/throughput"
)

// init registers each capability with k6. The configuration itself is read
// earlier, in internal/config's init(), which Go runs before this one.
func init() {
	output.RegisterExtension(sink.Name, func(params output.Params) (output.Output, error) {
		cfg, err := config.Loaded()
		return newSink(cfg, err, params)
	})
	modules.Register(throughput.ModuleName, throughput.NewModule(config.Throughput))
	modules.Register(tagfilter.ModuleName, tagfilter.NewModule(config.TagFilter))
}

// newSink builds the output from the configuration loaded at init. The check
// filter is built here, not in the sink, so the sink never imports the tag
// filter.
func newSink(cfg *config.Config, loadErr error, params output.Params) (output.Output, error) {
	if loadErr != nil {
		return nil, loadErr
	}
	var checks sink.CheckFilter
	if cfg.Sink != nil && cfg.Sink.CheckFilter {
		checks = tagfilter.NewCheckFilter(cfg.TagFilter, params.ScriptOptions.SystemTags)
	}
	return sink.New(cfg.Sink, checks), nil
}
