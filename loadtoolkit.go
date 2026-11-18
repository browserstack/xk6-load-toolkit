// Package loadtoolkit is a k6 extension with three independently configured
// capabilities: an output that posts request, WebSocket and check rows as
// JSONEachRow, a request-rate limiter module and a tag filter module. All
// configuration comes from one JSON document in XK6_LOAD_TOOLKIT_CONFIG.
//
// Each capability lives in its own package under internal/; this package only
// registers them with k6 and keeps the small API external callers use.
package loadtoolkit

import (
	"go.k6.io/k6/v2/output"

	"github.com/browserstack/xk6-load-toolkit/internal/config"
	"github.com/browserstack/xk6-load-toolkit/internal/sink"
	"github.com/browserstack/xk6-load-toolkit/internal/tagfilter"
	"github.com/browserstack/xk6-load-toolkit/internal/throughput"
)

const (
	// ConfigEnv carries the whole toolkit configuration as JSON. It is read and
	// removed from the process environment at init, before k6 builds __ENV.
	ConfigEnv = config.ConfigEnv
	// DiagnosticsEnv optionally names a file that receives the detailed reason
	// when the configuration is invalid. Also removed at init.
	DiagnosticsEnv = config.DiagnosticsEnv
	// SchemaVersion is the only configuration version this build accepts.
	SchemaVersion = config.SchemaVersion

	// SinkName is the value passed to `k6 run --out`.
	SinkName = sink.Name
	// ThroughputModule is the JS import path of the request-rate limiter.
	ThroughputModule = throughput.ModuleName
	// TagFilterModule is the JS import path of the tag filter.
	TagFilterModule = tagfilter.ModuleName
)

// Config is the root of the toolkit configuration. A nil section switches that
// capability off.
type Config = config.Config

// ParseConfig decodes and validates a configuration document. Unknown fields
// are rejected so a typo fails fast instead of silently disabling a step. On a
// validation error the decoded config is returned alongside the error.
func ParseConfig(raw []byte) (*Config, error) { return config.Parse(raw) }

// NewSink is the output constructor registered with k6. It uses the
// configuration read at init.
func NewSink(params output.Params) (output.Output, error) {
	cfg, err := config.Loaded()
	return newSink(cfg, err, params)
}

// newSink builds the output from a loaded configuration, handing the sink the
// tag filter's check-sample policy when the sink section asks for it.
func newSink(cfg *Config, loadErr error, params output.Params) (output.Output, error) {
	if loadErr != nil {
		return nil, loadErr
	}
	var checks sink.CheckFilter
	if cfg.Sink != nil && cfg.Sink.CheckFilter {
		checks = tagfilter.NewCheckFilter(cfg.TagFilter, params.ScriptOptions.SystemTags)
	}
	return sink.New(cfg.Sink, checks), nil
}
