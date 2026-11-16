// Package loadtoolkit is a k6 extension with three independently configured
// capabilities: an output that posts request, WebSocket and check rows as
// JSONEachRow, a request-rate limiter module and a tag filter module. All
// configuration comes from one JSON document in XK6_LOAD_TOOLKIT_CONFIG.
package loadtoolkit

import (
	"go.k6.io/k6/v2/js/modules"
	"go.k6.io/k6/v2/output"
)

func init() {
	output.RegisterExtension(SinkName, NewSink)
	modules.Register(ThroughputModule, NewThroughput())
	modules.Register(TagFilterModule, NewTagFilter())
}
