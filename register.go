package loadtoolkit

import (
	"go.k6.io/k6/v2/js/modules"
	"go.k6.io/k6/v2/output"

	"github.com/browserstack/xk6-load-toolkit/internal/config"
	"github.com/browserstack/xk6-load-toolkit/internal/tagfilter"
	"github.com/browserstack/xk6-load-toolkit/internal/throughput"
)

// init registers each capability with k6. The configuration itself is read
// earlier, in internal/config's init(), which Go runs before this one.
func init() {
	output.RegisterExtension(SinkName, NewSink)
	modules.Register(ThroughputModule, throughput.NewModule(config.Throughput))
	modules.Register(TagFilterModule, tagfilter.NewModule(config.TagFilter))
}
