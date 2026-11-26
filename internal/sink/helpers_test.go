package sink

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.k6.io/k6/v2/metrics"

	"github.com/browserstack/xk6-load-toolkit/internal/testutil"
)

// testRow decodes a row rendered with testSinkConfig's columns.
type testRow struct {
	Timestamp     int64  `json:"timestamp"`
	Job           string `json:"job"`
	Scenario      string `json:"scenario"`
	Status        string `json:"status"`
	StatusMessage string `json:"statusMessage"`
	URL           string `json:"url"`
	RequestName   string `json:"requestName"`
	Group         string `json:"group"`
	VUs           int64  `json:"vus"`
	BytesReceived int64  `json:"bytesReceived"`
	BytesSent     int64  `json:"bytesSent"`
	Latency       int64  `json:"latency"`
	Connect       int64  `json:"connect"`
	Duration      int64  `json:"duration"`
	ErrorCount    int64  `json:"errorCount"`
	Instance      string `json:"instance"`
}

type testAssertion struct {
	Name     string `json:"name"`
	Label    string `json:"label"`
	Scenario string `json:"scenario"`
	Status   string `json:"status"`
	Failed   bool   `json:"failed"`
	Job      string `json:"job"`
}

const (
	testBytesSent     = "req_bytes_sent"
	testBytesReceived = "req_bytes_received"
	testBytesID       = "reqId"
)

// testSinkConfig is a neutral sink configuration exposing every computed value.
func testSinkConfig() *Config {
	dec := json.NewDecoder(strings.NewReader(testutil.SinkConfigJSON))
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		panic("testutil.SinkConfigJSON: " + err.Error())
	}
	return &cfg
}

func newTestBuffer(t *testing.T, maxEntries int) *metricBuffer {
	t.Helper()
	cfg := testSinkConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("test config invalid: %v", err)
	}
	return newMetricBuffer(newRowEngine(cfg), maxEntries, time.Minute)
}

func decodeRows(t *testing.T, rows []row) []testRow {
	t.Helper()
	out := make([]testRow, len(rows))
	for i, r := range rows {
		if err := json.Unmarshal(r, &out[i]); err != nil {
			t.Fatalf("row %d is not JSON: %v (%s)", i, err, r)
		}
	}
	return out
}

func decodeAssertions(t *testing.T, rows []row) []testAssertion {
	t.Helper()
	out := make([]testAssertion, len(rows))
	for i, r := range rows {
		if err := json.Unmarshal(r, &out[i]); err != nil {
			t.Fatalf("assertion %d is not JSON: %v (%s)", i, err, r)
		}
	}
	return out
}

func mkSample(reg *metrics.Registry, name string, tags map[string]string, at time.Time, value float64) metrics.Sample {
	m, err := reg.NewMetric(name, metrics.Counter)
	if err != nil {
		m = reg.Get(name)
	}
	return metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: m, Tags: reg.RootTagSet().WithTagsFromMap(tags)},
		Time:       at,
		Value:      value,
	}
}

func withMeta(s metrics.Sample, meta map[string]string) metrics.Sample {
	s.Metadata = meta
	return s
}

func reqTags(vu string) map[string]string {
	return map[string]string{"vu": vu, "iter": "0", "method": "GET", "name": "req", "status": "200"}
}

// renderOne buffers one complete request from the given tags/meta and returns its row.
func renderOne(t *testing.T, tags, meta map[string]string) testRow {
	t.Helper()
	b := newTestBuffer(t, 0)
	reg := metrics.NewRegistry()
	b.add(withMeta(mkSample(reg, "http_reqs", tags, time.Unix(1700000000, 0), 1), meta))
	rows := decodeRows(t, b.drainAll())
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	return rows[0]
}

// Fixture names for the joined-result, verdict and hold tests. The request
// samples carry the operation metadata; a separate opResultMetric sample, keyed
// by the request id, carries the result.
const (
	opResultMetric  = "op_result"
	opCallsMetric   = "calls_seen"
	opVerdictMetric = "http_req_failed"
	opNameKey       = "opName"
	opPendingKey    = "opPending"
	opStateKey      = "opState"
	opCodeKey       = "opCode"
	opCodeSourceKey = "opCodeSource"
	opTypeKey       = "opType"
	opShapeKey      = "opShape"
	opBatchKey      = "opBatch"
	otherOps        = "~otherOperations"
	otherCodes      = "~otherCodes"
	opMaxNames      = 500
	opMaxCodes      = 100
	opNameRunes     = 200
	opCodeRunes     = 80
	opStateChecked  = 3 // the state a result with no valid state, or an expired hold, reads as
)

// opColumnsJSON are the bounded columns the joined-result fixture appends to the
// base columns.
const opColumnsJSON = `[
	{"name": "failed_verdict", "from": "computed:failedVerdict"},
	{"name": "op_name", "from": "meta:opName", "maxRunes": 200,
		"distinct": {"limit": 500, "overflow": "~otherOperations"}},
	{"name": "op_state", "from": "meta:opState", "type": "uint", "min": 1, "max": 3, "joinDefault": "3"},
	{"name": "op_code", "from": "meta:opCode", "maxRunes": 80,
		"when": [{"source": "meta:opState", "equals": "2"}],
		"distinct": {"limit": 100, "overflow": "~otherCodes", "partitionBy": "meta:opCodeSource", "partitions": ["2"]}},
	{"name": "op_code_source", "from": "meta:opCodeSource", "type": "uint", "max": 2,
		"when": [{"source": "meta:opState", "equals": "2"}]},
	{"name": "op_type", "from": "meta:opType", "type": "uint", "max": 3, "when": [{"source": "meta:opName"}]},
	{"name": "op_shape", "from": "meta:opShape", "type": "uint", "max": 6, "when": [{"source": "meta:opName"}]},
	{"name": "op_batch", "from": "meta:opBatch", "type": "uint", "max": 65535,
		"when": [{"source": "meta:opName"}, {"source": "meta:opShape", "equals": "2"}]}
]`

// opRow decodes the columns opColumnsJSON adds, plus the base ones the tests read.
type opRow struct {
	RequestName   string `json:"requestName"`
	ErrorCount    int64  `json:"errorCount"`
	FailedVerdict int64  `json:"failed_verdict"`
	OpName        string `json:"op_name"`
	OpState       uint64 `json:"op_state"`
	OpCode        string `json:"op_code"`
	OpCodeSource  uint64 `json:"op_code_source"`
	OpType        uint64 `json:"op_type"`
	OpShape       uint64 `json:"op_shape"`
	OpBatch       uint64 `json:"op_batch"`
}

func mustColumns(raw string) []ColumnSpec {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var specs []ColumnSpec
	if err := dec.Decode(&specs); err != nil {
		panic("column fixture: " + err.Error())
	}
	return specs
}

// withVerdict configures only the verdict join and its column.
func withVerdict(c *Config) {
	c.Correlation.VerdictMetric = opVerdictMetric
	c.Columns = append(c.Columns, mustColumns(`[{"name": "failed_verdict", "from": "computed:failedVerdict"}]`)...)
}

// withOpJoin configures the verdict, the result join, the hold and the bounded columns.
func withOpJoin(c *Config) {
	c.Correlation.VerdictMetric = opVerdictMetric
	c.Correlation.JoinMetrics = []string{opResultMetric}
	c.Hold = &HoldConfig{PendingMetaKey: opPendingKey, PendingValue: "1", OnExpireMeta: map[string]string{opStateKey: "3"}}
	c.Columns = append(c.Columns, mustColumns(opColumnsJSON)...)
}

// newConfiguredBuffer builds a metric buffer from the base config as mutated.
func newConfiguredBuffer(t *testing.T, mutate func(*Config)) *metricBuffer {
	t.Helper()
	cfg := testSinkConfig()
	if mutate != nil {
		mutate(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}
	return newMetricBuffer(newRowEngine(cfg), 0, cfg.Transport.StaleAfter.or(defaultStaleAfter))
}

func decodeOpRows(t *testing.T, rows []row) []opRow {
	t.Helper()
	out := make([]opRow, len(rows))
	for i, r := range rows {
		if err := json.Unmarshal(r, &out[i]); err != nil {
			t.Fatalf("row %d is not JSON: %v (%s)", i, err, r)
		}
	}
	return out
}

func opRowByName(t *testing.T, rows []opRow, requestName string) opRow {
	t.Helper()
	for _, r := range rows {
		if r.RequestName == requestName {
			return r
		}
	}
	t.Fatalf("no row named %q in %d rows", requestName, len(rows))
	return opRow{}
}

// addOpRequest adds one request's samples carrying the request metadata.
func addOpRequest(b *metricBuffer, reg *metrics.Registry, name string, at time.Time, meta map[string]string) {
	tags := map[string]string{"vu": "1", "iter": "0", "method": "POST", "name": name, "status": "200"}
	b.add(withMeta(mkSample(reg, "http_reqs", tags, at, 1), meta))
	b.add(withMeta(mkSample(reg, "http_req_duration", tags, at, 30), meta))
}

func opResult(reg *metrics.Registry, at time.Time, meta map[string]string) metrics.Sample {
	return withMeta(mkSample(reg, opResultMetric, map[string]string{"scenario": "default"}, at, 1), meta)
}
