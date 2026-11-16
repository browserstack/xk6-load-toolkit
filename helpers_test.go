package loadtoolkit

import (
	"encoding/json"
	"testing"
	"time"

	"go.k6.io/k6/v2/metrics"
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

func computedColumn(name string) ColumnSpec {
	return ColumnSpec{Name: name, From: Sources{"computed:" + name}}
}

// testSinkConfig is a neutral sink configuration exposing every computed value.
func testSinkConfig() *SinkConfig {
	cols := []ColumnSpec{computedColumn("timestamp"), {Name: "job", From: Sources{"tag:job", "const:job"}}}
	for _, name := range []string{"scenario", "status", "statusMessage", "url", "requestName", "group",
		"vus", "bytesReceived", "bytesSent", "latency", "connect", "duration", "errorCount"} {
		cols = append(cols, computedColumn(name))
	}
	cols = append(cols, ColumnSpec{Name: "instance", From: Sources{"tag:instance", "const:instance"}})
	return &SinkConfig{
		Transport: TransportConfig{Endpoint: "http://127.0.0.1:1", Table: "requests", AssertionTable: "checks"},
		Constants: map[string]string{"job": "job-const", "instance": "main"},
		Columns:   cols,
		AssertionColumns: []ColumnSpec{
			{Name: "name", From: Sources{"tag:check"}},
			{Name: "label", From: Sources{"tag:name", "tag:url", "computed:parentLabel"}},
			{Name: "scenario", From: Sources{"tag:group", "tag:scenario"}},
			{Name: "status", From: Sources{"tag:status", "computed:parentStatus"}},
			{Name: "failed", From: Sources{"computed:failed"}},
			{Name: "job", From: Sources{"tag:job", "const:job"}},
		},
		Correlation: CorrelationConfig{
			RequestMetrics:      []string{"http_reqs", "http_req_duration", "http_req_waiting", "http_req_blocked", "http_req_connecting", "http_req_tls_handshaking"},
			URLMetaKey:          "fullUrl",
			URLLegacyTag:        "legacyUrl",
			BytesIDMetaKey:      testBytesID,
			BytesSentMetric:     testBytesSent,
			BytesReceivedMetric: testBytesReceived,
			WSMetrics:           []string{"ws_connecting", "ws_sessions", "ws_session_duration", "ws_ping", "ws_connect_error"},
			WSSummedMetrics:     []string{"ws_msgs_sent", "ws_msgs_received"},
			WSErrorMetric:       "ws_connect_error",
			WSErrorMetaKey:      "wsError",
			CheckMetric:         "checks",
		},
		SendTimeRestamp: true,
	}
}

func newTestBuffer(t *testing.T, maxEntries int) *metricBuffer {
	t.Helper()
	cfg := testSinkConfig()
	if err := cfg.validate(); err != nil {
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
