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
