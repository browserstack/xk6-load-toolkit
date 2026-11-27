package sink

import (
	"strings"
	"testing"
	"time"

	"go.k6.io/k6/v2/metrics"
)

// addVerdictRequest adds one request and, when failed is non-nil, its verdict sample.
func addVerdictRequest(b *metricBuffer, reg *metrics.Registry, tags map[string]string, at time.Time, failed *float64) {
	b.add(mkSample(reg, "http_reqs", tags, at, 1))
	b.add(mkSample(reg, "http_req_duration", tags, at, 40))
	if failed != nil {
		b.add(mkSample(reg, opVerdictMetric, tags, at, *failed))
	}
}

func verdict(v float64) *float64 { return &v }

// The verdict sample joins its request row by the correlation key and never
// forms a row; errorCount keeps the HTTP-status rule.
func TestVerdict_TakesTheVerdictOfItsOwnRequest(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withVerdict)
	at := time.Unix(1700000000, 0)
	addVerdictRequest(b, reg, map[string]string{"method": "POST", "name": "Failed", "status": "200"}, at, verdict(1))
	addVerdictRequest(b, reg, map[string]string{"method": "POST", "name": "Ok", "status": "500"}, at, verdict(0))

	rows := decodeOpRows(t, b.drainAll())
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2: the verdict must join its request row, not form one", len(rows))
	}
	failed := opRowByName(t, rows, "POST Failed")
	ok := opRowByName(t, rows, "POST Ok")
	if failed.FailedVerdict != 1 || ok.FailedVerdict != 0 {
		t.Fatalf("failedVerdict = %d / %d, want 1 / 0", failed.FailedVerdict, ok.FailedVerdict)
	}
	if failed.ErrorCount != 0 || ok.ErrorCount != 1 {
		t.Fatalf("errorCount = %d / %d, want 0 / 1 (status rule unchanged)", failed.ErrorCount, ok.ErrorCount)
	}
}

// Batch entries share the VU and iteration but carry their own name and end
// time, so each verdict lands on its own row.
func TestVerdict_BatchEntriesKeepTheirOwnVerdict(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withVerdict)
	at := time.Unix(1700000000, 0)
	for i, name := range []string{"A", "B", "C"} {
		v := 0.0
		if name == "B" {
			v = 1
		}
		addVerdictRequest(b, reg, map[string]string{"method": "GET", "name": name, "status": "200"}, at.Add(time.Duration(i)*time.Microsecond), verdict(v))
	}
	rows := decodeOpRows(t, b.drainAll())
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	for name, want := range map[string]int64{"GET A": 0, "GET B": 1, "GET C": 0} {
		if got := opRowByName(t, rows, name).FailedVerdict; got != want {
			t.Fatalf("%s: failedVerdict = %d, want %d", name, got, want)
		}
	}
}

// No verdict sample: the row carries -1.
func TestVerdict_NoVerdictIsMinusOne(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withVerdict)
	addVerdictRequest(b, reg, map[string]string{"method": "GET", "name": "n", "status": "200"}, time.Unix(1700000000, 0), nil)
	rows := decodeOpRows(t, b.drainAll())
	if len(rows) != 1 || rows[0].FailedVerdict != -1 {
		t.Fatalf("rows = %+v, want one row with failedVerdict -1", rows)
	}
}

// Sample order inside a request is not a contract: the verdict may come first.
func TestVerdict_BeforeRequestLine(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withVerdict)
	at := time.Unix(1700000000, 0)
	tags := map[string]string{"method": "GET", "name": "n", "status": "200"}
	b.add(mkSample(reg, opVerdictMetric, tags, at, 1))
	b.add(mkSample(reg, "http_reqs", tags, at, 1))
	rows := decodeOpRows(t, b.drainAll())
	if len(rows) != 1 || rows[0].FailedVerdict != 1 {
		t.Fatalf("rows = %+v, want one row with failedVerdict 1", rows)
	}
}

// Without verdictMetric the verdict sample is ignored and no column exists.
func TestVerdict_UnconfiguredRowsAreUnchanged(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, nil)
	addVerdictRequest(b, reg, map[string]string{"method": "GET", "name": "n", "status": "503"}, time.Unix(1700000000, 0), verdict(1))
	raw := b.drainAll()
	if len(raw) != 1 {
		t.Fatalf("got %d rows, want 1", len(raw))
	}
	if strings.Contains(string(raw[0]), "failed_verdict") {
		t.Fatalf("unconfigured row carries the verdict column: %s", raw[0])
	}
	if r := decodeRows(t, raw)[0]; r.ErrorCount != 1 {
		t.Fatalf("errorCount = %d, want 1", r.ErrorCount)
	}
}

func TestVerdict_RendersAsANumber(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withVerdict)
	addVerdictRequest(b, reg, map[string]string{"method": "GET", "name": "n", "status": "200"}, time.Unix(1700000000, 0), verdict(0))
	if raw := string(b.drainAll()[0]); !strings.Contains(raw, `"failed_verdict":0`) {
		t.Fatalf("row lacks failed_verdict:0: %s", raw)
	}
}

// A requeued row (failed insert) keeps its verdict.
func TestVerdict_SurvivesRequeue(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withVerdict)
	addVerdictRequest(b, reg, map[string]string{"method": "GET", "name": "n", "status": "200"}, time.Unix(1700000000, 0), verdict(1))
	b.requeue(b.drainAll())
	rows := decodeOpRows(t, b.drainAll())
	if len(rows) != 1 || rows[0].FailedVerdict != 1 {
		t.Fatalf("rows after requeue = %+v, want failedVerdict 1", rows)
	}
}

// WebSocket rows never carry a verdict.
func TestVerdict_WebSocketRowsHaveNoVerdict(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withVerdict)
	b.add(mkSample(reg, "ws_sessions", wsTags("101", "wss://x"), time.Unix(1700000000, 0), 1))
	if rows := decodeOpRows(t, b.drainAll()); len(rows) != 1 || rows[0].FailedVerdict != -1 {
		t.Fatalf("rows = %+v, want failedVerdict -1", rows)
	}
}
