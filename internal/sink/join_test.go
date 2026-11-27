package sink

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"go.k6.io/k6/v2/metrics"
)

func TestJoin_ResultJoinsItsRequestRowByID(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	at := time.Unix(1700000000, 0)
	addOpRequest(b, reg, "GetCart", at, map[string]string{testBytesID: "1-1", opNameKey: "GetCart"})
	addOpRequest(b, reg, "Broken", at.Add(time.Microsecond), map[string]string{testBytesID: "1-2", opNameKey: "BrokenQuery"})
	b.add(opResult(reg, at, map[string]string{testBytesID: "1-1", opStateKey: "1"}))
	b.add(opResult(reg, at, map[string]string{testBytesID: "1-2", opStateKey: "2", opCodeKey: "INTERNAL_SERVER_ERROR", opCodeSourceKey: "1"}))

	rows := decodeOpRows(t, b.drainAll())
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2: a join sample never forms a row", len(rows))
	}
	clean := opRowByName(t, rows, "POST GetCart")
	broken := opRowByName(t, rows, "POST Broken")
	if clean.OpName != "GetCart" || clean.OpState != 1 || clean.OpCode != "" {
		t.Fatalf("clean row = %+v", clean)
	}
	if broken.OpName != "BrokenQuery" || broken.OpState != 2 || broken.OpCode != "INTERNAL_SERVER_ERROR" || broken.OpCodeSource != 1 {
		t.Fatalf("errors row = %+v", broken)
	}
	if broken.ErrorCount != 0 {
		t.Fatalf("errorCount = %d, want 0: a 200 with result errors stays an HTTP success", broken.ErrorCount)
	}
	if got := b.unjoinedCount(); got != 0 {
		t.Fatalf("unjoined = %d, want 0", got)
	}
}

// A result with no row is counted, never a row of its own; its value is the
// count, clamped to 1..1e6 with anything else counting as one.
func TestJoin_ResultWithoutARowIsUnjoined(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	at := time.Unix(1700000000, 0)
	b.add(opResult(reg, at, map[string]string{opStateKey: "3"}))
	b.add(withMeta(mkSample(reg, opResultMetric, map[string]string{"scenario": "default"}, at, 3), nil)) // one batch, three entries
	b.add(opResult(reg, at, map[string]string{testBytesID: "9-9", opStateKey: "1"}))
	if len(b.entries) != 0 {
		t.Fatalf("results created %d entries", len(b.entries))
	}
	if got := b.unjoinedCount(); got != 5 {
		t.Fatalf("unjoined = %d, want 5", got)
	}
	for _, v := range []float64{math.NaN(), math.Inf(1), 1e18, 0, -4} {
		b.add(withMeta(mkSample(reg, opResultMetric, map[string]string{"scenario": "default"}, at, v), nil))
	}
	if got := b.unjoinedCount(); got != 10 {
		t.Fatalf("unjoined = %d, want 10", got)
	}
}

// A request whose result never arrives (no id) keeps its operation and state 0.
func TestJoin_OperationWithoutResultKeepsStateZero(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	addOpRequest(b, reg, "q", time.Unix(1700000000, 0), map[string]string{opNameKey: "GetUser"})
	rows := decodeOpRows(t, b.drainAll())
	if rows[0].OpName != "GetUser" || rows[0].OpState != 0 {
		t.Fatalf("row = %+v", rows[0])
	}
}

func TestJoin_CodesCapFoldsIntoOverflow(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	at := time.Unix(1700000000, 0)
	for i := 0; i <= opMaxCodes; i++ {
		id := fmt.Sprintf("1-%d", i)
		addOpRequest(b, reg, fmt.Sprintf("r%d", i), at.Add(time.Duration(i)), map[string]string{testBytesID: id, opNameKey: "Op"})
		b.add(opResult(reg, at, map[string]string{testBytesID: id, opStateKey: "2", opCodeKey: fmt.Sprintf("CODE_%d", i), opCodeSourceKey: "1"}))
	}
	folded := 0
	for _, r := range decodeOpRows(t, b.drainAll()) {
		if r.OpCode == otherCodes {
			folded++
		}
	}
	if folded != 1 {
		t.Fatalf("folded codes = %d, want 1 (limit %d)", folded, opMaxCodes)
	}
}

// Codes in the listed partition have their own budget: variable codes in one
// partition cannot push the other partition's codes into the overflow.
func TestJoin_PartitionedCodesDoNotFoldOtherCodes(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	at := time.Unix(1700000000, 0)
	for i := 0; i <= opMaxCodes; i++ {
		id := fmt.Sprintf("1-%d", i)
		addOpRequest(b, reg, fmt.Sprintf("r%d", i), at.Add(time.Duration(i)), map[string]string{testBytesID: id, opNameKey: "Op"})
		b.add(opResult(reg, at, map[string]string{testBytesID: id, opStateKey: "2", opCodeKey: fmt.Sprintf("msg %d", i), opCodeSourceKey: "2"}))
	}
	addOpRequest(b, reg, "real", at.Add(time.Second), map[string]string{testBytesID: "1-real", opNameKey: "Op"})
	b.add(opResult(reg, at, map[string]string{testBytesID: "1-real", opStateKey: "2", opCodeKey: "UNAUTHENTICATED", opCodeSourceKey: "1"}))
	folded, kept := 0, false
	for _, r := range decodeOpRows(t, b.drainAll()) {
		if r.OpCode == otherCodes {
			folded++
		}
		if r.OpCode == "UNAUTHENTICATED" && r.OpCodeSource == 1 {
			kept = true
		}
	}
	if folded != 1 || !kept {
		t.Fatalf("folded = %d, other partition kept = %v; want 1 folded and UNAUTHENTICATED kept", folded, kept)
	}
}

func TestJoin_JoinedColumnsRender(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	at := time.Unix(1700000000, 0)
	addOpRequest(b, reg, "q", at, map[string]string{testBytesID: "1-1", opNameKey: "GetUser"})
	b.add(opResult(reg, at, map[string]string{testBytesID: "1-1", opStateKey: "2", opCodeKey: "UNAUTHENTICATED", opCodeSourceKey: "1"}))
	raw := string(b.drainAll()[0])
	for _, want := range []string{`"op_name":"GetUser"`, `"op_state":2`, `"op_code":"UNAUTHENTICATED"`, `"op_code_source":1`} {
		if !strings.Contains(raw, want) {
			t.Fatalf("row %s lacks %s", raw, want)
		}
	}
}

// Without joinMetrics the result metric and the request metadata are ignored:
// nothing is held, counted or written.
func TestJoin_UnconfiguredRowsAreUnchanged(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, nil)
	at := time.Unix(1700000000, 0)
	addOpRequest(b, reg, "q", at, map[string]string{testBytesID: "1-1", opNameKey: "GetUser", opPendingKey: "1"})
	b.add(opResult(reg, at, map[string]string{testBytesID: "1-1", opStateKey: "2", opCodeKey: "X"}))
	rows := b.drainReady(time.Now().Add(time.Hour))
	if len(rows) != 1 {
		t.Fatalf("an unconfigured buffer must not hold rows: %d drained", len(rows))
	}
	for _, key := range []string{"op_name", "op_state", "op_code", "op_code_source", "GetUser"} {
		if strings.Contains(string(rows[0]), key) {
			t.Fatalf("unconfigured row carries %s: %s", key, rows[0])
		}
	}
	if b.unjoinedCount() != 0 {
		t.Fatal("an unconfigured buffer must not count results")
	}
}

// The request classification rides the operation onto the row; out-of-range
// reads as 0 and a row without an operation carries none.
func TestJoin_RequestClassificationIsBounded(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	at := time.Unix(1700000000, 0)
	addOpRequest(b, reg, "batch", at, map[string]string{opNameKey: "Batch: A, B", opTypeKey: "1", opShapeKey: "2", opBatchKey: "2"})
	addOpRequest(b, reg, "get", at.Add(time.Microsecond), map[string]string{opNameKey: "GetUser", opTypeKey: "2", opShapeKey: "4", opBatchKey: "9"})
	addOpRequest(b, reg, "junk", at.Add(2*time.Microsecond), map[string]string{opNameKey: "Junk", opTypeKey: "7", opShapeKey: "x", opBatchKey: "70000"})
	addOpRequest(b, reg, "rest", at.Add(3*time.Microsecond), map[string]string{opTypeKey: "1", opShapeKey: "1"})
	raw := b.drainAll()
	rows := decodeOpRows(t, raw)
	if r := opRowByName(t, rows, "POST batch"); r.OpType != 1 || r.OpShape != 2 || r.OpBatch != 2 {
		t.Fatalf("batch row = %+v", r)
	}
	if r := opRowByName(t, rows, "POST get"); r.OpType != 2 || r.OpShape != 4 || r.OpBatch != 0 {
		t.Fatalf("get row = %+v (batch size only for shape 2)", r)
	}
	if r := opRowByName(t, rows, "POST junk"); r.OpType != 0 || r.OpShape != 0 || r.OpBatch != 0 {
		t.Fatalf("out-of-range row = %+v", r)
	}
	if r := opRowByName(t, rows, "POST rest"); r.OpType != 0 || r.OpShape != 0 {
		t.Fatalf("a row without an operation carries no classification: %+v", r)
	}
	found := false
	for _, r := range raw {
		if strings.Contains(string(r), `"op_type":1,"op_shape":2,"op_batch":2`) {
			found = true
		}
	}
	if !found {
		t.Fatal("uint columns must render as JSON numbers")
	}
}

// Exempt metrics spend none of the series budget.
func TestJoin_ExemptMetricsDoNotSpendTheSeriesBudget(t *testing.T) {
	s, _ := testSink(t, "http://127.0.0.1:1", func(c *Config) {
		withOpJoin(c)
		c.Cardinality = &CardinalityConfig{MaxTimeSeries: 1, ExemptMetrics: []string{opResultMetric, opCallsMetric}}
	})
	stopped := false
	s.SetTestRunStopCallback(func(error) { stopped = true })
	reg := metrics.NewRegistry()
	at := time.Unix(1700000000, 0)
	batch := []metrics.Sample{distinctSeriesSample(reg, 1, at)}
	for i := 0; i < 5; i++ {
		batch = append(batch,
			mkSample(reg, opResultMetric, map[string]string{"scenario": fmt.Sprintf("s%d", i)}, at, 1),
			mkSample(reg, opCallsMetric, map[string]string{"scenario": fmt.Sprintf("s%d", i)}, at, 1))
	}
	s.AddMetricSamples(samples(batch...))
	if stopped {
		t.Fatal("exempt samples tripped the guard")
	}
}

// Only the listed names are exempt: a metric sharing their prefix still counts.
func TestJoin_ExemptionIsByExactName(t *testing.T) {
	s, _ := testSink(t, "http://127.0.0.1:1", func(c *Config) {
		withOpJoin(c)
		c.Cardinality = &CardinalityConfig{MaxTimeSeries: 1, ExemptMetrics: []string{opResultMetric}}
	})
	stopped := false
	s.SetTestRunStopCallback(func(error) { stopped = true })
	reg := metrics.NewRegistry()
	at := time.Unix(1700000000, 0)
	batch := []metrics.Sample{distinctSeriesSample(reg, 1, at)}
	for i := 0; i < 5; i++ {
		batch = append(batch, mkSample(reg, opResultMetric+"_custom", map[string]string{"id": fmt.Sprint(i)}, at, 1))
	}
	s.AddMetricSamples(samples(batch...))
	if !stopped {
		t.Fatal("a non-exempt metric bypassed the guard")
	}
}

func TestCardinalityGuard_ExemptMetricNeverCounts(t *testing.T) {
	reg := metrics.NewRegistry()
	g := newCardinalityGuard(2, opResultMetric)
	at := time.Unix(1700000000, 0)
	for i := 0; i < 10; i++ {
		if g.observe(mkSample(reg, opResultMetric, map[string]string{"id": fmt.Sprint(i)}, at, 1).TimeSeries) {
			t.Fatal("an exempt metric tripped the guard")
		}
	}
	if g.distinctCount() != 0 {
		t.Fatalf("distinctCount = %d, want 0", g.distinctCount())
	}
}
