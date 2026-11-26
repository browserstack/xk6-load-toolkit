package sink

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"go.k6.io/k6/v2/metrics"
)

// Past the limit, new distinct values fold into the overflow value; a value
// already admitted stays named. The budget is spent in arrival order.
func TestBounds_DistinctCapFoldsIntoOverflow(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	at := time.Unix(1700000000, 0)
	for i := 0; i < opMaxNames+100; i++ {
		addOpRequest(b, reg, fmt.Sprintf("r%d", i), at.Add(time.Duration(i)), map[string]string{opNameKey: fmt.Sprintf("Op%d", i)})
	}
	addOpRequest(b, reg, "again", at.Add(time.Second), map[string]string{opNameKey: "Op7"})
	folded, named := 0, map[string]bool{}
	for _, r := range decodeOpRows(t, b.drainAll()) {
		if r.OpName == otherOps {
			folded++
		} else {
			named[r.OpName] = true
		}
	}
	if len(named) != opMaxNames || folded != 100 {
		t.Fatalf("named = %d, folded = %d; want %d / 100 (an admitted value stays named)", len(named), folded, opMaxNames)
	}
}

// The first value a row sees wins its distinct column.
func TestBounds_DistinctFirstValueWins(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	at := time.Unix(1700000000, 0)
	tags := map[string]string{"vu": "1", "iter": "0", "method": "POST", "name": "q", "status": "200"}
	b.add(withMeta(mkSample(reg, "http_reqs", tags, at, 1), map[string]string{opNameKey: "First"}))
	b.add(withMeta(mkSample(reg, "http_req_duration", tags, at, 5), map[string]string{opNameKey: "Second"}))
	if rows := decodeOpRows(t, b.drainAll()); rows[0].OpName != "First" {
		t.Fatalf("op_name = %q, want First", rows[0].OpName)
	}
}

func TestBounds_ValuesAreBounded(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	long := strings.Repeat("é", 300)
	addOpRequest(b, reg, "q", time.Unix(1700000000, 0), map[string]string{testBytesID: "1-1", opNameKey: long})
	b.add(opResult(reg, time.Unix(1700000000, 0), map[string]string{testBytesID: "1-1", opStateKey: "9", opCodeKey: long}))
	r := decodeOpRows(t, b.drainAll())[0]
	if n := len([]rune(r.OpName)); n != opNameRunes {
		t.Fatalf("op_name runes = %d, want %d", n, opNameRunes)
	}
	if r.OpState != opStateChecked || r.OpCode != "" {
		t.Fatalf("an invalid state on a joined row must read as the joinDefault with no code: %+v", r)
	}
}

func TestBounds_CodeIsTruncatedByRunes(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	addOpRequest(b, reg, "q", time.Unix(1700000000, 0), map[string]string{testBytesID: "1-1", opNameKey: "Op"})
	b.add(opResult(reg, time.Unix(1700000000, 0), map[string]string{testBytesID: "1-1", opStateKey: "2", opCodeKey: strings.Repeat("ж", 100)}))
	if n := len([]rune(decodeOpRows(t, b.drainAll())[0].OpCode)); n != opCodeRunes {
		t.Fatalf("op_code runes = %d, want %d", n, opCodeRunes)
	}
}

// A joined row whose result carries no state takes the column's joinDefault; a
// row no result reached does not.
func TestBounds_JoinDefaultOnlyOnJoinedRows(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	at := time.Unix(1700000000, 0)
	addOpRequest(b, reg, "joined", at, map[string]string{testBytesID: "1-1", opNameKey: "Op"})
	addOpRequest(b, reg, "plain", at.Add(time.Microsecond), map[string]string{opNameKey: "Op"})
	b.add(opResult(reg, at, map[string]string{testBytesID: "1-1"}))
	rows := decodeOpRows(t, b.drainAll())
	if got := opRowByName(t, rows, "POST joined").OpState; got != opStateChecked {
		t.Fatalf("joined op_state = %d, want %d", got, opStateChecked)
	}
	if got := opRowByName(t, rows, "POST plain").OpState; got != 0 {
		t.Fatalf("plain op_state = %d, want 0", got)
	}
}

func TestBounds_TruncateRunes(t *testing.T) {
	cases := []struct {
		in   string
		n    int
		want string
	}{
		{"abc", 0, "abc"},
		{"abc", 5, "abc"},
		{"abcdef", 3, "abc"},
		{"ééé", 2, "éé"},
		{"éé", 3, "éé"}, // 4 bytes, 2 runes
	}
	for _, c := range cases {
		if got := truncateRunes(c.in, c.n); got != c.want {
			t.Errorf("truncateRunes(%q, %d) = %q, want %q", c.in, c.n, got, c.want)
		}
	}
}

// A uint column renders a number; unparseable or out-of-range values give 0.
func TestBounds_UintColumn(t *testing.T) {
	for raw, want := range map[string]string{"4": "4", "0": "0", "9": "0", "x": "0", "-1": "0", "": "0"} {
		b := newConfiguredBuffer(t, func(c *Config) {
			c.Columns = append(c.Columns, mustColumns(`[{"name": "n", "from": "meta:n", "type": "uint", "max": 5}]`)...)
		})
		reg := metrics.NewRegistry()
		b.add(withMeta(mkSample(reg, "http_reqs", reqTags("1"), time.Unix(1700000000, 0), 1), map[string]string{"n": raw}))
		if got := string(b.drainAll()[0]); !strings.HasSuffix(got, `,"n":`+want+`}`) {
			t.Errorf("raw %q: row %s, want n:%s", raw, got, want)
		}
	}
}

// Bounds apply to check rows as well.
func TestBounds_AssertionColumns(t *testing.T) {
	cfg := testSinkConfig()
	cfg.AssertionColumns = append(cfg.AssertionColumns, mustColumns(`[{"name": "short", "from": "tag:check", "maxRunes": 3,
		"distinct": {"limit": 1, "overflow": "other"}}]`)...)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	b := newAssertionBuffer(cfg)
	reg := metrics.NewRegistry()
	for _, name := range []string{"alpha", "alphabet", "beta"} {
		b.add(mkSample(reg, "checks", map[string]string{"check": name}, time.Unix(1700000000, 0), 1), "", "")
	}
	var got []string
	for _, r := range b.drain() {
		got = append(got, string(r[strings.LastIndex(string(r), `"short":`):]))
	}
	want := []string{`"short":"alp"}`, `"short":"alp"}`, `"short":"other"}`}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("short = %v, want %v", got, want)
	}
}

func TestBounds_ColumnOptionsAreValidated(t *testing.T) {
	cases := map[string]struct {
		columns string
		want    string
	}{
		"unknown type":             {`[{"name":"x","from":"meta:a","type":"float"}]`, "unknown type"},
		"uint without max":         {`[{"name":"x","from":"meta:a","type":"uint"}]`, "max: required"},
		"min above max":            {`[{"name":"x","from":"meta:a","type":"uint","min":4,"max":3}]`, "must not exceed max"},
		"max on a string":          {`[{"name":"x","from":"meta:a","max":3}]`, "only to type"},
		"uint over numeric":        {`[{"name":"x","from":"computed:vus","type":"uint","max":3}]`, "reads string sources"},
		"maxRunes on uint":         {`[{"name":"x","from":"meta:a","type":"uint","max":3,"maxRunes":2}]`, "only to string columns"},
		"negative maxRunes":        {`[{"name":"x","from":"meta:a","maxRunes":-1}]`, "maxRunes"},
		"bound on numeric":         {`[{"name":"x","from":"computed:vus","maxRunes":2}]`, "do not apply"},
		"distinct zero limit":      {`[{"name":"x","from":"meta:a","distinct":{"limit":0,"overflow":"o"}}]`, "distinct.limit"},
		"distinct no overflow":     {`[{"name":"x","from":"meta:a","distinct":{"limit":1}}]`, "distinct.overflow"},
		"partitions without by":    {`[{"name":"x","from":"meta:a","distinct":{"limit":1,"overflow":"o","partitions":["2"]}}]`, "set together"},
		"partitionBy without list": {`[{"name":"x","from":"meta:a","distinct":{"limit":1,"overflow":"o","partitionBy":"meta:b"}}]`, "set together"},
		"bad partition source":     {`[{"name":"x","from":"meta:a","distinct":{"limit":1,"overflow":"o","partitionBy":"b","partitions":["2"]}}]`, "partitionBy"},
		"bad when source":          {`[{"name":"x","from":"meta:a","when":[{"source":"env:a"}]}]`, "when[0].source"},
		"numeric when source":      {`[{"name":"x","from":"meta:a","when":[{"source":"computed:vus"}]}]`, "string source"},
		"joinDefault out of range": {`[{"name":"x","from":"meta:a","type":"uint","max":3,"joinDefault":"7"}]`, "joinDefault"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := testSinkConfig()
			cfg.Correlation.JoinMetrics = []string{opResultMetric}
			cfg.Columns = append(cfg.Columns, mustColumns(tc.columns)...)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// ---- With none of the new configuration, rows are byte-identical ----

// The samples a configured sink would join, hold, count or bound. Unconfigured,
// they must change nothing.
func featureSamples(reg *metrics.Registry) []metrics.Sample {
	at := time.UnixMilli(1_700_000_000_000)
	tags := reqTags("1")
	meta := map[string]string{
		"fullUrl": "https://x/a", testBytesID: "1-1",
		opNameKey: "GetUser", opPendingKey: "1", opTypeKey: "1", opShapeKey: "1",
	}
	return []metrics.Sample{
		withMeta(mkSample(reg, "http_reqs", tags, at, 1), meta),
		withMeta(mkSample(reg, "http_req_duration", tags, at, 30), meta),
		mkSample(reg, opVerdictMetric, tags, at, 1),
		withMeta(mkSample(reg, opResultMetric, map[string]string{"scenario": "default"}, at, 1),
			map[string]string{testBytesID: "1-1", opStateKey: "2", opCodeKey: "X"}),
		mkSample(reg, opCallsMetric, map[string]string{"scenario": "default"}, at, 1),
		mkSample(reg, "ws_sessions", wsTags("101", "wss://x"), time.Unix(1700000000, 0), 1),
		mkSample(reg, "checks", map[string]string{"check": "ok", "vu": "1"}, at, 1),
	}
}

const (
	goldenRequestRow = `{"timestamp":1699999999970,"job":"job-const","scenario":"default","status":"200","statusMessage":"OK",` +
		`"url":"GET https://x/a","requestName":"GET req","group":"","vus":1,"bytesReceived":0,"bytesSent":0,` +
		`"latency":0,"connect":0,"duration":30,"errorCount":0,"instance":"main"}`
	goldenWSRow = `{"timestamp":1700000000000,"job":"job-const","scenario":"default","status":"101","statusMessage":"OK",` +
		`"url":"wss://x","requestName":"ws_sessions","group":"g","vus":0,"bytesReceived":0,"bytesSent":0,` +
		`"latency":0,"connect":0,"duration":1,"errorCount":0,"instance":"main"}`
	goldenCheckRow = `{"name":"ok","label":"req","scenario":"","status":"200","failed":false,"job":"job-const"}`
)

func TestUnconfigured_BufferRowsAreByteIdentical(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, nil)
	for _, s := range featureSamples(reg) {
		b.add(s)
	}
	// Nothing is held: the request row drains on the first tick.
	rows := b.drainReady(time.Now().Add(time.Hour))
	if len(rows) != 2 || string(rows[0]) != goldenRequestRow || string(rows[1]) != goldenWSRow {
		t.Fatalf("rows =\n%s\n%s\nwant\n%s\n%s", rowAt(rows, 0), rowAt(rows, 1), goldenRequestRow, goldenWSRow)
	}
	if b.unjoinedCount() != 0 || b.holdMax != 0 {
		t.Fatalf("unjoined = %d, holdMax = %s; want both zero", b.unjoinedCount(), b.holdMax)
	}
}

func TestUnconfigured_SinkPayloadsAreByteIdentical(t *testing.T) {
	rec := newRecorder(t)
	s, _ := testSink(t, rec.srv.URL, nil)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	reg := metrics.NewRegistry()
	s.AddMetricSamples(samples(featureSamples(reg)...))
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(rec.bodies["requests"], ""), goldenRequestRow+"\n"+goldenWSRow+"\n"; got != want {
		t.Fatalf("request payload =\n%s\nwant\n%s", got, want)
	}
	if got, want := strings.Join(rec.bodies["checks"], ""), goldenCheckRow+"\n"; got != want {
		t.Fatalf("check payload =\n%s\nwant\n%s", got, want)
	}
}

func rowAt(rows []row, i int) string {
	if i < len(rows) {
		return string(rows[i])
	}
	return "<none>"
}
