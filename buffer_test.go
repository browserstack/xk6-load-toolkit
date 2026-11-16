package loadtoolkit

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"go.k6.io/k6/v2/metrics"
)

func TestURL_NameAndRealURLSeparated(t *testing.T) {
	r := renderOne(t,
		map[string]string{"method": "GET", "name": "GetUser", "url": "GetUser", "status": "200"},
		map[string]string{"fullUrl": "https://api.example.com/users/123"})
	if r.RequestName != "GET GetUser" || r.URL != "GET https://api.example.com/users/123" {
		t.Fatalf("RequestName=%q URL=%q", r.RequestName, r.URL)
	}
}

func TestURL_NameAlreadyMethodPrefixed(t *testing.T) {
	r := renderOne(t,
		map[string]string{"method": "POST", "name": "POST /api/cart (unauth)", "url": "POST /api/cart (unauth)", "status": "404"},
		map[string]string{"fullUrl": "https://shop.example.com/api/cart"})
	if r.RequestName != "POST /api/cart (unauth)" {
		t.Errorf("RequestName = %q, want no double method prefix", r.RequestName)
	}
	if r.URL != "POST https://shop.example.com/api/cart" {
		t.Errorf("URL = %q", r.URL)
	}
}

func TestURL_UnnamedRequestUsesURLTag(t *testing.T) {
	r := renderOne(t, map[string]string{"method": "GET", "url": "https://api.example.com/items/", "status": "200"}, nil)
	want := "GET https://api.example.com/items/"
	if r.URL != want || r.RequestName != want {
		t.Fatalf("URL=%q RequestName=%q, want both %q", r.URL, r.RequestName, want)
	}
}

func TestURL_NoMetadataNamedFallback(t *testing.T) {
	r := renderOne(t, map[string]string{"method": "GET", "name": "GetUser", "url": "GetUser", "status": "200"}, nil)
	if r.URL != "GET GetUser" || r.RequestName != "GET GetUser" {
		t.Fatalf("URL=%q RequestName=%q, want the name in both", r.URL, r.RequestName)
	}
}

func TestURL_LegacyTagFallback(t *testing.T) {
	r := renderOne(t, map[string]string{
		"method": "GET", "name": "Get user by id", "url": "Get user by id",
		"legacyUrl": "https://api.example.com/users/123", "status": "200",
	}, nil)
	if r.URL != "GET https://api.example.com/users/123" || r.RequestName != "GET Get user by id" {
		t.Fatalf("URL=%q RequestName=%q", r.URL, r.RequestName)
	}
}

func TestGroupColumnAndConstants(t *testing.T) {
	r := renderOne(t, map[string]string{"method": "GET", "name": "GetUser", "group": "::Checkout Flow", "status": "200"}, nil)
	if r.Group != "::Checkout Flow" {
		t.Errorf("Group = %q", r.Group)
	}
	if r.Job != "job-const" || r.Instance != "main" {
		t.Errorf("constants not applied: job=%q instance=%q", r.Job, r.Instance)
	}
	tagged := renderOne(t, map[string]string{"method": "GET", "job": "job-tag", "instance": "second"}, nil)
	if tagged.Job != "job-tag" || tagged.Instance != "second" {
		t.Errorf("a tag must win over the constant: job=%q instance=%q", tagged.Job, tagged.Instance)
	}
	if tagged.Scenario != "default" || tagged.Status != "0" || tagged.StatusMessage != "Error" || tagged.ErrorCount != 1 {
		t.Errorf("defaults: %+v", tagged)
	}
}

func TestExtractMeta(t *testing.T) {
	reg := metrics.NewRegistry()
	s := mkSample(reg, "http_reqs", reqTags("1"), time.Unix(1700000000, 0), 1)
	if got := extractMeta(s); len(got) != 0 {
		t.Errorf("extractMeta(no metadata) = %v, want empty", got)
	}
	s.Metadata = map[string]string{"fullUrl": "https://x/y"}
	if got := extractMeta(s); got["fullUrl"] != "https://x/y" {
		t.Errorf("extractMeta fullUrl = %q", got["fullUrl"])
	}
}

func TestMetricBuffer_CapShedsNewEntriesWhenFull(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 3)
	for i := 0; i < 10; i++ {
		b.add(mkSample(reg, "http_reqs", reqTags(fmt.Sprint(i)), time.Unix(1700000000, 0), 1))
	}
	if got := len(b.entries); got != 3 {
		t.Fatalf("len(entries) = %d, want 3", got)
	}
	if got := b.droppedCount(); got != 7 {
		t.Fatalf("droppedCount() = %d, want 7", got)
	}
}

// A buffered request is kept whole: updates land even at capacity.
func TestMetricBuffer_UpdatesToExistingEntriesNotShed(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 1)
	at := time.Unix(1700000000, 0)
	tags := reqTags("1")
	b.add(mkSample(reg, "http_reqs", tags, at, 1))
	b.add(mkSample(reg, "http_req_duration", tags, at, 123))
	b.add(mkSample(reg, "http_req_waiting", tags, at, 100))

	if got := b.droppedCount(); got != 0 {
		t.Fatalf("droppedCount() = %d, want 0", got)
	}
	rows := decodeRows(t, b.drainAll())
	if len(rows) != 1 || rows[0].Duration != 123 || rows[0].Latency != 100 {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestMetricBuffer_NoDropsBelowCap(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 1000)
	for i := 0; i < 500; i++ {
		b.add(mkSample(reg, "http_reqs", reqTags(fmt.Sprint(i)), time.Unix(1700000000, 0), 1))
	}
	if b.droppedCount() != 0 || len(b.entries) != 500 {
		t.Fatalf("dropped=%d entries=%d", b.droppedCount(), len(b.entries))
	}
}

func TestMetricBuffer_RequeueRespectsCap(t *testing.T) {
	b := newTestBuffer(t, 2)
	b.requeue([]row{row(`{}`), row(`{}`), row(`{}`), row(`{}`), row(`{}`)})
	if len(b.entries) != 2 || b.droppedCount() != 3 {
		t.Fatalf("entries=%d dropped=%d, want 2/3", len(b.entries), b.droppedCount())
	}
}

func TestMetricBuffer_DefaultCapWhenUnset(t *testing.T) {
	if b := newTestBuffer(t, 0); b.maxEntries != defaultMetricBufferMax {
		t.Fatalf("maxEntries = %d", b.maxEntries)
	}
	if b := newTestBuffer(t, -5); b.maxEntries != defaultMetricBufferMax {
		t.Fatalf("maxEntries = %d for a negative cap", b.maxEntries)
	}
}

func TestAssertionBuffer_AddDropsOldestOnOverflow(t *testing.T) {
	reg := metrics.NewRegistry()
	cfg := testSinkConfig()
	cfg.Buffers.AssertionMax = 10
	b := newAssertionBuffer(cfg)
	for i := 0; i < 15; i++ {
		b.add(mkSample(reg, "checks", map[string]string{"check": fmt.Sprintf("c%d", i)}, time.Unix(1700000000, 0), 1), "", "")
	}
	rows := decodeAssertions(t, b.rows)
	if len(rows) != 10 || rows[0].Name != "c5" {
		t.Fatalf("len=%d first=%q, want 10 rows starting at c5", len(rows), rows[0].Name)
	}
}

func TestAssertionBuffer_RequeueTruncatesToMax(t *testing.T) {
	b := newAssertionBuffer(testSinkConfig())
	b.requeue(make([]row, defaultAssertionBufferMax+10))
	if got := len(b.rows); got != defaultAssertionBufferMax {
		t.Fatalf("len(rows) = %d, want %d", got, defaultAssertionBufferMax)
	}
}

func TestAssertionBuffer_RowShape(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newAssertionBuffer(testSinkConfig())
	b.add(mkSample(reg, "checks", map[string]string{"check": "ok", "scenario": "s1"}, time.Unix(1700000000, 0), 0), "GET /x", "503")
	b.add(mkSample(reg, "checks", map[string]string{"check": "named", "name": "N", "status": "201", "group": "::G"}, time.Unix(1700000000, 0), 1), "GET /x", "503")
	b.add(mkSample(reg, "checks", map[string]string{"scenario": "s1"}, time.Unix(1700000000, 0), 1), "", "")
	rows := decodeAssertions(t, b.drain())
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 (an unnamed check is skipped)", len(rows))
	}
	if r := rows[0]; r.Label != "GET /x" || r.Status != "503" || r.Scenario != "s1" || !r.Failed || r.Job != "job-const" {
		t.Errorf("parent-correlated row = %+v", r)
	}
	if r := rows[1]; r.Label != "N" || r.Status != "201" || r.Scenario != "::G" || r.Failed {
		t.Errorf("tagged row = %+v", r)
	}
}

// pending counts only entries that can become rows; a request phase sample
// whose request line never arrives must not hold it above zero.
func TestMetricBuffer_PendingIgnoresEntriesThatCannotBecomeRows(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	tags := map[string]string{"name": "conn", "scenario": "default"}
	b.add(mkSample(reg, "http_req_waiting", tags, time.Unix(1700000000, 0), 12))
	if len(b.entries) == 0 {
		t.Fatal("precondition: http_req_waiting did not buffer")
	}
	if got := b.pending(); got != 0 {
		t.Fatalf("pending() = %d, want 0", got)
	}
	b.add(mkSample(reg, "http_reqs", tags, time.Unix(1700000000, 0), 1))
	if got := b.pending(); got != 1 {
		t.Fatalf("pending() = %d, want 1", got)
	}
}

// Byte counters share no tags or timestamp with their request; only the id joins them.
func TestMetricBuffer_JoinsRequestBytesByID(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	sent := time.Unix(1700000000, 0)
	meta := map[string]string{testBytesID: "3-17"}
	b.add(withMeta(mkSample(reg, "http_reqs", reqTags("3"), sent, 1), meta))
	b.add(withMeta(mkSample(reg, "http_req_duration", reqTags("3"), sent, 120), meta))
	vuOnly := map[string]string{"scenario": "default"}
	later := sent.Add(3 * time.Millisecond)
	b.add(withMeta(mkSample(reg, testBytesSent, vuOnly, later, 231), meta))
	b.add(withMeta(mkSample(reg, testBytesReceived, vuOnly, later, 4096), meta))

	rows := decodeRows(t, b.drainAll())
	if len(rows) != 1 || rows[0].BytesSent != 231 || rows[0].BytesReceived != 4096 {
		t.Fatalf("rows = %+v", rows)
	}
	if len(b.byBytesReqID) != 0 {
		t.Fatalf("byBytesReqID holds %d ids after drain", len(b.byBytesReqID))
	}
}

func TestMetricBuffer_RequestBytesStayOnTheirOwnRow(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	at := time.Unix(1700000000, 0)
	vuOnly := map[string]string{"scenario": "default"}
	for vu := 1; vu <= 2; vu++ {
		b.add(withMeta(mkSample(reg, "http_reqs", reqTags(fmt.Sprint(vu)), at, 1), map[string]string{testBytesID: fmt.Sprintf("%d-1", vu)}))
	}
	b.add(withMeta(mkSample(reg, testBytesReceived, vuOnly, at, 200), map[string]string{testBytesID: "2-1"}))
	b.add(withMeta(mkSample(reg, testBytesReceived, vuOnly, at, 100), map[string]string{testBytesID: "1-1"}))

	got := map[int64]int64{}
	for _, r := range decodeRows(t, b.drainAll()) {
		got[r.VUs] = r.BytesReceived
	}
	if got[1] != 100 || got[2] != 200 {
		t.Fatalf("bytes by VU = %v", got)
	}
}

func TestMetricBuffer_DropsUnmatchedRequestBytes(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	vuOnly := map[string]string{"scenario": "default"}
	b.add(mkSample(reg, testBytesSent, vuOnly, time.Unix(1700000000, 0), 50))
	b.add(withMeta(mkSample(reg, testBytesSent, vuOnly, time.Unix(1700000000, 0), 50), map[string]string{testBytesID: "9-9"}))
	if len(b.entries) != 0 || len(b.drainAll()) != 0 {
		t.Fatalf("unmatched byte counters buffered %d entries", len(b.entries))
	}
}

// k6's per-iteration data_sent/data_received never join a request.
func TestMetricBuffer_IgnoresIterationByteCounters(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	at := time.Unix(1700000000, 0)
	vuOnly := map[string]string{"scenario": "default"}
	b.add(mkSample(reg, "http_reqs", reqTags("1"), at, 1))
	b.add(mkSample(reg, "data_sent", vuOnly, at.Add(2*time.Second), 900))
	b.add(mkSample(reg, "data_received", vuOnly, at.Add(2*time.Second), 9000))
	if len(b.entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(b.entries))
	}
	rows := decodeRows(t, b.drainAll())
	if rows[0].BytesSent != 0 || rows[0].BytesReceived != 0 {
		t.Fatalf("iteration counters leaked onto the request row: %+v", rows[0])
	}
}

// requeue must not write through the caller's backing array: the caller hands
// back drained[end:] right after requeueing drained[start:end].
func TestAssertionBuffer_RequeueDoesNotClobberCallersTail(t *testing.T) {
	b := newAssertionBuffer(testSinkConfig())
	drained := make([]row, 6)
	for i := range drained {
		drained[i] = row(fmt.Sprintf("orig-%d", i))
	}
	b.rows = []row{row("arrived-during-retry")}
	b.requeue(drained[2:4])

	for i, want := range []string{"orig-4", "orig-5"} {
		if got := string(drained[4+i]); got != want {
			t.Fatalf("drained[%d] = %q, want %q", 4+i, got, want)
		}
	}
	want := []string{"orig-2", "orig-3", "arrived-during-retry"}
	if len(b.rows) != len(want) {
		t.Fatalf("len(b.rows) = %d", len(b.rows))
	}
	for i := range want {
		if string(b.rows[i]) != want[i] {
			t.Fatalf("b.rows[%d] = %q, want %q", i, b.rows[i], want[i])
		}
	}
}

// Rows are stamped at the send instant: blocked (which encloses dial and TLS)
// plus duration before the completion time.
func TestRowIsStampedAtSendTime(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	completed := time.UnixMilli(1_700_000_000_000)
	tags := reqTags("1")
	b.add(mkSample(reg, "http_reqs", tags, completed, 1))
	b.add(mkSample(reg, "http_req_blocked", tags, completed, 270))
	b.add(mkSample(reg, "http_req_connecting", tags, completed, 30))
	b.add(mkSample(reg, "http_req_tls_handshaking", tags, completed, 200))
	b.add(mkSample(reg, "http_req_duration", tags, completed, 30))

	rows := decodeRows(t, b.drainAll())
	if rows[0].Timestamp != completed.UnixMilli()-300 || rows[0].Duration != 30 || rows[0].Connect != 30 {
		t.Fatalf("row = %+v, want timestamp %d", rows[0], completed.UnixMilli()-300)
	}
}

func TestSendTimeRestampCanBeOff(t *testing.T) {
	cfg := testSinkConfig()
	cfg.SendTimeRestamp = false
	b := newMetricBuffer(newRowEngine(cfg), 0, time.Minute)
	reg := metrics.NewRegistry()
	completed := time.UnixMilli(1_700_000_000_000)
	b.add(mkSample(reg, "http_reqs", reqTags("1"), completed, 1))
	b.add(mkSample(reg, "http_req_duration", reqTags("1"), completed, 30))
	if rows := decodeRows(t, b.drainAll()); rows[0].Timestamp != completed.UnixMilli() {
		t.Fatalf("timestamp = %d, want the completion time", rows[0].Timestamp)
	}
}

func TestWarmConnectionShiftsByDurationOnly(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	completed := time.UnixMilli(1_700_000_000_000)
	b.add(mkSample(reg, "http_reqs", reqTags("2"), completed, 1))
	b.add(mkSample(reg, "http_req_duration", reqTags("2"), completed, 5))
	if rows := decodeRows(t, b.drainAll()); rows[0].Timestamp != completed.UnixMilli()-5 {
		t.Fatalf("timestamp = %d", rows[0].Timestamp)
	}
}

// A request aborted before blocked was recorded uses dial + TLS as a floor.
func TestAbortedRequestUsesConnectionPhasesAsFloor(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	completed := time.UnixMilli(1_700_000_000_000)
	tags := reqTags("3")
	b.add(mkSample(reg, "http_reqs", tags, completed, 1))
	b.add(mkSample(reg, "http_req_blocked", tags, completed, 0))
	b.add(mkSample(reg, "http_req_connecting", tags, completed, 30))
	b.add(mkSample(reg, "http_req_tls_handshaking", tags, completed, 200))
	b.add(mkSample(reg, "http_req_duration", tags, completed, 10))
	if rows := decodeRows(t, b.drainAll()); rows[0].Timestamp != completed.UnixMilli()-240 {
		t.Fatalf("timestamp = %d", rows[0].Timestamp)
	}
}

// A requeued row was already shifted; it must not drift on later drains.
func TestRequeuedRowIsNotReStamped(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	completed := time.UnixMilli(1_700_000_000_000)
	b.add(mkSample(reg, "http_reqs", reqTags("1"), completed, 1))
	b.add(mkSample(reg, "http_req_blocked", reqTags("1"), completed, 300))
	b.add(mkSample(reg, "http_req_duration", reqTags("1"), completed, 200))

	first := b.drainAll()
	want := completed.UnixMilli() - 500
	if decodeRows(t, first)[0].Timestamp != want {
		t.Fatal("first drain not at send time")
	}
	b.requeue(first)
	second := b.drainAll()
	b.requeue(second)
	third := decodeRows(t, b.drainAll())
	if len(third) != 1 || third[0].Timestamp != want {
		t.Fatalf("requeued row drifted: %+v", third)
	}
}

func wsTags(status, url string) map[string]string {
	return map[string]string{"status": status, "url": url, "scenario": "default", "region": "r1", "group": "g"}
}

func byRequestName(rows []testRow) map[string]testRow {
	out := map[string]testRow{}
	for _, r := range rows {
		out[r.RequestName] = r
	}
	return out
}

func TestAddWS_PerSessionMetricsBecomeRows(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	at := time.Unix(1700000000, 0)
	b.add(mkSample(reg, "ws_connecting", wsTags("101", "wss://x"), at, 120))
	b.add(mkSample(reg, "ws_session_duration", wsTags("101", "wss://x"), at.Add(2*time.Second), 2000))
	b.add(mkSample(reg, "ws_sessions", wsTags("101", "wss://x"), at, 1))
	b.add(mkSample(reg, "ws_ping", wsTags("101", "wss://x"), at, 45))

	rows := decodeRows(t, b.drainAll())
	if len(rows) != 4 {
		t.Fatalf("got %d rows, want 4", len(rows))
	}
	by := byRequestName(rows)
	if r := by["ws_connecting"]; r.Connect != 120 || r.Duration != 120 || r.Status != "101" || r.StatusMessage != "OK" || r.ErrorCount != 0 {
		t.Errorf("ws_connecting = %+v", r)
	}
	if r := by["ws_session_duration"]; r.Duration != 2000 || r.Timestamp != at.Add(2*time.Second).UnixMilli() {
		t.Errorf("ws_session_duration = %+v", r)
	}
	if r := by["ws_ping"]; r.Latency != 45 || r.VUs != 0 || r.URL != "wss://x" {
		t.Errorf("ws_ping = %+v", r)
	}
}

func TestAddWS_MessageCountersSummedPerFlush(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	at := time.Unix(1700000000, 0)
	for i := 0; i < 9; i++ {
		b.add(mkSample(reg, "ws_msgs_sent", wsTags("101", "wss://x"), at, 1))
		b.add(mkSample(reg, "ws_msgs_received", wsTags("101", "wss://x"), at, 1))
	}
	rows := decodeRows(t, b.drainAll())
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	for _, r := range rows {
		if r.Duration != 9 {
			t.Errorf("%s summed = %d, want 9", r.RequestName, r.Duration)
		}
	}
}

func TestAddWS_Non101IsError(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	b.add(mkSample(reg, "ws_sessions", wsTags("403", "wss://x"), time.Unix(1700000000, 0), 1))
	if r := decodeRows(t, b.drainAll())[0]; r.StatusMessage != "Error" || r.ErrorCount != 1 {
		t.Fatalf("403 = %+v", r)
	}
}

func TestAddWS_ConnectErrorCarriesCause(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	b.add(withMeta(mkSample(reg, "ws_connect_error", map[string]string{"url": "wss://x"}, time.Unix(1700000000, 0), 1),
		map[string]string{"wsError": "dial tcp: connection refused"}))
	b.add(mkSample(reg, "ws_connect_error", map[string]string{}, time.Unix(1700000001, 0), 1))
	rows := decodeRows(t, b.drainAll())
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	if r := rows[0]; r.RequestName != "ws_connect_error" || r.Status != "0" || r.StatusMessage != "dial tcp: connection refused" {
		t.Errorf("with cause = %+v", r)
	}
	if r := rows[1]; r.StatusMessage != "Error" || r.URL != "unknown" {
		t.Errorf("without cause = %+v", r)
	}
}

func TestAddWS_DrainsEachTick(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	b.add(mkSample(reg, "ws_connecting", wsTags("101", "wss://x"), time.Unix(1700000000, 0), 10))
	if rows := b.drainReady(time.Unix(0, 0)); len(rows) != 1 {
		t.Fatalf("drainReady got %d WS rows, want 1", len(rows))
	}
}

func TestAddWS_CapShedsNewRows(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 1)
	b.add(mkSample(reg, "ws_sessions", wsTags("101", "wss://x"), time.Unix(1700000000, 0), 1))
	b.add(mkSample(reg, "ws_sessions", wsTags("101", "wss://x"), time.Unix(1700000001, 0), 1))
	if b.droppedCount() != 1 {
		t.Fatalf("dropped = %d, want 1", b.droppedCount())
	}
}

// Rows render as JSON with the same escaping encoding/json gives a struct field.
func TestRowEscapingMatchesEncodingJSON(t *testing.T) {
	r := renderOne(t, map[string]string{"method": "GET", "name": "N"}, map[string]string{"fullUrl": `https://x/?a=<b>&c="d"`})
	if r.URL != `GET https://x/?a=<b>&c="d"` {
		t.Fatalf("URL = %q", r.URL)
	}
	b := newTestBuffer(t, 0)
	reg := metrics.NewRegistry()
	b.add(withMeta(mkSample(reg, "http_reqs", map[string]string{"name": "N"}, time.Unix(1700000000, 0), 1), map[string]string{"fullUrl": "<&>"}))
	raw := string(b.drainAll()[0])
	if want := "\"url\":\"GET \\u003c\\u0026\\u003e\""; !strings.Contains(raw, want) {
		t.Fatalf("row %s lacks %s", raw, want)
	}
}

// A buffered request keeps only the tags and metadata its row can read.
func TestMetricBuffer_KeepsOnlyReadableKeys(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newTestBuffer(t, 0)
	tags := reqTags("1")
	tags["job"] = "j"
	tags["custom_dimension"] = "x"
	b.add(withMeta(mkSample(reg, "http_reqs", tags, time.Unix(1700000000, 0), 1),
		map[string]string{"fullUrl": "https://x/", "unrelated": "y"}))
	for _, entry := range b.entries {
		if _, ok := entry.tags["custom_dimension"]; ok || entry.tags["job"] != "j" || entry.tags["vu"] != "1" {
			t.Fatalf("tags = %v", entry.tags)
		}
		if _, ok := entry.meta["unrelated"]; ok || entry.meta["fullUrl"] != "https://x/" {
			t.Fatalf("meta = %v", entry.meta)
		}
	}
}
