package loadtoolkit

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.k6.io/k6/v2/errext"
	"go.k6.io/k6/v2/errext/exitcodes"
	"go.k6.io/k6/v2/lib"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

func outputParams() output.Params {
	tags := metrics.DefaultSystemTagSet
	return output.Params{ScriptOptions: lib.Options{SystemTags: &tags}}
}

// recorder is an endpoint that records each POST body and can fail on demand.
type recorder struct {
	mu     sync.Mutex
	bodies map[string][]string // table -> bodies
	fail   func(table string, n int) bool
	srv    *httptest.Server
}

func newRecorder(t *testing.T) *recorder {
	r := &recorder{bodies: map[string][]string{}}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		query := req.URL.Query().Get("query")
		table := strings.Fields(query)[2]
		r.mu.Lock()
		n := len(r.bodies[table])
		failing := r.fail != nil && r.fail(table, n)
		if !failing {
			r.bodies[table] = append(r.bodies[table], string(body))
		}
		r.mu.Unlock()
		if failing {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *recorder) rows(table string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, b := range r.bodies[table] {
		n += strings.Count(b, "\n")
	}
	return n
}

func (r *recorder) posts(table string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies[table])
}

func testSink(t *testing.T, endpoint string, mutate func(*SinkConfig)) (*sink, string) {
	t.Helper()
	cfg := testSinkConfig()
	cfg.Transport.Endpoint = endpoint
	cfg.Transport.Retry = RetryConfig{Max: 1}
	dir := t.TempDir()
	cfg.Markers = &MarkersConfig{
		Dir: dir, TotalRows: "total", ScriptData: "data", PushAttempted: "pushed", Iteration: "iter",
		DataMetricPrefixes: []string{"http_req", "ws_"},
	}
	if mutate != nil {
		mutate(cfg)
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("config: %v", err)
	}
	return newSink(cfg, nil, outputParams().ScriptOptions.SystemTags), dir
}

func samples(s ...metrics.Sample) []metrics.SampleContainer {
	return []metrics.SampleContainer{metrics.Samples(s)}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestSink_EndToEndRowsAndMarkers(t *testing.T) {
	rec := newRecorder(t)
	s, dir := testSink(t, rec.srv.URL, nil)
	for _, name := range []string{"data", "pushed", "iter"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("stale"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"data", "pushed", "iter"} {
		if exists(filepath.Join(dir, name)) {
			t.Fatalf("Start left the stale %s marker", name)
		}
	}
	if readFile(t, filepath.Join(dir, "total")) != "0" {
		t.Fatal("Start must initialise the total rows file")
	}

	reg := metrics.NewRegistry()
	at := time.Unix(1700000000, 0)
	s.AddMetricSamples(samples(mkSample(reg, "iterations", nil, at, 1), mkSample(reg, "iterations", nil, at, 1)))
	if exists(filepath.Join(dir, "data")) || exists(filepath.Join(dir, "iter")) {
		t.Fatal("iterations alone are not script data and two are below the threshold")
	}
	s.AddMetricSamples(samples(
		mkSample(reg, "iterations", nil, at, 1),
		mkSample(reg, "http_reqs", reqTags("1"), at, 1),
		mkSample(reg, "checks", map[string]string{"check": "ok", "vu": "1"}, at, 1),
	))
	if readFile(t, filepath.Join(dir, "data")) != "1" || readFile(t, filepath.Join(dir, "iter")) != "1" {
		t.Fatal("data and iteration markers must be written")
	}
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	if rec.rows("requests") != 1 || rec.rows("checks") != 1 {
		t.Fatalf("rows: requests=%d checks=%d", rec.rows("requests"), rec.rows("checks"))
	}
	if !strings.Contains(rec.bodies["checks"][0], `"label":"req","scenario":"","status":"200"`) {
		t.Fatalf("check not correlated to its request: %s", rec.bodies["checks"][0])
	}
	if readFile(t, filepath.Join(dir, "total")) != "1" || readFile(t, filepath.Join(dir, "pushed")) != "1" {
		t.Fatal("total rows and push marker must reflect the insert")
	}
}

func TestSink_InsertURL(t *testing.T) {
	tr := TransportConfig{Endpoint: "http://h:1/", Database: "db", Table: "t", AsyncInsert: true}
	want := "http://h:1/?query=INSERT+INTO+db.t+FORMAT+JSONEachRow&async_insert=1&wait_for_async_insert=1"
	if got := insertURL(tr, "t"); got != want {
		t.Fatalf("insertURL = %s", got)
	}
	tr.Database, tr.AsyncInsert = "", false
	if got := insertURL(tr, "t"); got != "http://h:1/?query=INSERT+INTO+t+FORMAT+JSONEachRow" {
		t.Fatalf("insertURL without database = %s", got)
	}
}

// Rows go out in chunks; the first failed chunk stops the flush and the rest
// are requeued rather than retried one by one.
func TestSink_ChunksAndFailsFast(t *testing.T) {
	rec := newRecorder(t)
	rec.fail = func(table string, n int) bool { return table == "requests" && n == 1 }
	s, _ := testSink(t, rec.srv.URL, func(c *SinkConfig) { c.Transport.MaxInsertRows = 2 })

	reg := metrics.NewRegistry()
	for i := 0; i < 5; i++ {
		s.buffer.add(mkSample(reg, "http_reqs", reqTags(string(rune('a'+i))), time.Unix(1700000000, 0), 1))
	}
	if err := s.insertRows(s.buffer.drainAll()); err == nil {
		t.Fatal("expected the second chunk to fail")
	}
	if rec.posts("requests") != 1 || rec.rows("requests") != 2 {
		t.Fatalf("delivered %d posts / %d rows, want 1 / 2", rec.posts("requests"), rec.rows("requests"))
	}
	if got := s.buffer.pending(); got != 3 {
		t.Fatalf("requeued %d rows, want 3 (failed chunk + remainder)", got)
	}
	if s.totalRows.Load() != 2 {
		t.Fatalf("totalRows = %d, want 2", s.totalRows.Load())
	}
}

// An assertion push marks push_attempted only when no request rows are settling.
func TestSink_AssertionPushMarkerWaitsForRequestRows(t *testing.T) {
	rec := newRecorder(t)
	s, dir := testSink(t, rec.srv.URL, nil)
	reg := metrics.NewRegistry()
	at := time.Unix(1700000000, 0)
	s.AddMetricSamples(samples(mkSample(reg, "http_reqs", reqTags("1"), at, 1), mkSample(reg, "checks", map[string]string{"check": "c"}, at, 1)))
	if err := s.insertAssertionRows(s.assertions.drain()); err != nil {
		t.Fatal(err)
	}
	if exists(filepath.Join(dir, "pushed")) {
		t.Fatal("assertion push marked while a request row was pending")
	}
	s.buffer.drainAll()
	s.AddMetricSamples(samples(mkSample(reg, "checks", map[string]string{"check": "c"}, at, 1)))
	if err := s.insertAssertionRows(s.assertions.drain()); err != nil {
		t.Fatal(err)
	}
	if !exists(filepath.Join(dir, "pushed")) {
		t.Fatal("assertion-only push must mark push_attempted")
	}
}

func TestSink_FailedAssertionInsertIsRequeued(t *testing.T) {
	rec := newRecorder(t)
	rec.fail = func(string, int) bool { return true }
	s, _ := testSink(t, rec.srv.URL, nil)
	reg := metrics.NewRegistry()
	s.AddMetricSamples(samples(mkSample(reg, "checks", map[string]string{"check": "c"}, time.Unix(1700000000, 0), 1)))
	if err := s.insertAssertionRows(s.assertions.drain()); err == nil {
		t.Fatal("expected failure")
	}
	if got := len(s.assertions.drain()); got != 1 {
		t.Fatalf("requeued %d assertion rows, want 1", got)
	}
}

func TestSink_ChecksWithoutAssertionTableAreDropped(t *testing.T) {
	rec := newRecorder(t)
	s, _ := testSink(t, rec.srv.URL, func(c *SinkConfig) {
		c.Transport.AssertionTable = ""
		c.AssertionColumns = nil
	})
	reg := metrics.NewRegistry()
	s.AddMetricSamples(samples(mkSample(reg, "checks", map[string]string{"check": "c"}, time.Unix(1700000000, 0), 1)))
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	if rec.posts("requests") != 0 || rec.posts("checks") != 0 {
		t.Fatal("a check must never become a request row")
	}
}

// A check the tag filter fabricated reaches neither the markers nor the rows.
func TestSink_FilteredChecksAreDroppedBeforeMarkers(t *testing.T) {
	rec := newRecorder(t)
	cfg := testSinkConfig()
	cfg.Transport.Endpoint = rec.srv.URL
	cfg.CheckFilter = true
	dir := t.TempDir()
	cfg.Markers = &MarkersConfig{Dir: dir, ScriptData: "data", DataMetricPrefixes: []string{"http_req"}}
	s := newSink(cfg, &TagFilterConfig{Include: `{"group":["Keep"]}`}, groupEnabled())

	reg := metrics.NewRegistry()
	s.AddMetricSamples(samples(mkSample(reg, "checks", map[string]string{"check": "c", "group": "::Other"}, time.Unix(1700000000, 0), 1)))
	if exists(filepath.Join(dir, "data")) || len(s.assertions.drain()) != 0 {
		t.Fatal("a filtered check must not mark data or become an assertion")
	}
	s.AddMetricSamples(samples(mkSample(reg, "checks", map[string]string{"check": "c", "group": "::Keep"}, time.Unix(1700000000, 0), 1)))
	if !exists(filepath.Join(dir, "data")) || len(s.assertions.drain()) != 1 {
		t.Fatal("a selected check must be kept")
	}
}

func TestSink_AbortsOnCardinality(t *testing.T) {
	s, _ := testSink(t, "http://127.0.0.1:1", func(c *SinkConfig) {
		c.Cardinality = &CardinalityConfig{MaxTimeSeries: 20, Message: "too many: {count} > {limit}"}
	})
	var mu sync.Mutex
	calls := 0
	var gotErr error
	s.SetTestRunStopCallback(func(err error) {
		mu.Lock()
		calls++
		gotErr = err
		mu.Unlock()
	})
	reg := metrics.NewRegistry()
	for i := 0; i < 40; i++ {
		s.AddMetricSamples(samples(distinctSeriesSample(reg, i, time.Unix(1700000000, 0))))
	}
	if calls != 1 || gotErr == nil {
		t.Fatalf("stop callback called %d times (err %v), want once", calls, gotErr)
	}
	if gotErr.Error() != "too many: 21 > 20" {
		t.Fatalf("message = %q", gotErr.Error())
	}
	var ec errext.HasExitCode
	if !errors.As(gotErr, &ec) || ec.ExitCode() != exitcodes.ExternalAbort {
		t.Fatalf("exit code missing or wrong: %v", gotErr)
	}
	var ar errext.HasAbortReason
	if !errors.As(gotErr, &ar) || ar.AbortReason() != errext.AbortedByOutput {
		t.Fatalf("abort reason missing or wrong: %v", gotErr)
	}
}

func TestSink_CardinalityCustomExitCode(t *testing.T) {
	s, _ := testSink(t, "http://127.0.0.1:1", func(c *SinkConfig) {
		c.Cardinality = &CardinalityConfig{MaxTimeSeries: 1, ExitCode: 42}
	})
	var gotErr error
	s.SetTestRunStopCallback(func(err error) { gotErr = err })
	reg := metrics.NewRegistry()
	for i := 0; i < 3; i++ {
		s.AddMetricSamples(samples(distinctSeriesSample(reg, i, time.Unix(1700000000, 0))))
	}
	var ec errext.HasExitCode
	if !errors.As(gotErr, &ec) || ec.ExitCode() != 42 {
		t.Fatalf("exit code = %v", gotErr)
	}
}

func TestSink_NoAbortWhenGuardDisabledOrAbsent(t *testing.T) {
	for _, card := range []*CardinalityConfig{nil, {MaxTimeSeries: 0}} {
		s, _ := testSink(t, "http://127.0.0.1:1", func(c *SinkConfig) { c.Cardinality = card })
		called := false
		s.SetTestRunStopCallback(func(error) { called = true })
		reg := metrics.NewRegistry()
		for i := 0; i < 500; i++ {
			s.AddMetricSamples(samples(distinctSeriesSample(reg, i, time.Unix(1700000000, 0))))
		}
		if called {
			t.Fatalf("guard %+v aborted the run", card)
		}
	}
}

// Without a stop callback the guard logs and does not panic.
func TestSink_AbortWithoutCallback(t *testing.T) {
	s, _ := testSink(t, "http://127.0.0.1:1", func(c *SinkConfig) { c.Cardinality = &CardinalityConfig{MaxTimeSeries: 1} })
	reg := metrics.NewRegistry()
	for i := 0; i < 3; i++ {
		s.AddMetricSamples(samples(distinctSeriesSample(reg, i, time.Unix(1700000000, 0))))
	}
}

func TestSink_NoMarkersSection(t *testing.T) {
	rec := newRecorder(t)
	s, _ := testSink(t, rec.srv.URL, func(c *SinkConfig) { c.Markers = nil })
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	reg := metrics.NewRegistry()
	s.AddMetricSamples(samples(mkSample(reg, "http_reqs", reqTags("1"), time.Unix(1700000000, 0), 1), mkSample(reg, "iterations", nil, time.Unix(1700000000, 0), 1)))
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	if rec.rows("requests") != 1 {
		t.Fatal("rows must still be written without markers")
	}
}

func TestRetryDelay(t *testing.T) {
	cases := map[int]time.Duration{0: 500 * time.Millisecond, 1: 500 * time.Millisecond, 2: time.Second, 3: 2 * time.Second, 5: 5 * time.Second}
	for attempt, want := range cases {
		if got := retryDelay(attempt, 500*time.Millisecond, 5*time.Second); got != want {
			t.Errorf("retryDelay(%d) = %s, want %s", attempt, got, want)
		}
	}
}
