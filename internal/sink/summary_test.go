package sink

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.k6.io/k6/v2/metrics"
)

func checkSample(reg *metrics.Registry) metrics.Sample {
	return mkSample(reg, "checks", map[string]string{"check": "c", "name": "r", "status": "200"}, time.Unix(1700000000, 0), 1)
}

// ---- Check rows that will never be delivered ----

func TestAssertionBuffer_CountsOverflowDrops(t *testing.T) {
	reg := metrics.NewRegistry()
	cfg := testSinkConfig()
	cfg.Buffers.AssertionMax = 10
	b := newAssertionBuffer(cfg)
	for i := 0; i < 17; i++ {
		b.add(checkSample(reg), "", "")
	}
	if got := b.droppedCount(); got != 7 {
		t.Fatalf("droppedCount = %d, want 7", got)
	}
}

func TestAssertionBuffer_CountsRequeueTruncation(t *testing.T) {
	b := newAssertionBuffer(testSinkConfig())
	b.requeue(make([]row, defaultAssertionBufferMax+3))
	if got := b.droppedCount(); got != 3 {
		t.Fatalf("droppedCount = %d, want 3", got)
	}
}

func TestAssertionBuffer_CountUndeliveredEmptiesTheBuffer(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newAssertionBuffer(testSinkConfig())
	b.add(checkSample(reg), "", "")
	b.add(checkSample(reg), "", "")
	b.countUndelivered()
	if got := b.droppedCount(); got != 2 {
		t.Fatalf("droppedCount = %d, want 2", got)
	}
	if len(b.drain()) != 0 {
		t.Fatal("buffer not empty after countUndelivered")
	}
}

// ---- The summary file ----

func summaryPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "summary")
}

// baseSummary reports the two counters every sink has.
func baseSummary(path string) *SummaryConfig {
	return &SummaryConfig{Path: path, Keys: SummaryKeys{CardinalityLimitHit: "cardinalityLimitHit", AssertionsDropped: "assertionsDropped"}}
}

// opSummary adds the seen flag, the unjoined counter and a static pair.
func opSummary(path string) *SummaryConfig {
	sum := baseSummary(path)
	sum.Keys.Unjoined = "unjoined"
	sum.Keys.Seen = map[string]string{"callsSeen": opCallsMetric}
	return sum
}

// The request insert fails, so the final flush never reaches the checks: every
// buffered check counts as dropped, and the summary is written although Stop
// returns the error.
func TestSummary_WrittenWhenTheFinalFlushFails(t *testing.T) {
	rec := newRecorder(t)
	rec.fail = func(string, int) bool { return true }
	path := summaryPath(t)
	s, _ := testSink(t, rec.srv.URL, func(c *Config) { c.Summary = baseSummary(path) })

	reg := metrics.NewRegistry()
	s.AddMetricSamples(samples(mkSample(reg, "http_reqs", reqTags("1"), time.Unix(1700000000, 0), 1), checkSample(reg), checkSample(reg)))
	if err := s.Stop(); err == nil {
		t.Fatal("Stop() = nil, want the insert error")
	}
	if got := readFile(t, path); got != "cardinalityLimitHit=0;assertionsDropped=2\n" {
		t.Fatalf("summary = %q", got)
	}
}

func TestSummary_CarriesGuardHitAndSeenFlag(t *testing.T) {
	rec := newRecorder(t)
	path := summaryPath(t)
	s, _ := testSink(t, rec.srv.URL, func(c *Config) {
		withOpJoin(c)
		c.Cardinality = &CardinalityConfig{MaxTimeSeries: 1, ExemptMetrics: []string{opResultMetric, opCallsMetric}}
		c.Summary = opSummary(path)
	})
	s.SetTestRunStopCallback(func(error) {})

	reg := metrics.NewRegistry()
	at := time.Unix(1700000000, 0)
	s.AddMetricSamples(samples(
		mkSample(reg, opCallsMetric, map[string]string{"scenario": "default"}, at, 1),
		distinctSeriesSample(reg, 1, at),
		distinctSeriesSample(reg, 2, at),
	))
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	if got := readFile(t, path); got != "cardinalityLimitHit=1;assertionsDropped=0;callsSeen=1;unjoined=0\n" {
		t.Fatalf("summary = %q", got)
	}
}

// The guard is reported hit only when it actually stopped the run.
func TestSummary_GuardHitNeedsAStopCallback(t *testing.T) {
	path := summaryPath(t)
	s, _ := testSink(t, "http://127.0.0.1:1", func(c *Config) {
		c.Cardinality = &CardinalityConfig{MaxTimeSeries: 1}
		c.Summary = baseSummary(path)
	})
	reg := metrics.NewRegistry()
	for i := 0; i < 3; i++ {
		s.AddMetricSamples(samples(distinctSeriesSample(reg, i, time.Unix(1700000000, 0))))
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	if got := readFile(t, path); got != "cardinalityLimitHit=0;assertionsDropped=0\n" {
		t.Fatalf("summary = %q", got)
	}
}

// The unjoined counter is always written when configured (zero included);
// static pairs follow it.
func TestSummary_CarriesUnjoinedAndStaticPairs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		static map[string]string
		want   string
	}{
		{"no static", nil, "cardinalityLimitHit=0;assertionsDropped=0;callsSeen=0;unjoined=2\n"},
		{"static", map[string]string{"sampleN": "4"}, "cardinalityLimitHit=0;assertionsDropped=0;callsSeen=0;unjoined=2;sampleN=4\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecorder(t)
			path := summaryPath(t)
			s, _ := testSink(t, rec.srv.URL, func(c *Config) {
				withOpJoin(c)
				c.Summary = opSummary(path)
				c.Summary.Static = tc.static
			})
			reg := metrics.NewRegistry()
			s.AddMetricSamples(samples(
				opResult(reg, time.Unix(1700000000, 0), map[string]string{opStateKey: "3"}),
				opResult(reg, time.Unix(1700000000, 0), map[string]string{opStateKey: "3"}),
			))
			if err := s.Stop(); err != nil {
				t.Fatalf("Stop() = %v", err)
			}
			if got := readFile(t, path); got != tc.want {
				t.Fatalf("summary = %q, want %q", got, tc.want)
			}
		})
	}
}

// Without a seen key the metric is not special, and it never forms a row.
func TestSummary_UnlistedMetricIsNotReported(t *testing.T) {
	rec := newRecorder(t)
	path := summaryPath(t)
	s, _ := testSink(t, rec.srv.URL, func(c *Config) { c.Summary = baseSummary(path) })
	reg := metrics.NewRegistry()
	s.AddMetricSamples(samples(mkSample(reg, opCallsMetric, map[string]string{"scenario": "default"}, time.Unix(1700000000, 0), 1)))
	if s.buffer.pending() != 0 {
		t.Fatal("the metric formed a buffered row")
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	if got := readFile(t, path); got != "cardinalityLimitHit=0;assertionsDropped=0\n" {
		t.Fatalf("summary = %q", got)
	}
}

// A seen metric sets its flag and never reaches the rows or the markers.
func TestSummary_SeenMetricNeverBecomesARow(t *testing.T) {
	rec := newRecorder(t)
	path := summaryPath(t)
	s, dir := testSink(t, rec.srv.URL, func(c *Config) {
		c.Summary = baseSummary(path)
		c.Summary.Keys.Seen = map[string]string{"reqSeen": "http_req_seen"}
	})
	reg := metrics.NewRegistry()
	s.AddMetricSamples(samples(mkSample(reg, "http_req_seen", reqTags("1"), time.Unix(1700000000, 0), 1)))
	if exists(filepath.Join(dir, "data")) {
		t.Fatal("a seen metric marked script data")
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	if rec.posts("requests") != 0 {
		t.Fatal("a seen metric reached the rows")
	}
	if got := readFile(t, path); got != "cardinalityLimitHit=0;assertionsDropped=0;reqSeen=1\n" {
		t.Fatalf("summary = %q", got)
	}
}

// A planted symlink at the summary path is replaced, not written through.
func TestSummary_ReplacesASymlinkInsteadOfWritingThroughIt(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "victim")
	if err := os.WriteFile(target, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "summary")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	s, _ := testSink(t, "http://127.0.0.1:1", func(c *Config) { c.Summary = baseSummary(path) })
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	if got := readFile(t, target); got != "keep" {
		t.Fatalf("symlink target overwritten: %q", got)
	}
	if fi, err := os.Lstat(path); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("summary is still a symlink (err=%v)", err)
	}
}

func TestSummary_StartClearsAStaleFile(t *testing.T) {
	path := summaryPath(t)
	if err := os.WriteFile(path, []byte("cardinalityLimitHit=1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, _ := testSink(t, "http://127.0.0.1:1", func(c *Config) { c.Summary = baseSummary(path) })
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if exists(path) {
		t.Fatal("Start left the stale summary")
	}
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); got != "cardinalityLimitHit=0;assertionsDropped=0\n" {
		t.Fatalf("summary = %q", got)
	}
}

// Without a summary section no file is written and Stop behaves as before.
func TestSummary_AbsentWritesNothing(t *testing.T) {
	s, dir := testSink(t, "http://127.0.0.1:1", nil)
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "total" {
			t.Fatalf("unexpected file %s", e.Name())
		}
	}
}

func TestSummary_ConfigIsValidated(t *testing.T) {
	abs := filepath.Join(string(filepath.Separator), "tmp", "summary")
	cases := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"relative path": {func(c *Config) { c.Summary = baseSummary("logs/summary") }, "absolute path"},
		"empty path":    {func(c *Config) { c.Summary = baseSummary("") }, "absolute path"},
		"no keys":       {func(c *Config) { c.Summary = &SummaryConfig{Path: abs} }, "at least one key"},
		"bad key": {func(c *Config) {
			c.Summary = baseSummary(abs)
			c.Summary.Keys.AssertionsDropped = "a=b"
		}, "must match"},
		"duplicate key": {func(c *Config) {
			c.Summary = baseSummary(abs)
			c.Summary.Keys.AssertionsDropped = "cardinalityLimitHit"
		}, "duplicate key"},
		"unjoined without joins": {func(c *Config) {
			c.Summary = baseSummary(abs)
			c.Summary.Keys.Unjoined = "unjoined"
		}, "requires sink.correlation.joinMetrics"},
		"seen row metric": {func(c *Config) {
			c.Summary = baseSummary(abs)
			c.Summary.Keys.Seen = map[string]string{"x": "http_reqs"}
		}, "never becomes a row"},
		"seen join metric": {func(c *Config) {
			withOpJoin(c)
			c.Summary = baseSummary(abs)
			c.Summary.Keys.Seen = map[string]string{"x": opResultMetric}
		}, "never becomes a row"},
		"static value with separator": {func(c *Config) {
			c.Summary = baseSummary(abs)
			c.Summary.Static = map[string]string{"n": "1;x=2"}
		}, "must not contain"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := testSinkConfig()
			tc.mutate(cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// The join, verdict and hold keys are validated against each other.
func TestJoinConfigIsValidated(t *testing.T) {
	cases := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"verdict is a request metric": {func(c *Config) { c.Correlation.VerdictMetric = "http_req_duration" }, "verdictMetric"},
		"verdict is the check metric": {func(c *Config) { c.Correlation.VerdictMetric = "checks" }, "the check metric"},
		"failedVerdict without verdict": {func(c *Config) {
			c.Columns = append(c.Columns, mustColumns(`[{"name":"v","from":"computed:failedVerdict"}]`)...)
		}, "requires sink.correlation.verdictMetric"},
		"failedVerdict on checks": {func(c *Config) {
			withVerdict(c)
			c.AssertionColumns = append(c.AssertionColumns, mustColumns(`[{"name":"v","from":"computed:failedVerdict"}]`)...)
		}, "unknown computed"},
		"join without request id": {func(c *Config) {
			c.Correlation.JoinMetrics = []string{opResultMetric}
			c.Correlation.BytesIDMetaKey = ""
		}, "bytesIdMetaKey"},
		"join is a WebSocket metric": {func(c *Config) { c.Correlation.JoinMetrics = []string{"ws_ping"} }, "WebSocket"},
		"join equals verdict": {func(c *Config) {
			c.Correlation.VerdictMetric = opVerdictMetric
			c.Correlation.JoinMetrics = []string{opVerdictMetric}
		}, "the verdict metric"},
		"duplicate join":     {func(c *Config) { c.Correlation.JoinMetrics = []string{"a", "a"} }, "a join metric"},
		"empty join":         {func(c *Config) { c.Correlation.JoinMetrics = []string{""} }, "must not be empty"},
		"hold without joins": {func(c *Config) { c.Hold = &HoldConfig{PendingMetaKey: "p", PendingValue: "1"} }, "sink.hold"},
		"hold without marker": {func(c *Config) {
			withOpJoin(c)
			c.Hold.PendingValue = ""
		}, "pendingValue"},
		"joinDefault without joins": {func(c *Config) {
			c.Columns = append(c.Columns, mustColumns(`[{"name":"x","from":"meta:a","joinDefault":"d"}]`)...)
		}, "joinDefault"},
		"joinDefault on checks": {func(c *Config) {
			withOpJoin(c)
			c.AssertionColumns = append(c.AssertionColumns, mustColumns(`[{"name":"x","from":"tag:a","joinDefault":"d"}]`)...)
		}, "never joined"},
		"empty exempt metric": {func(c *Config) {
			c.Cardinality = &CardinalityConfig{MaxTimeSeries: 5, ExemptMetrics: []string{""}}
		}, "exemptMetrics"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := testSinkConfig()
			tc.mutate(cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
	// Every new key together is valid.
	cfg := testSinkConfig()
	withOpJoin(cfg)
	cfg.Cardinality = &CardinalityConfig{MaxTimeSeries: 10, ExemptMetrics: []string{opResultMetric, opCallsMetric}}
	cfg.Summary = opSummary(filepath.Join(string(filepath.Separator), "tmp", "summary"))
	cfg.Summary.Static = map[string]string{"sampleN": fmt.Sprint(4)}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("full configuration rejected: %v", err)
	}
}
