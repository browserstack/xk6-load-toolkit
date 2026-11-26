package sink

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.k6.io/k6/v2/errext"
	"go.k6.io/k6/v2/errext/exitcodes"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

// Name is the value passed to `k6 run --out`.
const Name = "load-toolkit-sink"

// CheckFilter decides whether a check sample was recorded against a request the
// tag filter skipped. The tagfilter package provides it; the sink only consumes it.
type CheckFilter interface {
	// Mode names the active policy for the startup log line.
	Mode() string
	// DropsCheck reports whether a check sample with this group path is fabricated.
	DropsCheck(groupPath string, hasGroup bool) bool
}

var (
	_ output.Output          = (*sink)(nil)
	_ output.WithTestRunStop = (*sink)(nil)
)

const (
	defaultFlushInterval     = 5 * time.Second
	defaultCompleteAfter     = 2 * time.Second
	defaultStaleAfter        = time.Minute
	defaultTimeout           = 30 * time.Second
	defaultRetryMax          = 3
	defaultRetryBase         = 500 * time.Millisecond
	defaultRetryMaxDelay     = 5 * time.Second
	defaultMaxInsertRows     = 10_000
	defaultIterThreshold     = 3
	defaultCardinalityExit   = int(exitcodes.ExternalAbort)
	defaultCardinalityReason = "aborting run: generated {count} unique metric time series, exceeding the limit of {limit}."
	iterationsMetric         = "iterations"
	httpReqsMetric           = "http_reqs"
)

// vuReqContext holds one VU's most recent request label and status, so a check
// sample can be attributed to the request it followed.
type vuReqContext struct {
	label  string
	status string
}

// marker is a one-shot file written the first time a condition holds.
type marker struct {
	path    string
	written atomic.Bool
}

// mark writes "1" to the marker once. A failed write rolls back so a later call retries.
func (m *marker) mark() {
	if m == nil || m.path == "" || m.written.Load() || !m.written.CompareAndSwap(false, true) {
		return
	}
	if err := writeFile(m.path, []byte("1")); err != nil {
		logf("failed to write marker %s: %v", filepath.Base(m.path), err)
		m.written.Store(false)
	}
}

// clear removes a stale marker left by an earlier run on the same volume.
func (m *marker) clear() {
	if m == nil || m.path == "" {
		return
	}
	if err := os.Remove(m.path); err != nil && !os.IsNotExist(err) {
		logf("failed to clear stale marker %s: %v", filepath.Base(m.path), err)
	}
}

type sink struct {
	cfg               *Config
	insertEndpoint    string
	assertionEndpoint string
	flushInterval     time.Duration
	completeAfter     time.Duration
	retryMax          int
	retryBase         time.Duration
	retryMaxDelay     time.Duration
	maxInsertRows     int

	buffer     *metricBuffer
	assertions *assertionBuffer
	httpClient *http.Client
	stopCh     chan struct{}
	wg         sync.WaitGroup

	totalRows      atomic.Int64
	totalRowsPath  string
	scriptData     *marker
	pushAttempted  *marker
	iteration      *marker
	iterThreshold  int64
	iterCount      atomic.Int64
	dataPrefixes   []string
	vuContexts     sync.Map
	cardinality    *cardinalityGuard
	stopTestRun    func(error)
	abortOnce      sync.Once
	checkFilter    CheckFilter
	cardinalityCfg CardinalityConfig

	// Run summary (sink.summary). cardinalityHit is set when the guard stops
	// the run; seenByMetric sets the seen flags of a metric's samples.
	summaryPath    string
	summary        SummaryConfig
	cardinalityHit atomic.Bool
	seenFlags      []seenFlag
	seenByMetric   map[string][]*atomic.Bool
}

// seenFlag is one summary.keys.seen entry: set once a sample of its metric arrives.
type seenFlag struct {
	key  string
	flag *atomic.Bool
}

// noopSink accepts samples and writes nothing; used when the sink section is absent.
type noopSink struct{}

func (noopSink) Description() string                        { return Name + " (disabled)" }
func (noopSink) Start() error                               { return nil }
func (noopSink) Stop() error                                { return nil }
func (noopSink) AddMetricSamples([]metrics.SampleContainer) {}

// New builds the output for a validated sink section. A nil section gives an
// output that accepts samples and writes nothing. checks is consulted only when
// the section sets checkFilter; nil there means an inert policy.
func New(cfg *Config, checks CheckFilter) output.Output {
	if cfg == nil {
		logf("no sink configuration; no rows will be written")
		return noopSink{}
	}
	return newSink(cfg, checks)
}

func newSink(cfg *Config, checks CheckFilter) *sink {
	tr := cfg.Transport
	engine := newRowEngine(cfg)
	s := &sink{
		cfg:            cfg,
		insertEndpoint: insertURL(tr, tr.Table),
		flushInterval:  tr.FlushInterval.or(defaultFlushInterval),
		completeAfter:  tr.CompleteAfter.or(defaultCompleteAfter),
		retryMax:       tr.Retry.Max,
		retryBase:      tr.Retry.Base.or(defaultRetryBase),
		retryMaxDelay:  tr.Retry.MaxDelay.or(defaultRetryMaxDelay),
		maxInsertRows:  tr.MaxInsertRows,
		buffer:         newMetricBuffer(engine, cfg.Buffers.MetricMax, tr.StaleAfter.or(defaultStaleAfter)),
		assertions:     newAssertionBuffer(cfg),
		stopCh:         make(chan struct{}),
		httpClient: &http.Client{
			Timeout: tr.Timeout.or(defaultTimeout),
			Transport: &http.Transport{
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
	if s.retryMax == 0 {
		s.retryMax = defaultRetryMax
	}
	if s.maxInsertRows == 0 {
		s.maxInsertRows = defaultMaxInsertRows
	}
	if tr.AssertionTable != "" {
		s.assertionEndpoint = insertURL(tr, tr.AssertionTable)
	}
	if m := cfg.Markers; m != nil {
		s.totalRowsPath = markerPath(m.Dir, m.TotalRows)
		s.scriptData = &marker{path: markerPath(m.Dir, m.ScriptData)}
		s.pushAttempted = &marker{path: markerPath(m.Dir, m.PushAttempted)}
		s.iteration = &marker{path: markerPath(m.Dir, m.Iteration)}
		s.iterThreshold = int64(m.IterationThreshold)
		if s.iterThreshold == 0 {
			s.iterThreshold = defaultIterThreshold
		}
		s.dataPrefixes = m.DataMetricPrefixes
	}
	if c := cfg.Cardinality; c != nil {
		s.cardinalityCfg = *c
	}
	if s.cardinalityCfg.ExitCode == 0 {
		s.cardinalityCfg.ExitCode = defaultCardinalityExit
	}
	if s.cardinalityCfg.Message == "" {
		s.cardinalityCfg.Message = defaultCardinalityReason
	}
	s.cardinality = newCardinalityGuard(s.cardinalityCfg.MaxTimeSeries, s.cardinalityCfg.ExemptMetrics...)

	if sum := cfg.Summary; sum != nil {
		s.summaryPath = sum.Path
		s.summary = *sum
		s.seenByMetric = make(map[string][]*atomic.Bool, len(sum.Keys.Seen))
		for _, key := range sortedKeys(sum.Keys.Seen) {
			flag := &atomic.Bool{}
			s.seenFlags = append(s.seenFlags, seenFlag{key: key, flag: flag})
			metric := sum.Keys.Seen[key]
			s.seenByMetric[metric] = append(s.seenByMetric[metric], flag)
		}
	}

	if cfg.CheckFilter {
		s.checkFilter = checks
		// A fully filtered run reports empty, so the resolved mode is logged.
		logf("check-sample filter: mode=%s", checkFilterMode(checks))
	}
	return s
}

// checkFilterMode is the policy's mode, or "inert" when there is none.
func checkFilterMode(c CheckFilter) string {
	if c == nil {
		return "inert"
	}
	return c.Mode()
}

// insertURL builds a ClickHouse HTTP-interface insert URL.
func insertURL(tr TransportConfig, table string) string {
	target := table
	if tr.Database != "" {
		target = tr.Database + "." + table
	}
	query := fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", target)
	endpoint := strings.TrimRight(tr.Endpoint, "/") + "/?query=" + url.QueryEscape(query)
	if tr.AsyncInsert {
		endpoint += "&async_insert=1&wait_for_async_insert=1"
	}
	return endpoint
}

func markerPath(dir, name string) string {
	if name == "" {
		return ""
	}
	return filepath.Join(dir, name)
}

func (s *sink) Description() string {
	return "Streams request metrics as JSONEachRow batches"
}

// SetTestRunStopCallback satisfies output.WithTestRunStop. k6 calls it once,
// before Start and any sample.
func (s *sink) SetTestRunStopCallback(stopFunc func(error)) {
	s.stopTestRun = stopFunc
}

// fireCardinalityAbort stops the run once the time-series budget is crossed.
// Buffered rows are kept and flushed by the graceful shutdown that follows.
func (s *sink) fireCardinalityAbort() {
	s.abortOnce.Do(func() {
		msg := strings.NewReplacer(
			"{count}", strconv.FormatInt(s.cardinality.distinctCount(), 10),
			"{limit}", strconv.Itoa(s.cardinality.limit),
		).Replace(s.cardinalityCfg.Message)
		logf("%s", msg)
		if s.stopTestRun == nil {
			logf("cardinality limit hit but no test-run stop callback is registered; cannot abort run")
			return
		}
		s.cardinalityHit.Store(true)
		s.stopTestRun(errext.WithAbortReasonIfNone(
			errext.WithExitCodeIfNone(errors.New(msg), exitcodes.ExitCode(s.cardinalityCfg.ExitCode)),
			errext.AbortedByOutput,
		))
	})
}

func (s *sink) Start() error {
	if err := writeTotalRows(s.totalRowsPath, 0); err != nil {
		return fmt.Errorf("initialize total rows file: %w", err)
	}
	s.scriptData.clear()
	s.pushAttempted.clear()
	s.iteration.clear()
	// A stale summary would report an earlier run's counters.
	if s.summaryPath != "" {
		if err := os.Remove(s.summaryPath); err != nil && !os.IsNotExist(err) {
			logf("failed to clear stale summary %s: %v", filepath.Base(s.summaryPath), err)
		}
	}

	s.wg.Add(1)
	go s.runFlusher()
	return nil
}

func (s *sink) Stop() error {
	close(s.stopCh)
	s.wg.Wait()
	flushErr := s.flushAll()
	// Whatever the final flush could not deliver is lost with the process: a
	// failed final check insert, or checks never reached because the request
	// insert failed first.
	s.assertions.countUndelivered()
	s.writeSummary()
	if flushErr != nil {
		return flushErr
	}
	return writeTotalRows(s.totalRowsPath, s.totalRows.Load())
}

// summaryLine renders the configured counters as k=v;k=v and a newline:
// cardinalityLimitHit, assertionsDropped, the seen flags by key, unjoined,
// then the static pairs by key.
func (s *sink) summaryLine() string {
	k := &s.summary.Keys
	var parts []string
	if k.CardinalityLimitHit != "" {
		parts = append(parts, k.CardinalityLimitHit+"="+boolDigit(s.cardinalityHit.Load()))
	}
	if k.AssertionsDropped != "" {
		parts = append(parts, k.AssertionsDropped+"="+strconv.FormatUint(s.assertions.droppedCount(), 10))
	}
	for _, f := range s.seenFlags {
		parts = append(parts, f.key+"="+boolDigit(f.flag.Load()))
	}
	if k.Unjoined != "" {
		parts = append(parts, k.Unjoined+"="+strconv.FormatUint(s.buffer.unjoinedCount(), 10))
	}
	for _, key := range sortedKeys(s.summary.Static) {
		parts = append(parts, key+"="+s.summary.Static[key])
	}
	return strings.Join(parts, ";") + "\n"
}

// writeSummary replaces the summary file rather than writing through whatever
// sits at the path, so a planted symlink is never followed. Best effort: a
// missing file reads as no summary.
func (s *sink) writeSummary() {
	path := s.summaryPath
	if path == "" {
		return
	}
	name := filepath.Base(path)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logf("failed to replace summary %s: %v", name, err)
		return
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		logf("failed to write summary %s: %v", name, err)
		return
	}
	if _, err := f.WriteString(s.summaryLine()); err != nil {
		logf("failed to write summary %s: %v", name, err)
	}
	if err := f.Close(); err != nil {
		logf("failed to close summary %s: %v", name, err)
	}
}

func boolDigit(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func (s *sink) getVUContext(vuID string) *vuReqContext {
	if val, ok := s.vuContexts.Load(vuID); ok {
		return val.(*vuReqContext)
	}
	actual, _ := s.vuContexts.LoadOrStore(vuID, &vuReqContext{})
	return actual.(*vuReqContext)
}

func (s *sink) isDataMetric(name string) bool {
	if name == s.cfg.Correlation.CheckMetric && name != "" {
		return true
	}
	for _, prefix := range s.dataPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func (s *sink) AddMetricSamples(sampleContainers []metrics.SampleContainer) {
	checkMetric := s.cfg.Correlation.CheckMetric
	for _, container := range sampleContainers {
		for _, sample := range container.GetSamples() {
			// Every sample but an exempt metric's counts against the budget;
			// observe is true exactly once.
			if s.cardinality.observe(sample.TimeSeries) {
				s.fireCardinalityAbort()
			}

			metricName := sampleMetricName(sample)
			// A seen metric only sets its summary flags; it never becomes a row.
			if flags, ok := s.seenByMetric[metricName]; ok {
				for _, f := range flags {
					f.Store(true)
				}
				continue
			}
			tags := extractTags(sample)
			// An absent group key is not the same as an empty one.
			groupTag, hasGroupTag := tags["group"]

			// A check recorded against a request the tag filter skipped must not
			// reach the markers or the assertion rows.
			if metricName == checkMetric && s.checkFilter != nil && s.checkFilter.DropsCheck(groupTag, hasGroupTag) {
				continue
			}

			if s.isDataMetric(metricName) {
				s.scriptData.mark()
			}

			if metricName == iterationsMetric && s.iteration != nil {
				if s.iterCount.Add(1) >= s.iterThreshold {
					s.iteration.mark()
				}
			}

			vuID := tags["vu"]
			if metricName == httpReqsMetric && vuID != "" {
				ctx := s.getVUContext(vuID)
				if label := nonEmpty(tags["name"], tags["url"]); label != "" {
					ctx.label = label
				}
				if status := tags["status"]; status != "" {
					ctx.status = status
				}
			}

			if checkMetric != "" && metricName == checkMetric {
				if !s.assertions.enabled() {
					continue
				}
				var parentLabel, parentStatus string
				if vuID != "" {
					ctx := s.getVUContext(vuID)
					parentLabel, parentStatus = ctx.label, ctx.status
				}
				s.assertions.add(sample, parentLabel, parentStatus)
				continue
			}
			s.buffer.add(sample)
		}
	}
}

func (s *sink) runFlusher() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	var lastDropped uint64
	for {
		select {
		case <-ticker.C:
			if err := s.flushReady(); err != nil {
				logf("flush error: %v", err)
			}
			// Shedding under backpressure is logged whenever the count rises.
			if dropped := s.buffer.droppedCount(); dropped > lastDropped {
				logf("metric buffer overflow: %d writes shed since start (cap=%d entries); the endpoint is not keeping up", dropped, s.buffer.maxEntries)
				lastDropped = dropped
			}
		case <-s.stopCh:
			return
		}
	}
}

func (s *sink) flushReady() error {
	if err := s.insertRows(s.buffer.drainReady(time.Now().Add(-s.completeAfter))); err != nil {
		return err
	}
	return s.insertAssertionRows(s.assertions.drain())
}

func (s *sink) flushAll() error {
	if err := s.insertRows(s.buffer.drainAll()); err != nil {
		return err
	}
	return s.insertAssertionRows(s.assertions.drain())
}

// insertRows posts rows in chunks of at most maxInsertRows. It stops at the
// first failed chunk and requeues the rest: retrying every chunk would multiply
// the worst-case stall by the chunk count.
func (s *sink) insertRows(rows []row) error {
	for start := 0; start < len(rows); start += s.maxInsertRows {
		end := min(start+s.maxInsertRows, len(rows))
		if err := s.insertRowsBatch(rows[start:end]); err != nil {
			if end < len(rows) {
				s.buffer.requeue(rows[end:])
			}
			return err
		}
	}
	return nil
}

func (s *sink) insertRowsBatch(rows []row) error {
	if len(rows) == 0 {
		return nil
	}
	// Marked before the POST: it records that a push was attempted, not that it succeeded.
	s.pushAttempted.mark()

	payload := joinRows(rows)
	if err := s.postWithRetry(s.insertEndpoint, payload, "insert"); err != nil {
		s.buffer.requeue(rows)
		return err
	}
	total := s.totalRows.Add(int64(len(rows)))
	if err := writeTotalRows(s.totalRowsPath, total); err != nil {
		logf("failed to persist total rows (%d): %v", total, err)
	}
	return nil
}

func (s *sink) insertAssertionRows(rows []row) error {
	for start := 0; start < len(rows); start += s.maxInsertRows {
		end := min(start+s.maxInsertRows, len(rows))
		if err := s.insertAssertionBatch(rows[start:end]); err != nil {
			if end < len(rows) {
				s.assertions.requeue(rows[end:])
			}
			return err
		}
	}
	return nil
}

func (s *sink) insertAssertionBatch(rows []row) error {
	if len(rows) == 0 {
		return nil
	}
	// Only when no request rows are still settling: otherwise an early assertion
	// push would report a push before the first request row leaves.
	if s.buffer.pending() == 0 {
		s.pushAttempted.mark()
	}
	if err := s.postWithRetry(s.assertionEndpoint, joinRows(rows), "assertion insert"); err != nil {
		logf("assertion insert failed after %d attempts (%d rows requeued): %v", s.retryMax, len(rows), err)
		s.assertions.requeue(rows)
		return err
	}
	return nil
}

func (s *sink) postWithRetry(endpoint, payload, what string) error {
	attempts := max(s.retryMax, 1)
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if lastErr = s.post(endpoint, payload); lastErr == nil {
			return nil
		}
		if attempt < attempts {
			delay := retryDelay(attempt, s.retryBase, s.retryMaxDelay)
			logf("%s attempt %d/%d failed: %v (retrying in %s)", what, attempt, attempts, lastErr, delay)
			time.Sleep(delay)
		}
	}
	return lastErr
}

func (s *sink) post(endpoint, payload string) error {
	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("insert request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("insert request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func joinRows(rows []row) string {
	var b strings.Builder
	for _, r := range rows {
		b.Write(r)
		b.WriteByte('\n')
	}
	return b.String()
}

func writeTotalRows(path string, totalRows int64) error {
	if path == "" {
		return nil
	}
	return writeFile(path, []byte(strconv.FormatInt(totalRows, 10)))
}

func writeFile(path string, content []byte) error {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, content, 0o644)
}

func retryDelay(attempt int, baseDelay, maxDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := baseDelay * time.Duration(1<<uint(attempt-1))
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[xk6-load-toolkit] "+format+"\n", args...)
}
