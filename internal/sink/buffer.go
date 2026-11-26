package sink

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.k6.io/k6/v2/metrics"
)

// httpPhaseMetrics are the k6 HTTP metrics the row engine understands.
var httpPhaseMetrics = map[string]struct{}{
	"http_reqs":                {},
	"http_req_duration":        {},
	"http_req_waiting":         {},
	"http_req_blocked":         {},
	"http_req_connecting":      {},
	"http_req_tls_handshaking": {},
}

const (
	defaultMetricBufferMax    = 500_000
	defaultAssertionBufferMax = 100_000
	defaultHoldMax            = 30 * time.Second
	// maxUnjoinedPerSample bounds what one join sample's value adds to the
	// unjoined counter; a script can emit the metric too.
	maxUnjoinedPerSample = 1e6
)

// row is one rendered JSON object, without the trailing newline.
type row []byte

// rowEngine turns samples into rows. It is built once from the sink config.
type rowEngine struct {
	corr            *CorrelationConfig
	columns         columns
	sendTimeRestamp bool
	requestMetrics  map[string]struct{}
	wsMetrics       map[string]struct{}
	wsSummedMetrics map[string]struct{}
	// keepTags / keepMeta are the keys a request row can read; a buffered request
	// keeps only these, so its memory does not grow with the script's tag count.
	keepTags map[string]struct{}
	keepMeta map[string]struct{}
	// joinMetrics merge into a request row instead of forming one; hold, when
	// set, delays a pending row until its join arrives.
	joinMetrics map[string]struct{}
	hold        *HoldConfig
	// distinctCols are the indexes of the columns with a distinct budget.
	distinctCols []int
}

// builtinTags are the tags the computed request values read.
var builtinTags = []string{"scenario", "status", "url", "name", "method", "group", "vu"}

func newRowEngine(cfg *Config) *rowEngine {
	cols, _ := compileColumns(cfg.Columns, requestBuiltins, cfg.Constants, "sink.columns")
	keepTags := toSet(append(builtinTags, cfg.Correlation.URLLegacyTag))
	keepMeta := toSet([]string{cfg.Correlation.URLMetaKey})
	var distinctCols []int
	for i := range cols {
		for _, s := range cols[i].allSources() {
			switch {
			case s.isTag:
				keepTags[s.tag] = struct{}{}
			case s.isMeta:
				keepMeta[s.meta] = struct{}{}
			}
		}
		if cols[i].budget != nil {
			distinctCols = append(distinctCols, i)
		}
	}
	return &rowEngine{
		corr:            &cfg.Correlation,
		columns:         cols,
		sendTimeRestamp: cfg.SendTimeRestamp,
		requestMetrics:  toSet(cfg.Correlation.RequestMetrics),
		wsMetrics:       toSet(cfg.Correlation.WSMetrics),
		wsSummedMetrics: toSet(cfg.Correlation.WSSummedMetrics),
		keepTags:        keepTags,
		keepMeta:        keepMeta,
		joinMetrics:     toSet(cfg.Correlation.JoinMetrics),
		hold:            cfg.Hold,
		distinctCols:    distinctCols,
	}
}

func toSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, v := range values {
		out[v] = struct{}{}
	}
	return out
}

// pendingRequest collects every sample of one HTTP request until it is complete.
type pendingRequest struct {
	tags           map[string]string
	meta           map[string]string
	time           time.Time
	hasRequestLine bool
	lastSeen       time.Time
	duration       int64
	latency        int64
	connect        int64
	bytesSent      int64
	bytesReceived  int64
	// Unrounded phase durations, so the send-time shift rounds once.
	blockedMs float64
	connectMs float64
	tlsMs     float64
	// rendered is set on a requeued row: it is already final and is emitted as is.
	rendered row
	// bytesReqID is kept so its index entry is removed with the request.
	bytesReqID string
	// verdictSet/verdictFailed carry the verdict metric's sample, joined marks a
	// row a join sample reached, and pending a row held for its join until
	// firstSeen plus the hold maximum.
	verdictSet    bool
	verdictFailed bool
	joined        bool
	pending       bool
	firstSeen     time.Time
	// resolved holds distinct-bounded column values, fixed on first arrival.
	resolved map[int]string
}

// render produces the request's row, shifted to the send instant when enabled.
func (e *rowEngine) render(p *pendingRequest) row {
	if p.rendered != nil {
		return p.rendered
	}
	ts := p.time.UnixMilli()
	if e.sendTimeRestamp {
		// blocked spans DNS, dial and TLS, so connecting/tls are only a floor for
		// requests aborted before blocked was recorded.
		wallMs := math.Max(p.blockedMs, p.connectMs+p.tlsMs) + float64(p.duration)
		if wallMs > 0 {
			ts -= roundedInt64(wallMs)
		}
	}
	v := e.view(p)
	v.timestamp = ts
	return e.columns.appendRow(nil, &v)
}

// view is what a column can read from a buffered request, without the timestamp.
func (e *rowEngine) view(p *pendingRequest) rowView {
	return rowView{
		tags: p.tags, meta: p.meta, corr: e.corr,
		duration: p.duration, latency: p.latency, connect: p.connect,
		bytesSent: p.bytesSent, bytesReceived: p.bytesReceived,
		verdictSet: p.verdictSet, verdictFailed: p.verdictFailed,
		joined: p.joined, resolved: p.resolved,
	}
}

// resolveDistinct fixes each distinct-bounded column the first time it has a
// value, so the budget is spent in the order values arrive rather than the
// order rows drain. Caller holds the buffer lock.
func (e *rowEngine) resolveDistinct(p *pendingRequest) {
	if len(e.distinctCols) == 0 {
		return
	}
	var v rowView
	built := false
	for _, idx := range e.distinctCols {
		if _, done := p.resolved[idx]; done {
			continue
		}
		if !built {
			v, built = e.view(p), true
		}
		value, ok := e.columns[idx].resolveDistinct(&v)
		if !ok {
			continue
		}
		if p.resolved == nil {
			p.resolved = make(map[int]string, len(e.distinctCols))
		}
		p.resolved[idx] = value
	}
}

// wsSummed accumulates one chatty WebSocket counter per group key.
type wsSummed struct {
	view rowView
}

// metricBuffer holds in-flight request rows and WebSocket rows.
type metricBuffer struct {
	mu           sync.Mutex
	engine       *rowEngine
	staleAfter   time.Duration
	entries      map[string]*pendingRequest
	byBytesReqID map[string]string
	requeueSeq   uint64
	// maxEntries is set once at construction, so the flusher may read it lock-free.
	maxEntries int
	// dropped counts writes shed on overflow; guarded by mu.
	dropped uint64
	wsRows  []row
	wsSums  map[string]*wsSummed
	// holdMax bounds how long a pending row waits for its join; unjoined counts
	// join samples that found no row and held rows that expired. Both under mu.
	holdMax  time.Duration
	unjoined uint64
}

func newMetricBuffer(engine *rowEngine, maxEntries int, staleAfter time.Duration) *metricBuffer {
	if staleAfter <= 0 {
		staleAfter = time.Minute
	}
	if maxEntries <= 0 {
		maxEntries = defaultMetricBufferMax
	}
	b := &metricBuffer{
		engine:       engine,
		staleAfter:   staleAfter,
		entries:      make(map[string]*pendingRequest),
		byBytesReqID: make(map[string]string),
		maxEntries:   maxEntries,
		wsSums:       make(map[string]*wsSummed),
	}
	if h := engine.hold; h != nil {
		// A held row must leave before staleAfter would drop it.
		b.holdMax = min(h.Max.or(defaultHoldMax), staleAfter)
	}
	return b
}

func (b *metricBuffer) add(sample metrics.Sample) {
	e := b.engine
	metricName := sampleMetricName(sample)
	if _, ok := e.wsMetrics[metricName]; ok {
		b.addWS(sample, metricName, false)
		return
	}
	if _, ok := e.wsSummedMetrics[metricName]; ok {
		b.addWS(sample, metricName, true)
		return
	}

	if _, ok := e.joinMetrics[metricName]; ok {
		meta := extractMeta(sample)
		b.mu.Lock()
		b.addJoin(sample, meta)
		b.mu.Unlock()
		return
	}

	isBytes := metricName != "" && (metricName == e.corr.BytesSentMetric || metricName == e.corr.BytesReceivedMetric)
	isVerdict := metricName != "" && metricName == e.corr.VerdictMetric
	if _, ok := e.requestMetrics[metricName]; !ok && !isBytes && !isVerdict {
		return
	}

	meta := extractMeta(sample)
	if isBytes {
		b.mu.Lock()
		b.addRequestBytes(metricName, sample, meta)
		b.mu.Unlock()
		return
	}

	tags := extractTags(sample)
	correlationID := buildCorrelationID(sample, tags)
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	entry, exists := b.entries[correlationID]
	if !exists {
		// Shed a NEW request when full; updates to buffered ones always proceed,
		// so a request is kept or dropped whole.
		if len(b.entries) >= b.maxEntries {
			b.dropped++
			return
		}
		entry = &pendingRequest{tags: map[string]string{}, meta: map[string]string{}, time: sample.Time, firstSeen: now}
		b.entries[correlationID] = entry
	}
	mergeKept(entry.tags, tags, e.keepTags)
	mergeKept(entry.meta, meta, e.keepMeta)
	entry.lastSeen = now
	if e.corr.BytesIDMetaKey != "" {
		if reqID := meta[e.corr.BytesIDMetaKey]; reqID != "" && entry.bytesReqID == "" {
			entry.bytesReqID = reqID
			b.byBytesReqID[reqID] = correlationID
		}
	}

	switch metricName {
	case "http_reqs":
		entry.hasRequestLine = true
	case "http_req_duration":
		entry.duration = roundedInt64(sample.Value)
	case "http_req_waiting":
		entry.latency = roundedInt64(sample.Value)
	case "http_req_blocked":
		entry.blockedMs = sample.Value
	case "http_req_connecting":
		entry.connect = roundedInt64(sample.Value)
		entry.connectMs = sample.Value
	case "http_req_tls_handshaking":
		entry.tlsMs = sample.Value
	}
	if isVerdict {
		entry.verdictSet = true
		entry.verdictFailed = sample.Value != 0
	}
	if h := e.hold; h != nil && !entry.joined && meta[h.PendingMetaKey] == h.PendingValue {
		entry.pending = true
	}
	e.resolveDistinct(entry)
}

// entryByReqID finds a buffered request by the request id in meta; nil when the
// id is absent or the request already left the buffer. Caller holds b.mu.
func (b *metricBuffer) entryByReqID(meta map[string]string) *pendingRequest {
	key := b.engine.corr.BytesIDMetaKey
	if key == "" {
		return nil
	}
	reqID := meta[key]
	if reqID == "" {
		return nil
	}
	correlationID, ok := b.byBytesReqID[reqID]
	if !ok {
		return nil
	}
	return b.entries[correlationID]
}

// addRequestBytes sets a buffered request's byte count from a counter carrying
// its request id. A counter whose request is gone never forms a row. Caller holds b.mu.
func (b *metricBuffer) addRequestBytes(metricName string, sample metrics.Sample, meta map[string]string) {
	entry := b.entryByReqID(meta)
	if entry == nil {
		return
	}
	if metricName == b.engine.corr.BytesSentMetric {
		entry.bytesSent = roundedInt64(sample.Value)
	} else {
		entry.bytesReceived = roundedInt64(sample.Value)
	}
}

// addJoin merges a join sample's metadata into the request row with its request
// id. A join sample with no row is counted unjoined, unless it carries the hold's
// pending marker: that row was already counted when its hold expired. Caller holds b.mu.
func (b *metricBuffer) addJoin(sample metrics.Sample, meta map[string]string) {
	e := b.engine
	entry := b.entryByReqID(meta)
	if entry == nil {
		if h := e.hold; h != nil && meta[h.PendingMetaKey] == h.PendingValue {
			return
		}
		n := sample.Value
		if !(n >= 1 && n <= maxUnjoinedPerSample) { // NaN, Inf and junk count as one
			n = 1
		}
		b.unjoined += uint64(math.Round(n))
		return
	}
	entry.pending = false
	entry.joined = true
	mergeKept(entry.meta, meta, e.keepMeta)
	e.resolveDistinct(entry)
}

// expireHold releases a held row whose join never arrived: it takes the hold's
// onExpireMeta and counts as unjoined. Caller holds b.mu.
func (b *metricBuffer) expireHold(entry *pendingRequest) {
	entry.pending = false
	for k, v := range b.engine.hold.OnExpireMeta {
		entry.meta[k] = v
	}
	b.unjoined++
	b.engine.resolveDistinct(entry)
}

func (b *metricBuffer) unjoinedCount() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.unjoined
}

// removeEntry deletes a buffered request and its request-id index. Caller holds b.mu.
func (b *metricBuffer) removeEntry(id string, entry *pendingRequest) {
	delete(b.entries, id)
	if entry.bytesReqID != "" {
		delete(b.byBytesReqID, entry.bytesReqID)
	}
}

// addWS records a WebSocket sample. Per-session metrics become one row each;
// summed counters accumulate per (metric, url, status, scenario, region) until the next drain.
func (b *metricBuffer) addWS(sample metrics.Sample, metricName string, summed bool) {
	e := b.engine
	tags := extractTags(sample)
	value := roundedInt64(sample.Value)

	b.mu.Lock()
	defer b.mu.Unlock()

	if summed {
		key := wsSumKey(metricName, tags)
		acc, exists := b.wsSums[key]
		if !exists {
			if len(b.wsSums) >= b.maxEntries {
				b.dropped++
				return
			}
			acc = &wsSummed{view: rowView{
				tags: tags, corr: e.corr, ws: true, wsMetric: metricName, timestamp: sample.Time.UnixMilli(),
			}}
			b.wsSums[key] = acc
		}
		acc.view.duration += value
		return
	}

	if len(b.wsRows) >= b.maxEntries {
		b.dropped++
		return
	}
	v := rowView{
		tags: tags, corr: e.corr, ws: true, wsMetric: metricName,
		timestamp: sample.Time.UnixMilli(), duration: value,
	}
	switch metricName {
	case "ws_connecting":
		v.connect = value
	case "ws_ping":
		v.latency = value
	}
	if metricName == e.corr.WSErrorMetric && e.corr.WSErrorMetaKey != "" {
		v.wsMessage = extractMeta(sample)[e.corr.WSErrorMetaKey]
	}
	b.wsRows = append(b.wsRows, e.columns.appendRow(nil, &v))
}

func wsSumKey(metricName string, tags map[string]string) string {
	return sanitizeKeyPart(metricName) + "|" + sanitizeKeyPart(tags["url"]) + "|" +
		sanitizeKeyPart(tags["status"]) + "|" + sanitizeKeyPart(tags["scenario"]) + "|" +
		sanitizeKeyPart(tags["region"])
}

// drainWSLocked returns the WebSocket rows and resets them. Caller holds b.mu.
func (b *metricBuffer) drainWSLocked() []row {
	rows := b.wsRows
	b.wsRows = nil
	for _, acc := range b.wsSums {
		rows = append(rows, b.engine.columns.appendRow(nil, &acc.view))
	}
	b.wsSums = make(map[string]*wsSummed)
	return rows
}

// drainReady returns complete requests last seen before completeBefore, drops
// stale incomplete ones, and returns every WebSocket row.
func (b *metricBuffer) drainReady(completeBefore time.Time) []row {
	b.mu.Lock()
	defer b.mu.Unlock()

	rows := make([]row, 0)
	now := time.Now()
	staleCutoff := now.Add(-b.staleAfter)
	for id, entry := range b.entries {
		if entry.hasRequestLine && !entry.lastSeen.After(completeBefore) {
			if entry.pending {
				if now.Sub(entry.firstSeen) < b.holdMax {
					continue
				}
				b.expireHold(entry)
			}
			rows = append(rows, b.engine.render(entry))
			b.removeEntry(id, entry)
			continue
		}
		if !entry.hasRequestLine && entry.lastSeen.Before(staleCutoff) {
			b.removeEntry(id, entry)
		}
	}
	return append(rows, b.drainWSLocked()...)
}

func (b *metricBuffer) drainAll() []row {
	b.mu.Lock()
	defer b.mu.Unlock()

	rows := make([]row, 0, len(b.entries))
	for id, entry := range b.entries {
		if entry.hasRequestLine {
			if entry.pending {
				b.expireHold(entry)
			}
			rows = append(rows, b.engine.render(entry))
		}
		b.removeEntry(id, entry)
	}
	return append(rows, b.drainWSLocked()...)
}

// requeue puts rows from a failed insert back. They are already final, so they
// are never re-stamped. Overflow beyond the cap is dropped.
func (b *metricBuffer) requeue(rows []row) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	for idx, r := range rows {
		if len(b.entries) >= b.maxEntries {
			b.dropped += uint64(len(rows) - idx)
			return
		}
		b.requeueSeq++
		key := fmt.Sprintf("retry|%d|%d", idx, b.requeueSeq)
		b.entries[key] = &pendingRequest{rendered: r, hasRequestLine: true, lastSeen: now}
	}
}

func (b *metricBuffer) droppedCount() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}

// pending counts buffered requests that can still become rows. An entry with no
// request line never does, so it must not hold this above zero.
func (b *metricBuffer) pending() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, entry := range b.entries {
		if entry.hasRequestLine {
			n++
		}
	}
	return n
}

// assertionBuffer accumulates check rows. Each check sample is self-contained.
type assertionBuffer struct {
	mu      sync.Mutex
	columns columns
	corr    *CorrelationConfig
	maxRows int
	rows    []row
	// dropped counts check rows that will never be delivered.
	dropped uint64
}

func newAssertionBuffer(cfg *Config) *assertionBuffer {
	cols, _ := compileColumns(cfg.AssertionColumns, assertionBuiltins, cfg.Constants, "sink.assertionColumns")
	maxRows := cfg.Buffers.AssertionMax
	if maxRows <= 0 {
		maxRows = defaultAssertionBufferMax
	}
	return &assertionBuffer{columns: cols, corr: &cfg.Correlation, maxRows: maxRows}
}

func (b *assertionBuffer) enabled() bool { return len(b.columns) > 0 }

func (b *assertionBuffer) add(sample metrics.Sample, parentLabel, parentStatus string) {
	tags := extractTags(sample)
	if tags["check"] == "" {
		return
	}
	v := rowView{
		tags: tags, meta: extractMeta(sample), corr: b.corr,
		timestamp: sample.Time.UnixMilli(), parentLabel: parentLabel, parentStatus: parentStatus,
		failed: sample.Value == 0.0,
	}
	r := b.columns.appendRow(nil, &v)

	b.mu.Lock()
	b.rows = append(b.rows, r)
	// Drop the oldest rows on overflow to bound memory when the endpoint is slow.
	if len(b.rows) > b.maxRows {
		overflow := len(b.rows) - b.maxRows
		b.rows = append(b.rows[:0], b.rows[overflow:]...)
		b.dropped += uint64(overflow)
	}
	b.mu.Unlock()
}

func (b *assertionBuffer) drain() []row {
	b.mu.Lock()
	defer b.mu.Unlock()
	rows := b.rows
	b.rows = nil
	return rows
}

// requeue prepends rows after a failed insert. It builds a fresh slice: the
// caller's chunk shares capacity with the rows it requeues next, so appending
// into it would overwrite them.
func (b *assertionBuffer) requeue(rows []row) {
	if len(rows) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	merged := make([]row, 0, len(rows)+len(b.rows))
	merged = append(merged, rows...)
	merged = append(merged, b.rows...)
	b.rows = merged
	if len(b.rows) > b.maxRows {
		b.dropped += uint64(len(b.rows) - b.maxRows)
		b.rows = b.rows[:b.maxRows]
	}
}

// countUndelivered moves whatever is still buffered into the dropped count.
// Called once, after the final flush: nothing flushes the buffer again.
func (b *assertionBuffer) countUndelivered() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dropped += uint64(len(b.rows))
	b.rows = nil
}

func (b *assertionBuffer) droppedCount() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}

func sampleMetricName(sample metrics.Sample) string {
	if sample.Metric == nil {
		return ""
	}
	return sample.Metric.Name
}

func extractTags(sample metrics.Sample) map[string]string {
	if sample.Tags == nil {
		return map[string]string{}
	}
	return sample.Tags.Map()
}

// extractMeta returns the sample's non-indexed metadata, which carries no
// cardinality cost.
func extractMeta(sample metrics.Sample) map[string]string {
	if sample.Metadata == nil {
		return map[string]string{}
	}
	return sample.Metadata
}

// mergeKept copies the kept keys of src into dst. A later empty value never
// replaces an earlier one; the first sample's empty values are kept as is.
func mergeKept(dst, src map[string]string, keep map[string]struct{}) {
	for k := range keep {
		v, ok := src[k]
		if !ok {
			continue
		}
		if prev, seen := dst[k]; !seen || v != "" || prev == "" {
			dst[k] = v
		}
	}
}

func buildCorrelationID(sample metrics.Sample, tags map[string]string) string {
	name := nonEmpty(tags["name"], tags["url"])
	return sanitizeKeyPart(nonEmpty(tags["vu"], "0")) + "|" +
		sanitizeKeyPart(nonEmpty(tags["iter"], "0")) + "|" +
		sanitizeKeyPart(nonEmpty(tags["method"], "unknown")) + "|" +
		sanitizeKeyPart(nonEmpty(name, "unknown")) + "|" +
		strconv.FormatInt(sample.Time.UnixNano(), 10)
}

func sanitizeKeyPart(value string) string {
	if value == "" {
		return "unknown"
	}
	return strings.ReplaceAll(value, "|", "_")
}

func roundedInt64(v float64) int64 {
	return int64(math.Round(v))
}
