// Package sink is the output capability: it posts request, WebSocket and
// check rows as JSONEachRow to a ClickHouse-HTTP-compatible endpoint, writes
// the run-progress markers and enforces the time-series cardinality budget.
package sink

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

// Config configures the output.
type Config struct {
	Transport        TransportConfig    `json:"transport"`
	Buffers          BuffersConfig      `json:"buffers"`
	Constants        map[string]string  `json:"constants"`
	Columns          []ColumnSpec       `json:"columns"`
	AssertionColumns []ColumnSpec       `json:"assertionColumns"`
	Correlation      CorrelationConfig  `json:"correlation"`
	SendTimeRestamp  bool               `json:"sendTimeRestamp"`
	CheckFilter      bool               `json:"checkFilter"`
	Markers          *MarkersConfig     `json:"markers"`
	Cardinality      *CardinalityConfig `json:"cardinality"`
	// Hold keeps a request row marked pending until its joined result arrives.
	Hold *HoldConfig `json:"hold,omitempty"`
	// Summary writes a one-line run summary file at Stop.
	Summary *SummaryConfig `json:"summary,omitempty"`
}

// HoldConfig holds a request row whose request samples carry PendingMetaKey ==
// PendingValue until a join sample reaches it, or until Max after it was first
// seen. On expiry OnExpireMeta is merged into the row's metadata and the
// unjoined counter is incremented.
type HoldConfig struct {
	PendingMetaKey string            `json:"pendingMetaKey"`
	PendingValue   string            `json:"pendingValue"`
	Max            Duration          `json:"max"`
	OnExpireMeta   map[string]string `json:"onExpireMeta"`
}

// SummaryConfig names the run summary file and the counters it reports.
type SummaryConfig struct {
	Path   string            `json:"path"`
	Keys   SummaryKeys       `json:"keys"`
	Static map[string]string `json:"static,omitempty"`
}

// SummaryKeys maps each counter to the key it is written under. An empty key
// leaves that counter out of the file.
type SummaryKeys struct {
	CardinalityLimitHit string            `json:"cardinalityLimitHit,omitempty"`
	AssertionsDropped   string            `json:"assertionsDropped,omitempty"`
	Unjoined            string            `json:"unjoined,omitempty"`
	Seen                map[string]string `json:"seen,omitempty"`
}

// TransportConfig describes the ClickHouse-HTTP-compatible endpoint rows are posted to.
type TransportConfig struct {
	Endpoint       string      `json:"endpoint"`
	Database       string      `json:"database"`
	Table          string      `json:"table"`
	AssertionTable string      `json:"assertionTable"`
	Format         string      `json:"format"`
	AsyncInsert    bool        `json:"asyncInsert"`
	Timeout        Duration    `json:"timeout"`
	Retry          RetryConfig `json:"retry"`
	MaxInsertRows  int         `json:"maxInsertRows"`
	FlushInterval  Duration    `json:"flushInterval"`
	CompleteAfter  Duration    `json:"completeAfter"`
	StaleAfter     Duration    `json:"staleAfter"`
}

// RetryConfig bounds the per-payload retry loop.
type RetryConfig struct {
	Max      int      `json:"max"`
	Base     Duration `json:"base"`
	MaxDelay Duration `json:"maxDelay"`
}

// BuffersConfig caps the in-memory row buffers.
type BuffersConfig struct {
	MetricMax    int `json:"metricMax"`
	AssertionMax int `json:"assertionMax"`
}

// CorrelationConfig names the metrics, tags and metadata keys the row engine joins on.
type CorrelationConfig struct {
	RequestMetrics      []string `json:"requestMetrics"`
	URLMetaKey          string   `json:"urlMetaKey"`
	URLLegacyTag        string   `json:"urlLegacyTag"`
	BytesIDMetaKey      string   `json:"bytesIdMetaKey"`
	BytesSentMetric     string   `json:"bytesSentMetric"`
	BytesReceivedMetric string   `json:"bytesReceivedMetric"`
	WSMetrics           []string `json:"wsMetrics"`
	WSSummedMetrics     []string `json:"wsSummedMetrics"`
	WSErrorMetric       string   `json:"wsErrorMetric"`
	WSErrorMetaKey      string   `json:"wsErrorMetaKey"`
	CheckMetric         string   `json:"checkMetric"`
	// VerdictMetric is a per-request pass/fail metric (e.g. http_req_failed) that
	// joins its request row by the correlation key; it feeds computed:failedVerdict.
	VerdictMetric string `json:"verdictMetric,omitempty"`
	// JoinMetrics never form rows: their metadata is merged into the request row
	// whose bytesIdMetaKey id they carry.
	JoinMetrics []string `json:"joinMetrics,omitempty"`
}

// MarkersConfig names the run-progress marker files. An empty name disables that marker.
type MarkersConfig struct {
	Dir                string   `json:"dir"`
	TotalRows          string   `json:"totalRows"`
	ScriptData         string   `json:"scriptData"`
	PushAttempted      string   `json:"pushAttempted"`
	Iteration          string   `json:"iteration"`
	IterationThreshold int      `json:"iterationThreshold"`
	DataMetricPrefixes []string `json:"dataMetricPrefixes"`
}

// CardinalityConfig configures the distinct-time-series guard. MaxTimeSeries <= 0 disables it.
type CardinalityConfig struct {
	MaxTimeSeries int    `json:"maxTimeSeries"`
	ExitCode      int    `json:"exitCode"`
	Message       string `json:"message"`
	// ExemptMetrics do not count toward MaxTimeSeries.
	ExemptMetrics []string `json:"exemptMetrics,omitempty"`
}

// ColumnSpec maps one output column to an ordered list of sources; the first
// non-empty string source wins. Every other field is an optional bound.
type ColumnSpec struct {
	Name string  `json:"name"`
	From Sources `json:"from"`
	// Type "uint" renders the value as a number in [Min, Max]; anything
	// unparseable or out of range renders 0.
	Type string  `json:"type,omitempty"`
	Min  *uint64 `json:"min,omitempty"`
	Max  *uint64 `json:"max,omitempty"`
	// MaxRunes truncates a string value to that many runes.
	MaxRunes int `json:"maxRunes,omitempty"`
	// Distinct caps the number of distinct values this process writes.
	Distinct *DistinctSpec `json:"distinct,omitempty"`
	// When lists conditions that must all hold, else the column is empty (0 for uint).
	When []WhenSpec `json:"when,omitempty"`
	// JoinDefault is written on a row a join sample reached when the sources
	// give no valid value.
	JoinDefault *string `json:"joinDefault,omitempty"`
}

// DistinctSpec caps the distinct values of a column per process. Values beyond
// Limit are written as Overflow. With PartitionBy, each value listed in
// Partitions has its own budget; every other partition value shares one.
type DistinctSpec struct {
	Limit       int      `json:"limit"`
	Overflow    string   `json:"overflow"`
	PartitionBy string   `json:"partitionBy,omitempty"`
	Partitions  []string `json:"partitions,omitempty"`
}

// WhenSpec holds when Source reads Equals, or, with Equals absent, any
// non-empty value.
type WhenSpec struct {
	Source string  `json:"source"`
	Equals *string `json:"equals,omitempty"`
}

// Sources accepts either one source string or an array of them.
type Sources []string

// UnmarshalJSON implements json.Unmarshaler.
func (s *Sources) UnmarshalJSON(b []byte) error {
	var one string
	if err := json.Unmarshal(b, &one); err == nil {
		*s = Sources{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return errors.New("must be a string or an array of strings")
	}
	*s = many
	return nil
}

// Duration is a time.Duration written as a Go duration string ("5s").
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var raw string
	if err := json.Unmarshal(b, &raw); err != nil {
		return errors.New("must be a duration string such as \"5s\"")
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return fmt.Errorf("invalid duration %q", raw)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) or(fallback time.Duration) time.Duration {
	if d <= 0 {
		return fallback
	}
	return time.Duration(d)
}

var identifierRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// Validate checks the section as part of the configuration document.
func (s *Config) Validate() error {
	tr := &s.Transport
	u, err := url.Parse(tr.Endpoint)
	if tr.Endpoint == "" || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("sink.transport.endpoint: must be an http(s) URL")
	}
	if tr.Database != "" && !identifierRE.MatchString(tr.Database) {
		return errors.New("sink.transport.database: must match [A-Za-z0-9_]+")
	}
	if !identifierRE.MatchString(tr.Table) {
		return errors.New("sink.transport.table: must match [A-Za-z0-9_]+")
	}
	if tr.AssertionTable != "" && !identifierRE.MatchString(tr.AssertionTable) {
		return errors.New("sink.transport.assertionTable: must match [A-Za-z0-9_]+")
	}
	if (tr.AssertionTable == "") != (len(s.AssertionColumns) == 0) {
		return errors.New("sink.assertionColumns: set together with sink.transport.assertionTable")
	}
	if tr.Format != "" && tr.Format != "JSONEachRow" {
		return errors.New("sink.transport.format: only JSONEachRow is supported")
	}
	if tr.MaxInsertRows < 0 || tr.Retry.Max < 0 || s.Buffers.MetricMax < 0 || s.Buffers.AssertionMax < 0 {
		return errors.New("sink: row limits, buffer caps and retry counts must not be negative")
	}
	for _, name := range s.Correlation.RequestMetrics {
		if _, ok := httpPhaseMetrics[name]; !ok {
			return fmt.Errorf("sink.correlation.requestMetrics: unknown metric %q", name)
		}
	}
	if len(s.Columns) == 0 {
		return errors.New("sink.columns: at least one column is required")
	}
	if _, err := compileColumns(s.Columns, requestBuiltins, s.Constants, "sink.columns"); err != nil {
		return err
	}
	if _, err := compileColumns(s.AssertionColumns, assertionBuiltins, s.Constants, "sink.assertionColumns"); err != nil {
		return err
	}
	if m := s.Markers; m != nil && m.IterationThreshold < 0 {
		return errors.New("sink.markers.iterationThreshold: must not be negative")
	}
	if c := s.Cardinality; c != nil && c.ExitCode != 0 && (c.ExitCode < 1 || c.ExitCode > 255) {
		return errors.New("sink.cardinality.exitCode: must be between 1 and 255")
	}
	if c := s.Cardinality; c != nil {
		for i, name := range c.ExemptMetrics {
			if name == "" {
				return fmt.Errorf("sink.cardinality.exemptMetrics[%d]: must not be empty", i)
			}
		}
	}
	if err := s.validateJoins(); err != nil {
		return err
	}
	return s.validateSummary()
}

// rowMetrics names every metric that feeds a row, by role, so a metric is
// never given two roles.
func (s *Config) rowMetrics() map[string]string {
	c := &s.Correlation
	roles := map[string]string{}
	add := func(role string, names ...string) {
		for _, n := range names {
			if n != "" {
				if _, taken := roles[n]; !taken {
					roles[n] = role
				}
			}
		}
	}
	for name := range httpPhaseMetrics {
		add("an HTTP request metric", name)
	}
	add("a byte counter", c.BytesSentMetric, c.BytesReceivedMetric)
	add("a WebSocket metric", c.WSMetrics...)
	add("a WebSocket metric", c.WSSummedMetrics...)
	add("the check metric", c.CheckMetric)
	return roles
}

// validateJoins checks the verdict metric, the join metrics, the hold and the
// column options that depend on them.
func (s *Config) validateJoins() error {
	c := &s.Correlation
	roles := s.rowMetrics()
	if c.VerdictMetric != "" {
		if role, taken := roles[c.VerdictMetric]; taken {
			return fmt.Errorf("sink.correlation.verdictMetric: %q is already %s", c.VerdictMetric, role)
		}
		roles[c.VerdictMetric] = "the verdict metric"
	}
	for i, name := range c.JoinMetrics {
		where := fmt.Sprintf("sink.correlation.joinMetrics[%d]", i)
		if name == "" {
			return fmt.Errorf("%s: must not be empty", where)
		}
		if role, taken := roles[name]; taken {
			return fmt.Errorf("%s: %q is already %s", where, name, role)
		}
		roles[name] = "a join metric"
	}
	joins := len(c.JoinMetrics) > 0
	if joins && c.BytesIDMetaKey == "" {
		return errors.New("sink.correlation.joinMetrics: requires sink.correlation.bytesIdMetaKey, the request id a join sample carries")
	}
	for i, spec := range s.Columns {
		for _, src := range spec.From {
			if src == "computed:failedVerdict" && c.VerdictMetric == "" {
				return fmt.Errorf("sink.columns[%d].from: computed:failedVerdict requires sink.correlation.verdictMetric", i)
			}
		}
		if spec.JoinDefault != nil && !joins {
			return fmt.Errorf("sink.columns[%d].joinDefault: requires sink.correlation.joinMetrics", i)
		}
	}
	for i, spec := range s.AssertionColumns {
		if spec.JoinDefault != nil {
			return fmt.Errorf("sink.assertionColumns[%d].joinDefault: check rows are never joined", i)
		}
	}
	if h := s.Hold; h != nil {
		if !joins {
			return errors.New("sink.hold: requires sink.correlation.joinMetrics")
		}
		if h.PendingMetaKey == "" || h.PendingValue == "" {
			return errors.New("sink.hold: pendingMetaKey and pendingValue must not be empty")
		}
		for k := range h.OnExpireMeta {
			if k == "" {
				return errors.New("sink.hold.onExpireMeta: keys must not be empty")
			}
		}
	}
	return nil
}

// summaryValueRE keeps a static summary value from breaking the k=v;k=v line.
var summaryValueRE = regexp.MustCompile(`^[^;=\r\n]*$`)

func (s *Config) validateSummary() error {
	sum := s.Summary
	if sum == nil {
		return nil
	}
	if sum.Path == "" || !filepath.IsAbs(sum.Path) {
		return errors.New("sink.summary.path: must be an absolute path")
	}
	used := map[string]struct{}{}
	useKey := func(where, key string) error {
		if !identifierRE.MatchString(key) {
			return fmt.Errorf("%s: key %q must match [A-Za-z0-9_]+", where, key)
		}
		if _, dup := used[key]; dup {
			return fmt.Errorf("%s: duplicate key %q", where, key)
		}
		used[key] = struct{}{}
		return nil
	}
	k := &sum.Keys
	for _, named := range []struct{ where, key string }{
		{"sink.summary.keys.cardinalityLimitHit", k.CardinalityLimitHit},
		{"sink.summary.keys.assertionsDropped", k.AssertionsDropped},
		{"sink.summary.keys.unjoined", k.Unjoined},
	} {
		if named.key == "" {
			continue
		}
		if err := useKey(named.where, named.key); err != nil {
			return err
		}
	}
	if k.Unjoined != "" && len(s.Correlation.JoinMetrics) == 0 {
		return errors.New("sink.summary.keys.unjoined: requires sink.correlation.joinMetrics")
	}
	roles := s.rowMetrics()
	if v := s.Correlation.VerdictMetric; v != "" {
		roles[v] = "the verdict metric"
	}
	for _, j := range s.Correlation.JoinMetrics {
		roles[j] = "a join metric"
	}
	for _, key := range sortedKeys(k.Seen) {
		where := "sink.summary.keys.seen"
		if err := useKey(where, key); err != nil {
			return err
		}
		metric := k.Seen[key]
		if metric == "" {
			return fmt.Errorf("%s.%s: metric must not be empty", where, key)
		}
		if role, taken := roles[metric]; taken {
			return fmt.Errorf("%s.%s: %q is %s; a seen metric never becomes a row", where, key, metric, role)
		}
	}
	for _, key := range sortedKeys(sum.Static) {
		if err := useKey("sink.summary.static", key); err != nil {
			return err
		}
		if !summaryValueRE.MatchString(sum.Static[key]) {
			return fmt.Errorf("sink.summary.static.%s: value must not contain ';', '=' or a line break", key)
		}
	}
	if len(used) == 0 {
		return errors.New("sink.summary.keys: at least one key is required")
	}
	return nil
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
