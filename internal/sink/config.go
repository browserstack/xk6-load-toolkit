// Package sink is the output capability: it posts request, WebSocket and
// check rows as JSONEachRow to a ClickHouse-HTTP-compatible endpoint, writes
// the run-progress markers and enforces the time-series cardinality budget.
package sink

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
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
}

// ColumnSpec maps one output column to an ordered list of sources; the first
// non-empty string source wins.
type ColumnSpec struct {
	Name string  `json:"name"`
	From Sources `json:"from"`
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
	return nil
}
