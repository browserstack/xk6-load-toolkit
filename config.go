package loadtoolkit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"go.k6.io/k6/v2/errext/exitcodes"
)

const (
	// ConfigEnv carries the whole toolkit configuration as JSON. It is read and
	// removed from the process environment in init(), before k6 builds __ENV.
	ConfigEnv = "XK6_LOAD_TOOLKIT_CONFIG"
	// DiagnosticsEnv optionally names a file that receives the detailed reason
	// when the configuration is invalid. Also removed in init().
	DiagnosticsEnv = "XK6_LOAD_TOOLKIT_DIAGNOSTICS"
	// SchemaVersion is the only configuration version this build accepts.
	SchemaVersion = 1
)

// errStartup is what a script author sees when the configuration is invalid.
// It never carries config values or paths; those go to the diagnostics file.
var errStartup = errors.New("The test could not start because of a problem on our side. " +
	"No load was generated. Run the test again; if it keeps failing, contact support with the run ID.")

// Config is the root of the toolkit configuration. A nil section switches that
// capability off.
type Config struct {
	Version    int               `json:"version"`
	Env        *EnvConfig        `json:"env"`
	Throughput *ThroughputConfig `json:"throughput"`
	TagFilter  *TagFilterConfig  `json:"tagFilter"`
	Sink       *SinkConfig       `json:"sink"`
}

// EnvConfig lists extra variables removed from the process environment in init().
type EnvConfig struct {
	Unset []string `json:"unset"`
}

// ThroughputConfig configures the request-rate limiter module.
type ThroughputConfig struct {
	PodRPS    float64 `json:"podRps"`
	TraceFile string  `json:"traceFile"`
	TraceCap  int     `json:"traceCap"`
}

// TagFilterConfig holds the raw include/exclude rule strings,
// shaped {"test":[...],"group":[...],"scenario":[...]}. Include wins.
type TagFilterConfig struct {
	Include string `json:"include"`
	Exclude string `json:"exclude"`
}

// SinkConfig configures the output.
type SinkConfig struct {
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

var (
	loadedConfig *Config
	configErr    error
)

func init() {
	loadedConfig, configErr = loadFromEnv()
	if errors.Is(configErr, errUnreadable) {
		// Nothing in the document could be read, so the variables it meant to hide
		// may still be set. Stop now, before k6 runs any script code.
		fmt.Fprintln(os.Stderr, errStartup)
		os.Exit(int(exitcodes.InvalidConfig))
	}
}

// errUnreadable marks a configuration that cannot be read as a JSON object;
// init() aborts the process on it. Its message is errStartup's.
var errUnreadable = fmt.Errorf("%w", errStartup)

// loadFromEnv reads, validates and unsets the configuration. An absent
// variable means every capability is off.
//
// The keys in env.unset are removed before strict validation, by a lenient
// pass that ignores every other field and anything after the document, so a
// mistake elsewhere never leaves them visible in __ENV. If that pass hits
// malformed JSON, loadFromEnv returns errUnreadable and init() exits before
// any script code runs. The producer of the variable must emit valid JSON,
// with env before the other sections so its keys are read even when a later
// field is malformed.
func loadFromEnv() (*Config, error) {
	raw, present := os.LookupEnv(ConfigEnv)
	diagnostics := os.Getenv(DiagnosticsEnv)
	_ = os.Unsetenv(ConfigEnv)
	_ = os.Unsetenv(DiagnosticsEnv)
	if !present {
		return &Config{Version: SchemaVersion}, nil
	}

	if err := unsetListed(raw); err != nil {
		writeDiagnostics(diagnostics, fmt.Errorf("decode: %w", err))
		return nil, errUnreadable
	}

	cfg, err := ParseConfig([]byte(raw))
	if err != nil {
		writeDiagnostics(diagnostics, err)
		return nil, errStartup
	}
	return cfg, nil
}

// unsetListed walks the top-level object and unsets env.unset as soon as it is
// read. It stops at the first malformed token; keys read before it stay unset.
func unsetListed(raw string) error {
	dec := json.NewDecoder(strings.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return errors.New("the configuration is not a JSON object")
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return err
		}
		if keyTok != "env" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return err
			}
			continue
		}
		var env struct {
			Unset []string `json:"unset"`
		}
		if err := dec.Decode(&env); err != nil {
			return err
		}
		for _, key := range env.Unset {
			_ = os.Unsetenv(key)
		}
	}
	_, err = dec.Token() // the closing brace; an error here means a truncated document
	return err
}

func writeDiagnostics(path string, cause error) {
	if path == "" {
		return
	}
	_ = os.WriteFile(path, []byte("xk6-load-toolkit: invalid configuration: "+cause.Error()+"\n"), 0o600)
}

// ParseConfig decodes and validates a configuration document. Unknown fields
// are rejected so a typo fails fast instead of silently disabling a step. On a
// validation error the decoded config is returned alongside the error.
func ParseConfig(raw []byte) (*Config, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if dec.More() {
		return nil, errors.New("decode: trailing data after the configuration object")
	}
	return &cfg, cfg.validate()
}

var identifierRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func (c *Config) validate() error {
	if c.Version != SchemaVersion {
		return fmt.Errorf("version: unsupported value %d (want %d)", c.Version, SchemaVersion)
	}
	if t := c.Throughput; t != nil {
		if math.IsNaN(t.PodRPS) || math.IsInf(t.PodRPS, 0) || t.PodRPS < 1 {
			return fmt.Errorf("throughput.podRps: must be a number >= 1, got %v", t.PodRPS)
		}
		if t.TraceCap < 0 {
			return errors.New("throughput.traceCap: must not be negative")
		}
	}
	if c.Sink != nil {
		return c.Sink.validate()
	}
	return nil
}

func (s *SinkConfig) validate() error {
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
