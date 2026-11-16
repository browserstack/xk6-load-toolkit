package loadtoolkit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfigJSON(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	raw, err := json.Marshal(Config{Version: SchemaVersion, Sink: testSinkConfig()})
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	// Durations marshal as numbers; a document carries strings.
	tr := doc["sink"].(map[string]any)["transport"].(map[string]any)
	for _, k := range []string{"timeout", "flushInterval", "completeAfter", "staleAfter"} {
		delete(tr, k)
	}
	delete(tr, "retry")
	if mutate != nil {
		mutate(doc)
	}
	out, _ := json.Marshal(doc)
	return out
}

func sinkOf(doc map[string]any) map[string]any { return doc["sink"].(map[string]any) }

func TestParseConfig_Valid(t *testing.T) {
	cfg, err := ParseConfig(validConfigJSON(t, nil))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Sink == nil || cfg.Throughput != nil || cfg.TagFilter != nil {
		t.Fatalf("sections = %+v", cfg)
	}
}

func TestParseConfig_Minimal(t *testing.T) {
	cfg, err := ParseConfig([]byte(`{"version":1}`))
	if err != nil || cfg.Sink != nil || cfg.Throughput != nil {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
}

func TestParseConfig_Rejects(t *testing.T) {
	cases := map[string]struct {
		raw  []byte
		want string
	}{
		"not json":        {[]byte(`{`), "decode"},
		"trailing data":   {[]byte(`{"version":1}{}`), "trailing"},
		"unknown field":   {[]byte(`{"version":1,"sinks":{}}`), "unknown field"},
		"bad version":     {[]byte(`{"version":2}`), "version"},
		"rate below one":  {[]byte(`{"version":1,"throughput":{"podRps":0.5}}`), "throughput.podRps"},
		"negative cap":    {[]byte(`{"version":1,"throughput":{"podRps":5,"traceCap":-1}}`), "traceCap"},
		"bad duration":    {validConfigJSON(t, func(d map[string]any) { sinkOf(d)["transport"].(map[string]any)["timeout"] = "soon" }), "duration"},
		"no endpoint":     {validConfigJSON(t, func(d map[string]any) { sinkOf(d)["transport"].(map[string]any)["endpoint"] = "" }), "endpoint"},
		"ftp endpoint":    {validConfigJSON(t, func(d map[string]any) { sinkOf(d)["transport"].(map[string]any)["endpoint"] = "ftp://x" }), "endpoint"},
		"bad table":       {validConfigJSON(t, func(d map[string]any) { sinkOf(d)["transport"].(map[string]any)["table"] = "a;b" }), "table"},
		"bad database":    {validConfigJSON(t, func(d map[string]any) { sinkOf(d)["transport"].(map[string]any)["database"] = "a b" }), "database"},
		"bad format":      {validConfigJSON(t, func(d map[string]any) { sinkOf(d)["transport"].(map[string]any)["format"] = "CSV" }), "format"},
		"half assertions": {validConfigJSON(t, func(d map[string]any) { sinkOf(d)["transport"].(map[string]any)["assertionTable"] = "" }), "assertionColumns"},
		"no columns":      {validConfigJSON(t, func(d map[string]any) { sinkOf(d)["columns"] = []any{} }), "sink.columns"},
		"unknown builtin": {validConfigJSON(t, func(d map[string]any) {
			sinkOf(d)["columns"] = []any{map[string]any{"name": "x", "from": "computed:nope"}}
		}), "sink.columns[0].from[0]"},
		"unknown const": {validConfigJSON(t, func(d map[string]any) {
			sinkOf(d)["columns"] = []any{map[string]any{"name": "x", "from": "const:nope"}}
		}), "constant"},
		"bad source kind": {validConfigJSON(t, func(d map[string]any) {
			sinkOf(d)["columns"] = []any{map[string]any{"name": "x", "from": "env:HOME"}}
		}), "source kind"},
		"numeric in chain": {validConfigJSON(t, func(d map[string]any) {
			sinkOf(d)["columns"] = []any{map[string]any{"name": "x", "from": []any{"computed:vus", "tag:vu"}}}
		}), "fallback"},
		"duplicate column": {validConfigJSON(t, func(d map[string]any) {
			c := map[string]any{"name": "x", "from": "tag:a"}
			sinkOf(d)["columns"] = []any{c, c}
		}), "duplicate"},
		"assertion builtin in request columns": {validConfigJSON(t, func(d map[string]any) {
			sinkOf(d)["columns"] = []any{map[string]any{"name": "x", "from": "computed:failed"}}
		}), "unknown computed"},
		"unknown request metric": {validConfigJSON(t, func(d map[string]any) {
			sinkOf(d)["correlation"].(map[string]any)["requestMetrics"] = []any{"http_req_sending"}
		}), "requestMetrics"},
		"bad exit code": {validConfigJSON(t, func(d map[string]any) {
			sinkOf(d)["cardinality"] = map[string]any{"maxTimeSeries": 5, "exitCode": 300}
		}), "exitCode"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseConfig(tc.raw)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestSources_StringOrArray(t *testing.T) {
	var spec ColumnSpec
	if err := json.Unmarshal([]byte(`{"name":"a","from":"tag:x"}`), &spec); err != nil || len(spec.From) != 1 {
		t.Fatalf("string form: %v %v", spec.From, err)
	}
	if err := json.Unmarshal([]byte(`{"name":"a","from":["tag:x","const:y"]}`), &spec); err != nil || len(spec.From) != 2 {
		t.Fatalf("array form: %v %v", spec.From, err)
	}
	if err := json.Unmarshal([]byte(`{"name":"a","from":5}`), &spec); err == nil {
		t.Fatal("a number must be rejected")
	}
}

func TestLoadFromEnv_AbsentMeansEverythingOff(t *testing.T) {
	t.Setenv(ConfigEnv, "")
	os.Unsetenv(ConfigEnv)
	cfg, err := loadFromEnv()
	if err != nil || cfg.Sink != nil || cfg.Throughput != nil || cfg.TagFilter != nil {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
}

// The configuration and the listed variables are gone before k6 builds __ENV.
func TestLoadFromEnv_UnsetsConfigAndListedKeys(t *testing.T) {
	t.Setenv("TOOLKIT_TEST_SECRET", "s")
	t.Setenv("TOOLKIT_TEST_KEEP", "k")
	t.Setenv(ConfigEnv, `{"version":1,"env":{"unset":["TOOLKIT_TEST_SECRET"]},"throughput":{"podRps":3}}`)
	cfg, err := loadFromEnv()
	if err != nil || cfg.Throughput == nil || cfg.Throughput.PodRPS != 3 {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
	for _, key := range []string{ConfigEnv, "TOOLKIT_TEST_SECRET"} {
		if _, ok := os.LookupEnv(key); ok {
			t.Errorf("%s still set after init", key)
		}
	}
	if os.Getenv("TOOLKIT_TEST_KEEP") != "k" {
		t.Error("an unlisted variable was removed")
	}
}

// An invalid configuration gives a fixed startup error that carries no config
// detail; the reason goes to the diagnostics file only.
func TestLoadFromEnv_InvalidConfig(t *testing.T) {
	diag := filepath.Join(t.TempDir(), "diag.log")
	t.Setenv(DiagnosticsEnv, diag)
	t.Setenv("TOOLKIT_TEST_SECRET", "s")
	t.Setenv(ConfigEnv, `{"version":1,"env":{"unset":["TOOLKIT_TEST_SECRET"]},"sink":{"transport":{"endpoint":"http://x","table":"t"},"columns":[{"name":"c","from":"computed:nope"}]}}`)

	cfg, err := loadFromEnv()
	if cfg != nil || err != errStartup {
		t.Fatalf("cfg=%+v err=%v, want errStartup", cfg, err)
	}
	if strings.Contains(err.Error(), "sink") || strings.Contains(err.Error(), "nope") {
		t.Fatalf("startup error leaks config detail: %q", err)
	}
	b, readErr := os.ReadFile(diag)
	if readErr != nil || !strings.Contains(string(b), "sink.columns[0].from[0]") {
		t.Fatalf("diagnostics = %q (%v), want the failing path", b, readErr)
	}
	for _, key := range []string{ConfigEnv, DiagnosticsEnv, "TOOLKIT_TEST_SECRET"} {
		if _, ok := os.LookupEnv(key); ok {
			t.Errorf("%s still set after a failed init", key)
		}
	}
}

func TestNewSink_ConfigErrorAndAbsentSink(t *testing.T) {
	prevCfg, prevErr := loadedConfig, configErr
	t.Cleanup(func() { loadedConfig, configErr = prevCfg, prevErr })

	loadedConfig, configErr = nil, errStartup
	if _, err := NewSink(outputParams()); err != errStartup {
		t.Fatalf("err = %v, want errStartup", err)
	}

	loadedConfig, configErr = &Config{Version: SchemaVersion}, nil
	o, err := NewSink(outputParams())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := o.(noopSink); !ok {
		t.Fatalf("absent sink section gave %T, want noopSink", o)
	}
}
