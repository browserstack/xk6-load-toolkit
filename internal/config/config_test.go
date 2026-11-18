package config

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.k6.io/k6/v2/errext/exitcodes"

	"github.com/browserstack/xk6-load-toolkit/internal/sink"
	"github.com/browserstack/xk6-load-toolkit/internal/testutil"
)

func validConfigJSON(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	var sc sink.Config
	if err := json.Unmarshal([]byte(testutil.SinkConfigJSON), &sc); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(Config{Version: SchemaVersion, Sink: &sc})
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
	cfg, err := Parse(validConfigJSON(t, nil))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Sink == nil || cfg.Throughput != nil || cfg.TagFilter != nil {
		t.Fatalf("sections = %+v", cfg)
	}
}

func TestParseConfig_Minimal(t *testing.T) {
	cfg, err := Parse([]byte(`{"version":1}`))
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
			_, err := Parse(tc.raw)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
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

// env.unset is applied even when strict decoding rejects the document.
func TestLoadFromEnv_UnsetsListedKeysWhenStrictDecodeFails(t *testing.T) {
	cases := map[string]struct {
		raw     string
		wantErr error
	}{
		"unknown field":          {`{"version":1,"env":{"unset":["TOOLKIT_TEST_SECRET"]},"sinks":{}}`, errStartup},
		"trailing data":          {`{"version":1,"env":{"unset":["TOOLKIT_TEST_SECRET"]}} {"x":1}`, errStartup},
		"truncated document":     {`{"version":1,"env":{"unset":["TOOLKIT_TEST_SECRET"]},"sink":`, errUnreadable},
		"syntax error after env": {`{"version":1,"env":{"unset":["TOOLKIT_TEST_SECRET"]},"sink":{,}}`, errUnreadable},
		"unknown field in env":   {`{"version":1,"env":{"unset":["TOOLKIT_TEST_SECRET"],"extra":true}}`, errStartup},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TOOLKIT_TEST_SECRET", "s")
			t.Setenv(ConfigEnv, tc.raw)
			cfg, err := loadFromEnv()
			if cfg != nil || err != tc.wantErr {
				t.Fatalf("cfg=%v err=%v, want %v", cfg, err, tc.wantErr)
			}
			if !errors.Is(err, errStartup) || err.Error() != errStartup.Error() {
				t.Fatalf("error %q must carry only the fixed startup message", err)
			}
			if _, ok := os.LookupEnv("TOOLKIT_TEST_SECRET"); ok {
				t.Fatal("a listed key is still set after a rejected document")
			}
		})
	}
}

// A document that is not a readable JSON object is fatal: init() exits before
// k6 runs script code, so nothing listed can leak.
func TestLoadFromEnv_UnreadableDocument(t *testing.T) {
	for _, raw := range []string{`{`, `not json`, `["env"]`, `{"env":{"unset":"K"}}`} {
		t.Setenv(ConfigEnv, raw)
		if _, err := loadFromEnv(); err != errUnreadable {
			t.Fatalf("%q: err=%v, want errUnreadable", raw, err)
		}
	}
}

// init() exits with k6's invalid-config code on an unreadable document.
func TestInit_ExitsOnUnreadableDocument(t *testing.T) {
	if os.Getenv("TOOLKIT_INIT_CHILD") == "1" {
		return // reached only if init() did not exit
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestInit_ExitsOnUnreadableDocument$")
	cmd.Env = append(os.Environ(), "TOOLKIT_INIT_CHILD=1", ConfigEnv+"={not json")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != int(exitcodes.InvalidConfig) {
		t.Fatalf("child err=%v, want exit %d; output:\n%s", err, exitcodes.InvalidConfig, out)
	}
	if !strings.Contains(string(out), errStartup.Error()) || strings.Contains(string(out), "not json") {
		t.Fatalf("child output must be the fixed message only:\n%s", out)
	}
}

// encoding/json matches field names case-insensitively; the lenient pass must
// not read a list the strict pass would read differently.
func TestLoadFromEnv_CaseVariantsAndDuplicates(t *testing.T) {
	cases := map[string]struct {
		raw     string
		wantErr error
	}{
		"ENV":                    {`{"version":1,"ENV":{"unset":["TOOLKIT_TEST_SECRET"]}}`, errUnreadable},
		"Env":                    {`{"version":1,"Env":{"unset":["TOOLKIT_TEST_SECRET"]}}`, errUnreadable},
		"UNSET":                  {`{"version":1,"env":{"UNSET":["TOOLKIT_TEST_SECRET"]}}`, errUnreadable},
		"duplicate, first lists": {`{"version":1,"env":{"unset":["TOOLKIT_TEST_SECRET"]},"env":{"unset":[]}}`, nil},
		"duplicate, last lists":  {`{"version":1,"env":{"unset":[]},"env":{"unset":["TOOLKIT_TEST_SECRET"]}}`, nil},
		"duplicate, Env last":    {`{"version":1,"env":{"unset":[]},"Env":{"unset":["TOOLKIT_TEST_SECRET"]}}`, errUnreadable},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TOOLKIT_TEST_SECRET", "s")
			t.Setenv(ConfigEnv, tc.raw)
			_, err := loadFromEnv()
			if err != tc.wantErr {
				t.Fatalf("err=%v, want %v", err, tc.wantErr)
			}
			// Whether the run continues or init() aborts, the key is gone.
			if _, ok := os.LookupEnv("TOOLKIT_TEST_SECRET"); ok {
				t.Fatal("a listed key is still set")
			}
		})
	}
}

// The schedule section is part of the strict configuration schema.
func TestParseConfig_ScheduleSection(t *testing.T) {
	cfg, err := Parse([]byte(`{"version":1,"throughput":{"schedule":{"stages":"[]","durationSec":"1","peakRps":"1","t0Ms":"1","warmupSec":2}}}`))
	if err != nil || cfg.Throughput.Schedule == nil || cfg.Throughput.Schedule.WarmupSec != 2 {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
	if _, err := Parse([]byte(`{"version":1,"throughput":{"schedule":{"stage":"[]"}}}`)); err == nil {
		t.Fatal("an unknown schedule field must be rejected")
	}
	if _, err := Parse([]byte(`{"version":1,"throughput":{"podRps":0}}`)); err == nil {
		t.Fatal("a flat section still needs podRps >= 1")
	}
}
