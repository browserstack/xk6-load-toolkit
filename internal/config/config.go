// Package config reads the toolkit configuration document from the process
// environment, applies env.unset, strictly decodes and validates it, and
// composes each capability's own section. It is the only package that knows
// the whole document; capability packages own their section types.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"go.k6.io/k6/v2/errext/exitcodes"

	"github.com/browserstack/xk6-load-toolkit/internal/sink"
	"github.com/browserstack/xk6-load-toolkit/internal/tagfilter"
	"github.com/browserstack/xk6-load-toolkit/internal/throughput"
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
	Version    int                `json:"version"`
	Env        *EnvConfig         `json:"env"`
	Throughput *throughput.Config `json:"throughput"`
	TagFilter  *tagfilter.Config  `json:"tagFilter"`
	Sink       *sink.Config       `json:"sink"`
}

// EnvConfig lists extra variables removed from the process environment in init().
type EnvConfig struct {
	Unset []string `json:"unset"`
}

// The configuration is read once, in init(), because k6 copies the process
// environment into the script's __ENV before any extension code runs again.
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

// Loaded returns the configuration read in init(), or the startup error.
func Loaded() (*Config, error) { return loadedConfig, configErr }

// Throughput returns the throughput section read in init(), or the startup error.
func Throughput() (*throughput.Config, error) {
	if configErr != nil {
		return nil, configErr
	}
	return loadedConfig.Throughput, nil
}

// TagFilter returns the tagFilter section read in init(), or the startup error.
func TagFilter() (*tagfilter.Config, error) {
	if configErr != nil {
		return nil, configErr
	}
	return loadedConfig.TagFilter, nil
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

	cfg, err := Parse([]byte(raw))
	if cfg != nil && cfg.Env != nil {
		unsetAll(cfg.Env.Unset) // backstop: the list as the strict pass read it
	}
	if err != nil {
		writeDiagnostics(diagnostics, err)
		return nil, errStartup
	}
	return cfg, nil
}

// unsetListed walks the top-level object and unsets env.unset as soon as it is
// read. It stops at the first malformed token; keys read before it stay unset.
// encoding/json matches field names case-insensitively and keeps the last of
// duplicate keys, so this pass unsets the list under every "env"/"unset" key in
// any case, then rejects a key that is not spelled exactly, so the strict pass
// never reads a list this one did not.
func unsetListed(raw string) error {
	dec := json.NewDecoder(strings.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return errors.New("the configuration is not a JSON object")
	}
	var casing error
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return err
		}
		key, _ := keyTok.(string)
		if !strings.EqualFold(key, "env") {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return err
			}
			continue
		}
		if key != "env" {
			casing = fmt.Errorf("key %q must be spelled \"env\"", key)
		}
		var env map[string]json.RawMessage
		if err := dec.Decode(&env); err != nil {
			return err
		}
		for name, value := range env {
			if !strings.EqualFold(name, "unset") {
				continue
			}
			if name != "unset" {
				casing = fmt.Errorf("key %q must be spelled \"unset\"", name)
			}
			var keys []string
			if err := json.Unmarshal(value, &keys); err != nil {
				return err
			}
			unsetAll(keys)
		}
	}
	if _, err := dec.Token(); err != nil { // the closing brace; an error means a truncated document
		return err
	}
	return casing
}

func unsetAll(keys []string) {
	for _, key := range keys {
		_ = os.Unsetenv(key)
	}
}

func writeDiagnostics(path string, cause error) {
	if path == "" {
		return
	}
	_ = os.WriteFile(path, []byte("xk6-load-toolkit: invalid configuration: "+cause.Error()+"\n"), 0o600)
}

// Parse decodes and validates a configuration document. Unknown fields are
// rejected so a typo fails fast instead of silently disabling a step. On a
// validation error the decoded config is returned alongside the error.
func Parse(raw []byte) (*Config, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if dec.More() {
		return nil, errors.New("decode: trailing data after the configuration object")
	}
	return &cfg, cfg.Validate()
}

// Validate checks the version, then each present section with that
// capability's own Validate.
func (c *Config) Validate() error {
	if c.Version != SchemaVersion {
		return fmt.Errorf("version: unsupported value %d (want %d)", c.Version, SchemaVersion)
	}
	if c.Throughput != nil {
		if err := c.Throughput.Validate(); err != nil {
			return err
		}
	}
	if c.TagFilter != nil {
		if err := c.TagFilter.Validate(); err != nil {
			return err
		}
	}
	if c.Sink != nil {
		return c.Sink.Validate()
	}
	return nil
}
