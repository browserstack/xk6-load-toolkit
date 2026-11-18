// Package throughput is the request-rate limiter capability: a flat sliding
// window or a time-varying schedule, exposed to scripts as the
// k6/x/load-toolkit/throughput JS module.
package throughput

import (
	"errors"
	"fmt"
	"math"
)

// Config configures the request-rate limiter module.
type Config struct {
	PodRPS    float64         `json:"podRps"`
	Schedule  *ScheduleConfig `json:"schedule"`
	TraceFile string          `json:"traceFile"`
	TraceCap  int             `json:"traceCap"`
}

// ScheduleConfig selects the time-varying limiter instead of podRps. Stages,
// DurationSec, PeakRps and T0Ms are carried as the producer received them and
// are checked when the limiter is built (module.go), so a bad value fails the
// run through the module rather than a guessed default.
type ScheduleConfig struct {
	// Stages is a JSON array of {fromRps, toRps, durationSec} segments.
	Stages string `json:"stages"`
	// DurationSec and PeakRps must equal the stages' total and peak.
	DurationSec string `json:"durationSec"`
	PeakRps     string `json:"peakRps"`
	// T0Ms is the schedule start in Unix milliseconds, shared by every process of a run.
	T0Ms string `json:"t0Ms"`
	// WarmupSec is excluded from the per-stage delivery log lines only.
	WarmupSec float64 `json:"warmupSec"`
}

// Validate checks the section as part of the configuration document.
func (t *Config) Validate() error {
	if t.Schedule == nil && (math.IsNaN(t.PodRPS) || math.IsInf(t.PodRPS, 0) || t.PodRPS < 1) {
		return fmt.Errorf("throughput.podRps: must be a number >= 1, got %v", t.PodRPS)
	}
	if t.TraceCap < 0 {
		return errors.New("throughput.traceCap: must not be negative")
	}
	return nil
}
