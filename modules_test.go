package loadtoolkit

import "testing"

// The rate comes only from configuration: absent means off, fractional shares truncate.
func TestLimiterFromConfig(t *testing.T) {
	if l, _, err := limiterFromConfig(nil); l != nil || err != nil {
		t.Fatalf("absent section: l=%v err=%v", l, err)
	}
	for raw, want := range map[float64]int{50: 50, 1: 1, 1.9: 1} {
		l, _, err := limiterFromConfig(&ThroughputConfig{PodRPS: raw})
		if err != nil || l.Rate() != want {
			t.Fatalf("podRps %v: rate=%v err=%v, want %d", raw, l, err, want)
		}
		if l.traceCap != defaultTraceCap {
			t.Fatalf("traceCap = %d", l.traceCap)
		}
	}
	l, _, err := limiterFromConfig(&ThroughputConfig{PodRPS: 5, TraceCap: 7})
	if err != nil || l.traceCap != 7 {
		t.Fatalf("traceCap override: %v %v", l, err)
	}
}
