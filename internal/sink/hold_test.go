package sink

import (
	"testing"
	"time"

	"go.k6.io/k6/v2/metrics"
)

func TestHold_PendingRowWaitsForItsResult(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	at := time.Now().Add(-10 * time.Second)
	addOpRequest(b, reg, "Async", at, map[string]string{testBytesID: "1-7", opNameKey: "GetUser", opPendingKey: "1"})

	if rows := b.drainReady(time.Now().Add(time.Hour)); len(rows) != 0 {
		t.Fatalf("held row drained before its result: %d rows", len(rows))
	}
	b.add(opResult(reg, at, map[string]string{testBytesID: "1-7", opPendingKey: "1", opStateKey: "2", opCodeKey: `Field "x" not found`, opCodeSourceKey: "2"}))
	rows := decodeOpRows(t, b.drainReady(time.Now().Add(time.Hour)))
	if len(rows) != 1 || rows[0].OpState != 2 || rows[0].OpCodeSource != 2 || rows[0].OpCode != `Field "x" not found` {
		t.Fatalf("rows after result = %+v", rows)
	}
	if got := b.unjoinedCount(); got != 0 {
		t.Fatalf("unjoined = %d, want 0", got)
	}
}

// On expiry the row takes onExpireMeta and counts as unjoined once; its late
// result, carrying the pending marker, is not counted again.
func TestHold_ExpiryAppliesOnExpireMeta(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, func(c *Config) {
		withOpJoin(c)
		c.Hold.Max = Duration(time.Millisecond)
	})
	at := time.Unix(1700000000, 0)
	addOpRequest(b, reg, "Async", at, map[string]string{testBytesID: "1-7", opNameKey: "GetUser", opPendingKey: "1"})
	time.Sleep(5 * time.Millisecond)

	rows := decodeOpRows(t, b.drainReady(time.Now().Add(time.Hour)))
	if len(rows) != 1 || rows[0].OpState != opStateChecked || rows[0].OpName != "GetUser" {
		t.Fatalf("expired row = %+v", rows)
	}
	b.add(opResult(reg, at, map[string]string{testBytesID: "1-7", opPendingKey: "1", opStateKey: "1"}))
	if got := b.unjoinedCount(); got != 1 {
		t.Fatalf("unjoined = %d, want 1", got)
	}
}

func TestHold_FinalDrainExpiresHeldRows(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	addOpRequest(b, reg, "Async", time.Unix(1700000000, 0), map[string]string{testBytesID: "1-7", opNameKey: "GetUser", opPendingKey: "1"})
	rows := decodeOpRows(t, b.drainAll())
	if len(rows) != 1 || rows[0].OpState != opStateChecked || b.unjoinedCount() != 1 {
		t.Fatalf("rows = %+v, unjoined = %d", rows, b.unjoinedCount())
	}
}

// A row without the pending marker is never held.
func TestHold_UnmarkedRowIsNotHeld(t *testing.T) {
	reg := metrics.NewRegistry()
	b := newConfiguredBuffer(t, withOpJoin)
	addOpRequest(b, reg, "Sync", time.Now(), map[string]string{testBytesID: "1-1", opNameKey: "GetUser"})
	if rows := b.drainReady(time.Now().Add(time.Hour)); len(rows) != 1 {
		t.Fatalf("drained %d rows, want 1", len(rows))
	}
}

func TestHold_MaxNeverExceedsStaleAfter(t *testing.T) {
	b := newConfiguredBuffer(t, func(c *Config) {
		withOpJoin(c)
		c.Transport.StaleAfter = Duration(10 * time.Second)
		c.Hold.Max = Duration(time.Minute)
	})
	if b.holdMax != 10*time.Second {
		t.Fatalf("holdMax = %s, want 10s", b.holdMax)
	}
	if d := newConfiguredBuffer(t, withOpJoin).holdMax; d != defaultHoldMax {
		t.Fatalf("default holdMax = %s, want %s", d, defaultHoldMax)
	}
	if d := newConfiguredBuffer(t, nil).holdMax; d != 0 {
		t.Fatalf("holdMax without a hold = %s, want 0", d)
	}
}
