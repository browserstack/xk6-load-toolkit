package loadtoolkit

import (
	"errors"
	"fmt"
	"testing"

	"go.k6.io/k6/v2/lib"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

func outputParams() output.Params {
	tags := metrics.DefaultSystemTagSet
	return output.Params{ScriptOptions: lib.Options{SystemTags: &tags}}
}

// A configuration error reaches k6 unchanged, and an absent sink section gives
// the no-op output.
func TestNewSink_ConfigErrorAndAbsentSink(t *testing.T) {
	errStartup := errors.New("startup error")
	if _, err := newSink(nil, errStartup, outputParams()); err != errStartup {
		t.Fatalf("err = %v, want errStartup", err)
	}

	o, err := newSink(&Config{Version: SchemaVersion}, nil, outputParams())
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%T", o); got != "sink.noopSink" {
		t.Fatalf("absent sink section gave %s, want noopSink", got)
	}
}
