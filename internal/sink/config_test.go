package sink_test

import (
	"encoding/json"
	"testing"

	"github.com/browserstack/xk6-load-toolkit/internal/sink"
)

func TestSources_StringOrArray(t *testing.T) {
	var spec sink.ColumnSpec
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
