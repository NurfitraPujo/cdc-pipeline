package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDurationMarshalsAsString(t *testing.T) {
	g := GlobalConfig{BatchSize: 100, BatchWait: 5 * time.Second}
	g.SetDefaults()

	data, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// The whole point: not a nanosecond integer.
	if strings.Contains(string(data), "5000000000") {
		t.Fatalf("batch_wait marshalled as nanoseconds: %s", data)
	}

	var wire map[string]any
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatalf("remarshal: %v", err)
	}
	for field, want := range map[string]string{
		"batch_wait":           "5s",
		"drain_timeout":        "30s",
		"shutdown_timeout":     "30s",
		"stabilization_delay":  "2s",
		"crash_recovery_delay": "5s",
		"global_reload_delay":  "2s",
	} {
		if got, ok := wire[field].(string); !ok || got != want {
			t.Errorf("%s = %v, want %q", field, wire[field], want)
		}
	}

	// Non-duration fields must survive the shadowing.
	if wire["batch_size"] != float64(100) {
		t.Errorf("batch_size = %v, want 100", wire["batch_size"])
	}
}

func TestGlobalConfigRoundTrip(t *testing.T) {
	want := GlobalConfig{
		BatchSize: 250,
		BatchWait: 1500 * time.Millisecond,
		Retry: RetryConfig{
			MaxRetries:      4,
			InitialInterval: 500 * time.Millisecond,
			MaxInterval:     time.Minute,
			EnableDLQ:       true,
		},
	}
	want.SetDefaults()

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got GlobalConfig
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got != want {
		t.Errorf("round trip mismatch:\n got %+v\nwant %+v", got, want)
	}
}

// Configs written before Duration existed hold nanosecond integers in NATS KV.
// Reading them must keep working or an upgrade bricks the deployment.
func TestUnmarshalAcceptsLegacyNanoseconds(t *testing.T) {
	legacy := `{
		"batch_size": 100,
		"batch_wait": 5000000000,
		"drain_timeout": 30000000000,
		"retry": {"max_retries": 3, "initial_interval": 1000000000, "max_interval": 30000000000, "enable_dlq": true}
	}`

	var got GlobalConfig
	if err := json.Unmarshal([]byte(legacy), &got); err != nil {
		t.Fatalf("legacy unmarshal: %v", err)
	}

	if got.BatchWait != 5*time.Second {
		t.Errorf("batch_wait = %v, want 5s", got.BatchWait)
	}
	if got.DrainTimeout != 30*time.Second {
		t.Errorf("drain_timeout = %v, want 30s", got.DrainTimeout)
	}
	if got.Retry.InitialInterval != time.Second {
		t.Errorf("retry.initial_interval = %v, want 1s", got.Retry.InitialInterval)
	}
	if got.Retry.MaxInterval != 30*time.Second {
		t.Errorf("retry.max_interval = %v, want 30s", got.Retry.MaxInterval)
	}
}

// The string form is what the frontend actually sends.
func TestUnmarshalAcceptsDurationStrings(t *testing.T) {
	body := `{
		"batch_size": 100,
		"batch_wait": "5s",
		"drain_timeout": "1m30s",
		"retry": {"max_retries": 3, "initial_interval": "100ms", "max_interval": "30s"}
	}`

	var got GlobalConfig
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.BatchWait != 5*time.Second {
		t.Errorf("batch_wait = %v, want 5s", got.BatchWait)
	}
	if got.DrainTimeout != 90*time.Second {
		t.Errorf("drain_timeout = %v, want 1m30s", got.DrainTimeout)
	}
	if got.Retry.InitialInterval != 100*time.Millisecond {
		t.Errorf("retry.initial_interval = %v, want 100ms", got.Retry.InitialInterval)
	}
}

func TestUnmarshalRejectsGarbageDuration(t *testing.T) {
	var got GlobalConfig
	err := json.Unmarshal([]byte(`{"batch_size":1,"batch_wait":"not-a-duration"}`), &got)
	if err == nil {
		t.Fatal("expected an error for an unparseable duration")
	}
	if !strings.Contains(err.Error(), "not-a-duration") {
		t.Errorf("error should quote the bad value, got: %v", err)
	}
}

func TestPipelineConfigRoundTrip(t *testing.T) {
	want := PipelineConfig{
		ID:        "p1",
		Name:      "pipeline one",
		Sources:   []string{"src"},
		Sinks:     []string{"snk"},
		Tables:    []string{"public.orders"},
		BatchSize: 10,
		BatchWait: 250 * time.Millisecond,
		Processors: []ProcessorConfig{{
			Name:           "mask-email",
			Type:           "mask",
			Options:        map[string]any{"maxLength": float64(8), "field_1": "email"},
			OperationTypes: []OperationType{OpInsert},
		}},
		Retry: &RetryConfig{MaxRetries: 2, InitialInterval: time.Second, MaxInterval: 10 * time.Second},
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "250000000") {
		t.Fatalf("batch_wait marshalled as nanoseconds: %s", data)
	}

	var got PipelineConfig
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.BatchWait != want.BatchWait {
		t.Errorf("batch_wait = %v, want %v", got.BatchWait, want.BatchWait)
	}
	if got.Retry == nil || got.Retry.InitialInterval != time.Second {
		t.Errorf("retry did not survive: %+v", got.Retry)
	}
	if len(got.Processors) != 1 {
		t.Fatalf("processors = %d, want 1", len(got.Processors))
	}
	// Opaque option keys must pass through verbatim -- they are user data,
	// not part of the config schema.
	if got.Processors[0].Options["maxLength"] != float64(8) {
		t.Errorf("option key maxLength was rewritten: %+v", got.Processors[0].Options)
	}
	if got.Processors[0].Options["field_1"] != "email" {
		t.Errorf("option key field_1 was rewritten: %+v", got.Processors[0].Options)
	}
	if len(got.Processors[0].OperationTypes) != 1 {
		t.Errorf("operation_types lost: %+v", got.Processors[0].OperationTypes)
	}
}

func TestSourceConfigRoundTrip(t *testing.T) {
	want := SourceConfig{
		ID: "src", Type: "postgres", Host: "localhost", Port: 5432,
		Database:          "app",
		BatchWait:         2 * time.Second,
		DiscoveryInterval: 30 * time.Second,
		SnapshotInterval:  time.Second,
		Schemas:           []string{"public", "sales"},
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var wire map[string]any
	_ = json.Unmarshal(data, &wire)
	for field, expect := range map[string]string{
		"batch_wait":         "2s",
		"discovery_interval": "30s",
		"snapshot_interval":  "1s",
	} {
		if wire[field] != expect {
			t.Errorf("%s = %v, want %q", field, wire[field], expect)
		}
	}

	var got SourceConfig
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.DiscoveryInterval != 30*time.Second || got.SnapshotInterval != time.Second {
		t.Errorf("intervals did not round trip: %+v", got)
	}
	if len(got.Schemas) != 2 {
		t.Errorf("schemas lost: %+v", got.Schemas)
	}
}

// A zero duration must stay zero rather than becoming an error or a default,
// since SetDefaults distinguishes "unset" from "explicitly set".
func TestZeroDurationRoundTrips(t *testing.T) {
	data, err := json.Marshal(PipelineConfig{ID: "p", Name: "n"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got PipelineConfig
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.BatchWait != 0 {
		t.Errorf("zero batch_wait = %v, want 0", got.BatchWait)
	}
}
