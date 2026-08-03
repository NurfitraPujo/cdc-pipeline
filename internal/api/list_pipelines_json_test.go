package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

// Regression guard for the embedded-MarshalJSON promotion trap.
//
// ListPipelines returns each pipeline's config plus a computed "status". It
// used to do that with a struct embedding protocol.PipelineConfig. Once
// PipelineConfig grew a MarshalJSON method (to render durations as "10s"
// instead of a nanosecond integer), that method was promoted to the wrapper
// and the wrapper serialised as a bare PipelineConfig -- silently dropping
// "status", which the pipeline list UI reads for every row.
func TestEmbeddingPipelineConfigDropsSiblingFields(t *testing.T) {
	type embedding struct {
		protocol.PipelineConfig
		Status string `json:"status"`
	}

	data, err := json.Marshal(embedding{
		PipelineConfig: protocol.PipelineConfig{ID: "p1", Name: "n"},
		Status:         "healthy",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if _, ok := decoded["status"]; ok {
		t.Fatal("embedding now preserves sibling fields; the splice in " +
			"ListPipelines can be replaced with a plain embedded struct")
	}
}

// The shape ListPipelines actually emits: duration as a string AND status
// present alongside the config fields.
func TestPipelineListItemShape(t *testing.T) {
	cfg := protocol.PipelineConfig{
		ID:        "p1",
		Name:      "orders",
		Sources:   []string{"src"},
		Sinks:     []string{"snk"},
		BatchWait: 10 * time.Second,
	}

	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	extra, err := json.Marshal(map[string]string{"status": "healthy"})
	if err != nil {
		t.Fatalf("marshal extra: %v", err)
	}

	merged := make([]byte, 0, len(encoded)+len(extra))
	merged = append(merged, encoded[:len(encoded)-1]...)
	merged = append(merged, ',')
	merged = append(merged, extra[1:]...)

	var decoded map[string]any
	if err := json.Unmarshal(merged, &decoded); err != nil {
		t.Fatalf("merged output is not valid JSON: %v (%s)", err, merged)
	}

	if decoded["status"] != "healthy" {
		t.Errorf("status = %v, want healthy", decoded["status"])
	}
	if decoded["batch_wait"] != "10s" {
		t.Errorf("batch_wait = %v, want \"10s\"", decoded["batch_wait"])
	}
	if decoded["id"] != "p1" {
		t.Errorf("id = %v, want p1", decoded["id"])
	}
	if decoded["name"] != "orders" {
		t.Errorf("name = %v, want orders", decoded["name"])
	}
}
