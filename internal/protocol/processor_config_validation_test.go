package protocol

import "testing"

// WS-8 item 1: a processor with an empty operation_types list used to be
// skipped entirely at runtime (Consumer.processMessages, engine/consumer.go)
// with no warning and no match-all default, so a typo'd or forgotten
// operation_types field silently turned a configured transformer into a
// no-op. ProcessorConfig.Validate() now rejects that at config-load time
// instead of letting the pipeline start and misbehave.

func TestProcessorConfig_Validate_RejectsEmptyOperationTypes(t *testing.T) {
	c := ProcessorConfig{
		Name: "p1",
		Type: "mask",
		// OperationTypes deliberately left empty.
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected an error for a processor with no operation_types, got nil")
	}
}

func TestProcessorConfig_Validate_AcceptsPopulatedOperationTypes(t *testing.T) {
	c := ProcessorConfig{
		Name:           "p1",
		Type:           "mask",
		OperationTypes: []OperationType{OpInsert, OpUpdate},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("expected no error for a valid processor config, got: %v", err)
	}
}

// PipelineConfig.Validate() must cascade into each processor's own
// Validate() -- otherwise a misconfigured processor is invisible at the
// point where the pipeline is actually accepted (the API handlers all call
// PipelineConfig.Validate(), not ProcessorConfig.Validate() directly).
func TestPipelineConfig_Validate_RejectsProcessorWithEmptyOperationTypes(t *testing.T) {
	c := PipelineConfig{
		ID:      "p1",
		Name:    "n",
		Sources: []string{"s1"},
		Sinks:   []string{"snk1"},
		Processors: []ProcessorConfig{
			{Name: "broken", Type: "mask"}, // no OperationTypes
		},
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected PipelineConfig.Validate() to reject a processor with empty operation_types, got nil")
	}
}

func TestPipelineConfig_Validate_AcceptsProcessorWithOperationTypes(t *testing.T) {
	c := PipelineConfig{
		ID:      "p1",
		Name:    "n",
		Sources: []string{"s1"},
		Sinks:   []string{"snk1"},
		Processors: []ProcessorConfig{
			{Name: "ok", Type: "mask", OperationTypes: []OperationType{OpInsert}},
		},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("expected no error for a valid pipeline config, got: %v", err)
	}
}
