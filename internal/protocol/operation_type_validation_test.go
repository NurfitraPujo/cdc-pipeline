package protocol

import (
	"strings"
	"testing"
)

func TestProcessorRejectsUnknownOperationType(t *testing.T) {
	p := ProcessorConfig{
		Name: "mask-email",
		Type: "mask",
		// A plausible typo. This used to pass validation, match no message at
		// runtime, and leave the pipeline reporting healthy while transforming
		// nothing -- exactly what requiring a non-empty list was meant to stop.
		OperationTypes: []OperationType{"insrt"},
	}

	err := p.Validate()
	if err == nil {
		t.Fatal("expected an unknown operation type to be rejected")
	}
	if !strings.Contains(err.Error(), "insrt") {
		t.Errorf("error should name the offending value, got: %v", err)
	}
}

func TestProcessorRejectsUnknownAmongValidOperationTypes(t *testing.T) {
	p := ProcessorConfig{
		Name:           "mask-email",
		Type:           "mask",
		OperationTypes: []OperationType{OpInsert, "bogus", OpUpdate},
	}

	if err := p.Validate(); err == nil {
		t.Fatal("expected rejection when one entry of several is unknown")
	}
}

func TestProcessorAcceptsEveryKnownOperationType(t *testing.T) {
	all := []OperationType{
		OpInsert, OpUpdate, OpDelete,
		OpSnapshot, OpSchemaChange, OpSchemaChangeAck,
	}

	for _, op := range all {
		p := ProcessorConfig{
			Name:           "p",
			Type:           "mask",
			OperationTypes: []OperationType{op},
		}
		if err := p.Validate(); err != nil {
			t.Errorf("operation type %q was rejected: %v", op, err)
		}
	}

	// And all of them together.
	p := ProcessorConfig{Name: "p", Type: "mask", OperationTypes: all}
	if err := p.Validate(); err != nil {
		t.Errorf("the full set was rejected: %v", err)
	}
}

func TestProcessorStillRejectsAnEmptyOperationTypeList(t *testing.T) {
	p := ProcessorConfig{Name: "p", Type: "mask"}
	if err := p.Validate(); err == nil {
		t.Fatal("expected an empty operation type list to be rejected")
	}
}

// Case matters: the constants are lowercase and the consumer compares exactly.
func TestProcessorRejectsWrongCaseOperationType(t *testing.T) {
	p := ProcessorConfig{
		Name:           "p",
		Type:           "mask",
		OperationTypes: []OperationType{"INSERT"},
	}
	if err := p.Validate(); err == nil {
		t.Fatal("expected an uppercase operation type to be rejected")
	}
}
