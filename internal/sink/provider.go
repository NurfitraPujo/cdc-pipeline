package sink

import (
	"context"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

type Sink interface {
	Name() string
	// BatchUpload sends a batch of messages to the analytical sink.
	// It should handle deduplication and ensures idempotency.
	BatchUpload(ctx context.Context, messages []protocol.Message) error
	// ApplySchema updates the sink's schema (DDL).
	ApplySchema(ctx context.Context, m protocol.Message) error
	Stop() error
}

// SchemaValidator is an optional capability, checked via a type assertion at
// the same call site that already does this for DebugCapturer
// (internal/engine/factory.go), for sinks that support a startup-time check
// that every target schema/database is usable before the pipeline is
// considered started.
//
// MULTI_SCHEMA_PLAN.md §7.4 item 2: when a sink's auto-provisioning is
// disabled, it must "validate at STARTUP that every target database exists
// and refuse to start ... never fall into per-message retry."
// PipelineFactory.CreateWorker calls ValidateSchemas on sinks implementing
// this interface, after sink.New and before the worker is returned, so a
// permanently-unsatisfiable target fails startup instead of looping.
type SchemaValidator interface {
	ValidateSchemas(ctx context.Context, tables []protocol.TableRef) error
}
