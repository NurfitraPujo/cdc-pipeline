package protocol

import (
	"fmt"
	"time"
)

// ReconciliationProgressKey addresses the persisted progress record for
// WS-7's best-effort delete-reconciliation sweep -- distinct from
// LifecycleStateKey's PipelineLifecycleRecord.Reconciliation, which carries
// only the coarse idle/running/stale sub-status the UI shows (plan section
// 4.2). This key holds the sweep's own bookkeeping: which chunk it is
// resuming from, so the sweep is interruptible and resumable across manager
// restarts without losing forward progress or re-scanning already-clean
// chunks (plan WS-7 shape: "interruptible, resumable, rate-limitable,
// progress-reportable").
func ReconciliationProgressKey(id string) string {
	return fmt.Sprintf("%s%s.reconciliation", PrefixPipelineState, id)
}

// ReconciliationProgress is the persisted state of one pipeline's
// delete-reconciliation sweep. It is written by the sweep engine
// (internal/config/reconciliation.go) after every chunk -- not batched --
// so a crash or restart between ticks resumes at the next chunk rather than
// from the beginning, and never re-does work already committed to the sink.
type ReconciliationProgress struct {
	// NextChunkOrdinal is the zero-based index, into the pipeline's full
	// ordered list of integer_range chunks (across all its tables), of the
	// next chunk the sweep has not yet compared. It is a flat ordinal
	// rather than a per-table cursor because the chunk list itself is
	// re-read from cdc_snapshot_chunks every sweep tick (cheap metadata
	// query) rather than cached, so table additions/removals are picked up
	// automatically; a flat ordinal into that freshly-read list is the
	// simplest thing that stays valid across such a re-read as long as the
	// list's ordering is stable (table_schema, table_name, chunk_index).
	NextChunkOrdinal int `json:"next_chunk_ordinal"`
	// ChunksTotal is the size of the chunk list as of the most recent
	// tick, reported alongside NextChunkOrdinal purely so callers (the API,
	// the UI) can show "chunk N of M" progress without re-querying
	// cdc_snapshot_chunks themselves.
	ChunksTotal int `json:"chunks_total"`
	// RowsReconciled is a running total of sink rows this sweep has soft-
	// deleted because they were absent from the corresponding source PK
	// range. Reset to zero when a new sweep starts (StartedAt advances).
	RowsReconciled int64 `json:"rows_reconciled"`
	// StartedAt is when the current sweep attempt began -- set once, the
	// first tick a pipeline is observed with reconciliation stale (or
	// running with no prior progress), and never touched by the lifecycle
	// record's own UpdatedAt churn. This is deliberately its own field, not
	// derived from PipelineLifecycleRecord.UpdatedAt: writing
	// Reconciliation: Running every tick would otherwise reset the
	// staleness clock this field measures against, defeating the
	// alert-past-threshold signal invariant 5 requires.
	StartedAt time.Time `json:"started_at"`
	// UpdatedAt is set on every tick that makes progress (or confirms
	// there is nothing yet to do), for operator visibility into whether the
	// sweep is still alive.
	UpdatedAt time.Time `json:"updated_at"`
}
