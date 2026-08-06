package databend

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Sink-scoped Prometheus metrics. These live alongside the Databend sink so the
// sink can be wired up without dragging cross-package dependencies into the
// internal sink/databend namespace.
var (
	// SinkDLQTotal counts the number of dead letter events emitted by the
	// Databend sink. Labels: sink_id, table, reason.
	SinkDLQTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_sink_dlq_total",
		Help: "Total number of dead letter events emitted by the Databend sink",
	}, []string{"sink_id", "table", "reason"})

	// SinkChunksTotal counts the number of chunked REPLACE INTO statements
	// issued by the Databend sink. A batch that splits into N chunks contributes
	// N to this counter.
	SinkChunksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_sink_chunks_total",
		Help: "Total number of chunked REPLACE INTO statements issued by the Databend sink",
	}, []string{"sink_id", "table"})

	// SinkPKResolved indicates whether the sink resolved a table's primary key
	// from the Databend SHOW CREATE TABLE output (1) or fell back to the
	// built-in default (0).
	SinkPKResolved = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_sink_pk_resolved",
		Help: "Whether the sink resolved a table's primary key from Databend (1=yes, 0=fallback)",
	}, []string{"sink_id", "table"})

	// SinkDeletedAtPreservationFailuresTotal (round-5c review LOW) counts
	// fetchCurrentDeletedAt failures during uploadTableBatch's
	// tombstone-preservation step. This failure is intentionally
	// non-fatal -- the batch still writes rather than being blocked on a
	// best-effort read -- but a failure here means the batch proceeds
	// WITHOUT preserving deleted_at for any row that needed it, silently
	// reproducing the "upsert resurrects a soft-deleted row" bug for that
	// one flush. A log line alone is easy to miss; this is what makes a
	// sustained degradation (e.g. Databend read-path errors) visible on a
	// dashboard instead of only in logs.
	SinkDeletedAtPreservationFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_sink_deleted_at_preservation_failures_total",
		Help: "Total number of times the Databend sink failed to fetch current deleted_at for tombstone-preserving upserts and proceeded without preservation for that batch",
	}, []string{"sink_id", "table"})

	// SinkToastPreservationFailuresTotal (WS-7) counts fetchCurrentColumns
	// failures during uploadTableBatch's TOAST-preservation step -- the
	// step that fetches and carries forward the current value of any
	// column Postgres omitted from the WAL tuple because it is an
	// unchanged TOASTed value (protocol.ColumnKindToastedUnchanged), never
	// because it is genuinely NULL. Like
	// SinkDeletedAtPreservationFailuresTotal, this failure is non-fatal --
	// the batch still writes -- but a failure here means REPLACE INTO
	// nulls out a large column it should have left untouched for every row
	// that needed preservation in this flush.
	SinkToastPreservationFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_sink_toast_preservation_failures_total",
		Help: "Total number of times the Databend sink failed to fetch current values for TOAST-unchanged columns and proceeded without preservation for that batch",
	}, []string{"sink_id", "table"})

	// SinkSchemaTypeDivergenceTotal (WS-6) counts ApplySchema calls where an
	// incoming schema_change message declares a column type that differs
	// from what the sink already has recorded for that column. Custom
	// objects do not permit field type changes (enforced app-side in
	// daya-core), so a divergence here means either a non-custom-object
	// source table changed a column's type (ApplySchema is add-only and
	// cannot propagate it) or the two sides have drifted for some other
	// reason. Either way this is surfaced as an alert-worthy signal, not
	// silently ignored.
	SinkSchemaTypeDivergenceTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_sink_schema_type_divergence_total",
		Help: "Total number of times ApplySchema observed a declared column type that differs from the previously recorded type for that column",
	}, []string{"sink_id", "table", "column"})

	// SinkCompactionsTotal (docs/todos/custom_object_cdc_followups.md item 2)
	// counts successful OPTIMIZE TABLE <t> COMPACT statements issued by the
	// sink's throttled compaction policy. REPLACE INTO is a copy-on-write
	// MERGE that appends a snapshot version per statement, so a table under
	// continuous CDC updates needs periodic compaction to bound block count
	// and read amplification.
	SinkCompactionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_sink_compactions_total",
		Help: "Total number of successful OPTIMIZE TABLE COMPACT statements issued by the Databend sink",
	}, []string{"sink_id", "table"})

	// SinkCompactionErrorsTotal counts OPTIMIZE TABLE <t> COMPACT failures.
	// Compaction is best-effort and never fails the triggering batch, so this
	// counter is the only signal that a table's copy-on-write blocks are
	// growing unmanaged (e.g. an OPTIMIZE permission problem on the sink
	// role).
	SinkCompactionErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_sink_compaction_errors_total",
		Help: "Total number of failed OPTIMIZE TABLE COMPACT statements issued by the Databend sink",
	}, []string{"sink_id", "table"})
)
