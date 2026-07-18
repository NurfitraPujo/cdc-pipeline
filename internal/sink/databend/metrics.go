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
)
