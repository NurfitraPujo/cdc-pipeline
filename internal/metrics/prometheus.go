package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RecordsSynced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "daya_pipeline_records_synced_total",
		Help: "The total number of records successfully synced to the sink",
	}, []string{"pipeline_id", "source_id", "table"})

	SyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "daya_pipeline_errors_total",
		Help: "The total number of errors encountered during sync",
	}, []string{"pipeline_id", "source_id", "table"})

	PipelineLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "daya_pipeline_lag_milliseconds",
		Help: "The current lag between source and sink in milliseconds",
	}, []string{"pipeline_id", "source_id", "table"})

	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "daya_pipeline_circuit_breaker_state",
		Help: "The state of the NATS circuit breaker (0=closed, 1=open, 2=half-open)",
	}, []string{"pipeline_id"})

	WorkerHeartbeat = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "daya_pipeline_worker_heartbeat_timestamp",
		Help: "The last heartbeat timestamp of the worker",
	}, []string{"worker_id"})
)
