package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RecordsSynced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_pipeline_records_synced_total",
		Help: "The total number of records successfully synced to the sink",
	}, []string{"pipeline_id", "source_id", "table"})

	SyncErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_pipeline_errors_total",
		Help: "The total number of errors encountered during sync",
	}, []string{"pipeline_id", "source_id", "table"})

	PipelineLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_pipeline_lag_milliseconds",
		Help: "The current lag between source and sink in milliseconds",
	}, []string{"pipeline_id", "source_id", "table"})

	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_pipeline_circuit_breaker_state",
		Help: "The state of the NATS circuit breaker (0=closed, 1=open, 2=half-open)",
	}, []string{"pipeline_id"})

	WorkerHeartbeat = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_pipeline_worker_heartbeat_timestamp",
		Help: "The last heartbeat timestamp of the worker",
	}, []string{"worker_id"})

	APICleanupRuns = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cdc_api_cleanup_runs_total",
		Help: "The total number of stale heartbeat cleanup runs triggered by ListPipelines",
	})

	NatsPublisherPendingAcks = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cdc_nats_publisher_pending_acks",
		Help: "The number of pending ACKs during batch publishing",
	})

	// The following (WS-9) give transformers -- the nats/protobuf transformer
	// in particular -- liveness observability. Before this, a transformer
	// that matched zero rows (see the schema-filter defect this measures) was
	// indistinguishable from a working one: nothing recorded that it ran, let
	// alone what it did to each record.

	TransformRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_transform_requests_total",
		Help: "The total number of transform RPCs issued by a transformer, by outcome (success, error)",
	}, []string{"pipeline_id", "transformer", "outcome"})

	TransformDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_transform_duration_seconds",
		Help:    "The duration of a transform round trip (request build + RPC + response parse)",
		Buckets: prometheus.DefBuckets,
	}, []string{"pipeline_id", "transformer"})

	TransformRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_transform_records_total",
		Help: "The total number of records handled by a transformer, by outcome (transformed, passthrough, dropped, failed)",
	}, []string{"pipeline_id", "transformer", "outcome"})

	TransformRequestBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_transform_request_bytes",
		Help:    "The marshalled size of transform request payloads",
		Buckets: prometheus.ExponentialBuckets(64, 4, 10), // 64B .. ~16MB
	}, []string{"pipeline_id", "transformer"})

	TransformResponseBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_transform_response_bytes",
		Help:    "The size of transform response payloads",
		Buckets: prometheus.ExponentialBuckets(64, 4, 10),
	}, []string{"pipeline_id", "transformer"})

	// TransformChunksPerBatch (WS-3.4) makes an oversized batch visible
	// rather than silently slow: 1 means the batch fit in a single request,
	// >1 means buildTransformRequest's payload-size guard split it.
	TransformChunksPerBatch = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_transform_chunks_per_batch",
		Help:    "The number of transform request chunks a single TransformBatch call was split into (WS-3 payload-size guard)",
		Buckets: []float64{1, 2, 3, 4, 5, 8, 12, 20, 32, 64},
	}, []string{"pipeline_id", "transformer"})

	// ConsumerTransformInvocationsTotal counts every time Consumer.processMessages
	// considers a configured transformer for a batch, labeled by outcome, *before*
	// any per-record transform RPC happens. This exists because
	// TransformRequestsTotal/TransformRecordsTotal only increment inside the
	// transformer implementation itself -- a pipeline whose transformer is skipped
	// entirely (e.g. the empty-OperationTypes skip in engine/consumer.go) or whose
	// batch simply matches zero rows produces no series there at all, which is
	// indistinguishable from "not deployed". This counter is emitted at the
	// consumer call site regardless of whether the transformer ends up invoked, so
	// operators can tell "deployed but skipping everything" apart from "not wired
	// up".
	ConsumerTransformInvocationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_consumer_transform_invocations_total",
		Help: "The total number of times the consumer considered a transformer for a batch, by outcome (invoked, skipped_no_operation_types, skipped_no_match)",
	}, []string{"pipeline_id", "transformer", "outcome"})
)
