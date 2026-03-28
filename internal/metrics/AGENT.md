# Internal Metrics: Observability

The `internal/metrics` package provides Prometheus-compatible metrics for monitoring the CDC Data Pipeline, implementing Google's [Four Golden Signals](https://sre.google/sre-book/monitoring-distributed-systems/#xref_monitoring_golden-signals) for distributed systems observability.

## The Four Golden Signals

### 1. Latency

**Metric**: `cdc_pipeline_lag_milliseconds`

**Type**: Gauge

**Labels**: `pipeline_id`, `source_id`, `table`

**Description**: The current time delay between the source database transaction commit and the successful write to the sink. This represents the end-to-end latency of the pipeline.

**Source**: Calculated in `internal/engine/consumer.go` as `now.Sub(m.Timestamp).Milliseconds()`

**Usage**: Alert when lag exceeds acceptable thresholds (e.g., > 30 seconds).

---

### 2. Traffic

**Metric**: `cdc_pipeline_records_synced_total`

**Type**: Counter

**Labels**: `pipeline_id`, `source_id`, `table`

**Description**: The total number of records successfully synced to the sink. This represents the throughput/volume of data flowing through the pipeline.

**Source**: Incremented in `internal/engine/consumer.go` after successful batch upload.

**Usage**: Calculate records-per-second (RPS) using `rate(cdc_pipeline_records_synced_total[5m])`.

---

### 3. Errors

**Metric**: `cdc_pipeline_errors_total`

**Type**: Counter

**Labels**: `pipeline_id`, `source_id`, `table`

**Description**: The total number of errors encountered during sync operations, including:
- Sink upload failures
- Schema application failures
- Message deserialization errors

**Source**: Incremented when batch uploads fail and trigger isolation mode or DLQ routing.

**Usage**: Monitor error rate with `rate(cdc_pipeline_errors_total[5m])`. Alert if error rate > 1% of traffic.

---

### 4. Saturation

**Metric**: `cdc_pipeline_circuit_breaker_state`

**Type**: Gauge

**Labels**: `pipeline_id`

**Description**: The current state of the NATS circuit breaker in the Producer:
- `0` = Closed (normal operation)
- `1` = Open (failing fast, not accepting messages)
- `2` = Half-Open (testing if service recovered)

**Source**: Updated by `gobreaker` state transitions in `internal/engine/producer.go`.

**Usage**: Saturation here represents the pipeline's ability to accept new data. When the circuit breaker is open, the pipeline is saturated and cannot process additional messages.

---

## Additional Metrics

### Worker Health

**Metric**: `cdc_pipeline_worker_heartbeat_timestamp`

**Type**: Gauge

**Labels**: `worker_id`

**Description**: Unix timestamp of the last worker heartbeat. Used to detect worker crashes or network partitions.

**Source**: Updated every 10 seconds by the pipeline worker in `cmd/pipeline/main.go`.

**Usage**: Alert if `time() - cdc_pipeline_worker_heartbeat_timestamp > 60`.

## Metric Collection

All metrics are registered with Prometheus using `promauto` and exposed via:

- **API Server**: `GET /metrics` endpoint (port 8080)
- **Pipeline Worker**: `GET /metrics` endpoint (port 8081, health port)

## Prometheus Queries

### Pipeline Health Overview

```promql
# Lag by pipeline
cdc_pipeline_lag_milliseconds

# Records per second by table
rate(cdc_pipeline_records_synced_total[5m])

# Error rate
rate(cdc_pipeline_errors_total[5m]) / rate(cdc_pipeline_records_synced_total[5m])

# Circuit breaker open pipelines
cdc_pipeline_circuit_breaker_state == 1

# Stale workers (no heartbeat in 60s)
time() - cdc_pipeline_worker_heartbeat_timestamp > 60
```

## Implementation Notes

- Metrics use `promauto.NewCounterVec` and `promauto.NewGaugeVec` for automatic registration
- Label cardinality is bounded by pipeline/table count
- No custom histograms are currently implemented (lag uses gauge, not histogram)
- Consider adding histogram buckets for latency distribution if p50/p99 percentiles are needed
