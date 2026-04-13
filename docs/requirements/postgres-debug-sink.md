# PostgreSQL Debug Sink - Detailed Requirements (Revised v3)

## Overview
A PostgreSQL debug sink that acts as a data logger for data engineers to observe data transformation in the CDC pipeline. It captures both original and transformed payloads at different pipeline stages, enabling debugging of data quality issues and transformation logic.

---

## Architecture Integration

### Core Architecture Change: Per-Sink Consumers

The pipeline must be rearchitected so that **each sink has its own dedicated consumer** within the pipeline. This provides full isolation between sinks: a failure or slowdown in one sink does not affect others.

**Current architecture (single consumer, single sink):**
```
NATS → Consumer → Transformers → Sink (Databend)
```

**New architecture (one consumer per sink):**
```
NATS ──┬── Consumer₁ (for Databend)  → Transformers → DatabendSink
       └── Consumer₂ (for Debug)     → Transformers → DebugSink
                                           ↑ captures before/after
```

Each consumer:
- Has its own NATS JetStream durable subscription with a unique name
- Independently processes messages from the same stream
- Applies the same pipeline transformers
- Maintains its own egress checkpoints
- Has its own retry/DLQ handling
- Runs in its own goroutine within the pipeline

### Why Per-Sink Consumers

| Aspect | MultiSink (fan-out) | Per-Sink Consumers |
|--------|---------------------|---------------------|
| Failure isolation | One sink failure blocks all | Each sink isolated |
| Retry handling | Shared retry state | Each sink has own retry |
| Checkpoints | Shared egress LSN | Per-sink egress LSN |
| Backpressure | Slow sink blocks all | Slow sink only affects itself |
| NATS ACK | Single ACK point | Independent ACK per consumer |
| Debug capture | Must hook into shared consumer | Natural before/after capture |

### NATS JetStream Subscription Changes

**Current**: Single durable consumer per pipeline.
```
Durable name: "cdc-worker-{pipelineID}"
Topic: "cdc_pipeline_{pipelineID}_ingest"
```

**New**: Per-sink durable consumers with sink identity.
```
Durable name: "cdc-worker-{pipelineID}-sink-{sinkID}"
Topic: "cdc_pipeline_{pipelineID}_ingest" (shared)
```

This ensures:
- Each sink gets its own independent consumer position in JetStream
- All consumers see every message (fan-out, not competing consumers)
- Each consumer tracks its own egress LSN independently

### Egress Checkpoint Changes

**Current**: Egress checkpoint is per pipeline+source+table.
```
EgressCheckpointKey(pid, sid, table) = "cdc.pipeline.{pid}.sources.{sid}.tables.{table}.egress_checkpoint"
```

**New**: Egress checkpoint includes sink ID for independent tracking.
```
EgressCheckpointKey(pid, sid, sinkID, table) = "cdc.pipeline.{pid}.sources.{sid}.sinks.{sinkID}.tables.{table}.egress_checkpoint"
```

Similarly, `TableStatsKey` should include sink ID:
```
TableStatsKey(pid, sid, sinkID, table) = "cdc.pipeline.{pid}.sources.{sid}.sinks.{sinkID}.tables.{table}.stats"
```

### Pipeline Struct Changes

**Current** (`internal/engine/pipeline.go`):
```go
type Pipeline struct {
    id       string
    producer *Producer
    consumer *Consumer          // Single consumer
    config   protocol.PipelineConfig
    ctx      context.Context
    cancel   context.CancelFunc
    wg       sync.WaitGroup
    finished chan struct{}
}
```

**New**:
```go
type Pipeline struct {
    id        string
    producer  *Producer
    consumers []*Consumer         // One consumer per sink
    config    protocol.PipelineConfig
    ctx       context.Context
    cancel    context.CancelFunc
    wg        sync.WaitGroup
    finished  chan struct{}
}
```

**Pipeline.Start()**: Spawns one goroutine per consumer, plus the producer goroutine.

### Consumer Changes

**Current** (`internal/engine/consumer.go`):
```go
type Consumer struct {
    pipelineID      string
    subscriber      stream.Subscriber
    publisher       stream.Publisher
    sink            sink.Sink            // Single sink
    transformers    []transformer.Transformer
    kv              nats.KeyValue
    batchSize       int
    batchWait       time.Duration
    retryConfig     protocol.RetryConfig
    // ...
}
```

**New**:
```go
type Consumer struct {
    pipelineID      string
    sinkID          string                // NEW: identifies which sink this consumer serves
    subscriber      stream.Subscriber
    publisher       stream.Publisher
    sink            sink.Sink
    transformers    []transformer.Transformer
    kv              nats.KeyValue
    batchSize       int
    batchWait       time.Duration
    retryConfig     protocol.RetryConfig
    // ... existing fields unchanged
    
    // NEW: Optional hooks for debug capture (nil for regular consumers)
    preTransformHook  PreTransformHook
    postTransformHook PostTransformHook
}

type PreTransformHook func(ctx context.Context, pipelineID string, messages []protocol.Message)
type PostTransformHook func(ctx context.Context, pipelineID string, originals []protocol.Message, transformed []protocol.Message, filteredIndices []int)
```

The hooks allow the debug sink's consumer to capture before/after state without modifying the core `Consumer` logic beyond adding two optional calls.

### Main Factory Changes

**Current** (`cmd/pipeline/main.go`):
```go
sinkID := cfg.Sinks[0]  // Only uses first sink
// ... creates single sink, single consumer
```

**New**:
```go
var consumers []*engine.Consumer
var capturer sink.DebugCapturer // nil unless debug sink is configured

for _, sinkID := range cfg.Sinks {
    sinkKey := protocol.SinkConfigKey(sinkID)
    sinkEntry, err := kv.Get(sinkKey)
    // ...
    
    var snk sink.Sink
    switch snkCfg.Type {
    case "databend":
        snk, err = databend.NewDatabendSink(sinkID, snkCfg.DSN)
    case "postgres_debug":
        var opts postgresdebug.DebugSinkOptions
        opts, err = postgresdebug.ParseOptions(snkCfg.Options)
        if err != nil { return nil, err }
        snk, err = postgresdebug.NewDebugSink(sinkID, snkCfg.DSN, opts)
        if err != nil { return nil, err }
        capturer = snk.(sink.DebugCapturer) // Implements both interfaces
    default:
        return nil, fmt.Errorf("unknown sink type: %s", snkCfg.Type)
    }
    
    // Each sink gets its own subscriber with unique durable name
    sub, err := nats.NewNatsSubscriber(
        natsURL,
        fmt.Sprintf("cdc-worker-%s-sink-%s", id, sinkID),  // Unique durable name
        maxAckPending,
        30*time.Second,
    )
    
    // Set up hooks if this is a debug sink
    var preHook engine.PreTransformHook
    var postHook engine.PostTransformHook
    if capturer != nil && snkCfg.Type == "postgres_debug" {
        preHook = capturer.CaptureBefore
        postHook = capturer.CaptureAfter
    }
    
    cons := engine.NewConsumer(id, sinkID, sub, pub, snk, transformers, kv, 
        cfg.BatchSize, cfg.BatchWait, retry, preHook, postHook)
    consumers = append(consumers, cons)
}

pipe := engine.NewPipeline(id, prod, consumers, cfg)
```

### Debug Sink Capture Mechanism

The debug sink implements **both** `Sink` and `DebugCapturer`:

```go
// internal/sink/debug_capturer.go
type DebugCapturer interface {
    CaptureBefore(ctx context.Context, pipelineID string, messages []protocol.Message)
    CaptureAfter(ctx context.Context, pipelineID string, originals []protocol.Message, transformed []protocol.Message, filteredIndices []int)
}
```

**Consumer's `processMessages()` with hooks:**
```go
func (c *Consumer) processMessages(ctx context.Context, msgs []protocol.Message) []protocol.Message {
    // Call pre-transform hook (captures "before" state for debug sink)
    if c.preTransformHook != nil {
        c.preTransformHook(ctx, c.pipelineID, msgs)
    }

    if len(c.transformers) == 0 {
        // No transformers, call post-transform hook with same messages
        if c.postTransformHook != nil {
            c.postTransformHook(ctx, c.pipelineID, msgs, msgs, nil)
        }
        return msgs
    }

    processed := make([]protocol.Message, 0, len(msgs))
    filteredIndices := make([]int, 0)

    for i, m := range msgs {
        current := &m
        keep := true
        var err error

        for _, t := range c.transformers {
            current, keep, err = t.Transform(ctx, current)
            if err != nil {
                log.Error().Err(err).Str("pipeline_id", c.pipelineID).
                    Str("transformer", t.Name()).Msg("Transformation error")
            }
            if !keep {
                break
            }
        }

        if !keep || current == nil {
            filteredIndices = append(filteredIndices, i)
        }
        if keep && current != nil {
            processed = append(processed, *current)
        }
    }

    // Call post-transform hook (captures "after" state for debug sink)
    if c.postTransformHook != nil {
        c.postTransformHook(ctx, c.pipelineID, msgs, processed, filteredIndices)
    }

    return processed
}
```

**Key point**: The hooks are a thin, non-invasive addition. Regular consumers have `nil` hooks and behave exactly as before. Only debug-sink consumers set the hooks.

### Handling Special Operations

| Op Value | Description | Debug Sink Handling |
|----------|-------------|---------------------|
| `insert` | New row inserted | Capture before/after via hooks |
| `update` | Row updated | Capture before/after via hooks |
| `delete` | Row deleted | Capture before/after via hooks |
| `snapshot` | Initial table dump | Capture before/after via hooks |
| `schema_change` | DDL change | Record in `BatchUpload` as `capture_stage='schema_change'` |
| `drain_marker` | Pipeline drain signal | **Skip** — internal control message |

**Important**: `schema_change` messages go through `ApplySchema()` on the regular path. The debug sink's `BatchUpload()` also receives them via its own consumer and records them. `drain_marker` messages are never captured.

### Handling Filtered Messages

When a transformer filters out a message (`keep=false`):
1. The "before" record is already captured by `CaptureBefore()`
2. No "after" record exists (message was dropped)
3. The debug sink marks the "before" record with `filtered=TRUE`
4. The `postTransformHook` receives `filteredIndices` to know which messages were filtered

### Error Isolation

Each consumer operates independently:
- If the debug sink's PostgreSQL is down, only the debug consumer retries/backs off
- The Databend consumer continues processing normally
- Each consumer has its own NATS ACK/NACK cycle
- Each consumer maintains its own egress checkpoints
- A consumer crash only affects its own sink, not others

### SinkConfig Changes

**Current** ((`internal/protocol/config.go`):
```go
type SinkConfig struct {
    ID   string `msg:"id" yaml:"id" json:"id"`
    Type string `msg:"type" yaml:"type" json:"type"`
    DSN  string `msg:"dsn" yaml:"dsn" json:"dsn"`
}
```

**New**:
```go
type SinkConfig struct {
    ID      string                 `msg:"id" yaml:"id" json:"id"`
    Type    string                 `msg:"type" yaml:"type" json:"type"`
    DSN     string                 `msg:"dsn" yaml:"dsn" json:"dsn"`
    Options map[string]interface{} `msg:"options" yaml:"options" json:"options"`
}

func (s SinkConfig) Validate() error {
    return validation.ValidateStruct(&s,
        validation.Field(&s.ID, validation.Required, is.Alphanumeric),
        validation.Field(&s.Type, validation.Required, validation.In("databend", "postgres_debug")),
        validation.Field(&s.DSN, validation.Required),
    )
}
```

---

## Data Storage Schema

### Table Structure (Single Table Design)
Using **one shared table** with `pipeline_id` column for operational simplicity.

```sql
CREATE TABLE cdc_debug_messages (
    id BIGSERIAL PRIMARY KEY,
    correlation_id UUID NOT NULL,
    pipeline_id VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    sink_id VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255),
    operation_type VARCHAR(20) NOT NULL,
    lsn BIGINT,
    primary_key TEXT,
    message_uuid VARCHAR(255),
    capture_stage VARCHAR(20) NOT NULL,
    filtered BOOLEAN DEFAULT FALSE,
    transformer_names TEXT[],
    payload JSONB NOT NULL,
    payload_hash VARCHAR(64),
    processing_latency_ms INTEGER,
    captured_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    message_timestamp TIMESTAMP WITH TIME ZONE,
    CONSTRAINT valid_stage CHECK (
        capture_stage IN ('before', 'after', 'schema_change')
    )
);

-- Core Indexes (always created)
CREATE INDEX idx_cdc_debug_captured_at ON cdc_debug_messages(captured_at DESC);
CREATE INDEX idx_cdc_debug_correlation ON cdc_debug_messages(correlation_id);
CREATE INDEX idx_cdc_debug_pipeline_lookup 
    ON cdc_debug_messages(pipeline_id, sink_id, table_name, captured_at DESC);

-- Optional Indexes (configurable, default OFF for high-volume)
-- CREATE INDEX idx_cdc_debug_operation ON cdc_debug_messages(operation_type);
-- CREATE INDEX idx_cdc_debug_lsn ON cdc_debug_messages(lsn);
-- CREATE INDEX idx_cdc_debug_payload_gin 
--     ON cdc_debug_messages USING GIN(payload jsonb_path_ops);
-- CREATE INDEX idx_cdc_debug_filtered ON cdc_debug_messages(filtered) WHERE filtered = TRUE;
```

Note: `sink_id` column added to `cdc_debug_messages` and `idx_cdc_debug_pipeline_lookup` to support per-sink queries, since each sink runs its own consumer.

### Why Single Table?
- **Operational simplicity**: One table to monitor, backup, and optimize
- **Cross-pipeline queries**: Easy to compare data across pipelines
- **No orphan risk**: Pipeline renames don't leave orphaned tables
- **Partitioning ready**: Can partition by `pipeline_id` or `captured_at` if needed

### Row Model Explained

Each message can generate 1-3 rows depending on what happens:

**Normal message (not filtered):**
| correlation_id | capture_stage | filtered | payload | processing_latency_ms |
|----------------|---------------|----------|---------|----------------------|
| uuid-123 | before | false | {original data} | NULL |
| uuid-123 | after | false | {transformed data} | 45 |

**Filtered message (transformer dropped it):**
| correlation_id | capture_stage | filtered | payload | processing_latency_ms |
|----------------|---------------|----------|---------|----------------------|
| uuid-456 | before | true | {original data} | NULL |

**Schema change message:**
| correlation_id | capture_stage | filtered | payload | processing_latency_ms |
|----------------|---------------|----------|---------|----------------------|
| uuid-789 | schema_change | false | {schema metadata} | NULL |

### Message Data Deserialization

The `protocol.Message` struct has two payload paths:
1. `m.Data` — populated `map[string]interface{}` (preferred, already deserialized)
2. `m.Payload` — raw msgpack or JSON bytes (used when Data is nil)

The debug sink must handle both, matching the Databend sink's approach:

```go
func extractPayload(m protocol.Message) (map[string]interface{}, error) {
    if m.Data != nil {
        return m.Data, nil
    }
    if m.Payload != nil {
        var data map[string]interface{}
        if err := msgpack.Unmarshal(m.Payload, &data); err != nil {
            if err2 := json.Unmarshal(m.Payload, &data); err2 != nil {
                return nil, fmt.Errorf("failed to unmarshal payload: %w", err2)
            }
        }
        return data, nil
    }
    return nil, nil
}
```

---

## Configuration Structure

Sink-specific config uses the new `Options` field on `SinkConfig`.

```yaml
sinks:
  - id: databend-analytics
    type: databend
    dsn: "http://root:@localhost:8000"
    
  - id: debug_sink_main
    type: postgres_debug
    dsn: "postgres://user:pass@localhost:5432/debugdb"
    options:
      table_name: "cdc_debug_messages"
      
      retention:
        mode: "age"
        max_age: "168h"  # 7 days in hours (Go time.Duration format)
        cleanup_interval: "1h"
      
      filters:
        include_tables: ["orders", "order_items"]
        exclude_tables: ["logs", "temp_*"]
        include_operations: ["insert", "update", "delete", "snapshot"]
        
        conditions:
          - field: "table_name"
            operator: "in"
            value: ["orders", "order_items"]
          - field: "operation_type"
            operator: "eq"
            value: "delete"
      
      sampling:
        mode: "percentage"
        value: 10
        table_overrides:
          orders:
            mode: "percentage"
            value: 50
          logs:
            mode: "systematic"
            value: 1000
      
      capture:
        stages: ["before", "after"]
        include_payload: true
        include_metrics: true
      
      indexes:
        operation_type: false
        lsn: false
        payload_gin: false

pipelines:
  - id: "pipe-01"
    name: "Main Data Sync"
    sources: ["postgres-primary"]
    sinks: ["databend-analytics", "debug_sink_main"]  # Multiple sinks
    processors:
      - name: "mask_emails"
        type: "mask"
        options:
          fields: ["email"]
    tables: ["users", "orders", "products"]
```

### Configuration Reference

**Retention Modes:**

| Mode | Description | Required Fields |
|------|-------------|-----------------|
| age | Delete records older than max_age | max_age (Go duration: "168h", "24h", "30m") |
| count | Keep only last N records per pipeline | max_count (integer) |
| disabled | No automatic cleanup | None |

**Note**: Go's `time.Duration` doesn't support "d" suffix. Use hours (e.g., "168h" for 7 days).

**Sampling Modes:**

| Mode | Description | Value Meaning |
|------|-------------|---------------|
| percentage | Random sampling | 0-100 (percent) |
| systematic | Every Nth record | N (interval) |
| disabled | Capture all records | Ignored |

**Filter Operators:**

| Operator | Description | Value Type |
|----------|-------------|------------|
| eq | Equals | string |
| ne | Not equals | string |
| in | In list | string array |
| contains | Contains substring | string |

---

## Functional Requirements

### Data Capture

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-001 | Capture raw message data BEFORE any transformers are applied | P0 |
| FR-002 | Capture transformed message data AFTER all transformers complete | P0 |
| FR-003 | Link before/after records with the same correlation_id | P0 |
| FR-004 | Mark messages filtered out by transformers with `filtered=true` | P0 |
| FR-005 | Capture schema_change messages as single records | P1 |
| FR-006 | Skip drain_marker messages (never capture them) | P0 |
| FR-007 | Handle both m.Data and m.Payload (msgpack/JSON) deserialization | P0 |
| FR-008 | Store transformer chain names in transformer_names array | P1 |
| FR-009 | Calculate and store processing latency for 'after' stage | P1 |
| FR-010 | Store payload hash for quick change detection | P2 |

### Per-Sink Isolation

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-011 | Each sink has its own NATS JetStream durable subscription | P0 |
| FR-012 | Each sink maintains its own egress checkpoint (per pipeline+source+sink+table) | P0 |
| FR-013 | Debug sink failure does not block other sinks | P0 |
| FR-014 | Debug sink has its own retry/DLQ handling | P1 |

### Filtering and Sampling

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-015 | Support table inclusion/exclusion lists with wildcards | P1 |
| FR-016 | Support operation type filtering | P1 |
| FR-017 | Support structured field-based conditions | P2 |
| FR-018 | Support percentage-based random sampling | P1 |
| FR-019 | Support systematic (every Nth) sampling | P1 |
| FR-020 | Allow per-table sampling overrides | P2 |

### Retention Management

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-021 | Automatic cleanup based on max_age (time-based) | P1 |
| FR-022 | Automatic cleanup based on max_count (count-based) | P1 |
| FR-023 | Configurable cleanup interval | P2 |
| FR-024 | Partition table by captured_at for efficient cleanup | P2 |

### Query Support

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-025 | Enable time-based queries with captured_at index | P0 |
| FR-026 | Enable correlation-based queries with correlation_id index | P0 |
| FR-027 | Enable pipeline/sink/table filtering with composite index | P1 |
| FR-028 | Support JSONB operators for payload inspection | P1 |

---

## Non-Functional Requirements

### Performance

| ID | Requirement | Priority |
|----|-------------|----------|
| NFR-001 | Use connection pooling for parallel writes | P0 |
| NFR-002 | Support batch inserts (like Databend sink) | P0 |
| NFR-003 | Debug sink failures must not block main pipeline | P0 |
| NFR-004 | Sampling and filtering applied before database write | P1 |
| NFR-005 | Configurable indexes to balance read/write performance | P1 |
| NFR-006 | Debug capture hook errors logged but never block transformation | P0 |

### Storage Efficiency

| ID | Requirement | Priority |
|----|-------------|----------|
| NFR-007 | Use JSONB for payloads (PostgreSQL native compression) | P1 |
| NFR-008 | Partition tables by time for efficient cleanup (future) | P2 |
| NFR-009 | Support table-level retention policies | P2 |

### Observability

| ID | Requirement | Priority |
|----|-------------|----------|
| NFR-010 | Expose Prometheus metrics for records captured, filtered, sampled, dropped | P1 |
| NFR-011 | Expose storage bytes and cleanup duration metrics | P2 |
| NFR-012 | Use existing zerolog logger for debug sink events | P1 |

### Reliability

| ID | Requirement | Priority |
|----|-------------|----------|
| NFR-013 | Table auto-creation on startup if not exists | P0 |
| NFR-014 | Correlation ID uniqueness across restarts (use UUID v4) | P1 |
| NFR-015 | Graceful cleanup goroutine shutdown on Stop() | P1 |
| NFR-016 | Handle PostgreSQL connection loss with reconnection | P1 |

---

## Implementation Plan

### Phase 1: Per-Sink Consumer Architecture (Prerequisite)

This phase rearchitects the pipeline to support multiple sinks with dedicated consumers. It does NOT add the debug sink yet — it only enables multi-sink support.

**1. Update PipelineConfig and SinkConfig** (`internal/protocol/config.go`)
- Add `Options map[string]interface{}` to `SinkConfig`
- Update validation to allow `"postgres_debug"` type
- No change to `PipelineConfig.Sinks` (already `[]string`)

**2. Update Egress Checkpoint Keys** (`internal/protocol/config.go`)
- `EgressCheckpointKey(pid, sid, table)` → `EgressCheckpointKey(pid, sid, sinkID, table)`
- `TableStatsKey(pid, sid, table)` → `TableStatsKey(pid, sid, sinkID, table)`
- These are breaking changes — existing checkpoints will need migration

**3. Update Consumer** (`internal/engine/consumer.go`)
- Add `sinkID string` field
- Add optional `preTransformHook` and `postTransformHook` fields
- Modify `processMessages()` to call hooks if set
- Update `NewConsumer()` signature to accept `sinkID`, hooks
- Update checkpoint keys to include `sinkID`

**4. Update Pipeline** (`internal/engine/pipeline.go`)
- Change `consumer *Consumer` to `consumers []*Consumer`
- `Start()` spawns one goroutine per consumer
- `Drain()` signals all consumers to drain
- `Shutdown()` cancels context for all goroutines

**5. Update Factory** (`cmd/pipeline/main.go`)
- Loop over all `cfg.Sinks` instead of `cfg.Sinks[0]`
- Create a `NatsSubscriber` per sink with unique durable name: `cdc-worker-{pipelineID}-sink-{sinkID}`
- Create a `Consumer` per sink
- Pass appropriate hooks for debug sinks

**6. Add `DebugCapturer` interface** (`internal/sink/debug_capturer.go`)
```go
type DebugCapturer interface {
    CaptureBefore(ctx context.Context, pipelineID string, messages []protocol.Message)
    CaptureAfter(ctx context.Context, pipelineID string, originals []protocol.Message, transformed []protocol.Message, filteredIndices []int)
}
```

**7. Update Tests**
- Add test for multi-consumer pipeline
- Add test for per-sink egress checkpoints
- Add test for hook-based before/after capture

### Phase 2: Debug Sink Implementation

**1. Create `internal/sink/postgresdebug/` package**
- `sink.go` — implements `Sink` and `DebugCapturer`
- `config.go` — parse `SinkConfig.Options` into typed config
- `retention.go` — background cleanup goroutine
- `filter.go` — table/operation/condition filtering
- `sampling.go` — percentage and systematic sampling
- `hooks.go` — `CaptureBefore` and `CaptureAfter` implementations

**2. Sink registration** (in `cmd/pipeline/main.go`)
```go
case "postgres_debug":
    opts := postgresdebug.ParseOptions(snkCfg.Options)
    debugSnk, err := postgresdebug.NewDebugSink(sinkID, snkCfg.DSN, opts)
    // ...
    // Set hooks on this sink's consumer
    preHook = debugSnk.CaptureBefore
    postHook = debugSnk.CaptureAfter
```

**3. Debug Sink `BatchUpload` Implementation**
The debug sink's `BatchUpload` handles `schema_change` messages. Normal data messages are captured via hooks.
```go
func (s *DebugSink) BatchUpload(ctx context.Context, messages []protocol.Message) error {
    for _, m := range messages {
        if m.Op == "schema_change" && s.shouldCapture(m) {
            if err := s.captureSchemaChange(ctx, m); err != nil {
                log.Error().Err(err).Msg("Debug sink: failed to capture schema_change")
                // Don't return error — don't block other processing
            }
        }
        // drain_marker: skip
        // insert/update/delete/snapshot: already captured via hooks
    }
    return nil
}
```

**4. Debug Sink `ApplySchema`**
No-op — the debug table has a fixed schema.
```go
func (s *DebugSink) ApplySchema(ctx context.Context, schema protocol.SchemaMetadata) error {
    return nil
}
```

### Phase 3: Testing

1. Unit tests for filtering, sampling, and retention logic
2. Integration test with testcontainers (matching existing Databend test pattern)
3. E2E test with debug sink alongside Databend sink
4. E2E test verifying sink isolation (debug sink failure doesn't affect Databend)

---

## Correlation Matching After Filtering

When transformers filter out messages, `beforeMessages` and `afterMessages` arrays have different lengths. The `postTransformHook` receives:
- `originals []protocol.Message` — full original batch
- `transformed []protocol.Message` — filtered/transformed results (may be shorter)
- `filteredIndices []int` — indices into `originals` that were filtered out

The debug sink uses this to:
1. Mark `originals[filteredIndices[i]]` as `filtered=TRUE`
2. Match `transformed[j]` to its original via a mapping built from the filtered indices

**Implementation approach**: Build a reverse mapping from transformed index to original index.

```go
func buildBeforeAfterMapping(originals int, filteredIndices []int) map[int]int {
    filteredSet := make(map[int]bool)
    for _, idx := range filteredIndices {
        filteredSet[idx] = true
    }
    mapping := make(map[int]int)
    afterIdx := 0
    for i := 0; i < originals; i++ {
        if !filteredSet[i] {
            mapping[afterIdx] = i
            afterIdx++
        }
    }
    return mapping
}
```

---

## Table Initialization

On startup, debug sink should:
1. Connect to PostgreSQL using DSN with connection pooling (matching Databend sink: `SetMaxOpenConns(25)`, `SetMaxIdleConns(25)`, `SetConnMaxLifetime(5*time.Minute)`)
2. Check if configured table exists via `information_schema.tables`
3. Create table with proper schema if missing
4. Create core indexes (always)
5. Create optional indexes based on configuration
6. Start cleanup goroutine based on cleanup_interval

---

## Query Examples for Data Engineers

### View Recent Activity
```sql
SELECT 
    pipeline_id, sink_id, table_name, operation_type,
    capture_stage, filtered, captured_at,
    jsonb_pretty(payload) as data
FROM cdc_debug_messages
WHERE captured_at > NOW() - INTERVAL '1 hour'
ORDER BY captured_at DESC
LIMIT 100;
```

### Find Records That Were Actually Transformed
```sql
SELECT 
    b.correlation_id, b.table_name, b.operation_type,
    jsonb_pretty(b.payload) as before_data,
    jsonb_pretty(a.payload) as after_data,
    a.transformer_names, a.processing_latency_ms
FROM cdc_debug_messages b
JOIN cdc_debug_messages a ON b.correlation_id = a.correlation_id
WHERE b.capture_stage = 'before'
  AND a.capture_stage = 'after'
  AND b.payload_hash != a.payload_hash
ORDER BY a.captured_at DESC
LIMIT 50;
```

### Find Filtered Messages
```sql
SELECT pipeline_id, table_name, operation_type,
    jsonb_pretty(payload) as original_data, captured_at
FROM cdc_debug_messages
WHERE filtered = TRUE
  AND captured_at > NOW() - INTERVAL '1 day'
ORDER BY captured_at DESC;
```

### Schema Changes
```sql
SELECT pipeline_id, table_name,
    jsonb_pretty(payload) as schema_data, captured_at
FROM cdc_debug_messages
WHERE capture_stage = 'schema_change'
ORDER BY captured_at DESC
LIMIT 50;
```

### Count by Pipeline/Sink/Table
```sql
SELECT pipeline_id, sink_id, table_name, capture_stage,
    COUNT(*) as count,
    COUNT(*) FILTER (WHERE filtered = TRUE) as filtered_count
FROM cdc_debug_messages
WHERE captured_at > NOW() - INTERVAL '1 day'
GROUP BY pipeline_id, sink_id, table_name, capture_stage
ORDER BY count DESC;
```

---

## Files That Need Modification

| File | Change |
|------|--------|
| `internal/protocol/config.go` | Add `Options` to `SinkConfig`; update validation; update checkpoint keys to include `sinkID` |
| `cmd/pipeline/main.go` | Multi-sink loop; per-sink subscriber/consumer; hook setup |
| `internal/engine/consumer.go` | Add `sinkID`, hooks; update `processMessages`; update checkpoint keys |
| `internal/engine/pipeline.go` | Change `consumer *Consumer` to `consumers []*Consumer`; update lifecycle |
| `config.example.yaml` | Add `postgres_debug` sink example |
| **New**: `internal/sink/debug_capturer.go` | `DebugCapturer` interface |
| **New**: `internal/sink/postgresdebug/sink.go` | Main sink + `DebugCapturer` impl |
| **New**: `internal/sink/postgresdebug/config.go` | Config parsing |
| **New**: `internal/sink/postgresdebug/retention.go` | Cleanup logic |
| **New**: `internal/sink/postgresdebug/filter.go` | Filtering logic |
| **New**: `internal/sink/postgresdebug/sampling.go` | Sampling logic |
| **New**: `internal/sink/postgresdebug/hooks.go` | Before/after capture hooks |
| **New**: `internal/sink/postgresdebug/sink_test.go` | Tests |

---

## Future Enhancements (Phase 2)

- REST API endpoint for querying debug data
- Web UI for visualizing data transformations
- Diff view showing before/after changes side-by-side
- Integration with alerting (notify on specific patterns)
- Export to file (CSV/JSON) for offline analysis
- JSONPath-based filtering conditions
- Automatic schema migration on table structure changes
- Compression of old partitions
- Per-sink configurable batch size and timeout