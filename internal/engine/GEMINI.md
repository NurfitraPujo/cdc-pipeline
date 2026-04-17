# Internal Engine: Pipeline Logic

The `internal/engine` package contains the core streaming logic of the CDC Data Pipeline. It orchestrates the movement of data between sources and sinks using an asynchronous, batch-oriented approach.

## Core Features

- **`Producer`**:
    - Interfaces with ingress sources (e.g., `internal/source`).
    - Handles **Ingress LSN Checkpointing**: Persists LSNs to NATS KV after successful publishing.
    - Wraps NATS publishing in a **Circuit Breaker** (using `gobreaker`) to prevent head-of-line blocking during transient failures.
    - **Chaotic-Safe Dynamic Discovery**:
        - **Chunked Dynamic Snapshots**: When a new table is discovered, the Producer performs an automatic catch-up snapshot using **paginated SELECT snapshots** to bridge the gap before replication starts.
        - **Snapshot Isolation**: While a table is in **Snapshotting** or **Draining** states, concurrent CDC data is automatically routed to a dedicated JetStream buffer topic (`cdc_pipeline_{id}_buffer_{table}`) to maintain strict ordering and prevent data loss.
    - **Cache Warming**: Initial discovery now warms the producer's memory cache, preventing false-positive evolution freezes on startup.
- **`Consumer`**:
    - Interfaces with egress sinks (e.g., `internal/sink`).
    - Implements **Heterogeneous Batching**: Groups records by table and column-set.
    - Handles **Egress LSN Checkpointing**: Persists LSNs only after successful sink upload.
    - **Isolation Mode**: Detects "poison-pill" batches and switches to processing individual messages to isolate failures.
- **`DLQ` (Dead Letter Queue)**:
    - Routes repeatedly failing messages to a dedicated NATS topic for manual inspection and replay.
- **`Pipeline`**:
    - The top-level orchestrator that starts and coordinates the Producer and Consumer goroutines.
    - **Robust Schema Evolution State Machine**:
        - Managed via NATS KV using **CAS (Compare-And-Swap)** fencing tokens to prevent split-brain corruption.
        - **States**: Includes `Initial`, `Snapshotting` (paginated fetch), `Draining` (clearing buffer), `ApplyingSchema`, `Verifying`, and `SteadyState`.
        - **Secured Acknowledgment**: Uses cryptographic **Correlation IDs** (generated using SHA256) between Consumer and Producer to prevent spoofed acknowledgments and ensure lineage.
        - **Schema Circuit Breaker**: Rate-limits schema changes (max 5/min) to prevent DoS attacks.
        - **Non-blocking Acknowledgment Protocol**: Ensures all source acknowledgments are non-blocking to prevent Producer deadlocks.
        - **JSON-Backed Persistence**: Evolution state is fully serialized with exported fields and JSON tags for reliable recovery.

## PnP Transformer Architecture

The CDC Data Pipeline supports programmatic pre-processing through the `Transformer` interface. This allows developers to define custom logic for data masking, filtering, or enhancement before it reaches the sink.

### The `Transformer` Interface

```go
type Transformer interface {
    Name() string
    Transform(ctx context.Context, m *protocol.Message) (*protocol.Message, bool, error)
}
```

- **Programmatic Control**: Use `RegisterTransformer` to add custom logic.
- **Filtering**: Return `false` for `should_continue` to drop a message.
- **Chaining**: Multiple transformers can be configured in a single pipeline.

### Built-in Transformers

- **Masking (`mask`)**: Hashes PII fields using SHA256 with an optional salt.

### Configuration Example

Add processors to your pipeline config:
```yaml
pipelines:
  - id: "my-pipeline"
    processors:
      - name: "mask-emails"
        type: "mask"
        options:
          fields: ["email"]
          salt: "secret-salt"
```

## Key Files

- **`producer.go`**: Ingress orchestration, circuit breaker logic, and ingress checkpointing.
- **`consumer.go`**: Egress orchestration, batch flushing, isolation mode, and egress checkpointing.
- **`pipeline.go`**: lifecycle management and `drain_marker` propagation.
- **`dlq_test.go` & `engine_test.go`**: Unit tests with extensive mocking of streams, sinks, and KV stores.

## Conventions

- **At-Least-Once Delivery**: Achieved through strict checkpointing: `Persist Ingress LSN -> Publish to NATS -> (Stream) -> Pull from NATS -> Upload to Sink -> Persist Egress LSN -> Ack NATS`.
- **Drain Marker**: A special control message (`Op: "drain_marker"`) used to signal the end of a stream during transitions.
- **Circuit Breaker States**: Metrics are exported for `Closed`, `Open`, and `Half-Open` states.
