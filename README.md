# CDC Data Pipeline

A high-performance, modular CDC (Change Data Capture) data pipeline from PostgreSQL to Databend using NATS JetStream. Built for production-grade reliability, zero-allocation efficiency, and autonomous self-healing.

## 🚀 Core Features
- **🛡️ Zero-Crash Architecture**: Robust first-principle lifecycle management (Cancel->Sleep->Close) and dual-layer panic recovery.
- **⚡ Resilience First**: Built-in **Circuit Breaker** (Producer) and **Isolation Mode** (Consumer) to handle transient failures and "poison-pill" batches.
- **🤖 Autonomous Supervision**: Recursive worker supervisor that detects unplanned crashes and automatically reboots pipelines with exponential backoff.
- **🔗 Per-Sink Consumer Architecture**: Complete isolation between egress targets. A failure or slowdown in one sink (e.g., Debug DB) never blocks production data flow to others (e.g., Databend).
- **🧬 Auto-Schema Evolution**: Automatic DDL application to the sink (`CREATE/ALTER TABLE`) when source schemas change.
- **📦 Heterogeneous Batching**: Sophisticated grouping of CDC messages by column-set within a single batch, preventing SQL mismatches during live schema changes.
- **🔄 Graceful Transitions**: Two-phase "Drain -> Shutdown -> Restart" protocol for zero-downtime, LSN-consistent configuration reloads.
- **📊 Live Observability**: Real-time SSE (Server-Sent Events) metrics stream and full Prometheus integration for the "Four Golden Signals."
- **🔍 Deep Debugging**: Integrated PostgreSQL Debug Sink for full data lineage, capturing "before" and "after" transformation states with correlation IDs.

---

## 🏗️ Architecture Overview

The pipeline operates as a reactive distributed system with a **Fan-Out Consumer Model**:

1.  **Control Plane (API)**: Hardened REST API (Go/Gin) for managing configurations, authentication (JWT), and live monitoring.
2.  **Stateful Worker (Pipeline)**: Orchestrates Producers and Consumers. It is self-bootstrapping and self-healing.
3.  **Per-Sink Isolation**: For every configured sink, the worker spawns a dedicated **Consumer Goroutine** with its own:
    - Unique NATS JetStream durable subscription.
    - Independent egress checkpoint (LSN).
    - Isolated retry logic and Dead Letter Queue (DLQ).
4.  **NATS JetStream**: The high-performance messaging backbone and persistent state store.

### 🔄 At-Least-Once Delivery Flow
1.  **Ingress**: Producer reads from Postgres LSN -> Persists Ingress LSN to NATS KV.
2.  **Transport**: Batch published to NATS JetStream (Circuit Breaker protected).
3.  **Consumption (N-Sinks)**: Each Sink's Consumer pulls the same batch independently.
4.  **Transformation**: Consumer applies **Transformers** (Masking/Filtering).
5.  **Egress**: Consumer uploads to target -> Persists Egress LSN to NATS KV -> Acks NATS.

---

## 🔍 PostgreSQL Debug Sink

The Debug Sink acts as a high-fidelity data logger for data engineers. It captures the state of data at different stages of the pipeline to aid in debugging transformation logic and data quality issues.

### Key Capabilities
- **Correlation ID**: Links "before" and "after" records using a unique UUID, even across complex transformer chains.
- **Transformation Audit**: Records the names of all transformers applied to a message and the processing latency.
- **Filtering Visibility**: Messages dropped by transformers are still recorded in the "before" stage but marked as `filtered=TRUE`.
- **Separated Storage**: 
    - **Row Debug Data**: Stored in `cdc_debug_messages` (default) for ephemeral troubleshooting.
    - **Schema Changes**: Stored in `cdc_debug_schema_changes` (default) for long-term schema evolution tracking.
- **Smart Retention**: Configurable age-based (e.g., "7 days") or count-based (e.g., "last 1M rows") automatic cleanup.

### Configuration Example
```yaml
sinks:
  - id: debug-storage
    type: postgres_debug
    dsn: "postgres://user:pass@localhost:5432/debug_db"
    options:
      table_name: "my_debug_rows"
      schema_table_name: "my_debug_schemas"
      retention:
        mode: "age"
        max_age: "168h" # 7 days
      sampling:
        mode: "percentage"
        value: 10 # Capture 10% of traffic
```

---

## ⚡ Resilience & Reliability

- **Fail-Fast Orchestration**: If the Producer fails (e.g., lost Postgres connection), the pipeline immediately signals all sibling Consumers to stop, preventing inconsistent "zombie" states.
- **Circuit Breaker**: The Producer wraps NATS publishing in a circuit breaker. If NATS is unreachable, it trips to prevent head-of-line blocking.
- **Isolation Mode**: If a batch fails repeatedly (poison-pill), the Consumer automatically identifies and routes failing messages to the **DLQ (Dead Letter Queue)** while allowing the rest of the stream to continue.
- **Worker Group Isolation**: Use the `WORKER_GROUP` environment variable to ensure durable consumer names don't collide across logical environments (prod/staging) sharing a NATS cluster.

---

## ⚙️ Core Technologies

- **MessagePack (`msgp`)**: High-performance, zero-allocation serialization format. 5-10x faster and significantly smaller than JSON.
- **NATS JetStream**: Provides durable, persistent streams and a distributed Key-Value store for configuration and state.
- **Watermill**: Used for building event-driven applications with a clean, pluggable messaging API.
- **Google UUID**: RFC 4122 compliant unique identifiers for robust data correlation.
- **Testcontainers-go**: Powers the E2E suite by orchestrating real infrastructure (Postgres, NATS, Databend) in Docker.

---

## 🔌 Adding a Custom Transformer

The pipeline is designed for extensibility. You can add custom logic (e.g., decryption, data enrichment, or advanced filtering) by implementing the `Transformer` interface.

### 1. Implement the Interface
Create a new file in `internal/transformer/` or your own package:

```go
type DecryptTransformer struct {
    Key []byte
}

func (t *DecryptTransformer) Name() string { return "decrypt" }

func (t *DecryptTransformer) Transform(ctx context.Context, m *protocol.Message) (*protocol.Message, bool, error) {
    // Custom logic: Decrypt a specific field
    if val, ok := m.Data["secret_field"].(string); ok {
        m.Data["secret_field"] = myDecryptFunc(val, t.Key)
    }
    return m, true, nil
}
```

### 2. Register the Factory
In `internal/transformer/builtin.go` or your init logic:

```go
RegisterTransformer("decrypt", func(options map[string]any) (Transformer, error) {
    key := options["key"].(string)
    return &DecryptTransformer{Key: []byte(key)}, nil
})
```

---

## 📚 API Documentation

Once the API server is running, access the interactive Swagger UI:
👉 **[http://localhost:8080/swagger/index.html](http://localhost:8080/swagger/index.html)**

---

## 🛠️ Development & Testing

### Prerequisites
- **Go 1.26+**
- **Node.js & pnpm** (for dashboard)
- **Docker/Podman** (for E2E tests)

### 1. Running the Full E2E Suite
The integration tests orchestrate real NATS, Postgres, and Databend instances. 
```bash
go test -v -timeout 10m ./internal/test/e2e/...
```

### 2. Tuning Throughput
You can tune NATS backpressure per-sink using `max_ack_pending` in the Sink configuration:
```yaml
sinks:
  - id: high-volume-sink
    type: databend
    max_ack_pending: 5000 # Default is BatchSize * 2
```

---

## 📄 License
This project is licensed under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for the full text.
