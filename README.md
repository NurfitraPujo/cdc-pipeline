# CDC Data Pipeline

A high-performance, modular CDC (Change Data Capture) data pipeline from PostgreSQL to Databend using NATS JetStream. Built for production-grade reliability, zero-allocation efficiency, and autonomous self-healing.

## 🚀 Core Features
- **🛡️ Zero-Crash Architecture**: Robust first-principle lifecycle management (Cancel->Sleep->Close) and dual-layer panic recovery.
- **⚡ Resilience First**: Built-in **Circuit Breaker** (Producer) and **Isolation Mode** (Consumer) to handle transient failures and "poison-pill" batches.
- **🤖 Autonomous Supervision**: Recursive worker supervisor that detects unplanned crashes and automatically reboots pipelines with exponential backoff.
- **🧬 Auto-Schema Evolution**: Automatic DDL application to the sink (`CREATE/ALTER TABLE`) when source schemas change.
- **📦 Heterogeneous Batching**: Sophisticated grouping of CDC messages by column-set within a single batch, preventing SQL mismatches during live schema changes.
- **🔄 Graceful Transitions**: Two-phase "Drain -> Shutdown -> Restart" protocol for zero-downtime, LSN-consistent configuration reloads.
- **📊 Live Observability**: Real-time SSE (Server-Sent Events) metrics stream and full Prometheus integration for the "Four Golden Signals."
- **🔌 Plug-and-Play (PnP)**: Fully interface-driven architecture for Sources, Sinks, Transformers, and Messaging backends.

---

## 🏗️ Architecture Overview

The pipeline operates as a reactive distributed system:

1.  **Control Plane (API)**: Hardened REST API (Go/Gin) for managing configurations, authentication (JWT), and live monitoring.
2.  **Stateful Worker (Pipeline)**: Orchestrates Producers and Consumers. It is self-bootstrapping, self-healing, and reacts to configuration triggers in real-time.
3.  **NATS JetStream**: The high-performance messaging backbone and persistent state store (`cdc-dp-config` bucket).
4.  **LSN Checkpointing**: "At-least-once" delivery guarantee using NATS-persisted checkpoints for both Ingress (Postgres) and Egress (Databend).

### 🔄 At-Least-Once Delivery Flow
1.  **Ingress**: Producer reads from Postgres LSN -> Persists Ingress LSN to NATS KV.
2.  **Transport**: Batch published to NATS JetStream (Circuit Breaker protected).
3.  **Consumption**: Consumer pulls batch -> Applies **Transformers** (Masking/Filtering).
4.  **Egress**: Consumer uploads to Databend (Upsert/Replace) -> Persists Egress LSN to NATS KV -> Acks NATS.

---

## ⚡ Resilience & Reliability

- **Circuit Breaker**: The Producer wraps NATS publishing in a circuit breaker. If NATS is unreachable, it trips to prevent head-of-line blocking and enters a retry/wait state.
- **Isolation Mode**: If a batch fails repeatedly (poison-pill), the Consumer automatically switches to "Isolation Mode," processing messages individually to identify and route failing messages to the **DLQ (Dead Letter Queue)** while allowing the rest of the stream to continue.
- **Autonomous Self-Healing**: 
    1. **Panic Recovery**: Library-level panics are caught by `recover()` blocks.
    2. **Termination**: The worker signals an "Unexpected Exit" to the supervisor.
    3. **Reboot**: The supervisor re-fetches the latest config and restarts the pipeline from the last checkpoint.

---

## ⚙️ Core Technologies

- **MessagePack (`msgp`)**: High-performance, zero-allocation serialization format. 5-10x faster and significantly smaller than JSON.
- **NATS JetStream**: Provides durable, persistent streams and a distributed Key-Value store for configuration and state.
- **Watermill**: Used for building event-driven applications with a clean, pluggable messaging API.
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
In `internal/transformer/provider.go`, register your factory function:

```go
RegisterTransformer("decrypt", func(options map[string]any) (Transformer, error) {
    key := options["key"].(string)
    return &DecryptTransformer{Key: []byte(key)}, nil
})
```

### 3. Configure in YAML
Add your new transformer to the pipeline configuration:

```yaml
pipelines:
  - id: "secure-pipeline"
    processors:
      - name: "decrypt-secrets"
        type: "decrypt"
        options:
          key: "base64-encoded-key..."
```

---

## 🔌 Adding a New Source

To support a new database (e.g., MySQL, MongoDB), implement the `Source` interface.

### 1. Implement the Interface
```go
type MySource struct { /* ... */ }

func (s *MySource) Name() string { return "mysource" }

func (s *MySource) Start(ctx context.Context, config protocol.SourceConfig, checkpoint protocol.Checkpoint) (<-chan []protocol.Message, chan<- struct{}, error) {
    out := make(chan []protocol.Message)
    ack := make(chan struct{})
    
    go func() {
        // Fetch data and send to 'out'
        // Wait for 'ack' before sending next batch
    }()
    
    return out, ack, nil
}

func (s *MySource) Stop() error { /* Cleanup */ }
```

### 2. Register in Worker Factory
In `cmd/pipeline/main.go`, add your source to the factory logic:
```go
if srcCfg.Type == "mysource" {
    src = mysource.NewMySource(sourceID)
}
```

---

## 🔌 Adding a New Sink

To support a new analytical store (e.g., BigQuery, ClickHouse), implement the `Sink` interface.

### 1. Implement the Interface
```go
type MySink struct { /* ... */ }

func (s *MySink) Name() string { return "mysink" }

func (s *MySink) BatchUpload(ctx context.Context, messages []protocol.Message) error {
    // Perform bulk upsert/replace to ensure idempotency
    return nil
}

func (s *MySink) ApplySchema(ctx context.Context, schema protocol.SchemaMetadata) error {
    // Execute DDL (CREATE/ALTER TABLE)
    return nil
}

func (s *MySink) Stop() error { /* Cleanup */ }
```

### 2. Register in Worker Factory
In `cmd/pipeline/main.go`, add your sink to the factory logic:
```go
if snkCfg.Type == "mysink" {
    snk = mysink.NewMySink(sinkID, snkCfg.DSN)
}
```

---

## 📚 API Documentation

Once the API server is running, access the interactive Swagger UI:
👉 **[http://localhost:8080/swagger/index.html](http://localhost:8080/swagger/index.html)**

---

## ⚡ API Quickstart

### 1. Authenticate
```bash
curl -X POST http://localhost:8080/api/v1/login \
     -H "Content-Type: application/json" \
     -d '{"username": "admin", "password": "admin"}'
```
*Returns a JWT token.*

### 2. Stream Live Metrics (SSE)
Receive a real-time stream of LSN progress and table stats:
```bash
curl -N -H "Authorization: Bearer <TOKEN>" http://localhost:8080/api/v1/pipelines/sync-01/metrics
```

---

## 🛠️ Development & Testing

### Prerequisites
Before running the pipeline or its tests, ensure you have the following installed:
- **Go 1.26+**: For the backend services.
- **Node.js & pnpm**: For the frontend dashboard.
- **Container Runtime**: Docker or Podman (with the Docker socket enabled). This is **required** for the E2E integration tests as they use [Testcontainers](https://testcontainers.com/).

### 1. Backend Setup
```bash
# Install dependencies
go mod tidy

# (Optional) Generate MessagePack encoders if you modify internal/protocol
go generate ./internal/protocol/...
```

### 2. Running the Full E2E Suite
The integration tests orchestrate real NATS, Postgres, and Databend instances. Ensure your Docker/Podman daemon is running:
```bash
go test -v -timeout 10m ./internal/test/e2e/...
```

### 3. Running the Components Locally
```bash
# Terminal 1: Start the Stateful Worker
# It will automatically bootstrap NATS KV if not present
export NATS_URL="nats://localhost:4222"
go run ./cmd/pipeline

# Terminal 2: Start the Control Plane API
export NATS_URL="nats://localhost:4222"
export JWT_SECRET="your-dev-secret"
go run ./cmd/api
```

### 4. Frontend Setup
```bash
cd web
pnpm install
pnpm dev
```

---

## 🛡️ Stability Note
This project includes a critical patch for `go-pq-cdc` located in `internal/vendor/go-pq-cdc/`. This patch replaces an aggressive `panic` with an error log during connection EOF on shutdown. This is linked via a `replace` directive in `go.mod`. Combined with the **Cancel->Sleep->Close** sequence in `internal/source/postgres`, this ensures your production process never crashes during routine reloads.

---

## 📄 License
This project is licensed under the **Apache License 2.0**. See the [LICENSE](LICENSE) file for the full text.
