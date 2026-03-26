# CDC Data Pipeline

A high-performance, modular CDC (Change Data Capture) data pipeline from PostgreSQL to Databend using NATS JetStream. Built for production-grade reliability, zero-allocation efficiency, and autonomous self-healing.

## 🚀 Core Features
- **🛡️ Zero-Crash Architecture**: Robust first-principle lifecycle management (Cancel->Sleep->Close) and dual-layer panic recovery.
- **🤖 Autonomous Supervision**: Recursive worker supervisor that detects unplanned crashes and automatically reboots pipelines with exponential backoff.
- **🧬 Auto-Schema Evolution**: Automatic DDL application to the sink (`CREATE/ALTER TABLE`) when source schemas change.
- **📦 Heterogeneous Batching**: Sophisticated grouping of CDC messages by column-set within a single batch, preventing SQL mismatches during live schema changes.
- **🔄 Graceful Transitions**: Two-phase "Drain -> Shutdown -> Restart" protocol for zero-downtime, LSN-consistent configuration reloads.
- **📊 Live Observability**: Real-time SSE (Server-Sent Events) metrics stream and full Prometheus integration for the "Four Golden Signals."
- **⚙️ Dynamic Control Plane**: Centralized, hierarchical configuration in NATS KV with built-in rate limiting for concurrent restarts.

---

## 🏗️ Architecture Overview

The pipeline operates as a reactive distributed system:

1.  **Control Plane (API)**: Hardened REST API for managing configurations, authentication, and live monitoring.
2.  **Stateful Worker**: Orchestrates Producers and Consumers. It is self-bootstrapping, self-healing, and reacts to configuration triggers in real-time.
3.  **NATS JetStream**: The high-performance messaging backbone and persistent state store (`cdc-dp-config` bucket).
4.  **LSN Checkpointing**: "At-least-once" delivery guarantee using NATS-persisted checkpoints for both Ingress (Postgres) and Egress (Databend).

### The Autonomous Self-Healing Loop
1.  **Panic Recovery**: Any library-level panic in the source handler is caught by a `recover()` block.
2.  **Termination**: The worker exits its run loop gracefully but signals an "Unexpected Exit" to the supervisor.
3.  **Reboot**: The supervisor detects the missing `transition` flag, waits 5s, re-fetches the latest config, and restarts the pipeline from the last LSN.

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

### 3. Get Pipeline Status
```bash
curl -H "Authorization: Bearer <TOKEN>" http://localhost:8080/api/v1/pipelines/sync-01/status
```

---

## 🛠️ Development & Testing

### Prerequisites
- **Go**: 1.26+
- **NATS**: Server with JetStream enabled (`nats-server -js`)
- **PostgreSQL**: 14+ (logical replication enabled, `wal_level = logical`)
- **Databend**: Latest

### Running the Full E2E Suite
Our E2E tests use **Testcontainers** to spin up isolated Postgres, NATS, and Databend instances. This verifies Initial Snapshots, Live CDC, and Schema Evolution.
```bash
go test -v -timeout 10m ./internal/test/e2e/...
```

### Running the Components
```bash
# Start the Worker (Auto-bootstraps NATS KV)
export NATS_URL="nats://localhost:4222"
go run ./cmd/pipeline

# Start the API Control Plane
export NATS_URL="nats://localhost:4222"
export PORT="8080"
go run ./cmd/api
```

---

## 🛡️ Stability Note
This project includes a critical patch for `go-pq-cdc` located in the `vendor/` directory. This patch replaces an aggressive `panic` with an error log during connection EOF on shutdown. Combined with the **Cancel->Sleep->Close** sequence in `internal/source/postgres`, this ensures your production process never crashes during routine reloads.
