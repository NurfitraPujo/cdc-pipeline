# Daya Data Pipeline

A high-performance, modular CDC (Change Data Capture) data pipeline from PostgreSQL to Databend using NATS JetStream. Built for reliability, zero-allocation efficiency, and reactive scalability.

## Core Features
- **🚀 High-Performance Ingest**: Zero-allocation batching logic with MessagePack serialization.
- **🛡️ Reliable Delivery**: Strict at-least-once guarantees via a two-phase ingress feedback loop (Postgres WAL only advances after NATS confirmation).
- **⚙️ Dynamic Configuration**: Centralized, hierarchical configuration in NATS KV (Global Defaults > Pipeline Overrides).
- **🔄 Graceful Transitions**: Automatic two-phase reload protocol (Drain -> Resume) for zero data loss during config updates.
- **🚦 Dynamic Rate Limiting**: API-level protection to prevent concurrent pipeline restarts.
- **📊 Real-time Monitoring**: Aggregated multi-table status tracking and SSE-based metrics.
- **📚 Interactive Documentation**: Full Swagger/OpenAPI 2.0 integration.

---

## Architecture Overview

The pipeline operates as a reactive distributed system:

1.  **Control Plane (API)**: Manage configurations and monitor health.
2.  **Stateful Worker**: Orchestrates Producers and Consumers. It bootstraps itself from an embedded config and reacts to KV changes.
3.  **NATS JetStream**: The high-performance messaging backbone and persistent state store (`daya-dp-config` bucket).

### The Reactive Lifecycle
1.  **Config Update**: User updates config via REST API.
2.  **KV Trigger**: `ConfigManager` in the worker detects the change.
3.  **Graceful Reload**: 
    - **Phase 1**: Ingress (Producer) stops and returns the last processed LSN.
    - **Phase 2**: Egress (Consumer) catches up to that LSN and commits to the Sink.
4.  **Resumption**: A new worker instance starts precisely from the last known checkpoint.

---

## API Documentation

Once the API server is running, access the interactive Swagger UI:
👉 **[http://localhost:8080/swagger/index.html](http://localhost:8080/swagger/index.html)**

---

## API Quickstart

### 1. Authenticate
```bash
curl -X POST http://localhost:8080/api/v1/login \
     -H "Content-Type: application/json" \
     -d '{"username": "admin", "password": "admin"}'
```
*Returns a JWT token.*

### 2. Define a Source
```bash
curl -X POST http://localhost:8080/api/v1/sources \
     -H "Authorization: Bearer <TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
       "id": "pg-primary",
       "type": "postgres",
       "host": "localhost",
       "port": 5432,
       "user": "cdc_user",
       "pass": "encrypted-pass",
       "database": "production_db",
       "slot_name": "daya_cdc_slot",
       "publication_name": "daya_cdc_pub",
       "batch_size": 100
     }'
```

### 3. Create a Pipeline
```bash
curl -X POST http://localhost:8080/api/v1/pipelines \
     -H "Authorization: Bearer <TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{
       "id": "sync-01",
       "name": "Production to Analytics",
       "sources": ["pg-primary"],
       "tables": ["users", "orders"],
       "batch_size": 1000,
       "batch_wait": "5s"
     }'
```

---

## Monitoring
Get real-time status of all tables in a pipeline:
```bash
curl -H "Authorization: Bearer <TOKEN>" http://localhost:8080/api/v1/pipelines/sync-01/status
```

---

## Development

### Prerequisites
- Go 1.26.1
- NATS Server with JetStream enabled (`nats-server -js`)
- PostgreSQL (logical replication enabled)
- Databend

### Running the Worker
The worker automatically bootstraps NATS KV if empty.
```bash
export NATS_URL="nats://localhost:4222"
go run ./cmd/pipeline
```
*Note: The worker exposes health checks on `:8081` (`/healthz` and `/readyz`) for Kubernetes.*

#### Kubernetes Probe Example:
```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8081
readinessProbe:
  httpGet:
    path: /readyz
    port: 8081
```

### Running the API
```bash
export NATS_URL="nats://localhost:4222"
export PORT="8080"
go run ./cmd/api
```
