# Daya Data Pipeline

A high-performance, modular CDC (Change Data Capture) data pipeline from PostgreSQL to Databend using NATS JetStream.

## Features
- **Zero-Allocation Batching**: Optimized message handling for high throughput.
- **Reliable Ingress**: Two-phase feedback loop with at-least-once delivery guarantees.
- **Dynamic Configuration**: Reactive pipeline management via NATS KV (No restarts required).
- **Graceful Transitions**: Safe two-phase reload protocol (Drain -> Resume).
- **Real-time Monitoring**: SSE-based metrics for ingress and egress LSN tracking.

---

## Architecture Overview

The pipeline consists of three main components:

1.  **Control Plane (API)**: A REST API to manage pipeline and source configurations.
2.  **Stateful Worker**: A reactive engine that watches NATS KV for config changes and orchestrates Producers/Consumers.
3.  **NATS JetStream**: The persistent, high-performance messaging backbone.

### The Lifecycle
1.  **API POST**: Save a JSON config to NATS KV (`pipelines.{id}.config`).
2.  **Worker Discovery**: `ConfigManager` detects the new key via a NATS Watcher.
3.  **Reactive Factory**: The Worker resolves Source/Sink dependencies and starts the `Pipeline`.
4.  **Producer (Postgres)**: Fetches WAL events and publishes batches to NATS.
5.  **Consumer (Databend)**: Subscribes to the pipeline topic and performs idempotent `REPLACE INTO` uploads.

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
       "publication_name": "daya_cdc_pub"
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
       "batch_wait": 5000000000
     }'
```

---

## Monitoring
Get real-time status of all tables in a pipeline:
```bash
curl -H "Authorization: Bearer <TOKEN>" http://localhost:8080/api/v1/pipelines/sync-01/status
```

Or stream metrics via SSE:
```bash
curl -H "Authorization: Bearer <TOKEN>" http://localhost:8080/api/v1/pipelines/sync-01/metrics
```

---

## Development

### Prerequisites
- Go 1.26.1
- NATS Server with JetStream enabled (`nats-server -js`)
- PostgreSQL (with logical replication enabled)
- Databend

### Running the Worker
```bash
export NATS_URL="nats://localhost:4222"
go run ./cmd/pipeline
```

### Running the API
```bash
export NATS_URL="nats://localhost:4222"
export PORT="8080"
go run ./cmd/api
```
