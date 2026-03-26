# CDC Core Data Pipeline - Comprehensive Requirements & Design

The purpose of this repository is to consume database changes in realtime manner using CDC and forward them to preconfigured sinks. It is a modular, pluggable system designed for moderate-volume (millions of events per day) data processing with high observability and reliability.

## Architecture
### Directory Structure
.
├── cmd
│   ├── pipeline/          # The CDC worker (Backend - Producer/Consumer)
│   └── api/               # The Monitoring API & Control Plane
├── internal
│   ├── engine/            # Core Orchestration Logic (The "Chassis")
│   │   ├── producer.go     # Ingress: Source -> NATS (via Watermill)
│   │   └── consumer.go     # Egress: NATS -> Sink (via Watermill)
│   ├── source/            # Input Plugins (Pluggable)
│   │   ├── provider.go    # Unified interface (Start/Stop)
│   │   └── postgres/      # Implements Trendyol go-pq-cdc
│   ├── stream/            # Streaming Backend Plugins (Pluggable)
│   │   ├── provider.go    # Interface (Publish/Subscribe via Watermill)
│   │   └── nats/          # NATS JetStream implementation
│   ├── sink/              # Data Warehouse Plugins (Pluggable)
│   │   ├── provider.go    # Interface (BatchUpload)
│   │   └── databend/      # Databend implementation
│   └── protocol/          # Shared MessagePack schemas & serialization
├── pkg/                   # Publicly shareable types
├── web/                   # Frontend (React+Vite monitoring app with TanStack Start)
├── config/                # YAML/JSON config handling
├── docs/                  # Documentation and Plans
├── docker-compose.yaml
└── go.mod

## Worker Implementation
### 1. General
- Use golang 1.26.x or higher.
- Modular architecture with pluggable Source, Sink, and Stream backends.
- Graceful shutdown and recovery support.
- MessagePack serialization for efficient data transport.

### 2. Source & Ingress (Producer)
- **Interface:** Pluggable source interface that returns `<-chan []Message` for unified batch processing.
- **Implementation:** Initial focus on PostgreSQL using `github.com/Trendyol/go-pq-cdc`.
- **Snapshotting:** 
    - Support both full and incremental/periodic snapshots.
    - Leverage `go-pq-cdc` built-in snapshotting with custom queries.
    - Batches of historical data (e.g., 500-1000 rows) are fetched and pushed to the Engine.
- **CDC:** Stream logical replication events.
- **Ingress Tracking:** Record successful publish to NATS in NATS KV (`ingress_checkpoint`).

### 3. Streaming (Watermill & NATS)
- Use `github.com/ThreeDotsLabs/watermill` for stream abstraction (Publish/Subscribe).
- **Default:** NATS JetStream with durable consumers.
- **Reliability:** Use `watermill.Message`'s `Ack()` and `Nack()` for automatic redelivery and reliable processing.
- **Unique IDs:** Map the unique source LSN/PK to `watermill.Message.UUID` for consistent deduplication.

### 4. Sink & Egress (Consumer)
- **Interface:** Pluggable Sink interface with `BatchUpload` capability.
- **Implementation:** Initial focus on **Databend**.
- **Batching:** Accumulate messages from NATS until count/time thresholds are met (e.g., 1000 messages or 5 seconds).
- **Idempotency:** Ensure no data duplication at the sink level (using unique constraints or pre-upload checks).
- **Egress Tracking:** Record successful upload to Sink in NATS KV (`egress_checkpoint`).

### 5. State Management (NATS KV)
- **Persistence:** Use NATS KV with JetStream persistence for all state.
- **Metadata:** Store detailed database connection details (encrypted) and table schema metadata (Columns, Types, PKs).
- **Checkpointing:** Granular tracking for each database and each table (Ingress vs Egress checkpoints).
- **Status:** Real-time state tracking (Snapshotting, CDC, Paused, Error) per table.

## Control Plane (API & Web)
### 1. Web Frontend
- Use **TanStack Start** for a modern, hybrid monitoring experience.
- Dashboard with real-time charts (using `recharts` and SSE/WebSocket) showing:
    - Throughput (msgs/sec) per pipeline/table.
    - Lag (Ingress vs Egress progress) per table.
    - Health and status of each worker segment.
- Configuration UI for adding/editing sources, sinks, and tables.

### 2. Control API
- Go-based API in `./cmd/api`.
- Reads/Writes NATS KV for pipeline configuration and user state.
- Supports operations at different levels:
    - Start/Stop/Restart pipelines.
    - Trigger manual snapshots for specific tables.
    - Configure new sources and sinks via UI.
- Streams metrics to the frontend via Server-Sent Events (SSE).

### 3. Authentication
- Username and password authentication (hashed with `bcrypt`) using NATS KV as the backend for user storage.
- Simple JWT-based auth flow.

## Deployment & Infrastructure
- Docker Compose for local development (NATS, Postgres, Databend).
- Configurable via YAML/JSON and Environment Variables.
