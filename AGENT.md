# CDC Data Pipeline

A high-performance, modular CDC (Change Data Capture) data pipeline from PostgreSQL to Databend using NATS JetStream. Built for production-grade reliability, zero-allocation efficiency, and autonomous self-healing.

## Project Overview

The project consists of a distributed system designed for reliable data replication:
- **Control Plane (API)**: A Go-based REST API (using Gin) for managing pipeline configurations, authentication (JWT), and real-time monitoring via SSE.
- **Stateful Worker (Pipeline)**: A Go-based orchestrator that manages Producers (PostgreSQL logical replication) and Consumers (Databend sink). It features autonomous self-healing, crash detection, and automatic schema evolution.
- **NATS JetStream**: Serves as the high-performance messaging backbone, configuration store (KV), and persistent state store for LSN checkpoints.
- **Frontend Dashboard**: A modern React application for visualizing and managing pipelines.

## Tech Stack

- **Backend**: Go 1.26+, Gin, NATS JetStream, PostgreSQL (Source), Databend (Sink), Prometheus, Zerolog.
- **Frontend**: React 19, TypeScript, Vite, TanStack Router, TanStack Query, Tailwind CSS, Biome.
- **Testing**: Testcontainers-go for E2E integration testing.

## Building and Running

### Backend (Go)
- **Start API Server**: `go run ./cmd/api`
- **Start Pipeline Worker**: `go run ./cmd/pipeline`
- **Run Unit Tests**: `go test ./internal/...`
- **Run E2E Tests**: `go test -v -timeout 10m ./internal/test/e2e/...` (Requires Docker)

### Frontend (React)
- **Install Dependencies**: `cd web && pnpm install`
- **Development Mode**: `cd web && pnpm dev`
- **Production Build**: `cd web && pnpm build`
- **Lint and Format**: `cd web && pnpm check`

## Deployment

The project includes Kubernetes manifests for deploying the system:
- **NATS**: `k8s/nats.yaml` (JetStream enabled)
- **API Server**: `k8s/api.yaml`
- **Pipeline Worker**: `k8s/pipeline.yaml`

## Architecture & Conventions

Detailed technical documentation is organized by directory:

- **[`internal/`](internal/AGENT.md)**: Core architectural overview and design principles.
    - **[`api/`](internal/api/AGENT.md)**: REST Control Plane implementation.
    - **[`config/`](internal/config/AGENT.md)**: Dynamic configuration and worker supervision.
    - **[`engine/`](internal/engine/AGENT.md)**: Pipeline logic, batching, and checkpointing.
    - **[`metrics/`](internal/metrics/AGENT.md)**: Prometheus metrics (Four Golden Signals).
    - **[`protocol/`](internal/protocol/AGENT.md)**: Shared schemas and MessagePack serialization.
    - **[`source/`](internal/source/AGENT.md)**: Data ingress (PostgreSQL logical replication).
    - **[`sink/`](internal/sink/AGENT.md)**: Data egress (Databend analytical store).
    - **[`stream/`](internal/stream/AGENT.md)**: Messaging infrastructure (Watermill & NATS).
    - **[`test/e2e/`](internal/test/e2e/AGENT.md)**: End-to-end integration testing suite.

## Development Conventions (High-Level)

- **Configuration**: Managed dynamically via NATS KV. The `ConfigManager` handles graceful transitions.
- **Reliability**: Dual-layer panic recovery and `Cancel->Sleep->Close` lifecycles ensure zero-crash stability.
- **State**: LSN checkpoints are persisted in NATS KV for "at-least-once" guarantees.
- **Schema**: Auto-evolves based on source DDL changes.

## Patched Dependencies

This project includes a **critical patch** for `go-pq-cdc` (PostgreSQL CDC library) located in `internal/vendor/go-pq-cdc/`.

### The Patch

**File**: `internal/vendor/go-pq-cdc/pq/replication/stream.go:211`

**Change**: Replaced `panic("corrupted connection")` with `logger.Error("corrupted connection")`

**Why**: The original library would panic on connection EOF during shutdown, causing the entire stateful worker process to crash. This is a deficiency in the upstream library design. The patch ensures graceful handling of connection issues without bringing down all pipelines.

**Maintenance**: See [`VENDOR.md`](VENDOR.md) for instructions on updating the patched dependency.

## Observability: Four Golden Signals

The pipeline exports Prometheus metrics aligned with Google's [Four Golden Signals](https://sre.google/sre-book/monitoring-distributed-systems/#xref_monitoring_golden-signals) for SRE best practices:

| Signal | Metric | Description |
|--------|--------|-------------|
| **Latency** | `cdc_pipeline_lag_milliseconds` | Time delay between source and sink (per pipeline/table) |
| **Traffic** | `cdc_pipeline_records_synced_total` | Records processed (counter, per pipeline/table) |
| **Errors** | `cdc_pipeline_errors_total` | Error count (counter, per pipeline/table) |
| **Saturation** | `cdc_pipeline_circuit_breaker_state` | Circuit breaker state (0=closed, 1=open, 2=half-open) |

**Additional Metrics**:
- `cdc_pipeline_worker_heartbeat_timestamp` - Worker health monitoring

See [`internal/metrics/`](internal/metrics/AGENT.md) for implementation details.
