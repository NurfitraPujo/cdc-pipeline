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

- **[`internal/`](internal/GEMINI.md)**: Core architectural overview and design principles.
    - **[`api/`](internal/api/GEMINI.md)**: REST Control Plane implementation.
    - **[`config/`](internal/config/GEMINI.md)**: Dynamic configuration and worker supervision.
    - **[`engine/`](internal/engine/GEMINI.md)**: Pipeline logic, batching, and checkpointing.
    - **[`protocol/`](internal/protocol/GEMINI.md)**: Shared schemas and MessagePack serialization.
    - **[`source/`](internal/source/GEMINI.md)**: Data ingress (PostgreSQL logical replication).
    - **[`sink/`](internal/sink/GEMINI.md)**: Data egress (Databend analytical store).
    - **[`stream/`](internal/stream/GEMINI.md)**: Messaging infrastructure (Watermill & NATS).
    - **[`test/e2e/`](internal/test/e2e/GEMINI.md)**: End-to-end integration testing suite.

## Development Conventions (High-Level)

- **Configuration**: Managed dynamically via NATS KV. The `ConfigManager` handles graceful transitions.
- **Reliability**: Dual-layer panic recovery and `Cancel->Sleep->Close` lifecycles ensure zero-crash stability.
- **State**: LSN checkpoints and evolution states are persisted in NATS KV for "at-least-once" guarantees.
- **Robust Schema Evolution**: Distributed state machine powered by **NATS KV CAS (Compare-and-Swap)** fencing tokens to prevent split-brain issues and ensure atomic transitions.
- **Chaotic-Safe Dynamic Discovery**: Bridges the "Gap of Uncertainty" for new tables using **paginated SELECT snapshots** and **JetStream buffering** to prevent data loss even when inserts immediately follow table creation.
- **Schema**: Auto-evolves based on source DDL changes with a Schema Circuit Breaker for stability.
