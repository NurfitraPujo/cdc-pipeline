# Internal Architecture Overview

The `internal/` directory contains the core logic of the CDC Data Pipeline. The system follows a "Control Plane vs. Data Plane" separation of concerns, coordinated through NATS JetStream.

## Directory Structure

- **`api/`**: Implementation of the REST Control Plane.
- **`config/`**: The orchestration hub for the Data Plane, managing dynamic reloads and worker supervision.
- **`engine/`**: Core pipeline logic, including Producers, Consumers, and the DLQ.
- **`protocol/`**: Shared data structures and MessagePack serialization logic.
- **`source/`**: Data ingress abstractions (e.g., PostgreSQL CDC).
- **`sink/`**: Data egress abstractions (e.g., Databend).
- **`transformer/`**: Dedicated framework for data pre-processing plugins.
- **`stream/`**: Messaging wrappers around Watermill and NATS JetStream.
- **`metrics/` & `logger/`**: Observability using Prometheus and Zerolog.
- **`test/e2e/`**: End-to-end integration tests using Testcontainers.

## Core Design Principles

1.  **Plug-and-Play (PnP) Architecture**: Ingress (`source`), Egress (`sink`), and Messaging (`stream`) layers are strictly interface-driven, allowing new backends to be added with zero changes to the core engine.
2.  **Reactive Configuration**: Workers react to NATS KV changes in real-time.
3.  **Graceful Transitions**: Use of a "Two-Phase Transition Protocol" (Drain -> Shutdown -> Restart) to maintain LSN consistency.
4.  **Autonomous Self-Healing**: Supervisor goroutines monitor for crashes and reboot workers automatically.
5.  **Zero-Allocation Efficiency**: Extensive use of MessagePack for high-performance serialization.
6.  **Robust Error Handling**: Strict adherence to the `Cancel -> Sleep -> Close` lifecycle and dual-layer panic recovery.
7.  **Robust Schema Evolution**: Uses a Distributed State Machine with NATS KV CAS fencing, Schema Circuit Breaker, and Correlation ID-secured acknowledgments.
8.  **Chaotic-Safe Discovery**: Handles new tables with Chunked Dynamic Snapshots and Snapshot Isolation via JetStream buffering to ensure no data loss during discovery.

Refer to nested `AGENT.md` files in each subdirectory for more detailed technical documentation.

## 🛡️ Testing Conventions & Harness Rules

To ensure maintainability, architectural fitness, and regression safety:

### 1. Architectural Boundaries (Fencing)
- **Control/Data Separation**: The Control Plane (`api/`) is strictly separated from the Data Plane. Core pipeline packages (`engine/`, `source/`, `sink/`, `stream/`, `metrics/`) **must not** import `api/` to avoid dependency cycles. This is enforced by `depguard`.
- **Domain Isolation**: Ingress (`source/`) and Egress (`sink/`) must remain modular and independent of one another. They should only interact via defined interfaces and common protocols (`protocol/`).

### 2. Unit Testing Conventions
- **Table-Driven Tests**: Write tests using table-driven patterns (`struct{ name string; ... }`) to ensure comprehensive coverage of edge cases, inputs, and outputs.
- **Mocking**: Leverage interface mock generated via `go.uber.org/mock/mockgen`. Mock external infrastructure (like PostgreSQL, Databend, or NATS) for quick unit tests. Use `go test -short` to run unit tests without requiring active database containers.
- **Maintain Test Coverage**: Ensure new code is accompanied by corresponding unit tests. Never submit code that degrades test coverage.

### 3. End-to-End (E2E) Integration Tests
- **The E2E Safety Net**: E2E tests (located in `test/e2e/`) run using Testcontainers to validate full-flow correctness with live postgres, Databend, and NATS JetStream.
- **Safety Rule**: You are strictly **prohibited from altering or deleting E2E tests** to bypass a failing test unless explicitly approved by a human. Treat an E2E failure as a design regression in your implementation code.
- **E2E Additions**: If you introduce new system-wide behavior or new configuration transitions, you **must add a corresponding E2E integration test** to cover the transition.

