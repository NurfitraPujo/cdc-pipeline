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

Refer to nested `AGENT.md` files in each subdirectory for more detailed technical documentation.
