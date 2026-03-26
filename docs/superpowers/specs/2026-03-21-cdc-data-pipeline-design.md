# CDC Data Pipeline - Detailed Design Specification

**Status:** Approved (2026-03-21)
**Authors:** Gemini CLI

## Overview
CDC Data Pipeline is a high-performance, modular system for CDC (Change Data Capture) and data ingestion. It leverages PostgreSQL logical replication for real-time updates and efficient batch processing for historical data snapshots. The system is designed to be pluggable, allowing for easy addition of new data sources and analytical sinks.

## Architecture
The system follows a Producer-Consumer architecture decoupled by a NATS JetStream message broker.

### Ingress (Producer)
- **Source Interface:** Abstracted to support various databases. Initial implementation: PostgreSQL using `go-pq-cdc`.
- **Snapshotting:** High-throughput batch fetching of historical data using PK-based chunking.
- **Engine (Producer):** Subscribes to the Source, wraps data in a standardized schema (Source, Table, LSN, Timestamp), serializes with MessagePack, and publishes to NATS.
- **Tracking:** Successful publish to NATS updates the `ingress_checkpoint` in NATS KV.

### Transport (Streaming)
- **Watermill:** Provides a unified interface for Pub/Sub.
- **NATS JetStream:** Used for durable message storage and reliable delivery.
- **Message UUID:** Mapped to the unique source LSN/PK to ensure deduplication across retries.

### Egress (Consumer)
- **Engine (Consumer):** Subscribes to NATS topics, accumulates messages for batching, and invokes the Sink.
- **Sink Interface:** Abstracted for different data warehouses. Initial implementation: Databend.
- **Tracking:** Successful batch-upload to the Sink updates the `egress_checkpoint` in NATS KV and acknowledges the messages to NATS.

## State Management
- **Durable Store:** NATS KV with JetStream persistence.
- **Granular Checkpointing:** Separate Ingress and Egress checkpoints for each database and table.
- **Metadata:** Stores detailed database connection info and table schema (columns, types, PKs).
- **Status Tracking:** Real-time operational status (Snapshotting, CDC, Paused, Error) for each table.

## Control Plane
- **Monitoring API:** Go-based service managing pipeline configuration and user state.
- **Web Frontend:** TanStack Start app providing real-time visibility into throughput, lag, and errors.
- **Operations:** Ability to pause/resume pipelines, trigger snapshots, and manage configurations.
- **Authentication:** Username/password stored in NATS KV.

## Reliability & Scalability
- **Fault Isolation:** Two-step tracking allows the dashboard to distinguish between Ingress and Egress failures.
- **Redelivery:** Relies on NATS JetStream and Watermill's Ack/Nack for automatic redelivery on failure.
- **Consistency:** Sink-side deduplication ensures no duplicate data.
- **Scalability:** Stateless workers and API for easy horizontal scaling.
