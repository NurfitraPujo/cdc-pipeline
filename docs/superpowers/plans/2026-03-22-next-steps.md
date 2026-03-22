# Daya Data Pipeline - Roadmap & Next Steps

This document outlines the strategic directions to take the Daya Data Pipeline from a high-performance prototype to a production-ready platform.

## 1. Schema Evolution & Auto-DDL (Feature)
**Goal:** Automate the mapping of source schema changes to the analytical sink.
- **Auto-Provisioning:** Use `protocol.TableMetadata` to automatically run `CREATE TABLE IF NOT EXISTS` in the Sink upon first discovery.
- **DDL Handling:** Capture `ALTER TABLE` events from the PostgreSQL WAL and propagate them to the Sink.
- **Value:** Reduces operational overhead and ensures the data warehouse is always in sync with production.

## 2. Dead Letter Queue (DLQ) & Error Isolation (Reliability)
**Goal:** Prevent "Poison Pill" messages from blocking the entire pipeline.
- **Error Handling:** If a batch fails repeatedly, isolate the individual failed messages.
- **NATS DLQ:** Route failed messages to a dedicated `daya.pipeline.{id}.dlq` NATS topic.
- **Retry Logic:** Implement an exponential backoff for transient sink errors before routing to the DLQ.
- **Value:** Prevents Head-of-Line blocking and ensures continuous data flow.

## 3. Production Hardening (Infrastructure)
**Goal:** Prepare the services for deployment in a cloud-native environment.
- **Containerization:** Multi-stage `Dockerfiles` for both `cmd/api` and `cmd/pipeline`.
- **Kubernetes Orchestration:** Helm charts or K8s manifests including HPA rules and resource limits.
- **Structured Logging:** Replace standard `log` with `zerolog` for machine-readable JSON logs.
- **Value:** Standardizes the deployment process and improves log observability.

## 4. End-to-End Integration Testing (Validation)
**Goal:** Verify the full data flow with real dependencies.
- **Testcontainers:** Integrate `testcontainers-go` to orchestrate ephemeral Postgres, NATS, and Databend instances.
- **Data Integrity Tests:** Verify that a record inserted into Postgres correctly arrives in Databend after a full trip through the pipeline.
- **Value:** Catches integration bugs and environment-specific issues early.
