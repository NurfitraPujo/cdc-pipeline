# E2E Integration Testing - Design Specification

**Status:** Approved (2026-03-22)
**Authors:** Gemini CLI

## Overview
Daya Data Pipeline requires a comprehensive end-to-end integration test suite to verify the entire data flow from PostgreSQL through the Producer, NATS JetStream, and finally to the Databend sink. The suite will use **Testcontainers-go** to orchestrate ephemeral, isolated instances of all external dependencies.

## Architecture

### 1. Test Harness (`internal/test/e2e`)
A shared `Environment` struct will manage the lifecycle of the entire stack for each test case.

- **Orchestration:**
    - **NATS:** Starts with JetStream enabled (`-js`).
    - **PostgreSQL:** Starts with `wal_level=logical`. Automatically creates a `PUBLICATION` for all tables.
    - **Databend:** Starts in single-node mode.
- **Dynamic Configuration:** The harness will capture randomly assigned Docker host ports and generate valid `SourceConfig`, `SinkConfig`, and `PipelineConfig` objects automatically.
- **In-Process Worker:** The pipeline worker will run within the test process to allow for easy state inspection and code coverage reporting.

### 2. Assertion Strategy (Polling)
To handle the asynchronous nature of the pipeline and the analytical sink (Databend), the suite will use a "polling assertion" pattern instead of fixed sleeps.

- **`EventuallyCount(table, expected)`:** Polls the sink until the row count matches.
- **`EventuallyMatch(table, query, expectedMap)`:** Verifies that specific data updates have propagated correctly.
- **Checksum Verification:** For bulk tests, the harness will compare row counts and sampled data between source and sink.

### 3. Test Scenarios

| Scenario | Scope |
| :--- | :--- |
| **`InitialSnapshot`** | Verifies bulk sync of pre-existing Postgres data (10k+ rows). |
| **`LiveCDC`** | Verifies real-time propagation of `INSERT`, `UPDATE`, and `DELETE`. |
| **`DynamicDiscovery`** | Verifies that creating a table in Postgres during runtime triggers auto-discovery and snapshotting. |
| **`SchemaEvolution`** | Verifies that `ALTER TABLE` in Postgres correctly triggers DDL propagation to Databend. |
| **`NetworkResilience`** | Simulates NATS unavailability; verifies Circuit Breaker logic and recovery without data loss. |

## Dependencies
- `github.com/testcontainers/testcontainers-go`
- `github.com/stretchr/testify/require` (for assertions)
- `github.com/jackc/pgx/v5` (for Postgres interaction)
- `github.com/datafuselabs/databend-go` (for Databend interaction)

## Exit Criteria
The suite is considered complete when all five scenarios pass reliably in a single execution of `go test ./internal/test/e2e -v -count=1`.
