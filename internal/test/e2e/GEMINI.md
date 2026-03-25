# End-to-End (E2E) Testing

The `internal/test/e2e` package contains comprehensive integration tests that verify the entire pipeline flow using real infrastructure instances.

## Core Features

- **Infrastructure Isolation**: Uses **Testcontainers-go** to spin up fresh, isolated instances of:
    - **PostgreSQL**: Configured with `wal_level=logical`.
    - **NATS**: Configured with `-js` (JetStream enabled).
    - **Databend**: Latest available image.
- **Scenario Coverage**:
    - **Initial Snapshot**: Verifies that existing data in Postgres is correctly copied to Databend before CDC begins.
    - **Live CDC**: Verifies `INSERT`, `UPDATE`, and `DELETE` operations are synced in real-time.
    - **Schema Evolution**: Verifies that `ALTER TABLE` commands in Postgres are automatically propagated to Databend.
    - **Dynamic Discovery**: Verifies that creating a new table in Postgres triggers a pipeline reload and sync.
    - **DLQ & Retries**: Verifies poison-pill handling and routing to the Dead Letter Queue.

## Key Files

- **`env.go`**: Centralized setup and teardown logic for the E2E environment.
- **`containers.go`**: Testcontainer definitions and wait strategies.
- **`*_test.go`**: Individual test scenarios.

## Conventions

- **Eventually Assertions**: Uses `require.Eventually` to handle the asynchronous nature of the pipeline, allowing time for data to propagate through NATS and into the sink.
- **Clean State**: Every test scenario should ideally use unique table names or fresh container instances to avoid side effects.
- **Timeout Management**: E2E tests have a default timeout of 10 minutes (`go test -v -timeout 10m`) due to container startup times.
- **Logical Replication**: The Postgres container is explicitly started with logical replication flags to support CDC.
