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
    - **Dynamic Discovery**: Verifies that creating a new table in Postgres triggers a pipeline reload and sync. Includes **Production Chaos** testing: inserting data immediately after DDL without waiting for discovery to finish, verified via JetStream buffering.
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

## Asserting against Databend

Databend assertions **must be schema-qualified**. A Postgres schema maps to a Databend database,
so a table seeded into `public` lands in the `public` database -- not the one the DSN selects. Use
the `qualifyTarget()` helper in `env.go` (or write `"public"."table"` explicitly) rather than a
bare name, which resolves against the DSN default and fails with Databend error **1025 (unknown
table)** even when the row synced correctly.

## Running the suite

Do not run all 31 tests in one command. Output is buffered per package, so an interrupted run
leaves an empty log, and sequential container churn makes the testcontainers Postgres wait strategy
time out. Use `-v -p 1`, run in batches of ~5, and redirect to a log.

**Before diagnosing a red suite, grep for `matched 1 times, expected 2`.** That is the container
startup flake, not a code defect -- re-run the affected tests in isolation to confirm.
