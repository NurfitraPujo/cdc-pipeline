# Internal Sink: PnP Data Egress

The `internal/sink` package provides a **Plug-and-Play (PnP)** layer for writing data to target analytical stores. The core engine is agnostic to the specific database technology used for egress.

## The `Sink` Interface

Located in `provider.go`, the interface ensures all egress backends provide consistent behavior:
- `Name() string`: Returns the unique identifier for the sink type.
- `BatchUpload(ctx, messages)`: Uploads a batch of heterogeneous messages. Implementations must handle idempotency.
- **`ApplySchema(ctx context.Context, m protocol.Message)`**: Translates source metadata into target DDL (e.g., `CREATE TABLE`, `ALTER TABLE`). By passing the full `protocol.Message`, sinks like `postgres_debug` can capture cryptographic **Correlation IDs** and operational metadata during schema evolution.
- `Stop()`: Closes connection pools and releases resources.

## Supported Backends

- **Databend Sink (`databend/`)**:
    - High-performance implementation using the `databend-go` driver.
    - Implements **Heterogeneous Batching** to group records by column-set within a single CDC batch.
    - Automatic DDL mapping and execution.

## Plugging in New Backends

To support a new sink (e.g., ClickHouse, BigQuery, Snowflake):
1.  Create a new subdirectory (e.g., `internal/sink/clickhouse`).
2.  Implement the `Sink` interface.
3.  Add the corresponding type mapping logic for DDL application.

## Conventions

- **Batched Upserts**: PnP implementations should prioritize bulk operations (`VALUES (..), (..), ..`) for performance.
- **Idempotency**: Must use `UPSERT`, `REPLACE INTO`, or `ON CONFLICT` logic to ensure "at-least-once" message delivery does not create duplicates.
- **Column Sanitization**: Always quote column and table names to handle case-sensitivity and reserved keywords.
