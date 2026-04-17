# Internal Source: PnP Data Ingress

The `internal/source` package implements a **Plug-and-Play (PnP)** architecture for data ingress. By defining a strict `Source` interface, the pipeline can support multiple database backends simultaneously.

## The `Source` Interface

Located in `provider.go`, the interface defines the contract for any ingress backend:
- `Name() string`: Returns the unique identifier for the backend type.
- `Start(ctx, config, checkpoint)`: Begins the data fetching loop (Snapshot or CDC). Returns a message channel and an acknowledgment channel.
- `Stop()`: Gracefully releases resources.

## Supported Backends

- **PostgreSQL CDC (`postgres/`)**:
    - Leverages logical replication via `go-pq-cdc`.
    - Handles snapshots and real-time CDC.
    - Implements **Chaotic-Safe Dynamic Discovery** via `ALTER PUBLICATION` for zero-downtime table additions.
    - **Non-blocking Acknowledgment**: Integrates a NATS Subscriber to listen for schema acknowledgments, preventing source-side deadlocks during pipeline restarts.
    - Implements dynamic table discovery.

## Plugging in New Backends

To add a new source (e.g., MySQL, MongoDB):
1.  Create a new subdirectory (e.g., `internal/source/mysql`).
2.  Implement the `Source` interface.
3.  Register the new provider in the worker factory logic.

## Conventions

- **Safe Shutdown (Cancel->Sleep->Close)**: Mandatory for all PnP implementations to prevent library-level crashes during worker transitions.
- **LSN-Aware**: Implementations must respect the provided `Checkpoint` to resume from the last known position.
- **Schema Discovery**: Whenever possible, backends should detect schema changes and emit `schema_change` messages.
