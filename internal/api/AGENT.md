# Internal API: Control Plane

The `internal/api` package implements the RESTful Control Plane for the CDC Data Pipeline. It provides endpoints for managing the lifecycle of sources, sinks, and pipelines.

## Core Features

- **Framework**: Uses **Gin** for high-performance HTTP routing.
- **Dynamic Configuration**: All configurations are stored in and retrieved from **NATS Key-Value (KV)** store (`cdc-dp-config` bucket).
- **Authentication**: Secured via **JWT (JSON Web Tokens)**. See `auth.go` for the middleware and token handling logic.
- **Dynamic Rate Limiting**: Implements logic in `handler.go` to prevent configuration updates if a pipeline is currently in a `Transitioning` state.
- **Observability**:
    - **SSE (Server-Sent Events)**: Provides real-time metrics streaming for pipeline LSN progress and table stats.
    - **Health Checks**: Integrated with the dashboard for worker status monitoring.

## Web Frontend

The web dashboard is planned but not yet developed. See `web/plans/featureplan.md` for the full requirements specification. The planned dashboard will include:
- Authentication (Login Page)
- Home Dashboard with summary cards and gauges
- Global Configuration editor
- Pipeline List and Detail views
- Real-time metrics via SSE
- Source and Sink management

**Tech Stack**: React 19, TypeScript, Vite, TanStack Router/Query, Tailwind CSS, Zustand.

## Key Files

- **`handler.go`**: Contains the REST handlers for `Sources`, `Sinks`, and `Pipelines`.
- **`auth.go`**: Implements JWT-based authentication and user validation.
- **`api_test.go`**: Unit tests for API endpoints using standard Go testing and mocks.

## Conventions

- **Input Validation**: Uses `ozzo-validation` through the `protocol` package structures.
- **Error Responses**: Standardized JSON error format: `{"error": "message"}`.
- **Self-Cleaning**: The `ListPipelines` handler includes opportunistic cleanup of stale worker heartbeats.
