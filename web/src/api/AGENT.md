# Web API Integration Layer

The `src/api/` directory encapsulates all backend HTTP integration logic, mapping REST endpoints to clean, typesafe functions.

## Key Files

- **`client.ts`**: The core API client. Implements a custom `ApiClient` wrapping the native `fetch` API. Handles content negotiation, standard JSON formatting, JWT token injection, and global `401 Unauthorized` intercepting to redirect to the login page.
- **`types.ts`**: Unified TypeScript types and interfaces representing the Go backend domain models (e.g., `SourceConfig`, `SinkConfig`, `PipelineConfig`, `PipelineStatus`, `WorkerStats`, `AuthResponse`).
- **`auth.ts`**: Authentication functions (e.g., `login`).
- **`pipelines.ts`**: Service layer functions for CRUD operations on pipelines (`listPipelines`, `getPipeline`, `createPipeline`, `updatePipeline`, `deletePipeline`).
- **`sources.ts`**: Service layer functions for CRUD operations on data sources (`listSources`, `createSource`, `deleteSource`).
- **`sinks.ts`**: Service layer functions for CRUD operations on analytical data sinks (`listSinks`, `createSink`, `deleteSink`).
- **`stats.ts`**: Services for fetching system metrics and table-level statistics.

## Conventions

1. **Authentication Token Persistence**: The authorization token is read from `localStorage` and injected via the `Authorization: Bearer <token>` header on every authenticated request.
2. **Error Propagation**: All request failures are deserialized into standard `ApiError` shapes (`{ message: string, code?: string, status?: number }`) and raised as Standard JS Errors to be caught by TanStack Query or error boundaries.
3. **Strict Type Safety**: All service calls must be strongly typed using interfaces from `types.ts`. Avoid utilizing `any` under all circumstances.
