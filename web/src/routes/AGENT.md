# Routing Architecture & Page Views

The `src/routes/` directory defines the application's page structure and layouts, built on **TanStack Router** to enable file-based, typesafe routing.

## Route Structure

- **`__root.tsx`**: The master wrapper layout. Integrates the base HTML wrapper, navigation `Header`, `Footer`, active TanStack Query and DevTools wrappers, and routes state through the `<Outlet />` element.
- **`index.tsx`**: The entrypoint page that performs redirection logic (mapping to `/dashboard` for authenticated users and `/login` for anonymous traffic).
- **`login.tsx`**: The user authentication login page, facilitating JWT login requests.
- **`dashboard.tsx`**: The central operational dashboard. Aggregates system metrics into summary cards and lists current pipelines.
- **`config.tsx`**: The global configuration panel, containing the YAML Editor to manage pipeline engine configurations.
- **`pipelines/`**:
  - `index.tsx`: Displays all active data capture pipelines.
  - `create.tsx`: Multi-step configuration helper or direct YAML input editor to spin up new pipelines.
  - `$id/index.tsx`: Pipeline detail control deck, using `useSSE` to stream real-time metrics.
  - `$id/edit.tsx`: Configuration editor to modify active pipeline settings.
- **`sources/` & `sinks/`**: Dedicated subdirectories for registering and analyzing data connectors (PostgreSQL ingest sources and Databend egress sinks).

## Conventions & Rules

1. **Authentication Protection (Fencing)**: Fenced routes (all routes except `/login`) must assert the presence of a valid session JWT. If missing, they must trigger redirection to `/login` immediately.
2. **Typesafe Routing Parameters**: Route params (e.g., pipeline ID `$id`) must be strictly typed using TanStack Router's dynamic segment configurations to prevent missing-param errors.
3. **Route Lazy Loading**: Complex pages (like code editors or detail charts) should leverage code splitting and lazy component loading to keep the initial dashboard bundle lightweight.
