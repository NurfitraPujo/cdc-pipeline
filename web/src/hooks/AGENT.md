# Custom React Hooks

The `src/hooks/` directory defines reusable React hooks that encapsulate stateful side effects and core web api subscriptions.

## Key Hooks

- **`useSSE.ts`**: Facilitates full integration with the Go Control Plane SSE endpoints (e.g., `/api/v1/pipelines/{id}/metrics`).
  - **Auto-Authentication**: Appends the active JWT authorization token as a query parameter (since the browser standard `EventSource` doesn't natively support headers).
  - **Robust Reconnection**: Detects network droppage or stream failures and initiates automatic reconnection after a 3-second delay.
  - **Graceful Parsing**: Attempts to parse incoming messages as structured JSON models with raw string fallbacks to handle unstructured logs.
- **`useLocalStorage.ts`**: Provides state synchronization with the browser `localStorage` API, with state hydration support.

## Guidelines

1. **State Cleanups**: All subscription hooks must implement strict effect cleanups (e.g., calling `.close()` on `EventSource` or clearing timeouts) to prevent memory leaks during component unmounting.
2. **Reconnection Boundaries**: Reconnection attempts should avoid high-frequency spinning (use debounced timers) to mitigate DDOS side effects on the NATS/Gin metrics endpoints.
