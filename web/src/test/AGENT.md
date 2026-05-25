# Automated Testing Suite

The `src/test/` directory contains the automated test suite for the CDC Pipeline web dashboard, validating core units, interactive components, and E2E integration flows.

## Test Stack

- **Runner**: **Vitest** for fast, concurrent test execution and native ESM support.
- **Environment**: **happy-dom** for a lightweight, fast simulated browser environment.
- **Component Renderers**: **@testing-library/react** for testing components in a way that closely resembles how they are used in production.
- **User Interactions**: **@testing-library/user-event** for realistic browser event simulation.
- **API Mocking**: **MSW (Mock Service Worker)** to intercept and mock REST network traffic at the request layer without polluting unit codes with brittle fetch stubs.

## Directory Structure

- **`mocks/`**: MSW server configuration and mock data suppliers.
  - `server.ts`: Initialized MSW server.
  - `handlers.ts`: REST endpoint handlers mapping success payloads and error states.
  - `data.ts`: Factory helpers generating deterministic test fixtures (`createMockPipeline`, `createMockToken`, `mockStatsSummary`).
- **`unit/`**: Isolated unit tests validating independent modules, utility functions, stores (`authStore.test.ts`), and API services (`api.test.ts`).
- **`components/`**: Presentation testing for reusable UI elements (`MetricCard.test.tsx`, `StatusBadge.test.tsx`), validating correct CSS styling output and accessibility attributes.
- **`integration/`**: Integration tests asserting correct page workflows and navigation under real-world scenarios.
  - `login.test.tsx`: Validates credential submission and successful token storage.
  - `dashboard.test.tsx`: Asserts metrics presentation and lists active pipelines.
  - `pipelines.test.tsx`: Validates pipeline CRUD flows, inputs, and form validations.
  - `navigation.test.tsx`: Checks router state transitions and sidebar controls.

## Test Utilities & Helpers

- **`setup.ts`**: Global configuration file setting up MSW server lifehooks (`beforeAll`, `afterEach`, `afterAll`), registering `jest-dom` matchers, and configuring testing environment limits.
- **`utils.tsx`**: Provides the custom `renderWithProviders` and `renderWithRouter` functions to mount components with query providers and authentication contexts already hydrated:
  ```typescript
  // Render routes in authenticated mode
  renderWithRouter("/dashboard", { authenticated: true });
  ```

## Best Practices

1. **Simulate Real Use**: Assert against user-visible texts or roles (`screen.getByRole`) rather than fragile internal HTML selectors.
2. **Handle Cleanup**: Ensure asynchronous operations are awaited correctly (`find*` queries) to avoid bleeding async state across tests.
3. **MSW Resets**: MSW routes are automatically reset after each test block inside `setup.ts`. Always define specific mock overrides inside the individual test files using `server.use()` to maintain isolation.
