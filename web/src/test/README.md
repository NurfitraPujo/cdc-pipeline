# CDC Pipeline Web Frontend Tests

This directory contains the test suite for the CDC Pipeline web dashboard.

## Test Stack

- **Vitest** - Test runner and assertion library
- **@testing-library/react** - React component testing utilities
- **@testing-library/user-event** - User interaction simulation
- **@testing-library/jest-dom** - Additional DOM matchers
- **MSW (Mock Service Worker)** - API mocking
- **happy-dom** - Browser environment for tests

## Directory Structure

```
test/
├── setup.ts                 # Test setup and MSW server configuration
├── utils.tsx                # Test utilities (renderWithProviders)
├── README.md                # This file
├── mocks/
│   ├── data.ts              # Test data factories
│   ├── handlers.ts          # MSW request handlers
│   └── server.ts            # MSW server setup
├── integration/             # Integration tests
│   ├── login.test.tsx       # Login flow tests
│   ├── dashboard.test.tsx   # Dashboard tests
│   ├── pipelines.test.tsx   # Pipeline CRUD tests
│   └── navigation.test.tsx  # Sidebar/routing tests
├── unit/                    # Unit tests
│   ├── api.test.ts          # API client tests
│   └── authStore.test.ts    # Zustand store tests
└── components/              # Component tests
    ├── MetricCard.test.tsx
    └── StatusBadge.test.tsx
```

## Running Tests

```bash
# Run all tests
pnpm test

# Run tests in watch mode
pnpm vitest

# Run tests with coverage
pnpm vitest --coverage

# Run specific test file
pnpm vitest src/test/unit/api.test.ts
```

## Test Status

### ✅ Working Tests
- **Unit Tests**: API client, Auth store
- **Component Tests**: MetricCard, StatusBadge

### ⚠️ Known Issues
- **Integration Tests**: Router-based integration tests have mocking challenges with:
  - Zustand store `getState()` method in `beforeLoad` hooks
  - React Router context initialization
  - Auth state synchronization

These tests work individually but have issues when run together due to module mocking conflicts.

## Mocking

### API Mocking with MSW

All API calls are mocked using MSW. The mock handlers are defined in `mocks/handlers.ts`.

To override handlers for specific tests:

```typescript
import { server } from "../mocks/server";
import { errorHandlers } from "../mocks/handlers";

// Use error handler for a test
server.use(errorHandlers.loginError);
```

### Mock Data

Test data factories are available in `mocks/data.ts`:

- `createMockToken()` - Generate JWT token
- `createMockPipeline(overrides)` - Create pipeline with defaults
- `mockStatsSummary` - Sample stats data
- `mockPipelines` - Array of sample pipelines
- `mockSources` / `mockSinks` - Sample connection data

## Best Practices

1. **Use test utilities**: Always use `renderWithProviders` for components that need QueryClient.

2. **Mock external dependencies**: Mock stores, API calls, and browser APIs.

3. **Test user behavior**: Write tests from the user's perspective.

4. **Clean up after tests**: MSW server is reset after each test automatically.

5. **Use user-event**: Prefer `@testing-library/user-event` over `fireEvent`.

## Coverage Report

After running tests with coverage, view the HTML report:

```bash
open coverage/index.html
```

## Adding New Tests

### Unit Tests
Test individual functions/modules in isolation:

```typescript
import { describe, it, expect } from "vitest";
import { myFunction } from "@/utils/myFunction";

describe("myFunction", () => {
  it("should do something", () => {
    expect(myFunction()).toBe(expected);
  });
});
```

### Component Tests
Test React components with mocked dependencies:

```typescript
import { describe, it, expect } from "vitest";
import { screen } from "@testing-library/react";
import { renderWithProviders } from "../utils";
import MyComponent from "@/components/MyComponent";

describe("MyComponent", () => {
  it("should render", () => {
    renderWithProviders(<MyComponent />);
    expect(screen.getByText("Hello")).toBeInTheDocument();
  });
});
```
