# CDC Data Pipeline Agent Instructions

Refer to [AGENT.md](./AGENT.md) for architectural guidelines.

## 🚀 The Steering Loop & Harness Rules

As an AI coding agent, you must operate within a strict harness of feedforward guides and feedback sensors:

### 1. Fast Feedback Loop (Sensors)
- **Always** run local sensors after any code changes:
  - Go Backend: `go vet ./...` and `golangci-lint run ./...`
  - React Frontend: `cd web && pnpm check`
  - Regression Tests: `go test -short ./...`
- Do not consider a task complete or ask for review until all local sensors pass successfully.

### 2. Strict End-to-End (E2E) Test Rules
- **E2E tests are the ultimate safety net**: You are strictly **prohibited from altering or deleting E2E tests** (located in `internal/test/e2e/`) to bypass a failing test.
- If an E2E test fails, treat it as a critical regression. Inspect and fix the underlying implementation code.
- Any change to existing E2E tests requires explicit human approval.
- **System-Wide Behaviors**: If you introduce new system-wide behavior or new configuration transitions, you **must write a corresponding E2E integration test** in the E2E suite to safeguard it.
