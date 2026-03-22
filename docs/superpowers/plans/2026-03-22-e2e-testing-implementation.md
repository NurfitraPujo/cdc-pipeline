# E2E Integration Testing Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a comprehensive end-to-end integration test suite using Testcontainers-go to verify the full data pipeline from PostgreSQL to Databend.

**Architecture:** A centralized `Environment` harness in `internal/test/e2e` manages container lifecycles, dynamic configuration, and polling assertions.

**Tech Stack:** Go 1.26+, Testcontainers-go, NATS JetStream, PostgreSQL, Databend, Testify.

---

### Task 1: Environment Harness Setup

**Files:**
- Create: `internal/test/e2e/env.go`
- Create: `internal/test/e2e/containers.go`

- [ ] **Step 1: Implement container orchestration in `containers.go`**
Define `StartPostgres`, `StartNats`, and `StartDatabend` functions using Testcontainers. Ensure Postgres starts with `wal_level=logical`.
- [ ] **Step 2: Implement `Environment` struct in `env.go`**
Add `Setup(t *testing.T)` which starts all containers, returns DSNs, and registers `t.Cleanup` for automatic teardown.
- [ ] **Step 3: Implement In-Process Worker helper**
Add a method to start a real `pipeline.Worker` within the test process using the container DSNs.
- [ ] **Step 4: Commit**
`git add internal/test/e2e && git commit -m "feat: implement e2e test harness and container orchestration"`

---

### Task 2: Seeding & Assertion Helpers

**Files:**
- Modify: `internal/test/e2e/env.go`

- [ ] **Step 1: Implement `Postgres.Seed` helper**
Method to generate and insert dummy data into Postgres.
- [ ] **Step 2: Implement `Databend.EventuallyCount` helper**
Polling logic using `require.Eventually` to check row counts in the sink.
- [ ] **Step 3: Implement `Databend.EventuallyMatch` helper**
Logic to verify specific row content in Databend.
- [ ] **Step 4: Commit**
`git add internal/test/e2e && git commit -m "feat: add data seeding and polling assertion helpers"`

---

### Task 3: Happy Path Scenarios (Snapshot & CDC)

**Files:**
- Create: `internal/test/e2e/snapshot_test.go`
- Create: `internal/test/e2e/cdc_test.go`

- [ ] **Step 1: Write `TestE2E_InitialSnapshot`**
Verify 5,000 pre-existing rows are synced on worker start.
- [ ] **Step 2: Write `TestE2E_LiveCDC`**
Verify real-time `INSERT`, `UPDATE`, `DELETE` propagation.
- [ ] **Step 3: Run tests**
`go test ./internal/test/e2e/... -v`
- [ ] **Step 4: Commit**
`git add internal/test/e2e && git commit -m "test: add snapshot and live cdc e2e scenarios"`

---

### Task 4: Platform Scenarios (Discovery & Evolution)

**Files:**
- Create: `internal/test/e2e/discovery_test.go`
- Create: `internal/test/e2e/evolution_test.go`

- [ ] **Step 1: Write `TestE2E_DynamicDiscovery`**
Create a new table during runtime; verify auto-detect and sync.
- [ ] **Step 2: Write `TestE2E_SchemaEvolution`**
Add column to Postgres; verify Databend DDL propagation and data sync.
- [ ] **Step 3: Run tests**
`go test ./internal/test/e2e/... -v`
- [ ] **Step 4: Commit**
`git add internal/test/e2e && git commit -m "test: add dynamic discovery and schema evolution e2e scenarios"`

---

### Task 5: Failure Path Scenario (Resilience)

**Files:**
- Create: `internal/test/e2e/resilience_test.go`

- [ ] **Step 1: Write `TestE2E_NetworkResilience`**
Stop NATS container; verify worker pauses; resume NATS; verify worker recovers without data loss.
- [ ] **Step 2: Final full suite run**
`go test ./internal/test/e2e/... -v -count=1`
- [ ] **Step 3: Commit**
`git add internal/test/e2e && git commit -m "test: add network resilience e2e scenario"`
