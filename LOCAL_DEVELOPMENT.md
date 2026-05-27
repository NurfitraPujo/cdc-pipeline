# Local Development Guide

This guide outlines how to set up, run, and test the CDC Data Pipeline locally. It includes instructions for running individual services locally as well as orchestrating the entire stack using containerization.

---

## Running Services Separately (Bare Metal)

### Backend (Go)
1. **Start NATS JetStream locally**:
   ```bash
   nats-server -js
   ```
2. **Start API Server**:
   ```bash
   go run ./cmd/api
   ```
3. **Start Pipeline Worker**:
   ```bash
   go run ./cmd/pipeline
   ```
4. **Run Unit Tests**:
   ```bash
   go test ./internal/...
   ```

### Frontend (React)
1. **Install Dependencies**:
   ```bash
   cd web && pnpm install
   ```
2. **Start Development Server**:
   ```bash
   pnpm dev
   ```

---

## Orchestrating with Docker Compose / Podman Compose

You can spin up the entire CDC data pipeline infrastructure, backend servers, and web dashboard with a single command. Since you are using **Podman**, the instructions below are configured to use `podman compose`.

### Services & Ports Map

The compose setup deploys 7 services coordinated in a secure bridge network (`cdc-network`):

| Container Name | Service | Host Port | Protocol / API |
| :--- | :--- | :--- | :--- |
| `cdc-nats` | NATS JetStream | `4222`, `8222` | Core broker & Admin console |
| `cdc-postgres-source` | PostgreSQL (Source) | `5432` | Logical replication source database |
| `cdc-postgres-debug` | PostgreSQL (Debug Sink) | `5433` | Egress audit logging database |
| `cdc-databend` | Databend (Sink) | `8000`, `3307` | Analytical engine query endpoints |
| `cdc-api` | Control Plane API | `8080` | REST API (Gin) & Swagger endpoints |
| `cdc-pipeline` | Stateful Orchestrator | `8081` | Worker status checks & SRE Metrics |
| `cdc-web-dashboard` | Web Dashboard | `3000` | TanStack Start React Admin |

---

### Step 1: Spin Up the Stack

To build the custom images (`api`, `pipeline`, and `web-dashboard`) and spin up the complete environment, execute:

```bash
podman compose up --build -d
```

#### Checking Service Health
To query the status of the services and their healthchecks, run:

```bash
podman compose ps
```

All infrastructure services (`nats`, `postgres-source`, `postgres-debug`, `databend`) will show as `healthy`. The Go and React containers will automatically wait for their prerequisites before starting.

---

### Step 2: Live Log Auditing

You can monitor the dynamic config bootstrapping and connection establishments by tracing the logs:

```bash
podman compose logs -f pipeline
```

Behind the scenes:
1. **Dynamic Config Bootstrapping**: `pipeline` starts, checks NATS KV, and initializes configurations.
2. **Runtime Configuration Overrides**: It reads environment variables (such as `POSTGRES_SOURCE_HOST`, `POSTGRES_PASSWORD`, `DATABEND_HOST`, etc.) to dynamically override default configurations at runtime, maintaining complete separation between the image build stage and runtime credentials/topology.
3. **Replication Streaming**: The pipeline automatically spawns a logical replication slot on `cdc-postgres-source` and opens batch consumers to `cdc-databend` and `cdc-postgres-debug`.

---

### Step 3: Accessing the Control Tools

#### 1. Web Dashboard
Open your browser and navigate to:
👉 **[http://localhost:3000](http://localhost:3000)**
This gives you access to the modern TanStack Start dashboard showing real-time throughput, operational stats, and configuration editors.

#### 2. Interactive Swagger API Docs
To view and test the REST endpoints manually:
👉 **[http://localhost:8080/swagger/index.html](http://localhost:8080/swagger/index.html)**

---

### Step 4: Verification & Smoke Test

To verify that Change Data Capture (CDC) replication is executing end-to-end:

1. **Insert sample data into PostgreSQL source**:
   Connect to `postgres-source` and insert some rows:
   ```bash
   podman exec -it cdc-postgres-source psql -U postgres -d production_db -c \
   "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100));"
   
   podman exec -it cdc-postgres-source psql -U postgres -d production_db -c \
   "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com');"
   ```

2. **Verify Analytical Replication in Databend**:
   Query Databend to confirm the change records were captured, batched, and replicated:
   ```bash
   podman exec -it cdc-databend bendctl query "SELECT * FROM users;"
   ```

3. **Verify Debug Logs in the Debug Sink**:
   Check the `cdc-postgres-debug` instance to audit the lineage records:
   ```bash
   podman exec -it cdc-postgres-debug psql -U postgres -d debug_db -c \
   "SELECT * FROM cdc_debug_messages;"
   ```

---

### Shutting Down

To stop the containers and tear down the network (while preserving database volume data), run:

```bash
podman compose down
```

To fully reset the environment (including all database volumes):

```bash
podman compose down -v
```
