package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// containerProvider selects the testcontainers runtime, honouring
// TESTCONTAINER_PROVIDER=podman the same way the e2e suite does.
func containerProvider() testcontainers.ProviderType {
	if os.Getenv("TESTCONTAINER_PROVIDER") == "podman" {
		return testcontainers.ProviderPodman
	}
	return testcontainers.ProviderDocker
}

// discardingMetric is a no-op Metric used only to satisfy the Snapshotter
// constructor; this test asserts on chunk bookkeeping, not metrics.
type discardingMetric struct{}

func (discardingMetric) InsertOpIncrement(int64)                      {}
func (discardingMetric) UpdateOpIncrement(int64)                      {}
func (discardingMetric) DeleteOpIncrement(int64)                      {}
func (discardingMetric) SetCDCLatency(int64)                          {}
func (discardingMetric) SetProcessLatency(int64)                      {}
func (discardingMetric) SetSlotActivity(bool)                         {}
func (discardingMetric) SetSlotCurrentLSN(float64)                    {}
func (discardingMetric) SetSlotConfirmedFlushLSN(float64)             {}
func (discardingMetric) SetSlotRetainedWALSize(float64)               {}
func (discardingMetric) SetSlotLag(float64)                           {}
func (discardingMetric) SetSnapshotInProgress(bool)                   {}
func (discardingMetric) SetSnapshotTotalTables(int)                   {}
func (discardingMetric) SetSnapshotCompletedTables(int)               {}
func (discardingMetric) SnapshotRowsIncrement(int64)                  {}
func (discardingMetric) SetSnapshotDurationSeconds(float64)           {}
func (discardingMetric) SetSnapshotTotalChunks(int)                   {}
func (discardingMetric) SetSnapshotCompletedChunks(int)               {}
func (discardingMetric) SetSnapshotActiveWorkers(int)                 {}
func (discardingMetric) PrometheusCollectors() []prometheus.Collector { return nil }

// dataCounter counts Snapshot data events delivered to the handler.
type dataCounter struct {
	mu   sync.Mutex
	data int
}

func (c *dataCounter) inc() {
	c.mu.Lock()
	c.data++
	c.mu.Unlock()
}

func (c *dataCounter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data
}

// TestExecuteWorker_ConcurrentChunks drives the real concurrent snapshot worker
// path (vendored-patch: SC-1) against a real PostgreSQL via testcontainers.
//
// This is the coverage the reviewer flagged as missing: the old wiring test only
// asserted the config value propagated, so it could never catch that N goroutines
// sharing the single metadataConn/healthcheckConn would hammer pgx with
// concurrent Execs. Here N workers drain a fixture set of chunks for real, which
// makes the concurrent claimNextChunk/markChunkCompleted/loadJob and the shared
// heartbeat goroutine all run against the shared connections at once. Without
// the connMu serialisation a "conn busy" *connLockError escapes and the worker
// errors; with it, every chunk completes exactly once.
//
// Run with `go test github.com/Trendyol/go-pq-cdc/pq/snapshot -run
// TestExecuteWorker_ConcurrentChunks -race -v` from the repo root (uses the main
// module's dependency graph). Skipped under -short and when no container runtime
// is available.
func TestExecuteWorker_ConcurrentChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers integration test in short mode")
	}

	ctx := context.Background()
	logger.InitLogger(logger.NewSlog(slog.LevelWarn))
	pg, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("cdc_src"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{"-c", "wal_level=logical", "-c", "max_wal_senders=100", "-c", "max_replication_slots=100"},
			},
			ProviderType: containerProvider(),
		}),
	)
	if err != nil {
		t.Skipf("testcontainers unavailable (no container runtime): %v", err)
	}
	defer func() { _ = pg.Terminate(ctx) }()

	host, err := pg.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := pg.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatal(err)
	}
	dsn := fmt.Sprintf("postgres://postgres:postgres@%s:%s/cdc_src?sslmode=disable", host, port.Port())

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// --- Fixture: source data + bookkeeping metadata ---------------------
	const rowsPerTable = 1000
	const numChunks = 8
	const chunkSize = rowsPerTable / numChunks // 125

	mustExec(t, db, "CREATE TABLE public.sc_conc (id INT PRIMARY KEY, name TEXT)")
	for i := 0; i < rowsPerTable; i++ {
		mustExec(t, db, "INSERT INTO public.sc_conc (id, name) VALUES ($1, $2)", i, fmt.Sprintf("row-%d", i))
	}

	// The worker's chunk transactions call SET TRANSACTION SNAPSHOT, which needs
	// a live exported snapshot. Mirror what the coordinator does: open a
	// REPEATABLE READ transaction, export its snapshot, and keep it open for the
	// whole worker run.
	snapTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapTx.ExecContext(ctx, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		t.Fatal(err)
	}
	var snapshotID string
	if err := snapTx.QueryRowContext(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = snapTx.ExecContext(context.Background(), "ROLLBACK") }()

	// Bookkeeping tables (schema matches initTables).
	mustExec(t, db, `CREATE TABLE public.cdc_snapshot_job (
		slot_name TEXT PRIMARY KEY, snapshot_id TEXT NOT NULL,
		snapshot_lsn TEXT NOT NULL, started_at TIMESTAMP NOT NULL,
		completed BOOLEAN DEFAULT FALSE, total_chunks INT NOT NULL DEFAULT 0,
		completed_chunks INT NOT NULL DEFAULT 0)`)
	mustExec(t, db, `CREATE TABLE public.cdc_snapshot_chunks (
		id SERIAL PRIMARY KEY, slot_name TEXT NOT NULL,
		table_schema TEXT NOT NULL, table_name TEXT NOT NULL,
		chunk_index INT NOT NULL, chunk_start BIGINT NOT NULL, chunk_size BIGINT NOT NULL,
		range_start BIGINT, range_end BIGINT, block_start BIGINT, block_end BIGINT,
		is_last_chunk BOOLEAN NOT NULL DEFAULT FALSE,
		partition_strategy TEXT NOT NULL DEFAULT 'offset',
		status TEXT NOT NULL DEFAULT 'pending', claimed_by TEXT,
		claimed_at TIMESTAMP, heartbeat_at TIMESTAMP, completed_at TIMESTAMP,
		rows_processed BIGINT DEFAULT 0,
		UNIQUE(slot_name, table_schema, table_name, chunk_index))`)

	const slotName = "conc_slot"
	mustExec(t, db,
		"INSERT INTO public.cdc_snapshot_job (slot_name, snapshot_id, snapshot_lsn, started_at, completed, total_chunks, completed_chunks) VALUES ($1, $2, '0/1', now(), false, $3, 0)",
		slotName, snapshotID, numChunks)
	for i := 0; i < numChunks; i++ {
		mustExec(t, db,
			`INSERT INTO public.cdc_snapshot_chunks
			 (slot_name, table_schema, table_name, chunk_index, chunk_start, chunk_size, partition_strategy, status)
			 VALUES ($1, 'public', 'sc_conc', $2, $3, $4, 'offset', 'pending')`,
			slotName, i, i*chunkSize, chunkSize)
	}

	// --- Drive the concurrent worker --------------------------------
	cfg := config.SnapshotConfig{
		Concurrency:       2,
		ChunkSize:         int64(chunkSize),
		ClaimTimeout:      30 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		Mode:              config.SnapshotModeInitial,
	}
	s := New(cfg, publication.Tables{}, dsn, "public", discardingMetric{})
	if err := s.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer s.Close(ctx)

	count := &dataCounter{}
	handler := func(ev *format.Snapshot) error {
		if ev.EventType == format.SnapshotEventTypeData {
			count.inc()
		}
		return nil
	}

	job, err := s.loadJob(ctx, slotName)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.executeWorker(ctx, slotName, "test-worker", job, handler, time.Now()); err != nil {
		t.Fatalf("concurrent executeWorker failed (conn-busy or other): %v", err)
	}

	// --- Assert: every chunk completed exactly once ------------------
	if got := count.get(); got != rowsPerTable {
		t.Fatalf("handler saw %d data events, want %d", got, rowsPerTable)
	}

	rows, err := db.Query(
		"SELECT chunk_index, status, rows_processed FROM public.cdc_snapshot_chunks WHERE slot_name = $1 ORDER BY chunk_index", slotName)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var completed, totalRows int
	for rows.Next() {
		var idx int
		var status string
		var rp int64
		if err := rows.Scan(&idx, &status, &rp); err != nil {
			t.Fatal(err)
		}
		if status != "completed" {
			t.Fatalf("chunk %d left in status %q, want completed", idx, status)
		}
		if rp != chunkSize {
			t.Fatalf("chunk %d processed %d rows, want %d (double-processing?)", idx, rp, chunkSize)
		}
		completed++
		totalRows += int(rp)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if completed != numChunks {
		t.Fatalf("%d chunks completed, want %d", completed, numChunks)
	}
	if totalRows != rowsPerTable {
		t.Fatalf("total rows processed %d, want %d", totalRows, rowsPerTable)
	}
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("setup query failed: %v\nquery: %s", err, query)
	}
}
