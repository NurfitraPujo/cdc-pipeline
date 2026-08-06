package snapshot

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

// Sentinel errors for snapshot operations
var (
	// ErrSnapshotInvalidated indicates the snapshot transaction was closed (coordinator restart)
	ErrSnapshotInvalidated = goerrors.New("snapshot invalidated by coordinator restart")
)

// Handler SnapshotHandler is a function that handles snapshot events
type Handler func(event *format.Snapshot) error

type Snapshotter struct {
	metadataConn       pq.Connection
	healthcheckConn    pq.Connection
	exportSnapshotConn pq.Connection
	// connMu serialises every SQL statement issued against the two shared
	// connections (metadataConn, healthcheckConn). The historical library runs
	// one worker process, which owns its connections exclusively; SC-1's
	// in-process concurrency runs N workers sharing these two, and the
	// underlying jackc/pgx v5 *pgconn.PgConn is NOT concurrency-safe -- a
	// second concurrent Exec returns a *connLockError{"conn busy"}.
	// execQueryShared (helpers.go) takes this lock so workers never overlap an
	// Exec on either connection. The connection-pool connections used for the
	// long-running per-chunk SELECTs are excluded: they are checked out one at a
	// time and exclusively owned, so locking them would serialise the very data
	// path the concurrency knob exists to parallelise.
	connMu           sync.Mutex
	metric           metric.Metric
	typeMap          *pgtype.Map
	decoderCache     *DecoderCache
	connectionPool   *ConnectionPool
	orderByCache     map[string]orderByCacheEntry
	dsn              string
	cachedSnapshotID string
	tables           publication.Tables
	config           config.SnapshotConfig
	// vendored-patch: MS-2 (MULTI_SCHEMA_PLAN.md §3 Stage 4, task 3) - the schema
	// the cdc_snapshot_job/cdc_snapshot_chunks bookkeeping tables are created in
	// and checked against. Resolved once, in New(), from the same SearchPath the
	// metadataConn's DSN pins (vendored-patch MS-1) -- see resolveMetadataSchema.
	// Keeping this as an explicit field (rather than re-deriving it at each call
	// site, or trusting the connection's live search_path) means initTables'
	// CREATE TABLE and tableExists'/indexExists' existence checks are
	// structurally guaranteed to agree: both read this one field.
	metadataSchema string
	orderByMu      sync.RWMutex
}

type orderByCacheEntry struct {
	clause  string
	columns []string
}

// vendored-patch: T1-4 - Deferred connection establishment to avoid eager allocation when snapshot is skipped
//
// vendored-patch: MS-2 (MULTI_SCHEMA_PLAN.md §3 Stage 4, task 3) - gained the
// searchPath parameter. Before this, initTables' CREATE TABLE statements
// created cdc_snapshot_job/cdc_snapshot_chunks unqualified (resolving against
// whatever search_path the metadataConn happened to have -- the same
// SearchPath value MS-1 pins via dsn), while tableExists/indexExists hardcoded
// table_schema/schemaname = 'public'. Once a caller pins SearchPath to a
// non-public schema (Stage 2), the two disagree: the tables land in the
// pinned schema but the existence check keeps looking in 'public', so
// initTables never finds what it just created and re-runs CREATE TABLE --
// erroring "relation already exists" -- on every restart. searchPath is
// resolved once here into metadataSchema so both sides read the same value.
func New(snapshotConfig config.SnapshotConfig, tables publication.Tables, dsn string, searchPath string, m metric.Metric) *Snapshotter {
	// Create decoder cache for efficient type decoding
	decoderCache := NewDecoderCache()

	return &Snapshotter{
		dsn:            dsn,
		decoderCache:   decoderCache,
		config:         snapshotConfig,
		tables:         tables,
		typeMap:        pgtype.NewMap(),
		metric:         m,
		orderByCache:   make(map[string]orderByCacheEntry),
		metadataSchema: resolveMetadataSchema(searchPath),
	}
}

// resolveMetadataSchema mirrors Postgres's own unqualified-name resolution
// rule for an unqualified CREATE TABLE: the first schema named in
// search_path. searchPath is the same comma-separated value MS-1's
// Config.SearchPath pins into the DSN (empty when unset, in which case the
// connection falls back to the server's default search_path, which is
// "public" for every deployment this library is used against -- see
// SourceConfig.Schemas' "empty means public only" rule, MULTI_SCHEMA_PLAN.md
// §8 item 4). vendored-patch: MS-2.
func resolveMetadataSchema(searchPath string) string {
	if searchPath == "" {
		return "public"
	}
	first, _, _ := strings.Cut(searchPath, ",")
	first = strings.TrimSpace(first)
	if first == "" {
		return "public"
	}
	return first
}

// vendored-patch: T1-4 - Connect establishes database connections lazily.
// Call this only when snapshot is actually needed.
func (s *Snapshotter) Connect(ctx context.Context) error {
	metadataConn, err := pq.NewConnection(ctx, s.dsn)
	if err != nil {
		return errors.Wrap(err, "create metadata connection")
	}
	s.metadataConn = metadataConn

	healthcheckConn, err := pq.NewConnection(ctx, s.dsn)
	if err != nil {
		return errors.Wrap(err, "create healthcheck connection")
	}
	s.healthcheckConn = healthcheckConn

	// Create connection pool for chunk processing (5 connections)
	connectionPool, err := NewConnectionPool(ctx, s.dsn, 5)
	if err != nil {
		return errors.Wrap(err, "create connection pool")
	}
	s.connectionPool = connectionPool

	return nil
}

// Prepare sets up snapshot metadata and exports snapshot transaction
// This must be called BEFORE creating the replication slot to avoid data loss
// Returns the snapshot LSN that should be used for replication slot creation
//
// Flow:
//  1. Coordinator election
//  2. Capture current LSN
//  3. Create metadata (job, chunks)
//  4. Export snapshot transaction (keeps transaction OPEN)
//  5. Return LSN for slot creation
//
// IMPORTANT: Replication slot MUST be created immediately after this returns
// to ensure no WAL changes are lost during snapshot execution
func (s *Snapshotter) Prepare(ctx context.Context, slotName string) error {
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] preparing", "instanceID", instanceID)

	isCoordinator, err := s.setupJob(ctx, slotName, instanceID)
	if err != nil {
		return errors.Wrap(err, "setup job")
	}

	if isCoordinator {
		logger.Debug("[coordinator] snapshot transaction kept OPEN - replication slot must be created NOW")
	}
	return nil
}

// Execute performs the actual snapshot data collection
// This should be called AFTER the replication slot is created with the LSN from Prepare()
// Returns when snapshot is complete
func (s *Snapshotter) Execute(ctx context.Context, handler Handler, slotName string) error {
	startTime := time.Now()
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] executing", "instanceID", instanceID)

	// Load job
	job, err := s.loadJob(ctx, slotName)
	if err != nil || job == nil {
		return errors.New("job not found - Prepare() must be called first")
	}

	// Execute worker processing (ALL instances work, including coordinator)
	if err := s.executeWorker(ctx, slotName, instanceID, job, handler, startTime); err != nil {
		return errors.Wrap(err, "execute worker")
	}

	// Finalize (check completion, send END marker)
	if err := s.finalizeSnapshot(ctx, slotName, job, handler); err != nil {
		return errors.Wrap(err, "finalize snapshot")
	}

	logger.Info("[snapshot] execution completed", "instanceID", instanceID, "duration", time.Since(startTime))
	return nil
}

// finalizeSnapshot checks completion, closes connections, and sends END marker
func (s *Snapshotter) finalizeSnapshot(ctx context.Context, slotName string, job *Job, handler Handler) error {
	allCompleted, err := s.checkJobCompleted(ctx, slotName)
	if err != nil {
		return errors.Wrap(err, "check job completed")
	}

	if !allCompleted {
		return nil // Not done yet, keep processing
	}

	logger.Info("[snapshot] all chunks completed, finalizing snapshot")

	// Mark job as completed (idempotent - safe for multiple workers)
	if err := s.markJobAsCompleted(ctx, slotName); err != nil {
		logger.Warn("[snapshot] failed to mark job as completed", "error", err)
		return err
	}

	// Close all connections now that snapshot is complete
	s.closeAllConnections(ctx, true)

	// Send END marker
	return handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeEnd,
		ServerTime: time.Now().UTC(),
		LSN:        job.SnapshotLSN,
	})
}

// closeAllConnections closes all snapshot connections
// commitExport: if true, commits the export snapshot transaction; if false, rolls it back
func (s *Snapshotter) closeAllConnections(ctx context.Context, commitExport bool) {
	logger.Info("[snapshot] closing all connections")

	// Close export snapshot connection (coordinator only)
	if s.exportSnapshotConn != nil {
		s.closeExportSnapshotConnection(ctx, commitExport)
	}

	// Close connection pool
	if s.connectionPool != nil {
		s.connectionPool.Close(ctx)
		s.connectionPool = nil
	}

	// Close metadata connection
	if s.metadataConn != nil {
		if err := s.metadataConn.Close(ctx); err != nil {
			logger.Warn("[snapshot] error closing metadata connection", "error", err)
		}
		s.metadataConn = nil
	}

	// Close healthcheck connection
	if s.healthcheckConn != nil {
		if err := s.healthcheckConn.Close(ctx); err != nil {
			logger.Warn("[snapshot] error closing healthcheck connection", "error", err)
		}
		s.healthcheckConn = nil
	}

	logger.Info("[snapshot] all connections closed")
}

// closeExportSnapshotConnection commits or rolls back and closes the export snapshot connection
func (s *Snapshotter) closeExportSnapshotConnection(ctx context.Context, commit bool) {
	if commit {
		logger.Info("[coordinator] committing and closing snapshot export connection")
		if err := s.execSQL(ctx, s.exportSnapshotConn, "COMMIT"); err != nil {
			logger.Warn("[coordinator] failed to commit snapshot transaction, attempting rollback", "error", err)
			if rollbackErr := s.execSQL(ctx, s.exportSnapshotConn, "ROLLBACK"); rollbackErr != nil {
				logger.Error("[coordinator] failed to rollback snapshot transaction", "error", rollbackErr)
			}
		}
	} else {
		logger.Info("[coordinator] rolling back and closing snapshot export connection")
		if err := s.execSQL(ctx, s.exportSnapshotConn, "ROLLBACK"); err != nil {
			logger.Warn("[coordinator] failed to rollback snapshot transaction", "error", err)
		}
	}

	if err := s.exportSnapshotConn.Close(ctx); err != nil {
		logger.Warn("[coordinator] error closing export snapshot connection", "error", err)
	}
	s.exportSnapshotConn = nil
}

// Close closes all connections held by the Snapshotter
// Safe to call multiple times (idempotent)
// This is a fallback for cleanup in case snapshot doesn't complete normally
func (s *Snapshotter) Close(ctx context.Context) {
	if s == nil {
		return
	}
	logger.Debug("[snapshot] closing snapshotter (fallback cleanup)")
	s.closeAllConnections(ctx, false) // Rollback on abnormal termination
}

// decodeColumnData decodes PostgreSQL column data using cached decoder
func (s *Snapshotter) decodeColumnData(data []byte, dataTypeOID uint32) (interface{}, error) {
	// Use cached decoder (optimization: avoid reflection overhead)
	decoder := s.decoderCache.Get(dataTypeOID)
	return decoder.Decode(s.typeMap, data)
}

// generateInstanceID generates a unique instance identifier
func generateInstanceID(configuredID string) string {
	if configuredID != "" {
		return configuredID
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	return fmt.Sprintf("%s-%d", hostname, pid)
}
