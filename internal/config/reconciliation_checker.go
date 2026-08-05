package config

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	// databend-go registers the "databend" database/sql driver used to open sink connections for reconciliation.
	_ "github.com/datafuselabs/databend-go"
	libpq "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// reconcileChunk is one row of the ordered chunk list
// NewPostgresDatabendReconcileStepper walks -- the same integer_range
// chunks the snapshotter recorded in cdc_snapshot_chunks (plan WS-7 shape:
// "reuse the same integer_range chunking as the snapshotter"), read back
// rather than recomputed, so reconciliation's PK ranges always agree with
// what was actually snapshotted for this slot.
type reconcileChunk struct {
	TableSchema string
	TableName   string
	RangeStart  int64
	RangeEnd    int64
}

// NewPostgresDatabendReconcileStepper builds the production ReconcileStepFunc
// (plan WS-7). Per chunk it:
//
//  1. Re-reads the pipeline's ordered integer_range chunk list from
//     cdc_snapshot_chunks (queryIntegerRangeChunks) -- cheap metadata, not
//     cached, so a chunk list that grows or shrinks between ticks (e.g. a
//     later re-snapshot) is picked up automatically.
//  2. Resolves the chunk at prev.NextChunkOrdinal, looks up that table's
//     primary key column (source: information_schema; sink: the durable
//     cdc_meta.pk_columns table WS-4.6 already populates -- see
//     internal/sink/databend/sink.go's persistPKMetadata/loadPKMetadata,
//     reused here by direct SQL rather than instantiating a full
//     DatabendSink for one query).
//  3. Reads the PK set present in that range on both sides and computes
//     sink-minus-source: rows the sink still has that the source no longer
//     does.
//  4. Soft-deletes those rows the same way the live sink path does
//     (UPDATE ... SET deleted_at = now(), never a hard DELETE -- plan
//     WS-4/PIPE-OQ-5's "soft delete everywhere" applies here too, since a
//     hard DELETE from a background sweep racing a redelivered upsert for
//     the same key would be a correctness regression the live path was
//     specifically changed to avoid).
//
// Only single-column integer primary keys are handled -- the same
// constraint integer_range chunking itself requires (OQ-5/OQ-7); a table
// whose chunks are NOT integer_range never reaches this stepper's PK
// resolution because queryIntegerRangeChunks only returns integer_range
// rows in the first place.
func NewPostgresDatabendReconcileStepper(kv nats.KeyValue) ReconcileStepFunc {
	return func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig, prev protocol.ReconciliationProgress) (protocol.ReconciliationProgress, bool, error) {
		if len(cfg.Sources) == 0 || len(cfg.Sinks) == 0 {
			return prev, false, fmt.Errorf("reconciliation: pipeline %s has no source/sink configured", pipelineID)
		}

		srcDB, slotName, err := openSourceDBForHealthCheck(kv, cfg.Sources[0])
		if err != nil {
			return prev, false, fmt.Errorf("reconciliation: open source: %w", err)
		}
		defer func() { _ = srcDB.Close() }()

		sinkDB, err := openSinkDBForReconciliation(kv, cfg.Sinks[0])
		if err != nil {
			return prev, false, fmt.Errorf("reconciliation: open sink: %w", err)
		}
		defer func() { _ = sinkDB.Close() }()

		chunks, err := queryIntegerRangeChunks(ctx, srcDB, resolveSnapshotJobSchema(cfg), slotName)
		if err != nil {
			return prev, false, fmt.Errorf("reconciliation: list chunks: %w", err)
		}

		next := prev
		next.ChunksTotal = len(chunks)
		if len(chunks) == 0 {
			// No integer_range chunks recorded for this slot (e.g. every
			// table degraded per OQ-7, or the source's search_path put
			// cdc_snapshot_chunks somewhere other than
			// resolveSnapshotJobSchema's hardcoded "public"): this is
			// "nothing reconcilable", NOT "nothing to reconcile" -- there
			// is zero coverage this sweep could ever have provided, so it
			// must never be reported complete=true (plan 4.4 invariant 5,
			// section 10 OQ-7's "degrade explicitly (log + stale)").
			// Returning an error here routes through
			// maybeSweepReconciliation's existing error path, which
			// already does exactly that: logs a warning, leaves
			// Stale/Running untouched, and keeps the staleness clock (and
			// its 24h alert) running instead of silently clearing it.
			return next, false, fmt.Errorf("reconciliation: no integer_range chunks recorded for pipeline %s (slot %s); nothing reconcilable, cannot confirm sweep coverage", pipelineID, slotName)
		}
		if next.NextChunkOrdinal >= len(chunks) {
			return next, true, nil
		}

		chunk := chunks[next.NextChunkOrdinal]
		deleted, err := reconcileOneChunk(ctx, srcDB, sinkDB, chunk)
		if err != nil {
			return prev, false, fmt.Errorf("reconciliation: chunk %d/%d (%s.%s [%d,%d]): %w",
				next.NextChunkOrdinal, len(chunks), chunk.TableSchema, chunk.TableName, chunk.RangeStart, chunk.RangeEnd, err)
		}

		next.NextChunkOrdinal++
		next.RowsReconciled += deleted
		log.Info().Str("pipeline_id", pipelineID).
			Str("table", chunk.TableSchema+"."+chunk.TableName).
			Int64("range_start", chunk.RangeStart).Int64("range_end", chunk.RangeEnd).
			Int64("rows_deleted", deleted).
			Int("chunk_ordinal", next.NextChunkOrdinal).Int("chunks_total", next.ChunksTotal).
			Msg("reconciliation: chunk complete")

		return next, next.NextChunkOrdinal >= len(chunks), nil
	}
}

// queryIntegerRangeChunks reads the ordered list of integer_range chunks
// recorded for slotName, across every table, sorted (table_schema,
// table_name, chunk_index) -- the same ordering
// ReconciliationProgress.NextChunkOrdinal's doc comment assumes is stable
// across re-reads. Only integer_range rows are returned: mirrors
// internal/api/partition_strategy_probe.go's allowlist reasoning (OQ-7)
// applied to the sweep's own input instead of StartPipeline's guard --
// this is the second of the two places that decision needs enforcing.
func queryIntegerRangeChunks(ctx context.Context, db *sql.DB, schema, slotName string) ([]reconcileChunk, error) {
	if db == nil || slotName == "" {
		return nil, fmt.Errorf("no source connection or slot name")
	}
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	//nolint:gosec // G202: schema is QuoteIdentifier-quoted and sourced from pipeline config, never request input; slot_name is a bound parameter
	query := `SELECT table_schema, table_name, range_start, range_end
		FROM ` + libpq.QuoteIdentifier(schema) + `.cdc_snapshot_chunks
		WHERE slot_name = $1 AND partition_strategy = 'integer_range'
			AND range_start IS NOT NULL AND range_end IS NOT NULL
		ORDER BY table_schema, table_name, chunk_index`
	rows, err := db.QueryContext(qctx, query, slotName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var chunks []reconcileChunk
	for rows.Next() {
		var c reconcileChunk
		if err := rows.Scan(&c.TableSchema, &c.TableName, &c.RangeStart, &c.RangeEnd); err != nil {
			return nil, err
		}
		chunks = append(chunks, c)
	}
	return chunks, rows.Err()
}

// getSourceIntegerPrimaryKey resolves the single-column integer primary key
// for schema.table on the source, via information_schema -- a plain,
// self-contained query rather than importing the vendored connector's
// unexported equivalent (internal/vendor/go-pq-cdc/pq/snapshot/coordinator.go's
// getSingleIntegerPrimaryKey), which is scoped to a *Snapshotter and not
// reusable from here. ok=false for a composite or non-integer PK -- the
// same condition integer_range chunking itself requires upstream, so this
// should already agree with what the chunk rows imply; treated as
// degraded (skip, don't guess) rather than an error if it ever disagrees.
func getSourceIntegerPrimaryKey(ctx context.Context, db *sql.DB, schema, table string) (string, bool, error) {
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	const query = `
		SELECT kcu.column_name, c.data_type
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		JOIN information_schema.columns c
			ON c.table_schema = kcu.table_schema AND c.table_name = kcu.table_name AND c.column_name = kcu.column_name
		WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = $1 AND tc.table_name = $2`
	rows, err := db.QueryContext(qctx, query, schema, table)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = rows.Close() }()

	var col, dataType string
	count := 0
	for rows.Next() {
		if err := rows.Scan(&col, &dataType); err != nil {
			return "", false, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return "", false, err
	}
	if count != 1 {
		return "", false, nil
	}
	switch dataType {
	case "integer", "bigint", "smallint":
		return col, true, nil
	default:
		return "", false, nil
	}
}

// reconcileOneChunk compares the PK set in [chunk.RangeStart,
// chunk.RangeEnd] between source and sink and soft-deletes (deleted_at =
// now()) whatever the sink still has that the source does not. Returns the
// number of rows soft-deleted.
func reconcileOneChunk(ctx context.Context, srcDB, sinkDB *sql.DB, chunk reconcileChunk) (int64, error) {
	pkCol, ok, err := getSourceIntegerPrimaryKey(ctx, srcDB, chunk.TableSchema, chunk.TableName)
	if err != nil {
		return 0, fmt.Errorf("resolve source PK: %w", err)
	}
	if !ok {
		// Chunk metadata says integer_range but the source PK no longer
		// agrees (schema drift since the snapshot ran): this chunk cannot
		// be safely compared at all. Returning an error (rather than the
		// previous (0, nil) "silently skip and advance") routes this tick
		// through maybeSweepReconciliation's error path -- log + retry,
		// NextChunkOrdinal does NOT advance -- so a chunk this stepper can
		// no longer verify can never contribute to a complete=true sweep
		// (plan 4.4 invariant 5, OQ-7's "degrade explicitly", not silently
		// treated as done). An operator-triggered re-snapshot re-derives
		// chunking and clears the condition.
		return 0, fmt.Errorf("reconciliation: table %s.%s recorded as integer_range but source no longer has a single integer PK; cannot verify this chunk", chunk.TableSchema, chunk.TableName)
	}

	// Read the SINK key set first, then the SOURCE key set. This ordering
	// matters: sink-minus-source computed from sink@T1 and source@T2>T1
	// has no false-positive window, because anything present in the sink
	// at T1 must have existed at the source at some point before T1 (the
	// sink only ever gets a row via a CDC event or snapshot derived from
	// the source). The reverse order (source read first, sink read
	// second, as this used to do) has a window: a row INSERTed at the
	// source between the two reads, and already replicated to the sink by
	// the time the sink read runs, looks exactly like "sink has it, source
	// doesn't" and gets wrongly soft-deleted. Reconciliation deliberately
	// runs concurrently with live replication (plan 4.2: it never gates
	// Running), so that window is the normal case here, not an edge case.
	sinkPK := pkCol
	if cols, found, err := loadSinkPKColumns(ctx, sinkDB, chunk.TableSchema, chunk.TableName); err == nil && found && len(cols) == 1 {
		sinkPK = cols[0]
	}
	// deleted_at IS NULL: rows the sink has already soft-deleted (by a
	// prior sweep, or by a live tombstone from the CDC stream itself) are
	// not "still there" from this sweep's point of view -- comparing
	// against them again would either no-op harmlessly or, worse, race a
	// concurrent live delete's own deleted_at write. Either way they add
	// nothing this sweep needs to redo.
	sinkKeys, err := queryIntegerKeySet(ctx, sinkDB, chunk.TableSchema, chunk.TableName, sinkPK, chunk.RangeStart, chunk.RangeEnd, "deleted_at IS NULL")
	if err != nil {
		return 0, fmt.Errorf("read sink keys: %w", err)
	}
	if len(sinkKeys) == 0 {
		return 0, nil
	}

	srcKeys, err := queryIntegerKeySet(ctx, srcDB, chunk.TableSchema, chunk.TableName, pkCol, chunk.RangeStart, chunk.RangeEnd, "")
	if err != nil {
		return 0, fmt.Errorf("read source keys: %w", err)
	}

	var candidate []int64
	for k := range sinkKeys {
		if !srcKeys[k] {
			candidate = append(candidate, k)
		}
	}
	if len(candidate) == 0 {
		return 0, nil
	}

	// Confirm the candidate deletions against a source read that strictly
	// post-dates the sink read, targeted at exactly the candidate keys.
	// This closes the residual window between "srcKeys was read" and "we
	// decide to delete": a row that arrived at the source in that gap
	// would otherwise still be wrongly flagged.
	stillMissing, err := confirmKeysMissingFromSource(ctx, srcDB, chunk.TableSchema, chunk.TableName, pkCol, candidate)
	if err != nil {
		return 0, fmt.Errorf("confirm candidate deletions against source: %w", err)
	}
	if len(stillMissing) == 0 {
		return 0, nil
	}
	return int64(len(stillMissing)), softDeleteSinkKeys(ctx, sinkDB, chunk.TableSchema, chunk.TableName, sinkPK, stillMissing)
}

// confirmKeysMissingFromSource re-checks exactly the candidate keys against
// the source, immediately before a soft-delete is issued, and returns the
// subset still absent. This is the final read in reconcileOneChunk's
// sink-then-source-then-confirm ordering (see reconcileOneChunk's doc
// comment): it strictly post-dates the sink read, so any row that arrived
// at the source after the earlier full-range source read but before the
// delete is caught here rather than wrongly soft-deleted.
func confirmKeysMissingFromSource(ctx context.Context, srcDB *sql.DB, schema, table, pkCol string, candidate []int64) ([]int64, error) {
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	placeholders := make([]string, len(candidate))
	for i, k := range candidate {
		placeholders[i] = strconv.FormatInt(k, 10)
	}
	//nolint:gosec // G202: identifiers are quoteIdent-quoted and sourced from cdc_snapshot_chunks/information_schema; placeholders are strconv-formatted int64 keys, never request input
	query := "SELECT " + quoteIdent(pkCol) + " FROM " + quoteIdent(schema) + "." + quoteIdent(table) +
		" WHERE " + quoteIdent(pkCol) + " IN (" + strings.Join(placeholders, ",") + ")"
	rows, err := srcDB.QueryContext(qctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	present := make(map[int64]bool, len(candidate))
	for rows.Next() {
		var k int64
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		present[k] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	missing := make([]int64, 0, len(candidate))
	for _, k := range candidate {
		if !present[k] {
			missing = append(missing, k)
		}
	}
	return missing, nil
}

// queryIntegerKeySet reads the set of integer pkCol values in [start, end]
// for schema.table, optionally filtered by an extra WHERE clause fragment
// (used to exclude already-soft-deleted sink rows). Loads the whole range
// into memory, which is bounded by the snapshotter's own chunk size
// (plan section 9/WS-7 shape: chunking exists precisely so this per-chunk
// working set stays small, unlike the ~800 MB single-anti-join OQ-1 rules
// out).
func queryIntegerKeySet(ctx context.Context, db *sql.DB, schema, table, pkCol string, start, end int64, extraWhere string) (map[int64]bool, error) {
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	where := quoteIdent(pkCol) + " BETWEEN " + strconv.FormatInt(start, 10) + " AND " + strconv.FormatInt(end, 10)
	if extraWhere != "" {
		where += " AND " + extraWhere
	}
	//nolint:gosec // G202: identifiers are quoteIdent-quoted and sourced from cdc_snapshot_chunks/information_schema; range bounds are strconv-formatted int64, never request input
	query := "SELECT " + quoteIdent(pkCol) + " FROM " + quoteIdent(schema) + "." + quoteIdent(table) + " WHERE " + where

	rows, err := db.QueryContext(qctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	keys := make(map[int64]bool)
	for rows.Next() {
		var k int64
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys[k] = true
	}
	return keys, rows.Err()
}

// softDeleteSinkKeys marks rows absent from the source as deleted the same
// way the live sink write path does (deleteTableBatch,
// internal/sink/databend/sink.go): UPDATE ... SET deleted_at, never a hard
// DELETE. Batches all keys for this chunk into a single statement --
// bounded by chunk size, so this never approaches the >800MB single
// anti-join OQ-1 rules out.
func softDeleteSinkKeys(ctx context.Context, sinkDB *sql.DB, schema, table, pkCol string, keys []int64) error {
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	placeholders := make([]string, len(keys))
	for i, k := range keys {
		placeholders[i] = strconv.FormatInt(k, 10)
	}
	//nolint:gosec // G202: identifiers are quoteIdent-quoted and sourced from cdc_snapshot_chunks/information_schema; placeholders are strconv-formatted int64 keys, never request input
	query := "UPDATE " + quoteIdent(schema) + "." + quoteIdent(table) +
		" SET deleted_at = now() WHERE " + quoteIdent(pkCol) + " IN (" + strings.Join(placeholders, ",") + ")"
	_, err := sinkDB.ExecContext(qctx, query)
	return err
}

// pkMetaDatabase/pkMetaTable mirror internal/sink/databend/sink.go's
// unexported constants of the same values -- WS-4.6's durable
// cdc_meta.pk_columns table, read here directly by SQL rather than
// instantiating a full DatabendSink for a single lookup.
const (
	reconcilePKMetaDatabase = "cdc_meta"
	reconcilePKMetaTable    = "pk_columns"
)

// loadSinkPKColumns reads a table's primary key columns back from
// cdc_meta.pk_columns (see internal/sink/databend/sink.go's
// persistPKMetadata/loadPKMetadata). found=false (not an error) when no
// row exists yet, e.g. a table synced before WS-4.6 landed -- callers fall
// back to the source's PK column name, which is correct for every
// integer_range table in the prioritised set (OQ-7).
func loadSinkPKColumns(ctx context.Context, sinkDB *sql.DB, schema, table string) ([]string, bool, error) {
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ref := schema + "." + table
	query := "SELECT pk_columns FROM " + quoteIdent(reconcilePKMetaDatabase) + "." + quoteIdent(reconcilePKMetaTable) + " WHERE table_ref = ?"
	var joined string
	err := sinkDB.QueryRowContext(qctx, query, ref).Scan(&joined)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		// cdc_meta.pk_columns may not exist yet for a sink that has never
		// applied a schema (e.g. reconciliation running before any data
		// landed): treat as "not found" rather than surfacing a query
		// error that would abort the whole chunk step.
		return nil, false, nil
	}
	if joined == "" {
		return nil, false, nil
	}
	return strings.Split(joined, ","), true, nil
}

// quoteIdent double-quotes a SQL identifier. Shared by the source
// (PostgreSQL) and sink (Databend) queries in this file -- both accept
// standard double-quoted identifiers. schema/table/column names here are
// always sourced from cdc_snapshot_chunks or information_schema, never
// operator/request input, but this keeps the query construction consistent
// with the rest of the codebase's "always quote identifiers" convention
// (internal/sink/AGAENT.md) rather than relying on that invariant silently.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// openSinkDBForReconciliation reads a sink config from KV and opens a
// short-lived Databend connection, mirroring
// openSourceDBForHealthCheck's shape on the sink side. Only "databend" is
// supported today (integration/reconciliation with postgres_debug, the
// other registered sink type, is out of scope for WS-7 -- that sink exists
// for local dev/testing, not a production warehouse that needs delete
// reconciliation).
func openSinkDBForReconciliation(kv nats.KeyValue, sinkID string) (*sql.DB, error) {
	entry, err := kv.Get(protocol.SinkConfigKey(sinkID))
	if err != nil {
		return nil, fmt.Errorf("sink not found: %w", err)
	}
	var cfg protocol.SinkConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		return nil, err
	}
	if cfg.Type != "databend" {
		return nil, fmt.Errorf("reconciliation: sink type %q is not supported (only databend)", cfg.Type)
	}
	if err := cfg.Decrypt(); err != nil {
		return nil, err
	}

	db, err := sql.Open("databend", cfg.DSN)
	if err != nil {
		return nil, err
	}
	return db, nil
}
