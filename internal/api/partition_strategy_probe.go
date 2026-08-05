package api

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	libpq "github.com/lib/pq"
)

// integerRangeStrategy is the sole cdc_snapshot_chunks.partition_strategy
// value a WS-5 resume CAN guarantee coverage for (plan section 10: "WS-5
// therefore resumes only chunks recorded as integer_range"). Every other
// value -- ctid_block, offset (both key off physical row position, which
// drifts under concurrent UPDATE/DELETE/VACUUM even while the slot stays
// alive through a pause), an empty string, or an unrecognised/future
// strategy name -- is treated as degraded. This is deliberately an
// allowlist, not a denylist of the known-bad strategies: OQ-7 exists to
// DETECT an unsafe resume, not to assume safety for anything the code
// happens not to recognise yet. Mirrors
// internal/vendor/go-pq-cdc/pq/snapshot/job.go's PartitionStrategyIntegerRange
// constant as a plain string rather than importing the vendored package,
// since this query only ever compares against the column's on-disk text
// value.
const integerRangeStrategy = "integer_range"

// isIntegerRangeStrategy is the pure allowlist check queryNonIntegerRangeTables
// applies per row: only a non-NULL column reading exactly "integer_range" is
// safe. Split out from the scan loop so it is trivially unit-testable
// without a database -- the NULL and unknown-strategy cases are exactly
// what OQ-7 exists to catch, so they need their own coverage.
func isIntegerRangeStrategy(strategy sql.NullString) bool {
	return strategy.Valid && strategy.String == integerRangeStrategy
}

// NewPartitionStrategyChecker builds the production PartitionStrategyChecker
// StartPipeline consults when resuming from Paused (plan section 10,
// OQ-5/OQ-7). It opens its own short-lived connection to the pipeline's
// first source -- the same "Sources[0]" convention NewSlotLagRateSampler
// uses (wal_probe.go) -- and reads cdc_snapshot_chunks directly, rather
// than trusting the operator's "every prioritised table has a single
// integer PK" claim.
func (h *Handler) NewPartitionStrategyChecker() PartitionStrategyChecker {
	return func(ctx context.Context, _ string, cfg protocol.PipelineConfig) (bool, []string, bool) {
		if len(cfg.Sources) == 0 {
			return false, nil, false
		}
		db, srcCfg, err := h.openSourceDBByID(cfg.Sources[0])
		if err != nil {
			return false, nil, false
		}
		defer func() { _ = db.Close() }()

		if srcCfg.SlotName == "" {
			return false, nil, false
		}

		schema := resolveSnapshotMetadataSchema(srcCfg.Schemas)
		return queryNonIntegerRangeTables(ctx, db, schema, srcCfg.SlotName)
	}
}

// resolveSnapshotMetadataSchema mirrors the vendored connector's own
// unqualified-name resolution rule for cdc_snapshot_chunks
// (internal/vendor/go-pq-cdc/pq/snapshot/snapshot.go:resolveMetadataSchema):
// the first schema in the source's configured search path, defaulting to
// "public" when none is set -- the same "empty means public only"
// convention srcConfig.Schemas uses everywhere else in this package.
func resolveSnapshotMetadataSchema(schemas []string) string {
	if len(schemas) == 0 {
		return "public"
	}
	first := strings.TrimSpace(schemas[0])
	if first == "" {
		return "public"
	}
	return first
}

// queryNonIntegerRangeTables reads the distinct (table_schema, table_name)
// pairs recorded against slotName in cdc_snapshot_chunks whose
// partition_strategy is anything other than exactly "integer_range" --
// including NULL (scanned via sql.NullString rather than a plain string, so
// a NULL column degrades this table instead of failing the whole probe) and
// any empty/unrecognised/future strategy name (see integerRangeStrategy's
// doc comment: this is an allowlist, so unknown means degraded, never
// safe). ok=false only on an error that prevents reading the table at all
// -- the query itself failing, or an unexpected non-NULL Scan error --
// including the table not existing yet, e.g. a pipeline that has never
// snapshotted -- mirroring every other probe in this package's "skip this
// check, never report a fake answer" contract; StartPipeline treats
// ok=false as "nothing to report" and resumes without the check, exactly
// as if no checker were installed.
func queryNonIntegerRangeTables(ctx context.Context, db *sql.DB, schema, slotName string) (bool, []string, bool) {
	if db == nil || slotName == "" {
		return false, nil, false
	}
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// schema is derived from the source's configured search path
	// (protocol.SourceConfig.Schemas), not user request input, but is
	// escaped through QuoteIdentifier for the same defence-in-depth reason
	// the vendored connector's tableExists does for its own metadataSchema
	// (internal/vendor/go-pq-cdc/pq/snapshot/coordinator.go:1218).
	//nolint:gosec // G202: schema is QuoteIdentifier-quoted and sourced from pipeline config, never request input; slot_name is a bound parameter
	query := `SELECT DISTINCT table_schema, table_name, partition_strategy
		FROM ` + libpq.QuoteIdentifier(schema) + `.cdc_snapshot_chunks
		WHERE slot_name = $1`
	rows, err := db.QueryContext(qctx, query, slotName)
	if err != nil {
		return false, nil, false
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	seen := make(map[string]bool)
	for rows.Next() {
		var tableSchema, tableName string
		var strategy sql.NullString
		if err := rows.Scan(&tableSchema, &tableName, &strategy); err != nil {
			return false, nil, false
		}
		if isIntegerRangeStrategy(strategy) {
			continue
		}
		qualified := tableSchema + "." + tableName
		if !seen[qualified] {
			seen[qualified] = true
			tables = append(tables, qualified)
		}
	}
	if err := rows.Err(); err != nil {
		return false, nil, false
	}

	return len(tables) > 0, tables, true
}
