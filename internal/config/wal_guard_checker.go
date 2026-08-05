package config

import (
	"context"
	"database/sql"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	// stdlib registers the "pgx" database/sql driver used to open source Postgres connections.
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// NewPostgresWALGuardChecker builds the production WALGuardChecker WS-4
// leaves as a seam. It opens the same short-lived, per-tick connection
// NewPostgresSlotHealthChecker (slot_health_checker.go) does -- reusing
// openSourceDBForHealthCheck rather than adding a second connection helper
// -- and reads safe_wal_size and wal_status off pg_replication_slots in one
// query, the same table querySlotLagBytes (internal/source/postgres/
// source.go) already probes for cdc_source_slot_lag_bytes. cmd/pipeline/
// main.go installs this so the pause-expiry ticker's WAL-guard sweep
// (maybeEscalateWALGuardBreach) actually consults live slot state instead
// of the optimistic default (an empty WALGuardStatus, which
// EvaluateWALGuardBreach always treats as "no breach").
//
// A probe failure (source not found, connection refused, query error, no
// matching slot row) returns a zero WALGuardStatus, which
// EvaluateWALGuardBreach also treats as "no breach" -- the same direction
// SlotHealthChecker's own probe failure fails in the opposite sense
// ("do not resume"). Both choices are deliberately the side that leaves the
// pipeline exactly where it is for the next tick to retry, never a snap
// judgement made on missing data.
func NewPostgresWALGuardChecker(kv nats.KeyValue) WALGuardChecker {
	return func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) WALGuardStatus {
		if len(cfg.Sources) == 0 {
			log.Warn().Str("pipeline_id", pipelineID).Msg("wal guard probe: pipeline has no source configured")
			return WALGuardStatus{}
		}

		db, slotName, err := openSourceDBForHealthCheck(kv, cfg.Sources[0])
		if err != nil {
			log.Warn().Err(err).Str("pipeline_id", pipelineID).Msg("wal guard probe: failed to open source connection")
			return WALGuardStatus{}
		}
		defer func() { _ = db.Close() }()

		status, ok := queryWALGuardStatus(ctx, db, slotName)
		if !ok {
			log.Warn().Str("pipeline_id", pipelineID).Str("slot", slotName).Msg("wal guard probe: query failed")
			return WALGuardStatus{}
		}
		return status
	}
}

// queryWALGuardStatus reads pg_replication_slots.safe_wal_size and
// wal_status for the given slot in a single round trip (plan section 7),
// plus the same pg_wal_lsn_diff lag querySlotLagBytes computes, so
// EvaluateWALGuardBreach's fallback path has a value ready without a
// second query. safe_wal_size is read into a sql.NullInt64: NULL exactly
// when max_slot_wal_keep_size is unset (defaults to -1, unlimited) -- the
// trap plan section 7 calls out. ok=false on any error (nil db, missing
// slot, query/scan error), mirroring querySlotLagBytes's own
// "skip this tick" contract -- callers must never treat a failed probe as
// evidence of anything.
func queryWALGuardStatus(ctx context.Context, db *sql.DB, slotName string) (WALGuardStatus, bool) {
	if db == nil || slotName == "" {
		return WALGuardStatus{}, false
	}

	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var safeWALSize sql.NullInt64
	var walStatus sql.NullString
	var lagBytes sql.NullInt64
	err := db.QueryRowContext(qctx,
		`SELECT safe_wal_size, wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
		 FROM pg_replication_slots WHERE slot_name = $1`,
		slotName,
	).Scan(&safeWALSize, &walStatus, &lagBytes)
	if err != nil {
		log.Debug().Err(err).Str("slot", slotName).Msg("queryWALGuardStatus: failed to read slot WAL state (slot may not exist yet)")
		return WALGuardStatus{}, false
	}

	status := WALGuardStatus{
		WALStatus: walStatus.String,
		LagBytes:  lagBytes.Int64,
		LagOK:     lagBytes.Valid,
	}
	if safeWALSize.Valid {
		v := safeWALSize.Int64
		status.SafeWALSizeBytes = &v
	}
	return status, true
}
