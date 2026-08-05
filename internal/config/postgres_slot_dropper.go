package config

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	// stdlib registers the "pgx" database/sql driver used to open source Postgres connections.
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// NewPostgresSlotDropper builds the production SlotDropper WS-5 leaves as a
// seam (see defaultSlotDropper's doc comment). It reuses
// openSourceDBForHealthCheck (slot_health_checker.go) for the same
// short-lived connection-string assembly the other probes in this package
// already use, rather than adding a third. cmd/pipeline/main.go installs
// this so finalizeStop (manager.go) actually releases WAL instead of
// pretending to.
func NewPostgresSlotDropper(kv nats.KeyValue) SlotDropper {
	return func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) error {
		if len(cfg.Sources) == 0 {
			return fmt.Errorf("slot dropper: pipeline %s has no source configured", pipelineID)
		}

		db, slotName, err := openSourceDBForHealthCheck(kv, cfg.Sources[0])
		if err != nil {
			return fmt.Errorf("slot dropper: failed to open source connection: %w", err)
		}
		defer func() { _ = db.Close() }()

		return dropReplicationSlot(ctx, db, slotName)
	}
}

// dropReplicationSlot drops the named replication slot, releasing the WAL
// it was retaining (plan section 1's "the point of stopping"). A slot that
// is still active (the worker's drain raced ahead of PostgreSQL noticing
// the connection closed) cannot be dropped directly -- pg_drop_replication_
// slot errors on an active slot -- so an active backend is terminated first
// via pg_terminate_backend. A slot that no longer exists at all (err ==
// sql.ErrNoRows, e.g. a retried finalizeStop after a prior call's drop
// succeeded but the KV write that would have recorded Stopped did not) is
// treated as success: the goal state -- no slot -- already holds.
func dropReplicationSlot(ctx context.Context, db *sql.DB, slotName string) error {
	if db == nil || slotName == "" {
		return fmt.Errorf("slot dropper: missing db connection or slot name")
	}

	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var active sql.NullBool
	var activePID sql.NullInt64
	err := db.QueryRowContext(qctx,
		`SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = $1`,
		slotName,
	).Scan(&active, &activePID)
	if err == sql.ErrNoRows {
		log.Info().Str("slot", slotName).Msg("slot dropper: slot already absent, treating as already dropped")
		return nil
	}
	if err != nil {
		return fmt.Errorf("slot dropper: failed to read pg_replication_slots for %q: %w", slotName, err)
	}

	if active.Valid && active.Bool && activePID.Valid {
		log.Warn().Str("slot", slotName).Int64("active_pid", activePID.Int64).Msg("slot dropper: slot still active after drain, terminating backend before drop")
		if _, err := db.ExecContext(qctx, `SELECT pg_terminate_backend($1)`, activePID.Int64); err != nil {
			log.Warn().Err(err).Str("slot", slotName).Msg("slot dropper: failed to terminate active backend; drop may still fail")
		}
	}

	if _, err := db.ExecContext(qctx, `SELECT pg_drop_replication_slot($1)`, slotName); err != nil {
		return fmt.Errorf("slot dropper: failed to drop replication slot %q: %w", slotName, err)
	}
	log.Info().Str("slot", slotName).Msg("slot dropper: replication slot dropped, WAL released")
	return nil
}
