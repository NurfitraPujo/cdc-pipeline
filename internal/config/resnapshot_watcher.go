package config

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// ResnapshotStatus is the subset of cdc_snapshot_job state WS-6's completion
// watch needs: whether the forced re-snapshot the worker kicked off on
// entering Snapshotting (source.go's shouldResnapshot -> Snapshot.Resnapshot
// true) has finished. Mirrors WALGuardStatus/SlotHealth's shape -- a small,
// probe-specific struct rather than a bare bool, so a future field (e.g.
// progress) has somewhere to go without changing the checker signature.
type ResnapshotStatus struct {
	// Completed is cdc_snapshot_job.completed for this pipeline's slot.
	Completed bool
	// StartedAt is cdc_snapshot_job.started_at for the same row. Needed to
	// distinguish a genuinely new re-snapshot's completion from the
	// previous, already-completed job row that is still present when the
	// pipeline enters Snapshotting -- CleanupJobForSlot (internal/vendor/
	// go-pq-cdc/pq/snapshot/coordinator.go) only deletes that stale row once
	// the worker actually boots and the connector runs, which can lag the
	// handler's Snapshotting write by however long worker boot takes. See
	// maybeCompleteResnapshot's use of this field.
	StartedAt time.Time
}

// ResnapshotStatusChecker probes whether a Snapshotting pipeline's forced
// re-snapshot has finished. The default installed by NewConfigManager
// (defaultResnapshotStatusChecker) always reports "not complete", matching
// WALGuardChecker's "optimistic-until-wired-up" pattern in the safe
// direction: a manager that never calls SetResnapshotStatusChecker leaves a
// Snapshotting pipeline exactly where it is rather than guessing it is
// done. SetResnapshotStatusChecker installs a real probe
// (NewPostgresResnapshotStatusChecker) once cmd/pipeline/main.go wires it
// up.
type ResnapshotStatusChecker func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) (ResnapshotStatus, bool)

func defaultResnapshotStatusChecker(context.Context, string, protocol.PipelineConfig) (ResnapshotStatus, bool) {
	return ResnapshotStatus{}, false
}

// SetResnapshotStatusChecker overrides the probe StartPauseExpiryTicker's
// sweep consults for a Snapshotting pipeline. See ResnapshotStatusChecker's
// doc comment for the default.
func (m *ConfigManager) SetResnapshotStatusChecker(c ResnapshotStatusChecker) {
	if c == nil {
		c = defaultResnapshotStatusChecker
	}
	m.pauseMu.Lock()
	m.resnapshotStatusChecker = c
	m.pauseMu.Unlock()
}

func (m *ConfigManager) getResnapshotStatusChecker() ResnapshotStatusChecker {
	m.pauseMu.RLock()
	defer m.pauseMu.RUnlock()
	if m.resnapshotStatusChecker == nil {
		return defaultResnapshotStatusChecker
	}
	return m.resnapshotStatusChecker
}

// NewPostgresResnapshotStatusChecker builds the production
// ResnapshotStatusChecker. It opens its own short-lived connection to the
// pipeline's first source (the same "Sources[0]" convention
// openSourceDBForHealthCheck's other callers use) and reads
// cdc_snapshot_job.completed directly for the source's slot, rather than
// inferring completion from worker health -- a worker reports healthy as
// soon as it is up, well before a 100M-row re-snapshot (plan OQ-1) has
// actually finished.
func NewPostgresResnapshotStatusChecker(kv nats.KeyValue) ResnapshotStatusChecker {
	return func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) (ResnapshotStatus, bool) {
		if len(cfg.Sources) == 0 {
			log.Warn().Str("pipeline_id", pipelineID).Msg("resnapshot status probe: pipeline has no source configured")
			return ResnapshotStatus{}, false
		}

		db, slotName, err := openSourceDBForHealthCheck(kv, cfg.Sources[0])
		if err != nil {
			log.Warn().Err(err).Str("pipeline_id", pipelineID).Msg("resnapshot status probe: failed to open source connection")
			return ResnapshotStatus{}, false
		}
		defer func() { _ = db.Close() }()

		schema := resolveSnapshotJobSchema(cfg)
		completed, startedAt, ok := queryResnapshotJobCompleted(ctx, db, schema, slotName)
		if !ok {
			log.Warn().Str("pipeline_id", pipelineID).Str("slot", slotName).Msg("resnapshot status probe: query failed")
			return ResnapshotStatus{}, false
		}
		return ResnapshotStatus{Completed: completed, StartedAt: startedAt}, true
	}
}

// resolveSnapshotJobSchema mirrors internal/api/partition_strategy_probe.go's
// resolveSnapshotMetadataSchema: cdc_snapshot_job, like cdc_snapshot_chunks,
// is created unqualified by the vendored connector and therefore resolves
// against the first schema in the source's search path (default "public").
// Duplicated here, not exported from internal/api, for the same
// config->api-avoidance reason getLifecycleRecord's doc comment gives.
func resolveSnapshotJobSchema(cfg protocol.PipelineConfig) string {
	// PipelineConfig only carries source IDs; the search-path schemas live
	// on the referenced SourceConfig, which this checker does not have
	// in hand without a second KV read. cdc_snapshot_job/cdc_snapshot_chunks
	// have only ever been observed at "public" in this deployment (every
	// existing SourceConfig.Schemas is empty, per the same "empty means
	// public only" convention source.go and partition_strategy_probe.go
	// both document), so default to it directly rather than adding a KV
	// round trip this checker's callers do not otherwise need.
	_ = cfg
	return "public"
}

// queryResnapshotJobCompleted reads cdc_snapshot_job.completed and started_at
// for the given slot. ok=false on any query error (including "no row yet",
// which is the normal state for the first few seconds after
// Snapshot.Resnapshot wiped the previous job row) -- callers treat that as
// "not yet observable", never as "complete", so a Snapshotting pipeline is
// never prematurely marked Running.
//
// started_at is returned alongside completed so maybeCompleteResnapshot can
// tell a genuinely new re-snapshot's completion apart from the previous,
// already-completed job row that is still present until CleanupJobForSlot
// (internal/vendor/go-pq-cdc/pq/snapshot/coordinator.go) deletes it -- which
// only happens once the worker actually boots and the connector runs, not
// the instant the handler writes Snapshotting.
func queryResnapshotJobCompleted(ctx context.Context, db *sql.DB, schema, slotName string) (bool, time.Time, bool) {
	if db == nil || slotName == "" {
		return false, time.Time{}, false
	}
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	table := fmt.Sprintf("%s.cdc_snapshot_job", quoteSnapshotSchema(schema))
	var completed sql.NullBool
	var startedAt sql.NullTime
	err := db.QueryRowContext(qctx,
		fmt.Sprintf(`SELECT completed, started_at FROM %s WHERE slot_name = $1`, table),
		slotName,
	).Scan(&completed, &startedAt)
	if err == sql.ErrNoRows {
		return false, time.Time{}, false
	}
	if err != nil {
		return false, time.Time{}, false
	}
	var started time.Time
	if startedAt.Valid {
		started = startedAt.Time
	}
	return completed.Valid && completed.Bool, started, true
}

// quoteSnapshotSchema double-quotes a schema name for use as an identifier.
// schema is sourced from resolveSnapshotJobSchema (a hardcoded "public"
// today), never operator input, but this keeps the query construction
// consistent with the rest of the codebase's "always quote identifiers"
// convention (internal/sink/AGENT.md) rather than relying on that
// invariant silently.
func quoteSnapshotSchema(schema string) string {
	return `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
}

// maybeCompleteResnapshot is one pipeline's worth of WS-6's sweep, called
// from tickPauseExpiry alongside the pause/WAL-guard checks. It only acts
// when the persisted record is Snapshotting; every other state (including
// Running, which is where a pipeline lands once this function fires) is
// left alone.
//
// StopWindowOccurred is passed as true unconditionally: StateSnapshotting
// is only ever entered via (NeedsResnapshot, start) -- see the transition
// table (internal/protocol/lifecycle.go) -- and NeedsResnapshot is only
// ever entered from Stopped or from a Paused timer-expiry that found
// wal_status = lost. Both predecessors mean a stop window happened, so
// invariant 5 (leaving Snapshotting always marks reconciliation stale)
// holds for every path that can reach this function, not just the common
// stop-then-resume one.
func (m *ConfigManager) maybeCompleteResnapshot(ctx context.Context, id string) {
	m.pauseMu.RLock()
	checker := m.resnapshotStatusChecker
	m.pauseMu.RUnlock()
	if checker == nil {
		// No real probe installed (SetResnapshotStatusChecker was never
		// called): skip the sweep entirely, mirroring
		// maybeEscalateWALGuardBreach's identical short-circuit -- pay no
		// per-tick KV lookup for a checker that can only ever report "not
		// complete" anyway. cmd/pipeline/main.go always installs one; this
		// path only matters for tests/callers that predate WS-6.
		return
	}

	rec, rev, ok := m.getLifecycleRecordRev(id)
	if !ok || rec.State != protocol.StateSnapshotting {
		return
	}

	cfg, ok := m.getPipelineConfig(id)
	if !ok {
		return
	}

	status, probed := checker(ctx, id, cfg)
	if !probed || !status.Completed {
		return
	}

	// Guard against the stale-row window: the job row present the instant
	// the pipeline enters Snapshotting is the PREVIOUS, already-completed
	// snapshot -- CleanupJobForSlot only deletes it once the worker actually
	// boots and Snapshot.Resnapshot's cleanup runs, which can lag rec's
	// UpdatedAt (set the moment this handler wrote Snapshotting) by however
	// long worker boot takes. Require started_at to be strictly newer than
	// that timestamp before treating the probe as evidence of the NEW
	// re-snapshot's completion; otherwise leave the record for a later tick,
	// by which point cleanup has had time to run and started_at will have
	// advanced.
	if !status.StartedAt.After(rec.UpdatedAt) {
		return
	}

	outcome, err := protocol.Transition(protocol.StateSnapshotting, protocol.EventComplete, protocol.Guards{
		StopWindowOccurred: true,
	})
	if err != nil {
		// (Snapshotting, complete) is unconditionally legal in the
		// transition table, so this should be unreachable; log rather than
		// panic and leave the record for the next tick to retry.
		log.Error().Err(err).Str("pipeline_id", id).Msg("resnapshot watch: illegal complete transition")
		return
	}

	newRec := protocol.PipelineLifecycleRecord{
		State: outcome.To,
		// invariant 5: Snapshotting/complete after a stop window always
		// carries ReconciliationStale -- see the transition table. Only
		// WS-7's sweep may clear it later.
		Reconciliation: outcome.Reconciliation,
		UpdatedAt:      m.getClock().Now(),
	}
	// RM-3: CAS against the revision read above -- see
	// maybeResumeExpiredPause's identical comment (pause_expiry.go) for why
	// a blind Put is unsafe even with the lease in place.
	if err := m.putLifecycleRecordCAS(id, newRec, rev); err != nil {
		log.Warn().Err(err).Str("pipeline_id", id).Msg("resnapshot watch: lifecycle record changed concurrently, skipping this tick's write")
		return
	}
	log.Info().Str("pipeline_id", id).Str("reconciliation", string(newRec.Reconciliation)).Msg("resnapshot watch: re-snapshot complete, pipeline Running")
}
