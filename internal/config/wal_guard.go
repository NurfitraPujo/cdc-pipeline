package config

import (
	"context"
	"fmt"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/rs/zerolog/log"
)

// WALGuardStatus is the subset of replication-slot WAL state WS-4's guard
// needs to decide whether a Paused pipeline must be escalated to Stopping
// (plan section 7). It deliberately does not add a second signal: the two
// fields below are read by the same short-lived pg_replication_slots probe
// querySlotLagBytes (internal/source/postgres/source.go) already runs for
// cdc_source_slot_lag_bytes, plus the wal_status column
// probeSlotHealth (slot_health_checker.go) already decodes.
type WALGuardStatus struct {
	// SafeWALSizeBytes is pg_replication_slots.safe_wal_size. It is nil
	// whenever the column is NULL -- which plan section 7 notes is the
	// case whenever max_slot_wal_keep_size is unset (defaults to -1,
	// unlimited). The production source sets it to 30 GiB (OQ-2), so it
	// IS populated there; EvaluateWALGuardBreach only falls back to
	// LagBytes when this is nil.
	SafeWALSizeBytes *int64

	// WALStatus is pg_replication_slots.wal_status, following the ladder
	// reserved -> extended -> unreserved -> lost (plan section 7).
	// Empty means the probe could not read it (e.g. slot missing).
	WALStatus string

	// LagBytes is the byte gap between pg_current_wal_lsn() and the
	// slot's confirmed_flush_lsn -- the same quantity querySlotLagBytes
	// exports as cdc_source_slot_lag_bytes. Only consulted as a fallback
	// when SafeWALSizeBytes is nil.
	LagBytes int64
	// LagOK reports whether LagBytes was actually read this probe (mirrors
	// querySlotLagBytes's own ok return): false means "skip this signal",
	// never "lag is zero".
	LagOK bool
}

// WALGuardChecker probes a pipeline's replication slot for WS-4's WAL
// guard. The default installed by NewConfigManager (defaultWALGuardChecker)
// reports an empty WALGuardStatus, which EvaluateWALGuardBreach always
// treats as "no breach" -- the same optimistic-until-wired-up shape
// SlotHealthChecker uses. SetWALGuardChecker installs a real probe
// (NewPostgresWALGuardChecker) once cmd/pipeline/main.go wires it up.
type WALGuardChecker func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) WALGuardStatus

func defaultWALGuardChecker(context.Context, string, protocol.PipelineConfig) WALGuardStatus {
	return WALGuardStatus{}
}

// defaultWALGuardLagThresholdBytes is the fallback-probe threshold
// EvaluateWALGuardBreach compares LagBytes against when SafeWALSizeBytes is
// nil (plan section 7: "fall back to the existing lag probe measured
// against a configured threshold when it is NULL, so the guard still works
// on a source that has not set the GUC"). It mirrors
// protocol.WALBudgetBytes -- the same 30 GiB max_slot_wal_keep_size the
// production source is configured with (OQ-2) -- so a source that has not
// set the GUC is still guarded against the same budget the one that has
// set it is measured against. SetWALGuardLagThresholdBytes overrides it.
const defaultWALGuardLagThresholdBytes = protocol.WALBudgetBytes

// SetWALGuardChecker overrides the WAL-guard probe StartPauseExpiryTicker's
// sweep consults for a Paused pipeline. See WALGuardChecker's doc comment
// for the default.
func (m *ConfigManager) SetWALGuardChecker(c WALGuardChecker) {
	if c == nil {
		c = defaultWALGuardChecker
	}
	m.pauseMu.Lock()
	m.walGuardChecker = c
	m.pauseMu.Unlock()
}

// SetWALGuardLagThresholdBytes overrides the fallback lag threshold
// EvaluateWALGuardBreach uses when safe_wal_size is NULL. Pass <= 0 to
// restore the default (protocol.WALBudgetBytes).
func (m *ConfigManager) SetWALGuardLagThresholdBytes(thresholdBytes int64) {
	m.pauseMu.Lock()
	m.walGuardLagThresholdBytes = thresholdBytes
	m.pauseMu.Unlock()
}

func (m *ConfigManager) getWALGuardChecker() WALGuardChecker {
	m.pauseMu.RLock()
	defer m.pauseMu.RUnlock()
	if m.walGuardChecker == nil {
		return defaultWALGuardChecker
	}
	return m.walGuardChecker
}

func (m *ConfigManager) getWALGuardLagThresholdBytes() int64 {
	m.pauseMu.RLock()
	defer m.pauseMu.RUnlock()
	if m.walGuardLagThresholdBytes <= 0 {
		return defaultWALGuardLagThresholdBytes
	}
	return m.walGuardLagThresholdBytes
}

// EvaluateWALGuardBreach is the pure decision at the heart of WS-4 (plan
// section 7), deliberately free of any database access so it is trivially
// unit-testable: given the latest WALGuardStatus and the fallback lag
// threshold, decide whether the guard has tripped and, if so, why.
//
// Order of evaluation matters and mirrors the plan's own ordering:
//
//  1. wal_status is checked first and independently of the WAL-size signal.
//     Escalate at "unreserved", NOT "lost" -- past "lost" the WAL is
//     already gone and the choice has been made for us; acting one rung
//     early keeps the stop orderly and lets us record why (plan section 7).
//  2. safe_wal_size, when non-NULL, is the most direct answer to "how much
//     WAL can still be written before this slot dies" and is preferred
//     over the lag fallback.
//  3. The existing lag probe against thresholdBytes is consulted only when
//     safe_wal_size is NULL (source has not set max_slot_wal_keep_size).
//
// A status with neither signal available (wal_status empty, SafeWALSizeBytes
// nil, LagOK false) never breaches -- an unreadable probe must never be
// read as "guard tripped" any more than SlotHealthChecker's probe failure
// is read as "slot fine" in the opposite direction; both fail toward
// leaving the pipeline exactly where it is for the next tick to retry.
func EvaluateWALGuardBreach(status WALGuardStatus, thresholdBytes int64) (breach bool, reason string) {
	switch status.WALStatus {
	case "unreserved", "lost":
		return true, fmt.Sprintf("replication slot wal_status reached %q; escalating to Stopping before WAL is lost (plan section 7)", status.WALStatus)
	}

	if status.SafeWALSizeBytes != nil {
		if *status.SafeWALSizeBytes <= 0 {
			return true, fmt.Sprintf("replication slot safe_wal_size exhausted (%d bytes remaining)", *status.SafeWALSizeBytes)
		}
		return false, ""
	}

	// safe_wal_size is NULL (max_slot_wal_keep_size unset): fall back to
	// the existing lag probe against the configured threshold.
	if status.LagOK && thresholdBytes > 0 && status.LagBytes >= thresholdBytes {
		return true, fmt.Sprintf("replication slot lag %d bytes reached the fallback WAL threshold %d bytes (safe_wal_size unavailable)", status.LagBytes, thresholdBytes)
	}

	return false, ""
}

// maybeEscalateWALGuardBreach is one pipeline's worth of WS-4's sweep,
// called from tickPauseExpiry alongside maybeResumeExpiredPause. The WAL
// guard is an independent backstop (plan section 5: "Whichever of guard or
// timer trips first wins"), so it is checked unconditionally for every
// Paused pipeline every tick -- not only when paused_until has elapsed.
// Returns true when it escalated the pipeline this tick, so the caller can
// skip the (now moot) timer-expiry check for the same id.
func (m *ConfigManager) maybeEscalateWALGuardBreach(ctx context.Context, id string) bool {
	m.pauseMu.RLock()
	checker := m.walGuardChecker
	m.pauseMu.RUnlock()
	if checker == nil {
		// No real probe installed (SetWALGuardChecker was never called):
		// skip the sweep entirely rather than pay a config lookup for a
		// checker that can only ever report "no breach" anyway. Every
		// production entrypoint installs one (cmd/pipeline/main.go); this
		// path only matters for tests/callers that predate WS-4.
		return false
	}

	rec, rev, ok := m.getLifecycleRecordRev(id)
	if !ok || rec.State != protocol.StatePaused {
		return false
	}

	cfg, ok := m.getPipelineConfig(id)
	if !ok {
		return false
	}

	status := checker(ctx, id, cfg)
	breach, reason := EvaluateWALGuardBreach(status, m.getWALGuardLagThresholdBytes())
	if !breach {
		return false
	}

	outcome, err := protocol.Transition(protocol.StatePaused, protocol.EventWALGuardBreach, protocol.Guards{})
	if err != nil {
		// (Paused, wal_guard_breach) is unconditionally legal in the
		// transition table, so this should be unreachable; log rather
		// than panic and leave the record for the next tick to retry.
		log.Error().Err(err).Str("pipeline_id", id).Msg("wal guard: illegal breach transition")
		return false
	}

	newRec := protocol.PipelineLifecycleRecord{
		State: outcome.To,
		// invariant 3: paused_until only applies to Pausing/Paused; the
		// destination here is Stopping, so it is dropped by omission
		// rather than copied forward from rec.
		Reconciliation: rec.Reconciliation,
		Reason:         reason,
		UpdatedAt:      m.getClock().Now(),
	}
	// RM-3: CAS against the revision read above -- see
	// maybeResumeExpiredPause's identical comment (pause_expiry.go) for why
	// a blind Put is unsafe even with the lease in place.
	if err := m.putLifecycleRecordCAS(id, newRec, rev); err != nil {
		log.Warn().Err(err).Str("pipeline_id", id).Msg("wal guard: lifecycle record changed concurrently, skipping this tick's write")
		return false
	}

	// Flip desired_state to stopped so ConfigManager's own config-watch
	// (handlePipelineUpdates/reloadAllWorkers -> honourDesiredState ->
	// finalizeStop) actually drops the slot and drives the record on to
	// Stopped -- mirroring what maybeResumeExpiredPause does for its own
	// Resuming leg above. Without this write the record is stuck at
	// Stopping forever: honourDesiredState only calls finalizeStop when
	// cfg.EffectiveDesiredState() == stopped, and that only ever changes
	// on a config write.
	cfg.DesiredState = protocol.DesiredStateStopped
	if !m.putPipelineConfig(id, cfg) {
		log.Error().Str("pipeline_id", id).Msg("wal guard: failed to persist desired_state=stopped after breach; pipeline stuck at Stopping")
		return true
	}

	log.Warn().Str("pipeline_id", id).Str("reason", reason).Msg("wal guard: breach detected on a paused pipeline, escalating to Stopping")
	return true
}
