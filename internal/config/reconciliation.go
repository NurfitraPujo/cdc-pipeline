package config

import (
	"context"
	"encoding/json"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

// reconciliationStaleSecondsGauge exports how long a pipeline has been
// owed a reconciliation sweep (plan section 4.4 invariant 5, section 11
// risk "best-effort deletes quietly become never-deletes"). It is the
// alert-able signal the plan asks for: a Prometheus alert rule (owned by
// daya-infra per the cluster-monitoring split, not this repo) can fire on
// this exceeding reconciliationStaleAlertThreshold for a sustained window,
// mirroring the CDCSourceSlotLagWarning/Critical pattern built on
// slotLagBytesGauge (internal/source/postgres/source.go). Reset to 0 the
// moment a sweep completes (maybeSweepReconciliation), so the gauge itself
// -- not just a log line -- is the source of truth for "how stale, right
// now".
var reconciliationStaleSecondsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cdc_reconciliation_stale_seconds",
	Help: "Seconds since this pipeline's delete reconciliation sweep was last known to be complete, while reconciliation is stale or running",
}, []string{"pipeline"})

// reconciliationChunksTotalGauge/reconciliationChunksDoneGauge export WS-7's
// chunk progress (plan WS-7 shape: "progress-reportable") independently of
// the API surface, so a dashboard can show sweep progress without polling
// GET /pipelines.
var reconciliationChunksTotalGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cdc_reconciliation_chunks_total",
	Help: "Total chunks in the current delete-reconciliation sweep, as of the most recent tick",
}, []string{"pipeline"})

var reconciliationChunksDoneGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cdc_reconciliation_chunks_done",
	Help: "Chunks completed by the current delete-reconciliation sweep, as of the most recent tick",
}, []string{"pipeline"})

// reconciliationStaleAlertThreshold is the "past a threshold" duration
// plan section 4.4 invariant 5 and section 11 ask for ("alert if it
// persists beyond a threshold"). 24h: long enough that a normal sweep of
// even the ~100M-row table (OQ-1) at one chunk per pause-expiry tick
// (~1/minute, see StartPauseExpiryTicker) comfortably finishes well inside
// it for any reasonably sized chunk count, short enough that an operator
// still finds out about a genuinely stuck sweep the same day.
const reconciliationStaleAlertThreshold = 24 * time.Hour

// ReconcileStepFunc advances one pipeline's delete-reconciliation sweep by
// exactly one chunk (plan WS-7 shape: "interruptible between chunks,
// resumable after restart, rate-limitable"). It is called at most once per
// pipeline per pause-expiry tick (~1/minute, StartPauseExpiryTicker),
// which is what makes the sweep both interruptible (ctx cancellation or a
// process restart between ticks loses at most one chunk's worth of work)
// and rate-limited (it can never do more than one chunk of source+sink
// I/O per tick, regardless of table size).
//
// prev is the progress persisted from the previous tick (zero value on a
// pipeline's first tick as stale/running). Implementations return the
// updated progress and whether the ENTIRE sweep (every chunk of every
// integer_range table) is now done -- not just this one chunk -- which is
// the only signal maybeSweepReconciliation accepts as license to clear
// Stale (invariant 5: "the only way out of stale is a completed sweep").
//
// The nil default (no ReconcileStepFunc installed) is deliberately NOT an
// always-succeeds no-op: maybeSweepReconciliation skips the sweep entirely
// when this is nil, leaving Stale exactly where it is, mirroring
// WALGuardChecker/ResnapshotStatusChecker's "unwired means untouched, never
// unwired means silently satisfied" contract. cmd/pipeline/main.go installs
// the real implementation via SetReconcileStepper.
type ReconcileStepFunc func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig, prev protocol.ReconciliationProgress) (protocol.ReconciliationProgress, bool, error)

// SetReconcileStepper overrides the delete-reconciliation stepper
// maybeSweepReconciliation consults. See ReconcileStepFunc's doc comment
// for the nil default.
func (m *ConfigManager) SetReconcileStepper(f ReconcileStepFunc) {
	m.pauseMu.Lock()
	m.reconcileStepper = f
	m.pauseMu.Unlock()
}

func (m *ConfigManager) getReconcileStepper() ReconcileStepFunc {
	m.pauseMu.RLock()
	defer m.pauseMu.RUnlock()
	return m.reconcileStepper
}

// maybeSweepReconciliation is one pipeline's worth of WS-7's sweep, called
// from tickPauseExpiry alongside the pause/WAL-guard/resnapshot checks. It
// only acts when the persisted lifecycle record's reconciliation
// sub-status is Stale or Running -- Idle (the zero value) means there is
// nothing owed, and every other lifecycle-record field is left untouched
// either way, since reconciliation is deliberately not a lifecycle state
// (plan section 4.2) and must never gate Running.
//nolint:gocyclo // reconciliation sweep decision walks several distinct staleness/state conditions in sequence; splitting hurts the readability of the ordered decision flow
func (m *ConfigManager) maybeSweepReconciliation(ctx context.Context, id string) {
	stepper := m.getReconcileStepper()
	if stepper == nil {
		// No real stepper installed: skip entirely, the same short-circuit
		// maybeEscalateWALGuardBreach/maybeCompleteResnapshot use for their
		// own unwired-checker case. Every production entrypoint
		// (cmd/pipeline/main.go) installs one; this path only matters for
		// tests/callers that predate WS-7.
		return
	}

	rec, rev, ok := m.getLifecycleRecordRev(id)
	if !ok {
		return
	}
	if rec.Reconciliation != protocol.ReconciliationStale && rec.Reconciliation != protocol.ReconciliationRunning {
		reconciliationStaleSecondsGauge.WithLabelValues(id).Set(0)
		return
	}

	cfg, ok := m.getPipelineConfig(id)
	if !ok {
		return
	}

	prev, hadProgress := m.getReconciliationProgress(id)
	now := m.getClock().Now()
	if !hadProgress || prev.StartedAt.IsZero() {
		// First tick this pipeline has been observed stale/running with no
		// prior attempt (or a prior attempt's progress record is gone,
		// e.g. after a completed sweep was cleared): start the staleness
		// clock now. Deliberately NOT reset on every tick -- see
		// ReconciliationProgress.StartedAt's doc comment -- so the alert
		// threshold measures wall-clock time actually owed, not time since
		// the last tick happened to run.
		prev = protocol.ReconciliationProgress{StartedAt: now}
	}

	next, complete, err := stepper(ctx, id, cfg, prev)
	if err != nil {
		log.Warn().Err(err).Str("pipeline_id", id).Msg("reconciliation sweep: chunk step failed, will retry next tick")
		// Leave StartedAt/progress as they were (prev), just refresh
		// UpdatedAt so operators can see the sweep is still alive and
		// retrying, not silently dead.
		prev.UpdatedAt = now
		if err := m.putReconciliationProgress(id, prev); err != nil {
			log.Error().Err(err).Str("pipeline_id", id).Msg("reconciliation sweep: failed to persist retry progress")
		}
		staleness := now.Sub(prev.StartedAt)
		reconciliationStaleSecondsGauge.WithLabelValues(id).Set(staleness.Seconds())
		if staleness > reconciliationStaleAlertThreshold {
			log.Warn().Str("pipeline_id", id).Dur("stale_for", staleness).
				Msg("reconciliation sweep: stale beyond alert threshold")
		}
		return
	}
	next.UpdatedAt = now
	if next.StartedAt.IsZero() {
		next.StartedAt = prev.StartedAt
	}

	reconciliationChunksTotalGauge.WithLabelValues(id).Set(float64(next.ChunksTotal))
	reconciliationChunksDoneGauge.WithLabelValues(id).Set(float64(next.NextChunkOrdinal))

	if !complete {
		if err := m.putReconciliationProgress(id, next); err != nil {
			log.Error().Err(err).Str("pipeline_id", id).Msg("reconciliation sweep: failed to persist progress")
			return
		}
		if rec.Reconciliation != protocol.ReconciliationRunning {
			newRec := rec
			newRec.Reconciliation = protocol.ReconciliationRunning
			newRec.UpdatedAt = now
			if err := m.putLifecycleRecordCAS(id, newRec, rev); err != nil {
				// CAS failure means something else (pause/resume/stop,
				// another tick) wrote the lifecycle record between our
				// Get and here -- see this function's doc comment. Do NOT
				// fall back to a blind Put: that would silently clobber
				// whatever the concurrent writer just did (plan 4.5,
				// invariant 6, the "choke point" rule Transition()
				// enforces for every other lifecycle write). Just skip
				// this tick's marker write; the next tick re-reads fresh.
				log.Warn().Err(err).Str("pipeline_id", id).Msg("reconciliation sweep: lifecycle record changed concurrently, skipping running-marker write this tick")
			}
		}
		staleness := now.Sub(next.StartedAt)
		reconciliationStaleSecondsGauge.WithLabelValues(id).Set(staleness.Seconds())
		if staleness > reconciliationStaleAlertThreshold {
			log.Warn().Str("pipeline_id", id).Dur("stale_for", staleness).
				Int("chunks_done", next.NextChunkOrdinal).Int("chunks_total", next.ChunksTotal).
				Msg("reconciliation sweep: stale beyond alert threshold")
		}
		return
	}

	// The entire sweep is done: this is the only place invariant 5 permits
	// clearing Stale. Reset progress so a FUTURE stop window starts a
	// fresh sweep (and a fresh staleness clock) rather than resuming a
	// stale cursor into a chunk list that may have grown or shrunk.
	//
	// A failed delete here is treated as fatal for THIS tick: it must NOT
	// proceed to write ReconciliationOK, because the leftover progress
	// record (NextChunkOrdinal == ChunksTotal) would look complete on
	// sight to the very next stale sweep -- reconciliation_checker.go's
	// `next.NextChunkOrdinal >= len(chunks)` check -- and that next sweep
	// would clear Stale having compared zero chunks. There genuinely is a
	// next tick that will retry the delete (reconciliation stays
	// Stale/Running until this succeeds), so bail out now rather than
	// claim completion that hasn't actually been durably recorded.
	if err := m.deleteReconciliationProgress(id); err != nil {
		log.Error().Err(err).Str("pipeline_id", id).Msg("reconciliation sweep: failed to clear completed progress record; leaving reconciliation stale, will retry deletion next tick")
		return
	}
	newRec := rec
	newRec.Reconciliation = protocol.ReconciliationOK
	newRec.UpdatedAt = now
	if err := m.putLifecycleRecordCAS(id, newRec, rev); err != nil {
		log.Warn().Err(err).Str("pipeline_id", id).Msg("reconciliation sweep: lifecycle record changed concurrently, skipping completion write this tick")
		return
	}
	reconciliationStaleSecondsGauge.WithLabelValues(id).Set(0)
	reconciliationChunksDoneGauge.WithLabelValues(id).Set(float64(next.ChunksTotal))
	log.Info().Str("pipeline_id", id).Int64("rows_reconciled", next.RowsReconciled).
		Msg("reconciliation sweep: complete, reconciliation no longer stale")
}

// getLifecycleRecordRev is getLifecycleRecord plus the KV entry's revision,
// so a caller that does slow, unbounded I/O (the reconciliation stepper:
// source+sink queries with 10s/5s/30s/30s worst-case timeouts) between
// reading and writing the lifecycle record can write back with an
// optimistic-concurrency guard (kv.Update, the same primitive
// UpdateSchemaStateCAS already uses) instead of a blind Put. A blind Put
// here would silently clobber a concurrent Transition()-driven write (e.g.
// an operator's pause/resume landing mid-sweep) -- see
// maybeSweepReconciliation's call sites.
func (m *ConfigManager) getLifecycleRecordRev(id string) (protocol.PipelineLifecycleRecord, uint64, bool) {
	entry, err := m.kv.Get(protocol.LifecycleStateKey(id))
	if err != nil {
		return protocol.PipelineLifecycleRecord{}, 0, false
	}
	var rec protocol.PipelineLifecycleRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil || rec.State == "" {
		return protocol.PipelineLifecycleRecord{}, 0, false
	}
	return rec, entry.Revision(), true
}

// putLifecycleRecordCAS writes rec only if the lifecycle record is still at
// revision rev -- i.e. nothing wrote it since the matching
// getLifecycleRecordRev call. Returns an error (revision mismatch or
// otherwise) if anything else won the race; callers must not fall back to
// an unconditional Put on failure, or this loses the guarantee entirely.
func (m *ConfigManager) putLifecycleRecordCAS(id string, rec protocol.PipelineLifecycleRecord, rev uint64) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = m.kv.Update(protocol.LifecycleStateKey(id), data, rev)
	return err
}

func (m *ConfigManager) getReconciliationProgress(id string) (protocol.ReconciliationProgress, bool) {
	entry, err := m.kv.Get(protocol.ReconciliationProgressKey(id))
	if err != nil {
		return protocol.ReconciliationProgress{}, false
	}
	var p protocol.ReconciliationProgress
	if err := json.Unmarshal(entry.Value(), &p); err != nil {
		return protocol.ReconciliationProgress{}, false
	}
	return p, true
}

func (m *ConfigManager) putReconciliationProgress(id string, p protocol.ReconciliationProgress) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = m.kv.Put(protocol.ReconciliationProgressKey(id), data)
	return err
}

func (m *ConfigManager) deleteReconciliationProgress(id string) error {
	err := m.kv.Delete(protocol.ReconciliationProgressKey(id))
	if err != nil && err == nats.ErrKeyNotFound {
		return nil
	}
	return err
}
