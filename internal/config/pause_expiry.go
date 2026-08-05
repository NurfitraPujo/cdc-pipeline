package config

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// Clock abstracts time.Now so the pause-expiry ticker below -- and the
// tests that exercise it -- never depend on wall-clock time. Plan WS-3:
// "Make the ticker testable without real time -- inject a clock."
type Clock interface {
	Now() time.Time
}

// realClock is the production Clock, backed by time.Now. It is the default
// installed by NewConfigManager; SetClock overrides it for tests.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// SlotHealth is the subset of replication-slot state the pause-expiry
// ticker needs before acting on an elapsed paused_until (plan section 4.3,
// row "Paused | timer_expiry"). It fills in the SlotAlive/WALStatusLost
// guards protocol.Transition consults for that row.
type SlotHealth struct {
	// Alive reports whether the replication slot is still usable. Consulted
	// when WALStatusLost is false.
	Alive bool
	// WALStatusLost reports whether wal_status has reached "lost" (plan
	// section 7's ladder: reserved -> extended -> unreserved -> lost). When
	// true, timer expiry lands on NeedsResnapshot rather than Resuming,
	// per plan section 4.3, regardless of Alive.
	WALStatusLost bool
}

// SlotHealthChecker probes a pipeline's replication slot health at
// timer-expiry time. The default installed by NewConfigManager
// (defaultSlotHealthChecker) is optimistic -- Alive: true, WALStatusLost:
// false -- because the slot is never dropped before WS-5 lands; this
// mirrors the same simplification StartPipeline (internal/api/handler.go)
// makes today ("SlotAlive: true ... WS-4/WS-5 replace this constant with a
// real probe"). SetSlotHealthChecker installs a real probe once WS-4/WS-5
// land.
type SlotHealthChecker func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) SlotHealth

func defaultSlotHealthChecker(context.Context, string, protocol.PipelineConfig) SlotHealth {
	return SlotHealth{Alive: true}
}

// SetClock overrides the pause-expiry ticker's time source. Tests use this
// to make paused_until expire deterministically instead of sleeping.
func (m *ConfigManager) SetClock(c Clock) {
	if c == nil {
		c = realClock{}
	}
	m.pauseMu.Lock()
	m.clock = c
	m.pauseMu.Unlock()
}

// SetSlotHealthChecker overrides the slot-health probe consulted before
// resuming a Paused pipeline on timer expiry. See SlotHealthChecker's doc
// comment for the default.
func (m *ConfigManager) SetSlotHealthChecker(c SlotHealthChecker) {
	if c == nil {
		c = defaultSlotHealthChecker
	}
	m.pauseMu.Lock()
	m.slotHealthChecker = c
	m.pauseMu.Unlock()
}

func (m *ConfigManager) getClock() Clock {
	m.pauseMu.RLock()
	defer m.pauseMu.RUnlock()
	if m.clock == nil {
		return realClock{}
	}
	return m.clock
}

func (m *ConfigManager) getSlotHealthChecker() SlotHealthChecker {
	m.pauseMu.RLock()
	defer m.pauseMu.RUnlock()
	if m.slotHealthChecker == nil {
		return defaultSlotHealthChecker
	}
	return m.slotHealthChecker
}

// StartPauseExpiryTicker launches the manager-level ticker plan WS-3 adds.
// It cannot hang off either of ConfigManager's existing timers: the
// per-worker heartbeat in monitorWorker never runs for a Paused pipeline
// (there is no worker), and the KV watch in handlePipelineUpdates is
// event-driven, but a Paused pipeline sitting still emits no events either.
// Nothing else notices paused_until has elapsed -- this ticker is that
// notice.
//
// Expiry is self-healing by construction: every tick re-reads each
// pipeline's persisted lifecycle record from KV rather than tracking
// per-pipeline timers in memory, so a manager that was down when
// paused_until elapsed still resumes (or escalates) it on the first tick
// after restart, exactly as plan section 8 requires.
func (m *ConfigManager) StartPauseExpiryTicker(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.tickPauseExpiry(ctx)
			}
		}
	}()
}

// tickPauseExpiry is one sweep of the ticker: list every persisted
// lifecycle record and act on any Paused one whose paused_until has
// elapsed, any Paused one whose WAL guard has breached, and (WS-6) any
// Snapshotting one whose forced re-snapshot has completed. It is unexported
// but called directly (rather than only via the ticker goroutine) by tests
// that want to drive a deterministic tick.
func (m *ConfigManager) tickPauseExpiry(ctx context.Context) {
	// RM-3: with a lease loop running (StartLeaseLoop), only the current
	// leaseholder runs the sweep body below -- production runs 3-20
	// replicas (deploy/helm-chart/values.production.yml), and running this
	// unconditionally on every replica multiplied WS-7's "one chunk per
	// tick" rate limit by the replica count and raced blind lifecycle
	// writes across replicas. Ticking cheaply and returning here (rather
	// than not ticking at all) is what lets a non-leader take over within
	// ~one TTL of the leader dying: it keeps trying to acquire the lease
	// via its own StartLeaseLoop goroutine regardless of what this ticker
	// does. When no lease loop was ever started (leaseGatingEnabled ==
	// false), this manager is the only one running -- exactly the pre-RM-3
	// single-manager assumption -- so the sweep runs unconditionally, which
	// is also what keeps every pre-existing test working without having to
	// start a lease loop.
	if m.leaseGatingEnabled() && !m.IsLeader() {
		return
	}

	keys, err := m.kv.Keys()
	if err != nil {
		if err == nats.ErrNoKeysFound {
			return
		}
		log.Warn().Err(err).Msg("pause-expiry ticker: failed to list KV keys")
		return
	}

	now := m.getClock().Now()
	for _, key := range keys {
		id := pipelineIDFromLifecycleKey(key)
		if id == "" {
			continue
		}
		// WS-4: the WAL guard is an independent backstop that can trip a
		// Paused pipeline to Stopping regardless of paused_until (plan
		// section 5: "Whichever of guard or timer trips first wins"), so
		// it is checked first, every tick, unconditionally. If it fires
		// this tick, the pipeline is no longer Paused, so the
		// timer-expiry check below would be a no-op anyway -- skip it.
		if m.maybeEscalateWALGuardBreach(ctx, id) {
			continue
		}
		m.maybeResumeExpiredPause(ctx, id, now)

		// WS-6: independent of the pause/WAL-guard checks above (they only
		// ever act on a Paused pipeline; this only ever acts on a
		// Snapshotting one), so it runs unconditionally every tick rather
		// than behind a continue.
		m.maybeCompleteResnapshot(ctx, id)

		// WS-7: independent of every check above -- best-effort delete
		// reconciliation runs against a Stale/Running sub-status
		// regardless of the pipeline's lifecycle state (plan section 4.2:
		// reconciliation must never gate Running), so it too runs
		// unconditionally every tick.
		m.maybeSweepReconciliation(ctx, id)
	}
}

// lifecycleKeySuffix matches the suffix protocol.LifecycleStateKey appends;
// no other PrefixPipelineState-keyed helper in internal/protocol/config.go
// produces this suffix, so trimming it is an unambiguous way to recover the
// pipeline ID from a raw KV key listing.
const lifecycleKeySuffix = ".lifecycle"

func pipelineIDFromLifecycleKey(key string) string {
	if !strings.HasPrefix(key, protocol.PrefixPipelineState) || !strings.HasSuffix(key, lifecycleKeySuffix) {
		return ""
	}
	id := strings.TrimPrefix(key, protocol.PrefixPipelineState)
	id = strings.TrimSuffix(id, lifecycleKeySuffix)
	return id
}

// maybeResumeExpiredPause is the per-pipeline decision at the heart of the
// ticker. It only acts when the persisted record is Paused with an elapsed
// paused_until; everything else (Running, Pausing, Stopping, an
// already-cleared PausedUntil, a still-future PausedUntil) is left alone.
func (m *ConfigManager) maybeResumeExpiredPause(ctx context.Context, id string, now time.Time) {
	rec, rev, ok := m.getLifecycleRecordRev(id)
	if !ok || rec.State != protocol.StatePaused || rec.PausedUntil == nil {
		return
	}
	if rec.PausedUntil.After(now) {
		return
	}

	cfg, ok := m.getPipelineConfig(id)
	if !ok {
		log.Warn().Str("pipeline_id", id).Msg("pause-expiry ticker: paused_until elapsed but pipeline config is gone; skipping")
		return
	}

	health := m.getSlotHealthChecker()(ctx, id, cfg)
	outcome, err := protocol.Transition(protocol.StatePaused, protocol.EventTimerExpiry, protocol.Guards{
		SlotAlive:     health.Alive,
		WALStatusLost: health.WALStatusLost,
	})
	if err != nil {
		// Neither guard value protocol.Transition accepts for (Paused,
		// timer_expiry) is illegal, so this should be unreachable; log
		// rather than panic and leave the record untouched for the next
		// tick to retry.
		log.Error().Err(err).Str("pipeline_id", id).Msg("pause-expiry ticker: illegal timer-expiry transition")
		return
	}

	newRec := protocol.PipelineLifecycleRecord{
		State:          outcome.To,
		Reconciliation: rec.Reconciliation,
		UpdatedAt:      now,
		// invariant 3: paused_until is only meaningful in Pausing/Paused.
		// Both outcomes below leave Paused, so it is always cleared here.
	}

	switch outcome.To {
	case protocol.StateResuming:
		// Mirror StartPipeline's synchronous Resuming -> Running shortcut
		// (internal/api/handler.go): there is no async drain/worker-healthy
		// watcher for this path yet, so the two legal hops are taken
		// back-to-back rather than left half-finished.
		final, err := protocol.Transition(protocol.StateResuming, protocol.EventWorkerHealthy, protocol.Guards{})
		if err != nil {
			log.Error().Err(err).Str("pipeline_id", id).Msg("pause-expiry ticker: illegal worker-healthy transition")
			return
		}
		newRec.State = final.To

		// Flip desired_state back to running so ConfigManager's own
		// config-watch (handlePipelineUpdates -> startNewWorker) starts the
		// worker; the ticker does not start it directly, the same
		// separation of concerns PausePipeline/StartPipeline use.
		cfg.DesiredState = protocol.DesiredStateRunning
		if !m.putPipelineConfig(id, cfg) {
			return
		}
		log.Info().Str("pipeline_id", id).Msg("pause-expiry ticker: paused_until elapsed, resuming")
	case protocol.StateNeedsResnapshot:
		// Slot is gone or wal_status has reached "lost": leave desired_state
		// untouched (still paused) so ConfigManager never starts a plain
		// worker for a pipeline that needs a re-snapshot first -- the same
		// invariant-1 guard StartPipeline's doc comment describes for
		// Stopped -> NeedsResnapshot.
		log.Warn().Str("pipeline_id", id).Msg("pause-expiry ticker: paused_until elapsed but wal_status is lost, escalating to NeedsResnapshot")
	default:
		log.Error().Str("pipeline_id", id).Str("to", string(outcome.To)).Msg("pause-expiry ticker: unexpected timer-expiry outcome")
		return
	}

	// RM-3: CAS against the revision read above, not a blind Put -- with the
	// lease now making concurrent sweeps rare rather than impossible (a
	// lease can still be raced during handover, or an operator's own
	// pause/resume/stop request can land mid-tick regardless of leasing),
	// a stale overwrite here could resurrect a superseded lifecycle state.
	// A lost race just means this tick's write is skipped; the next tick
	// re-reads fresh and retries.
	if err := m.putLifecycleRecordCAS(id, newRec, rev); err != nil {
		log.Warn().Err(err).Str("pipeline_id", id).Msg("pause-expiry ticker: lifecycle record changed concurrently, skipping this tick's write")
	}
}

// getLifecycleRecord/putLifecycleRecord/getPipelineConfig/putPipelineConfig
// mirror the equivalently-named helpers on internal/api.Handler. Both
// packages read and write the same two KV keys (protocol.LifecycleStateKey,
// protocol.PipelineConfigKey) but do not share a common client type to hang
// a shared helper off, so the small amount of marshal/unmarshal
// boilerplate is duplicated here rather than introducing a dependency
// between the api and config packages.
func (m *ConfigManager) getLifecycleRecord(id string) (protocol.PipelineLifecycleRecord, bool) {
	entry, err := m.kv.Get(protocol.LifecycleStateKey(id))
	if err != nil {
		return protocol.PipelineLifecycleRecord{}, false
	}
	var rec protocol.PipelineLifecycleRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil || rec.State == "" {
		return protocol.PipelineLifecycleRecord{}, false
	}
	return rec, true
}

func (m *ConfigManager) putLifecycleRecord(id string, rec protocol.PipelineLifecycleRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = m.kv.Put(protocol.LifecycleStateKey(id), data)
	return err
}

func (m *ConfigManager) getPipelineConfig(id string) (protocol.PipelineConfig, bool) {
	entry, err := m.kv.Get(protocol.PipelineConfigKey(id))
	if err != nil {
		return protocol.PipelineConfig{}, false
	}
	var cfg protocol.PipelineConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		return protocol.PipelineConfig{}, false
	}
	return cfg, true
}

func (m *ConfigManager) putPipelineConfig(id string, cfg protocol.PipelineConfig) bool {
	data, err := json.Marshal(cfg)
	if err != nil {
		log.Error().Err(err).Str("pipeline_id", id).Msg("pause-expiry ticker: failed to marshal config")
		return false
	}
	if _, err := m.kv.Put(protocol.PipelineConfigKey(id), data); err != nil {
		log.Error().Err(err).Str("pipeline_id", id).Msg("pause-expiry ticker: failed to persist desired_state")
		return false
	}
	return true
}
