package config

import (
	"context"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

// SlotDropper drops a pipeline's replication slot once the drain
// stopWorker performs has completed (plan section 4.3: "Stopping | drain
// complete, slot dropped | Stopped"). This is what actually releases WAL --
// the entire point of stop, as opposed to pause (plan section 1) -- so it
// must run before finalizeStop (manager.go) is allowed to persist Stopped.
type SlotDropper func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) error

// defaultSlotDropper is the optimistic placeholder installed by
// NewConfigManager, mirroring SlotHealthChecker's "always installed,
// defaults optimistic" pattern (see pause_expiry.go's doc comment on
// defaultSlotHealthChecker) rather than WALGuardChecker's nil-skip one:
// unlike the WAL guard, which is purely a backstop that is safe to leave
// off, a Stopping pipeline has nowhere else to go without a slot dropper,
// so a no-op that reports success would leave every pre-WS-5 caller (and
// any test that never calls SetSlotDropper) with a pipeline stuck in
// Stopping forever. Reporting success here mirrors those pipelines' status
// quo (no slot-dropping code existed before WS-5, so nothing was ever
// dropped) while unblocking the Stopping -> Stopped transition;
// cmd/pipeline/main.go installs the real probe (NewPostgresSlotDropper)
// for production.
func defaultSlotDropper(context.Context, string, protocol.PipelineConfig) error {
	return nil
}

// SetSlotDropper overrides the slot-drop probe finalizeStop consults after
// a stopped pipeline's worker has drained. Pass nil to restore the
// optimistic default.
func (m *ConfigManager) SetSlotDropper(d SlotDropper) {
	if d == nil {
		d = defaultSlotDropper
	}
	m.pauseMu.Lock()
	m.slotDropper = d
	m.pauseMu.Unlock()
}

func (m *ConfigManager) getSlotDropper() SlotDropper {
	m.pauseMu.RLock()
	defer m.pauseMu.RUnlock()
	if m.slotDropper == nil {
		return defaultSlotDropper
	}
	return m.slotDropper
}
