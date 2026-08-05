package protocol

import (
	"fmt"
	"time"
)

// LifecycleStateKey addresses the persisted lifecycle-state record for a
// pipeline -- the System-owned "what is it actually doing right now" side
// of section 4.1, as distinct from PipelineConfig.DesiredState (operator
// intent) and the heartbeat-derived health. Nothing outside the handlers
// that call protocol.Transition (internal/protocol/lifecycle.go) may write
// this key; it is the on-disk form of an Outcome.
func LifecycleStateKey(id string) string {
	return fmt.Sprintf("%s%s.lifecycle", PrefixPipelineState, id)
}

// PipelineLifecycleRecord is the persisted form of a pipeline's lifecycle
// state, plus the two pieces of data the machine's invariants (section
// 4.4) require alongside the state itself:
//
//   - PausedUntil is set only while State is Pausing/Paused (invariant 3)
//     and is cleared on exit. It is an absolute RFC3339 timestamp computed
//     at request time, never a duration and never a NATS KV TTL -- per
//     plan section 8, a KV TTL would delete the pipeline config on expiry,
//     which ConfigManager already reads as "remove this worker"
//     (manager.go ~341), not "resume it". Storing an absolute time also
//     makes expiry self-healing: a manager that was down when the timer
//     elapsed sees paused_until in the past on its first tick and resumes,
//     no missed-timer bookkeeping required.
//   - Reconciliation carries invariant 5's sub-status (idle/running/stale)
//     forward across transitions; only Snapshotting/complete sets it to
//     stale (see Transition), and only WS-7's sweep clears it.
//   - Reason is an optional operator-facing note attached by a transition
//     that was system-driven rather than requested, so the "why" survives
//     alongside the "what". WS-4 sets it on (Paused, wal_guard_breach) ->
//     Stopping (plan section 7: escalating at wal_status=unreserved, or a
//     safe_wal_size/lag-threshold breach, before PostgreSQL invalidates the
//     slot for us). Cleared implicitly by any later transition that does
//     not set it, since PipelineLifecycleRecord is written wholesale, never
//     patched field-by-field.
type PipelineLifecycleRecord struct {
	State          State                `json:"state"`
	PausedUntil    *time.Time           `json:"paused_until,omitempty"`
	Reconciliation ReconciliationStatus `json:"reconciliation,omitempty"`
	Reason         string               `json:"reason,omitempty"`
	UpdatedAt      time.Time            `json:"updated_at"`
}
