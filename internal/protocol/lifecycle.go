package protocol

import "fmt"

// State is a pipeline lifecycle state, as distinct from desired state
// (operator intent, see PipelineConfig.desired_state) and health (derived
// from worker heartbeat). See plans/2026-08-03-pipeline-lifecycle-control.md
// section 4.
type State string

// State values: the exhaustive set of lifecycle states.
const (
	StateRunning         State = "Running"
	StatePausing         State = "Pausing"
	StatePaused          State = "Paused"
	StateStopping        State = "Stopping"
	StateStopped         State = "Stopped"
	StateNeedsResnapshot State = "NeedsResnapshot"
	StateSnapshotting    State = "Snapshotting"
	StateResuming        State = "Resuming"
	StateFailed          State = "Failed"
)

// allStates is the exhaustive set of lifecycle states. Used only by tests to
// assert every (State, Event) pair has been considered.
var allStates = []State{
	StateRunning,
	StatePausing,
	StatePaused,
	StateStopping,
	StateStopped,
	StateNeedsResnapshot,
	StateSnapshotting,
	StateResuming,
	StateFailed,
}

// Event is a lifecycle transition trigger.
type Event string

// Event values: the exhaustive set of lifecycle transition triggers.
const (
	EventPause          Event = "pause"            // operator requested pause(ttl)
	EventStop           Event = "stop"             // operator requested stop
	EventStart          Event = "start"            // operator requested start/resume
	EventDrainComplete  Event = "drain_complete"   // worker finished draining in-flight work
	EventTimerExpiry    Event = "timer_expiry"     // paused_until has elapsed
	EventWALGuardBreach Event = "wal_guard_breach" // WAL budget guard tripped
	EventSlotDropped    Event = "slot_dropped"     // slot confirmed dropped after drain
	EventComplete       Event = "complete"         // snapshot/reconciliation step finished
	EventWorkerHealthy  Event = "worker_healthy"   // worker reported healthy after (re)start
	EventFailure        Event = "failure"          // any transient transition failed
)

// allEvents is the exhaustive set of lifecycle events. Used only by tests.
var allEvents = []Event{
	EventPause,
	EventStop,
	EventStart,
	EventDrainComplete,
	EventTimerExpiry,
	EventWALGuardBreach,
	EventSlotDropped,
	EventComplete,
	EventWorkerHealthy,
	EventFailure,
}

// transientStates are the states from which any EventFailure lands in
// StateFailed (see section 4.3, "any transient | failure | Failed").
var transientStates = map[State]bool{
	StatePausing:      true,
	StateStopping:     true,
	StateSnapshotting: true,
	StateResuming:     true,
}

// Guards are the externally-observed facts a transition may depend on.
// Callers populate only the fields relevant to the event being fired;
// unused fields are ignored by the transition being evaluated.
type Guards struct {
	// SlotAlive reports whether the replication slot is currently usable.
	// Consulted by (Paused, start) and (Paused, timer_expiry).
	SlotAlive bool

	// WALStatusLost reports whether the slot's wal_status has reached
	// "lost" (see section 7). Consulted by (Paused, timer_expiry).
	WALStatusLost bool

	// StopWindowOccurred reports whether the pipeline passed through a
	// stop (as opposed to a pause) before this Snapshotting run, which
	// forces the reconciliation sub-status to ReconciliationStale per
	// invariant 5. Consulted by (Snapshotting, complete).
	StopWindowOccurred bool

	// WasFailed indicates the transition is being re-evaluated out of
	// StateFailed (Failed, start), and disambiguates whether the retry
	// should attempt a plain resume or requires a re-snapshot.
	NeedsResnapshot bool
}

// ReconciliationStatus is the reconciliation sub-status carried alongside a
// destination state, per section 4.3's "Running, reconciliation sub-status
// stale" row. It is only meaningful when the destination state is Running;
// zero value ReconciliationOK means no staleness is implied by the
// transition itself.
type ReconciliationStatus string

const (
	// ReconciliationOK means the transition carries no reconciliation
	// staleness implication.
	ReconciliationOK ReconciliationStatus = ""
	// ReconciliationStale means the pipeline left Snapshotting after
	// having passed through a stop window (Guards.StopWindowOccurred),
	// per invariant 5: it must be reported Running but flagged stale
	// until WS-7's reconciliation pass clears it.
	ReconciliationStale ReconciliationStatus = "stale"
	// ReconciliationRunning means WS-7's chunked delete-reconciliation
	// sweep is actively working through a pipeline's tables. Set by the
	// sweep itself (internal/config/reconciliation.go), never by
	// Transition -- reconciliation is deliberately not a lifecycle state
	// (plan section 4.2), so nothing in the transition table below
	// produces this value. Progressing from Stale to Running to OK is the
	// only way invariant 5's "the only way out of stale is a completed
	// sweep" is satisfied.
	ReconciliationRunning ReconciliationStatus = "running"
)

// Outcome is the result of a legal transition: the destination state, plus
// any reconciliation sub-status that state carries (section 4.3).
type Outcome struct {
	To             State
	Reconciliation ReconciliationStatus
}

// transitionKey identifies one row of the table in section 4.3.
type transitionKey struct {
	from  State
	event Event
}

// transitionFunc computes the outcome for a legal (from, event) pair, given
// the guards observed at call time. It returns an error if the guards are
// insufficient to decide, which Transition surfaces to the caller rather
// than guessing.
type transitionFunc func(g Guards) (Outcome, error)

// transitions is the transition table from plan section 4.3, encoded as
// data. This is the single source of truth for legal lifecycle moves: no
// switch statement buried in a handler decides this, and nothing outside
// Transition may write the lifecycle state key directly.
var transitions = map[transitionKey]transitionFunc{
	{StateRunning, EventPause}: constant(StatePausing),
	{StateRunning, EventStop}:  constant(StateStopping),

	{StatePausing, EventDrainComplete}: constant(StatePaused),

	{StatePaused, EventStart}: func(g Guards) (Outcome, error) {
		if !g.SlotAlive {
			return Outcome{}, fmt.Errorf("lifecycle: cannot start from Paused: slot is not alive")
		}
		return Outcome{To: StateResuming}, nil
	},
	{StatePaused, EventTimerExpiry}: func(g Guards) (Outcome, error) {
		if g.WALStatusLost {
			return Outcome{To: StateNeedsResnapshot}, nil
		}
		if !g.SlotAlive {
			return Outcome{}, fmt.Errorf("lifecycle: cannot resume from Paused on timer expiry: slot is not alive and wal_status is not lost")
		}
		return Outcome{To: StateResuming}, nil
	},
	{StatePaused, EventWALGuardBreach}: constant(StateStopping),
	{StatePaused, EventStop}:           constant(StateStopping),

	// Extending a pause: the plan's section 11 risk row ("Auto-resume
	// fires during the incident the operator paused for") is mitigated by
	// making this trivial, not by routing an already-paused pipeline back
	// through Pausing (the worker is already down; there is nothing to
	// drain again). The caller (PausePipeline) recomputes paused_until
	// from the fresh ttl the same way it does for Running -> Pausing ->
	// Paused; this row just makes that a legal move instead of a 409.
	{StatePaused, EventPause}: constant(StatePaused),

	{StateStopping, EventSlotDropped}: constant(StateStopped),

	{StateStopped, EventStart}: constant(StateNeedsResnapshot),

	{StateNeedsResnapshot, EventStart}: constant(StateSnapshotting),

	// Snapshotting/complete is the sole producer of invariant 5's stale
	// marking: if the pipeline passed through a stop window before this
	// snapshot ran, the caller must observe Reconciliation ==
	// ReconciliationStale and treat Running as provisional until WS-7
	// reconciles it.
	{StateSnapshotting, EventComplete}: func(g Guards) (Outcome, error) {
		if g.StopWindowOccurred {
			return Outcome{To: StateRunning, Reconciliation: ReconciliationStale}, nil
		}
		return Outcome{To: StateRunning}, nil
	},

	{StateResuming, EventWorkerHealthy}: constant(StateRunning),

	{StateFailed, EventStart}: func(g Guards) (Outcome, error) {
		if g.NeedsResnapshot {
			return Outcome{To: StateNeedsResnapshot}, nil
		}
		return Outcome{To: StateResuming}, nil
	},
}

func constant(to State) transitionFunc {
	return func(Guards) (Outcome, error) {
		return Outcome{To: to}, nil
	}
}

// Transition is the single choke point for lifecycle state changes. Given
// the current state and a triggering event, it returns the outcome (next
// state, plus any reconciliation sub-status) or an error. Illegal (state,
// event) pairs always error rather than silently landing somewhere; later
// phases must route every state write through this function and never
// write the lifecycle state key directly.
func Transition(from State, e Event, g Guards) (Outcome, error) {
	if e == EventFailure {
		if !transientStates[from] {
			return Outcome{}, fmt.Errorf("lifecycle: illegal transition: %s does not accept event %q (failure is only valid from a transient state)", from, e)
		}
		return Outcome{To: StateFailed}, nil
	}

	fn, ok := transitions[transitionKey{from, e}]
	if !ok {
		return Outcome{}, fmt.Errorf("lifecycle: illegal transition: %s does not accept event %q", from, e)
	}
	return fn(g)
}
