package protocol

import (
	"testing"
)

// permissiveGuards is a Guards value tuned to make every guarded transition
// succeed, used where a test only cares whether a (state, event) pair is
// legal at all, not which guard branch fires.
var permissiveGuards = Guards{
	SlotAlive:          true,
	WALStatusLost:      false,
	StopWindowOccurred: true,
	NeedsResnapshot:    false,
}

// TestExhaustive_EveryStateEventPairIsDecided asserts that for every
// combination of State and Event, Transition either returns the documented
// destination state (with the documented Reconciliation sub-status where
// applicable) or an explicit error - never a panic, and never an
// undocumented gap, and never a right-shaped-but-wrong destination. This is
// what makes it safe to add a new state or event: the table forces a
// decision for every pair it participates in, and this test pins WHERE
// that decision lands, not merely that it lands somewhere.
//
// Two rows branch on a guard (Paused/timer_expiry, Failed/start); those are
// asserted separately below by TestPausedTimerExpiry_* /
// TestFailedStart_RoutesByNeedsResnapshotGuard, so here legalPairs records
// the destination reached under permissiveGuards specifically.
func TestExhaustive_EveryStateEventPairIsDecided(t *testing.T) {
	legalPairs := map[transitionKey]Outcome{
		{StateRunning, EventPause}:          {To: StatePausing},
		{StateRunning, EventStop}:           {To: StateStopping},
		{StatePausing, EventDrainComplete}:  {To: StatePaused},
		{StatePaused, EventStart}:           {To: StateResuming},        // permissiveGuards.SlotAlive == true
		{StatePaused, EventTimerExpiry}:     {To: StateResuming},        // permissiveGuards: SlotAlive true, WALStatusLost false
		{StatePaused, EventWALGuardBreach}:  {To: StateStopping},
		{StatePaused, EventStop}:            {To: StateStopping},
		{StatePaused, EventPause}:           {To: StatePaused}, // extending a pause (plan section 11)
		{StateStopping, EventSlotDropped}:   {To: StateStopped},
		{StateStopped, EventStart}:          {To: StateNeedsResnapshot},
		{StateNeedsResnapshot, EventStart}:  {To: StateSnapshotting},
		{StateSnapshotting, EventComplete}:  {To: StateRunning, Reconciliation: ReconciliationStale}, // permissiveGuards.StopWindowOccurred == true
		{StateResuming, EventWorkerHealthy}: {To: StateRunning},
		{StateFailed, EventStart}:           {To: StateResuming}, // permissiveGuards.NeedsResnapshot == false
	}

	for _, from := range allStates {
		for _, e := range allEvents {
			t.Run(string(from)+"/"+string(e), func(t *testing.T) {
				outcome, err := Transition(from, e, permissiveGuards)

				if e == EventFailure {
					if transientStates[from] {
						if err != nil {
							t.Fatalf("expected %s to accept failure and land in Failed, got error: %v", from, err)
						}
						if outcome.To != StateFailed {
							t.Fatalf("expected failure from %s to land in Failed, got %s", from, outcome.To)
						}
					} else {
						if err == nil {
							t.Fatalf("expected failure from non-transient state %s to be rejected, got %s with no error", from, outcome.To)
						}
					}
					return
				}

				want, isLegal := legalPairs[transitionKey{from, e}]
				if isLegal {
					if err != nil {
						t.Fatalf("expected (%s, %s) to be legal, got error: %v", from, e, err)
					}
					if outcome.To != want.To {
						t.Fatalf("expected (%s, %s) to reach %s, got %s", from, e, want.To, outcome.To)
					}
					if outcome.Reconciliation != want.Reconciliation {
						t.Fatalf("expected (%s, %s) to carry reconciliation %q, got %q", from, e, want.Reconciliation, outcome.Reconciliation)
					}
				} else {
					if err == nil {
						t.Fatalf("expected (%s, %s) to be illegal, got destination %s with no error", from, e, outcome.To)
					}
				}
			})
		}
	}
}

// TestExhaustive_TableCoversOnlyDocumentedStatesAndEvents guards against the
// enumeration itself silently drifting from section 4.2/4.3: every state
// referenced by the transition table must be in allStates, and every event
// must be in allEvents.
func TestExhaustive_TableCoversOnlyDocumentedStatesAndEvents(t *testing.T) {
	stateSet := map[State]bool{}
	for _, s := range allStates {
		stateSet[s] = true
	}
	eventSet := map[Event]bool{}
	for _, e := range allEvents {
		eventSet[e] = true
	}

	for key := range transitions {
		if !stateSet[key.from] {
			t.Errorf("transition table references undeclared state %s", key.from)
		}
		if !eventSet[key.event] {
			t.Errorf("transition table references undeclared event %s", key.event)
		}
	}
	for s := range transientStates {
		if !stateSet[s] {
			t.Errorf("transientStates references undeclared state %s", s)
		}
	}
}

// TestInvariant1_RunningUnreachableFromStoppedOrNeedsResnapshotWithoutSnapshotting
// is a graph-reachability test, not a spot check: it BFS-explores every
// state reachable from Stopped and from NeedsResnapshot under every legal
// (state, event) pair, and asserts that the only way to reach Running is via
// an edge that departs Snapshotting. Section 4.4 invariant 1.
//nolint:gocyclo // exhaustive state-machine invariant test; iterates every (State,Event) pair, so complexity is inherent to the coverage it provides
func TestInvariant1_RunningUnreachableFromStoppedOrNeedsResnapshotWithoutSnapshotting(t *testing.T) {
	// Build the full edge set (from -> to) using guard values chosen so
	// that every legal transition fires down its "happy" branch. Where a
	// guard branches to two different destinations for the same event
	// (Paused/timer_expiry, Failed/start), both branches are added as
	// edges, since invariant 1 must hold regardless of which branch a
	// real run takes.
	type edge struct{ from, to State }
	var edges []edge
	for _, from := range allStates {
		for _, e := range allEvents {
			if e == EventFailure {
				continue // failure edges do not lead to Running; irrelevant here
			}
			for _, g := range []Guards{
				{SlotAlive: true, WALStatusLost: false, NeedsResnapshot: false},
				{SlotAlive: false, WALStatusLost: true, NeedsResnapshot: true},
				{SlotAlive: true, WALStatusLost: true, NeedsResnapshot: false},
			} {
				if outcome, err := Transition(from, e, g); err == nil {
					edges = append(edges, edge{from, outcome.To})
				}
			}
		}
	}

	// removingSnapshottingExits explores reachability from `start` using
	// the edge set with every edge that departs StateSnapshotting deleted
	// - i.e. "can we reach Running without ever leaving Snapshotting
	// (which is the only way Snapshotting produces Running)".
	reachableWithoutLeavingSnapshotting := func(start State) map[State]bool {
		visited := map[State]bool{start: true}
		queue := []State{start}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, e := range edges {
				if e.from != cur {
					continue
				}
				if e.from == StateSnapshotting {
					continue // the one edge allowed to produce Running
				}
				if !visited[e.to] {
					visited[e.to] = true
					queue = append(queue, e.to)
				}
			}
		}
		return visited
	}

	for _, start := range []State{StateStopped, StateNeedsResnapshot} {
		reachable := reachableWithoutLeavingSnapshotting(start)
		if reachable[StateRunning] {
			t.Fatalf("invariant 1 violated: Running is reachable from %s without passing through Snapshotting", start)
		}
	}

	// Sanity check the graph isn't vacuously trivial: Running must be
	// reachable from both states when Snapshotting's exit edge is allowed.
	for _, start := range []State{StateStopped, StateNeedsResnapshot} {
		visited := map[State]bool{start: true}
		queue := []State{start}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, e := range edges {
				if e.from == cur && !visited[e.to] {
					visited[e.to] = true
					queue = append(queue, e.to)
				}
			}
		}
		if !visited[StateRunning] {
			t.Fatalf("test setup problem: Running is not reachable at all from %s (edge set likely incomplete)", start)
		}
	}
}

// TestInvariant2_SlotAliveness checks the documented slot aliveness per
// state (section 4.4 invariant 2) is at least consistent with the states
// themselves as named/commented in section 4.2 - i.e. that this file's
// state set matches the documented alive/absent/unreliable classification.
// This is a documentation-consistency check at this layer; actual slot
// liveness is enforced by the components that manage the slot, not by
// Transition itself.
func TestInvariant2_SlotAliveness(t *testing.T) {
	slotAlive := map[State]bool{
		StateRunning:      true,
		StatePausing:      true,
		StatePaused:       true,
		StateResuming:     true,
		StateSnapshotting: true,
	}
	slotAbsentOrUnreliable := map[State]bool{
		StateStopped:         true,
		StateNeedsResnapshot: true,
	}

	for _, s := range allStates {
		alive, aliveDoc := slotAlive[s]
		_, absentDoc := slotAbsentOrUnreliable[s]
		if !aliveDoc && !absentDoc && s != StateStopping && s != StateFailed {
			t.Fatalf("state %s has no documented slot-aliveness classification; update this test alongside section 4.2 if a new state is added", s)
		}
		if aliveDoc && !alive {
			t.Fatalf("internal test table inconsistency for %s", s)
		}
	}
}

// TestInvariant3_PausedUntilOnlySetInPausingOrPaused documents, at this
// layer, which states are allowed to carry a live pause timer. Transition
// itself does not store paused_until (that's WS-3's job), but the set of
// states it can land in constrains where a caller may legally set/clear it,
// so this pins down that set against drift.
func TestInvariant3_PausedUntilOnlySetInPausingOrPaused(t *testing.T) {
	allowed := map[State]bool{StatePausing: true, StatePaused: true}
	for _, s := range allStates {
		if allowed[s] {
			continue
		}
		if s == StatePausing || s == StatePaused {
			t.Fatalf("test table drift for %s", s)
		}
	}
	// Every legal exit from Paused must land somewhere outside {Pausing,
	// Paused}, i.e. paused_until always gets cleared on exit. EventPause is
	// deliberately not an "exit": (Paused, pause) is the extend-a-pause row
	// added for plan section 11 ("make extending it trivial") and it stays
	// in Paused on purpose, with the caller recomputing (not clearing)
	// paused_until from the fresh ttl -- see PausePipeline.
	for _, e := range allEvents {
		if e == EventFailure || e == EventPause {
			continue
		}
		for _, g := range []Guards{
			{SlotAlive: true}, {SlotAlive: false, WALStatusLost: true},
		} {
			outcome, err := Transition(StatePaused, e, g)
			if err != nil {
				continue
			}
			if allowed[outcome.To] {
				t.Fatalf("exit from Paused via %s landed back in a paused_until-bearing state %s", e, outcome.To)
			}
		}
	}
}

// TestInvariant4_HealthOnlyWrittenWhileRunning documents that Running is the
// only state where health is meaningful (section 4.4 invariant 4). This is
// not enforceable inside Transition itself (health is written elsewhere),
// so this test pins the state name callers must gate on.
func TestInvariant4_HealthOnlyWrittenWhileRunning(t *testing.T) {
	if StateRunning != "Running" {
		t.Fatalf("StateRunning value drifted; health-gating callers key off this constant")
	}
}

// TestInvariant5_LeavingSnapshottingAfterStopMarksStale asserts the
// enforced part of invariant 5: Transition(Snapshotting, complete, g) always
// reaches Running, but the Reconciliation sub-status differs by
// StopWindowOccurred - ReconciliationStale when a stop window preceded this
// snapshot, ReconciliationOK otherwise. This is the choke point itself, not
// just documentation: a caller cannot land on Running without Transition
// telling it whether to mark stale, and cannot forget to check because the
// field is part of the returned Outcome.
func TestInvariant5_LeavingSnapshottingAfterStopMarksStale(t *testing.T) {
	outcome, err := Transition(StateSnapshotting, EventComplete, Guards{StopWindowOccurred: true})
	if err != nil {
		t.Fatalf("unexpected error leaving Snapshotting (StopWindowOccurred=true): %v", err)
	}
	if outcome.To != StateRunning {
		t.Fatalf("expected Snapshotting/complete to reach Running, got %s", outcome.To)
	}
	if outcome.Reconciliation != ReconciliationStale {
		t.Fatalf("expected StopWindowOccurred=true to mark reconciliation stale, got %q", outcome.Reconciliation)
	}

	outcome, err = Transition(StateSnapshotting, EventComplete, Guards{StopWindowOccurred: false})
	if err != nil {
		t.Fatalf("unexpected error leaving Snapshotting (StopWindowOccurred=false): %v", err)
	}
	if outcome.To != StateRunning {
		t.Fatalf("expected Snapshotting/complete to reach Running, got %s", outcome.To)
	}
	if outcome.Reconciliation != ReconciliationOK {
		t.Fatalf("expected StopWindowOccurred=false to NOT mark reconciliation stale, got %q", outcome.Reconciliation)
	}
}

// TestInvariant6_EveryTransitionIsATableRow is the flip side of the
// exhaustive test above: it asserts Transition never computes a destination
// by any means other than a lookup in `transitions` (or the hardcoded
// failure rule), by checking that removing an entry from the table (done
// here via a parallel illegal-pairs sweep) makes the pair illegal. Combined
// with TestExhaustive_EveryStateEventPairIsDecided this pins "no other way
// to change state" (section 4.4 invariant 6).
func TestInvariant6_EveryTransitionIsATableRow(t *testing.T) {
	for _, from := range allStates {
		for _, e := range allEvents {
			if e == EventFailure {
				continue
			}
			_, isRow := transitions[transitionKey{from, e}]
			_, err := Transition(from, e, permissiveGuards)
			if isRow && err != nil {
				t.Fatalf("(%s,%s) is a table row but Transition rejected it: %v", from, e, err)
			}
			if !isRow && err == nil {
				t.Fatalf("(%s,%s) is not a table row but Transition accepted it - state must be changing outside the table", from, e)
			}
		}
	}
}

func TestPausedTimerExpiry_WALLostRoutesToNeedsResnapshot(t *testing.T) {
	outcome, err := Transition(StatePaused, EventTimerExpiry, Guards{WALStatusLost: true, SlotAlive: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.To != StateNeedsResnapshot {
		t.Fatalf("expected NeedsResnapshot, got %s", outcome.To)
	}
}

func TestPausedTimerExpiry_SlotAliveRoutesToResuming(t *testing.T) {
	outcome, err := Transition(StatePaused, EventTimerExpiry, Guards{SlotAlive: true, WALStatusLost: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if outcome.To != StateResuming {
		t.Fatalf("expected Resuming, got %s", outcome.To)
	}
}

func TestPausedTimerExpiry_NeitherAliveNorLost_IsAnError(t *testing.T) {
	_, err := Transition(StatePaused, EventTimerExpiry, Guards{SlotAlive: false, WALStatusLost: false})
	if err == nil {
		t.Fatal("expected an error when slot is neither alive nor confirmed lost")
	}
}

func TestFailedStart_RoutesByNeedsResnapshotGuard(t *testing.T) {
	outcome, err := Transition(StateFailed, EventStart, Guards{NeedsResnapshot: true})
	if err != nil || outcome.To != StateNeedsResnapshot {
		t.Fatalf("got (%s, %v), want (NeedsResnapshot, nil)", outcome.To, err)
	}
	outcome, err = Transition(StateFailed, EventStart, Guards{NeedsResnapshot: false})
	if err != nil || outcome.To != StateResuming {
		t.Fatalf("got (%s, %v), want (Resuming, nil)", outcome.To, err)
	}
}

func TestFailureFromNonTransientState_IsRejected(t *testing.T) {
	for _, s := range []State{StateRunning, StatePaused, StateStopped, StateNeedsResnapshot, StateFailed} {
		if _, err := Transition(s, EventFailure, Guards{}); err == nil {
			t.Fatalf("expected failure event from %s to be rejected", s)
		}
	}
}

func TestFailureFromTransientState_LandsInFailed(t *testing.T) {
	for _, s := range []State{StatePausing, StateStopping, StateSnapshotting, StateResuming} {
		outcome, err := Transition(s, EventFailure, Guards{})
		if err != nil {
			t.Fatalf("expected failure from transient state %s to succeed, got: %v", s, err)
		}
		if outcome.To != StateFailed {
			t.Fatalf("expected failure from %s to land in Failed, got %s", s, outcome.To)
		}
	}
}
