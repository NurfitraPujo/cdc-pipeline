package postgres

import (
	"sort"
	"sync"

	"github.com/rs/zerolog/log"
)

// ackEntry tracks the confirmation state of a single observed LSN.
//
//   - observed counts how many times the replication callback has produced
//     this LSN (normally 1, but the vendored library can rewrite the last
//     message of a transaction to the commit LSN, so more than one event
//     may legitimately share an LSN — see plan §7 Q2).
//   - confirms maps sinkID -> number of times that sink has durably
//     written this LSN. A sink's confirms must reach `observed` before its
//     vote counts toward the watermark.
//   - selfAcked counts how many of those observations never leave the source
//     (filtered/relation events observed via ObserveConfirmed): each is
//     confirmed the moment it is observed and never waits on any sink.
//
// selfAcked is a COUNTER, not a flag, and this is load-bearing. An LSN can
// carry both a filtered event and a data event — the vendored library
// rewrites the last message of a transaction to the commit LSN, so a
// transaction touching both a tracked and an untracked table collides on it.
// While selfAcked was a bool, one filtered observation flipped the whole
// entry to "fully confirmed" and the watermark sailed past a data row no
// sink had written: the replication slot advanced past unacknowledged data,
// which is the exact loss this plan exists to prevent. Caught by
// TestKeepaliveDoesNotConfirmInflight once real WAL churn on an untracked
// table was running alongside a blocked sink.
type ackEntry struct {
	observed  int
	confirms  map[string]int
	selfAcked int
}

// AckManager tracks the lifecycle of CDC events flowing from the PostgreSQL
// replication slot into the pipeline. It is the single source of truth for
// "which LSNs have been confirmed by every required sink", and the value
// returned by Watermark is what is ultimately flushed back to PostgreSQL via
// SendStandbyStatusUpdate.
//
// PostgresSource guarantees an at-least-once delivery contract: a CDC event
// is only considered durably consumed once every sink in `required` has
// confirmed it has durably written it. The AckManager enforces this
// contract by only advancing the watermark after the contiguous run of
// LSNs has been fully confirmed, so that a crash mid-batch will replay
// unconfirmed events on restart rather than dropping them silently.
//
// Confirm-before-Observe (plan §7 Q3): because RecordAcks are redelivered
// after a producer restart, a Confirm for an LSN can arrive before the
// corresponding Observe (the replay of the event itself, which re-Observes
// it). Confirm-for-unknown-lsn therefore creates an entry with
// observed == 0, and such an entry is NEVER eligible for watermark
// inclusion until Observe raises observed to at least 1 and every
// required sink's confirm count catches up. This guarantees
// Confirm-then-Observe and Observe-then-Confirm converge on the same
// watermark, and an entry confirmed only by "ghosts" (no observation)
// can never advance the watermark.
//
// All exported methods are safe for concurrent use.
type AckManager struct {
	mu sync.Mutex
	// required is the set of sink IDs whose confirmation is required
	// before an LSN's entry is considered fully confirmed. Supplied once
	// at construction (from Start's ackers argument) and treated as
	// immutable thereafter — see plan §7 Q1.
	required  []string
	pending   map[uint64]*ackEntry
	lsns      []uint64 // sorted slice of observed-or-confirmed, not-yet-watermarked LSNs
	watermark uint64

	// highestSeen is the maximum LSN ever passed to Observe or ObserveConfirmed,
	// and hasSeenData is whether that has ever happened at least once. Together
	// they are IdleAdvance's defence-in-depth guard against fast-forwarding past
	// a replay backlog that has not actually reached Observe yet - see IdleAdvance.
	highestSeen uint64
	hasSeenData bool
	// idleTrusted gates how strictly IdleAdvance enforces the highestSeen check.
	// It starts false and is re-armed (set back to false) every time a new LSN
	// becomes pending (len(lsns) goes 0 -> >0) -- i.e. every time real backlog
	// shows up that could, under a regression of the T0-3 in-band-keepalive
	// ordering fix, race a keepalive past undelivered data.
	//
	// The first IdleAdvance attempted after such backlog fully drains is
	// checked against highestSeen: if it lands within range, it succeeds and
	// idleTrusted latches true directly. If instead it asks to jump past
	// highestSeen, that single attempt is refused and logged loudly as a
	// canary -- exactly the signal a T0-3 ordering regression would trip --
	// but idleTrusted ALSO latches true at that point, so it is a one-time
	// warning, not a standing block. This is deliberate: nothing in
	// AckManager can distinguish "a keepalive is racing ahead of decoded,
	// not-yet-Observed data" (dangerous - the T0-3 bug shape) from "WAL is
	// advancing on an untracked table whose transactions never produce an
	// Observe/ObserveConfirmed call at all" (safe, and exactly what
	// IdleAdvance exists to fast-forward past for WAL-bloat protection) --
	// only the *ordering guarantee* T0-3 provides tells them apart, and that
	// guarantee, once true, holds for every subsequent call. Latching after
	// one logged refusal keeps the guard from permanently defeating
	// legitimate WAL-bloat protection on a fully-acked, idle pipeline while
	// still surfacing a regression the moment it (re)appears.
	idleTrusted bool

	// belowWatermarkDrops counts Observe calls for an lsn <= watermark. In a
	// correct system this is unreachable: it means the replication slot has
	// already durably confirmed an LSN whose data event is only now arriving,
	// i.e. data that should have been protected by the watermark was not. See
	// Observe.
	belowWatermarkDrops uint64
}

// NewAckManager returns an AckManager ready to track observed LSNs, gated
// on confirmation from every sink ID in requiredSinks. The initial
// watermark is zero; callers should Hydrate it from a persisted checkpoint
// before observing new LSNs if a resume is desired.
func NewAckManager(requiredSinks []string) *AckManager {
	required := make([]string, len(requiredSinks))
	copy(required, requiredSinks)
	return &AckManager{
		required: required,
		pending:  make(map[uint64]*ackEntry),
	}
}

// entryLocked returns the entry for lsn, creating it if necessary. Callers
// must hold a.mu.
func (a *AckManager) entryLocked(lsn uint64) *ackEntry {
	e, ok := a.pending[lsn]
	if !ok {
		e = &ackEntry{confirms: make(map[string]int)}
		a.pending[lsn] = e
		a.insertSortedLocked(lsn)
	}
	return e
}

// insertSortedLocked inserts lsn into the sorted a.lsns slice if it is not
// already present. Callers must hold a.mu.
func (a *AckManager) insertSortedLocked(lsn uint64) {
	if len(a.lsns) == 0 {
		// New backlog just appeared: re-arm the idle-advance guard so the
		// first IdleAdvance attempted once this drains again must fall
		// within [watermark, highestSeen]. See idleTrusted's doc comment.
		a.idleTrusted = false
	}
	n := len(a.lsns)
	if n == 0 || lsn > a.lsns[n-1] {
		a.lsns = append(a.lsns, lsn)
		return
	}
	i := sort.Search(len(a.lsns), func(i int) bool { return a.lsns[i] >= lsn })
	if i < len(a.lsns) && a.lsns[i] == lsn {
		return
	}
	a.lsns = append(a.lsns, 0)
	copy(a.lsns[i+1:], a.lsns[i:])
	a.lsns[i] = lsn
}

// fullyConfirmedLocked reports whether entry e is eligible for the
// watermark. Each observation of an LSN is accounted for exactly once:
// selfAcked covers the filtered ones (which never reach a sink), and the
// remainder must be confirmed by EVERY required sink.
//
// Deliberately not a short-circuit on selfAcked: a single filtered event
// sharing an LSN with a data event must not vouch for the data event.
// Callers must hold a.mu.
func (a *AckManager) fullyConfirmedLocked(e *ackEntry) bool {
	if e.observed == 0 {
		// Confirmed only by ghosts (no matching Observe yet); never
		// eligible until the event is actually produced.
		return false
	}
	// Observations still awaiting a downstream write. Filtered events are
	// already accounted for by selfAcked and are subtracted out.
	needed := e.observed - e.selfAcked
	if needed <= 0 {
		// Every observation of this LSN was filtered at the source, so no
		// sink will ever see it.
		return true
	}
	for _, sink := range a.required {
		if e.confirms[sink] < needed {
			return false
		}
	}
	return true
}

// advanceLocked consumes fully-confirmed LSNs from the front of the sorted
// slice, advancing the watermark as far as the contiguous run allows.
// Callers must hold a.mu.
func (a *AckManager) advanceLocked() {
	for len(a.lsns) > 0 {
		oldest := a.lsns[0]
		e, ok := a.pending[oldest]
		if !ok || !a.fullyConfirmedLocked(e) {
			break
		}
		a.watermark = oldest
		delete(a.pending, oldest)
		a.lsns = a.lsns[1:]
	}
}

// Observe registers an LSN as produced by the replication stream: the
// callback has emitted (or re-emitted, on replay) the corresponding data
// event. Observe increments the entry's observed counter, which is what
// makes it eligible for watermark inclusion once every required sink's
// confirm count catches up. Observe alone never advances the watermark
// unless previously-recorded confirms now satisfy the entry.
func (a *AckManager) Observe(lsn uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hasSeenData = true
	if lsn > a.highestSeen {
		a.highestSeen = lsn
	}
	if lsn <= a.watermark {
		// Unreachable in a correct system: an Observe below the watermark means
		// the slot already told PostgreSQL this LSN (and everything before it)
		// is durably confirmed, yet its data event is only now arriving - i.e.
		// unacknowledged data was reported acknowledged. This branch being loud
		// is precisely what would have surfaced the IdleAdvance-past-backlog bug
		// (T0-3) instead of it hiding behind a healthy-looking pending-count
		// metric. Do not silence this without addressing why it fired.
		a.belowWatermarkDrops++
		log.Error().
			Uint64("lsn", lsn).
			Uint64("watermark", a.watermark).
			Uint64("belowWatermarkDrops", a.belowWatermarkDrops).
			Msg("AckManager.Observe: dropped an LSN at or below the watermark - data may have been lost")
		return
	}
	e := a.entryLocked(lsn)
	e.observed++
	a.advanceLocked()
}

// ObserveConfirmed registers an LSN that never leaves the source (filtered
// events: relation messages, unmatched tables, non-data snapshot events).
// These self-ack immediately so they cannot stall the watermark waiting on
// a downstream sink that will never see them.
func (a *AckManager) ObserveConfirmed(lsn uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hasSeenData = true
	if lsn > a.highestSeen {
		a.highestSeen = lsn
	}
	if lsn <= a.watermark {
		// Benign and left quiet: filtered/relation events legitimately arrive
		// below the watermark (e.g. replayed after a restart). Unlike Observe's
		// equivalent branch, this is not evidence of lost data.
		return
	}
	e := a.entryLocked(lsn)
	e.observed++
	// Count this observation as self-acked rather than flagging the whole
	// entry: a data event may share this LSN and must still wait on its
	// sinks. See the ackEntry doc comment.
	e.selfAcked++
	a.advanceLocked()
}

// Confirm marks an LSN as durably written by sinkID and returns the
// highest contiguous fully-confirmed LSN after the confirmation.
//
// Confirm for an lsn <= the current watermark is a no-op: this makes
// redelivered RecordAcks (after a producer restart) idempotent.
//
// Confirm for an unknown (not-yet-observed) lsn creates an entry with
// observed == 0 and records the confirm; per the state machine documented
// on AckManager, that entry is NOT eligible for the watermark until a
// matching Observe arrives.
func (a *AckManager) Confirm(lsn uint64, sinkID string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if lsn <= a.watermark {
		return a.watermark
	}
	e := a.entryLocked(lsn)
	e.confirms[sinkID]++
	a.advanceLocked()
	return a.watermark
}

// Watermark returns the highest contiguous fully-confirmed LSN. The
// watermark is the value that should be reported to PostgreSQL via
// SendStandbyStatusUpdate to advance the replication slot. Watermark never
// decreases.
func (a *AckManager) Watermark() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.watermark
}

// PendingCount returns the number of observed-or-confirmed LSNs not yet
// folded into the watermark. IdleAdvance uses this to decide whether it is
// safe to fast-forward.
func (a *AckManager) PendingCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.lsns)
}

// IdleAdvance fast-forwards the watermark to serverWALEnd IFF nothing is
// pending. This is the ONLY sanctioned fast-forward; it reinstates
// keepalive-driven slot advancement for idle streams (WAL-bloat
// protection) without reintroducing the eager-advance bug it replaces:
// there is nothing in flight to skip past. IdleAdvance never regresses the
// watermark and reports whether it actually advanced it.
func (a *AckManager) IdleAdvance(serverWALEnd uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.lsns) > 0 {
		return false
	}
	if serverWALEnd <= a.watermark {
		return false
	}
	// Defence-in-depth against T0-3: len(a.lsns) == 0 is only a true statement
	// about "nothing in flight" if the stream delivers keepalives strictly
	// after every preceding decoded message has reached Observe (guaranteed by
	// the T0-3 vendored patch). If some future change to the vendored stream
	// regresses that ordering, len(a.lsns) == 0 can once again mean "the
	// replay backlog hasn't reached Observe yet" rather than "there is no
	// backlog" - and IdleAdvance would silently fast-forward past it exactly
	// as it did before T0-3.
	//
	// Once any data has ever flowed (hasSeenData), refuse to advance past the
	// highest LSN actually Observe()'d: serverWALEnd > highestSeen means the
	// primary claims WAL exists beyond anything this process has seen through
	// Observe, which is precisely the unsafe fast-forward-past-backlog shape.
	//
	// A slot that has NEVER seen any traffic (hasSeenData == false) is exempt:
	// refusing to advance there would defeat the whole point of IdleAdvance -
	// WAL-bloat protection on a genuinely idle slot with nothing to lose.
	if a.hasSeenData && !a.idleTrusted && serverWALEnd > a.highestSeen {
		log.Error().
			Uint64("serverWALEnd", serverWALEnd).
			Uint64("highestSeen", a.highestSeen).
			Uint64("watermark", a.watermark).
			Msg("AckManager.IdleAdvance: refusing to fast-forward past the highest Observe()'d LSN " +
				"(first attempt after backlog drained) - retrying on the next keepalive")
		// Latch trust now, even though THIS call is refused: the point of the
		// guard is to prove, once, that a keepalive-driven jump beyond
		// highestSeen was actually observed happening (the canary a future
		// T0-3 ordering regression would trip), not to block forever - see
		// idleTrusted's doc comment. Blocking every call would starve
		// WAL-bloat protection on a legitimately idle pipeline whose ongoing
		// WAL activity (e.g. an untracked table) never produces an Observe.
		a.idleTrusted = true
		return false
	}
	a.watermark = serverWALEnd
	// Latch trust: this call landed at or below highestSeen (or hasSeenData
	// was false / already trusted), so subsequent calls - which, per T0-3,
	// only ever arrive in correctly-ordered fashion - no longer need to prove
	// themselves against highestSeen until new backlog resets idleTrusted.
	a.idleTrusted = true
	return true
}

// Hydrate sets the watermark directly to the supplied LSN, bypassing the
// contiguous-run rule used by Confirm/Observe. This is intended for
// resuming from a persisted checkpoint: the checkpoint is, by definition,
// the last LSN already durably acknowledged, so it is safe to fast-forward
// the watermark past it on startup.
//
// Hydrate is a no-op when the supplied value is less than or equal to the
// current watermark, since the watermark is monotonic. The pending map is
// cleared so any stale entries from a previous lifecycle do not interfere
// with future Confirm/Observe calls.
func (a *AckManager) Hydrate(watermark uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if watermark <= a.watermark {
		return
	}
	a.watermark = watermark
	a.pending = make(map[uint64]*ackEntry)
	a.lsns = nil
}
