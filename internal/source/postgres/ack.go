package postgres

import (
	"sort"
	"sync"
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
//   - selfAcked marks LSNs that never leave the source (filtered/relation
//     events observed via ObserveConfirmed): they are fully confirmed the
//     moment they are observed and never wait on any sink.
type ackEntry struct {
	observed  int
	confirms  map[string]int
	selfAcked bool
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
// watermark: it must have been observed at least once, and (either it is
// self-acked, or every required sink has confirmed it at least `observed`
// times). Callers must hold a.mu.
func (a *AckManager) fullyConfirmedLocked(e *ackEntry) bool {
	if e.observed == 0 {
		// Confirmed only by ghosts (no matching Observe yet); never
		// eligible until the event is actually produced.
		return false
	}
	if e.selfAcked {
		return true
	}
	for _, sink := range a.required {
		if e.confirms[sink] < e.observed {
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
	if lsn <= a.watermark {
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
	if lsn <= a.watermark {
		return
	}
	e := a.entryLocked(lsn)
	e.observed++
	e.selfAcked = true
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
	a.watermark = serverWALEnd
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
