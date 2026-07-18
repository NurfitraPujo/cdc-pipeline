package postgres

import (
	"sync"
)

// AckManager tracks the lifecycle of CDC events flowing from the PostgreSQL
// replication slot into the pipeline. It is the single source of truth for
// "which LSNs have been confirmed by downstream consumers", and the value
// returned by Watermark is what is ultimately flushed back to PostgreSQL via
// SendStandbyStatusUpdate.
//
// PostgresSource guarantees an at-least-once delivery contract: a CDC event
// is only considered durably consumed once it has been confirmed by the
// downstream engine (which in turn guarantees a successful NATS JetStream
// publish / sink write). The AckManager enforces this contract by only
// advancing the watermark after the contiguous run of LSNs has been
// confirmed, so that a crash mid-batch will replay unconfirmed events on
// restart rather than dropping them silently.
//
// All exported methods are safe for concurrent use.
type AckManager struct {
	mu        sync.Mutex
	pending   map[uint64]bool // LSN -> confirmed (true) or in-flight (false)
	watermark uint64          // Highest contiguous confirmed LSN
}

// NewAckManager returns an AckManager ready to track observed LSNs.
// The initial watermark is zero; callers should hydrate it from a
// persisted checkpoint before observing new LSNs if a resume is desired.
func NewAckManager() *AckManager {
	return &AckManager{pending: make(map[uint64]bool)}
}

// Observe registers an LSN as in-flight: the replication stream has produced
// the message but the downstream pipeline has not yet confirmed it. Observing
// an LSN that is already pending or already past the watermark is a no-op
// from the watermark's perspective, but the entry is preserved until the
// contiguous run reaches it.
//
// Observe does NOT advance the watermark. The watermark only advances after
// Confirm is called for each LSN in the contiguous run.
func (a *AckManager) Observe(lsn uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if lsn <= a.watermark {
		// Already durably acknowledged; nothing to track.
		return
	}
	if _, ok := a.pending[lsn]; !ok {
		a.pending[lsn] = false
	}
}

// Confirm marks an LSN as confirmed by the downstream pipeline and returns
// the highest contiguous confirmed LSN after the confirmation. The watermark
// only advances when the entire contiguous run up to and including the
// highest in-flight LSN has been confirmed; this preserves the at-least-once
// contract in the presence of out-of-order or gapped LSN delivery.
//
// Confirming an LSN that was not previously observed records it as confirmed
// and attempts to advance the watermark as far as possible.
func (a *AckManager) Confirm(lsn uint64) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pending[lsn] = true
	for {
		next := a.watermark + 1
		acked, ok := a.pending[next]
		if !ok || !acked {
			break
		}
		a.watermark = next
		delete(a.pending, next)
	}
	return a.watermark
}

// Watermark returns the highest contiguous confirmed LSN. The watermark is
// the value that should be reported to PostgreSQL via SendStandbyStatusUpdate
// to advance the replication slot. Watermark never decreases.
func (a *AckManager) Watermark() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.watermark
}

// Hydrate sets the watermark directly to the supplied LSN, bypassing the
// contiguous-run rule used by Confirm. This is intended for resuming from a
// persisted checkpoint: the checkpoint is, by definition, the last LSN
// already durably acknowledged, so it is safe to fast-forward the watermark
// past it on startup.
//
// Hydrate is a no-op when the supplied value is less than or equal to the
// current watermark, since the watermark is monotonic. The pending map is
// cleared so any stale entries from a previous lifecycle do not interfere
// with future Confirm calls.
func (a *AckManager) Hydrate(watermark uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if watermark <= a.watermark {
		return
	}
	a.watermark = watermark
	a.pending = make(map[uint64]bool)
}
