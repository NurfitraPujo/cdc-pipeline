package postgres

import (
	"context"
	"sync"
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
)

// TestAckManager covers the three contracts AckManager must uphold for the
// at-least-once delivery model: (1) in-order confirmations advance the
// contiguous watermark one LSN at a time, (2) gaps in the observed /
// confirmed stream hold the watermark at the last contiguous run, and
// (3) Confirm is safe under heavy concurrent use (no lost updates, no
// double advances).
func TestAckManager(t *testing.T) {
	t.Run("Empty manager starts at watermark zero", func(t *testing.T) {
		m := NewAckManager()
		assert.Equal(t, uint64(0), m.Watermark())
	})

	t.Run("In-order confirmations advance the contiguous watermark", func(t *testing.T) {
		m := NewAckManager()
		for lsn := uint64(1); lsn <= 10; lsn++ {
			m.Observe(lsn)
			wm := m.Confirm(lsn)
			assert.Equal(t, lsn, wm, "watermark must equal the most recent LSN after an in-order run")
		}
		assert.Equal(t, uint64(10), m.Watermark())
	})

	t.Run("Out-of-order confirmations do not advance past the gap", func(t *testing.T) {
		m := NewAckManager()
		// Observe a contiguous run of 1..5.
		for lsn := uint64(1); lsn <= 5; lsn++ {
			m.Observe(lsn)
		}
		// Confirm out of order: 3, then 2, then 5. The watermark must
		// stay at 0 because LSN 1 is still unconfirmed; 2, 3 and 5
		// cannot become "contiguous from the start" without LSN 1.
		assert.Equal(t, uint64(0), m.Confirm(3), "watermark must stay at 0 before LSN 1 is confirmed")
		assert.Equal(t, uint64(0), m.Confirm(2), "watermark must stay at 0 before LSN 1 is confirmed")
		assert.Equal(t, uint64(0), m.Confirm(5), "watermark must stay at 0 before LSN 1 is confirmed")
		// Confirming LSN 1 unblocks the contiguous run 1..3 (since 2
		// and 3 were already confirmed in earlier steps).
		assert.Equal(t, uint64(3), m.Confirm(1), "watermark must jump to 3 once LSN 1, 2 and 3 are all confirmed")
		// Confirming LSN 4 closes the remaining gap up to 5.
		assert.Equal(t, uint64(5), m.Confirm(4), "watermark must advance to 5 once the contiguous run 1..5 is confirmed")
		assert.Equal(t, uint64(5), m.Watermark())
	})

	t.Run("Gap in confirmations holds the watermark", func(t *testing.T) {
		m := NewAckManager()
		// Observe 1, 2, 3, 4, 5 then leave a gap and observe 100.
		for lsn := uint64(1); lsn <= 5; lsn++ {
			m.Observe(lsn)
		}
		m.Observe(100)

		// Confirm 1..5; the watermark should advance to 5 and then
		// stop because LSN 6..99 are missing.
		for lsn := uint64(1); lsn <= 5; lsn++ {
			m.Confirm(lsn)
		}
		assert.Equal(t, uint64(5), m.Watermark())

		// Confirming LSN 100 must not advance the watermark because the
		// contiguous run is still broken at 6.
		m.Confirm(100)
		assert.Equal(t, uint64(5), m.Watermark(), "watermark must not skip the gap even when a higher LSN is confirmed")

		// Filling the gap advances the watermark through the missing
		// LSNs (when the replication stream eventually redelivers them)
		// and on to 100.
		for lsn := uint64(6); lsn <= 100; lsn++ {
			m.Observe(lsn)
			m.Confirm(lsn)
		}
		assert.Equal(t, uint64(100), m.Watermark())
	})

	t.Run("Observe is a no-op for LSNs at or below the watermark", func(t *testing.T) {
		m := NewAckManager()
		// Build up a watermark of 10 via a fully-confirmed 1..10 run.
		for lsn := uint64(1); lsn <= 10; lsn++ {
			m.Observe(lsn)
			m.Confirm(lsn)
		}
		assert.Equal(t, uint64(10), m.Watermark())

		// Re-observing the same (already-confirmed) LSN must not
		// disturb the watermark.
		m.Observe(10)
		assert.Equal(t, uint64(10), m.Watermark())

		// Observing an LSN strictly below the watermark is also a
		// no-op for the watermark's perspective; this models the
		// replication stream redelivering an event after a crash.
		m.Observe(5)
		assert.Equal(t, uint64(10), m.Watermark())
	})

	t.Run("Hydrate fast-forwards the watermark past a persisted checkpoint", func(t *testing.T) {
		m := NewAckManager()
		m.Hydrate(500)
		assert.Equal(t, uint64(500), m.Watermark())

		// Hydrating with a smaller value is a no-op (monotonic).
		m.Hydrate(100)
		assert.Equal(t, uint64(500), m.Watermark())

		// After hydration, observing + confirming the next LSN advances
		// the watermark from the hydrated position.
		m.Observe(501)
		assert.Equal(t, uint64(501), m.Confirm(501))
	})

	t.Run("Confirm never decreases the watermark", func(t *testing.T) {
		m := NewAckManager()
		// Build up a watermark of 50 by confirming 1..50 in order.
		for lsn := uint64(1); lsn <= 50; lsn++ {
			m.Observe(lsn)
			m.Confirm(lsn)
		}
		assert.Equal(t, uint64(50), m.Watermark())

		// Confirming a smaller LSN must not move the watermark back.
		m.Confirm(10)
		assert.Equal(t, uint64(50), m.Watermark())
	})
}

// TestAckManager_ConcurrentConfirm stresses Confirm under many goroutines to
// ensure the internal mutex serialises updates correctly. The final
// watermark must equal the full contiguous range because every LSN has
// been observed and confirmed.
//
// Note: individual Confirm calls are not expected to return a watermark
// that is monotonic across goroutines — they observe at different points
// in time. The invariant we DO assert is that the AckManager's watermark
// never exceeds the largest observed LSN at any point (which would
// indicate a lost write) and that it eventually reaches the full
// contiguous range.
func TestAckManager_ConcurrentConfirm(t *testing.T) {
	m := NewAckManager()
	const total = 1000

	// Observe all LSNs up front so the only thing being tested is the
	// concurrent confirm path.
	for lsn := uint64(1); lsn <= total; lsn++ {
		m.Observe(lsn)
	}

	var wg sync.WaitGroup
	for lsn := uint64(1); lsn <= total; lsn++ {
		wg.Add(1)
		go func(target uint64) {
			defer wg.Done()
			wm := m.Confirm(target)
			// The watermark returned by a single Confirm call cannot
			// exceed the largest LSN that has been confirmed up to
			// that point in the lock-protected critical section. With
			// every LSN in [1, total] observed, the watermark is
			// bounded above by `total`.
			if wm > total {
				t.Errorf("watermark %d must not exceed the largest observed LSN %d", wm, total)
			}
		}(lsn)
	}
	wg.Wait()

	// Once every Confirm has returned, the final watermark must be the
	// full contiguous range (because all LSNs in [1, total] have been
	// confirmed).
	assert.Equal(t, uint64(total), m.Watermark(), "watermark must reach the full contiguous run after concurrent confirms")
}

// TestUpdateXLogPosPersistsCheckpoint is the small regression test for T1-2:
// UpdateXLogPos must advance s.lastCheckpoint.IngressLSN before forwarding
// the new position to the connector, so the in-memory checkpoint does not
// diverge from the value that was actually reported to PostgreSQL.
func TestUpdateXLogPosPersistsCheckpoint(t *testing.T) {
	s := NewPostgresSource("t1-2")
	// Pre-populate the lastCheckpoint with a non-zero baseline so we can
	// prove the assignment actually overwrites the existing value rather
	// than merely preserving it.
	s.lastCheckpoint = protocol.Checkpoint{IngressLSN: 99}

	if err := s.UpdateXLogPos(context.Background(), 42); err != nil {
		t.Fatalf("UpdateXLogPos returned unexpected error: %v", err)
	}

	if got, want := s.lastCheckpoint.IngressLSN, uint64(42); got != want {
		t.Fatalf("lastCheckpoint.IngressLSN = %d, want %d", got, want)
	}

	// A second call must continue advancing the in-memory checkpoint
	// (i.e. the assignment is not a one-shot).
	if err := s.UpdateXLogPos(context.Background(), 77); err != nil {
		t.Fatalf("UpdateXLogPos returned unexpected error: %v", err)
	}
	if got, want := s.lastCheckpoint.IngressLSN, uint64(77); got != want {
		t.Fatalf("lastCheckpoint.IngressLSN = %d, want %d (after second call)", got, want)
	}
}
