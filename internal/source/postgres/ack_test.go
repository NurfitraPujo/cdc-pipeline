package postgres

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
)

// TestAckManager covers the single-sink contracts AckManager must uphold
// for the at-least-once delivery model: (1) in-order confirmations advance
// the contiguous watermark one LSN at a time, (2) gaps in the observed /
// confirmed stream hold the watermark at the last contiguous run, and
// (3) Confirm is safe under heavy concurrent use (no lost updates, no
// double advances). A single required sink ("a") is used throughout so
// each Confirm behaves like the old single-sink API.
func TestAckManager(t *testing.T) {
	t.Run("Empty manager starts at watermark zero", func(t *testing.T) {
		m := NewAckManager([]string{"a"})
		assert.Equal(t, uint64(0), m.Watermark())
	})

	t.Run("In-order confirmations advance the contiguous watermark", func(t *testing.T) {
		m := NewAckManager([]string{"a"})
		for lsn := uint64(1); lsn <= 10; lsn++ {
			m.Observe(lsn)
			wm := m.Confirm(lsn, "a")
			assert.Equal(t, lsn, wm, "watermark must equal the most recent LSN after an in-order run")
		}
		assert.Equal(t, uint64(10), m.Watermark())
	})

	t.Run("Out-of-order confirmations do not advance past the gap", func(t *testing.T) {
		m := NewAckManager([]string{"a"})
		for lsn := uint64(1); lsn <= 5; lsn++ {
			m.Observe(lsn)
		}
		assert.Equal(t, uint64(0), m.Confirm(3, "a"), "watermark must stay at 0 before LSN 1 is confirmed")
		assert.Equal(t, uint64(0), m.Confirm(2, "a"), "watermark must stay at 0 before LSN 1 is confirmed")
		assert.Equal(t, uint64(0), m.Confirm(5, "a"), "watermark must stay at 0 before LSN 1 is confirmed")
		assert.Equal(t, uint64(3), m.Confirm(1, "a"), "watermark must jump to 3 once LSN 1, 2 and 3 are all confirmed")
		assert.Equal(t, uint64(5), m.Confirm(4, "a"), "watermark must advance to 5 once the contiguous run 1..5 is confirmed")
		assert.Equal(t, uint64(5), m.Watermark())
	})

	t.Run("Gap in confirmations holds the watermark", func(t *testing.T) {
		m := NewAckManager([]string{"a"})
		for lsn := uint64(1); lsn <= 100; lsn++ {
			m.Observe(lsn)
		}

		for lsn := uint64(1); lsn <= 5; lsn++ {
			m.Confirm(lsn, "a")
		}
		assert.Equal(t, uint64(5), m.Watermark())

		m.Confirm(100, "a")
		assert.Equal(t, uint64(5), m.Watermark(), "watermark must not skip the gap even when a higher LSN is confirmed")

		for lsn := uint64(6); lsn <= 100; lsn++ {
			m.Confirm(lsn, "a")
		}
		assert.Equal(t, uint64(100), m.Watermark())
	})

	t.Run("Observe is a no-op for LSNs at or below the watermark", func(t *testing.T) {
		m := NewAckManager([]string{"a"})
		for lsn := uint64(1); lsn <= 10; lsn++ {
			m.Observe(lsn)
			m.Confirm(lsn, "a")
		}
		assert.Equal(t, uint64(10), m.Watermark())

		m.Observe(10)
		assert.Equal(t, uint64(10), m.Watermark())

		m.Observe(5)
		assert.Equal(t, uint64(10), m.Watermark())
	})

	t.Run("Hydrate fast-forwards the watermark past a persisted checkpoint", func(t *testing.T) {
		m := NewAckManager([]string{"a"})
		m.Hydrate(500)
		assert.Equal(t, uint64(500), m.Watermark())

		m.Hydrate(100)
		assert.Equal(t, uint64(500), m.Watermark())

		m.Observe(501)
		assert.Equal(t, uint64(501), m.Confirm(501, "a"))
	})

	t.Run("Confirm never decreases the watermark", func(t *testing.T) {
		m := NewAckManager([]string{"a"})
		for lsn := uint64(1); lsn <= 50; lsn++ {
			m.Observe(lsn)
			m.Confirm(lsn, "a")
		}
		assert.Equal(t, uint64(50), m.Watermark())

		m.Confirm(10, "a")
		assert.Equal(t, uint64(50), m.Watermark())
	})
}

// TestAckManager_ConcurrentConfirm stresses Confirm under many goroutines to
// ensure the internal mutex serialises updates correctly. The final
// watermark must equal the full contiguous range because every LSN has
// been observed and confirmed by the sole required sink.
//
// Unlike the previous version of this test, we do not assert anything
// about the raw return value of an individual Confirm call from inside the
// goroutine: with `total` LSNs pre-observed, every Confirm call's return is
// trivially <= total (Watermark can never exceed the highest LSN present
// in a.lsns/pending in the first place), so `wm > total` could never fire
// — a vacuous assertion. Instead we assert the property concurrency could
// actually break: the watermark is monotonic non-decreasing across every
// observed read, and the final watermark reaches the full contiguous
// range.
func TestAckManager_ConcurrentConfirm(t *testing.T) {
	m := NewAckManager([]string{"a"})
	const total = 1000

	for lsn := uint64(1); lsn <= total; lsn++ {
		m.Observe(lsn)
	}

	// Monotonicity must be sampled by a SINGLE observer. Comparing the values
	// returned by Confirm across goroutines proves nothing: each return is a
	// snapshot taken under the manager's lock, and two goroutines can reach the
	// comparison in an order unrelated to the order they took that snapshot in
	// (one that read 1 may be descheduled while another reads and records 22).
	// An earlier revision of this test did exactly that and failed spuriously
	// with "watermark regressed" — the manager was never at fault.
	var wg sync.WaitGroup
	var samplerWg sync.WaitGroup
	stop := make(chan struct{})
	var regressed atomic.Bool
	var sawLow, sawHigh uint64

	samplerWg.Add(1)
	go func() {
		defer samplerWg.Done()
		prev := uint64(0)
		for {
			wm := m.Watermark()
			if wm < prev {
				sawLow, sawHigh = wm, prev
				regressed.Store(true)
				return
			}
			prev = wm
			select {
			case <-stop:
				return
			default:
			}
		}
	}()

	for lsn := uint64(1); lsn <= total; lsn++ {
		wg.Add(1)
		go func(target uint64) {
			defer wg.Done()
			_ = m.Confirm(target, "a")
		}(lsn)
	}
	wg.Wait()
	close(stop)
	samplerWg.Wait()

	if regressed.Load() {
		t.Errorf("watermark regressed: observed %d after %d", sawLow, sawHigh)
	}
	assert.Equal(t, uint64(total), m.Watermark(), "watermark must reach the full contiguous run after concurrent confirms")
}

func TestAckManager_GappedLSNs(t *testing.T) {
	m := NewAckManager([]string{"a"})

	gaps := []uint64{100, 250, 310, 480}
	for _, lsn := range gaps {
		m.Observe(lsn)
	}

	assert.Equal(t, uint64(100), m.Confirm(100, "a"))
	assert.Equal(t, uint64(100), m.Confirm(310, "a"))
	assert.Equal(t, uint64(310), m.Confirm(250, "a"))
	assert.Equal(t, uint64(480), m.Confirm(480, "a"))
}

// --- plan §5 tests 1-5, multi-sink -----------------------------------------

// Test 1: multi-sink gating. Every required sink must confirm an LSN
// before the watermark can pass it.
func TestAckManager_MultiSink_RequiresEverySink(t *testing.T) {
	m := NewAckManager([]string{"a", "b"})
	m.Observe(100)
	m.Observe(200)
	m.Observe(300)

	assert.Equal(t, uint64(0), m.Confirm(100, "a"), "sink b has not confirmed yet")
	assert.Equal(t, uint64(100), m.Confirm(100, "b"), "both sinks confirmed 100")

	m.Confirm(300, "a")
	m.Confirm(300, "b")
	assert.Equal(t, uint64(100), m.Watermark(), "gap at 200 must hold the watermark")

	m.Confirm(200, "a")
	m.Confirm(200, "b")
	assert.Equal(t, uint64(300), m.Watermark(), "filling the gap unblocks the run through 300")
}

// Test 2: multiplicity. An LSN observed twice (e.g. two events sharing a
// txn-commit LSN) requires each required sink to confirm it twice.
func TestAckManager_Multiplicity(t *testing.T) {
	m := NewAckManager([]string{"a", "b"})
	m.Observe(100)
	m.Observe(100)

	assert.Equal(t, uint64(0), m.Confirm(100, "a"))
	assert.Equal(t, uint64(0), m.Confirm(100, "b"), "each sink has only confirmed once, but observed twice")

	assert.Equal(t, uint64(0), m.Confirm(100, "a"))
	assert.Equal(t, uint64(100), m.Confirm(100, "b"), "second confirm from both sinks satisfies the multiplicity")
}

// Test 3: ObserveConfirmed. Filtered LSNs interleaved among data LSNs pass
// the watermark without any Confirm call.
func TestAckManager_ObserveConfirmed(t *testing.T) {
	m := NewAckManager([]string{"a"})

	m.Observe(100)
	m.ObserveConfirmed(150) // filtered event between two data events
	m.Observe(200)

	assert.Equal(t, uint64(0), m.Watermark(), "100 is still unconfirmed by sink a")

	assert.Equal(t, uint64(150), m.Confirm(100, "a"), "confirming 100 should sweep through the self-acked 150")

	assert.Equal(t, uint64(200), m.Confirm(200, "a"))
}

// Test 4: IdleAdvance. No advance while anything is pending; advances to
// serverWALEnd when idle; never regresses.
func TestAckManager_IdleAdvance(t *testing.T) {
	m := NewAckManager([]string{"a"})

	m.Observe(100)
	assert.False(t, m.IdleAdvance(500), "must not advance while an LSN is pending")
	assert.Equal(t, uint64(0), m.Watermark())

	m.Confirm(100, "a")
	assert.Equal(t, uint64(100), m.Watermark())

	// serverWALEnd (500) is beyond the highest LSN ever Observe()'d (100), so
	// this first post-drain attempt is refused and logged as a canary: see
	// TestAckManager_IdleAdvance_RefusesPastHighestSeen and idleTrusted's doc
	// comment for the T0-3 defence-in-depth this guards against.
	assert.False(t, m.IdleAdvance(500), "first post-drain attempt past highestSeen is refused")
	assert.Equal(t, uint64(100), m.Watermark())

	// That single refusal latches idleTrusted, so subsequent calls are no
	// longer checked against highestSeen - see idleTrusted's doc comment for
	// why permanently blocking here would defeat WAL-bloat protection.
	assert.True(t, m.IdleAdvance(500), "trusted after the one-time canary refusal: must fast-forward")
	assert.Equal(t, uint64(500), m.Watermark())

	assert.False(t, m.IdleAdvance(300), "must never regress the watermark")
	assert.Equal(t, uint64(500), m.Watermark())

	assert.False(t, m.IdleAdvance(500), "advancing to the same value again reports no advance")
}

// TestAckManager_IdleAdvance_RefusesPastHighestSeen is the defence-in-depth
// guard added alongside T0-3: even with the vendored stream fixed to deliver
// keepalives strictly after every preceding decoded message reaches Observe,
// IdleAdvance itself must refuse a fast-forward beyond anything it has
// actually seen once data has ever flowed, while still allowing WAL-bloat
// protection to fast-forward a slot that has never seen any traffic at all.
func TestAckManager_IdleAdvance_RefusesPastHighestSeen(t *testing.T) {
	t.Run("refuses to advance past highestSeen once data has flowed", func(t *testing.T) {
		m := NewAckManager([]string{"a"})

		m.Observe(100)
		m.Confirm(100, "a")
		assert.Equal(t, uint64(100), m.Watermark())
		assert.Equal(t, 0, m.PendingCount(), "nothing pending per the (unsound-in-isolation) len(lsns)==0 guard")

		// A keepalive claiming WAL exists far beyond anything Observe()'d must
		// be refused on its first occurrence after backlog drains: this is
		// the exact shape of the T0-3 data-loss bug (IdleAdvance
		// fast-forwarding past an un-Observe()'d replay backlog).
		assert.False(t, m.IdleAdvance(999_999), "must refuse: serverWALEnd is beyond highestSeen")
		assert.Equal(t, uint64(100), m.Watermark(), "watermark must not have moved")

		// New backlog arriving re-arms the guard for the next drain.
		m.Observe(200)
		assert.False(t, m.IdleAdvance(999_999), "must refuse: an LSN is pending again")
		m.Confirm(200, "a")
		assert.Equal(t, uint64(200), m.Watermark())

		// This time the ask (999_999) is still beyond the new highestSeen
		// (200), so it is refused once more - the guard re-checks every time
		// fresh backlog appears and drains, it is not a single lifetime grant.
		assert.False(t, m.IdleAdvance(999_999), "must refuse again: re-armed after new backlog drained")
		assert.Equal(t, uint64(200), m.Watermark())

		// And, per the one-time-canary latch, the very next call is trusted.
		assert.True(t, m.IdleAdvance(999_999), "trusted after the second canary refusal")
		assert.Equal(t, uint64(999_999), m.Watermark())
	})

	t.Run("still advances a genuinely idle slot that has seen nothing", func(t *testing.T) {
		m := NewAckManager([]string{"a"})

		assert.Equal(t, 0, m.PendingCount())
		assert.True(t, m.IdleAdvance(12345), "a slot with no traffic at all must still get WAL-bloat protection")
		assert.Equal(t, uint64(12345), m.Watermark())
	})
}

// Test 5: Confirm idempotency below the watermark models AcksTopic
// redelivery after a restart.
func TestAckManager_ConfirmIdempotentBelowWatermark(t *testing.T) {
	m := NewAckManager([]string{"a"})
	m.Hydrate(500)

	assert.Equal(t, uint64(500), m.Confirm(400, "a"), "confirm below the watermark is a no-op")
	assert.Equal(t, uint64(500), m.Watermark())
	assert.Equal(t, 0, m.PendingCount(), "no entry should have been created for the stale confirm")
}

// --- plan §7 Q3: confirm-before-observe state machine -----------------------

// TestAckManager_ConfirmBeforeObserve_OrderIndependence is the invariant
// mandated by plan §7 Open Question 3: a Confirm-before-Observe sequence
// must yield the same watermark as Observe-then-Confirm, and an entry that
// is confirmed but never observed must never advance the watermark on its
// own.
func TestAckManager_ConfirmBeforeObserve_OrderIndependence(t *testing.T) {
	t.Run("confirm-then-observe matches observe-then-confirm", func(t *testing.T) {
		// Sequence A: Observe first, then Confirm (the "normal" order).
		a := NewAckManager([]string{"x", "y"})
		a.Observe(100)
		a.Confirm(100, "x")
		a.Confirm(100, "y")
		wantA := a.Watermark()

		// Sequence B: Confirm arrives first (redelivered RecordAck from a
		// crashed producer), then the replay re-Observes the LSN.
		b := NewAckManager([]string{"x", "y"})
		b.Confirm(100, "x")
		b.Confirm(100, "y")
		// Not yet observed: must not have advanced.
		assert.Equal(t, uint64(0), b.Watermark(), "a confirmed-but-unobserved entry must not advance the watermark")
		b.Observe(100)
		wantB := b.Watermark()

		assert.Equal(t, wantA, wantB, "confirm-before-observe must converge to the same watermark as observe-before-confirm")
		assert.Equal(t, uint64(100), wantB)
	})

	t.Run("ghost confirms never advance the watermark without a matching observe", func(t *testing.T) {
		m := NewAckManager([]string{"x"})
		// LSN 100 is confirmed by a redelivered ack for an event the
		// current session has not (yet, or ever) produced.
		wm := m.Confirm(100, "x")
		assert.Equal(t, uint64(0), wm)
		assert.Equal(t, uint64(0), m.Watermark())
		// Even repeated ghost confirms change nothing.
		m.Confirm(100, "x")
		assert.Equal(t, uint64(0), m.Watermark())
	})

	t.Run("multiplicity interacts correctly with confirm-before-observe", func(t *testing.T) {
		m := NewAckManager([]string{"x"})
		m.Confirm(100, "x") // ghost confirm, observed still 0
		m.Observe(100)      // observed = 1; confirms[x] = 1 -> satisfied
		assert.Equal(t, uint64(100), m.Watermark(), "single observe should already be satisfied by the one confirm on record")

		m2 := NewAckManager([]string{"x"})
		m2.Confirm(200, "x") // observed=0, confirms[x]=1
		m2.Observe(200)      // observed=1, confirms[x]=1 -> satisfied, watermark advances
		assert.Equal(t, uint64(200), m2.Watermark())
		// A further Observe(200) after 200 has already been folded into
		// the watermark is a no-op (lsn <= watermark).
		m2.Observe(200)
		assert.Equal(t, uint64(200), m2.Watermark())
	})
}

// TestUpdateXLogPosPersistsCheckpoint is the small regression test for T1-2:
// UpdateXLogPos must advance s.lastCheckpoint.IngressLSN before forwarding
// the new position to the connector, so the in-memory checkpoint does not
// diverge from the value that was actually reported to PostgreSQL.
func TestUpdateXLogPosPersistsCheckpoint(t *testing.T) {
	s := NewPostgresSource("t1-2")
	s.lastCheckpoint = protocol.Checkpoint{IngressLSN: 99}

	if err := s.UpdateXLogPos(context.Background(), 42); err != nil {
		t.Fatalf("UpdateXLogPos returned unexpected error: %v", err)
	}

	if got, want := s.lastCheckpoint.IngressLSN, uint64(42); got != want {
		t.Fatalf("lastCheckpoint.IngressLSN = %d, want %d", got, want)
	}

	if err := s.UpdateXLogPos(context.Background(), 77); err != nil {
		t.Fatalf("UpdateXLogPos returned unexpected error: %v", err)
	}
	if got, want := s.lastCheckpoint.IngressLSN, uint64(77); got != want {
		t.Fatalf("lastCheckpoint.IngressLSN = %d, want %d (after second call)", got, want)
	}
}
