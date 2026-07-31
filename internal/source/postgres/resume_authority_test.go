package postgres

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

// TestStart_ResumeLSN_DoesNotSeedStartLSN is WI-7's core regression test:
// on a resume (checkpoint.IngressLSN > 0), cfg.StartLSN handed to the
// connector factory must be 0 -- the replication slot's own
// confirmed_flush_lsn is the sole resume authority -- while Snapshot
// remains unconditionally enabled (Critical 11 stays fixed).
//
// config.Config is non-comparable (it holds a func field via
// KeepaliveFunc), so this asserts individual captured fields rather than
// whole-struct equality, per B4.
func TestStart_ResumeLSN_DoesNotSeedStartLSN(t *testing.T) {
	s := NewPostgresSource("wi7-resume-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	checkpoint := protocol.Checkpoint{IngressLSN: 99999}

	_, _, err := s.Start(context.Background(), cfg, checkpoint, nil)
	require.NoError(t, err)
	defer s.Stop()

	got := factory.LastConfig()

	assert.Equal(t, pq.LSN(0), got.StartLSN, "StartLSN must always be 0; the slot's confirmed_flush_lsn is the sole resume authority")
	assert.True(t, got.Snapshot.Enabled, "Snapshot.Enabled must stay unconditional (Critical 11)")
}

// TestStart_HydratesWatermarkFloor verifies the watermark floor: after
// Start with checkpoint.IngressLSN > 0, the AckManager watermark must be
// >= checkpoint.IngressLSN, so the first UpdateXLogPos the coordinator
// sends can never regress below what KV already knows.
func TestStart_HydratesWatermarkFloor(t *testing.T) {
	s := NewPostgresSource("wi7-hydrate-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	checkpoint := protocol.Checkpoint{IngressLSN: 42}

	_, _, err := s.Start(context.Background(), cfg, checkpoint, nil)
	require.NoError(t, err)
	defer s.Stop()

	assert.GreaterOrEqual(t, s.ackMgr.Watermark(), checkpoint.IngressLSN,
		"the hydrated watermark must never be below the persisted checkpoint")
}

// TestStart_B3Mitigation_SeedsLastXLogPosOnResume is the regression test
// for B3: on the resume path (hydrated watermark > 0), startConnector
// must seed the vendored stream's lastXLogPos once via UpdateXLogPos
// BEFORE conn.Start is called, using the hydrated watermark. Without
// this, both vendored keepalive reply paths stay LoadXLogPos()==0-guarded
// until the coordinator's first flush, and a sink that is down from the
// very first event would send no standby status update at all --
// risking wal_sender_timeout on the primary.
func TestStart_B3Mitigation_SeedsLastXLogPosOnResume(t *testing.T) {
	s := NewPostgresSource("wi7-b3-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	cfg := validSourceConfig()
	checkpoint := protocol.Checkpoint{IngressLSN: 777}

	_, _, err := s.Start(context.Background(), cfg, checkpoint, nil)
	require.NoError(t, err)
	defer s.Stop()

	conn := factory.Last()
	require.NotNil(t, conn)

	require.Eventually(t, func() bool {
		return conn.updateCount.Load() > 0
	}, 2*time.Second, 10*time.Millisecond, "startConnector must seed lastXLogPos on the resume path")

	assert.Equal(t, checkpoint.IngressLSN, conn.lastUpdateLSN.Load(),
		"the B3 seed must use the hydrated watermark, which floors at checkpoint.IngressLSN")

	// Pin the ORDERING, not just the value: the coordinator's own 500ms
	// periodic flush would eventually push the same watermark value too,
	// so a bare "updateCount > 0" / "lastUpdateLSN == watermark" check
	// does not actually prove the pre-Start seed ran. Wait for conn.Start
	// to have been entered as well, then assert the first UpdateXLogPos
	// call happened strictly before the first Start call.
	require.Eventually(t, func() bool {
		return conn.firstStartNano.Load() > 0
	}, 2*time.Second, 10*time.Millisecond, "conn.Start must be reached")

	firstUpdate := conn.firstUpdateNano.Load()
	firstStart := conn.firstStartNano.Load()
	require.NotZero(t, firstUpdate)
	require.NotZero(t, firstStart)
	assert.Less(t, firstUpdate, firstStart,
		"the B3 resume seed must happen strictly before conn.Start, not merely eventually via the coordinator's periodic flush")
}

// TestStart_FreshSlot_FastPath_SeedsWhenSlotAlreadyExists covers the case
// where a fresh session (watermark == 0, e.g. this process's first Start
// against a slot a PRIOR process already created) queries
// confirmed_flush_lsn successfully on the very first, pre-Start attempt.
// This must seed lastXLogPos before conn.Start, same ordering guarantee
// as the resume-path seed.
func TestStart_FreshSlot_FastPath_SeedsWhenSlotAlreadyExists(t *testing.T) {
	s := NewPostgresSource("wi7-fresh-fastpath-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)
	s.slotConfirmedFlushLSN = func(_ context.Context, _ *sql.DB, slotName string) (pq.LSN, bool) {
		assert.Equal(t, "slot", slotName)
		return pq.LSN(555), true
	}

	cfg := validSourceConfig()

	_, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
	require.NoError(t, err)
	defer s.Stop()

	conn := factory.Last()
	require.NotNil(t, conn)

	require.Eventually(t, func() bool {
		return conn.updateCount.Load() > 0
	}, 2*time.Second, 10*time.Millisecond, "the fast path must seed when confirmed_flush_lsn is available pre-Start")
	assert.Equal(t, uint64(555), conn.lastUpdateLSN.Load())

	require.Eventually(t, func() bool {
		return conn.firstStartNano.Load() > 0
	}, 2*time.Second, 10*time.Millisecond, "conn.Start must be reached")
	assert.Less(t, conn.firstUpdateNano.Load(), conn.firstStartNano.Load(),
		"the fresh-slot fast-path seed must happen strictly before conn.Start")
}

// TestStart_FreshSlot_PostReadyPath_SeedsAfterSlotCreated is the
// regression test for the rejected-round defect: on a genuinely
// first-ever deployment the slot does not exist yet when the fast-path
// query runs (pre-Start), so it must correctly report "unavailable" and
// defer to the post-WaitUntilReady seed rather than silently giving up.
// The seam here simulates that: the first call (fast path) reports
// unavailable, later calls (post-ready path) succeed. The seed must not
// happen until AFTER conn.WaitUntilReady unblocks.
func TestStart_FreshSlot_PostReadyPath_SeedsAfterSlotCreated(t *testing.T) {
	s := NewPostgresSource("wi7-fresh-postready-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)

	var calls atomic.Int32
	s.slotConfirmedFlushLSN = func(_ context.Context, _ *sql.DB, _ string) (pq.LSN, bool) {
		if calls.Add(1) == 1 {
			// Fast path: slot does not exist yet (sql.ErrNoRows in
			// production terms) -- must report unavailable, not seed 0.
			return 0, false
		}
		// Post-ready path: slot now exists.
		return pq.LSN(888), true
	}

	// Install the WaitUntilReady gate on the factory BEFORE Start, so it
	// is already present on the connector at construction time -- race-
	// free, unlike gating conn.gateReady() after Start returns (by then
	// startConnector's goroutine may already have called, and an
	// ungated stub WaitUntilReady would already have returned from,
	// WaitUntilReady).
	release := factory.gateReady()

	cfg := validSourceConfig()

	_, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
	require.NoError(t, err)
	defer s.Stop()

	conn := factory.Last()
	require.NotNil(t, conn)

	// The gate is still closed: nothing should have been seeded yet,
	// since the fast path reported the slot unavailable and the
	// post-ready path is blocked on WaitUntilReady.
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, int32(0), conn.updateCount.Load(),
		"must not seed before WaitUntilReady unblocks, since the fast path reported the slot unavailable")

	release()

	require.Eventually(t, func() bool {
		return conn.updateCount.Load() > 0
	}, 2*time.Second, 10*time.Millisecond, "the post-ready path must seed once WaitUntilReady unblocks and the slot query succeeds")
	assert.Equal(t, uint64(888), conn.lastUpdateLSN.Load())
	assert.GreaterOrEqual(t, calls.Load(), int32(2), "both the fast path and the post-ready path must have queried")
}

// TestStart_FreshSlot_NeverAvailable_DoesNotSeed documents the remaining,
// deliberately-accepted gap: if confirmed_flush_lsn is unavailable on
// BOTH the fast path and after WaitUntilReady (e.g. the query keeps
// failing for unrelated reasons), lastXLogPos is never seeded and the
// WI-5a slot-lag alert is the backstop -- this code never falls back to
// pg_current_wal_lsn(), which would risk advancing the slot past
// undelivered data.
func TestStart_FreshSlot_NeverAvailable_DoesNotSeed(t *testing.T) {
	s := NewPostgresSource("wi7-fresh-unavailable-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)
	s.slotConfirmedFlushLSN = func(_ context.Context, _ *sql.DB, _ string) (pq.LSN, bool) {
		return 0, false
	}

	cfg := validSourceConfig()

	_, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
	require.NoError(t, err)
	defer s.Stop()

	conn := factory.Last()
	require.NotNil(t, conn)

	require.Eventually(t, func() bool {
		return conn.firstStartNano.Load() > 0
	}, 2*time.Second, 10*time.Millisecond, "conn.Start must be reached")

	// WaitUntilReady returns immediately (no gate installed), so the
	// post-ready goroutine has had its chance too by now.
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, int32(0), conn.updateCount.Load(),
		"must never seed when confirmed_flush_lsn is unavailable on every attempt")
}

// TestStart_FreshSlot_ShutdownDuringWaitUntilReady_DoesNotHang is the
// shutdown-path regression test: if WaitUntilReady never unblocks (e.g.
// setup is hung, or simply slow relative to a fast Stop()), the post-
// ready goroutine must still exit -- via ctx cancellation, not via the
// gate -- so s.runWg drains and Stop() returns promptly rather than
// leaking the goroutine or hanging shutdown.
func TestStart_FreshSlot_ShutdownDuringWaitUntilReady_DoesNotHang(t *testing.T) {
	s := NewPostgresSource("wi7-fresh-shutdown-source")
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)
	s.slotConfirmedFlushLSN = func(_ context.Context, _ *sql.DB, _ string) (pq.LSN, bool) {
		return 0, false // fast path: slot not there yet
	}

	// Install a gate (on the factory, before Start, for the same
	// race-freedom reason as the post-ready test above) that is NEVER
	// released: WaitUntilReady can only return via ctx.Done() (Stop()
	// cancelling sourceCtx), never via the gate closing.
	_ = factory.gateReady()

	cfg := validSourceConfig()

	_, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
	require.NoError(t, err)

	conn := factory.Last()
	require.NotNil(t, conn)

	require.Eventually(t, func() bool {
		return conn.firstStartNano.Load() > 0
	}, 2*time.Second, 10*time.Millisecond, "conn.Start must be reached")

	// Stop() runs on its own goroutine so a hang surfaces as a timeout rather
	// than blocking the test. Its error is reported back over a channel:
	// require/t.FailNow are only legal on the test goroutine, and calling them
	// here would turn a failure into a confusing hang instead of a clean report.
	stopErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		stopErr <- s.Stop()
	}()

	select {
	case <-done:
		require.NoError(t, <-stopErr, "Stop() should shut down cleanly")
		// Expected: Stop()'s s.runWg.Wait() drained the post-ready
		// goroutine because WaitUntilReady returned ctx.Err() once
		// sourceCtx was cancelled, not because the gate closed.
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() hung: the post-ready WaitUntilReady goroutine did not exit on shutdown")
	}

	assert.Equal(t, int32(0), conn.updateCount.Load(),
		"a WaitUntilReady that only ever returns via shutdown must never have seeded")
}
