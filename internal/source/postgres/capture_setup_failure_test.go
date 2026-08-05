package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// HA-1. A worker that loses the race for the replication slot must say so.
//
// Before this, the whole path was silent: the vendored connector logged
// "capture failed, slot in use" at Info and retried in place, WaitUntilReady
// blocked forever, /readyz (NATS-only) kept the pod Ready, and the heartbeat
// kept reporting Running. Every worker alert is keyed on {pipeline, source,
// slot} and measures a slot that IS being read, so a worker that never
// acquired one produced no series at all and fired nothing.
//
// The observable contract these tests pin down is the metric, since that is
// what an operator can alert on.
func TestStart_SlotInUse_IsCountedAsCaptureSetupFailure(t *testing.T) {
	const sourceName = "ha1-slot-in-use-source"
	cfg := validSourceConfig()

	before := testutil.ToFloat64(metrics.SourceCaptureSetupFailures.
		WithLabelValues(sourceName, cfg.SlotName, "slot_in_use"))

	s := NewPostgresSource(sourceName)
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)
	s.slotConfirmedFlushLSN = func(_ context.Context, _ *sql.DB, _ string) (pq.LSN, bool) {
		return 0, false // fresh-slot path, so the post-ready goroutine is the one that runs
	}

	// Wrapped, the way the vendored connector reports it, to prove the
	// classification survives %w rather than matching on a bare sentinel.
	factory.failReadyWith(fmt.Errorf(
		"replication slot %q is already in use by another walsender: %w",
		cfg.SlotName, replication.ErrorSlotInUse))

	_, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Stop() })

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.SourceCaptureSetupFailures.
			WithLabelValues(sourceName, cfg.SlotName, "slot_in_use")) == before+1
	}, 3*time.Second, 10*time.Millisecond,
		"a slot-in-use setup failure must increment cdc_source_capture_setup_failures_total{reason=\"slot_in_use\"}")

	conn := factory.Last()
	require.NotNil(t, conn)
	assert.Equal(t, int32(0), conn.updateCount.Load(),
		"a source that never captured must not seed an LSN")
}

// A non-slot failure must still be counted, but must not be misattributed to
// the multi-replica cause -- the reason label is what tells an operator
// whether to go looking at replica count or at the database.
func TestStart_OtherSetupFailure_IsCountedWithReasonOther(t *testing.T) {
	const sourceName = "ha1-other-failure-source"
	cfg := validSourceConfig()

	before := testutil.ToFloat64(metrics.SourceCaptureSetupFailures.
		WithLabelValues(sourceName, cfg.SlotName, "other"))
	slotBefore := testutil.ToFloat64(metrics.SourceCaptureSetupFailures.
		WithLabelValues(sourceName, cfg.SlotName, "slot_in_use"))

	s := NewPostgresSource(sourceName)
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)
	s.slotConfirmedFlushLSN = func(_ context.Context, _ *sql.DB, _ string) (pq.LSN, bool) {
		return 0, false
	}
	factory.failReadyWith(errors.New("publication does not exist"))

	_, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Stop() })

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.SourceCaptureSetupFailures.
			WithLabelValues(sourceName, cfg.SlotName, "other")) == before+1
	}, 3*time.Second, 10*time.Millisecond,
		"a non-slot setup failure must be counted with reason=\"other\"")

	assert.Equal(t, slotBefore, testutil.ToFloat64(metrics.SourceCaptureSetupFailures.
		WithLabelValues(sourceName, cfg.SlotName, "slot_in_use")),
		"an unrelated failure must not be reported as slot contention")
}

// Shutdown is the one benign way WaitUntilReady fails, and it happens on every
// single Stop(). Counting it would make the alert fire on every deploy, which
// is the fastest way to get a real signal ignored.
func TestStart_ShutdownDuringWaitUntilReady_IsNotCountedAsFailure(t *testing.T) {
	const sourceName = "ha1-shutdown-source"
	cfg := validSourceConfig()

	readTotal := func() float64 {
		var total float64
		for _, reason := range []string{"slot_in_use", "other"} {
			total += testutil.ToFloat64(metrics.SourceCaptureSetupFailures.
				WithLabelValues(sourceName, cfg.SlotName, reason))
		}
		return total
	}
	before := readTotal()

	s := NewPostgresSource(sourceName)
	factory := newStubFactory()
	s.SetConnectorFactory(factory.Build)
	s.slotConfirmedFlushLSN = func(_ context.Context, _ *sql.DB, _ string) (pq.LSN, bool) {
		return 0, false
	}
	// Never released: WaitUntilReady can only return via ctx.Done(), i.e. the
	// ctx.Canceled path that Stop() produces.
	_ = factory.gateReady()

	_, _, err := s.Start(context.Background(), cfg, protocol.Checkpoint{}, nil)
	require.NoError(t, err)

	conn := factory.Last()
	require.NotNil(t, conn)
	require.Eventually(t, func() bool {
		return conn.firstStartNano.Load() > 0
	}, 2*time.Second, 10*time.Millisecond, "conn.Start must be reached")

	require.NoError(t, s.Stop())

	assert.Equal(t, before, readTotal(),
		"a shutdown-cancelled WaitUntilReady is not a capture failure and must not be counted")
}
