package config

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/api/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// TestMaybeSweepReconciliation_NoStepperInstalledSkipsSweep mirrors
// TestMaybeCompleteResnapshot_NoCheckerInstalledSkipsSweep: a manager that
// never calls SetReconcileStepper must not pay any KV lookup for a Stale
// pipeline -- an unwired sweep must leave Stale exactly where it is, never
// fake progress.
func TestMaybeSweepReconciliation_NoStepperInstalledSkipsSweep(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)
	// No kv.EXPECT() calls set up at all -- any call fails the test.

	m.maybeSweepReconciliation(context.Background(), "p1")
}

// TestMaybeSweepReconciliation_IgnoresIdle asserts the sweep never
// consults the stepper for a pipeline whose reconciliation sub-status is
// Idle (the zero value) -- there is nothing owed.
func TestMaybeSweepReconciliation_IgnoresIdle(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)
	m.SetReconcileStepper(func(context.Context, string, protocol.PipelineConfig, protocol.ReconciliationProgress) (protocol.ReconciliationProgress, bool, error) {
		t.Fatal("stepper must not be consulted for a pipeline with no reconciliation owed")
		return protocol.ReconciliationProgress{}, false, nil
	})

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning, Reconciliation: protocol.ReconciliationOK}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)

	m.maybeSweepReconciliation(context.Background(), "p1")
}

// TestMaybeSweepReconciliation_IncompleteChunkStaysStale is invariant 5's
// contract exercised through the manager seam: a chunk step that reports
// complete=false must leave the record's sub-status as Running (progress
// made, but NOT cleared), never OK -- the only way out of Stale is a
// completed sweep, not a merely-attempted one.
func TestMaybeSweepReconciliation_IncompleteChunkStaysStale(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	stepped := false
	m.SetReconcileStepper(func(_ context.Context, id string, _ protocol.PipelineConfig, prev protocol.ReconciliationProgress) (protocol.ReconciliationProgress, bool, error) {
		stepped = true
		assert.Equal(t, "p1", id)
		next := prev
		next.NextChunkOrdinal = 1
		next.ChunksTotal = 3
		next.RowsReconciled = 5
		return next, false, nil
	})

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning, Reconciliation: protocol.ReconciliationStale}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)
	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStateRunning}
	cfgData, err := json.Marshal(cfg)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")
	progressKey := protocol.ReconciliationProgressKey("p1")

	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)
	kv.EXPECT().Get(progressKey).Return(nil, nats.ErrKeyNotFound)

	var putProgress protocol.ReconciliationProgress
	kv.EXPECT().Put(progressKey, gomock.Any()).DoAndReturn(func(_ string, data []byte) (uint64, error) {
		assert.NoError(t, json.Unmarshal(data, &putProgress))
		return 1, nil
	})

	var putRec protocol.PipelineLifecycleRecord
	kv.EXPECT().Update(lifecycleKey, gomock.Any(), uint64(1)).DoAndReturn(func(_ string, data []byte, _ uint64) (uint64, error) {
		assert.NoError(t, json.Unmarshal(data, &putRec))
		return 2, nil
	})

	m.maybeSweepReconciliation(context.Background(), "p1")

	assert.True(t, stepped)
	assert.Equal(t, protocol.ReconciliationRunning, putRec.Reconciliation)
	assert.Equal(t, 1, putProgress.NextChunkOrdinal)
	assert.Equal(t, 3, putProgress.ChunksTotal)
	assert.False(t, putProgress.StartedAt.IsZero(), "StartedAt must be set on first observation")
}

// TestMaybeSweepReconciliation_CompleteClearsToIdle asserts the ONLY path
// that clears Stale/Running to Idle is a stepper reporting complete=true,
// and that the persisted progress record is removed so a future stop
// window starts a fresh sweep rather than resuming a stale cursor.
func TestMaybeSweepReconciliation_CompleteClearsToIdle(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	m.SetReconcileStepper(func(_ context.Context, _ string, _ protocol.PipelineConfig, prev protocol.ReconciliationProgress) (protocol.ReconciliationProgress, bool, error) {
		next := prev
		next.NextChunkOrdinal = 3
		next.ChunksTotal = 3
		return next, true, nil
	})

	priorProgress := protocol.ReconciliationProgress{NextChunkOrdinal: 2, ChunksTotal: 3, StartedAt: clock.now.Add(-time.Hour)}
	progressData, err := json.Marshal(priorProgress)
	assert.NoError(t, err)

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning, Reconciliation: protocol.ReconciliationRunning}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)
	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStateRunning}
	cfgData, err := json.Marshal(cfg)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")
	progressKey := protocol.ReconciliationProgressKey("p1")

	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)
	kv.EXPECT().Get(progressKey).Return(fakeEntry{key: progressKey, value: progressData}, nil)
	kv.EXPECT().Delete(progressKey).Return(nil)

	var putRec protocol.PipelineLifecycleRecord
	kv.EXPECT().Update(lifecycleKey, gomock.Any(), uint64(1)).DoAndReturn(func(_ string, data []byte, _ uint64) (uint64, error) {
		assert.NoError(t, json.Unmarshal(data, &putRec))
		return 3, nil
	})

	m.maybeSweepReconciliation(context.Background(), "p1")

	assert.Equal(t, protocol.ReconciliationOK, putRec.Reconciliation)
}

// TestMaybeSweepReconciliation_ConcurrentLifecycleWriteAbortsSweepWrite is
// the regression test for the blind-Put race: a concurrent Transition()
// write (e.g. an operator's pause landing mid-sweep) must win, and the
// sweep's own write -- derived from a lifecycle snapshot taken before that
// concurrent write -- must be rejected rather than clobbering it. None of
// the other tests in this file assert anything about the CAS revision
// passed to kv.Update; this is the one that actually exercises the guard
// failing closed.
func TestMaybeSweepReconciliation_ConcurrentLifecycleWriteAbortsSweepWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	m.SetReconcileStepper(func(_ context.Context, _ string, _ protocol.PipelineConfig, prev protocol.ReconciliationProgress) (protocol.ReconciliationProgress, bool, error) {
		next := prev
		next.NextChunkOrdinal = 3
		next.ChunksTotal = 3
		return next, true, nil
	})

	priorProgress := protocol.ReconciliationProgress{NextChunkOrdinal: 2, ChunksTotal: 3, StartedAt: clock.now.Add(-time.Hour)}
	progressData, err := json.Marshal(priorProgress)
	assert.NoError(t, err)

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning, Reconciliation: protocol.ReconciliationRunning}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)
	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStateRunning}
	cfgData, err := json.Marshal(cfg)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")
	progressKey := protocol.ReconciliationProgressKey("p1")

	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)
	kv.EXPECT().Get(progressKey).Return(fakeEntry{key: progressKey, value: progressData}, nil)
	kv.EXPECT().Delete(progressKey).Return(nil)
	// Simulates an operator's pause landing on this pipeline while the
	// stepper was doing its (slow, blocking) source+sink I/O: the revision
	// this sweep read at is now stale, so the CAS write must fail rather
	// than silently overwrite whatever the pause wrote.
	kv.EXPECT().Update(lifecycleKey, gomock.Any(), uint64(1)).Return(uint64(0), nats.ErrKeyExists)

	m.maybeSweepReconciliation(context.Background(), "p1")
	// No further assertions needed: gomock's controller.Finish (via t.Cleanup)
	// already asserts kv.Put on the lifecycle key was never called, since it
	// has no corresponding EXPECT() -- any such call would fail the test.
}

// TestMaybeSweepReconciliation_ResumesFromPersistedProgress asserts a
// second tick resumes from the ordinal the previous tick persisted,
// rather than restarting at chunk 0 -- the "resumable after restart"
// property WS-7 requires, exercised here as "resumable across ticks",
// which is the same code path a process restart would hit (progress is
// always read from KV, never cached in memory).
func TestMaybeSweepReconciliation_ResumesFromPersistedProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	var seenOrdinal int
	m.SetReconcileStepper(func(_ context.Context, _ string, _ protocol.PipelineConfig, prev protocol.ReconciliationProgress) (protocol.ReconciliationProgress, bool, error) {
		seenOrdinal = prev.NextChunkOrdinal
		next := prev
		next.NextChunkOrdinal = prev.NextChunkOrdinal + 1
		next.ChunksTotal = 5
		return next, false, nil
	})

	priorProgress := protocol.ReconciliationProgress{NextChunkOrdinal: 2, ChunksTotal: 5, StartedAt: clock.now.Add(-time.Minute)}
	progressData, err := json.Marshal(priorProgress)
	assert.NoError(t, err)

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning, Reconciliation: protocol.ReconciliationRunning}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)
	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStateRunning}
	cfgData, err := json.Marshal(cfg)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")
	progressKey := protocol.ReconciliationProgressKey("p1")

	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)
	kv.EXPECT().Get(progressKey).Return(fakeEntry{key: progressKey, value: progressData}, nil)
	kv.EXPECT().Put(progressKey, gomock.Any()).Return(uint64(1), nil)
	// Already Running: no lifecycle-record Put expected for this tick.

	m.maybeSweepReconciliation(context.Background(), "p1")

	assert.Equal(t, 2, seenOrdinal)
}

// TestMaybeSweepReconciliation_StepperErrorLeavesRecordUntouched asserts a
// chunk step returning an error retries next tick rather than advancing
// progress or clearing Stale/Running -- mirroring every other checker in
// this package's "an unreadable/failing probe never reads as done" contract.
func TestMaybeSweepReconciliation_StepperErrorLeavesRecordUntouched(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	m.SetReconcileStepper(func(context.Context, string, protocol.PipelineConfig, protocol.ReconciliationProgress) (protocol.ReconciliationProgress, bool, error) {
		return protocol.ReconciliationProgress{}, false, assert.AnError
	})

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning, Reconciliation: protocol.ReconciliationStale}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)
	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStateRunning}
	cfgData, err := json.Marshal(cfg)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")
	progressKey := protocol.ReconciliationProgressKey("p1")

	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)
	kv.EXPECT().Get(progressKey).Return(nil, nats.ErrKeyNotFound)
	// Progress is still persisted (to refresh UpdatedAt/StartedAt bookkeeping)
	// but the lifecycle record itself is never Put -- no advancement.
	kv.EXPECT().Put(progressKey, gomock.Any()).Return(uint64(1), nil)

	m.maybeSweepReconciliation(context.Background(), "p1")
}
