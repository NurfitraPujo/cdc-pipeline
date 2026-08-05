package config

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/api/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// TestMaybeCompleteResnapshot_NoCheckerInstalledSkipsSweep mirrors
// TestTickPauseExpiry_NoWALGuardCheckerInstalledSkipsSweep: a manager that
// never calls SetResnapshotStatusChecker must not pay any KV lookup for a
// pipeline in Snapshotting -- the same "nil checker short-circuits before
// any KV call" contract maybeEscalateWALGuardBreach has.
func TestMaybeCompleteResnapshot_NoCheckerInstalledSkipsSweep(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)
	// No kv.EXPECT() calls set up at all -- any call fails the test.

	m.maybeCompleteResnapshot(context.Background(), "p1")
}

// TestMaybeCompleteResnapshot_IgnoresNonSnapshottingStates asserts the
// sweep only acts on StateSnapshotting, exactly as maybeEscalateWALGuardBreach
// only acts on StatePaused.
func TestMaybeCompleteResnapshot_IgnoresNonSnapshottingStates(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)
	m.SetResnapshotStatusChecker(func(context.Context, string, protocol.PipelineConfig) (ResnapshotStatus, bool) {
		t.Fatal("checker must not be consulted for a non-Snapshotting pipeline")
		return ResnapshotStatus{}, false
	})

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)

	m.maybeCompleteResnapshot(context.Background(), "p1")
}

// TestMaybeCompleteResnapshot_NotYetComplete asserts an incomplete
// re-snapshot leaves the Snapshotting record untouched -- no lifecycle Put
// is expected.
func TestMaybeCompleteResnapshot_NotYetComplete(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)
	m.SetResnapshotStatusChecker(func(context.Context, string, protocol.PipelineConfig) (ResnapshotStatus, bool) {
		return ResnapshotStatus{Completed: false}, true
	})

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateSnapshotting}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)
	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStateRunning}
	cfgData, err := json.Marshal(cfg)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)
	// No Put expected.

	m.maybeCompleteResnapshot(context.Background(), "p1")
}

// TestMaybeCompleteResnapshot_CompletesToRunningStale is invariant 5's
// contract exercised end to end through the manager seam: a completed
// re-snapshot lands Running with Reconciliation stale, since
// StateSnapshotting is only ever reached after a stop window.
func TestMaybeCompleteResnapshot_CompletesToRunningStale(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)
	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)
	recUpdatedAt := time.Date(2026, 8, 4, 11, 0, 0, 0, time.UTC)
	m.SetResnapshotStatusChecker(func(context.Context, string, protocol.PipelineConfig) (ResnapshotStatus, bool) {
		// StartedAt after rec's UpdatedAt: the new re-snapshot's job row, not
		// the stale one from before the pipeline entered Snapshotting.
		return ResnapshotStatus{Completed: true, StartedAt: recUpdatedAt.Add(time.Minute)}, true
	})

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateSnapshotting, UpdatedAt: recUpdatedAt}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)
	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStateRunning}
	cfgData, err := json.Marshal(cfg)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)

	var putRecData []byte
	kv.EXPECT().Update(lifecycleKey, gomock.Any(), gomock.Any()).DoAndReturn(func(_ string, data []byte, _ uint64) (uint64, error) {
		putRecData = data
		return 2, nil
	})

	m.maybeCompleteResnapshot(context.Background(), "p1")

	var gotRec protocol.PipelineLifecycleRecord
	assert.NoError(t, json.Unmarshal(putRecData, &gotRec))
	assert.Equal(t, protocol.StateRunning, gotRec.State)
	assert.Equal(t, protocol.ReconciliationStale, gotRec.Reconciliation)
}

// TestMaybeCompleteResnapshot_StaleJobRowLeavesRecordUntouched guards the
// window between the handler writing Snapshotting and the worker's connector
// actually running CleanupJobForSlot: a probe reporting Completed=true whose
// started_at predates (or equals) the lifecycle record's UpdatedAt is the
// PREVIOUS snapshot's row, not the new one, and must not advance the state.
func TestMaybeCompleteResnapshot_StaleJobRowLeavesRecordUntouched(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)
	recUpdatedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	m.SetResnapshotStatusChecker(func(context.Context, string, protocol.PipelineConfig) (ResnapshotStatus, bool) {
		// Completed=true but started_at is older than (before) the record
		// entered Snapshotting -- the stale, pre-stop-window job row.
		return ResnapshotStatus{Completed: true, StartedAt: recUpdatedAt.Add(-time.Hour)}, true
	})

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateSnapshotting, UpdatedAt: recUpdatedAt}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)
	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStateRunning}
	cfgData, err := json.Marshal(cfg)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)
	// No Put expected: the stale row must not be mistaken for completion.

	m.maybeCompleteResnapshot(context.Background(), "p1")
}

// TestMaybeCompleteResnapshot_ProbeFailureLeavesRecordUntouched mirrors the
// "unreadable signal is never read as complete" contract every other
// checker in this package follows (probeSlotHealth, queryWALGuardStatus):
// probed=false must not advance the state.
func TestMaybeCompleteResnapshot_ProbeFailureLeavesRecordUntouched(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)
	m.SetResnapshotStatusChecker(func(context.Context, string, protocol.PipelineConfig) (ResnapshotStatus, bool) {
		return ResnapshotStatus{}, false
	})

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateSnapshotting}
	recData, err := json.Marshal(rec)
	assert.NoError(t, err)
	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStateRunning}
	cfgData, err := json.Marshal(cfg)
	assert.NoError(t, err)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)
	// No Put expected.

	m.maybeCompleteResnapshot(context.Background(), "p1")
}
