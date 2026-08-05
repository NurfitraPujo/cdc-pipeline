package config

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/api/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
	"go.uber.org/mock/gomock"
)

func int64Ptr(v int64) *int64 { return &v }

// TestEvaluateWALGuardBreach is the exhaustive table test for the pure
// decision at the heart of WS-4 (plan section 7): wal_status is checked
// first and independently of the WAL-size signal, safe_wal_size wins over
// the lag fallback when non-NULL, and the lag fallback only applies when
// safe_wal_size is NULL.
func TestEvaluateWALGuardBreach(t *testing.T) {
	const threshold = int64(30 * 1024 * 1024 * 1024) // 30 GiB, mirrors protocol.WALBudgetBytes

	tests := []struct {
		name       string
		status     WALGuardStatus
		threshold  int64
		wantBreach bool
	}{
		{
			name:       "unreserved escalates regardless of safe_wal_size",
			status:     WALGuardStatus{WALStatus: "unreserved", SafeWALSizeBytes: int64Ptr(10 << 30)},
			threshold:  threshold,
			wantBreach: true,
		},
		{
			name:       "lost also escalates (never missed, just later than unreserved)",
			status:     WALGuardStatus{WALStatus: "lost"},
			threshold:  threshold,
			wantBreach: true,
		},
		{
			name:       "reserved with healthy safe_wal_size does not breach",
			status:     WALGuardStatus{WALStatus: "reserved", SafeWALSizeBytes: int64Ptr(10 << 30)},
			threshold:  threshold,
			wantBreach: false,
		},
		{
			name:       "extended with exhausted safe_wal_size breaches",
			status:     WALGuardStatus{WALStatus: "extended", SafeWALSizeBytes: int64Ptr(0)},
			threshold:  threshold,
			wantBreach: true,
		},
		{
			name:       "negative safe_wal_size breaches",
			status:     WALGuardStatus{WALStatus: "extended", SafeWALSizeBytes: int64Ptr(-1)},
			threshold:  threshold,
			wantBreach: true,
		},
		{
			name:       "safe_wal_size NULL, lag under fallback threshold does not breach",
			status:     WALGuardStatus{WALStatus: "reserved", LagBytes: threshold - 1, LagOK: true},
			threshold:  threshold,
			wantBreach: false,
		},
		{
			name:       "safe_wal_size NULL, lag at fallback threshold breaches",
			status:     WALGuardStatus{WALStatus: "reserved", LagBytes: threshold, LagOK: true},
			threshold:  threshold,
			wantBreach: true,
		},
		{
			name:       "safe_wal_size NULL, lag probe failed (LagOK false) does not breach",
			status:     WALGuardStatus{WALStatus: "reserved", LagOK: false, LagBytes: threshold * 2},
			threshold:  threshold,
			wantBreach: false,
		},
		{
			name:       "everything unavailable never breaches",
			status:     WALGuardStatus{},
			threshold:  threshold,
			wantBreach: false,
		},
		{
			name:       "safe_wal_size non-NULL wins over a would-be lag breach",
			status:     WALGuardStatus{WALStatus: "reserved", SafeWALSizeBytes: int64Ptr(5 << 30), LagBytes: threshold * 2, LagOK: true},
			threshold:  threshold,
			wantBreach: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			breach, reason := EvaluateWALGuardBreach(tt.status, tt.threshold)
			assert.Equal(t, tt.wantBreach, breach)
			if tt.wantBreach {
				assert.NotEmpty(t, reason, "a breach must always explain why")
			} else {
				assert.Empty(t, reason)
			}
		})
	}
}

// TestTickPauseExpiry_WALGuardBreachEscalatesToStopping exercises the
// ticker-integration path end-to-end: a Paused pipeline whose installed
// WALGuardChecker reports a breach must land on Stopping with the reason
// recorded, AND desired_state (config) must be flipped to stopped -- mirroring
// what maybeResumeExpiredPause does for its own Resuming leg -- so
// ConfigManager's config-watch drives honourDesiredState -> finalizeStop and
// actually drops the slot. Without this write the pipeline would be stuck at
// Stopping forever (no config write means no watcher trigger means
// finalizeStop never runs).
func TestTickPauseExpiry_WALGuardBreachEscalatesToStopping(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)
	m.SetWALGuardChecker(func(context.Context, string, protocol.PipelineConfig) WALGuardStatus {
		return WALGuardStatus{WALStatus: "unreserved"}
	})

	// paused_until still in the future: the guard must still fire, since
	// it is independent of the timer (plan section 5).
	pausedUntil := clock.now.Add(time.Hour)
	rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, PausedUntil: &pausedUntil}
	recData, _ := json.Marshal(rec)

	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStatePaused}
	cfgData, _ := json.Marshal(cfg)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")

	kv.EXPECT().Keys().Return([]string{lifecycleKey}, nil)
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)

	var putRecData []byte
	kv.EXPECT().Update(lifecycleKey, gomock.Any(), gomock.Any()).DoAndReturn(func(_ string, data []byte, _ uint64) (uint64, error) {
		putRecData = data
		return 2, nil
	})
	var putConfigData []byte
	kv.EXPECT().Put(configKey, gomock.Any()).DoAndReturn(func(_ string, data []byte) (uint64, error) {
		putConfigData = data
		return 2, nil
	})

	m.tickPauseExpiry(context.Background())

	var gotRec protocol.PipelineLifecycleRecord
	assert.NoError(t, json.Unmarshal(putRecData, &gotRec))
	assert.Equal(t, protocol.StateStopping, gotRec.State)
	assert.Nil(t, gotRec.PausedUntil)
	assert.Contains(t, gotRec.Reason, "unreserved")

	var gotCfg protocol.PipelineConfig
	assert.NoError(t, json.Unmarshal(putConfigData, &gotCfg))
	assert.Equal(t, protocol.DesiredStateStopped, gotCfg.DesiredState)
}

// TestTickPauseExpiry_WALGuardNoBreachStillChecksTimer confirms the guard
// short-circuiting the timer check only happens ON a breach: with no
// breach, maybeResumeExpiredPause must still run its own elapsed-timer
// logic for the same pipeline in the same tick.
func TestTickPauseExpiry_WALGuardNoBreachStillChecksTimer(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)
	m.SetWALGuardChecker(func(context.Context, string, protocol.PipelineConfig) WALGuardStatus {
		return WALGuardStatus{WALStatus: "reserved"}
	})

	pausedUntil := clock.now.Add(-time.Minute) // already elapsed
	rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, PausedUntil: &pausedUntil}
	recData, _ := json.Marshal(rec)

	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStatePaused}
	cfgData, _ := json.Marshal(cfg)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")

	kv.EXPECT().Keys().Return([]string{lifecycleKey}, nil)
	// maybeEscalateWALGuardBreach reads the record and config once (no
	// breach), then maybeResumeExpiredPause reads them again for its own
	// decision -- both call the shared, side-effect-free getters, so this
	// is exactly two Gets per key.
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil).Times(2)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil).Times(2)

	var putConfigData []byte
	kv.EXPECT().Put(configKey, gomock.Any()).DoAndReturn(func(_ string, data []byte) (uint64, error) {
		putConfigData = data
		return 2, nil
	})
	var putRecData []byte
	kv.EXPECT().Update(lifecycleKey, gomock.Any(), gomock.Any()).DoAndReturn(func(_ string, data []byte, _ uint64) (uint64, error) {
		putRecData = data
		return 2, nil
	})

	m.tickPauseExpiry(context.Background())

	var gotCfg protocol.PipelineConfig
	assert.NoError(t, json.Unmarshal(putConfigData, &gotCfg))
	assert.Equal(t, protocol.DesiredStateRunning, gotCfg.DesiredState)

	var gotRec protocol.PipelineLifecycleRecord
	assert.NoError(t, json.Unmarshal(putRecData, &gotRec))
	assert.Equal(t, protocol.StateRunning, gotRec.State)
}

// TestTickPauseExpiry_NoWALGuardCheckerInstalledSkipsSweep confirms that a
// manager which never calls SetWALGuardChecker (every pre-WS-4 test and
// caller) pays no extra KV read for the guard sweep -- maybeEscalateWALGuardBreach
// must bail out before touching KV at all.
func TestTickPauseExpiry_NoWALGuardCheckerInstalledSkipsSweep(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	pausedUntil := clock.now.Add(time.Hour) // not elapsed, and no guard installed
	rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, PausedUntil: &pausedUntil}
	recData, _ := json.Marshal(rec)

	lifecycleKey := protocol.LifecycleStateKey("p1")

	kv.EXPECT().Keys().Return([]string{lifecycleKey}, nil)
	// Only one Get, from maybeResumeExpiredPause -- the guard sweep must
	// not have issued any KV call at all.
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil).Times(1)

	m.tickPauseExpiry(context.Background())
}

// TestConfigManager_WALGuardBreach_EndsWithSlotDroppedAndStopped is the
// full integration path a mocked-KV test cannot exercise: a real NATS
// watcher observing the desired_state=stopped write maybeEscalateWALGuardBreach
// makes, driving honourDesiredState -> finalizeStop -> the installed
// SlotDropper, and landing the persisted record on Stopped. This is the
// regression test for the validator's blocking finding: before the fix,
// desired_state was never touched, so this test would time out with the
// record still at Stopping and dropCalls at 0.
//nolint:gocyclo // table-driven lifecycle integration test; branch count comes from asserting each step of a multi-stage transition, not from logic that would benefit from extraction
func TestConfigManager_WALGuardBreach_EndsWithSlotDroppedAndStopped(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	ctx := context.Background()

	natsC, err := tc_nats.Run(ctx, "nats:2.10-alpine",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{Cmd: []string{"-js"}},
		}),
	)
	if err != nil {
		t.Fatalf("Failed to start NATS container: %v", err)
	}
	defer func() { _ = natsC.Terminate(ctx) }()

	natsURL, err := natsC.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection string: %v", err)
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, _ := nc.JetStream()
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: protocol.KVBucketName})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}

	factory := func(_ context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		return &MockWorker{id: id, finished: make(chan struct{}), cfg: cfg}, nil
	}

	mgr := NewConfigManager(kv, factory)

	var dropCalls int32
	mgr.SetSlotDropper(func(_ context.Context, _ string, _ protocol.PipelineConfig) error {
		atomic.AddInt32(&dropCalls, 1)
		return nil
	})
	mgr.SetWALGuardChecker(func(context.Context, string, protocol.PipelineConfig) WALGuardStatus {
		return WALGuardStatus{WALStatus: "unreserved"}
	})

	pipelineID := "wal-breach1"
	cfg := protocol.PipelineConfig{
		ID: pipelineID, Name: "Busy source, paused",
		Sources: []string{"src1"}, Sinks: []string{"sink1"},
		DesiredState: protocol.DesiredStatePaused,
	}
	data, _ := json.Marshal(cfg)
	if _, err := kv.Put(protocol.PipelineConfigKey(pipelineID), data); err != nil {
		t.Fatalf("Failed to put paused pipeline config: %v", err)
	}

	pausedUntil := time.Now().UTC().Add(time.Hour) // far future: only the guard should fire
	if err := mgr.putLifecycleRecord(pipelineID, protocol.PipelineLifecycleRecord{
		State: protocol.StatePaused, PausedUntil: &pausedUntil, UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Failed to seed Paused lifecycle record: %v", err)
	}

	if err := mgr.Watch(ctx); err != nil {
		t.Fatalf("Failed to start watcher: %v", err)
	}
	mgr.StartPauseExpiryTicker(ctx, 50*time.Millisecond)

	deadline := time.Now().Add(10 * time.Second)
	var rec protocol.PipelineLifecycleRecord
	for time.Now().Before(deadline) {
		var ok bool
		rec, ok = mgr.getLifecycleRecord(pipelineID)
		if ok && rec.State == protocol.StateStopped {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if rec.State != protocol.StateStopped {
		t.Fatalf("expected the pipeline to reach Stopped after a WAL guard breach, got %s", rec.State)
	}
	if got := atomic.LoadInt32(&dropCalls); got != 1 {
		t.Errorf("expected the slot dropper to be called exactly once, got %d", got)
	}

	cfgAfter, ok := mgr.getPipelineConfig(pipelineID)
	if !ok {
		t.Fatalf("expected the pipeline config to still exist")
	}
	if cfgAfter.EffectiveDesiredState() != protocol.DesiredStateStopped {
		t.Errorf("expected desired_state to be flipped to stopped, got %s", cfgAfter.EffectiveDesiredState())
	}
}
