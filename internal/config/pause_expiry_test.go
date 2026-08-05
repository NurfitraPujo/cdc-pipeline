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

// fakeClock is a Clock a test can move forward on demand, so pause-expiry
// tests never sleep on real time.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// fakeEntry is a minimal nats.KeyValueEntry, matching the pattern
// internal/api/api_test.go's mockEntry uses.
type fakeEntry struct {
	key   string
	value []byte
}

func (e fakeEntry) Key() string                { return e.key }
func (e fakeEntry) Value() []byte              { return e.value }
func (e fakeEntry) Revision() uint64           { return 1 }
func (e fakeEntry) Created() time.Time         { return time.Now() }
func (e fakeEntry) Delta() uint64              { return 0 }
func (e fakeEntry) Operation() nats.KeyValueOp { return 0 }
func (e fakeEntry) Bucket() string             { return "test" }

var _ nats.KeyValueEntry = fakeEntry{}

func newTestManager(kv nats.KeyValue) *ConfigManager {
	return NewConfigManager(kv, nil)
}

func TestTickPauseExpiry_ResumesElapsedPause(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	pausedUntil := clock.now.Add(-time.Minute) // already elapsed
	rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, PausedUntil: &pausedUntil}
	recData, _ := json.Marshal(rec)

	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStatePaused}
	cfgData, _ := json.Marshal(cfg)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")

	kv.EXPECT().Keys().Return([]string{lifecycleKey}, nil)
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)

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
	assert.Nil(t, gotRec.PausedUntil)
}

// TestTickPauseExpiry_ResumesElapsedDefaultTTLPause is
// TestTickPauseExpiry_ResumesElapsedPause's twin for a pause that was
// requested with no ttl in the body. Since internal/api/handler.go now
// always sets PausedUntil (falling back to protocol.MaxPauseTTL rather than
// leaving it nil -- see the WS-3 blocking finding), the resulting record
// looks exactly like an explicit-ttl pause to the ticker: PausedUntil is
// non-nil and, once elapsed, is resumed the same way. This asserts that
// path end-to-end rather than relying on the handler-side fix alone.
func TestTickPauseExpiry_ResumesElapsedDefaultTTLPause(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	// Simulate a pause that was originally requested with no ttl: the
	// handler still sets PausedUntil to (request time + 4h ceiling), so by
	// the time it elapses the record is indistinguishable from an
	// explicit-ttl pause.
	pausedUntil := clock.now.Add(-time.Minute) // already elapsed
	rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, PausedUntil: &pausedUntil}
	recData, _ := json.Marshal(rec)

	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStatePaused}
	cfgData, _ := json.Marshal(cfg)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")

	kv.EXPECT().Keys().Return([]string{lifecycleKey}, nil)
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)

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
	assert.Nil(t, gotRec.PausedUntil)
}

func TestTickPauseExpiry_WALStatusLostGoesToNeedsResnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)
	m.SetSlotHealthChecker(func(context.Context, string, protocol.PipelineConfig) SlotHealth {
		return SlotHealth{Alive: false, WALStatusLost: true}
	})

	pausedUntil := clock.now.Add(-time.Second)
	rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, PausedUntil: &pausedUntil}
	recData, _ := json.Marshal(rec)

	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStatePaused}
	cfgData, _ := json.Marshal(cfg)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")

	kv.EXPECT().Keys().Return([]string{lifecycleKey}, nil)
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)
	// desired_state must NOT be touched -- config Put is never expected.

	var putRecData []byte
	kv.EXPECT().Update(lifecycleKey, gomock.Any(), gomock.Any()).DoAndReturn(func(_ string, data []byte, _ uint64) (uint64, error) {
		putRecData = data
		return 2, nil
	})

	m.tickPauseExpiry(context.Background())

	var gotRec protocol.PipelineLifecycleRecord
	assert.NoError(t, json.Unmarshal(putRecData, &gotRec))
	assert.Equal(t, protocol.StateNeedsResnapshot, gotRec.State)
	assert.Nil(t, gotRec.PausedUntil)
}

func TestTickPauseExpiry_IgnoresPauseNotYetElapsed(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	pausedUntil := clock.now.Add(time.Hour) // still in the future
	rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, PausedUntil: &pausedUntil}
	recData, _ := json.Marshal(rec)

	lifecycleKey := protocol.LifecycleStateKey("p1")

	kv.EXPECT().Keys().Return([]string{lifecycleKey}, nil)
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	// No config Get/Put and no lifecycle Put expected -- nothing should happen.

	m.tickPauseExpiry(context.Background())
}

func TestTickPauseExpiry_IgnoresNonPausedStates(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv)

	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	m.SetClock(clock)

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning}
	recData, _ := json.Marshal(rec)

	lifecycleKey := protocol.LifecycleStateKey("p1")

	kv.EXPECT().Keys().Return([]string{lifecycleKey}, nil)
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)

	m.tickPauseExpiry(context.Background())
}

// TestTickPauseExpiry_SelfHealingAfterRestart is the invariant plan section
// 8 exists for: a manager that starts fresh (no in-memory timer state at
// all -- a brand-new ConfigManager, exactly as after a restart) still
// resumes a pipeline whose paused_until elapsed while it was down, on the
// very first tick.
func TestTickPauseExpiry_SelfHealingAfterRestart(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)
	m := newTestManager(kv) // fresh manager, no prior ticks, default real clock

	longAgo := time.Now().Add(-24 * time.Hour)
	rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, PausedUntil: &longAgo}
	recData, _ := json.Marshal(rec)

	cfg := protocol.PipelineConfig{ID: "p1", DesiredState: protocol.DesiredStatePaused}
	cfgData, _ := json.Marshal(cfg)

	lifecycleKey := protocol.LifecycleStateKey("p1")
	configKey := protocol.PipelineConfigKey("p1")

	kv.EXPECT().Keys().Return([]string{lifecycleKey}, nil)
	kv.EXPECT().Get(lifecycleKey).Return(fakeEntry{key: lifecycleKey, value: recData}, nil)
	kv.EXPECT().Get(configKey).Return(fakeEntry{key: configKey, value: cfgData}, nil)
	kv.EXPECT().Put(configKey, gomock.Any()).Return(uint64(2), nil)
	kv.EXPECT().Update(lifecycleKey, gomock.Any(), gomock.Any()).Return(uint64(2), nil)

	m.tickPauseExpiry(context.Background())
}

func TestPipelineIDFromLifecycleKey(t *testing.T) {
	assert.Equal(t, "p1", pipelineIDFromLifecycleKey(protocol.LifecycleStateKey("p1")))
	assert.Equal(t, "", pipelineIDFromLifecycleKey(protocol.PipelineConfigKey("p1")))
	assert.Equal(t, "", pipelineIDFromLifecycleKey("cdc.pipeline.p1.transition"))
	assert.Equal(t, "", pipelineIDFromLifecycleKey("unrelated.key"))
}
