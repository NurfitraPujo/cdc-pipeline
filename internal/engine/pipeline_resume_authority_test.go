package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/source"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestPipeline_WarnIfSlotAheadOfMinLSN_Fires is WI-7's regression test for
// the pipeline.go observability warning: when the replication slot's
// confirmed_flush_lsn is reported ahead of minLSN (the min durably-
// egressed LSN across sinks), a warning must be logged. Under the
// WI-4/WI-5 invariant the slot must never legitimately be ahead of
// minLSN, so this condition means the invariant was violated upstream
// (e.g. a pre-upgrade slot that older code over-advanced) and an
// operator needs to know that replay cannot recover the affected rows.
func TestPipeline_WarnIfSlotAheadOfMinLSN_Fires(t *testing.T) {
	var buf bytes.Buffer
	prevLogger := log.Logger
	log.Logger = zerolog.New(&buf)
	defer func() { log.Logger = prevLogger }()

	p := NewPipeline("p-slot-ahead", nil, nil, protocol.PipelineConfig{})
	p.slotConfirmedFlushLSN = func(_ protocol.SourceConfig) (uint64, bool) {
		return 500, true // slot is ahead of minLSN
	}

	srcCfg := protocol.SourceConfig{Type: "postgres", SlotName: "slot1"}
	p.warnIfSlotAheadOfSinkFrontier(srcCfg, 100)

	assert.Contains(t, buf.String(), "ahead of the durable sink frontier",
		"expected a warning log when the slot's confirmed_flush_lsn is ahead of minLSN")
}

// TestPipeline_WarnIfSlotAheadOfMinLSN_NoWarnWhenBehindOrEqual is the
// negative case: the slot at or behind minLSN (the expected steady
// state under the WI-4/WI-5 invariant) must not produce a warning.
func TestPipeline_WarnIfSlotAheadOfMinLSN_NoWarnWhenBehindOrEqual(t *testing.T) {
	var buf bytes.Buffer
	prevLogger := log.Logger
	log.Logger = zerolog.New(&buf)
	defer func() { log.Logger = prevLogger }()

	p := NewPipeline("p-slot-behind", nil, nil, protocol.PipelineConfig{})
	p.slotConfirmedFlushLSN = func(_ protocol.SourceConfig) (uint64, bool) {
		return 100, true // slot == minLSN, the expected steady state
	}

	srcCfg := protocol.SourceConfig{Type: "postgres", SlotName: "slot1"}
	p.warnIfSlotAheadOfSinkFrontier(srcCfg, 100)

	assert.NotContains(t, buf.String(), "ahead of the durable sink frontier",
		"must not warn when the slot is not ahead of minLSN")
}

// TestPipeline_WarnIfSlotAheadOfMinLSN_SkipsWhenLookupFails asserts the
// observability-only contract: when the slot lookup itself fails (ok ==
// false, e.g. a transient DB error), the check must be skipped silently
// rather than warning on stale/zero data.
func TestPipeline_WarnIfSlotAheadOfMinLSN_SkipsWhenLookupFails(t *testing.T) {
	var buf bytes.Buffer
	prevLogger := log.Logger
	log.Logger = zerolog.New(&buf)
	defer func() { log.Logger = prevLogger }()

	p := NewPipeline("p-slot-unknown", nil, nil, protocol.PipelineConfig{})
	called := false
	p.slotConfirmedFlushLSN = func(_ protocol.SourceConfig) (uint64, bool) {
		called = true
		return 0, false
	}

	srcCfg := protocol.SourceConfig{Type: "postgres", SlotName: "slot1"}
	p.warnIfSlotAheadOfSinkFrontier(srcCfg, 100)

	require.True(t, called, "the lookup function should still be invoked")
	assert.NotContains(t, buf.String(), "ahead of the durable sink frontier")
}

// TestPipeline_RunProducer_StaleTableDoesNotFalseAlarm drives the REAL
// checkpoint scan in runProducer (:157-181), not synthetic scalars, to
// close the defect the coordinator flagged: a pipeline with one busy
// table (high EgressLSN) and one stale table (low EgressLSN) on the SAME
// sink has minLSN pinned to the stale table -- comparing the slot
// against minLSN would false-alarm on every restart since the slot
// legitimately advances to the busy table's position. Comparing against
// the per-sink frontier (max EgressLSN across that sink's tables)
// instead must NOT warn when the slot sits between the two: it is
// exactly the sink's own durable frontier, not ahead of it.
func TestPipeline_RunProducer_StaleTableDoesNotFalseAlarm(t *testing.T) {
	var buf bytes.Buffer
	prevLogger := log.Logger
	log.Logger = zerolog.New(&buf)
	defer func() { log.Logger = prevLogger }()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockKeyValue(ctrl)
	mockSrc := mocks.NewMockSource(ctrl)

	pipelineID := "p-stale-table"
	cfg := protocol.PipelineConfig{
		ID:      pipelineID,
		Sources: []string{"s1"},
		Sinks:   []string{"sink1"},
		Tables:  []string{"busy", "stale"},
	}

	srcCfg := protocol.SourceConfig{ID: "s1", Type: "postgres", SlotName: "slot1"}
	data, err := json.Marshal(srcCfg)
	require.NoError(t, err)
	mockKV.EXPECT().Get(protocol.SourceConfigKey("s1")).Return(mockEntry{value: data}, nil).AnyTimes()

	// busy table: sink1 has a recent, high EgressLSN.
	busyCP := protocol.Checkpoint{EgressLSN: 1000}
	busyData, err := busyCP.MarshalMsg(nil)
	require.NoError(t, err)
	mockKV.EXPECT().
		Get(protocol.EgressCheckpointKey(pipelineID, "s1", "sink1", protocol.TableRef{Schema: "public", Table: "busy"})).
		Return(mockEntry{value: busyData}, nil).AnyTimes()

	// stale table: sink1's checkpoint is old/low -- this is what pins
	// minLSN far below the slot under the OLD (buggy) comparison.
	staleCP := protocol.Checkpoint{EgressLSN: 100}
	staleData, err := staleCP.MarshalMsg(nil)
	require.NoError(t, err)
	mockKV.EXPECT().
		Get(protocol.EgressCheckpointKey(pipelineID, "s1", "sink1", protocol.TableRef{Schema: "public", Table: "stale"})).
		Return(mockEntry{value: staleData}, nil).AnyTimes()

	mockKV.EXPECT().Get(gomock.Any()).Return(nil, errors.New("no checkpoint")).AnyTimes()

	// The checkpoint scan + warning check run synchronously in runProducer
	// BEFORE producer.Run is called, so the assertion below is satisfied
	// regardless of what Run does. Run still executes though, and it
	// subscribes to the acks topic — so a real subscriber mock is required
	// or Run nil-derefs at producer.go:181 and takes the test binary down.
	srcMsgChan := make(chan []protocol.Message)
	ackChan := make(chan source.SourceAck)
	mockSrc.EXPECT().Start(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(srcMsgChan, ackChan, nil).AnyTimes()
	mockSrc.EXPECT().Stop().Return(nil).AnyTimes() // HIGH-2: Run defers source.Stop()

	mockSub := mocks.NewMockSubscriber(ctrl)
	ackMsgChan := make(chan *message.Message)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any()).
		Return((<-chan *message.Message)(ackMsgChan), nil).AnyTimes()

	producer := NewProducer(pipelineID, "nats://localhost:4222", cfg, mockSrc, nil, mockSub, mockKV, srcCfg)

	pipeline := NewPipeline(pipelineID, producer, nil, cfg)
	// The slot sits BETWEEN the stale table's checkpoint (100) and the
	// busy table's checkpoint (1000) -- exactly the value that would
	// trip the old minLSN-based comparison (500 > 100) but must NOT trip
	// the frontier-based one (500 <= max(sink1's tables) == 1000).
	pipeline.slotConfirmedFlushLSN = func(_ protocol.SourceConfig) (uint64, bool) {
		return 500, true
	}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, pipeline.Start(ctx))

	// Give runProducer's synchronous checkpoint-scan-and-warn a moment to
	// run before we tear down.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-pipeline.Finished():
	case <-time.After(3 * time.Second):
		t.Fatal("pipeline did not finish after cancel")
	}

	assert.NotContains(t, buf.String(), "ahead of the durable sink frontier",
		"the slot sitting between a stale table's and a busy table's checkpoint on the same sink must not false-alarm")
}
