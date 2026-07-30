package engine

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestPipeline_ZombieFix_KVGetFailureCancelsPipeline is the regression test for
// Critical 13 / WI-8 (pipeline.go:66-87 previously). Before the fix, a failed
// KV.Get for the source config caused the producer goroutine to return without
// calling p.cancel(): consumers kept running on p.ctx forever, p.wg.Wait()
// never returned, Finished() never closed, and the pipeline heartbeated
// "Running" forever despite having stopped producing anything.
//
// With the fix, runProducer() returns an error for the KV.Get failure, the
// caller in Start() calls p.cancel(), consumers observe ctx.Done() and exit,
// and Finished() closes well within the test timeout.
func TestPipeline_ZombieFix_KVGetFailureCancelsPipeline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockKeyValue(ctrl)
	mockSub := mocks.NewMockSubscriber(ctrl)

	pipelineID := "p-zombie"
	cfg := protocol.PipelineConfig{
		ID:      pipelineID,
		Sources: []string{"s1"},
		Sinks:   []string{"sink1"},
		Tables:  []string{"t1"},
	}

	// KV.Get for the source config fails: this is the config-load error path
	// that used to zombie the pipeline.
	mockKV.EXPECT().
		Get(protocol.SourceConfigKey("s1")).
		Return(nil, errors.New("nats kv: transient blip")).
		AnyTimes()

	producer := NewProducer(pipelineID, "nats://localhost:4222", cfg, nil, nil, mockSub, mockKV, protocol.SourceConfig{ID: "s1"})

	// Consumer subscribes and blocks on ctx.Done() — it must exit once the
	// pipeline is cancelled, not linger forever.
	consumerMsgChan := make(chan *message.Message)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Return((<-chan *message.Message)(consumerMsgChan), nil).AnyTimes()

	consumer := NewConsumer(pipelineID, "sink1", mockSub, nil, nil, nil, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	pipeline := NewPipeline(pipelineID, producer, []*Consumer{consumer}, cfg)

	require.NoError(t, pipeline.Start(context.Background()))

	select {
	case <-pipeline.Finished():
		// Expected: the KV.Get failure cancelled the pipeline, consumers exited,
		// and wg.Wait() unblocked.
	case <-time.After(3 * time.Second):
		t.Fatal("Pipeline zombied: Finished() did not close after a KV.Get failure in the producer goroutine (Critical 13 regression)")
	}
}

// TestPipeline_DynamicTablesChan_GoroutineExitsOnShutdown guards against the
// goroutine leak fixed alongside WI-8: SetDynamicTablesChan's goroutine used
// to range over a channel that is never closed and was not tracked anywhere,
// leaking one goroutine (plus the captured Producer) per pipeline instance.
//
// It deliberately does NOT just assert "Finished() closes after cancel" —
// that was already true before the leak was fixed (the goroutine wasn't
// tracked by anything, so it had no bearing on Finished()) and would pass
// whether or not the goroutine leaked. Instead it measures the goroutine
// count before Start and after Shutdown (which waits on Pipeline.auxWg,
// where the dynamic-tables goroutine now lives) to prove the goroutine
// actually exits rather than leaking silently.
func TestPipeline_DynamicTablesChan_GoroutineExitsOnShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockKeyValue(ctrl)
	mockSub := mocks.NewMockSubscriber(ctrl)
	mockSrc := mocks.NewMockSource(ctrl)

	pipelineID := "p-dyntables"
	cfg := protocol.PipelineConfig{
		ID:      pipelineID,
		Sources: []string{"s1"},
		Sinks:   []string{"sink1"},
		Tables:  []string{"t1"},
	}

	srcCfg := protocol.SourceConfig{ID: "s1"}
	data, err := json.Marshal(srcCfg)
	require.NoError(t, err)

	mockKV.EXPECT().Get(protocol.SourceConfigKey("s1")).Return(mockEntry{value: data}, nil).AnyTimes()
	mockKV.EXPECT().Get(gomock.Any()).Return(nil, errors.New("no checkpoint")).AnyTimes()

	// Producer.Run blocks until ctx is cancelled: the source's Start returns a
	// msgChan/ackChan pair that never delivers anything.
	srcMsgChan := make(chan []protocol.Message)
	ackChan := make(chan struct{})
	mockSrc.EXPECT().Start(gomock.Any(), gomock.Any(), gomock.Any()).Return(srcMsgChan, ackChan, nil).AnyTimes()

	producer := NewProducer(pipelineID, "nats://localhost:4222", cfg, mockSrc, nil, mockSub, mockKV, srcCfg)

	consumerMsgChan := make(chan *message.Message)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Return((<-chan *message.Message)(consumerMsgChan), nil).AnyTimes()

	consumer := NewConsumer(pipelineID, "sink1", mockSub, nil, nil, nil, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	pipeline := NewPipeline(pipelineID, producer, []*Consumer{consumer}, cfg)

	baseline := stableGoroutineCount(t)

	require.NoError(t, pipeline.Start(context.Background()))

	// Give the dynamic-tables goroutine and producer a moment to start, then
	// confirm goroutine count actually grew (sanity check that we're
	// measuring the right thing).
	time.Sleep(150 * time.Millisecond)
	afterStart := stableGoroutineCount(t)
	assert.Greater(t, afterStart, baseline, "expected goroutine count to grow after Start (producer, consumer, dynamic-tables goroutines)")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	require.NoError(t, pipeline.Shutdown(shutdownCtx))

	afterShutdown := stableGoroutineCount(t)
	assert.LessOrEqual(t, afterShutdown, baseline,
		"goroutine count did not return to baseline after Shutdown: dynamic-tables goroutine (or something else) leaked")
}

// stableGoroutineCount samples runtime.NumGoroutine a few times with GC in
// between to let recently-exited goroutines actually finish tearing down
// before we snapshot the count, reducing test flakiness.
func stableGoroutineCount(t *testing.T) int {
	t.Helper()
	var n int
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		n = runtime.NumGoroutine()
	}
	return n
}

// TestPipeline_Drain_FinishesWithoutCancel is the regression test the
// coordinator asked for: it exercises Pipeline.Drain() -> Finished() on a
// producer that completes cleanly, and asserts Finished() closes promptly.
// Before the auxWg fix, tracking the dynamic-tables goroutine on the main
// Pipeline.wg meant that on this exact graceful-drain path (which never
// calls p.cancel()) the dynamic-tables goroutine would still be parked on
// <-ctx.Done() forever, so p.wg.Wait() — and therefore Finished() — would
// never return, silently turning every graceful drain into a full
// DrainTimeout stall followed by a forced shutdown.
func TestPipeline_Drain_FinishesWithoutCancel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockKeyValue(ctrl)
	mockSub := mocks.NewMockSubscriber(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSrc := mocks.NewMockSource(ctrl)

	pipelineID := "p-drain"
	cfg := protocol.PipelineConfig{
		ID:      pipelineID,
		Sources: []string{"s1"},
		Sinks:   []string{"sink1"},
		Tables:  []string{"t1"},
	}

	srcCfg := protocol.SourceConfig{ID: "s1"}
	data, err := json.Marshal(srcCfg)
	require.NoError(t, err)

	mockKV.EXPECT().Get(protocol.SourceConfigKey("s1")).Return(mockEntry{value: data}, nil).AnyTimes()
	mockKV.EXPECT().Get(gomock.Any()).Return(nil, errors.New("no checkpoint")).AnyTimes()

	srcMsgChan := make(chan []protocol.Message)
	ackChan := make(chan struct{})
	mockSrc.EXPECT().Start(gomock.Any(), gomock.Any(), gomock.Any()).Return(srcMsgChan, ackChan, nil).AnyTimes()

	consumerMsgChan := make(chan *message.Message, 1)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Return((<-chan *message.Message)(consumerMsgChan), nil).AnyTimes()

	// Simulates the real NATS ingest topic connecting the producer's publish
	// to the consumer's subscription: forward whatever the producer publishes
	// (here, the drain marker) onto the consumer's channel.
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any()).DoAndReturn(func(_ string, wmMsg *message.Message) error {
		consumerMsgChan <- wmMsg
		return nil
	}).AnyTimes()

	producer := NewProducer(pipelineID, "nats://localhost:4222", cfg, mockSrc, mockPub, mockSub, mockKV, srcCfg)
	consumer := NewConsumer(pipelineID, "sink1", mockSub, nil, nil, nil, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	pipeline := NewPipeline(pipelineID, producer, []*Consumer{consumer}, cfg)

	require.NoError(t, pipeline.Start(context.Background()))
	time.Sleep(100 * time.Millisecond)

	// Pipeline.Drain() -> Producer.Drain() cancels only the producer's
	// source-scoped context (cancelSource), never p.ctx. In production the
	// source reacts to that by closing its msgChan; the mock source here does
	// the same directly to simulate that reaction.
	require.NoError(t, pipeline.Drain())
	close(srcMsgChan)

	select {
	case <-pipeline.Finished():
		// Expected: runProducer's normal-completion path ran cons.Drain(lsn),
		// the consumer saw the drain marker and returned nil, and the caller
		// in Start() never called p.cancel() because runProducer returned nil.
	case <-time.After(3 * time.Second):
		t.Fatal("Finished() did not close after a graceful Drain(): a goroutine tracked on p.wg is blocked on something that only p.cancel() (not Drain()) unblocks")
	}
}
