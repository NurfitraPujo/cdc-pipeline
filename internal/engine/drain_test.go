package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// Test 17: a single wmMsg wrapping [OpSchemaChange, OpInsert, OpInsert] must
// be acked exactly once, and only after BOTH ApplySchema succeeded AND
// BatchUpload of the two inserts succeeded. This is the flushWithFilter /
// pendingSchema wiring (WI-9): before this wiring, flushWithFilter was dead
// code and the mixed wrapper's data rows rode along in `batch` while the
// wrapper itself was excluded from wmMsgs and acked immediately after
// ApplySchema — acking before the data was durable.
func TestConsumer_MixedSchemaAndData_AckOnlyAfterBothDurable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	pub := newFakePublisher(0)

	c := NewConsumer("p1", "sinkA", mockSub, pub, mockSink, nil, mockKV, 10, 20*time.Millisecond, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgChan := make(chan *message.Message, 1)
	mockSub.EXPECT().Subscribe(gomock.Any(), "topic1").Return(msgChan, nil)
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

	// ApplySchema returns promptly; BatchUpload blocks until released so we
	// can observe the wrapper is NOT acked while data is still in flight.
	applySchemaCalled := make(chan struct{})
	mockSink.EXPECT().ApplySchema(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ protocol.Message) error {
		close(applySchemaCalled)
		return nil
	})
	batchUploadEntered := make(chan struct{})
	releaseBatchUpload := make(chan struct{})
	mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, msgs []protocol.Message) error {
		close(batchUploadEntered)
		<-releaseBatchUpload
		require.Len(t, msgs, 2, "expected both inserts, and only the inserts, in the durable write")
		return nil
	})

	errChan := make(chan error, 1)
	go func() {
		errChan <- c.Run(ctx, "topic1")
	}()

	time.Sleep(30 * time.Millisecond)

	schemaMsg := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpSchemaChange, Schema: &protocol.SchemaMetadata{Table: "t1"}}
	insert1 := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 10}
	insert2 := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 20}
	data, err := protocol.MessageBatch{schemaMsg, insert1, insert2}.MarshalMsg(nil)
	require.NoError(t, err)

	wmMsg := message.NewMessage("mixed-1", data)
	msgChan <- wmMsg

	select {
	case <-applySchemaCalled:
	case <-time.After(time.Second):
		t.Fatal("ApplySchema was never called")
	}

	// Give the batch timer a chance to fire and reach BatchUpload.
	select {
	case <-batchUploadEntered:
	case <-time.After(time.Second):
		t.Fatal("BatchUpload was never called for the wrapper's data rows")
	}

	// Schema applied, data upload in flight: wrapper must not be acked yet.
	select {
	case <-wmMsg.Acked():
		t.Fatal("wrapper was acked before its data rows were durably written")
	case <-time.After(150 * time.Millisecond):
		// expected
	}

	close(releaseBatchUpload)

	select {
	case <-wmMsg.Acked():
	case <-time.After(time.Second):
		t.Fatal("wrapper was never acked after ApplySchema and BatchUpload both succeeded")
	}

	// Exactly one RecordAck published, covering only the two insert LSNs.
	require.Len(t, pub.publishedByTopic[protocol.AcksTopic("p1")], 1)
	recordAck := decodeRecordAck(t, pub.publishedByTopic[protocol.AcksTopic("p1")][0])
	assert.Equal(t, []uint64{10, 20}, recordAck.LSNs)

	cancel()
	select {
	case <-errChan:
	case <-time.After(time.Second):
		t.Fatal("consumer did not exit after context cancel")
	}
}

// Test 17 (crash branch): if BatchUpload fails after ApplySchema succeeded
// (simulating a crash / durable-write failure), the wrapper must be Nacked,
// never acked, so JetStream redelivers the whole envelope (schema change is
// idempotent to re-apply; the data rows are retried).
func TestConsumer_MixedSchemaAndData_BatchUploadFailure_NacksNotAcks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	pub := newFakePublisher(0)

	retryCfg := protocol.RetryConfig{MaxRetries: 5, InitialInterval: time.Millisecond, MaxInterval: 2 * time.Millisecond}
	c := NewConsumer("p1", "sinkA", mockSub, pub, mockSink, nil, mockKV, 10, 20*time.Millisecond, retryCfg, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgChan := make(chan *message.Message, 1)
	mockSub.EXPECT().Subscribe(gomock.Any(), "topic1").Return(msgChan, nil)
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

	mockSink.EXPECT().ApplySchema(gomock.Any(), gomock.Any()).Return(nil)
	mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(errors.New("simulated crash: durable write failed")).AnyTimes()

	errChan := make(chan error, 1)
	go func() {
		errChan <- c.Run(ctx, "topic1")
	}()

	time.Sleep(30 * time.Millisecond)

	schemaMsg := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpSchemaChange, Schema: &protocol.SchemaMetadata{Table: "t1"}}
	insert1 := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 10}
	insert2 := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 20}
	data, err := protocol.MessageBatch{schemaMsg, insert1, insert2}.MarshalMsg(nil)
	require.NoError(t, err)

	wmMsg := message.NewMessage("mixed-2", data)
	msgChan <- wmMsg

	select {
	case <-wmMsg.Nacked():
	case <-time.After(2 * time.Second):
		t.Fatal("wrapper was never Nacked after BatchUpload failure")
	}

	select {
	case <-wmMsg.Acked():
		t.Fatal("wrapper must never be acked when its data upload failed")
	default:
	}

	cancel()
	select {
	case <-errChan:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not exit after context cancel")
	}
}

// Test 18: checkDrained is live. Drain(targetLSN) is called; a batch whose
// max LSN crosses targetLSN is flushed; the consumer must return on its own
// once that flush lands, without ever seeing (or needing) a drain_marker
// message.
func TestConsumer_CheckDrained_ReturnsWithoutDrainMarker(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	pub := newFakePublisher(0)

	c := NewConsumer("p1", "sinkA", mockSub, pub, mockSink, nil, mockKV, 10, 20*time.Millisecond, protocol.RetryConfig{MaxRetries: 3}, nil, nil)
	c.Drain(15)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgChan := make(chan *message.Message, 1)
	mockSub.EXPECT().Subscribe(gomock.Any(), "topic1").Return(msgChan, nil)
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
	mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(nil)

	errChan := make(chan error, 1)
	go func() {
		errChan <- c.Run(ctx, "topic1")
	}()

	time.Sleep(30 * time.Millisecond)

	batch := protocol.MessageBatch{
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, LSN: 20}, // crosses target of 15
	}
	data, err := batch.MarshalMsg(nil)
	require.NoError(t, err)
	wmMsg := message.NewMessage("wm-drain", data)
	msgChan <- wmMsg

	select {
	case err := <-errChan:
		assert.NoError(t, err, "consumer should return nil once the drain target LSN is reached")
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not self-terminate after crossing the drain target LSN")
	}

	select {
	case <-wmMsg.Acked():
	default:
		t.Fatal("wmMsg carrying the LSN that crossed the drain target should have been acked by the flush")
	}
}

// fakePendingCounter is a stream.PendingCounter test double whose NumPending
// sequence is scripted by the test. Guarded by a mutex since
// drainBufferedUntilIdle's polling ticker fires on its own goroutine.
type fakePendingCounter struct {
	mu       sync.Mutex
	calls    int
	sequence []uint64 // last value repeats once exhausted
	err      error
}

func (f *fakePendingCounter) PendingCount(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	idx := f.calls
	if idx >= len(f.sequence) {
		idx = len(f.sequence) - 1
	}
	f.calls++
	return f.sequence[idx], nil
}

func (f *fakePendingCounter) setSequence(seq []uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sequence = seq
	f.calls = 0
}

func (f *fakePendingCounter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// Test: drainBufferedUntilIdle terminates on NumPending==0 (server-side
// truth), not on a fixed client-side idle timer. The old implementation
// declared the buffer empty after 1s of channel silence; this drives the
// fake PendingCounter through several nonzero polls first, proving the
// function keeps waiting past what would have been the old 1s window before
// completing on the zero reading.
func TestDrainBufferedUntilIdle_TerminatesOnZeroPending_NotOnTimer(t *testing.T) {
	pub := newFakePublisher(0)
	p := &Producer{pipelineID: "p1", publisher: pub}

	msgChan := make(chan *message.Message)
	pc := &fakePendingCounter{sequence: []uint64{3, 2, 1, 0}}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	done, err := p.drainBufferedUntilIdle(ctx, "t1", msgChan, "main-topic", pc)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.True(t, done)
	assert.GreaterOrEqual(t, pc.callCount(), 4, "expected the drain to poll through the nonzero readings before the zero one")
	// bufferDrainPendingCheckInterval is 200ms; reaching the 4th poll takes
	// >600ms, comfortably past what the old 1s idle timer would have needed
	// to (wrongly) declare the buffer empty on the first tick.
	assert.GreaterOrEqual(t, elapsed, 600*time.Millisecond)
}

// Test: under redelivery lag exceeding what used to be the 1s idle-timeout
// window, a buffered message that arrives late is still republished — no
// message is stranded just because more than a second of quiet passed
// first.
func TestDrainBufferedUntilIdle_DoesNotStrandMessagesUnderRedeliveryLag(t *testing.T) {
	pub := newFakePublisher(0)
	p := &Producer{pipelineID: "p1", publisher: pub}

	msgChan := make(chan *message.Message)
	// Pending count never reaches zero until the deferred message is
	// consumed and republished by the test below cancelling via close.
	pc := &fakePendingCounter{sequence: []uint64{1}}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resultCh := make(chan struct {
		done bool
		err  error
	}, 1)
	go func() {
		done, err := p.drainBufferedUntilIdle(ctx, "t1", msgChan, "main-topic", pc)
		resultCh <- struct {
			done bool
			err  error
		}{done, err}
	}()

	// Simulate redelivery lag well past the old 1s idle window before the
	// buffered message actually shows up on the channel.
	time.Sleep(1200 * time.Millisecond)

	late := message.NewMessage("late-1", []byte("payload"))
	msgChan <- late

	select {
	case <-late.Acked():
	case <-time.After(time.Second):
		t.Fatal("late-arriving buffered message was never republished/acked")
	}
	require.Len(t, pub.published, 1)
	assert.Equal(t, late.UUID, pub.published[0].UUID)

	// Now let PendingCount settle at zero so the drain can conclude.
	pc.setSequence([]uint64{0})

	select {
	case res := <-resultCh:
		require.NoError(t, res.err)
		assert.True(t, res.done)
	case <-time.After(2 * time.Second):
		t.Fatal("drain never completed after pending count settled at zero")
	}
}

// Regression coverage for the review round that rejected the first WI-9
// pass: NumPending==0 alone does NOT mean the backlog is empty (it excludes
// NumAckPending — messages already delivered/prefetched or awaiting
// redelivery). PendingCount is responsible for returning the SUM, so any
// caller checking `== 0` gets the right answer. This test drives the fake
// counter the way a real NumPending==0 && NumAckPending>0 consumer would
// report through that summed API (i.e. a nonzero total that never reaches
// zero), and asserts the drain does NOT complete — it must keep waiting,
// not declare victory on the NumPending-only signal.
func TestDrainBufferedUntilIdle_DoesNotCompleteWhileAckPendingNonZero(t *testing.T) {
	pub := newFakePublisher(0)
	p := &Producer{pipelineID: "p1", publisher: pub}

	msgChan := make(chan *message.Message)
	// Simulates NumPending==0 but NumAckPending==1 (e.g. one prefetched or
	// Nacked-awaiting-redelivery message): PendingCount must report this as
	// non-empty, so this fake always returns a nonzero total.
	pc := &fakePendingCounter{sequence: []uint64{1}}

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()

	done, err := p.drainBufferedUntilIdle(ctx, "t1", msgChan, "main-topic", pc)

	assert.ErrorIs(t, err, context.DeadlineExceeded, "drain must not have concluded on its own")
	assert.False(t, done, "drain must not report done while ack-pending backlog is nonzero")
	assert.GreaterOrEqual(t, pc.callCount(), 2, "expected the drain to keep polling rather than exit on the first nonzero reading")
}

// Regression coverage: transitionTableToCDC's final verification (the one
// piece that runs under the muTableStates write lock) must not spin
// indefinitely when PendingCount errors (e.g. a NATS outage). It must fail
// fast and release the lock, bounded by bufferDrainPendingCheckTimeout, not
// hang the producer's main publish path (which needs the read lock).
func TestTransitionTableToCDC_PendingCountError_FailsFastAndReleasesLock(t *testing.T) {
	p := &Producer{
		pipelineID:  "p1",
		kv:          nil, // must not be reached: PendingCount errors before any KV write
		tableStates: map[string]string{"t1": protocol.TableStateDraining},
	}

	pc := &fakePendingCounter{err: errors.New("nats down")}

	start := time.Now()
	transitioned, err := p.transitionTableToCDC(context.Background(), "s1", "t1", func() (bool, error) {
		return true, nil // the unlocked verify step succeeds instantly
	}, pc)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.False(t, transitioned)
	// bufferDrainPendingCheckTimeout bounds the single locked recheck; this
	// must return in well under that many multiples, not spin.
	assert.Less(t, elapsed, 2*bufferDrainPendingCheckTimeout, "final recheck must fail fast on a PendingCount error, not retry under the lock")

	// The write lock must have been released: a concurrent RLock (as
	// publishBufferBatch takes) must succeed immediately.
	lockAcquired := make(chan struct{})
	go func() {
		p.muTableStates.RLock()
		defer p.muTableStates.RUnlock()
		close(lockAcquired)
	}()
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("muTableStates write lock was not released after the PendingCount error")
	}

	// Table state must be unchanged (still Draining), not left CDC on a
	// failed/uncertain final check.
	p.muTableStates.RLock()
	defer p.muTableStates.RUnlock()
	assert.Equal(t, protocol.TableStateDraining, p.tableStates["t1"])
}

// Test: transitionTableToCDC must not strand a table in Draining after a
// single unlucky interleaving where the locked final recheck observes
// pending != 0 (something landed in the buffer between the unlocked verify
// and the lock acquisition). Before the bounded retry loop, the first
// nonzero locked recheck would return (false, nil) immediately and the
// table would only recover on the next external trigger. This drives the
// fake PendingCounter to report nonzero on the first locked recheck and
// zero on a later one, and asserts the transition still succeeds.
func TestTransitionTableToCDC_RetriesPastOneUnluckyRecheck_ThenSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)

	const (
		pipelineID = "p1"
		sourceID   = "s1"
		table      = "t1"
	)
	stateKey := protocol.TableStateKey(pipelineID, sourceID, protocol.TableRef{Schema: "public", Table: table})
	kv.EXPECT().Put(stateKey, []byte(protocol.TableStateCDC)).Return(uint64(1), nil)

	p := &Producer{
		pipelineID:  pipelineID,
		kv:          kv,
		tableStates: map[string]string{table: protocol.TableStateDraining},
	}

	// Every locked recheck call gets the next value off this sequence: the
	// first attempt's locked recheck observes pending == 1 (unlucky
	// interleaving); the second attempt's locked recheck observes 0 and the
	// transition succeeds.
	pc := &fakePendingCounter{sequence: []uint64{1, 0}}

	verifyCalls := 0
	transitioned, err := p.transitionTableToCDC(context.Background(), sourceID, table, func() (bool, error) {
		// The unlocked verify step itself always reports empty; the race is
		// modeled entirely by the locked recheck's PendingCounter sequence.
		verifyCalls++
		return true, nil
	}, pc)

	require.NoError(t, err)
	assert.True(t, transitioned, "the bounded retry must recover from a single unlucky nonzero recheck")
	assert.Equal(t, 2, verifyCalls, "expected exactly one retried unlocked verify after the first unlucky locked recheck")
	assert.Equal(t, 2, pc.callCount(), "expected exactly two locked recheck calls")

	p.muTableStates.RLock()
	defer p.muTableStates.RUnlock()
	assert.Equal(t, protocol.TableStateCDC, p.tableStates[table])
}

// Test: when every attempt's locked recheck keeps observing pending != 0,
// transitionTableToCDC must exhaust its bounded retries and return cleanly
// (false, nil) without leaving the table anywhere but Draining, and without
// holding the write lock afterward — exactly the exhausted-case contract
// TestTransitionTableToCDC_PendingCountError_FailsFastAndReleasesLock
// asserts for the error path.
func TestTransitionTableToCDC_ExhaustsRetries_LeavesTableDrainingAndReleasesLock(t *testing.T) {
	p := &Producer{
		pipelineID:  "p1",
		kv:          nil, // must not be reached: pending never settles at 0
		tableStates: map[string]string{"t1": protocol.TableStateDraining},
	}

	// Always reports nonzero: every attempt's locked recheck fails.
	pc := &fakePendingCounter{sequence: []uint64{1}}

	transitioned, err := p.transitionTableToCDC(context.Background(), "s1", "t1", func() (bool, error) {
		return true, nil
	}, pc)

	require.NoError(t, err)
	assert.False(t, transitioned)
	assert.Equal(t, transitionToCDCMaxAttempts, pc.callCount(), "expected exactly transitionToCDCMaxAttempts locked rechecks")

	lockAcquired := make(chan struct{})
	go func() {
		p.muTableStates.RLock()
		defer p.muTableStates.RUnlock()
		close(lockAcquired)
	}()
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("muTableStates write lock was not released after exhausting retries")
	}

	p.muTableStates.RLock()
	defer p.muTableStates.RUnlock()
	assert.Equal(t, protocol.TableStateDraining, p.tableStates["t1"])
}
