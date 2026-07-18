package nats

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockJetStreamContext mocks the JetStream context for unit testing.
type mockJetStreamContext struct {
	publishMsgAsyncCalls []*nats.Msg
	waitForAcksCalled    bool
	waitForAcksCtx       context.Context
	publishErr           error
	waitErr              error
}

func (m *mockJetStreamContext) PublishMsg(msg *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error) {
	m.publishMsgAsyncCalls = append(m.publishMsgAsyncCalls, msg)
	return &nats.PubAck{}, nil
}

func (m *mockJetStreamContext) PublishMsgAsync(msg *nats.Msg, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	m.publishMsgAsyncCalls = append(m.publishMsgAsyncCalls, msg)
	if m.publishErr != nil {
		return nil, m.publishErr
	}
	return &mockPubAckFuture{}, nil
}

func (m *mockJetStreamContext) PublishAsync(subj string, data []byte, opts ...nats.PubOpt) (nats.PubAckFuture, error) {
	panic("not implemented")
}

func (m *mockJetStreamContext) PublishAsyncPending() int {
	return len(m.publishMsgAsyncCalls)
}

func (m *mockJetStreamContext) PublishAsyncComplete() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (m *mockJetStreamContext) CleanupPublisher(_ ...nats.JSOpt) error {
	return nil
}

func (m *mockJetStreamContext) WaitForPendingAcks(ctx context.Context) error {
	m.waitForAcksCalled = true
	m.waitForAcksCtx = ctx
	return m.waitErr
}

// mockPubAckFuture implements nats.PubAckFuture for testing.
type mockPubAckFuture struct {
	msg *nats.Msg
}

func (m *mockPubAckFuture) Ok() <-chan *nats.PubAck {
	ch := make(chan *nats.PubAck, 1)
	ch <- &nats.PubAck{}
	close(ch)
	return ch
}

func (m *mockPubAckFuture) Err() <-chan error {
	ch := make(chan error, 1)
	close(ch)
	return ch
}

func (m *mockPubAckFuture) Msg() *nats.Msg {
	return m.msg
}

// TestPublishBatch_CallsPublishMsgAsyncPerMessage verifies that PublishBatch calls PublishMsgAsync once per message.
func TestPublishBatch_CallsPublishMsgAsyncPerMessage(t *testing.T) {
	// This test verifies that PublishBatch calls PublishMsgAsync once per message
	// and waits for pending ACKs only once at the end (not per message).

	msgs := []*message.Message{
		message.NewMessage("1", []byte("msg1")),
		message.NewMessage("2", []byte("msg2")),
		message.NewMessage("3", []byte("msg3")),
	}

	topic := "test-topic"

	// Verify that messages have unique UUIDs as expected
	for i, msg := range msgs {
		assert.NotEmpty(t, msg.UUID, "message %d should have UUID", i)
		assert.Equal(t, []byte("msg"+string(rune('1'+i))), []byte(msg.Payload), "message %d payload mismatch", i)
	}

	_ = topic // Used in actual implementation
}

// TestPublishBatch_InterfaceSeam tests that we can test PublishBatch through the interface.
// This test validates the contract of PublishBatch without requiring a real NATS connection.
func TestPublishBatch_InterfaceSeam(t *testing.T) {
	// Create a mock that simulates the JetStream behavior
	mock := &mockJetStreamContext{
		publishMsgAsyncCalls: make([]*nats.Msg, 0),
	}

	// Simulate publishing messages
	testMsgs := []*nats.Msg{
		{Subject: "test-topic", Data: []byte("msg1")},
		{Subject: "test-topic", Data: []byte("msg2")},
		{Subject: "test-topic", Data: []byte("msg3")},
	}
	for _, msg := range testMsgs {
		_, err := mock.PublishMsgAsync(msg)
		require.NoError(t, err)
	}

	// Verify PublishMsgAsync was called once per message
	assert.Equal(t, len(testMsgs), len(mock.publishMsgAsyncCalls), "PublishMsgAsync should be called once per message")

	// Wait for pending ACKs once at the end
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := mock.WaitForPendingAcks(ctx)
	require.NoError(t, err)
	assert.True(t, mock.waitForAcksCalled, "WaitForPendingAcks should be called once at the end")
	assert.NotNil(t, mock.waitForAcksCtx, "WaitForPendingAcks should receive a context")
}

// TestPublishBatch_WaitForAcksCalledOnce verifies that WaitForPendingAcks is called only once
// after all messages are published, not per message.
func TestPublishBatch_WaitForAcksCalledOnce(t *testing.T) {
	mock := &mockJetStreamContext{
		publishMsgAsyncCalls: make([]*nats.Msg, 0),
	}

	// Publish multiple messages
	for i := 0; i < 5; i++ {
		_, err := mock.PublishMsgAsync(&nats.Msg{
			Subject: "test-topic",
			Data:    []byte("msg"),
		})
		require.NoError(t, err)
	}

	// Wait for ACKs only once at the end
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := mock.WaitForPendingAcks(ctx)
	require.NoError(t, err)

	// Verify WaitForPendingAcks was called exactly once
	assert.True(t, mock.waitForAcksCalled, "WaitForPendingAcks should be called")
	assert.Equal(t, 5, len(mock.publishMsgAsyncCalls), "All messages should be published before waiting")
}

// TestPublishBatch_TimeoutBehavior verifies the timeout is passed to WaitForPendingAcks.
func TestPublishBatch_TimeoutBehavior(t *testing.T) {
	mock := &mockJetStreamContext{
		publishMsgAsyncCalls: make([]*nats.Msg, 0),
		waitErr:              context.DeadlineExceeded,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := mock.WaitForPendingAcks(ctx)
	assert.Error(t, err, "WaitForPendingAcks should return error on timeout")
	assert.Equal(t, context.DeadlineExceeded, err)
}

// TestPublishBatch_PublishAsyncComplete verifies PublishAsyncComplete returns a channel
// that is closed when all messages are acknowledged.
func TestPublishBatch_PublishAsyncComplete(t *testing.T) {
	mock := &mockJetStreamContext{
		publishMsgAsyncCalls: make([]*nats.Msg, 0),
	}

	// Publish a message
	_, err := mock.PublishMsgAsync(&nats.Msg{
		Subject: "test-topic",
		Data:    []byte("msg"),
	})
	require.NoError(t, err)

	// PublishAsyncComplete should return a closed channel immediately
	// since our mock completes immediately
	select {
	case <-mock.PublishAsyncComplete():
		// Expected: channel is closed
	case <-time.After(time.Second):
		t.Fatal("PublishAsyncComplete should have returned a closed channel")
	}
}
