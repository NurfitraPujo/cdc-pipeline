package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestConsumer_LoadStats(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)
	c := NewConsumer("p1", "sink1", mockSub, nil, nil, nil, mockKV, 10, time.Second, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	st := protocol.TableStats{Status: "ACTIVE", TotalSynced: 100}
	data, _ := json.Marshal(st)
	key := protocol.TableStatsKey("p1", "s1", "sink1", "t1")
	mockKV.EXPECT().Get(key).Return(mockEntry{value: data}, nil)

	c.LoadStats("s1", []string{"t1"})
	assert.Equal(t, uint64(100), c.stats["s1.t1"].TotalSynced)
}

func TestUpperCaseTransformer(t *testing.T) {
	factory, ok := transformer.GetTransformer("uppercase")
	assert.True(t, ok)

	tf, err := factory(map[string]interface{}{"column": "name"})
	assert.NoError(t, err)

	msg := &protocol.Message{
		Data: map[string]interface{}{
			"name": "john doe",
			"age":  30,
		},
	}

	result, keep, err := tf.Transform(context.Background(), msg)
	assert.NoError(t, err)
	assert.True(t, keep)
	assert.Equal(t, "JOHN DOE", result.Data["name"])
	assert.Equal(t, 30, result.Data["age"])
}

func TestProducer_Drain(t *testing.T) {
	p := &Producer{}
	// Test safe drain when cancel is nil
	assert.Nil(t, p.Drain())

	ctx, cancel := context.WithCancel(context.Background())
	p.cancelSource = cancel
	assert.Nil(t, p.Drain())
	assert.Error(t, ctx.Err()) // Should be canceled
}

func TestConsumer_FailurePaths(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	c := NewConsumer("p1", "sink1", mockSub, nil, mockSink, nil, mockKV, 10, 100*time.Millisecond, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	t.Run("Sink BatchUpload Failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		msgChan := make(chan *message.Message, 1)
		mockSub.EXPECT().Subscribe(gomock.Any(), "topic1").Return(msgChan, nil)

		// Create a batch
		m := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpInsert}
		batch := []protocol.Message{m}
		data, _ := protocol.MessageBatch(batch).MarshalMsg(nil)

		wmMsg := message.NewMessage("1", data)
		msgChan <- wmMsg

		// Expect failure
		mockSink.EXPECT().BatchUpload(gomock.Any(), gomock.Any()).Return(errors.New("sink down"))

		// In flush on error, we update stats to ERROR
		mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

		// Run in goroutine
		errChan := make(chan error, 1)
		go func() {
			errChan <- c.Run(ctx, "topic1")
		}()

		// Give it time to process
		time.Sleep(200 * time.Millisecond)

		cancel()
		err := <-errChan
		assert.Error(t, err) // ctx canceled
	})

	t.Run("Schema Change Failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		msgChan := make(chan *message.Message, 1)
		mockSub.EXPECT().Subscribe(gomock.Any(), "topic2").Return(msgChan, nil)

		m := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpSchemaChange, Schema: &protocol.SchemaMetadata{Table: "t1"}}
		batch := []protocol.Message{m}
		data, _ := protocol.MessageBatch(batch).MarshalMsg(nil)

		wmMsg := message.NewMessage("2", data)
		msgChan <- wmMsg

		mockSink.EXPECT().ApplySchema(gomock.Any(), gomock.Any()).Return(errors.New("ddl failed"))

		errChan := make(chan error, 1)
		go func() {
			errChan <- c.Run(ctx, "topic2")
		}()

		time.Sleep(200 * time.Millisecond)
		err := <-errChan
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to apply schema change")
	})
}

func TestProducer_FailurePaths(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrc := mocks.NewMockSource(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSub := mocks.NewMockSubscriber(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	cfg := protocol.PipelineConfig{ID: "p1", Sources: []string{"s1"}}
	srcCfg := protocol.SourceConfig{ID: "s1"}
	ackChanMock := make(chan *message.Message)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Return(ackChanMock, nil).AnyTimes()
	p := NewProducer("p1", "nats://localhost:4222", cfg, mockSrc, mockPub, mockSub, mockKV, srcCfg)
	cp := protocol.Checkpoint{}

	t.Run("Source Start Failure", func(t *testing.T) {
		mockSrc.EXPECT().Start(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil, errors.New("pg failed"))

		_, err := p.Run(context.Background(), srcCfg, cp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pg failed")
	})

	t.Run("Publisher Failure and Circuit Breaker", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		srcMsgChan := make(chan []protocol.Message, 1)
		ackChan := make(chan struct{}, 1)
		mockSrc.EXPECT().Start(gomock.Any(), gomock.Any(), gomock.Any()).Return(srcMsgChan, ackChan, nil)

		srcMsgChan <- []protocol.Message{{SourceID: "s1", Table: "t1", Op: protocol.OpInsert}}

		// Publish fails
		mockPub.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(errors.New("nats down")).AnyTimes()

		// Circuit breaker logic: Get stats, update status to CIRCUIT_OPEN
		mockKV.EXPECT().Get(gomock.Any()).Return(mockEntry{value: []byte("{}")}, nil).AnyTimes()
		mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()

		errChan := make(chan error, 1)
		go func() {
			_, err := p.Run(ctx, srcCfg, cp)
			errChan <- err
		}()

		time.Sleep(200 * time.Millisecond)
		cancel()
		err := <-errChan
		assert.Error(t, err) // ctx canceled
	})
}

func TestProducer_DiscoveryEvolution(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSrc := mocks.NewMockSource(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSub := mocks.NewMockSubscriber(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	cfg := protocol.PipelineConfig{ID: "p2", Sources: []string{"s1"}, Tables: []string{"t1"}}
	srcCfg := protocol.SourceConfig{ID: "s1"}
	ackChanMock := make(chan *message.Message)
	mockSub.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Return(ackChanMock, nil).AnyTimes()
	p := NewProducer("p2", "nats://localhost:4222", cfg, mockSrc, mockPub, mockSub, mockKV, srcCfg)

	ctx := context.Background()

	// Warm cache first
	m1 := protocol.Message{
		SourceID: "s1",
		Op:       protocol.OpSchemaChange,
		LSN:      100,
		Schema: &protocol.SchemaMetadata{
			Table:   "t1",
			Columns: map[string]string{"id": "int"},
		},
	}
	p.handleDiscovery(ctx, m1)

	p.muEvo.RLock()
	state := p.evoStates["t1"]
	assert.NotNil(t, state)
	assert.Equal(t, protocol.SchemaStatusStable, state.Status)
	assert.Equal(t, "int", state.CachedSchema["id"])
	p.muEvo.RUnlock()

	// New columns in discovery
	m2 := protocol.Message{
		SourceID: "s1",
		Op:       protocol.OpSchemaChange,
		LSN:      101,
		Schema: &protocol.SchemaMetadata{
			Table:   "t1",
			Columns: map[string]string{"id": "int", "new_col": "text"},
		},
	}

	// Expectations for evolution
	mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Return(uint64(2), nil)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)

	p.handleDiscovery(ctx, m2)

	p.muEvo.RLock()
	assert.Equal(t, protocol.SchemaStatusFrozen, state.Status)
	assert.Equal(t, "text", state.CachedSchema["new_col"])
	assert.NotEmpty(t, state.CorrelationID)
	p.muEvo.RUnlock()
}

type mockEntry struct {
	key   string
	value []byte
}

func (m mockEntry) Key() string                { return m.key }
func (m mockEntry) Value() []byte              { return m.value }
func (m mockEntry) Revision() uint64           { return 0 }
func (m mockEntry) Created() time.Time         { return time.Now() }
func (m mockEntry) Delta() uint64              { return 0 }
func (m mockEntry) Operation() nats.KeyValueOp { return 0 }
func (m mockEntry) Bucket() string             { return "" }

type mockTransformer struct {
	name         string
	callCount    int
	ops          []protocol.OperationType
	transformErr error
}

func (m *mockTransformer) Name() string { return m.name }
func (m *mockTransformer) Transform(ctx context.Context, msg *protocol.Message) (*protocol.Message, bool, error) {
	m.callCount++
	return msg, true, m.transformErr
}

func TestConsumer_TransformerFiltering(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	retryCfg := protocol.RetryConfig{
		MaxRetries:      2,
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     2 * time.Millisecond,
		EnableDLQ:       false,
	}

	insertTf := &mockTransformer{name: "insert-only", ops: []protocol.OperationType{protocol.OpInsert}}
	allOpsTf := &mockTransformer{name: "all-ops", ops: []protocol.OperationType{protocol.OpInsert, protocol.OpUpdate, protocol.OpDelete}}
	emptyTf := &mockTransformer{name: "empty-ops", ops: nil}

	transformers := []ConfiguredTransformer{
		{Transformer: insertTf, OperationTypes: []protocol.OperationType{protocol.OpInsert}},
		{Transformer: allOpsTf, OperationTypes: []protocol.OperationType{protocol.OpInsert, protocol.OpUpdate, protocol.OpDelete}},
		{Transformer: emptyTf, OperationTypes: nil},
	}

	c := NewConsumer("p1", "sink1", mockSub, mockPub, mockSink, transformers, mockKV, 10, 50*time.Millisecond, retryCfg, nil, nil)

	msgs := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Payload: []byte(`{"id":1}`)},
		{SourceID: "s1", Table: "t1", Op: protocol.OpUpdate, Payload: []byte(`{"id":1}`)},
		{SourceID: "s1", Table: "t1", Op: protocol.OpDelete, Payload: []byte(`{"id":1}`)},
	}

	processed, err := c.processMessages(context.Background(), msgs)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(processed), "all messages should pass through (not filtered)")

	assert.Equal(t, 1, insertTf.callCount, "insert-only transformer should be called once (for insert msg)")
	assert.Equal(t, 3, allOpsTf.callCount, "all-ops transformer should be called 3 times (for all ops)")
	assert.Equal(t, 0, emptyTf.callCount, "empty-ops transformer should never be called (passthrough)")
}

func TestConsumer_TransformerFiltering_NotMatched(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	retryCfg := protocol.RetryConfig{
		MaxRetries:      2,
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     2 * time.Millisecond,
		EnableDLQ:       false,
	}

	schemaChangeTf := &mockTransformer{name: "ddl-only", ops: []protocol.OperationType{protocol.OpSchemaChange}}
	insertTf := &mockTransformer{name: "insert-only", ops: []protocol.OperationType{protocol.OpInsert}}

	transformers := []ConfiguredTransformer{
		{Transformer: schemaChangeTf, OperationTypes: []protocol.OperationType{protocol.OpSchemaChange}},
		{Transformer: insertTf, OperationTypes: []protocol.OperationType{protocol.OpInsert}},
	}

	c := NewConsumer("p1", "sink1", mockSub, mockPub, mockSink, transformers, mockKV, 10, 50*time.Millisecond, retryCfg, nil, nil)

	dmlMsg := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Payload: []byte(`{"id":1}`)}
	ddlMsg := protocol.Message{SourceID: "s1", Table: "t1", Op: protocol.OpSchemaChange, Payload: []byte(`{}`)}

	processed, err := c.processMessages(context.Background(), []protocol.Message{dmlMsg, ddlMsg})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(processed), "both messages should pass through")

	assert.Equal(t, 1, insertTf.callCount, "insert transformer called only for insert msg")
	assert.Equal(t, 1, schemaChangeTf.callCount, "ddl transformer called only for schema_change msg")
}

type mockFuncTransformer struct {
	name          string
	transformFunc func(msg *protocol.Message) (*protocol.Message, bool, error)
}

func (m *mockFuncTransformer) Name() string { return m.name }
func (m *mockFuncTransformer) Transform(ctx context.Context, msg *protocol.Message) (*protocol.Message, bool, error) {
	return m.transformFunc(msg)
}

type mockBatchTransformer struct {
	mockTransformer
	batchCallCount int
	batchErr       error
}

func (m *mockBatchTransformer) TransformBatch(ctx context.Context, msgs []protocol.Message) ([]protocol.Message, error) {
	m.batchCallCount++
	if m.batchErr != nil {
		return nil, m.batchErr
	}
	return msgs, nil
}

type mockFilteringTransformer struct {
	name string
}

func (m *mockFilteringTransformer) Name() string { return m.name }
func (m *mockFilteringTransformer) Transform(ctx context.Context, msg *protocol.Message) (*protocol.Message, bool, error) {
	if msg.UUID == "1" {
		return nil, false, nil
	}
	return msg, true, nil
}

func TestConsumer_DDL_Transformation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	tf := &mockFuncTransformer{
		name: "schema-modifier",
		transformFunc: func(msg *protocol.Message) (*protocol.Message, bool, error) {
			if msg.Op == protocol.OpSchemaChange && msg.Schema != nil {
				msg.Schema.Columns["extra"] = "text"
			}
			return msg, true, nil
		},
	}

	transformers := []ConfiguredTransformer{
		{Transformer: tf, OperationTypes: []protocol.OperationType{protocol.OpSchemaChange}},
	}

	c := NewConsumer("p1", "sink1", mockSub, mockPub, mockSink, transformers, mockKV, 1, 100*time.Millisecond, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	ddlMsg := protocol.Message{
		SourceID: "s1",
		Table:    "t1",
		Op:       protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:   "t1",
			Columns: map[string]string{"id": "int"},
		},
	}

	transformed, err := c.processMessages(context.Background(), []protocol.Message{ddlMsg})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(transformed))
	assert.Equal(t, "text", transformed[0].Schema.Columns["extra"])
}

func TestConsumer_DDL_Resilience(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	tf := &mockTransformer{name: "failing-tf", transformErr: errors.New("NATS timeout")}
	transformers := []ConfiguredTransformer{
		{Transformer: tf, OperationTypes: []protocol.OperationType{protocol.OpSchemaChange}},
	}

	c := NewConsumer("p1", "sink1", mockSub, mockPub, mockSink, transformers, mockKV, 1, 100*time.Millisecond, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	ddlMsg := protocol.Message{
		SourceID: "s1",
		Table:    "t1",
		Op:       protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table: "t1",
		},
	}

	_, err := c.processMessages(context.Background(), []protocol.Message{ddlMsg})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NATS timeout")
}

func TestConsumer_BatchTransformerSupport(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	bt := &mockBatchTransformer{
		mockTransformer: mockTransformer{name: "batch-tf"},
	}

	transformers := []ConfiguredTransformer{
		{Transformer: bt, OperationTypes: []protocol.OperationType{protocol.OpInsert}},
	}

	c := NewConsumer("p1", "sink1", mockSub, mockPub, mockSink, transformers, mockKV, 1, 100*time.Millisecond, protocol.RetryConfig{MaxRetries: 3}, nil, nil)

	msgs := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, UUID: "1"},
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, UUID: "2"},
	}

	processed, err := c.processMessages(context.Background(), msgs)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(processed))
	assert.Equal(t, 1, bt.batchCallCount, "TransformBatch should be called exactly once")
}

func TestConsumer_HooksAndFilteredIndices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSub := mocks.NewMockSubscriber(ctrl)
	mockPub := mocks.NewMockPublisher(ctrl)
	mockSink := mocks.NewMockSink(ctrl)
	mockKV := mocks.NewMockKeyValue(ctrl)

	filteringTf := &mockFilteringTransformer{name: "filter-even"}
	transformers := []ConfiguredTransformer{
		{Transformer: filteringTf, OperationTypes: []protocol.OperationType{protocol.OpInsert}},
	}

	var preCalled, postCalled bool
	var capturedFiltered []int

	preHook := func(ctx context.Context, pipelineID string, transformerNames []string, msgs []protocol.Message) []string {
		preCalled = true
		return []string{"corr-1"}
	}

	postHook := func(ctx context.Context, pipelineID string, correlationIDs []string, transformerNames []string, msgs []protocol.Message, processed []protocol.Message, filtered []int) {
		postCalled = true
		capturedFiltered = filtered
	}

	c := NewConsumer("p1", "sink1", mockSub, mockPub, mockSink, transformers, mockKV, 1, 100*time.Millisecond, protocol.RetryConfig{MaxRetries: 3}, preHook, postHook)

	msgs := []protocol.Message{
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, UUID: "1"},
		{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, UUID: "2"},
	}

	processed, err := c.processMessages(context.Background(), msgs)
	assert.NoError(t, err)
	assert.True(t, preCalled)
	assert.True(t, postCalled)
	assert.Equal(t, 1, len(processed))
	assert.Equal(t, []int{0}, capturedFiltered, "index 0 should be filtered")
}
