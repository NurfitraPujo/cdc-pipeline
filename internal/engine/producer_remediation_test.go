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
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProducerPersistEvoStateRetriesCASFailures(t *testing.T) {
	ctrl := gomock.NewController(t)
	kv := mocks.NewMockKeyValue(ctrl)

	const table = "orders"
	key := protocol.SchemaEvolutionKey("pipeline-1", table)
	state := &tableEvolution{
		Status:            protocol.SchemaStatusFrozen,
		Revision:          10,
		CachedSchema:      map[string]string{"id": "bigint"},
		AcknowledgedSinks: map[string]bool{},
	}
	producer := &Producer{
		pipelineID:  "pipeline-1",
		kv:          kv,
		tableStates: make(map[string]string),
	}

	gomock.InOrder(
		kv.EXPECT().Update(key, gomock.Any(), uint64(10)).Return(uint64(0), errors.New("temporary CAS failure")),
		kv.EXPECT().Get(key).Return(remediationKVEntry{key: key, revision: 11}, nil),
		kv.EXPECT().Update(key, gomock.Any(), uint64(11)).Return(uint64(0), errors.New("temporary CAS failure")),
		kv.EXPECT().Get(key).Return(remediationKVEntry{key: key, revision: 12}, nil),
		kv.EXPECT().Update(key, gomock.Any(), uint64(12)).Return(uint64(13), nil),
	)

	err := producer.persistEvoState(table, state)
	require.NoError(t, err)
	assert.Equal(t, uint64(13), state.Revision)
	assert.Empty(t, producer.tableStates, "a converged CAS write must not pause the table")
}

func TestProducerDrainToCDCRoutesConcurrentInsertToMainStream(t *testing.T) {
	ctrl := gomock.NewController(t)
	publisher := mocks.NewMockPublisher(ctrl)
	kv := mocks.NewMockKeyValue(ctrl)

	const (
		pipelineID = "pipeline-1"
		sourceID   = "source-1"
		table      = "orders"
	)
	mainTopic := "cdc_pipeline_pipeline-1_ingest"
	stateKey := protocol.TableStateKey(pipelineID, sourceID, table)

	publisher.EXPECT().Publish(mainTopic, gomock.Any()).Return(nil)
	kv.EXPECT().Put(stateKey, []byte(protocol.TableStateCDC)).Return(uint64(2), nil)

	producer := &Producer{
		pipelineID:      pipelineID,
		publisher:       publisher,
		kv:              kv,
		cb:              &remediationCircuitBreaker{},
		circuitCoolDown: time.Millisecond,
		evoStates:       make(map[string]*tableEvolution),
		tableStates: map[string]string{
			table: protocol.TableStateDraining,
		},
	}

	verificationStarted := make(chan struct{})
	allowTransition := make(chan struct{})
	transitionDone := make(chan error, 1)
	go func() {
		_, err := producer.transitionTableToCDC(sourceID, table, func() (bool, error) {
			close(verificationStarted)
			<-allowTransition
			return true, nil
		})
		transitionDone <- err
	}()

	<-verificationStarted // transition holds muTableStates.Lock here

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- producer.publishBufferBatch(context.Background(), table, protocol.MessageBatch{{
			SourceID: sourceID,
			Table:    table,
			Op:       protocol.OpInsert,
		}}, 1)
	}()

	select {
	case err := <-publishDone:
		t.Fatalf("CDC insert bypassed the locked empty-queue verification: %v", err)
	case <-time.After(50 * time.Millisecond):
		// Expected: the insert cannot choose a route until the state flip completes.
	}

	close(allowTransition)
	require.NoError(t, <-transitionDone)
	require.NoError(t, <-publishDone)

	producer.muTableStates.RLock()
	state := producer.tableStates[table]
	producer.muTableStates.RUnlock()
	assert.Equal(t, protocol.TableStateCDC, state)
}

func TestProducerPublishWithRetryWaitsForCircuitCoolDown(t *testing.T) {
	ctrl := gomock.NewController(t)
	publisher := mocks.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish("ingest", gomock.Any()).Return(nil)

	const openDuration = 200 * time.Millisecond
	breaker := &remediationCircuitBreaker{openUntil: time.Now().Add(openDuration)}
	producer := &Producer{
		publisher:       publisher,
		cb:              breaker,
		circuitCoolDown: openDuration,
	}

	started := time.Now()
	err := producer.publishWithRetry(context.Background(), "ingest", message.NewMessage("message-1", []byte("payload")), 1)
	elapsed := time.Since(started)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, elapsed, 180*time.Millisecond, "open circuit must wait instead of busy-looping")
	executeCalls, openChecks := breaker.counts()
	assert.Equal(t, 1, executeCalls)
	assert.LessOrEqual(t, openChecks, 3, "open-state polling must be timer paced")
}

type remediationCircuitBreaker struct {
	mu           sync.Mutex
	openUntil    time.Time
	executeCalls int
	openChecks   int
}

func (b *remediationCircuitBreaker) Execute(request func() (interface{}, error)) (interface{}, error) {
	b.mu.Lock()
	b.executeCalls++
	b.mu.Unlock()
	return request()
}

func (b *remediationCircuitBreaker) IsOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.openChecks++
	return time.Now().Before(b.openUntil)
}

func (b *remediationCircuitBreaker) counts() (executeCalls, openChecks int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.executeCalls, b.openChecks
}

type remediationKVEntry struct {
	key      string
	value    []byte
	revision uint64
}

func (e remediationKVEntry) Key() string                { return e.key }
func (e remediationKVEntry) Value() []byte              { return e.value }
func (e remediationKVEntry) Revision() uint64           { return e.revision }
func (e remediationKVEntry) Created() time.Time         { return time.Time{} }
func (e remediationKVEntry) Delta() uint64              { return 0 }
func (e remediationKVEntry) Operation() nats.KeyValueOp { return nats.KeyValuePut }
func (e remediationKVEntry) Bucket() string             { return "test" }
