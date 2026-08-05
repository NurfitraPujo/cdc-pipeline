package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/engine/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// fakeKVEntry is a minimal nats.KeyValueEntry, matching the pattern
// internal/config/pause_expiry_test.go's fakeEntry uses.
type fakeKVEntry struct {
	value []byte
}

func (e fakeKVEntry) Key() string                { return "test" }
func (e fakeKVEntry) Value() []byte              { return e.value }
func (e fakeKVEntry) Revision() uint64           { return 1 }
func (e fakeKVEntry) Created() time.Time         { return time.Now() }
func (e fakeKVEntry) Delta() uint64              { return 0 }
func (e fakeKVEntry) Operation() nats.KeyValueOp { return 0 }
func (e fakeKVEntry) Bucket() string             { return "test" }

var _ nats.KeyValueEntry = fakeKVEntry{}

// TestShouldResnapshot covers WS-6's shouldResnapshot: the single place a
// Start call decides Snapshot.Resnapshot.
func TestShouldResnapshot(t *testing.T) {
	t.Run("no kv configured resumes (pre-WS-6 default)", func(t *testing.T) {
		s := NewPostgresSource("src1")
		assert.False(t, s.shouldResnapshot(context.Background()))
	})

	t.Run("kv configured but pipelineID empty resumes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kv := mocks.NewMockKeyValue(ctrl)
		s := NewPostgresSource("src1")
		s.mu.Lock()
		s.kv = kv
		s.mu.Unlock()
		assert.False(t, s.shouldResnapshot(context.Background()))
	})

	t.Run("lifecycle record missing (Get error) resumes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kv := mocks.NewMockKeyValue(ctrl)
		kv.EXPECT().Get(protocol.LifecycleStateKey("p1")).Return(nil, assertErr)

		s := NewPostgresSource("src1").WithKV("p1", kv)
		assert.False(t, s.shouldResnapshot(context.Background()))
	})

	t.Run("lifecycle state Snapshotting forces resnapshot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kv := mocks.NewMockKeyValue(ctrl)
		rec := protocol.PipelineLifecycleRecord{State: protocol.StateSnapshotting}
		data, err := json.Marshal(rec)
		assert.NoError(t, err)
		kv.EXPECT().Get(protocol.LifecycleStateKey("p1")).Return(fakeKVEntry{value: data}, nil)

		s := NewPostgresSource("src1").WithKV("p1", kv)
		assert.True(t, s.shouldResnapshot(context.Background()))
	})

	t.Run("lifecycle state Running (resume from pause) does not resnapshot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		kv := mocks.NewMockKeyValue(ctrl)
		rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning}
		data, err := json.Marshal(rec)
		assert.NoError(t, err)
		kv.EXPECT().Get(protocol.LifecycleStateKey("p1")).Return(fakeKVEntry{value: data}, nil)

		s := NewPostgresSource("src1").WithKV("p1", kv)
		assert.False(t, s.shouldResnapshot(context.Background()))
	})
}

// assertErr is a stand-in error used only to make kv.Get fail deterministically.
var assertErr = errNotFound{}

type errNotFound struct{}

func (errNotFound) Error() string { return "not found" }
