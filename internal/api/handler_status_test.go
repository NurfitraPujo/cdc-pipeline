package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/api/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// TestGetPipelineStatusString covers getPipelineStatusString
// (internal/api/handler.go), the single source of truth for pipeline health
// used by both GetPipelineStatus/ListPipelines and (as of this change) the
// dashboard summary computed in GetStatsSummary. Before this change, no test
// exercised any of the status/health branches directly, and a bug fixed
// alongside this test (GetStatsSummary comparing hb.Status == "Ready", a
// status manager.go never emits) went unnoticed because the two call sites
// diverged silently.
func TestGetPipelineStatusString(t *testing.T) {
	const pipelineID = "p1"

	tests := []struct {
		name       string
		setupMocks func(mockKV *mocks.MockKeyValue)
		want       string
	}{
		{
			name: "transitioning takes priority",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				ts := protocol.PipelineTransitionState{ID: pipelineID, Status: "Transitioning", StartedAt: time.Now()}
				data, _ := json.Marshal(ts)
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(mockEntry{value: data}, nil)
			},
			want: "transitioning",
		},
		{
			name: "no heartbeat entry is an error",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
			},
			want: "error",
		},
		{
			name: "stale heartbeat is an error",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				hb := protocol.WorkerHeartbeat{WorkerID: pipelineID, Status: "Running", UpdatedAt: time.Now().Add(-5 * time.Minute)}
				data, _ := json.Marshal(hb)
				mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(pipelineID)).Return(mockEntry{value: data}, nil)
			},
			want: "error",
		},
		{
			name: "fresh Running heartbeat is healthy",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				hb := protocol.WorkerHeartbeat{WorkerID: pipelineID, Status: "Running", UpdatedAt: time.Now()}
				data, _ := json.Marshal(hb)
				mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(pipelineID)).Return(mockEntry{value: data}, nil)
			},
			want: "healthy",
		},
		{
			name: "fresh Retrying heartbeat is an error, not healthy",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				// manager.go's monitorWorker never emits "Ready"; a fresh
				// heartbeat that is being updated on schedule but reports a
				// non-Running status (e.g. stuck in retry backoff after a
				// processor construction failure, WS-8 item 2) must still be
				// surfaced as unhealthy.
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				hb := protocol.WorkerHeartbeat{WorkerID: pipelineID, Status: "Retrying", UpdatedAt: time.Now()}
				data, _ := json.Marshal(hb)
				mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(pipelineID)).Return(mockEntry{value: data}, nil)
			},
			want: "error",
		},
		{
			name: "fresh heartbeat with no status is healthy",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				hb := protocol.WorkerHeartbeat{WorkerID: pipelineID, Status: "", UpdatedAt: time.Now()}
				data, _ := json.Marshal(hb)
				mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(pipelineID)).Return(mockEntry{value: data}, nil)
			},
			want: "healthy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockKV := mocks.NewMockKeyValue(ctrl)
			tt.setupMocks(mockKV)

			h := NewHandler(mockKV)
			got := h.getPipelineStatusString(pipelineID)
			assert.Equal(t, tt.want, got)
		})
	}
}
