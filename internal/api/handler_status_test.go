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
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
			},
			want: "error",
		},
		{
			name: "stale heartbeat is an error",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
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
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
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
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
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
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
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

// TestGetPipelineLifecycleStatus is WS-1's split of health from lifecycle
// state (plan section 4.1): "no worker" must not always mean "broken" once a
// pipeline can be deliberately paused or stopped, and health must be
// reported empty -- not "error" -- whenever the lifecycle isn't Running.
func TestGetPipelineLifecycleStatus(t *testing.T) {
	const pipelineID = "p1"

	tests := []struct {
		name       string
		setupMocks func(mockKV *mocks.MockKeyValue)
		want       PipelineLifecycleStatus
	}{
		{
			name: "transitioning takes priority over desired_state",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				ts := protocol.PipelineTransitionState{ID: pipelineID, Status: "Transitioning", StartedAt: time.Now()}
				data, _ := json.Marshal(ts)
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(mockEntry{value: data}, nil)
			},
			want: PipelineLifecycleStatus{Lifecycle: "Transitioning"},
		},
		{
			name: "desired_state=paused with no worker is not an error",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStatePaused}
				data, _ := json.Marshal(cfg)
				mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: data}, nil)
				// No worker heartbeat is ever consulted for a paused pipeline.
			},
			want: PipelineLifecycleStatus{Lifecycle: "Paused"},
		},
		{
			name: "desired_state=stopped with no worker is not an error",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateStopped}
				data, _ := json.Marshal(cfg)
				mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: data}, nil)
			},
			want: PipelineLifecycleStatus{Lifecycle: "Stopped"},
		},
		{
			name: "desired_state empty (pre-WS-1 config) with no heartbeat is Running+error",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
			},
			want: PipelineLifecycleStatus{Lifecycle: "Running", Health: "error"},
		},
		{
			name: "desired_state=running with fresh heartbeat is Running+healthy",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
				cdata, _ := json.Marshal(cfg)
				mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cdata}, nil)
				hb := protocol.WorkerHeartbeat{WorkerID: pipelineID, Status: "Running", UpdatedAt: time.Now()}
				hdata, _ := json.Marshal(hb)
				mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(pipelineID)).Return(mockEntry{value: hdata}, nil)
			},
			want: PipelineLifecycleStatus{Lifecycle: "Running", Health: "healthy"},
		},
		{
			// WS-4's WAL guard escalation persists a Reason on the
			// PipelineLifecycleRecord (plan section 7) explaining why it
			// drove the pipeline to Stopping. That must surface here --
			// getPipelineLifecycleStatus is the one place a persisted
			// record is read for display -- not be dropped on the floor.
			name: "persisted record with a WAL guard reason surfaces it",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				rec := protocol.PipelineLifecycleRecord{
					State:  protocol.StateStopping,
					Reason: `replication slot wal_status reached "unreserved"`,
				}
				data, _ := json.Marshal(rec)
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: data}, nil)
			},
			want: PipelineLifecycleStatus{
				Lifecycle: "Stopping",
				Reason:    `replication slot wal_status reached "unreserved"`,
			},
		},
		{
			// Plan invariant 5: leaving Snapshotting after a stop window
			// always marks reconciliation stale, and "stale must be visible
			// in the UI ... hiding it would recreate the 'reports healthy
			// while diverging' failure this plan exists to prevent". A
			// pipeline can be Running AND healthy AND owed a delete sweep --
			// all three at once -- so the marker must survive alongside a
			// healthy verdict rather than being masked by it.
			name: "stale reconciliation survives a Running+healthy pipeline",
			setupMocks: func(mockKV *mocks.MockKeyValue) {
				mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
				rec := protocol.PipelineLifecycleRecord{
					State:          protocol.StateRunning,
					Reconciliation: protocol.ReconciliationStale,
				}
				data, _ := json.Marshal(rec)
				mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: data}, nil)
				hb := protocol.WorkerHeartbeat{WorkerID: pipelineID, Status: "Running", UpdatedAt: time.Now()}
				hdata, _ := json.Marshal(hb)
				mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(pipelineID)).Return(mockEntry{value: hdata}, nil)
			},
			want: PipelineLifecycleStatus{
				Lifecycle:      "Running",
				Health:         "healthy",
				Reconciliation: protocol.ReconciliationStale,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockKV := mocks.NewMockKeyValue(ctrl)
			tt.setupMocks(mockKV)

			h := NewHandler(mockKV)
			got := h.getPipelineLifecycleStatus(pipelineID)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPipelineWithStatus_EmitsLifecycleJSON pins the wire shape, not the
// struct. Both GET /pipelines and GET /pipelines/{id} render through
// pipelineWithStatus, so this is the only surface on which an operator can
// see that a Running pipeline is still owed a delete sweep.
//
// It asserts on the raw JSON deliberately. A struct-level assertion still
// passes if someone re-adds `json:"-"` to PipelineLifecycleStatus.Reconciliation
// or drops the field from the extras struct in pipelineWithStatus -- and that
// read path has already regressed once during this workstream.
func TestPipelineWithStatus_EmitsLifecycleJSON(t *testing.T) {
	const pipelineID = "p1"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockKeyValue(ctrl)
	mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
	rec := protocol.PipelineLifecycleRecord{
		State:          protocol.StateRunning,
		Reconciliation: protocol.ReconciliationStale,
	}
	data, _ := json.Marshal(rec)
	mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: data}, nil)
	hb := protocol.WorkerHeartbeat{WorkerID: pipelineID, Status: "Running", UpdatedAt: time.Now()}
	hdata, _ := json.Marshal(hb)
	mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(pipelineID)).Return(mockEntry{value: hdata}, nil)

	h := NewHandler(mockKV)
	raw, err := h.pipelineWithStatus(protocol.PipelineConfig{ID: pipelineID, Name: "p1"})
	assert.NoError(t, err)

	var m map[string]any
	assert.NoError(t, json.Unmarshal(raw, &m))

	// The config fields must survive the splice too -- that is the whole
	// reason pipelineWithStatus exists rather than an embedding wrapper.
	assert.Equal(t, pipelineID, m["id"], "config fields must survive the splice")
	assert.Equal(t, "Running", m["lifecycle_state"])
	assert.Equal(t, "healthy", m["health"])
	assert.Equal(t, "stale", m["reconciliation"],
		"invariant 5: the stale marker must reach the wire, not just the struct")
}
