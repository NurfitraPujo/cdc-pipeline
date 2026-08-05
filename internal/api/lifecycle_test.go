package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/api/mocks"
	"github.com/NurfitraPujo/cdc-pipeline/internal/config"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// setupTestRouterWithHandler is setupTestRouter (api_test.go), except it
// also returns the *Handler so a test can call handler-only setters like
// SetSlotLagRateSampler that setupTestRouter's signature has no room for.
func setupTestRouterWithHandler(kv nats.KeyValue) (*gin.Engine, *Handler) {
	_ = os.Setenv("JWT_SECRET", "test-secret")
	gin.SetMode(gin.TestMode)
	r := gin.Default()
	h := NewHandler(kv)

	v1 := r.Group("/api/v1")
	{
		v1.POST("/login", h.Login)
		authorized := v1.Group("/")
		authorized.Use(AuthMiddleware())
		{
			pipelines := authorized.Group("/pipelines")
			{
				pipelines.POST("/:id/pause", h.PausePipeline)
				pipelines.POST("/:id/start", h.StartPipeline)
				pipelines.POST("/:id/stop", h.StopPipeline)
				pipelines.GET("/:id/pause-projection", h.PausePauseProjectionRoute)
			}
		}
	}
	return r, h
}

// TestPausePipeline covers PausePipeline (internal/api/handler.go): it must
// drive Running -> Paused through protocol.Transition (never write the
// lifecycle state directly), persist paused_until only when a TTL is given,
// and flip desired_state to "paused" so ConfigManager's existing
// honourDesiredState/stopWorker machinery drains the worker.
func TestPausePipeline(t *testing.T) {
	const pipelineID = "p1"

	t.Run("running pipeline with no ttl still gets a bounded paused_until (4h ceiling default)", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)

		var putRecData []byte
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).DoAndReturn(
			func(_ string, data []byte) (uint64, error) {
				putRecData = data
				return 1, nil
			},
		)
		var putCfgData []byte
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).DoAndReturn(
			func(_ string, data []byte) (uint64, error) {
				putCfgData = data
				return 1, nil
			},
		)

		before := time.Now().UTC()
		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		after := time.Now().UTC()

		assert.Equal(t, http.StatusOK, w.Code)

		// A body-less pause must not bypass the 4h ceiling by leaving
		// paused_until nil -- see PausePipeline's doc comment and the WS-3
		// blocking finding this replaces. paused_until must land within
		// [before+4h, after+4h].
		var rec protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &rec))
		assert.Equal(t, protocol.StatePaused, rec.State)
		if assert.NotNil(t, rec.PausedUntil) {
			assert.False(t, rec.PausedUntil.Before(before.Add(protocol.MaxPauseTTL)))
			assert.False(t, rec.PausedUntil.After(after.Add(protocol.MaxPauseTTL)))
		}

		var persistedRec protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(putRecData, &persistedRec))
		assert.Equal(t, protocol.StatePaused, persistedRec.State)
		assert.NotNil(t, persistedRec.PausedUntil)

		var persistedCfg protocol.PipelineConfig
		assert.NoError(t, json.Unmarshal(putCfgData, &persistedCfg))
		assert.Equal(t, protocol.DesiredStatePaused, persistedCfg.DesiredState)
	})

	t.Run("ttl is persisted as an absolute paused_until timestamp", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).Return(uint64(1), nil)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		before := time.Now().UTC()
		body, _ := json.Marshal(map[string]string{"ttl": "30m"})
		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		after := time.Now().UTC()

		assert.Equal(t, http.StatusOK, w.Code)

		var rec protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &rec))
		assert.Equal(t, protocol.StatePaused, rec.State)
		if assert.NotNil(t, rec.PausedUntil) {
			assert.True(t, !rec.PausedUntil.Before(before.Add(30*time.Minute)))
			assert.True(t, !rec.PausedUntil.After(after.Add(30*time.Minute)))
		}
	})

	t.Run("ttl is honoured even when Content-Length is unknown (chunked/streamed body)", func(t *testing.T) {
		// Regression test for the validator finding: PausePipeline must not
		// gate the body bind on c.Request.ContentLength > 0. A chunked
		// request, an HTTP/2 stream, or any client that omits
		// Content-Length reports ContentLength == -1, and skipping the
		// bind there silently drops ttl -- turning a bounded pause into an
		// unbounded one.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).Return(uint64(1), nil)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		body, _ := json.Marshal(map[string]string{"ttl": "30m"})
		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = -1 // simulate chunked/streamed body with unknown length

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var rec protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &rec))
		assert.Equal(t, protocol.StatePaused, rec.State)
		assert.NotNil(t, rec.PausedUntil, "ttl must not be silently dropped when ContentLength is unknown")
	})

	t.Run("invalid ttl is rejected before touching state", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		body, _ := json.Marshal(map[string]string{"ttl": "not-a-duration"})
		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("pausing an already-paused pipeline extends it, recomputing paused_until", func(t *testing.T) {
		// (Paused, pause) is a legal transition to Paused (plan section 11:
		// "make extending it trivial") -- see
		// internal/protocol/lifecycle.go's {StatePaused, EventPause} row.
		// It must succeed with 200 and move paused_until later, not 409 the
		// way every other already-there transition (already-running,
		// already-stopping, ...) correctly does.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStatePaused}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		originalPausedUntil := time.Now().Add(15 * time.Minute)
		rec := protocol.PipelineLifecycleRecord{
			State:       protocol.StatePaused,
			PausedUntil: &originalPausedUntil,
			UpdatedAt:   time.Now(),
		}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)

		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(1), nil)
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).
			DoAndReturn(func(_ string, data []byte) (uint64, error) {
				var newRec protocol.PipelineLifecycleRecord
				require.NoError(t, json.Unmarshal(data, &newRec))
				assert.Equal(t, protocol.StatePaused, newRec.State)
				require.NotNil(t, newRec.PausedUntil)
				assert.True(t, newRec.PausedUntil.After(originalPausedUntil),
					"extended paused_until (%s) must be later than the original (%s)",
					newRec.PausedUntil, originalPausedUntil)
				return 1, nil
			})

		body, _ := json.Marshal(map[string]string{"ttl": "4h"})
		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("unknown pipeline is 404", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("failed config write does not leave the lifecycle record claiming Paused", func(t *testing.T) {
		// Regression test for the validator finding: desired_state must be
		// written (and its marshal error checked) BEFORE the lifecycle
		// record, so that a failed config Put never leaves a persisted
		// record saying "Paused" while desired_state -- and the worker --
		// never actually changed. If this ordering regresses, the
		// LifecycleStateKey Put below is called even though the config Put
		// failed, and gomock's "no unexpected calls" check fails.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(0), fmt.Errorf("kv put failed"))
		// No expectation for Put(LifecycleStateKey, ...): it must not be reached.

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// WS-3: enforce the 4h ceiling on requested TTL (plan section 2/OQ-3).
	t.Run("ttl beyond the 4h ceiling is rejected", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		// No LifecycleStateKey Get/Put expected: the ttl is rejected before
		// the lifecycle record is even read.

		body, _ := json.Marshal(map[string]string{"ttl": "4h1m"})
		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("ttl exactly at the 4h ceiling is accepted", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).Return(uint64(1), nil)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		body, _ := json.Marshal(map[string]string{"ttl": "4h"})
		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	// WS-3: the projected time-to-breach warning (plan section 5) is only
	// surfaced when a SlotLagRateSampler is installed, and only when the
	// projection is shorter than the requested TTL.
	t.Run("warns when projected time-to-breach is shorter than the requested ttl", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router, h := setupTestRouterWithHandler(mockKV)
		// A rate that breaches the 30GB budget in ~1h, well inside a 2h ttl.
		h.SetSlotLagRateSampler(func(_ context.Context, _ string, _ protocol.PipelineConfig) (float64, bool) {
			return float64(protocol.WALBudgetBytes) / time.Hour.Seconds(), true
		})
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).Return(uint64(1), nil)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		body, _ := json.Marshal(map[string]string{"ttl": "2h"})
		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp pausePipelineResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.NotEmpty(t, resp.Warning)
	})

	t.Run("no warning when no sampler is installed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).Return(uint64(1), nil)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		body, _ := json.Marshal(map[string]string{"ttl": "2h"})
		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/pause", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp pausePipelineResponse
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Empty(t, resp.Warning)
	})
}

// TestStartPipeline covers StartPipeline (internal/api/handler.go): the
// Paused -> Running round trip clears paused_until (invariant 3) and flips
// desired_state back to running; illegal transitions (e.g. starting an
// already-running pipeline) are rejected via protocol.Transition's own
// error rather than a handler-side special case.
func TestStartPipeline(t *testing.T) {
	const pipelineID = "p1"

	t.Run("paused pipeline resumes to running and clears paused_until", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStatePaused}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		pausedUntil := time.Now().Add(time.Hour)
		rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, PausedUntil: &pausedUntil, UpdatedAt: time.Now()}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)

		var putRecData []byte
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).DoAndReturn(
			func(_ string, data []byte) (uint64, error) {
				putRecData = data
				return 1, nil
			},
		)
		var putCfgData []byte
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).DoAndReturn(
			func(_ string, data []byte) (uint64, error) {
				putCfgData = data
				return 1, nil
			},
		)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/start", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, protocol.StateRunning, got.State)
		assert.Nil(t, got.PausedUntil)

		var persistedRec protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(putRecData, &persistedRec))
		assert.Equal(t, protocol.StateRunning, persistedRec.State)
		assert.Nil(t, persistedRec.PausedUntil)

		var persistedCfg protocol.PipelineConfig
		assert.NoError(t, json.Unmarshal(putCfgData, &persistedCfg))
		assert.Equal(t, protocol.DesiredStateRunning, persistedCfg.DesiredState)
	})

	t.Run("resume onto a dead slot is rejected rather than silently resumed", func(t *testing.T) {
		// Regression test for the validator finding: StartPipeline must not
		// hardcode SlotAlive: true. A paused pipeline whose installed
		// SlotHealthChecker reports the slot is gone/invalidated must be
		// rejected (409), never allowed through to Resuming -- resuming
		// there would silently drop every change made during the pause
		// (invariant 1).
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router, h := setupTestRouterWithHandler(mockKV)
		token := getTestToken(t, router, mockKV)

		h.SetSlotHealthChecker(func(context.Context, string, protocol.PipelineConfig) config.SlotHealth {
			return config.SlotHealth{Alive: false, WALStatusLost: true}
		})

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStatePaused}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, UpdatedAt: time.Now()}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)
		// No Put expected on either key: the transition is rejected before
		// any state is written.

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/start", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code)
	})

	t.Run("starting an already-running pipeline is a conflict", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		// No lifecycle record yet -> defaults to Running (getLifecycleRecord).
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/start", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code)
	})

	t.Run("unknown pipeline is 404", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/start", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("starting a stopped pipeline lands on NeedsResnapshot without touching desired_state", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateStopped}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		rec := protocol.PipelineLifecycleRecord{State: protocol.StateStopped, UpdatedAt: time.Now()}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)

		// Note: no Put on PipelineConfigKey -- desired_state must stay
		// "stopped" so ConfigManager doesn't start a plain worker for a
		// pipeline that needs a re-snapshot first (invariant 1).
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/start", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, protocol.StateNeedsResnapshot, got.State)
	})

	// WS-6: unlike Stopped -> NeedsResnapshot above, NeedsResnapshot ->
	// Snapshotting must flip desired_state back to running -- that is what
	// lets ConfigManager start the worker whose PostgresSource.shouldResnapshot
	// (internal/source/postgres/source.go) reads this very lifecycle record
	// back and sets Snapshot.Resnapshot: true. This is not invariant 1: the
	// forbidden move is reaching Running WITHOUT passing through
	// Snapshotting, not starting a worker while IN Snapshotting.
	t.Run("starting a NeedsResnapshot pipeline lands on Snapshotting and flips desired_state to running", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateStopped}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		rec := protocol.PipelineLifecycleRecord{State: protocol.StateNeedsResnapshot, UpdatedAt: time.Now()}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)

		var putCfgData []byte
		putCfg := mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).DoAndReturn(
			func(_ string, data []byte) (uint64, error) {
				putCfgData = data
				return 1, nil
			},
		)
		var putRecData []byte
		putRec := mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).DoAndReturn(
			func(_ string, data []byte) (uint64, error) {
				putRecData = data
				return 1, nil
			},
		)
		// Regression for the WS-6 write-ordering bug: the Snapshotting
		// lifecycle record must be durable BEFORE desired_state flips to
		// running, mirroring StopPipeline's rule -- PostgresSource.
		// shouldResnapshot (internal/source/postgres/source.go) reads the
		// lifecycle record back to decide whether to wipe
		// cdc_snapshot_chunks, and it can run as soon as the config write
		// lands.
		gomock.InOrder(putRec, putCfg)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/start", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, protocol.StateSnapshotting, got.State)
		assert.Nil(t, got.PausedUntil)

		var persistedRec protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(putRecData, &persistedRec))
		assert.Equal(t, protocol.StateSnapshotting, persistedRec.State)

		var persistedCfg protocol.PipelineConfig
		assert.NoError(t, json.Unmarshal(putCfgData, &persistedCfg))
		assert.Equal(t, protocol.DesiredStateRunning, persistedCfg.DesiredState)
	})

	t.Run("resume from Paused with a non-integer_range table degrades to reconciliation stale", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router, h := setupTestRouterWithHandler(mockKV)
		token := getTestToken(t, router, mockKV)

		h.SetPartitionStrategyChecker(func(_ context.Context, _ string, _ protocol.PipelineConfig) (bool, []string, bool) {
			return true, []string{"public.big_table"}, true
		})

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStatePaused}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, UpdatedAt: time.Now()}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)

		var putRecData []byte
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).DoAndReturn(
			func(_ string, data []byte) (uint64, error) {
				putRecData = data
				return 1, nil
			},
		)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/start", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, protocol.StateRunning, got.State)
		assert.Equal(t, protocol.ReconciliationStale, got.Reconciliation)
		assert.Contains(t, got.Reason, "public.big_table")

		var persistedRec protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(putRecData, &persistedRec))
		assert.Equal(t, protocol.ReconciliationStale, persistedRec.Reconciliation)
	})

	t.Run("resume from Paused with an unavailable partition-strategy probe is unaffected", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router, h := setupTestRouterWithHandler(mockKV)
		token := getTestToken(t, router, mockKV)

		h.SetPartitionStrategyChecker(func(_ context.Context, _ string, _ protocol.PipelineConfig) (bool, []string, bool) {
			return false, nil, false // probe failed; must never be read as "degraded"
		})

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStatePaused}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, UpdatedAt: time.Now()}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).Return(uint64(1), nil)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/start", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, protocol.StateRunning, got.State)
		assert.Equal(t, protocol.ReconciliationOK, got.Reconciliation)
	})

	// RM-2 regression: a Failed pipeline (e.g. left there by finalizeStop's
	// slot-drop failure, internal/config/manager.go) must not be a
	// permanent dead end -- POST /start has to route it back out via
	// {Failed, start}, taken immediately to Running the same way
	// Paused -> Resuming -> Running is, since the slot survived (drop
	// failed, meaning it was never actually dropped).
	t.Run("starting a Failed pipeline with a live slot resumes to running", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router, h := setupTestRouterWithHandler(mockKV)
		token := getTestToken(t, router, mockKV)

		h.SetSlotHealthChecker(func(context.Context, string, protocol.PipelineConfig) config.SlotHealth {
			return config.SlotHealth{Alive: true}
		})

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateStopped}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		rec := protocol.PipelineLifecycleRecord{State: protocol.StateFailed, Reason: "stop: failed to drop replication slot: boom", UpdatedAt: time.Now()}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)

		var putRecData []byte
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).DoAndReturn(
			func(_ string, data []byte) (uint64, error) {
				putRecData = data
				return 1, nil
			},
		)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/start", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, protocol.StateRunning, got.State)

		var persistedRec protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(putRecData, &persistedRec))
		assert.Equal(t, protocol.StateRunning, persistedRec.State)
	})

	t.Run("starting a Failed pipeline with a dead slot lands on NeedsResnapshot instead of silently resuming", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router, h := setupTestRouterWithHandler(mockKV)
		token := getTestToken(t, router, mockKV)

		h.SetSlotHealthChecker(func(context.Context, string, protocol.PipelineConfig) config.SlotHealth {
			return config.SlotHealth{Alive: false}
		})

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateStopped}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		rec := protocol.PipelineLifecycleRecord{State: protocol.StateFailed, UpdatedAt: time.Now()}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)

		// Note: no Put on PipelineConfigKey -- same invariant-1 reasoning as
		// Stopped -> NeedsResnapshot above: desired_state must stay
		// untouched until the operator's second /start reaches Snapshotting.
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/start", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, protocol.StateNeedsResnapshot, got.State)
	})
}

// TestStopPipeline covers StopPipeline (internal/api/handler.go): it must
// drive Running/Paused -> Stopping through protocol.Transition, persist the
// lifecycle record BEFORE flipping desired_state to "stopped" (the reverse
// of Pause/Start's ordering -- see StopPipeline's doc comment), and leave
// the actual slot-drop to ConfigManager's async finalizeStop.
func TestStopPipeline(t *testing.T) {
	const pipelineID = "p1"

	t.Run("running pipeline moves to Stopping and flips desired_state to stopped", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)

		var putOrder []string
		var putRecData, putCfgData []byte
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).DoAndReturn(
			func(_ string, data []byte) (uint64, error) {
				putOrder = append(putOrder, "record")
				putRecData = data
				return 1, nil
			},
		)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).DoAndReturn(
			func(_ string, data []byte) (uint64, error) {
				putOrder = append(putOrder, "config")
				putCfgData = data
				return 1, nil
			},
		)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/stop", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, protocol.StateStopping, got.State)

		var persistedRec protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(putRecData, &persistedRec))
		assert.Equal(t, protocol.StateStopping, persistedRec.State)

		var persistedCfg protocol.PipelineConfig
		assert.NoError(t, json.Unmarshal(putCfgData, &persistedCfg))
		assert.Equal(t, protocol.DesiredStateStopped, persistedCfg.DesiredState)

		// The lifecycle record must be durable before desired_state is
		// written, so ConfigManager's async finalizeStop never observes a
		// Stopping call it cannot find a record for -- see StopPipeline's
		// doc comment.
		assert.Equal(t, []string{"record", "config"}, putOrder)
	})

	t.Run("paused pipeline also moves to Stopping", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStatePaused}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		pausedUntil := time.Now().Add(time.Hour)
		rec := protocol.PipelineLifecycleRecord{State: protocol.StatePaused, PausedUntil: &pausedUntil, UpdatedAt: time.Now()}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)
		mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).Return(uint64(1), nil)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(1), nil)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/stop", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var got protocol.PipelineLifecycleRecord
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		assert.Equal(t, protocol.StateStopping, got.State)
		// invariant 3: paused_until must be cleared leaving Paused.
		assert.Nil(t, got.PausedUntil)
	})

	t.Run("stopping an already-stopping pipeline is a conflict", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateStopped}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

		rec := protocol.PipelineLifecycleRecord{State: protocol.StateStopping, UpdatedAt: time.Now()}
		recData, _ := json.Marshal(rec)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/stop", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code)
	})

	t.Run("unknown pipeline is 404", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/stop", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("a failed config write rolls the lifecycle record back so a retry is not permanently rejected", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)

		cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
		cfgData, _ := json.Marshal(cfg)
		mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)
		mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)

		// First Put: the Stopping record. Second Put: the config write,
		// which fails. Third Put: StopPipeline's best-effort rollback of
		// the lifecycle record back to what getLifecycleRecord returned
		// (the zero-value default, State: Running, since no record existed
		// yet).
		gomock.InOrder(
			mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).Return(uint64(1), nil),
			mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(0), fmt.Errorf("kv put failed")),
			mockKV.EXPECT().Put(protocol.LifecycleStateKey(pipelineID), gomock.Any()).DoAndReturn(
				func(_ string, data []byte) (uint64, error) {
					var rolledBack protocol.PipelineLifecycleRecord
					assert.NoError(t, json.Unmarshal(data, &rolledBack))
					assert.Equal(t, protocol.StateRunning, rolledBack.State)
					return 1, nil
				},
			),
		)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines/"+pipelineID+"/stop", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

// TestPauseProjectionRouteIsMounted guards the seam that WS-8 shipped broken:
// the projection handler, its OpenAPI entry, the generated types and the whole
// frontend call path all existed, but the route was never registered, so the
// endpoint 404'd in the real server. The pause dialog reads only the query's
// data and ignores its error, so a 404 renders as "no warning" -- silently
// defeating the pre-commit projection this endpoint exists to provide.
//
// Asserting "not 404" rather than a body is deliberate: the point is
// reachability, and the projection's contents are covered elsewhere.
func TestPauseProjectionRouteIsMounted(t *testing.T) {
	const pipelineID = "p1"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockKeyValue(ctrl)
	router, _ := setupTestRouterWithHandler(mockKV)
	token := getTestToken(t, router, mockKV)

	// The handler loads the config before parsing ttl, so feed it one and
	// then fail on the ttl: that yields 400, which is distinguishable from
	// the 404 an unmounted route (or a missing pipeline) would produce.
	cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
	cfgData, _ := json.Marshal(cfg)
	mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

	req, _ := http.NewRequest("GET", "/api/v1/pipelines/"+pipelineID+"/pause-projection?ttl=not-a-duration", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code,
		"GET /pipelines/{id}/pause-projection must be mounted and reach the handler; a 404 here makes the pause dialog silently show no warning")
}

// TestPauseProjection_WarnsWhenProjectedBreachIsShorterThanTTL exercises the
// projection maths (protocol.ProjectedTimeToBreach via
// Handler.projectPauseBreach), not just route mounting: with a
// SlotLagRateSampler installed that reports a rate breaching the WAL budget
// well inside the requested ttl, GET /pipelines/{id}/pause-projection must
// return a non-empty warning -- this is plan section 5's pre-commit
// mitigation for "a 4h pause on a busy source silently becomes a stop +
// re-snapshot", and TestPauseProjectionRouteIsMounted alone (a 400 on a bad
// ttl) never exercises this path.
func TestPauseProjection_WarnsWhenProjectedBreachIsShorterThanTTL(t *testing.T) {
	const pipelineID = "p1"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockKV := mocks.NewMockKeyValue(ctrl)
	router, h := setupTestRouterWithHandler(mockKV)
	// A rate that breaches the 30GB budget in ~1h, well inside a 2h ttl.
	h.SetSlotLagRateSampler(func(_ context.Context, _ string, _ protocol.PipelineConfig) (float64, bool) {
		return float64(protocol.WALBudgetBytes) / time.Hour.Seconds(), true
	})
	token := getTestToken(t, router, mockKV)

	cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
	cfgData, _ := json.Marshal(cfg)
	mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

	req, _ := http.NewRequest("GET", "/api/v1/pipelines/"+pipelineID+"/pause-projection?ttl=2h", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp pausePipelineProjectionResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Warning, "projected breach (~1h) is shorter than the requested ttl (2h); the pre-commit projection must warn")
}

// TestPauseProjection_NoWarningWhenNoSamplerInstalled is the projection
// endpoint's counterpart to TestPauseProjection_WarnsWhenProjectedBreachIsShorterThanTTL:
// without a SlotLagRateSampler, projectPauseBreach has no rate to project
// from and must report "no projection available" rather than a spurious
// warning.
func TestPauseProjection_NoWarningWhenNoSamplerInstalled(t *testing.T) {
	const pipelineID = "p1"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockKV := mocks.NewMockKeyValue(ctrl)
	router := setupTestRouter(mockKV)
	token := getTestToken(t, router, mockKV)

	cfg := protocol.PipelineConfig{ID: pipelineID, DesiredState: protocol.DesiredStateRunning}
	cfgData, _ := json.Marshal(cfg)
	mockKV.EXPECT().Get(protocol.PipelineConfigKey(pipelineID)).Return(mockEntry{value: cfgData}, nil)

	req, _ := http.NewRequest("GET", "/api/v1/pipelines/"+pipelineID+"/pause-projection?ttl=2h", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp pausePipelineProjectionResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Empty(t, resp.Warning)
}

// TestUpdatePipeline_RejectsDirectRunningBypass is RM-1's API-layer test
// (plan section 4.4 invariant 1). PUT /pipelines/{id} with an explicit
// desired_state: "running" body must not be a second path into Running for a
// pipeline whose persisted lifecycle record is Stopped or NeedsResnapshot --
// both reach Running only via Snapshotting (plan section 4.3). Before the
// fix, UpdatePipeline validated desired_state only against the closed set of
// values and never consulted protocol.LifecycleStateKey, so this request
// would have been accepted and handed to ConfigManager's config-watch, which
// starts a plain worker with no Snapshotting hop the moment
// EffectiveDesiredState()==running (invariant 1's exact bypass).
func TestUpdatePipeline_RejectsDirectRunningBypass(t *testing.T) {
	const pipelineID = "p1"

	for _, tc := range []struct {
		name  string
		state protocol.State
	}{
		{"Stopped", protocol.StateStopped},
		{"NeedsResnapshot", protocol.StateNeedsResnapshot},
		// Paused is the state the blocking finding called out: the
		// transition table (internal/protocol/lifecycle.go) routes
		// Paused->EventStart through Resuming, gated on the SlotAlive
		// guard, and a raw PUT desired_state=running must not be a second,
		// unguarded path into Running for it.
		{"Paused", protocol.StatePaused},
		{"Pausing", protocol.StatePausing},
		{"Stopping", protocol.StateStopping},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockKV := mocks.NewMockKeyValue(ctrl)
			router := setupTestRouter(mockKV)
			token := getTestToken(t, router, mockKV)
			authHeader := "Bearer " + token

			stored := protocol.PipelineConfig{
				ID: pipelineID, Name: "Pipe 1", Sources: []string{"s1"}, Sinks: []string{"snk1"},
				Tables:       []string{"t1"},
				DesiredState: protocol.DesiredStateStopped,
			}
			storedData, _ := json.Marshal(stored)

			// The update body explicitly asks for running -- the documented
			// contract the blocking finding described (docs/openapi.yaml
			// publishes desired_state as a writable property).
			updateBody, _ := json.Marshal(map[string]any{
				"name":          "Pipe 1",
				"sources":       []string{"s1"},
				"sinks":         []string{"snk1"},
				"tables":        []string{"t1"},
				"desired_state": "running",
			})

			// desired_state is explicitly present in the body, so
			// UpdatePipeline's "carry forward when omitted" branch never
			// re-reads the stored config; storedData above documents intent
			// only.
			_ = storedData
			mockKV.EXPECT().Get(protocol.SourceConfigKey("s1")).Return(mockEntry{value: []byte("{}")}, nil)
			mockKV.EXPECT().Get(protocol.SinkConfigKey("snk1")).Return(mockEntry{value: []byte("{}")}, nil)
			mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)

			rec := protocol.PipelineLifecycleRecord{State: tc.state, UpdatedAt: time.Now()}
			recData, _ := json.Marshal(rec)
			mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)

			// No Put on the config key must happen: the request must be
			// rejected before it ever reaches ConfigManager's config-watch.
			mockKV.EXPECT().Put(gomock.Any(), gomock.Any()).Times(0)

			req, _ := http.NewRequest("PUT", "/api/v1/pipelines/"+pipelineID, bytes.NewBuffer(updateBody))
			req.Header.Set("Authorization", authHeader)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, http.StatusConflict, w.Code,
				"PUT desired_state=running on a %s pipeline must be rejected (409), not silently start a plain worker", tc.state)
		})
	}
}

// TestUpdatePipeline_AllowsDesiredStateRunningWhenLifecycleAllowsIt is the
// negative-space companion to the bypass test above: RM-1's check must not
// over-reject legitimate PUTs. A pipeline whose lifecycle record is Running
// (or has no record at all -- the getLifecycleRecord default) setting
// desired_state=running is the ordinary, legal case and must still succeed.
func TestUpdatePipeline_AllowsDesiredStateRunningWhenLifecycleAllowsIt(t *testing.T) {
	const pipelineID = "p1"
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockKeyValue(ctrl)
	router := setupTestRouter(mockKV)
	token := getTestToken(t, router, mockKV)
	authHeader := "Bearer " + token

	stored := protocol.PipelineConfig{
		ID: pipelineID, Name: "Pipe 1", Sources: []string{"s1"}, Sinks: []string{"snk1"},
		Tables:       []string{"t1"},
		DesiredState: protocol.DesiredStateRunning,
	}
	storedData, _ := json.Marshal(stored)

	updateBody, _ := json.Marshal(map[string]any{
		"name":          "Pipe 1",
		"sources":       []string{"s1"},
		"sinks":         []string{"snk1"},
		"tables":        []string{"t1"},
		"desired_state": "running",
	})

	// desired_state is explicitly present in the body, so UpdatePipeline's
	// "carry forward when omitted" branch never re-reads the stored config;
	// storedData above documents intent only.
	_ = storedData
	mockKV.EXPECT().Get(protocol.SourceConfigKey("s1")).Return(mockEntry{value: []byte("{}")}, nil)
	mockKV.EXPECT().Get(protocol.SinkConfigKey("snk1")).Return(mockEntry{value: []byte("{}")}, nil)
	mockKV.EXPECT().Get(protocol.TransitionStateKey(pipelineID)).Return(nil, nats.ErrKeyNotFound)

	rec := protocol.PipelineLifecycleRecord{State: protocol.StateRunning, UpdatedAt: time.Now()}
	recData, _ := json.Marshal(rec)
	mockKV.EXPECT().Get(protocol.LifecycleStateKey(pipelineID)).Return(mockEntry{value: recData}, nil)

	mockKV.EXPECT().Put(protocol.PipelineConfigKey(pipelineID), gomock.Any()).Return(uint64(2), nil)

	req, _ := http.NewRequest("PUT", "/api/v1/pipelines/"+pipelineID, bytes.NewBuffer(updateBody))
	req.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
