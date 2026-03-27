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
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"golang.org/x/crypto/bcrypt"
)

func setupTestRouter(kv nats.KeyValue) *gin.Engine {
	os.Setenv("JWT_SECRET", "test-secret")
	gin.SetMode(gin.TestMode)
	r := gin.Default()
	h := NewHandler(kv)

	v1 := r.Group("/api/v1")
	{
		v1.POST("/login", h.Login)
		authorized := v1.Group("/")
		authorized.Use(AuthMiddleware())
		{
			authorized.GET("/global", h.GetGlobalConfig)
			authorized.PUT("/global", h.UpdateGlobalConfig)

			stats := authorized.Group("/stats")
			{
				stats.GET("/summary", h.GetStatsSummary)
				stats.GET("/history", h.GetStatsHistory)
			}
			
			pipelines := authorized.Group("/pipelines")
			{
				pipelines.GET("", h.ListPipelines)
				pipelines.POST("", h.CreatePipeline)
				pipelines.GET("/:id", h.GetPipeline)
				pipelines.PUT("/:id", h.UpdatePipeline)
				pipelines.DELETE("/:id", h.DeletePipeline)
				pipelines.GET("/:id/status", h.GetPipelineStatus)
				pipelines.POST("/:id/restart", h.RestartPipeline)
				pipelines.GET("/:id/metrics", h.StreamMetrics)
			}

			sources := authorized.Group("/sources")
			{
				sources.GET("", h.ListSources)
				sources.POST("", h.CreateSource)
				sources.GET("/:id", h.GetSource)
				sources.PUT("/:id", h.UpdateSource)
				sources.DELETE("/:id", h.DeleteSource)
				sources.GET("/:id/schema", h.GetSourceSchema)
				sources.GET("/:id/tables", h.ListSourceTables)
			}

			sinks := authorized.Group("/sinks")
			{
				sinks.GET("", h.ListSinks)
				sinks.POST("", h.CreateSink)
				sinks.PUT("/:id", h.UpdateSink)
				sinks.DELETE("/:id", h.DeleteSink)
			}

			workers := authorized.Group("/workers")
			{
				workers.GET("/:id/heartbeat", h.GetWorkerHeartbeat)
			}
		}
	}
	return r
}

func getTestToken(t *testing.T, router *gin.Engine, mockKV *mocks.MockKeyValue) string {
	hashed, _ := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
	user := protocol.UserConfig{Username: "admin", Password: string(hashed)}
	data, _ := json.Marshal(user)

	mockKV.EXPECT().Get(protocol.KeyAuthConfig).Return(mockEntry{value: data}, nil)
	loginBody, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "password",
	})
	req, _ := http.NewRequest("POST", "/api/v1/login", bytes.NewBuffer(loginBody))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	return resp["token"]
}

type mockEntry struct {
	key   string
	value []byte
}
func (m mockEntry) Key() string { return m.key }
func (m mockEntry) Value() []byte { return m.value }
func (m mockEntry) Revision() uint64 { return 0 }
func (m mockEntry) Created() time.Time { return time.Now() }
func (m mockEntry) Delta() uint64 { return 0 }
func (m mockEntry) Operation() nats.KeyValueOp { return 0 }
func (m mockEntry) Bucket() string { return "test" }

var _ nats.KeyValueEntry = mockEntry{}

type MockWatcher struct {
	updates chan nats.KeyValueEntry
}
func (m *MockWatcher) Updates() <-chan nats.KeyValueEntry { return m.updates }
func (m *MockWatcher) Stop() error { return nil }
func (m *MockWatcher) Context() context.Context { return context.Background() }

func TestAPI_Full(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("Global Config", func(t *testing.T) {
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)
		authHeader := "Bearer " + token

		// GET
		gCfg := protocol.GlobalConfig{BatchSize: 100, BatchWait: time.Second}
		gData, _ := json.Marshal(gCfg)
		mockKV.EXPECT().Get(protocol.KeyGlobalConfig).Return(mockEntry{value: gData}, nil)

		req, _ := http.NewRequest("GET", "/api/v1/global", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// PUT (Success)
		mockKV.EXPECT().Keys().Return([]string{"other.key"}, nil)
		mockKV.EXPECT().Put(protocol.KeyGlobalConfig, gomock.Any()).Return(uint64(1), nil)
		
		putBody, _ := json.Marshal(gCfg)
		req, _ = http.NewRequest("PUT", "/api/v1/global", bytes.NewBuffer(putBody))
		req.Header.Set("Authorization", authHeader)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("Pipeline Lifecycle", func(t *testing.T) {
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)
		authHeader := "Bearer " + token

		p1 := protocol.PipelineConfig{ID: "p1", Name: "Pipe 1", Sources: []string{"s1"}, Sinks: []string{"snk1"}, Tables: []string{"t1"}}
		pData, _ := json.Marshal(p1)

		// CREATE
		mockKV.EXPECT().Get(protocol.TransitionStateKey("p1")).Return(nil, nats.ErrKeyNotFound)
		mockKV.EXPECT().Get(protocol.SourceConfigKey("s1")).Return(mockEntry{value: []byte("{}")}, nil)
		mockKV.EXPECT().Get(protocol.SinkConfigKey("snk1")).Return(mockEntry{value: []byte("{}")}, nil)
		mockKV.EXPECT().Put(protocol.PipelineConfigKey("p1"), gomock.Any()).Return(uint64(1), nil)

		req, _ := http.NewRequest("POST", "/api/v1/pipelines", bytes.NewBuffer(pData))
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusCreated, w.Code)

		// LIST
		mockKV.EXPECT().Keys().Return([]string{protocol.PipelineConfigKey("p1")}, nil).AnyTimes()
		mockKV.EXPECT().Get(protocol.PipelineConfigKey("p1")).Return(mockEntry{value: pData}, nil).AnyTimes()
		
		req, _ = http.NewRequest("GET", "/api/v1/pipelines", nil)
		req.Header.Set("Authorization", authHeader)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// GET STATUS
		mockKV.EXPECT().Keys().Return([]string{protocol.PipelineStatusPrefix("p1") + "table1.stats"}, nil).AnyTimes()
		mockKV.EXPECT().Get(protocol.PipelineStatusPrefix("p1") + "table1.stats").Return(mockEntry{value: []byte("{}")}, nil).AnyTimes()

		req, _ = http.NewRequest("GET", "/api/v1/pipelines/p1/status", nil)
		req.Header.Set("Authorization", authHeader)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("Source and Table Discovery", func(t *testing.T) {
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)
		authHeader := "Bearer " + token

		s1 := protocol.SourceConfig{ID: "s1", Type: "postgres", Host: "h", Port: 5432, User: "u", Database: "d"}
		sData, _ := json.Marshal(s1)

		// CREATE
		mockKV.EXPECT().Put(protocol.SourceConfigKey("s1"), gomock.Any()).Return(uint64(1), nil)
		req, _ := http.NewRequest("POST", "/api/v1/sources", bytes.NewBuffer(sData))
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusCreated, w.Code)

		// LIST TABLES
		metaKey := fmt.Sprintf("cdc.pipeline.p1.sources.s1.tables.users.metadata")
		mockKV.EXPECT().Keys().Return([]string{metaKey}, nil).AnyTimes()
		mockKV.EXPECT().Get(metaKey).Return(mockEntry{value: []byte(`{"table":"users"}`)}, nil).AnyTimes()

		req, _ = http.NewRequest("GET", "/api/v1/sources/s1/tables", nil)
		req.Header.Set("Authorization", authHeader)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("Worker Heartbeat", func(t *testing.T) {
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)
		authHeader := "Bearer " + token

		hb := protocol.WorkerHeartbeat{WorkerID: "w1", Status: "online"}
		hbData, _ := json.Marshal(hb)
		mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey("w1")).Return(mockEntry{value: hbData}, nil)

		req, _ := http.NewRequest("GET", "/api/v1/workers/w1/heartbeat", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("New Endpoints", func(t *testing.T) {
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)
		authHeader := "Bearer " + token

		// Stats Summary (Cached)
		summaryData, _ := json.Marshal(protocol.StatsSummary{TotalPipelines: 1, HealthyCount: 1})
		mockKV.EXPECT().Get(protocol.KeyGlobalSummary).Return(mockEntry{value: summaryData}, nil)

		req, _ := http.NewRequest("GET", "/api/v1/stats/summary", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Stats History (Empty)
		req, _ = http.NewRequest("GET", "/api/v1/stats/history", nil)
		req.Header.Set("Authorization", authHeader)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "[]", w.Body.String())

		// Pipeline Restart
		mockKV.EXPECT().Get(protocol.PipelineConfigKey("p1")).Return(mockEntry{value: []byte("{}")}, nil)
		mockKV.EXPECT().Put(protocol.TransitionStateKey("p1"), gomock.Any()).Return(uint64(1), nil)
		req, _ = http.NewRequest("POST", "/api/v1/pipelines/p1/restart", nil)
		req.Header.Set("Authorization", authHeader)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusAccepted, w.Code)

		// Source Schema
		req, _ = http.NewRequest("GET", "/api/v1/sources/s1/schema", nil)
		req.Header.Set("Authorization", authHeader)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
	}

