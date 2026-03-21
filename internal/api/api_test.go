package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Helper functions
func setupTestRouter(kv nats.KeyValue) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New() // Use New() to avoid default middleware noise
	h := NewHandler(kv)

	v1 := r.Group("/api/v1")
	{
		v1.POST("/login", Login)
		authorized := v1.Group("/")
		authorized.Use(AuthMiddleware())
		{
			pipelines := authorized.Group("/pipelines")
			{
				pipelines.GET("", h.ListPipelines)
				pipelines.POST("", h.CreatePipeline)
				pipelines.GET("/:id", h.GetPipeline)
				pipelines.GET("/:id/status", h.GetPipelineStatus)
			}
			sources := authorized.Group("/sources")
			{
				sources.GET("", h.ListSources)
				sources.POST("", h.CreateSource)
				sources.GET("/:id/tables", h.ListTables)
			}
		}
	}
	return r
}

func getTestToken(t *testing.T, r *gin.Engine) string {
	loginData := map[string]string{
		"username": "admin",
		"password": "admin",
	}
	body, _ := json.Marshal(loginData)
	req, _ := http.NewRequest("POST", "/api/v1/login", bytes.NewBuffer(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	return resp["token"]
}

// Manual Mock for KeyValueEntry
type mockKeyValueEntry struct {
	key   string
	value []byte
}

func (m *mockKeyValueEntry) Bucket() string          { return "test" }
func (m *mockKeyValueEntry) Key() string             { return m.key }
func (m *mockKeyValueEntry) Value() []byte           { return m.value }
func (m *mockKeyValueEntry) Revision() uint64        { return 1 }
func (m *mockKeyValueEntry) Created() time.Time      { return time.Now() }
func (m *mockKeyValueEntry) Delta() uint64           { return 0 }
func (m *mockKeyValueEntry) Operation() nats.KeyValueOp { return nats.KeyValuePut }

// Mock for KeyValue using testify/mock
type MockKV struct {
	mock.Mock
}

func (m *MockKV) Get(key string) (nats.KeyValueEntry, error) {
	args := m.Called(key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(nats.KeyValueEntry), args.Error(1)
}

func (m *MockKV) Put(key string, value []byte) (uint64, error) {
	args := m.Called(key, value)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockKV) Keys(opts ...nats.WatchOpt) ([]string, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockKV) GetRevision(key string, revision uint64) (nats.KeyValueEntry, error) { return nil, nil }
func (m *MockKV) PutString(key string, value string) (uint64, error)                   { return 0, nil }
func (m *MockKV) Create(key string, value []byte) (uint64, error)                      { return 0, nil }
func (m *MockKV) Update(key string, value []byte, last uint64) (uint64, error)         { return 0, nil }
func (m *MockKV) Delete(key string, opts ...nats.DeleteOpt) error                      { return nil }
func (m *MockKV) Purge(key string, opts ...nats.DeleteOpt) error                       { return nil }
func (m *MockKV) Watch(keys string, opts ...nats.WatchOpt) (nats.KeyWatcher, error)    { return nil, nil }
func (m *MockKV) WatchAll(opts ...nats.WatchOpt) (nats.KeyWatcher, error)              { return nil, nil }
func (m *MockKV) ListKeys(opts ...nats.WatchOpt) (nats.KeyLister, error)               { return nil, nil }
func (m *MockKV) History(key string, opts ...nats.WatchOpt) ([]nats.KeyValueEntry, error) { return nil, nil }
func (m *MockKV) Bucket() string                                                       { return "test" }
func (m *MockKV) PurgeDeletes(opts ...nats.PurgeOpt) error                             { return nil }
func (m *MockKV) Status() (nats.KeyValueStatus, error)                                 { return nil, nil }

func TestAPI_Unit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	
	t.Run("Create Pipeline Success", func(t *testing.T) {
		mockKV := new(MockKV)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router)
		authHeader := "Bearer " + token

		pipeline := protocol.PipelineConfig{ID: "p1", Name: "Test"}
		data, _ := json.Marshal(pipeline)
		mockKV.On("Put", "pipelines.p1.config", data).Return(uint64(1), nil)

		body, _ := json.Marshal(pipeline)
		req, _ := http.NewRequest("POST", "/api/v1/pipelines", bytes.NewBuffer(body))
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)
		mockKV.AssertExpectations(t)
	})

	t.Run("List Pipelines Success", func(t *testing.T) {
		mockKV := new(MockKV)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router)
		authHeader := "Bearer " + token

		mockKV.On("Keys").Return([]string{"pipelines.p1.config"}, nil)
		pipeline := protocol.PipelineConfig{ID: "p1", Name: "Test"}
		data, _ := json.Marshal(pipeline)
		mockKV.On("Get", "pipelines.p1.config").Return(&mockKeyValueEntry{key: "pipelines.p1.config", value: data}, nil)

		req, _ := http.NewRequest("GET", "/api/v1/pipelines", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp map[string][]protocol.PipelineConfig
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Len(t, resp["pipelines"], 1)
		assert.Equal(t, "p1", resp["pipelines"][0].ID)
		mockKV.AssertExpectations(t)
	})

	t.Run("Create Source Success", func(t *testing.T) {
		mockKV := new(MockKV)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router)
		authHeader := "Bearer " + token

		source := protocol.SourceConfig{ID: "s1", Type: "postgres"}
		data, _ := json.Marshal(source)
		mockKV.On("Put", "sources.s1.config", data).Return(uint64(1), nil)

		body, _ := json.Marshal(source)
		req, _ := http.NewRequest("POST", "/api/v1/sources", bytes.NewBuffer(body))
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)
		mockKV.AssertExpectations(t)
	})

	t.Run("Get Pipeline Status Success", func(t *testing.T) {
		mockKV := new(MockKV)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router)
		authHeader := "Bearer " + token

		key := "pipelines.p1.sources.s1.tables.t1.ingress_checkpoint"
		mockKV.On("Keys").Return([]string{key}, nil)
		cp := protocol.Checkpoint{Status: "CDC", IngressLSN: 123}
		data, _ := json.Marshal(cp)
		mockKV.On("Get", key).Return(&mockKeyValueEntry{key: key, value: data}, nil)

		req, _ := http.NewRequest("GET", "/api/v1/pipelines/p1/status", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NotNil(t, resp["status"])
		mockKV.AssertExpectations(t)
	})
}
