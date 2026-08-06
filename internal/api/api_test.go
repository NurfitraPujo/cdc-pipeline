package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
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

func (m mockEntry) Key() string                { return m.key }
func (m mockEntry) Value() []byte              { return m.value }
func (m mockEntry) Revision() uint64           { return 0 }
func (m mockEntry) Created() time.Time         { return time.Now() }
func (m mockEntry) Delta() uint64              { return 0 }
func (m mockEntry) Operation() nats.KeyValueOp { return 0 }
func (m mockEntry) Bucket() string             { return "test" }

var _ nats.KeyValueEntry = mockEntry{}

func TestEnsureDevAuth(t *testing.T) {
	tests := []struct {
		name           string
		env            string
		usernameEnv    string
		passwordEnv    string
		kvGetErr       error
		wantUsername   string
		wantSeedCalled bool
	}{
		{
			name:           "ENV=production skips seed",
			env:            "production",
			wantSeedCalled: false,
		},
		{
			name:           "ENV=staging skips seed",
			env:            "staging",
			wantSeedCalled: false,
		},
		{
			name:           "ENV=development seeds default",
			env:            "development",
			kvGetErr:       nats.ErrKeyNotFound,
			wantUsername:   "admin",
			wantSeedCalled: true,
		},
		{
			name:           "ENV=dev with DEV_ADMIN_USERNAME seeds custom",
			env:            "dev",
			usernameEnv:    "foo",
			kvGetErr:       nats.ErrKeyNotFound,
			wantUsername:   "foo",
			wantSeedCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockKV := mocks.NewMockKeyValue(ctrl)

			// Only expect KV access when seeding is enabled (env allowlist).
			if tt.wantSeedCalled {
				if tt.kvGetErr == nil {
					mockKV.EXPECT().Get(protocol.KeyAuthConfig).Return(mockEntry{value: []byte("{}")}, nil)
				} else {
					mockKV.EXPECT().Get(protocol.KeyAuthConfig).Return(nil, tt.kvGetErr)
				}
			}

			var putCalled bool
			if tt.wantSeedCalled {
				mockKV.EXPECT().Put(protocol.KeyAuthConfig, gomock.Any()).DoAndReturn(
					func(key string, data []byte) (uint64, error) {
						putCalled = true
						return 1, nil
					},
				)
			}

			t.Setenv("ENV", tt.env)
			if tt.usernameEnv != "" {
				t.Setenv("DEV_ADMIN_USERNAME", tt.usernameEnv)
			}
			if tt.passwordEnv != "" {
				t.Setenv("DEV_ADMIN_PASSWORD", tt.passwordEnv)
			}

			err := EnsureDevAuth(mockKV)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantSeedCalled, putCalled, "seed mismatch")
		})
	}
}

type MockWatcher struct {
	updates chan nats.KeyValueEntry
}

func (m *MockWatcher) Updates() <-chan nats.KeyValueEntry { return m.updates }
func (m *MockWatcher) Stop() error                        { return nil }
func (m *MockWatcher) Context() context.Context           { return context.Background() }

func TestAPI_Full(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// T3-3: GetEncryptionKey now requires ENCRYPTION_KEY; provide a 32-byte
	// raw key for the duration of this test.
	t.Setenv("ENCRYPTION_KEY", "12345678901234567890123456789012")

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
		mockKV.EXPECT().Watch(protocol.PrefixPipelineConfig+">", gomock.Any()).DoAndReturn(
			func(_ string, _ ...nats.WatchOpt) (nats.KeyWatcher, error) {
				updates := make(chan nats.KeyValueEntry, 2)
				updates <- mockEntry{key: protocol.PipelineConfigKey("p1"), value: pData}
				updates <- nil
				return &MockWatcher{updates: updates}, nil
			},
		).AnyTimes()
		mockKV.EXPECT().Get(protocol.TransitionStateKey("p1")).Return(nil, nats.ErrKeyNotFound).AnyTimes()
		mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey("p1")).Return(nil, nats.ErrKeyNotFound).AnyTimes()

		req, _ = http.NewRequest("GET", "/api/v1/pipelines", nil)
		req.Header.Set("Authorization", authHeader)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// GET STATUS
		mockKV.EXPECT().Keys().Return([]string{protocol.PipelineStatusPrefix("p1") + "table1.stats"}, nil).AnyTimes()
		mockKV.EXPECT().Get(protocol.PipelineStatusPrefix("p1")+"table1.stats").Return(mockEntry{value: []byte("{}")}, nil).AnyTimes()

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

		// s1 carries an explicit `tables` whitelist that includes a table from
		// a schema NOT in `schemas` (inventory.stock). ListSourceTables must
		// surface it even though schema-scoped live discovery would never see
		// it -- otherwise it can't be picked when creating a pipeline.
		s1 := protocol.SourceConfig{ID: "s1", Type: "postgres", Host: "h", Port: 5432, User: "u", Database: "d",
			Schemas: []string{"sales"}, Tables: []string{"inventory.stock"}}
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
		mockKV.EXPECT().Get(metaKey).Return(mockEntry{value: []byte(`{"id":"users","name":"users","schema":"public"}`)}, nil).AnyTimes()
		mockKV.EXPECT().Get(protocol.SourceConfigKey("s1")).Return(mockEntry{value: sData}, nil).AnyTimes()

		req, _ = http.NewRequest("GET", "/api/v1/sources/s1/tables", nil)
		req.Header.Set("Authorization", authHeader)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		// The explicit cross-schema whitelist entry is surfaced alongside the
		// runtime-metadata table, without live DB discovery running.
		assert.Contains(t, w.Body.String(), "inventory")
		assert.Contains(t, w.Body.String(), "stock")
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

		// Source Schema: GetSourceSchema now loads the source config from KV
		// and dials the real database (MULTI_SCHEMA_PLAN.md §3 Stage 3) --
		// it is no longer a hardcoded stub. This unit test has no real
		// Postgres to connect to, so it exercises the KV-lookup wiring and
		// asserts the failure surfaces as 502 rather than a fabricated 200.
		// If GetSourceSchema regresses to the old unconditional mock, this
		// mockKV.Get expectation goes unfulfilled and the test fails.
		s1 := protocol.SourceConfig{ID: "s1", Type: "postgres", Host: "h", Port: 5432, User: "u", Database: "d"}
		s1Data, _ := json.Marshal(s1)
		mockKV.EXPECT().Get(protocol.SourceConfigKey("s1")).Return(mockEntry{value: s1Data}, nil)

		req, _ = http.NewRequest("GET", "/api/v1/sources/s1/schema", nil)
		req.Header.Set("Authorization", authHeader)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadGateway, w.Code)
	})

	t.Run("Source Schema 404 when source unknown", func(t *testing.T) {
		mockKV := mocks.NewMockKeyValue(ctrl)
		router := setupTestRouter(mockKV)
		token := getTestToken(t, router, mockKV)
		authHeader := "Bearer " + token

		mockKV.EXPECT().Get(protocol.SourceConfigKey("nope")).Return(nil, fmt.Errorf("not found"))

		req, _ := http.NewRequest("GET", "/api/v1/sources/nope/schema", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

// TestDiscoverySchemas_EmptyMeansPublicOnly pins MULTI_SCHEMA_PLAN.md §2.4/§8
// item 4: an empty/unset Schemas whitelist must query "public" only, never a
// wildcard. Every pre-Stage-3 source config has Schemas empty (the field was
// dead), so treating empty as "all schemas" would silently start
// discovering every schema on the database the moment this stage lands.

func TestDiscoverySchemas_EmptyMeansPublicOnly(t *testing.T) {
	got := discoverySchemas(nil)
	if len(got) != 1 || got[0] != "public" {
		t.Fatalf("nil Schemas should discover public only, got %v", got)
	}
	got = discoverySchemas([]string{})
	if len(got) != 1 || got[0] != "public" {
		t.Fatalf("empty Schemas should discover public only, got %v", got)
	}
	got = discoverySchemas([]string{"sales", "inventory"})
	if len(got) != 2 || got[0] != "sales" || got[1] != "inventory" {
		t.Fatalf("configured Schemas should pass through unchanged, got %v", got)
	}
}

// TestListPipelines_ConcurrentRequests_NoInlineCleanup verifies that
// ListPipelines no longer performs the heartbeat cleanup scan inline on the
// request path (that now runs on a background ticker, see
// Handler.StartBackgroundCleanup), and that concurrent requests are served
// correctly via the filtered Watch-based enumeration.
func TestListPipelines_ConcurrentRequests_NoInlineCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockKeyValue(ctrl)
	router := setupTestRouter(mockKV)
	token := getTestToken(t, router, mockKV)
	authHeader := "Bearer " + token

	p1 := protocol.PipelineConfig{ID: "p1", Name: "Pipe 1", Sources: []string{"s1"}, Sinks: []string{"snk1"}, Tables: []string{"t1"}}
	pData, _ := json.Marshal(p1)

	// Track total Watch() calls against the pipeline-config prefix across
	// all concurrent requests.
	var watchCallCount atomic.Int64

	mockKV.EXPECT().Watch(protocol.PrefixPipelineConfig+">", gomock.Any()).DoAndReturn(
		func(_ string, _ ...nats.WatchOpt) (nats.KeyWatcher, error) {
			watchCallCount.Add(1)
			updates := make(chan nats.KeyValueEntry, 2)
			updates <- mockEntry{key: protocol.PipelineConfigKey("p1"), value: pData}
			updates <- nil
			return &MockWatcher{updates: updates}, nil
		},
	).AnyTimes()

	// For ListPipelines: pipeline config entries
	mockKV.EXPECT().Get(protocol.TransitionStateKey("p1")).Return(nil, nats.ErrKeyNotFound).AnyTimes()
	mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey("p1")).Return(nil, nats.ErrKeyNotFound).AnyTimes()

	// Since cleanup no longer runs inline on the request path, the handler
	// must never call Watch for the worker-state prefix nor Delete any key
	// while serving ListPipelines. gomock.Controller.Finish() (deferred
	// above) would fail this test if such unexpected calls occurred, since
	// none are registered for that prefix.

	// Fire 5 concurrent ListPipelines requests.
	const numConcurrent = 5
	var wg sync.WaitGroup
	wg.Add(numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest("GET", "/api/v1/pipelines", nil)
			req.Header.Set("Authorization", authHeader)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			assert.Equal(t, http.StatusOK, w.Code)
		}()
	}

	wg.Wait()

	assert.Equal(t, int64(numConcurrent), watchCallCount.Load(),
		"expected exactly one Watch() per ListPipelines request")
}

// --- T2-1 SSRF Protection Tests ---

func TestIsPrivateHost(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		expected bool
	}{
		{"loopback 127.0.0.1", "127.0.0.1", true},
		{"loopback 127.255.255.255", "127.255.255.255", true},
		{"link-local 169.254.0.1", "169.254.0.1", true},
		{"RFC1918 10.0.0.1", "10.0.0.1", true},
		{"RFC1918 10.255.255.255", "10.255.255.255", true},
		{"RFC1918 172.16.0.1", "172.16.0.1", true},
		{"RFC1918 172.31.255.255", "172.31.255.255", true},
		{"RFC1918 192.168.0.1", "192.168.0.1", true},
		{"RFC1918 192.168.255.255", "192.168.255.255", true},
		{"CGNAT 100.64.0.1", "100.64.0.1", true},
		{"CGNAT 100.127.255.255", "100.127.255.255", true},
		{"public 8.8.8.8", "8.8.8.8", false},
		{"public 1.1.1.1", "1.1.1.1", false},
		{"public 203.0.113.1", "203.0.113.1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			result := isPrivateHost(ip)
			assert.Equal(t, tt.expected, result, "isPrivateHost(%s) = %v, want %v", tt.ip, result, tt.expected)
		})
	}
}

func TestValidateHost_RejectsLoopback(t *testing.T) {
	// Test that validateHost rejects loopback addresses
	errMsg := validateHost("localhost")
	assert.Contains(t, errMsg, "not allowed", "Expected localhost to be rejected")
}

func TestValidateHost_RejectsPrivateIPs(t *testing.T) {
	// Test that validateHost rejects private IP ranges
	errMsg := validateHost("192.168.1.1")
	assert.Contains(t, errMsg, "not allowed", "Expected 192.168.1.1 to be rejected")

	errMsg = validateHost("10.0.0.1")
	assert.Contains(t, errMsg, "not allowed", "Expected 10.0.0.1 to be rejected")

	errMsg = validateHost("172.16.0.1")
	assert.Contains(t, errMsg, "not allowed", "Expected 172.16.0.1 to be rejected")
}

func TestValidateHost_AllowsPublicHosts(t *testing.T) {
	// Test that validateHost allows public hosts
	// Note: This test may fail if DNS resolution fails or returns unexpected results
	// We use a well-known public DNS that should always work
	errMsg := validateHost("google.com")
	// If resolution fails, it returns empty string (allows connection attempt)
	// If it resolves and is public, it returns empty string
	// Only fails if it resolves to a private IP
	if errMsg != "" {
		assert.NotContains(t, errMsg, "private IP", "google.com should not resolve to private IP")
	}
}

// --- DB_HOST_ALLOWED_CIDRS allowlist (ADR 0027) ---
// net.LookupIP on a literal IP returns that IP without touching DNS, so these
// tests exercise validateHost deterministically.

func TestValidateHost_AllowlistPermitsPrivateIP(t *testing.T) {
	// The VPC range these tests model: staging/production RDS live on 10.200.x.
	t.Setenv("DB_HOST_ALLOWED_CIDRS", "10.200.0.0/16")

	errMsg := validateHost("10.200.38.64")
	assert.Empty(t, errMsg, "an IP inside the allowlisted CIDR should be permitted despite being private")
}

func TestValidateHost_AllowlistDoesNotWidenBeyondListedRange(t *testing.T) {
	// Allowlisting one private range must not permit unrelated private ranges.
	t.Setenv("DB_HOST_ALLOWED_CIDRS", "10.200.0.0/16")

	errMsg := validateHost("10.0.0.1")
	assert.Contains(t, errMsg, "not allowed", "a private IP outside the allowlisted CIDR must still be rejected")
}

func TestValidateHost_AllowlistNeverPermitsLoopback(t *testing.T) {
	// Even a broad allowlist entry must not expose loopback, since it does not
	// contain 127.x. This guards against an operator accidentally re-opening SSRF.
	t.Setenv("DB_HOST_ALLOWED_CIDRS", "10.0.0.0/8")

	errMsg := validateHost("127.0.0.1")
	assert.Contains(t, errMsg, "not allowed", "loopback must stay blocked regardless of the allowlist")
}

func TestValidateHost_MultipleCIDRsAndWhitespaceTolerated(t *testing.T) {
	t.Setenv("DB_HOST_ALLOWED_CIDRS", " 192.168.0.0/16 , 10.200.0.0/16 ")

	assert.Empty(t, validateHost("192.168.1.1"), "first allowlisted CIDR should be honoured")
	assert.Empty(t, validateHost("10.200.60.184"), "second allowlisted CIDR should be honoured")
}

func TestValidateHost_MalformedCIDRIsSkipped(t *testing.T) {
	// A bad entry is skipped, not fatal; the valid entry still applies.
	t.Setenv("DB_HOST_ALLOWED_CIDRS", "not-a-cidr,10.200.0.0/16")

	assert.Empty(t, validateHost("10.200.38.64"), "valid entry should still take effect")
	assert.Contains(t, validateHost("172.16.0.1"), "not allowed", "unlisted private range stays blocked")
}

func TestValidateHost_EmptyAllowlistPreservesGuard(t *testing.T) {
	// With no env set the guard behaves exactly as before the allowlist existed.
	t.Setenv("DB_HOST_ALLOWED_CIDRS", "")

	assert.Contains(t, validateHost("10.0.0.1"), "not allowed")
}

// --- T2-7 Pagination Clamping Tests ---

func TestListPipelines_PaginationClamping(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockKV := mocks.NewMockKeyValue(ctrl)
	router := setupTestRouter(mockKV)
	token := getTestToken(t, router, mockKV)
	authHeader := "Bearer " + token

	// Create 5 pipelines
	var pipelineEntries []nats.KeyValueEntry
	for i := 1; i <= 5; i++ {
		p := protocol.PipelineConfig{ID: fmt.Sprintf("p%d", i), Name: fmt.Sprintf("Pipe %d", i), Sources: []string{"s1"}, Sinks: []string{"snk1"}, Tables: []string{"t1"}}
		pData, _ := json.Marshal(p)
		mockKV.EXPECT().Get(protocol.TransitionStateKey(fmt.Sprintf("p%d", i))).Return(nil, nats.ErrKeyNotFound).AnyTimes()
		mockKV.EXPECT().Get(protocol.WorkerHeartbeatKey(fmt.Sprintf("p%d", i))).Return(nil, nats.ErrKeyNotFound).AnyTimes()
		pipelineEntries = append(pipelineEntries, mockEntry{key: protocol.PipelineConfigKey(fmt.Sprintf("p%d", i)), value: pData})
	}

	mockKV.EXPECT().Watch(protocol.PrefixPipelineConfig+">", gomock.Any()).DoAndReturn(
		func(_ string, _ ...nats.WatchOpt) (nats.KeyWatcher, error) {
			updates := make(chan nats.KeyValueEntry, len(pipelineEntries)+1)
			for _, e := range pipelineEntries {
				updates <- e
			}
			updates <- nil
			return &MockWatcher{updates: updates}, nil
		},
	).AnyTimes()

	t.Run("limit clamped to 100", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/pipelines?limit=500", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)
		// With 5 pipelines and limit=100, should return all 5
		pipelines := resp["pipelines"].([]any)
		assert.Equal(t, 5, len(pipelines))
		assert.Equal(t, float64(100), resp["limit"]) // limit should be clamped to 100
	})

	t.Run("page clamped to 10000", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/pipelines?page=50000", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)
		// page should be clamped to 10000
		assert.Equal(t, float64(10000), resp["page"])
	})

	t.Run("negative page becomes 1", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/pipelines?page=-5", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, float64(1), resp["page"])
	})

	t.Run("negative limit becomes 1", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/pipelines?limit=-10", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, float64(1), resp["limit"])
	})

	t.Run("out of range page returns empty", func(t *testing.T) {
		// With 5 pipelines and limit=10, page=100 should be out of range
		req, _ := http.NewRequest("GET", "/api/v1/pipelines?page=100&limit=10", nil)
		req.Header.Set("Authorization", authHeader)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)
		// When start >= total, pipelines becomes an empty slice (not nil)
		if resp["pipelines"] != nil {
			pipelines := resp["pipelines"].([]any)
			assert.Equal(t, 0, len(pipelines), "Out of range page should return empty pipelines")
		}
	})
}
