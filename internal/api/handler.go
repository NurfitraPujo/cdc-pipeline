package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/crypto"
	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"

	// Database drivers for test connections
	_ "github.com/datafuselabs/databend-go"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
)

// isPrivateHost checks if the given IP address is in a private/reserved range.
// This includes loopback (127.0.0.0/8), link-local (169.254.0.0/16), and RFC 1918 private ranges.
func isPrivateHost(ip net.IP) bool {
	// Check loopback
	if ip.IsLoopback() {
		return true
	}
	// Check link-local
	if ip.IsLinkLocalUnicast() {
		return true
	}
	// Check RFC 1918 private ranges: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
	// Also include 100.64.0.0/10 (Carrier-grade NAT)
	// And 192.0.0.0/24 (IETF Protocol assignments)
	privateBlocks := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"100.64.0.0/10",
		"192.0.0.0/24",
	}
	for _, block := range privateBlocks {
		_, cidr, _ := net.ParseCIDR(block)
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

// allowedHostCIDRs parses DB_HOST_ALLOWED_CIDRS -- a comma-separated list of
// CIDR blocks that are exempt from the private-IP guard in validateHost.
//
// The guard exists to stop an authenticated operator from pointing a connection
// test at loopback, link-local, or a cloud metadata endpoint (SSRF, T2-1). But
// the databases this pipeline actually targets live on private VPC addresses
// (e.g. an RDS instance on 10.x), so the guard as written rejects every real
// target. This allowlist re-permits the specific ranges the operator intends
// -- typically the VPC CIDR -- while still blocking everything else private.
//
// It is read per call rather than cached: this is a low-frequency, operator-
// triggered endpoint, so a fresh getenv keeps the function pure and trivially
// testable. Malformed entries are skipped rather than failing the request.
func allowedHostCIDRs() []*net.IPNet {
	raw := os.Getenv("DB_HOST_ALLOWED_CIDRS")
	if raw == "" {
		return nil
	}
	var nets []*net.IPNet
	for _, part := range strings.Split(raw, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		if _, cidr, err := net.ParseCIDR(trimmed); err == nil {
			nets = append(nets, cidr)
		}
	}
	return nets
}

// ipInAny reports whether ip falls inside any of the given CIDR blocks.
func ipInAny(ip net.IP, nets []*net.IPNet) bool {
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// validateHost resolves the hostname and checks if the resulting IP is allowed.
// Returns an error message if the host resolves to a private/reserved IP that is
// not covered by the DB_HOST_ALLOWED_CIDRS allowlist.
func validateHost(host string) string {
	ips, err := net.LookupIP(host)
	if err != nil {
		// If DNS resolution fails, we allow the connection attempt (it may fail for other reasons)
		return ""
	}
	allowed := allowedHostCIDRs()
	for _, ip := range ips {
		if isPrivateHost(ip) && !ipInAny(ip, allowed) {
			return fmt.Sprintf("host %s resolved to private IP %s not allowed", host, ip.String())
		}
	}
	return ""
}

type Handler struct {
	kv nats.KeyValue
	sf singleflight.Group
}

func NewHandler(kv nats.KeyValue) *Handler {
	return &Handler{kv: kv}
}

// --- Global Config ---

// GetGlobalConfig retrieves global defaults.
// @Summary      Get global configuration
// @Description  Retrieve global batching settings
// @Tags         config
// @Produce      json
// @Security     Bearer
// @Success      200  {object}  protocol.GlobalConfig
// @Failure      404  {object}  map[string]string "not found"
// @Router       /global [get]
func (h *Handler) GetGlobalConfig(c *gin.Context) {
	entry, err := h.kv.Get(protocol.KeyGlobalConfig)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "global config not found"})
		return
	}

	var cfg protocol.GlobalConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// UpdateGlobalConfig updates global defaults.
// @Summary      Update global configuration
// @Description  Update global batching settings and reload all pipelines
// @Tags         config
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        config  body      protocol.GlobalConfig  true  "Global Config"
// @Success      200     {object}  protocol.GlobalConfig
// @Failure      429     {object}  map[string]string "too many requests"
// @Router       /global [put]
func (h *Handler) UpdateGlobalConfig(c *gin.Context) {
	var cfg protocol.GlobalConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Dynamic Rate Limit for Global: Check if ANY pipeline is currently transitioning
	// since global update triggers a reload for all.
	keys, _ := h.kv.Keys()
	for _, key := range keys {
		if strings.HasSuffix(key, ".transition") {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "cannot update global config while pipelines are transitioning",
			})
			return
		}
	}

	data, _ := json.Marshal(cfg)
	if _, err := h.kv.Put(protocol.KeyGlobalConfig, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// --- Stats & Dashboard ---

// GetStatsSummary reads the pre-aggregated metrics from the global summary key.
// If the cache is older than 30 seconds (or missing), it triggers a re-computation.
// @Summary      Get dashboard summary
// @Description  Retrieve multi-pipeline totals with lazy refresh (Optimized)
// @Tags         stats
// @Produce      json
// @Security     Bearer
// @Success      200  {object}  protocol.StatsSummary
// @Router       /stats/summary [get]
func (h *Handler) GetStatsSummary(c *gin.Context) {
	entry, err := h.kv.Get(protocol.KeyGlobalSummary)

	// If entry exists and is "fresh" (< 30s), return it
	if err == nil && time.Since(entry.Created()) < 30*time.Second {
		var summary protocol.StatsSummary
		if err := json.Unmarshal(entry.Value(), &summary); err == nil {
			c.JSON(http.StatusOK, summary)
			return
		}
	}

	// Else: Stale or missing. Recompute (O(N)) and update cache
	// Use singleflight to ensure only one request performs the computation
	val, err, _ := h.sf.Do("global_summary", func() (any, error) {
		return h.computeAndStoreSummary(), nil
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to compute stats"})
		return
	}

	c.JSON(http.StatusOK, val.(protocol.StatsSummary))
}

// GetStatsHistory returns empty metrics history (Time-series requirement removed).
// @Summary      Get metrics history
// @Description  Retrieve time-series data (Deprecated/Ditched)
// @Tags         stats
// @Produce      json
// @Security     Bearer
// @Success      200  {array}  protocol.HistoryPoint
// @Router       /stats/history [get]
func (h *Handler) GetStatsHistory(c *gin.Context) {
	c.JSON(http.StatusOK, []protocol.HistoryPoint{})
}

func (h *Handler) computeAndStoreSummary() protocol.StatsSummary {
	keys, err := h.kv.Keys()
	summary := protocol.StatsSummary{}
	if err != nil {
		return summary
	}

	// 1. Identify debug sinks to exclude them from production metrics
	debugSinks := make(map[string]bool)
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSinkConfig) {
			if entry, err := h.kv.Get(key); err == nil {
				var sc protocol.SinkConfig
				if err := json.Unmarshal(entry.Value(), &sc); err == nil && sc.Type == "postgres_debug" {
					debugSinks[sc.ID] = true
				}
			}
		}
	}

	// 2. Group production stats by pipeline and table to deduplicate counts
	// pipelineID -> tableName -> max stats found across production sinks
	aggStats := make(map[string]map[string]protocol.TableStats)

	for _, key := range keys {
		info := protocol.ParseTableStatsKey(key)
		if info == nil {
			continue
		}

		// Exclude debug sinks from production summary
		if debugSinks[info.SinkID] {
			continue
		}

		if entry, err := h.kv.Get(key); err == nil {
			var st protocol.TableStats
			if err := protocol.UnmarshalState(entry.Value(), &st); err != nil {
				log.Error().Err(err).Str("key", key).Msg("Failed to decode table stats; excluded from summary totals")
			} else {
				if _, ok := aggStats[info.PipelineID]; !ok {
					aggStats[info.PipelineID] = make(map[string]protocol.TableStats)
				}

				current := aggStats[info.PipelineID][info.Table]
				// Aggregate: Max for count and lag
				if st.TotalSynced > current.TotalSynced {
					current.TotalSynced = st.TotalSynced
				}
				if st.LagMS > current.LagMS {
					current.LagMS = st.LagMS
				}
				aggStats[info.PipelineID][info.Table] = current
			}
		}
	}

	// 3. Compute final summary using aggregated data
	var totalLag int64
	var lagCount int64

	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixPipelineConfig) {
			summary.TotalPipelines++
			id := strings.TrimPrefix(key, protocol.PrefixPipelineConfig)

			// Check status/health
			tsKey := protocol.TransitionStateKey(id)
			if entry, err := h.kv.Get(tsKey); err == nil {
				var ts protocol.PipelineTransitionState
				if err := json.Unmarshal(entry.Value(), &ts); err == nil && ts.Status == "Transitioning" {
					summary.TransitioningCount++
				}
			}

			switch h.getPipelineStatusString(id) {
			case "healthy":
				summary.HealthyCount++
			case "error":
				summary.ErrorCount++
			}

			// Add this pipeline's aggregated production stats
			if pStats, ok := aggStats[id]; ok {
				for _, st := range pStats {
					summary.TotalRowsSynchronized += st.TotalSynced
					totalLag += st.LagMS
					lagCount++
				}
			}
		}
	}

	if lagCount > 0 {
		summary.AvgLagMS = totalLag / lagCount
	}

	data, _ := json.Marshal(summary)
	_, _ = h.kv.Put(protocol.KeyGlobalSummary, data)
	return summary
}

// --- Pipelines ---

// ListPipelines returns all pipelines.
// @Summary      List pipelines
// @Description  Retrieve pipeline configurations with search, status filtering, and pagination
// @Tags         pipelines
// @Produce      json
// @Security     Bearer
// @Param        search  query     string  false  "Search by name or ID"
// @Param        status  query     string  false  "Filter by status (Healthy, Error, Transitioning)"
// @Param        page    query     int     false  "Page number (default: 1)"
// @Param        limit   query     int     false  "Items per page (default: 10)"
// @Success      200  {object}  map[string]any
// @Router       /pipelines [get]
func (h *Handler) ListPipelines(c *gin.Context) {
	search := strings.ToLower(c.Query("search"))
	statusFilter := c.Query("status")

	page := 1
	limit := 10
	if p := c.Query("page"); p != "" {
		fmt.Sscanf(p, "%d", &page)
	}
	if l := c.Query("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}

	// T2-7: Clamp pagination values to safe ranges to prevent slice index overflow.
	// Large page values could cause (page-1)*limit to wrap negative.
	if page < 1 {
		page = 1
	}
	if page > 10000 {
		page = 10000
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 100 {
		limit = 100
	}

	watcher, err := h.kv.Watch(protocol.PrefixPipelineConfig + ">")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer func() { _ = watcher.Stop() }()

	var pipelines []protocol.PipelineConfig
	for entry := range watcher.Updates() {
		if entry == nil {
			// nil marks the end of the initial replay of values.
			break
		}
		if entry.Operation() == nats.KeyValueDelete || entry.Operation() == nats.KeyValuePurge {
			continue
		}
		var cfg protocol.PipelineConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
			// Search filter
			if search != "" && !strings.Contains(strings.ToLower(cfg.Name), search) && !strings.Contains(strings.ToLower(cfg.ID), search) {
				continue
			}

			// Status filter (expensive but necessary if requested)
			if statusFilter != "" {
				actualStatus := h.getPipelineStatusString(cfg.ID)
				if !strings.EqualFold(actualStatus, statusFilter) {
					continue
				}
			}

			pipelines = append(pipelines, cfg)
		}
	}

	total := len(pipelines)
	start := (page - 1) * limit
	end := start + limit

	// T2-7: Reject negative start values and handle out-of-bounds gracefully.
	// After clamping page and limit above, start should always be >= 0,
	// but we keep this check for defensive programming.
	if start < 0 || start >= total {
		pipelines = []protocol.PipelineConfig{}
	} else {
		if end > total {
			end = total
		}
		pipelines = pipelines[start:end]
	}

	items := make([]json.RawMessage, 0, len(pipelines))
	for _, pipe := range pipelines {
		item, err := h.pipelineWithStatus(pipe)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		items = append(items, item)
	}

	c.JSON(http.StatusOK, gin.H{
		"pipelines": items,
		"total":     total,
		"page":      page,
		"limit":     limit,
	})
}

// pipelineWithStatus renders a pipeline config with its computed "status"
// spliced in alongside the config fields.
//
// This deliberately does not use a struct embedding protocol.PipelineConfig.
// PipelineConfig has a MarshalJSON method (internal/protocol/config_json.go,
// which renders durations as "10s" rather than as nanosecond integers), and an
// embedded type's MarshalJSON is promoted to the outer struct -- so an
// embedding wrapper serialises as a bare PipelineConfig and silently drops
// every sibling field, including Status. See list_pipelines_json_test.go.
func (h *Handler) pipelineWithStatus(cfg protocol.PipelineConfig) (json.RawMessage, error) {
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	extra, err := json.Marshal(map[string]string{
		"status": h.getPipelineStatusString(cfg.ID),
	})
	if err != nil {
		return nil, err
	}

	// {"a":1} + {"status":"healthy"} -> {"a":1,"status":"healthy"}
	merged := make([]byte, 0, len(encoded)+len(extra))
	merged = append(merged, encoded[:len(encoded)-1]...)
	merged = append(merged, ',')
	merged = append(merged, extra[1:]...)
	return merged, nil
}

func (h *Handler) getPipelineStatusString(id string) string {
	actualStatus := "healthy"

	tsKey := protocol.TransitionStateKey(id)
	if tsEntry, err := h.kv.Get(tsKey); err == nil {
		var ts protocol.PipelineTransitionState
		if err := json.Unmarshal(tsEntry.Value(), &ts); err == nil && ts.Status == "Transitioning" {
			return "transitioning"
		}
	}

	hbKey := protocol.WorkerHeartbeatKey(id)
	if hbEntry, err := h.kv.Get(hbKey); err == nil {
		var hb protocol.WorkerHeartbeat
		if err := json.Unmarshal(hbEntry.Value(), &hb); err == nil {
			if time.Since(hb.UpdatedAt) > 60*time.Second {
				actualStatus = "error"
			} else if hb.Status != "" && hb.Status != "Running" {
				// A fresh heartbeat that reports a non-"Running" status (e.g. a
				// worker that failed to start and is looping in the retry
				// backoff, see config/manager.go monitorWorker) must not be
				// reported "healthy" just because it is being updated on
				// schedule (WS-8/WS-9: "a pipeline that fails to construct a
				// processor must no longer be reported healthy").
				actualStatus = "error"
			}
		}
	} else {
		actualStatus = "error"
	}

	return actualStatus
}

// RestartPipeline triggers a reload for a specific pipeline.
// @Summary      Restart pipeline
// @Description  Trigger a manual restart/reload for a pipeline
// @Tags         pipelines
// @Security     Bearer
// @Param        id   path      string  true  "Pipeline ID"
// @Success      202  "Accepted"
// @Failure      404  {object}  map[string]string "not found"
// @Router       /pipelines/{id}/restart [post]
func (h *Handler) RestartPipeline(c *gin.Context) {
	id := c.Param("id")
	key := protocol.PipelineConfigKey(id)
	if _, err := h.kv.Get(key); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pipeline not found"})
		return
	}

	// Trigger transition state to notify workers
	ts := protocol.PipelineTransitionState{
		ID:        id,
		Status:    "Transitioning",
		StartedAt: time.Now(),
	}
	data, _ := json.Marshal(ts)
	if _, err := h.kv.Put(protocol.TransitionStateKey(id), data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"status": "restart triggered"})
}

// CreatePipeline creates a new pipeline.
// @Summary      Create pipeline
// @Description  Create a new pipeline configuration
// @Tags         pipelines
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        pipeline  body      protocol.PipelineConfig  true  "Pipeline Config"
// @Success      201       {object}  protocol.PipelineConfig
// @Failure      429       {object}  map[string]string "too many requests"
// @Router       /pipelines [post]
func (h *Handler) CreatePipeline(c *gin.Context) {
	var cfg protocol.PipelineConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate sources/sinks exist
	for _, sid := range cfg.Sources {
		if _, err := h.kv.Get(protocol.SourceConfigKey(sid)); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("source %s not found", sid)})
			return
		}
	}
	for _, sid := range cfg.Sinks {
		if _, err := h.kv.Get(protocol.SinkConfigKey(sid)); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("sink %s not found", sid)})
			return
		}
	}

	// Dynamic Rate Limit: Check if pipeline is currently transitioning
	tsKey := protocol.TransitionStateKey(cfg.ID)
	if entry, err := h.kv.Get(tsKey); err == nil {
		var ts protocol.PipelineTransitionState
		if err := json.Unmarshal(entry.Value(), &ts); err == nil && ts.Status == "Transitioning" {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":      "pipeline is currently transitioning/restarting",
				"started_at": ts.StartedAt,
			})
			return
		}
	}

	data, _ := json.Marshal(cfg)
	key := protocol.PipelineConfigKey(cfg.ID)
	if _, err := h.kv.Put(key, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, cfg)
}

// UpdatePipeline updates an existing pipeline.
// @Summary      Update pipeline
// @Description  Update an existing pipeline configuration
// @Tags         pipelines
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        id        path      string                   true  "Pipeline ID"
// @Param        pipeline  body      protocol.PipelineConfig  true  "Pipeline Config"
// @Success      200       {object}  protocol.PipelineConfig
// @Failure      429       {object}  map[string]string "too many requests"
// @Router       /pipelines/{id} [put]
func (h *Handler) UpdatePipeline(c *gin.Context) {
	id := c.Param("id")
	var cfg protocol.PipelineConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cfg.ID = id
	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate sources/sinks exist
	for _, sid := range cfg.Sources {
		if _, err := h.kv.Get(protocol.SourceConfigKey(sid)); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("source %s not found", sid)})
			return
		}
	}
	for _, sid := range cfg.Sinks {
		if _, err := h.kv.Get(protocol.SinkConfigKey(sid)); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("sink %s not found", sid)})
			return
		}
	}

	tsKey := protocol.TransitionStateKey(id)
	if entry, err := h.kv.Get(tsKey); err == nil {
		var ts protocol.PipelineTransitionState
		if err := json.Unmarshal(entry.Value(), &ts); err == nil && ts.Status == "Transitioning" {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":      "pipeline is currently transitioning/restarting",
				"started_at": ts.StartedAt,
			})
			return
		}
	}

	data, _ := json.Marshal(cfg)
	key := protocol.PipelineConfigKey(id)
	if _, err := h.kv.Put(key, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// GetPipeline returns a single pipeline.
// @Summary      Get pipeline
// @Description  Retrieve a specific pipeline configuration
// @Tags         pipelines
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Pipeline ID"
// @Success      200  {object}  protocol.PipelineConfig
// @Failure      404  {object}  map[string]string "not found"
// @Router       /pipelines/{id} [get]
func (h *Handler) GetPipeline(c *gin.Context) {
	id := c.Param("id")
	key := protocol.PipelineConfigKey(id)
	entry, err := h.kv.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pipeline not found"})
		return
	}

	var cfg protocol.PipelineConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Include the computed status, matching ListPipelines. Without it the
	// detail page had no way to show a pipeline's health and fell back to a
	// hardcoded "Configured" badge.
	item, err := h.pipelineWithStatus(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Data(http.StatusOK, "application/json; charset=utf-8", item)
}

// DeletePipeline deletes a pipeline.
// @Summary      Delete pipeline
// @Description  Delete a pipeline configuration and stop its worker
// @Tags         pipelines
// @Security     Bearer
// @Param        id   path      string  true  "Pipeline ID"
// @Success      204  "No Content"
// @Router       /pipelines/{id} [delete]
func (h *Handler) DeletePipeline(c *gin.Context) {
	id := c.Param("id")
	key := protocol.PipelineConfigKey(id)
	if err := h.kv.Delete(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusNoContent, nil)
}

// GetPipelineStatus returns current LSN status and stats.
// @Summary      Get pipeline status
// @Description  Retrieve aggregated multi-table status and stats for a pipeline
// @Tags         pipelines
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Pipeline ID"
// @Success      200  {object}  map[string]any
// @Router       /pipelines/{id}/status [get]
func (h *Handler) GetPipelineStatus(c *gin.Context) {
	id := c.Param("id")
	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 1. Identify debug sinks for this pipeline
	debugSinks := make(map[string]bool)
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSinkConfig) {
			if entry, err := h.kv.Get(key); err == nil {
				var sc protocol.SinkConfig
				if err := json.Unmarshal(entry.Value(), &sc); err == nil && sc.Type == "postgres_debug" {
					debugSinks[sc.ID] = true
				}
			}
		}
	}

	statusMap := make(map[string]any)
	tableStats := make(map[string]protocol.TableStats)           // aggregated by table
	sinkStats := make(map[string]map[string]protocol.TableStats) // sinkID -> tableName -> stats

	prefix := protocol.PipelineStatusPrefix(id)
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}

			if strings.HasSuffix(key, "_checkpoint") {
				var cp protocol.Checkpoint
				if err := protocol.UnmarshalState(entry.Value(), &cp); err != nil {
					log.Error().Err(err).Str("key", key).Msg("Failed to decode checkpoint; omitting from status")
				} else {
					statusMap[key] = cp
				}
			} else if strings.HasSuffix(key, ".stats") {
				info := protocol.ParseTableStatsKey(key)
				var st protocol.TableStats
				if err := protocol.UnmarshalState(entry.Value(), &st); err != nil {
					log.Error().Err(err).Str("key", key).Msg("Failed to decode table stats; omitting from status")
				} else {
					// Raw map for backward compatibility
					statusMap[key] = st

					if info != nil {
						// Per-sink stats
						if _, ok := sinkStats[info.SinkID]; !ok {
							sinkStats[info.SinkID] = make(map[string]protocol.TableStats)
						}
						sinkStats[info.SinkID][info.Table] = st

						// Aggregated table stats (exclude debug sinks)
						if !debugSinks[info.SinkID] {
							current := tableStats[info.Table]
							if st.TotalSynced > current.TotalSynced {
								current.TotalSynced = st.TotalSynced
							}
							if st.LagMS > current.LagMS {
								current.LagMS = st.LagMS
							}
							// Keep most recent update time
							if st.UpdatedAt.After(current.UpdatedAt) {
								current.UpdatedAt = st.UpdatedAt
								current.Status = st.Status
							}
							tableStats[info.Table] = current
						}
					}
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"pipeline_id": id,
		"status":      statusMap,  // Backward compatibility
		"tables":      tableStats, // Aggregated production stats
		"sinks":       sinkStats,  // Per-sink detailed stats
	})
}

// --- Sources ---

// ListSources returns all sources.
// @Summary      List sources
// @Description  Retrieve all source configurations
// @Tags         sources
// @Produce      json
// @Security     Bearer
// @Success      200  {object}  map[string][]protocol.SourceConfig
// @Router       /sources [get]
func (h *Handler) ListSources(c *gin.Context) {
	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var sources []protocol.SourceConfig
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSourceConfig) {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}
			var cfg protocol.SourceConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
				cfg.PassEncrypted = ""
				sources = append(sources, cfg)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"sources": sources})
}

// CreateSource creates a new source.
// @Summary      Create source
// @Description  Create a new source configuration
// @Tags         sources
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        source  body      protocol.SourceConfig  true  "Source Config"
// @Success      201     {object}  protocol.SourceConfig
// @Router       /sources [post]
func (h *Handler) CreateSource(c *gin.Context) {
	var cfg protocol.SourceConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Encrypt sensitive fields
	key, err := crypto.GetEncryptionKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if cfg.PassEncrypted != "" {
		encrypted, err := crypto.Encrypt(cfg.PassEncrypted, key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encrypt password"})
			return
		}
		cfg.PassEncrypted = encrypted
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal config"})
		return
	}
	storageKey := protocol.SourceConfigKey(cfg.ID)
	if _, err := h.kv.Put(storageKey, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, cfg)
}

// UpdateSource updates an existing source.
// @Summary      Update source
// @Description  Update an existing source configuration
// @Tags         sources
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        id      path      string                 true  "Source ID"
// @Param        source  body      protocol.SourceConfig  true  "Source Config"
// @Success      200     {object}  protocol.SourceConfig
// @Router       /sources/{id} [put]
func (h *Handler) UpdateSource(c *gin.Context) {
	id := c.Param("id")
	var cfg protocol.SourceConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	cfg.ID = id
	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Encrypt sensitive fields or preserve existing
	var oldCfg protocol.SourceConfig
	oldKey := protocol.SourceConfigKey(id)
	if entry, err := h.kv.Get(oldKey); err == nil {
		_ = json.Unmarshal(entry.Value(), &oldCfg)
	}

	if cfg.PassEncrypted == "" {
		cfg.PassEncrypted = oldCfg.PassEncrypted
	} else if cfg.PassEncrypted == "__CLEAR__" {
		cfg.PassEncrypted = ""
	} else {
		key, err := crypto.GetEncryptionKey()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		encrypted, err := crypto.Encrypt(cfg.PassEncrypted, key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encrypt password"})
			return
		}
		cfg.PassEncrypted = encrypted
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal config"})
		return
	}
	storageKey := protocol.SourceConfigKey(id)
	if _, err := h.kv.Put(storageKey, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// DeleteSource deletes a source.
// @Summary      Delete source
// @Description  Delete a source configuration
// @Tags         sources
// @Security     Bearer
// @Param        id   path      string  true  "Source ID"
// @Success      204  "No Content"
// @Router       /sources/{id} [delete]
func (h *Handler) DeleteSource(c *gin.Context) {
	id := c.Param("id")
	key := protocol.SourceConfigKey(id)
	if err := h.kv.Delete(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusNoContent, nil)
}

// GetSource returns a single source configuration.
// @Summary      Get source
// @Description  Retrieve a specific source configuration
// @Tags         sources
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Source ID"
// @Success      200  {object}  protocol.SourceConfig
// @Failure      404  {object}  map[string]string "not found"
// @Router       /sources/{id} [get]
func (h *Handler) GetSource(c *gin.Context) {
	id := c.Param("id")
	key := protocol.SourceConfigKey(id)
	entry, err := h.kv.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "source not found"})
		return
	}

	var cfg protocol.SourceConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Scrub sensitive fields to avoid shoulder-surfing and leakages.
	// The frontend will preserve existing passwords by omitting them from PUT payloads.
	cfg.PassEncrypted = ""

	c.JSON(http.StatusOK, SourceConfigFromProtocol(cfg))
}

// discoverySchemas returns the schemas ListSourceTables' fallback discovery
// path should query. Empty configured Schemas means "public" only, NOT all
// schemas -- see MULTI_SCHEMA_PLAN.md §2.4/§8 item 4. Every existing source
// config has Schemas empty (the field was dead until this stage), so
// defaulting to a wildcard here would silently start discovering tables from
// every schema on the database on upgrade.
func discoverySchemas(configured []string) []string {
	if len(configured) == 0 {
		return []string{"public"}
	}
	return configured
}

// openSourceDB loads the source config for id, decrypts its password, and
// opens a connection to the source database. Callers must Close() the
// returned *sql.DB. Returns (nil, nil, false) with the response already
// written if the source cannot be loaded or connected to -- callers should
// just return in that case.
func (h *Handler) openSourceDB(c *gin.Context, id string) (*sql.DB, bool) {
	key := protocol.SourceConfigKey(id)
	entry, err := h.kv.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "source not found"})
		return nil, false
	}

	var cfg protocol.SourceConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return nil, false
	}

	if cfg.PassEncrypted != "" {
		encKey, err := crypto.GetEncryptionKey()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return nil, false
		}
		if decrypted, err := crypto.Decrypt(cfg.PassEncrypted, encKey); err == nil {
			cfg.PassEncrypted = decrypted
		}
	}

	u := &url.URL{
		Scheme: "postgres", Host: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		User: url.UserPassword(cfg.User, cfg.PassEncrypted), Path: cfg.Database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	q.Set("connect_timeout", "3")
	u.RawQuery = q.Encode()

	db, err := sql.Open("pgx", u.String())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("failed to open connection: %v", err)})
		return nil, false
	}
	return db, true
}

// GetSourceSchema triggers or retrieves table schema discovery for a source.
// @Summary      Get source schema
// @Description  Discover available schemas directly from the source database
// @Tags         sources
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Source ID"
// @Success      200  {object}  map[string]any
// @Router       /sources/{id}/schema [get]
func (h *Handler) GetSourceSchema(c *gin.Context) {
	id := c.Param("id")

	db, ok := h.openSourceDB(c, id)
	if !ok {
		return
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// pg_namespace, not information_schema.schemata, so the query needs no
	// per-database grants beyond CONNECT -- excludes catalog/toast/temp
	// schemas, which are never valid replication targets.
	rows, err := db.QueryContext(ctx, `
		SELECT nspname FROM pg_namespace
		WHERE nspname NOT IN ('pg_catalog', 'information_schema')
		  AND nspname NOT LIKE 'pg_toast%'
		  AND nspname NOT LIKE 'pg_temp_%'
		ORDER BY nspname`)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("failed to discover schemas: %v", err)})
		return
	}
	defer rows.Close()

	schemas := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			schemas = append(schemas, name)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"source_id":         id,
		"available_schemas": schemas,
		"discovery_status":  "ready",
	})
}

// ListSourceTables returns all discovered tables for a source.
// @Summary      List source tables
// @Description  Retrieve all discovered tables and their metadata for a source
// @Tags         sources
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Source ID"
// @Success      200  {object}  map[string][]protocol.TableMetadata
// @Router       /sources/{id}/tables [get]
func (h *Handler) ListSourceTables(c *gin.Context) {
	sourceID := c.Param("id")
	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var tables []protocol.TableMetadata
	suffix := fmt.Sprintf(".sources.%s.tables.", sourceID)
	for _, key := range keys {
		if strings.Contains(key, suffix) && strings.HasSuffix(key, ".metadata") {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}
			var meta protocol.TableMetadata
			if err := json.Unmarshal(entry.Value(), &meta); err == nil {
				tables = append(tables, meta)
			}
		}
	}

	if len(tables) == 0 {
		// Attempt dynamic discovery from the source database.
		key := protocol.SourceConfigKey(sourceID)
		entry, err := h.kv.Get(key)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"source_id": sourceID, "tables": tables})
			return
		}
		var cfg protocol.SourceConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
			c.JSON(http.StatusOK, gin.H{"source_id": sourceID, "tables": tables})
			return
		}

		db, ok := h.openSourceDB(c, sourceID)
		if !ok {
			return
		}
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		schemas := discoverySchemas(cfg.Schemas)

		rows, err := db.QueryContext(ctx,
			"SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = ANY($1) AND table_type = 'BASE TABLE'",
			schemas)
		if err != nil {
			// Surface the failure instead of swallowing it. database/sql
			// connects lazily, so openSourceDB above succeeds even when the
			// source is unreachable and this query is where that first shows
			// up. Reporting 200 with an empty list made "cannot reach the
			// database" indistinguishable from "connected fine, no tables" --
			// the discovery button just showed nothing. GetSourceSchema
			// already answers 502 for the same condition.
			c.JSON(http.StatusBadGateway, gin.H{
				"error": fmt.Sprintf("table discovery failed: %v", err),
			})
			return
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var schema, tableName string
			if err := rows.Scan(&schema, &tableName); err == nil {
				// Filter out snapshot tables
				if strings.Contains(tableName, "cdc_snapshot") {
					continue
				}
				ref := protocol.TableRef{Schema: schema, Table: tableName}
				tables = append(tables, protocol.TableMetadata{
					ID:     ref.KeyToken(),
					Name:   tableName,
					Schema: ref.Schema,
				})
			}
		}

		// An error part-way through iteration would otherwise truncate the
		// list silently.
		if err := rows.Err(); err != nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"error": fmt.Sprintf("table discovery failed: %v", err),
			})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"source_id": sourceID, "tables": tables})
}

// --- Sinks ---

// ListSinks returns all sinks.
// @Summary      List sinks
// @Description  Retrieve all sink configurations
// @Tags         sinks
// @Produce      json
// @Security     Bearer
// @Success      200  {object}  map[string][]protocol.SinkConfig
// @Router       /sinks [get]
func (h *Handler) ListSinks(c *gin.Context) {
	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	encryptionKey, err := crypto.GetEncryptionKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var sinks []protocol.SinkConfig
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSinkConfig) {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}
			var cfg protocol.SinkConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
				if cfg.DSN != "" {
					decrypted, err := crypto.Decrypt(cfg.DSN, encryptionKey)
					if err != nil {
						log.Warn().Err(err).Str("sink_id", cfg.ID).Msg("Failed to decrypt DSN, returning encrypted value")
					} else {
						cfg.DSN = maskDSN(decrypted)
					}
				}
				sinks = append(sinks, cfg)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"sinks": sinks})
}

// GetSink returns a single sink configuration.
// @Summary      Get sink
// @Description  Retrieve a specific sink configuration
// @Tags         sinks
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Sink ID"
// @Success      200  {object}  protocol.SinkConfig
// @Failure      404  {object}  map[string]string "not found"
// @Router       /sinks/{id} [get]
func (h *Handler) GetSink(c *gin.Context) {
	id := c.Param("id")
	key := protocol.SinkConfigKey(id)
	entry, err := h.kv.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "sink not found"})
		return
	}

	var cfg protocol.SinkConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Decrypt sensitive fields
	encryptionKey, err := crypto.GetEncryptionKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if cfg.DSN != "" {
		decrypted, err := crypto.Decrypt(cfg.DSN, encryptionKey)
		if err != nil {
			log.Warn().Err(err).Str("sink_id", cfg.ID).Msg("Failed to decrypt DSN, returning encrypted value")
		} else {
			cfg.DSN = maskDSN(decrypted)
		}
	}

	c.JSON(http.StatusOK, cfg)
}

// CreateSink creates a new sink.
// @Summary      Create sink
// @Description  Create a new sink configuration
// @Tags         sinks
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        sink  body      protocol.SinkConfig  true  "Sink Config"
// @Success      201   {object}  protocol.SinkConfig
// @Router       /sinks [post]
func (h *Handler) CreateSink(c *gin.Context) {
	var cfg protocol.SinkConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Encrypt sensitive fields
	key, err := crypto.GetEncryptionKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if cfg.DSN != "" {
		encrypted, err := crypto.Encrypt(cfg.DSN, key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encrypt DSN"})
			return
		}
		cfg.DSN = encrypted
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal config"})
		return
	}
	storageKey := protocol.SinkConfigKey(cfg.ID)
	if _, err := h.kv.Put(storageKey, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, cfg)
}

// UpdateSink updates an existing sink.
// @Summary      Update sink
// @Description  Update an existing sink configuration
// @Tags         sinks
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        id    path      string               true  "Sink ID"
// @Param        sink  body      protocol.SinkConfig  true  "Sink Config"
// @Success      200   {object}  protocol.SinkConfig
// @Router       /sinks/{id} [put]
func (h *Handler) UpdateSink(c *gin.Context) {
	id := c.Param("id")
	var cfg protocol.SinkConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	cfg.ID = id
	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Encrypt sensitive fields or reconstruct if masked
	key, err := crypto.GetEncryptionKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var oldCfg protocol.SinkConfig
	oldKey := protocol.SinkConfigKey(id)
	if entry, err := h.kv.Get(oldKey); err == nil {
		_ = json.Unmarshal(entry.Value(), &oldCfg)
	}

	var decryptedOldDSN string
	if oldCfg.DSN != "" {
		decrypted, err := crypto.Decrypt(oldCfg.DSN, key)
		if err == nil {
			decryptedOldDSN = decrypted
		}
	}

	reconstructed := reconstructDSN(cfg.DSN, decryptedOldDSN)
	cfg.DSN = reconstructed

	if cfg.DSN != "" {
		encrypted, err := crypto.Encrypt(cfg.DSN, key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encrypt DSN"})
			return
		}
		cfg.DSN = encrypted
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal config"})
		return
	}
	storageKey := protocol.SinkConfigKey(id)
	if _, err := h.kv.Put(storageKey, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// DeleteSink deletes a sink.
// @Summary      Delete sink
// @Description  Delete a sink configuration
// @Tags         sinks
// @Security     Bearer
// @Param        id   path      string  true  "Sink ID"
// @Success      204  "No Content"
// @Router       /sinks/{id} [delete]
func (h *Handler) DeleteSink(c *gin.Context) {
	id := c.Param("id")
	key := protocol.SinkConfigKey(id)
	if err := h.kv.Delete(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusNoContent, nil)
}

// --- Workers ---

// GetWorkerHeartbeat returns worker status.
// @Summary      Get worker status
// @Description  Retrieve heartbeat and uptime for a worker
// @Tags         workers
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Worker ID"
// @Success      200  {object}  protocol.WorkerHeartbeat
// @Router       /workers/{id}/heartbeat [get]
func (h *Handler) GetWorkerHeartbeat(c *gin.Context) {
	id := c.Param("id")
	key := protocol.WorkerHeartbeatKey(id)
	entry, err := h.kv.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
		return
	}

	var hb protocol.WorkerHeartbeat
	if err := json.Unmarshal(entry.Value(), &hb); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, hb)
}

func (h *Handler) cleanupStaleHeartbeats() {
	metrics.APICleanupRuns.Inc()

	watcher, err := h.kv.Watch(protocol.PrefixWorkerState + ">")
	if err != nil {
		return
	}
	defer func() { _ = watcher.Stop() }()

	for entry := range watcher.Updates() {
		if entry == nil {
			// nil marks the end of the initial replay of values.
			break
		}
		if entry.Operation() == nats.KeyValueDelete || entry.Operation() == nats.KeyValuePurge {
			continue
		}
		key := entry.Key()
		if strings.HasPrefix(key, "cdc.worker.") && strings.HasSuffix(key, ".heartbeat") {
			var hb protocol.WorkerHeartbeat
			if err := json.Unmarshal(entry.Value(), &hb); err == nil {
				if time.Since(hb.UpdatedAt) > 60*time.Second {
					log.Info().Str("worker_id", hb.WorkerID).Msg("Cleaning up stale heartbeat")
					h.kv.Delete(key)
				}
			}
		}
	}
}

// StartBackgroundCleanup runs cleanupStaleHeartbeats immediately and then on
// a 60s ticker, off the request path, until ctx is cancelled.
func (h *Handler) StartBackgroundCleanup(ctx context.Context) {
	go func() {
		h.cleanupStaleHeartbeats()

		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.cleanupStaleHeartbeats()
			}
		}
	}()
}

// StreamMetrics provides real-time status updates via SSE using NATS Watch.
// @Summary      Stream pipeline metrics
// @Description  Server-Sent Events stream for pipeline status and stats
// @Tags         pipelines
// @Produce      text/event-stream
// @Security     Bearer
// @Param        id   path      string  true  "Pipeline ID"
// @Router       /pipelines/{id}/metrics [get]
func (h *Handler) StreamMetrics(c *gin.Context) {
	pipelineID := c.Param("id")

	// 1. Identify debug sinks for this pipeline to help frontend filter
	keys, _ := h.kv.Keys()
	debugSinks := make(map[string]bool)
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSinkConfig) {
			if entry, err := h.kv.Get(key); err == nil {
				var sc protocol.SinkConfig
				if err := json.Unmarshal(entry.Value(), &sc); err == nil && sc.Type == "postgres_debug" {
					debugSinks[sc.ID] = true
				}
			}
		}
	}

	pattern := protocol.PipelineStatusPrefix(pipelineID) + "*"
	watcher, err := h.kv.Watch(pattern)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer watcher.Stop()

	// Set SSE headers only after watcher creation succeeds
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Transfer-Encoding", "chunked")

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case entry, ok := <-watcher.Updates():
			if !ok {
				log.Warn().Msg("watcher closed, exiting metric stream")
				return
			}
			if entry == nil {
				continue
			}

			var data any
			key := entry.Key()
			sinkID := ""
			isDebug := false

			if strings.HasSuffix(key, "_checkpoint") {
				var cp protocol.Checkpoint
				if err := protocol.UnmarshalState(entry.Value(), &cp); err != nil {
					log.Error().Err(err).Str("key", key).Msg("Failed to decode checkpoint; omitting from state stream")
					continue
				}
				data = cp
			} else if strings.HasSuffix(key, ".stats") {
				var st protocol.TableStats
				if err := protocol.UnmarshalState(entry.Value(), &st); err != nil {
					log.Error().Err(err).Str("key", key).Msg("Failed to decode table stats; omitting from state stream")
					continue
				}
				data = st

				if info := protocol.ParseTableStatsKey(key); info != nil {
					sinkID = info.SinkID
					isDebug = debugSinks[sinkID]
				}
			} else if strings.HasSuffix(key, ".transition") {
				var ts protocol.PipelineTransitionState
				if err := json.Unmarshal(entry.Value(), &ts); err != nil {
					continue
				}
				data = ts
			} else {
				data = string(entry.Value())
			}

			c.SSEvent("message", map[string]any{
				"key":      key,
				"data":     data,
				"sink_id":  sinkID,
				"is_debug": isDebug,
			})
			c.Writer.Flush()
		}
	}
}

const (
	// dsnMask stands in for a sink password on every read path.
	dsnMask = "***"
	// dsnMaskEncoded is what net/url writes for dsnMask inside userinfo.
	dsnMaskEncoded = "%2A%2A%2A"
)

func maskDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	if u.User != nil {
		_, hasPassword := u.User.Password()
		if hasPassword {
			u.User = url.UserPassword(u.User.Username(), dsnMask)
			// url.String() percent-encodes "*" in userinfo, so the mask went
			// out as "%2A%2A%2A" and was shown to the operator that way in the
			// sink list and the edit form. reconstructDSN still worked (Parse
			// decodes it back), but a masked DSN nobody recognises invites
			// hand-editing around it, which is how the real password gets
			// clobbered. "*" is a sub-delim and legal unencoded in userinfo,
			// so emitting it literally is valid and round-trips.
			return strings.Replace(u.String(), dsnMaskEncoded, dsnMask, 1)
		}
	}
	return dsn
}

func reconstructDSN(newDSN, oldDSN string) string {
	uNew, err := url.Parse(newDSN)
	if err != nil {
		return newDSN
	}
	if uNew.User != nil {
		pass, hasPassword := uNew.User.Password()
		// Parse decodes userinfo, so this matches whether the client echoed
		// back the literal "***" or the percent-encoded form older responses
		// emitted.
		if hasPassword && pass == dsnMask {
			uOld, err := url.Parse(oldDSN)
			if err == nil && uOld.User != nil {
				oldPass, hasOldPassword := uOld.User.Password()
				if hasOldPassword {
					uNew.User = url.UserPassword(uNew.User.Username(), oldPass)
					return uNew.String()
				}
			}
		}
	}
	return newDSN
}

// TestSourceConnection tests connection to the source database.
func (h *Handler) TestSourceConnection(c *gin.Context) {
	var cfg protocol.SourceConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	// T2-1: Validate host resolves to a non-private IP before attempting connection.
	if errMsg := validateHost(cfg.Host); errMsg != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return
	}

	// If id is provided and pass is empty, inject old password from KV
	if cfg.PassEncrypted == "" {
		if cfg.ID != "" {
			key := protocol.SourceConfigKey(cfg.ID)
			if entry, err := h.kv.Get(key); err == nil {
				var oldCfg protocol.SourceConfig
				if err := json.Unmarshal(entry.Value(), &oldCfg); err == nil {
					encKey, err := crypto.GetEncryptionKey()
					if err != nil {
						c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
						return
					}
					if oldCfg.PassEncrypted != "" {
						decrypted, err := crypto.Decrypt(oldCfg.PassEncrypted, encKey)
						if err == nil {
							cfg.PassEncrypted = decrypted
						}
					}
				}
			}
		}
	} else if cfg.PassEncrypted == "__CLEAR__" {
		cfg.PassEncrypted = ""
	}

	u := &url.URL{
		Scheme: "postgres", Host: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		User: url.UserPassword(cfg.User, cfg.PassEncrypted), Path: cfg.Database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	q.Set("connect_timeout", "3")
	u.RawQuery = q.Encode()
	dsn := u.String()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to open connection: %v", err)})
		return
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Database connection failed: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "Connection successful"})
}

// TestSinkConnection tests connection to the sink database.
func (h *Handler) TestSinkConnection(c *gin.Context) {
	var cfg protocol.SinkConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	// Parse DSN to check if password is masked. If ID is provided, reconstruct it.
	if cfg.ID != "" && cfg.DSN != "" {
		u, err := url.Parse(cfg.DSN)
		if err == nil && u.User != nil {
			pass, hasPass := u.User.Password()
			if hasPass && pass == "***" {
				key := protocol.SinkConfigKey(cfg.ID)
				if entry, err := h.kv.Get(key); err == nil {
					var oldCfg protocol.SinkConfig
					if err := json.Unmarshal(entry.Value(), &oldCfg); err == nil {
						encKey, err := crypto.GetEncryptionKey()
						if err != nil {
							c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
							return
						}
						decrypted, err := crypto.Decrypt(oldCfg.DSN, encKey)
						if err == nil {
							cfg.DSN = reconstructDSN(cfg.DSN, decrypted)
						}
					}
				}
			}
		}
	}

	if cfg.Type == "postgres_debug" || cfg.Type == "databend" {
		// T2-1: Validate host resolves to a non-private IP before attempting connection.
		// Extract host from DSN for validation.
		var host string
		if u, err := url.Parse(cfg.DSN); err == nil {
			host = u.Hostname()
		}
		if host != "" {
			if errMsg := validateHost(host); errMsg != "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
				return
			}
		}

		db, err := sql.Open(cfg.Type, cfg.DSN)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to open connection: %v", err)})
			return
		}
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := db.PingContext(ctx); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Connection failed: %v", err)})
			return
		}
	} else {
		_, err := url.Parse(cfg.DSN)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid DSN: %v", err)})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "Connection successful"})
}
