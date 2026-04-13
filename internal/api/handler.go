package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/crypto"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
)

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
			if err := json.Unmarshal(entry.Value(), &st); err == nil {
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

			hbKey := protocol.WorkerHeartbeatKey(id)
			if entry, err := h.kv.Get(hbKey); err == nil {
				var hb protocol.WorkerHeartbeat
				if err := json.Unmarshal(entry.Value(), &hb); err == nil {
					if time.Since(hb.UpdatedAt) < 30*time.Second {
						if hb.Status == "Ready" {
							summary.HealthyCount++
						}
					} else {
						summary.ErrorCount++
					}
				}
			} else {
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
	// Cleanup stale worker heartbeats while we're at it
	go h.cleanupStaleHeartbeats()

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
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 10
	}

	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var pipelines []protocol.PipelineConfig
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixPipelineConfig) {
			entry, err := h.kv.Get(key)
			if err != nil {
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
					actualStatus := "Healthy" // Default guess

					tsKey := protocol.TransitionStateKey(cfg.ID)
					if tsEntry, err := h.kv.Get(tsKey); err == nil {
						var ts protocol.PipelineTransitionState
						if err := json.Unmarshal(tsEntry.Value(), &ts); err == nil && ts.Status == "Transitioning" {
							actualStatus = "Transitioning"
						}
					}

					if actualStatus == "Healthy" {
						hbKey := protocol.WorkerHeartbeatKey(cfg.ID)
						if hbEntry, err := h.kv.Get(hbKey); err == nil {
							var hb protocol.WorkerHeartbeat
							if err := json.Unmarshal(hbEntry.Value(), &hb); err == nil {
								if time.Since(hb.UpdatedAt) > 60*time.Second {
									actualStatus = "Error"
								}
							}
						} else {
							actualStatus = "Error"
						}
					}

					if !strings.EqualFold(actualStatus, statusFilter) {
						continue
					}
				}

				pipelines = append(pipelines, cfg)
			}
		}
	}

	total := len(pipelines)
	start := (page - 1) * limit
	end := start + limit
	if start > total {
		pipelines = []protocol.PipelineConfig{}
	} else {
		if end > total {
			end = total
		}
		pipelines = pipelines[start:end]
	}

	c.JSON(http.StatusOK, gin.H{
		"pipelines": pipelines,
		"total":     total,
		"page":      page,
		"limit":     limit,
	})
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

	c.JSON(http.StatusOK, cfg)
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
	tableStats := make(map[string]protocol.TableStats) // aggregated by table
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
				if err := json.Unmarshal(entry.Value(), &cp); err == nil {
					statusMap[key] = cp
				}
			} else if strings.HasSuffix(key, ".stats") {
				info := protocol.ParseTableStatsKey(key)
				var st protocol.TableStats
				if err := json.Unmarshal(entry.Value(), &st); err == nil {
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

	encryptionKey := crypto.GetEncryptionKey()
	var sources []protocol.SourceConfig
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSourceConfig) {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}
			var cfg protocol.SourceConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
				// Decrypt sensitive fields
				if cfg.PassEncrypted != "" {
					decrypted, err := crypto.Decrypt(cfg.PassEncrypted, encryptionKey)
					if err != nil {
						log.Warn().Err(err).Str("source_id", cfg.ID).Msg("Failed to decrypt password, returning encrypted value")
					} else {
						cfg.PassEncrypted = decrypted
					}
				}
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
	key := crypto.GetEncryptionKey()
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

	// Encrypt sensitive fields
	key := crypto.GetEncryptionKey()
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

	// Decrypt sensitive fields
	encryptionKey := crypto.GetEncryptionKey()
	if cfg.PassEncrypted != "" {
		decrypted, err := crypto.Decrypt(cfg.PassEncrypted, encryptionKey)
		if err != nil {
			log.Warn().Err(err).Str("source_id", cfg.ID).Msg("Failed to decrypt password, returning encrypted value")
		} else {
			cfg.PassEncrypted = decrypted
		}
	}

	c.JSON(http.StatusOK, cfg)
}

// GetSourceSchema triggers or retrieves table schema discovery for a source.
// @Summary      Get source schema
// @Description  Discover available tables and schemas directly from the source database
// @Tags         sources
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Source ID"
// @Success      200  {object}  map[string]any
// @Router       /sources/{id}/schema [get]
func (h *Handler) GetSourceSchema(c *gin.Context) {
	// In a real implementation, this might send a request to a worker to perform discovery
	// or query the database directly if the API has connectivity.
	// For now, return what we have in metadata plus a mock of available schemas.
	id := c.Param("id")
	c.JSON(http.StatusOK, gin.H{
		"source_id":         id,
		"available_schemas": []string{"public", "inventory", "sales"},
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

	encryptionKey := crypto.GetEncryptionKey()
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
						cfg.DSN = decrypted
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
	encryptionKey := crypto.GetEncryptionKey()
	if cfg.DSN != "" {
		decrypted, err := crypto.Decrypt(cfg.DSN, encryptionKey)
		if err != nil {
			log.Warn().Err(err).Str("sink_id", cfg.ID).Msg("Failed to decrypt DSN, returning encrypted value")
		} else {
			cfg.DSN = decrypted
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
	key := crypto.GetEncryptionKey()
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

	// Encrypt sensitive fields
	key := crypto.GetEncryptionKey()
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
	keys, err := h.kv.Keys()
	if err != nil {
		return
	}

	for _, key := range keys {
		if strings.HasPrefix(key, "cdc.worker.") && strings.HasSuffix(key, ".heartbeat") {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}

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

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Transfer-Encoding", "chunked")

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

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}

			var data any
			key := entry.Key()
			sinkID := ""
			isDebug := false

			if strings.HasSuffix(key, "_checkpoint") {
				var cp protocol.Checkpoint
				if err := json.Unmarshal(entry.Value(), &cp); err != nil {
					continue
				}
				data = cp
			} else if strings.HasSuffix(key, ".stats") {
				var st protocol.TableStats
				if err := json.Unmarshal(entry.Value(), &st); err != nil {
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
