package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

type Handler struct {
	kv nats.KeyValue
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

// --- Pipelines ---

// ListPipelines returns all pipelines.
// @Summary      List pipelines
// @Description  Retrieve all pipeline configurations
// @Tags         pipelines
// @Produce      json
// @Security     Bearer
// @Success      200  {object}  map[string][]protocol.PipelineConfig
// @Router       /pipelines [get]
func (h *Handler) ListPipelines(c *gin.Context) {
	// Cleanup stale worker heartbeats while we're at it
	go h.cleanupStaleHeartbeats()

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
				pipelines = append(pipelines, cfg)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"pipelines": pipelines})
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

	statusMap := make(map[string]any)
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
				var st protocol.TableStats
				if err := json.Unmarshal(entry.Value(), &st); err == nil {
					statusMap[key] = st
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"pipeline_id": id, "status": statusMap})
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

	// #nosec G117
	data, _ := json.Marshal(cfg)
	key := protocol.SourceConfigKey(cfg.ID)
	if _, err := h.kv.Put(key, data); err != nil {
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

	// #nosec G117
	data, _ := json.Marshal(cfg)
	key := protocol.SourceConfigKey(id)
	if _, err := h.kv.Put(key, data); err != nil {
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

	c.JSON(http.StatusOK, cfg)
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

	var sinks []protocol.SinkConfig
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSinkConfig) {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}
			var cfg protocol.SinkConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
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

	data, _ := json.Marshal(cfg)
	key := protocol.SinkConfigKey(cfg.ID)
	if _, err := h.kv.Put(key, data); err != nil {
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

	data, _ := json.Marshal(cfg)
	key := protocol.SinkConfigKey(id)
	if _, err := h.kv.Put(key, data); err != nil {
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
	if err != nil { return }

	for _, key := range keys {
		if strings.HasPrefix(key, "daya.worker.") && strings.HasSuffix(key, ".heartbeat") {
			entry, err := h.kv.Get(key)
			if err != nil { continue }

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
				"key":  key,
				"data": data,
			})
			c.Writer.Flush()
		}
	}
}
