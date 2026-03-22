package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
)

type Handler struct {
	kv nats.KeyValue
}

func NewHandler(kv nats.KeyValue) *Handler {
	return &Handler{kv: kv}
}

// --- Global Config ---

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

func (h *Handler) ListPipelines(c *gin.Context) {
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

func (h *Handler) DeletePipeline(c *gin.Context) {
	id := c.Param("id")
	key := protocol.PipelineConfigKey(id)
	if err := h.kv.Delete(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusNoContent, nil)
}

func (h *Handler) GetPipelineStatus(c *gin.Context) {
	id := c.Param("id")
	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	statusMap := make(map[string]protocol.Checkpoint)
	prefix := protocol.PipelineStatusPrefix(id)
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) && (strings.HasSuffix(key, ".ingress_checkpoint") || strings.HasSuffix(key, ".egress_checkpoint")) {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}
			var cp protocol.Checkpoint
			if err := json.Unmarshal(entry.Value(), &cp); err == nil {
				statusMap[key] = cp
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"pipeline_id": id, "status": statusMap})
}

// --- Sources ---

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

func (h *Handler) CreateSource(c *gin.Context) {
	var cfg protocol.SourceConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	data, _ := json.Marshal(cfg)
	key := protocol.SourceConfigKey(cfg.ID)
	if _, err := h.kv.Put(key, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, cfg)
}

func (h *Handler) UpdateSource(c *gin.Context) {
	id := c.Param("id")
	var cfg protocol.SourceConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cfg.ID = id
	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	data, _ := json.Marshal(cfg)
	key := protocol.SourceConfigKey(id)
	if _, err := h.kv.Put(key, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

func (h *Handler) DeleteSource(c *gin.Context) {
	id := c.Param("id")
	key := protocol.SourceConfigKey(id)
	if err := h.kv.Delete(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusNoContent, nil)
}

func (h *Handler) ListTables(c *gin.Context) {
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
