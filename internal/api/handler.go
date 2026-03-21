package api

import (
	"encoding/json"
	"fmt"
	"net/http"

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

func (h *Handler) ListPipelines(c *gin.Context) {
	// Logic to list all pipelines from KV
	// Keys: pipelines.{id}.config
	c.JSON(http.StatusOK, gin.H{"pipelines": []string{}})
}

func (h *Handler) CreatePipeline(c *gin.Context) {
	var cfg protocol.PipelineConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	data, _ := json.Marshal(cfg)
	key := fmt.Sprintf("pipelines.%s.config", cfg.ID)
	if _, err := h.kv.Put(key, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, cfg)
}

func (h *Handler) GetPipeline(c *gin.Context) {
	id := c.Param("id")
	key := fmt.Sprintf("pipelines.%s.config", id)
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

func (h *Handler) ListSources(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"sources": []string{}})
}

func (h *Handler) CreateSource(c *gin.Context) {
	var cfg protocol.SourceConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	data, _ := json.Marshal(cfg)
	key := fmt.Sprintf("sources.%s.config", cfg.ID)
	if _, err := h.kv.Put(key, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, cfg)
}
