package api

import (
	"encoding/json"
	"fmt"
	"io"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
)

func (h *Handler) StreamMetrics(c *gin.Context) {
	pipelineID := c.Param("id")
	
	// Watch all ingress and egress checkpoints for this pipeline
	// Key pattern: pipelines.{id}.sources.*.tables.*.*_checkpoint
	pattern := fmt.Sprintf("pipelines.%s.sources.*.tables.*.*_checkpoint", pipelineID)
	watcher, err := h.kv.Watch(pattern)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	defer watcher.Stop()

	c.Stream(func(w io.Writer) bool {
		select {
		case <-c.Request.Context().Done():
			return false
		case entry := <-watcher.Updates():
			if entry == nil {
				return true
			}

			var cp protocol.Checkpoint
			if err := json.Unmarshal(entry.Value(), &cp); err != nil {
				return true
			}

			// Send as SSE event
			c.SSEvent("message", map[string]any{
				"key":        entry.Key(),
				"checkpoint": cp,
			})
			return true
		}
	})
}
