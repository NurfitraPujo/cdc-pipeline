package api

import (
	"encoding/json"
	"io"
	"log"
	"strings"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
)

func (h *Handler) StreamMetrics(c *gin.Context) {
	pipelineID := c.Param("id")
	
	// Watch all ingress and egress checkpoints, stats and transition state for this pipeline
	pattern := protocol.PipelineStatusPrefix(pipelineID) + "*"
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

			var data any
			key := entry.Key()

			if strings.HasSuffix(key, "_checkpoint") {
				var cp protocol.Checkpoint
				if err := json.Unmarshal(entry.Value(), &cp); err != nil {
					log.Printf("Error unmarshaling checkpoint %s: %v", key, err)
					return true
				}
				data = cp
			} else if strings.HasSuffix(key, ".stats") {
				var st protocol.TableStats
				if err := json.Unmarshal(entry.Value(), &st); err != nil {
					log.Printf("Error unmarshaling stats %s: %v", key, err)
					return true
				}
				data = st
			} else if strings.HasSuffix(key, ".transition") {
				var ts protocol.PipelineTransitionState
				if err := json.Unmarshal(entry.Value(), &ts); err != nil {
					log.Printf("Error unmarshaling transition %s: %v", key, err)
					return true
				}
				data = ts
			} else {
				data = string(entry.Value())
			}

			// Send as SSE event
			c.SSEvent("message", map[string]any{
				"key":  key,
				"data": data,
			})
			return true
		}
	})
}
