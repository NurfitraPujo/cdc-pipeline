package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// CORSMiddleware returns a Gin middleware that handles CORS for specific allowed origins.
func CORSMiddleware() gin.HandlerFunc {
	allowedOrigins := map[string]bool{
		"http://localhost:3000":     true,
		"http://cdc-stag.daya.ai":    true,
		"https://cdc-stag.daya.ai":   true,
		"http://cdc.daya.ai":         true,
		"https://cdc.daya.ai":        true,
	}

	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if allowedOrigins[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE, PATCH")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Access-Control-Max-Age", "86400")
		}

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
