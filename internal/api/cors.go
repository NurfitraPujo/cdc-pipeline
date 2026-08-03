package api

import (
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

// defaultAllowedOrigins is the production/staging allowlist used when
// CORS_ALLOWED_ORIGINS is unset.
var defaultAllowedOrigins = []string{
	"http://localhost:3000",
	"https://cdc-stag.daya.ai",
	"https://cdc.daya.ai",
}

// allowedOrigins resolves the CORS allowlist.
//
// CORS_ALLOWED_ORIGINS, when set, is a comma-separated list that REPLACES the
// defaults. The list was previously hardcoded, so the dashboard could only be
// served from one of three fixed origins; anything else -- another deployment,
// or a dev/test server on a different port -- got an opaque "Failed to fetch"
// in the browser with no CORS header to explain it.
func allowedOrigins() map[string]bool {
	list := defaultAllowedOrigins
	if raw := os.Getenv("CORS_ALLOWED_ORIGINS"); raw != "" {
		list = strings.Split(raw, ",")
	}

	origins := make(map[string]bool, len(list))
	for _, o := range list {
		if trimmed := strings.TrimSpace(o); trimmed != "" {
			origins[trimmed] = true
		}
	}
	return origins
}

// CORSMiddleware returns a Gin middleware that handles CORS for allowed origins.
func CORSMiddleware() gin.HandlerFunc {
	// Resolved once at construction: the allowlist is process configuration,
	// not per-request state.
	origins := allowedOrigins()

	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origins[origin] {
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
