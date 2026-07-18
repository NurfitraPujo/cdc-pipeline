package api

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

const (
	// RateLimit requests per second
	RateLimit = 10
	// BurstSize for token bucket
	BurstSize = 20
)

type clientLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type rateLimiters struct {
	mu        sync.RWMutex
	clients   map[string]*clientLimiter
	rate      rate.Limit
	burst     int
	expireAge time.Duration
}

func newRateLimiters(r rate.Limit, b int) *rateLimiters {
	rl := &rateLimiters{
		clients:   make(map[string]*clientLimiter),
		rate:      r,
		burst:     b,
		expireAge: 5 * time.Minute,
	}
	go rl.cleanup()
	return rl
}

func (rl *rateLimiters) getLimiter(clientIP string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if cl, exists := rl.clients[clientIP]; exists {
		cl.lastSeen = time.Now()
		return cl.limiter
	}

	limiter := rate.NewLimiter(rl.rate, rl.burst)
	rl.clients[clientIP] = &clientLimiter{
		limiter:  limiter,
		lastSeen: time.Now(),
	}
	return limiter
}

func (rl *rateLimiters) cleanup() {
	ticker := time.NewTicker(rl.expireAge)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		for ip, cl := range rl.clients {
			if time.Since(cl.lastSeen) > rl.expireAge {
				delete(rl.clients, ip)
			}
		}
		rl.mu.Unlock()
	}
}

var globalLimiters = newRateLimiters(RateLimit, BurstSize)

// RateLimitMiddleware returns a gin middleware that rate limits requests per client IP.
func RateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		clientIP := c.ClientIP()
		limiter := globalLimiters.getLimiter(clientIP)

		if !limiter.Allow() {
			c.Header("X-RateLimit-Remaining", "0")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded",
			})
			return
		}

		c.Header("X-RateLimit-Remaining", "19")
		c.Next()
	}
}
