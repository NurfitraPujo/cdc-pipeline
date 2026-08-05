package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/api"
	"github.com/NurfitraPujo/cdc-pipeline/internal/config"
	"github.com/NurfitraPujo/cdc-pipeline/internal/infra"
	"github.com/NurfitraPujo/cdc-pipeline/internal/logger"
	_ "github.com/NurfitraPujo/cdc-pipeline/docs"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// @title           CDC Data Pipeline API
// @version         1.0
// @description     Control plane API for managing CDC data pipelines.
// @host            localhost:8080
// @BasePath        /api/v1

// @securityDefinitions.apikey Bearer
// @in header
// @name Authorization
// @description Type "Bearer " followed by your JWT token.

// T3-2: Fail fast if JWT_SECRET is empty or too short
func validateJWTSecret() {
	jwtSecret := os.Getenv("JWT_SECRET")
	if len(jwtSecret) < 32 {
		log.Fatal().Msg("JWT_SECRET must be set and >= 32 bytes")
	}
}

func main() {
	// T3-2: Validate JWT_SECRET before anything else
	validateJWTSecret()

	// 1. Initialize Logger
	logLvl := os.Getenv("LOG_LEVEL")
	if logLvl == "" {
		logLvl = "info"
	}
	isDev := os.Getenv("ENV") != "production"
	logger.Init(logLvl, isDev)

	if !isDev {
		gin.SetMode(gin.ReleaseMode)
	}

	log.Info().Msg("CDC Data Pipeline API starting...")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Initialize Infrastructure
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, kv, err := infra.InitNATS(infra.NATSConfig{URL: natsURL})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize infrastructure")
	}
	defer nc.Close()

	h := api.NewHandler(kv)

	// WS-3: install the WAL-growth-rate sampler so PausePipeline's
	// time-to-breach warning (plan section 5) is populated in production
	// instead of always skipping the projection (h.lagRateSampler nil).
	h.SetSlotLagRateSampler(h.NewSlotLagRateSampler())

	// WS-5: install the per-table partition-strategy probe so resuming
	// from Paused detects (rather than assumes) whether every table's
	// recorded snapshot chunks are actually integer_range (plan section
	// 10, OQ-5/OQ-7) before trusting a plain resume to cover them.
	h.SetPartitionStrategyChecker(h.NewPartitionStrategyChecker())

	// WS-5: install the same replication-slot health probe the pause-expiry
	// ticker (internal/config) already consults on timer expiry, so
	// StartPipeline's (Paused, start) -> Resuming guard stops trusting a
	// hardcoded SlotAlive: true and can actually reject a resume onto a
	// dead/invalidated slot (plan section 4.3).
	h.SetSlotHealthChecker(config.NewPostgresSlotHealthChecker(kv))

	// Seed default admin credentials for local dev / E2E (no-op in production
	// and when an auth config already exists in NATS KV).
	if err := api.EnsureDevAuth(kv); err != nil {
		log.Fatal().Err(err).Msg("Failed to seed dev auth")
	}

	// Use custom recovery to log errors with zerolog
	r := gin.New()
	r.Use(gin.LoggerWithWriter(os.Stderr), gin.Recovery())
	r.Use(api.CORSMiddleware())

	// Public Health Checks & Metrics (for Kubernetes)
	r.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})
	r.GET("/readyz", func(c *gin.Context) {
		if nc.Status() == nats.CONNECTED {
			c.String(http.StatusOK, "READY")
		} else {
			c.String(http.StatusServiceUnavailable, "NATS NOT CONNECTED")
		}
	})
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	v1 := r.Group("/api/v1")
	{
		v1.POST("/login", api.RateLimitMiddleware(), h.Login)

		authorized := v1.Group("/")
		authorized.Use(api.AuthMiddleware())
		{
			authorized.GET("/global", h.GetGlobalConfig)
			authorized.PUT("/global", h.UpdateGlobalConfig)

			stats := authorized.Group("/stats")
			{
				stats.GET("/summary", h.GetStatsSummary)
				stats.GET("/history", h.GetStatsHistory)
			}

			pipelines := authorized.Group("/pipelines")
			{
				pipelines.GET("", h.ListPipelines)
				pipelines.POST("", h.CreatePipeline)
				pipelines.GET("/:id", h.GetPipeline)
				pipelines.PUT("/:id", h.UpdatePipeline)
				pipelines.DELETE("/:id", h.DeletePipeline)
				pipelines.GET("/:id/status", h.GetPipelineStatus)
				pipelines.POST("/:id/restart", h.RestartPipeline)
				pipelines.POST("/:id/pause", h.PausePipeline)
				pipelines.POST("/:id/start", h.StartPipeline)
				pipelines.POST("/:id/stop", h.StopPipeline)
				pipelines.GET("/:id/pause-projection", h.PausePauseProjectionRoute)
				pipelines.GET("/:id/metrics", h.StreamMetrics)
			}

			sources := authorized.Group("/sources")
			{
				sources.GET("", h.ListSources)
				sources.POST("", h.CreateSource)
				sources.GET("/:id", h.GetSource)
				sources.PUT("/:id", h.UpdateSource)
				sources.DELETE("/:id", h.DeleteSource)
				sources.GET("/:id/schema", h.GetSourceSchema)
				sources.GET("/:id/tables", h.ListSourceTables)
				sources.POST("/test", h.TestSourceConnection)
			}

			sinks := authorized.Group("/sinks")
			{
				sinks.GET("", h.ListSinks)
				sinks.POST("", h.CreateSink)
				sinks.GET("/:id", h.GetSink)
				sinks.PUT("/:id", h.UpdateSink)
				sinks.DELETE("/:id", h.DeleteSink)
				sinks.POST("/test", h.TestSinkConnection)
			}

			workers := authorized.Group("/workers")
			{
				workers.GET("/:id/heartbeat", h.GetWorkerHeartbeat)
			}
		}
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           r,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info().Str("port", port).Msg("API server listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("API server failed")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("Shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("API server forced shutdown")
	}
}
