package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/api"
	"github.com/NurfitraPujo/cdc-pipeline/internal/logger"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
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

func main() {
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

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to NATS")
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get JetStream context")
	}

	kv, err := js.KeyValue(protocol.KVBucketName)
	if err != nil {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: protocol.KVBucketName,
		})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to get or create KV bucket")
		}
	}

	h := api.NewHandler(kv)

	// Use custom recovery to log errors with zerolog
	r := gin.New()
	r.Use(gin.LoggerWithWriter(os.Stderr), gin.Recovery())

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
		v1.POST("/login", h.Login)

		authorized := v1.Group("/")
		authorized.Use(api.AuthMiddleware())
		{
			authorized.GET("/global", h.GetGlobalConfig)
			authorized.PUT("/global", h.UpdateGlobalConfig)

			pipelines := authorized.Group("/pipelines")
			{
				pipelines.GET("", h.ListPipelines)
				pipelines.POST("", h.CreatePipeline)
				pipelines.GET("/:id", h.GetPipeline)
				pipelines.PUT("/:id", h.UpdatePipeline)
				pipelines.DELETE("/:id", h.DeletePipeline)
				pipelines.GET("/:id/status", h.GetPipelineStatus)
				pipelines.GET("/:id/metrics", h.StreamMetrics)
			}

			sources := authorized.Group("/sources")
			{
				sources.GET("", h.ListSources)
				sources.POST("", h.CreateSource)
				sources.GET("/:id", h.GetSource)
				sources.PUT("/:id", h.UpdateSource)
				sources.DELETE("/:id", h.DeleteSource)
				sources.GET("/:id/tables", h.ListSourceTables)
			}

			sinks := authorized.Group("/sinks")
			{
				sinks.GET("", h.ListSinks)
				sinks.POST("", h.CreateSink)
				sinks.GET("/:id", h.GetSink)
				sinks.PUT("/:id", h.UpdateSink)
				sinks.DELETE("/:id", h.DeleteSink)
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
