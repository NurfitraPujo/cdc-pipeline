package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/api"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	_ "bitbucket.com/daya-engineering/daya-data-pipeline/docs"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// @title           Daya Data Pipeline API
// @version         1.0
// @description     Control plane API for managing CDC data pipelines.
// @host            localhost:8080
// @BasePath        /api/v1

// @securityDefinitions.apikey Bearer
// @in header
// @name Authorization
// @description Type "Bearer " followed by your JWT token.

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to get JetStream context: %v", err)
	}

	kv, err := js.KeyValue(protocol.KVBucketName)
	if err != nil {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: protocol.KVBucketName,
		})
		if err != nil {
			log.Fatalf("Failed to get or create KV bucket: %v", err)
		}
	}

	h := api.NewHandler(kv)

	r := gin.Default()

	// Public Health Checks (for Kubernetes)
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
				sources.PUT("/:id", h.UpdateSource)
				sources.DELETE("/:id", h.DeleteSource)
				sources.GET("/:id/tables", h.ListTables)
			}

			sinks := authorized.Group("/sinks")
			{
				sinks.GET("", h.ListSinks)
				sinks.POST("", h.CreateSink)
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
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// #nosec G706 -- port is from environment
	log.Printf("API Server started on :%s", port)

	<-ctx.Done()
	log.Println("Shutting down API server...")
}
