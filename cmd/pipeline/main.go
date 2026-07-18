package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/config"
	"github.com/NurfitraPujo/cdc-pipeline/internal/crypto"
	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/infra"
	"github.com/NurfitraPujo/cdc-pipeline/internal/logger"
	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	go_nats "github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

//go:embed config.example.yaml
var defaultConfigFile []byte

func main() {
	// 1. Initialize Logger
	logLvl := os.Getenv("LOG_LEVEL")
	if logLvl == "" {
		logLvl = "info"
	}
	isDev := os.Getenv("ENV") != "production"
	logger.Init(logLvl, isDev)

	log.Info().Msg("CDC Data Pipeline starting...")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Initialize Infrastructure
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = go_nats.DefaultURL
	}

	nc, kv, err := infra.InitNATS(infra.NATSConfig{URL: natsURL})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize infrastructure")
	}
	defer nc.Close()

	if err := bootstrapKV(kv); err != nil {
		log.Fatal().Err(err).Msg("Failed to bootstrap KV")
	}

	// 3. Shared Resources
	sharedPub, err := nats.NewNatsPublisher(natsURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create shared NATS publisher")
	}
	defer sharedPub.Close()

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown-worker"
	}
	workerID := fmt.Sprintf("%s-%s", hostname, time.Now().Format("05.000"))
	workerGroup := os.Getenv("WORKER_GROUP")

	// 4. Extract Factory
	pipelineFactory := &engine.PipelineFactory{
		KV:          kv,
		Publisher:   sharedPub,
		NatsURL:     natsURL,
		WorkerGroup: workerGroup,
	}

	mgr := config.NewConfigManager(kv, pipelineFactory.CreateWorker)
	if err := mgr.Watch(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to start config watcher")
	}

	// 5. Worker Lifecycle (Heartbeat)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		startTime := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				hb := protocol.WorkerHeartbeat{
					WorkerID:  workerID,
					Status:    "online",
					UptimeSec: int64(t.Sub(startTime).Seconds()),
					UpdatedAt: t,
				}
				data, _ := json.Marshal(hb)
				if _, err := kv.Put(protocol.WorkerHeartbeatKey(workerID), data); err != nil {
					log.Warn().Err(err).Msg("Failed to update worker heartbeat")
				}
				metrics.WorkerHeartbeat.WithLabelValues(workerID).Set(float64(t.Unix()))
			}
		}
	}()

	// 6. Observability
	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8081"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if nc.Status() == go_nats.CONNECTED {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("READY"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("NATS NOT CONNECTED"))
		}
	})
	mux.Handle("/metrics", promhttp.Handler())

	healthSrv := &http.Server{
		Addr:              ":" + healthPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info().Str("port", healthPort).Msg("Health check server started")
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("Health check server failed to start")
		}
	}()

	log.Info().Str("worker_id", workerID).Msg("CDC Data Pipeline Worker started. Waiting for configuration...")
	<-ctx.Done()
	log.Info().Msg("Shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mgr.Stop(shutdownCtx)
}

func bootstrapKV(kv go_nats.KeyValue) error {
	keys, err := kv.Keys()
	if err == nil && len(keys) > 0 {
		return nil
	}

	log.Info().Msg("KV bucket empty. Bootstrapping from embedded config.example.yaml...")

	var seed struct {
		Auth      protocol.UserConfig       `yaml:"auth"`
		Global    protocol.GlobalConfig     `yaml:"global"`
		Sources   []protocol.SourceConfig   `yaml:"sources"`
		Sinks     []protocol.SinkConfig     `yaml:"sinks"`
		Pipelines []protocol.PipelineConfig `yaml:"pipelines"`
	}

	if err := yaml.Unmarshal(defaultConfigFile, &seed); err != nil {
		return err
	}

	// Dynamically override config values using environment variables at runtime
	for i := range seed.Sources {
		if seed.Sources[i].Type == "postgres" {
			if h := os.Getenv("POSTGRES_SOURCE_HOST"); h != "" {
				seed.Sources[i].Host = h
			}
			if p := os.Getenv("POSTGRES_SOURCE_PORT"); p != "" {
				var port int
				if _, err := fmt.Sscanf(p, "%d", &port); err == nil {
					seed.Sources[i].Port = port
				}
			}
			if u := os.Getenv("POSTGRES_SOURCE_USER"); u != "" {
				seed.Sources[i].User = u
			} else if u := os.Getenv("POSTGRES_USER"); u != "" {
				seed.Sources[i].User = u
			}
			if pw := os.Getenv("POSTGRES_SOURCE_PASSWORD"); pw != "" {
				seed.Sources[i].PassEncrypted = pw
			} else if pw := os.Getenv("POSTGRES_PASSWORD"); pw != "" {
				seed.Sources[i].PassEncrypted = pw
			}
			if db := os.Getenv("POSTGRES_SOURCE_DB"); db != "" {
				seed.Sources[i].Database = db
			} else if db := os.Getenv("POSTGRES_DB"); db != "" {
				seed.Sources[i].Database = db
			}
		}
	}

	for i := range seed.Sinks {
		if seed.Sinks[i].Type == "databend" {
			if dsn := os.Getenv("DATABEND_DSN"); dsn != "" {
				seed.Sinks[i].DSN = dsn
			} else {
				dbHost := os.Getenv("DATABEND_HOST")
				if dbHost != "" {
					dbPort := os.Getenv("DATABEND_PORT")
					if dbPort == "" {
						dbPort = "8000"
					}
					seed.Sinks[i].DSN = fmt.Sprintf("http://root:@%s:%s", dbHost, dbPort)
				}
			}
		} else if seed.Sinks[i].Type == "postgres_debug" {
			if dsn := os.Getenv("POSTGRES_DEBUG_DSN"); dsn != "" {
				seed.Sinks[i].DSN = dsn
			} else {
				dbHost := os.Getenv("POSTGRES_DEBUG_HOST")
				if dbHost != "" {
					dbPort := os.Getenv("POSTGRES_DEBUG_PORT")
					if dbPort == "" {
						dbPort = "5432"
					}
					dbUser := os.Getenv("POSTGRES_DEBUG_USER")
					if dbUser == "" {
						dbUser = "postgres"
					}
					dbPass := os.Getenv("POSTGRES_DEBUG_PASSWORD")
					if dbPass == "" {
						dbPass = os.Getenv("POSTGRES_PASSWORD")
					}
					if dbPass == "" {
						dbPass = "postgres"
					}
					dbName := os.Getenv("POSTGRES_DEBUG_DB")
					if dbName == "" {
						dbName = "debug_db"
					}
					seed.Sinks[i].DSN = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort, dbName)
				}
			}
		}
	}

	// Hash the default password before storage
	hashed, err := bcrypt.GenerateFromPassword([]byte(seed.Auth.Password), bcrypt.DefaultCost)
	if err == nil {
		seed.Auth.Password = string(hashed)
	} else {
		log.Error().Err(err).Msg("Failed to hash bootstrap password")
	}

	// Encrypt sensitive credentials for Sources and Sinks using internal/crypto
	key, err := crypto.GetEncryptionKey()
	if err != nil {
		return fmt.Errorf("failed to bootstrap: %w", err)
	}
	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return fmt.Errorf("failed to bootstrap: ENCRYPTION_KEY must be 16, 24, or 32 bytes (got %d)", len(key))
	}

	for i := range seed.Sources {
		if seed.Sources[i].PassEncrypted != "" {
			enc, err := crypto.Encrypt(seed.Sources[i].PassEncrypted, key)
			if err != nil {
				return fmt.Errorf("failed to encrypt source password for %s: %w", seed.Sources[i].ID, err)
			}
			seed.Sources[i].PassEncrypted = enc
		}
	}

	for i := range seed.Sinks {
		if seed.Sinks[i].DSN != "" {
			enc, err := crypto.Encrypt(seed.Sinks[i].DSN, key)
			if err != nil {
				return fmt.Errorf("failed to encrypt sink DSN for %s: %w", seed.Sinks[i].ID, err)
			}
			seed.Sinks[i].DSN = enc
		}
	}

	authData, _ := json.Marshal(seed.Auth)
	if _, err := kv.Put(protocol.KeyAuthConfig, authData); err != nil {
		log.Warn().Err(err).Msg("Failed to bootstrap auth config")
	}

	globalData, _ := json.Marshal(seed.Global)
	if _, err := kv.Put(protocol.KeyGlobalConfig, globalData); err != nil {
		log.Warn().Err(err).Msg("Failed to bootstrap global config")
	}

	for _, sc := range seed.Sources {
		data, _ := json.Marshal(sc)
		if _, err := kv.Put(protocol.SourceConfigKey(sc.ID), data); err != nil {
			log.Warn().Err(err).Str("source_id", sc.ID).Msg("Failed to bootstrap source")
		}
	}
	for _, sc := range seed.Sinks {
		data, _ := json.Marshal(sc)
		if _, err := kv.Put(protocol.SinkConfigKey(sc.ID), data); err != nil {
			log.Warn().Err(err).Str("sink_id", sc.ID).Msg("Failed to bootstrap sink")
		}
	}
	for _, pc := range seed.Pipelines {
		data, _ := json.Marshal(pc)
		if _, err := kv.Put(protocol.PipelineConfigKey(pc.ID), data); err != nil {
			log.Warn().Err(err).Str("pipeline_id", pc.ID).Msg("Failed to bootstrap pipeline")
		}
	}

	log.Info().Msg("Bootstrapping complete.")
	return nil
}
