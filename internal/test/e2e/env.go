package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/config"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/engine"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/sink/databend"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/source/postgres"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/stream/nats"
	go_nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

type Environment struct {
	T *testing.T
	Ctx context.Context
	
	Nats     *go_nats.Conn
	JS       go_nats.JetStreamContext
	KV       go_nats.KeyValue
	NatsURL  string

	Postgres *sql.DB
	PgConfig protocol.SourceConfig

	Databend *sql.DB
	DbConfig protocol.SinkConfig

	Mgr *config.ConfigManager
}

func Setup(t *testing.T) *Environment {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	// 1. Start NATS
	natsC, err := StartNats(ctx)
	require.NoError(t, err)
	natsURL, _ := natsC.ConnectionString(ctx)
	nc, _ := go_nats.Connect(natsURL)
	js, _ := nc.JetStream()
	kv, _ := js.CreateKeyValue(&go_nats.KeyValueConfig{Bucket: protocol.KVBucketName})

	// 2. Start Postgres
	pgC, err := StartPostgres(ctx)
	require.NoError(t, err)
	pgHost, _ := pgC.Host(ctx)
	pgPort, _ := pgC.MappedPort(ctx, "5432")
	pgDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/daya_src?sslmode=disable", pgHost, pgPort.Port())
	pgDB, err := sql.Open("pgx", pgDSN)
	require.NoError(t, err)

	// Create Publication
	_, err = pgDB.Exec("CREATE PUBLICATION daya_pub FOR ALL TABLES")
	require.NoError(t, err)

	srcCfg := protocol.SourceConfig{
		ID:              "pg1",
		Type:            "postgres",
		Host:            pgHost,
		Port:            pgPort.Int(),
		User:            "postgres",
		PassEncrypted:   "postgres",
		Database:        "daya_src",
		SlotName:        "daya_slot",
		PublicationName: "daya_pub",
		Schemas:         []string{"public"},
	}

	// 3. Start Databend
	dbC, dbDSN, err := StartDatabend(ctx)
	require.NoError(t, err)
	dbDB, err := sql.Open("databend", dbDSN)
	require.NoError(t, err)

	snkCfg := protocol.SinkConfig{
		ID:   "db1",
		Type: "databend",
		DSN:  dbDSN,
	}

	// Store configs in KV
	srcData, _ := json.Marshal(srcCfg)
	kv.Put(protocol.SourceConfigKey(srcCfg.ID), srcData)
	snkData, _ := json.Marshal(snkCfg)
	kv.Put(protocol.SinkConfigKey(snkCfg.ID), snkData)

	t.Cleanup(func() {
		pgDB.Close()
		dbDB.Close()
		nc.Close()
		natsC.Terminate(context.Background())
		pgC.Terminate(context.Background())
		dbC.Terminate(context.Background())
	})

	return &Environment{
		T:        t,
		Ctx:      ctx,
		Nats:     nc,
		JS:       js,
		KV:       kv,
		NatsURL:  natsURL,
		Postgres: pgDB,
		PgConfig: srcCfg,
		Databend: dbDB,
		DbConfig: snkCfg,
	}
}

func (e *Environment) StartWorker() {
	factory := func(workerCtx context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		// Initialize Source
		src := postgres.NewPostgresSource(e.PgConfig.ID)
		
		// Initialize Publisher/Subscriber
		pub, err := nats.NewNatsPublisher(e.NatsURL)
		if err != nil {
			return nil, err
		}
		sub, err := nats.NewNatsSubscriber(e.NatsURL, fmt.Sprintf("daya-worker-%s", id))
		if err != nil {
			return nil, err
		}

		snk, err := databend.NewDatabendSink(e.DbConfig.ID, e.DbConfig.DSN)
		if err != nil {
			return nil, err
		}

		prod := engine.NewProducer(id, cfg, src, pub, e.KV)
		cons := engine.NewConsumer(id, sub, snk, e.KV, cfg.BatchSize, cfg.BatchWait)
		
		pipe := engine.NewPipeline(id, prod, cons, cfg)
		if err := pipe.Start(workerCtx); err != nil {
			return nil, err
		}
		return pipe, nil
	}

	e.Mgr = config.NewConfigManager(e.KV, factory)
	require.NoError(e.T, e.Mgr.Watch(e.Ctx))
}
