package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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
	"github.com/testcontainers/testcontainers-go"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
	tc_postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

type Environment struct {
	T *testing.T
	Ctx context.Context
	Cancel context.CancelFunc
	
	NatsConn *go_nats.Conn
	JS       go_nats.JetStreamContext
	KV       go_nats.KeyValue
	NatsURL  string

	Postgres *sql.DB
	PgConfig protocol.SourceConfig

	Databend *sql.DB
	DbConfig protocol.SinkConfig

	Mgr *config.ConfigManager

	// Containers for termination
	NatsC     *tc_nats.NATSContainer
	PostgresC *tc_postgres.PostgresContainer
	DatabendC testcontainers.Container
}

func Setup(t *testing.T) *Environment {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	// 1. Start NATS
	natsC, err := StartNats(ctx)
	require.NoError(t, err)
	natsURL, _ := natsC.ConnectionString(ctx)
	nc, _ := go_nats.Connect(natsURL)
	js, _ := nc.JetStream()
	
	_, err = js.AddStream(&go_nats.StreamConfig{
		Name:     "daya-data-pipeline",
		Subjects: []string{"daya.pipeline.*.ingest"},
	})
	require.NoError(t, err)

	kv, _ := js.CreateKeyValue(&go_nats.KeyValueConfig{Bucket: protocol.KVBucketName})

	// 2. Start Postgres
	pgC, err := StartPostgres(ctx)
	require.NoError(t, err)
	pgHost, _ := pgC.Host(ctx)
	pgPort, _ := pgC.MappedPort(ctx, "5432")
	pgDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/daya_src?sslmode=disable", pgHost, pgPort.Port())
	pgDB, err := sql.Open("pgx", pgDSN)
	require.NoError(t, err)

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
		SlotName:        fmt.Sprintf("daya_slot_%d", time.Now().UnixNano()),
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

	srcData, _ := json.Marshal(srcCfg)
	kv.Put(protocol.SourceConfigKey(srcCfg.ID), srcData)
	snkData, _ := json.Marshal(snkCfg)
	kv.Put(protocol.SinkConfigKey(snkCfg.ID), snkData)

	env := &Environment{
		T:         t,
		Ctx:       ctx,
		Cancel:    cancel,
		NatsConn:  nc,
		JS:        js,
		KV:        kv,
		NatsURL:   natsURL,
		Postgres:  pgDB,
		PgConfig:  srcCfg,
		Databend:  dbDB,
		DbConfig:  snkCfg,
		NatsC:     natsC,
		PostgresC: pgC,
		DatabendC: dbC,
	}

	t.Cleanup(func() {
		env.Close()
	})

	return env
}

func (e *Environment) GetKV() go_nats.KeyValue {
	return e.KV
}

func (e *Environment) Close() {
	log.Printf("Cleaning up E2E environment...")
	if e.Mgr != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

		e.Mgr.Stop(ctx)
		cancel()
	}
	
	time.Sleep(1 * time.Second)

	if e.Postgres != nil { e.Postgres.Close() }
	if e.Databend != nil { e.Databend.Close() }
	if e.NatsConn != nil { e.NatsConn.Close() }
	
	if e.PostgresC != nil { e.PostgresC.Terminate(context.Background()) }
	if e.DatabendC != nil { e.DatabendC.Terminate(context.Background()) }
	if e.NatsC != nil { e.NatsC.Terminate(context.Background()) }
	
	if e.Cancel != nil { e.Cancel() }
}

func (e *Environment) StartWorker() {
	factory := func(workerCtx context.Context, id string, cfg protocol.PipelineConfig) (engine.PipelineWorker, error) {
		if len(cfg.Sources) == 0 { return nil, fmt.Errorf("no sources") }
		sourceID := cfg.Sources[0]
		entry, _ := e.KV.Get(protocol.SourceConfigKey(sourceID))
		var srcCfg protocol.SourceConfig
		json.Unmarshal(entry.Value(), &srcCfg)
		srcCfg.Tables = cfg.Tables
		// Ensure unique slot for every worker instance to avoid contention on reload
		srcCfg.SlotName = fmt.Sprintf("%s_%d", srcCfg.SlotName, time.Now().UnixNano())

		src := postgres.NewPostgresSource(sourceID)
		
		if len(cfg.Sinks) == 0 { return nil, fmt.Errorf("no sinks") }
		sinkID := cfg.Sinks[0]
		entry, _ = e.KV.Get(protocol.SinkConfigKey(sinkID))
		var snkCfg protocol.SinkConfig
		json.Unmarshal(entry.Value(), &snkCfg)

		snk, _ := databend.NewDatabendSink(sinkID, snkCfg.DSN)
		pub, _ := nats.NewNatsPublisher(e.NatsURL)
		sub, _ := nats.NewNatsSubscriber(e.NatsURL, fmt.Sprintf("daya-worker-%s", id), 1000, 30*time.Second)

		prod := engine.NewProducer(id, cfg, src, pub, e.KV)
		cons := engine.NewConsumer(id, sub, pub, snk, e.KV, cfg.BatchSize, cfg.BatchWait, protocol.RetryConfig{MaxRetries: 3})
		
		pipe := engine.NewPipeline(id, prod, cons, cfg)
		pipe.Start(workerCtx)
		return pipe, nil
	}

	e.Mgr = config.NewConfigManager(e.KV, factory)
	e.Mgr.Watch(e.Ctx)
}

func (e *Environment) SeedPostgres(table string, rows int) {
	_, err := e.Postgres.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, name TEXT, age INT, metadata JSONB, created_at TIMESTAMP DEFAULT NOW())", table))
	require.NoError(e.T, err)

	for i := 0; i < rows; i++ {
		_, err = e.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age, metadata) VALUES ($1, $2, $3)", table), 
			fmt.Sprintf("user-%d", i), 20+(i%50), `{"key": "value"}`)
		require.NoError(e.T, err)
	}
}

func (e *Environment) EventuallyCountDatabend(table string, expected int, timeout time.Duration) {
	require.Eventually(e.T, func() bool {
		var count int
		err := e.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM \"%s\"", table)).Scan(&count)
		if err != nil {
			return false
		}
		if count > 0 {
			log.Printf("EventuallyCount [%s]: Current count: %d, Expected: %d", table, count, expected)
		}
		return count == expected
	}, timeout, 1*time.Second, "Expected %d rows in Databend table %s", expected, table)
}

func (e *Environment) EventuallyMatchDatabend(table string, id int, expected map[string]any, timeout time.Duration) {
	require.Eventually(e.T, func() bool {
		rows, err := e.Databend.Query(fmt.Sprintf("SELECT * FROM \"%s\" WHERE id = ?", table), id)
		if err != nil {
			return false
		}
		defer rows.Close()

		if !rows.Next() {
			return false
		}

		cols, _ := rows.Columns()
		values := make([]any, len(cols))
		pointers := make([]any, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return false
		}

		actual := make(map[string]any)
		for i, col := range cols {
			actual[col] = values[i]
		}

		match := true
		for k, v := range expected {
			if fmt.Sprintf("%v", actual[k]) != fmt.Sprintf("%v", v) {
				match = false
				break
			}
		}

		if !match {
			log.Printf("Match failed for table %s. Expected: %+v, Actual: %+v", table, expected, actual)
			return false
		}
		return true
	}, timeout, 1*time.Second)
}
