package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"

	"strings"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/config"
	"github.com/NurfitraPujo/cdc-pipeline/internal/engine"
	"github.com/NurfitraPujo/cdc-pipeline/internal/logger"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	_ "github.com/NurfitraPujo/cdc-pipeline/internal/sink/databend"
	_ "github.com/NurfitraPujo/cdc-pipeline/internal/sink/postgresdebug"
	"github.com/NurfitraPujo/cdc-pipeline/internal/stream/nats"
	go_nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
	tc_postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

type Environment struct {
	Ctx context.Context
	T   *testing.T

	NatsURL string
	KV      go_nats.KeyValue

	Postgres *sql.DB
	PgConfig protocol.SourceConfig

	Databend *sql.DB
	DbConfig protocol.SinkConfig

	DebugDB *sql.DB

	Mgr *config.ConfigManager

	// Containers for termination
	NatsC     *tc_nats.NATSContainer
	PostgresC *tc_postgres.PostgresContainer
	DatabendC testcontainers.Container
}

func Setup(t *testing.T) *Environment {
	ctx := context.Background()

	logger.Init("debug", true)
	SetTestContainerProvider()

	// 1. NATS
	natsC, err := StartNats(ctx)
	require.NoError(t, err)
	natsURL, _ := natsC.ConnectionString(ctx)

	nc, err := go_nats.Connect(natsURL)
	require.NoError(t, err)
	js, _ := nc.JetStream()
	
	bucketName := fmt.Sprintf("cdc-test-%s", strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_")))
	kv, _ := js.CreateKeyValue(&go_nats.KeyValueConfig{Bucket: bucketName})

	// Set small timeouts for faster E2E transitions
	globalCfg := protocol.GlobalConfig{
		BatchSize:          1000,
		BatchWait:          100 * time.Millisecond,
		DrainTimeout:       5 * time.Second,
		ShutdownTimeout:    5 * time.Second,
		StabilizationDelay: 2 * time.Second,
		GlobalReloadDelay:  500 * time.Millisecond,
	}
	globalData, _ := json.Marshal(globalCfg)
	kv.Put(protocol.KeyGlobalConfig, globalData)

	// 2. Postgres
	pgC, err := StartPostgres(ctx)
	require.NoError(t, err)
	pgHost, _ := pgC.Host(ctx)
	pgPort, _ := pgC.MappedPort(ctx, "5432")
	pgDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/cdc_src?sslmode=disable", pgHost, pgPort.Port())
	pgDB, _ := sql.Open("postgres", pgDSN)

	randomID := ""
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	for i := 0; i < 6; i++ {
		randomID += string(charset[rand.Intn(len(charset))])
	}
	slotName := fmt.Sprintf("cdc_slot_%s_%s", strings.ToLower(strings.ReplaceAll(t.Name(), "/", "_")), randomID)

	pgConfig := protocol.SourceConfig{
		ID:                "pg1",
		Type:              "postgres",
		Host:              pgHost,
		Port:              pgPort.Int(),
		User:              "postgres",
		PassEncrypted:     "postgres",
		Database:          "cdc_src",
		SlotName:          slotName,
		PublicationName:   "cdc_pub",
		Schemas:           []string{"public"},
		DiscoveryInterval: 2 * time.Second,
	}

	data, _ := json.Marshal(pgConfig)
	kv.Put(protocol.SourceConfigKey(pgConfig.ID), data)

	// 3. Databend
	dbC, dbDSN, err := StartDatabend(ctx)
	require.NoError(t, err)
	dbDB, _ := sql.Open("databend", dbDSN)

	dbConfig := protocol.SinkConfig{
		ID:   "db1",
		Type: "databend",
		DSN:  dbDSN,
	}

	data, _ = json.Marshal(dbConfig)
	kv.Put(protocol.SinkConfigKey(dbConfig.ID), data)

	env := &Environment{
		Ctx:       ctx,
		T:         t,
		NatsURL:   natsURL,
		KV:        kv,
		Postgres:  pgDB,
		PgConfig:  pgConfig,
		Databend:  dbDB,
		DbConfig:  dbConfig,
		NatsC:     natsC,
		PostgresC: pgC,
		DatabendC: dbC,
	}

	return env
}

func (e *Environment) Cleanup() {
	log.Println("Cleaning up E2E environment...")
	if e.Mgr != nil {
		e.Mgr.Stop(context.Background())
	}
	if e.Postgres != nil {
		e.Postgres.Close()
	}
	if e.Databend != nil {
		e.Databend.Close()
	}
	if e.DebugDB != nil {
		e.DebugDB.Close()
	}

	// Wait a bit for connections to close
	time.Sleep(1 * time.Second)

	if e.DatabendC != nil {
		e.DatabendC.Terminate(e.Ctx)
	}
	if e.PostgresC != nil {
		e.PostgresC.Terminate(e.Ctx)
	}
	if e.NatsC != nil {
		e.NatsC.Terminate(e.Ctx)
	}
}

func (e *Environment) Close() {
	e.Cleanup()
}

func (e *Environment) StartWorker() {
	pub, _ := nats.NewNatsPublisher(e.NatsURL)
	pipelineFactory := &engine.PipelineFactory{
		KV:        e.KV,
		Publisher: pub,
		NatsURL:   e.NatsURL,
	}

	e.Mgr = config.NewConfigManager(e.KV, pipelineFactory.CreateWorker)
	e.Mgr.Watch(e.Ctx)
}

func (e *Environment) SeedPostgres(table string, rows int) {
	_, err := e.Postgres.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, name TEXT, age INT, metadata JSONB, created_at TIMESTAMP DEFAULT NOW())", table))
	require.NoError(e.T, err)

	for i := 1; i <= rows; i++ {
		_, err := e.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age, metadata) VALUES ($1, $2, $3)", table),
			fmt.Sprintf("user-%d", i), 20+i, `{"role": "user"}`)
		require.NoError(e.T, err)
	}
}

func (e *Environment) EventuallyCountDatabend(table string, expected int, timeout time.Duration) {
	start := time.Now()
	require.Eventually(e.T, func() bool {
		var count uint64
		err := e.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count)
		if err != nil {
			log.Printf("Databend query failed for %s (elapsed %v): %v", table, time.Since(start), err)
			return false
		}

		log.Printf("Table %s: count=%d, expected=%d (elapsed %v)", table, count, expected, time.Since(start))
		return count >= uint64(expected)
	}, timeout, 500*time.Millisecond)
}

func (e *Environment) EventuallyAssertHeartbeat(pipelineID, expectedStatus string, timeout time.Duration) {
	require.Eventually(e.T, func() bool {
		keys, err := e.KV.Keys()
		if err != nil {
			return false
		}

		var workerKey string
		prefix := protocol.WorkerHeartbeatKey(pipelineID)
		for _, key := range keys {
			if strings.HasPrefix(key, prefix) {
				workerKey = key
				break
			}
		}

		if workerKey == "" {
			log.Printf("Heartbeat key for pipeline %s not found", pipelineID)
			return false
		}

		entry, err := e.KV.Get(workerKey)
		if err != nil {
			log.Printf("Failed to get heartbeat key %s: %v", workerKey, err)
			return false
		}

		var heartbeat protocol.WorkerHeartbeat
		if err := json.Unmarshal(entry.Value(), &heartbeat); err != nil {
			log.Printf("Failed to unmarshal heartbeat: %v", err)
			return false
		}

		log.Printf("Found heartbeat for %s: Status=%s", pipelineID, heartbeat.Status)
		return heartbeat.Status == expectedStatus
	}, timeout, 1*time.Second, "timed out waiting for worker heartbeat to be '%s'", expectedStatus)
}

func (e *Environment) EventuallyAssertTableState(pipelineID, sourceID, table, expectedState string, timeout time.Duration) {
	require.Eventually(e.T, func() bool {
		key := protocol.TableStateKey(pipelineID, sourceID, table)
		entry, err := e.KV.Get(key)
		if err != nil {
			log.Printf("Failed to get table state key %s: %v", key, err)
			return false
		}
		state := string(entry.Value())
		log.Printf("Found state for %s/%s: %s", pipelineID, table, state)
		return state == expectedState
	}, timeout, 1*time.Second, "timed out waiting for table %s state to be '%s'", table, expectedState)
}

func (e *Environment) EventuallyMatchDatabend(table string, expected map[string]any, timeout time.Duration) {
	e.EventuallyMatchDatabendRow(table, "", nil, expected, timeout)
}

func (e *Environment) EventuallyMatchDatabendRow(table string, filterCol string, filterVal any, expected map[string]any, timeout time.Duration) {
	start := time.Now()
	for time.Since(start) < timeout {
		if e.checkMatch(table, filterCol, filterVal, expected) {
			return
		}
		time.Sleep(1 * time.Second)
	}
	e.T.Fatalf("Timeout waiting for match in table %s", table)
}

func (e *Environment) checkMatch(table string, filterCol string, filterVal any, expected map[string]any) bool {
	query := fmt.Sprintf("SELECT * FROM %s", table)
	var args []any
	if filterCol != "" {
		query += fmt.Sprintf(" WHERE %s = ?", filterCol)
		args = append(args, filterVal)
	}
	query += " LIMIT 1"

	rows, err := e.Databend.Query(query, args...)
	if err != nil {
		log.Printf("Databend query failed: %v", err)
		return false
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	if !rows.Next() {
		log.Printf("Databend query returned no rows! Query: %s", query)
		return false
	}

	err = rows.Scan(valuePtrs...)
	if err != nil {
		log.Printf("Databend scan failed: %v", err)
		return false
	}

	actual := make(map[string]any)
	for i, col := range columns {
		val := values[i]
		if b, ok := val.([]byte); ok {
			var m any
			if err := json.Unmarshal(b, &m); err == nil {
				val = m
			} else {
				val = string(b)
			}
		}
		actual[col] = val
	}

	for k, v := range expected {
		actualVal := actual[k]
		actualStr := fmt.Sprintf("%v", actualVal)
		expectStr := fmt.Sprintf("%v", v)

		if actualStr != expectStr {
			// Try numeric comparison for floats
			if af, ok1 := actualVal.(float64); ok1 {
				if ef, ok2 := v.(float64); ok2 {
					if math.Abs(af-ef) < 0.001 {
						continue
					}
				}
			}
			log.Printf("Key %s: mismatch! actual %q (%T), expect %q (%T)", k, actualStr, actualVal, expectStr, v)
			return false
		}
	}
	return true
}

func (e *Environment) GetDatabendRowCount(table string) (uint64, error) {
	var count uint64
	err := e.Databend.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&count)
	return count, err
}

func (e *Environment) SetPipelineConfig(id string, cfg protocol.PipelineConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = e.KV.Put(protocol.PipelineConfigKey(id), data)
	return err
}

func (e *Environment) GetDefaultPipelineConfig(id string) protocol.PipelineConfig {
	return protocol.PipelineConfig{
		ID:        id,
		Name:      "E2E Pipeline " + id,
		Sources:   []string{e.PgConfig.ID},
		Sinks:     []string{e.DbConfig.ID},
		Tables:    []string{},
		BatchSize: 10,
		BatchWait: 10 * time.Millisecond,
	}
}

func (e *Environment) Teardown(ctx context.Context) {
	e.Cleanup()
}

func (e *Environment) GetKV() go_nats.KeyValue {
	return e.KV
}
