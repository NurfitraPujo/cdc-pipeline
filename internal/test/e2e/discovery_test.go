package e2e

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	go_nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestE2E_DynamicDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// 1. Start Pipeline with ONE table
	pipeCfg := protocol.PipelineConfig{
		ID:        "p_discovery",
		Name:      "Discovery Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{"table_initial"},
		BatchSize: 1,
		BatchWait: 100 * time.Millisecond,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)

	env.SeedPostgres("table_initial", 1)
	env.StartWorker()

	env.EventuallyCountDatabend("table_initial", 1, 30*time.Second)

	// 2. Create NEW table in Postgres
	_, err := env.Postgres.Exec("CREATE TABLE table_dynamic (id SERIAL PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// 3. Wait for discovery reload to COMPLETE
	log.Printf("Waiting for discovery reload to start...")
	require.Eventually(t, func() bool {
		entry, err := env.KV.Get(protocol.PipelineConfigKey(pipeCfg.ID))
		if err != nil { return false }
		var cfg protocol.PipelineConfig
		json.Unmarshal(entry.Value(), &cfg)
		for _, tbl := range cfg.Tables {
			if tbl == "table_dynamic" { return true }
		}
		return false
	}, 60*time.Second, 1*time.Second, "Table should be discovered in config")

	log.Printf("Waiting for transition to complete...")
	require.Eventually(t, func() bool {
		_, err := env.KV.Get(protocol.TransitionStateKey(pipeCfg.ID))
		return err == go_nats.ErrKeyNotFound
	}, 60*time.Second, 1*time.Second, "Transition should finish")

	// Extra buffer for worker startup
	log.Printf("Reload complete. Waiting for new worker to settle...")
	time.Sleep(15 * time.Second)

	// 4. Now perform inserts into the dynamic table
	log.Printf("Performing inserts into dynamic table...")
	for i := 0; i < 10; i++ {
		_, err = env.Postgres.Exec("INSERT INTO table_dynamic (name) VALUES ($1)", "dynamic-user")
		require.NoError(t, err)
	}

	// 5. Assert sync
	env.EventuallyCountDatabend("table_dynamic", 10, 60*time.Second)
}
