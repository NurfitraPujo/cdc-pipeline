package e2e

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
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

	// 3. Insert data into the new table IMMEDIATELY (Simulate production chaos)
	log.Printf("Performing inserts into dynamic table immediately after creation...")
	for i := range 10 {
		_, err = env.Postgres.Exec("INSERT INTO table_dynamic (name) VALUES ($1)", fmt.Sprintf("dynamic-user-%d", i))
		require.NoError(t, err)
	}

	// 4. Assert sync for the new table
	// The pipeline will eventually discover the table, perform a catch-up snapshot,
	// and drain the JetStream buffer to ensure all 10 rows are synced.
	env.EventuallyCountDatabend("table_dynamic", 10, 60*time.Second)
}
