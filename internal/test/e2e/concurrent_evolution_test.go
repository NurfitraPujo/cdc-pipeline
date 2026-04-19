package e2e

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

func TestE2E_ConcurrentSchemaEvolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// 1. Prepare multiple tables
	tables := []string{"table_a", "table_b", "table_c"}
	for _, table := range tables {
		env.SeedPostgres(table, 5)
	}

	// 2. Start Pipeline
	pipeCfg := protocol.PipelineConfig{
		ID:        "p_concurrent_evo",
		Name:      "Concurrent Evolution Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    tables,
		BatchSize: 1,
		BatchWait: 100 * time.Millisecond,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)

	env.StartWorker()

	// Verify initial sync for all
	for _, table := range tables {
		env.EventuallyCountDatabend(table, 5, 30*time.Second)
	}

	// 3. CONCURRENT DDL
	var wg sync.WaitGroup
	for i, table := range tables {
		wg.Add(1)
		go func(tbl string, idx int) {
			defer wg.Done()
			colName := fmt.Sprintf("extra_col_%d", idx)
			_, err := env.Postgres.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s TEXT", tbl, colName))
			if err != nil {
				t.Errorf("failed to alter table %s: %v", tbl, err)
				return
			}

			// Wait a bit to ensure concurrent processing in worker
			time.Sleep(500 * time.Millisecond)

			// Insert with new column
			val := fmt.Sprintf("val-%s", tbl)
			_, err = env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age, %s) VALUES ($1, $2, $3)", tbl, colName), "user-evo", 30, val)
			if err != nil {
				t.Errorf("failed to insert into %s: %v", tbl, err)
			}
		}(table, i)
	}
	wg.Wait()

	// 4. Verify everything arrived
	for i, table := range tables {
		colName := fmt.Sprintf("extra_col_%d", i)
		val := fmt.Sprintf("val-%s", table)
		
		env.EventuallyCountDatabend(table, 6, 60*time.Second)
		env.EventuallyMatchDatabendRow(table, "name", "user-evo", map[string]any{colName: val}, 30*time.Second)
	}
}
