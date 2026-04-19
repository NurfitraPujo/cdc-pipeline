package e2e

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	go_nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestE2E_Backpressure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)

	// 1. Start Pipeline
	pipelineID := "p_pressure"
	pipeCfg := protocol.PipelineConfig{
		ID:        pipelineID,
		Name:      "Pressure Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{"pressure_table"},
		BatchSize: 10,
		BatchWait: 100 * time.Millisecond,
	}
	data, _ := json.Marshal(pipeCfg)
	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)

	// Seed some initial data to ensure table exists and initial sync completes
	env.SeedPostgres("pressure_table", 1)
	env.StartWorker()

	// Wait for initial sync
	env.EventuallyCountDatabend("pressure_table", 1, 30*time.Second)

	// 2. Configure Stream with LIMITS
	nc, _ := go_nats.Connect(env.NatsURL)
	js, _ := nc.JetStream()
	
	topic := fmt.Sprintf("cdc_pipeline_%s_ingest", pipelineID)
	streamName := topic 

	// Wait for stream to be created by worker
	require.Eventually(t, func() bool {
		_, err := js.StreamInfo(streamName)
		return err == nil
	}, 20*time.Second, 1*time.Second)

	// Update stream with strict limits
	si, err := js.StreamInfo(streamName)
	require.NoError(t, err)
	cfg := si.Config
	cfg.MaxMsgs = 10 
	cfg.Discard = go_nats.DiscardNew
	_, err = js.UpdateStream(&cfg)
	require.NoError(t, err)

	// 3. Corrupt Sink Config to block consumer and TRIGGER RELOAD
	t.Log("Blocking consumer via bad sink DSN...")
	badDbCfg := env.DbConfig
	badDbCfg.DSN = "http://invalid-host-so-it-fails:1234"
	badData, _ := json.Marshal(badDbCfg)
	_, err = env.KV.Put(protocol.SinkConfigKey(env.DbConfig.ID), badData)
	require.NoError(t, err)
	
	// Trigger reload
	pipeData, _ := json.Marshal(pipeCfg)
	_, err = env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), pipeData)
	require.NoError(t, err)

	// Wait for reload to complete and consumer to start failing
	time.Sleep(5 * time.Second)

	// 4. Produce enough data to hit the limit
	t.Log("Producing 100 rows...")
	_, err = env.Postgres.Exec("INSERT INTO pressure_table (name, age) SELECT 'press-' || i, 20 FROM generate_series(2, 101) s(i)")
	require.NoError(t, err)

	// 5. Wait for stream to fill up
	t.Log("Waiting for NATS stream to hit limit...")
	require.Eventually(t, func() bool {
		si, err := js.StreamInfo(streamName)
		if err != nil {
			return false
		}
		t.Logf("Stream msgs: %d", si.State.Msgs)
		return si.State.Msgs >= 10
	}, 30*time.Second, 1*time.Second)

	// 6. Verify Postgres Slot exists and check LSN (it should be stalled)
	realSlotName := fmt.Sprintf("%s_%s", env.PgConfig.SlotName, pipelineID)
	var lsn1 string
	err = env.Postgres.QueryRow("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", realSlotName).Scan(&lsn1)
	require.NoError(t, err)
	t.Logf("LSN during backpressure: %s", lsn1)

	// 7. RESTORE Sink Config and Stream Limit
	t.Log("Restoring sink config and NATS limits...")
	goodSinkData, _ := json.Marshal(env.DbConfig)
	_, err = env.KV.Put(protocol.SinkConfigKey(env.DbConfig.ID), goodSinkData)
	require.NoError(t, err)
	
	si, err = js.StreamInfo(streamName)
	require.NoError(t, err)
	si.Config.MaxMsgs = 1000
	_, err = js.UpdateStream(&si.Config)
	require.NoError(t, err)

	// Trigger reload to pick up good sink config
	_, err = env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), pipeData)
	require.NoError(t, err)

	// 8. Verify all 101 rows eventually arrive
	env.EventuallyCountDatabend("pressure_table", 101, 90*time.Second)
}
