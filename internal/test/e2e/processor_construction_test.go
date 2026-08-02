package e2e

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/require"
)

// WS-8 item 2: a processor referencing an unregistered transformer type, or
// whose factory errors, used to only log and continue -- the pipeline then
// ran completely untransformed while still reporting "Running" to the API
// (getPipelineStatusString, internal/api/handler.go). This test drives the
// real supervisor loop (config.ConfigManager + engine.PipelineFactory) end
// to end and asserts that a pipeline configured with a broken processor
// never reaches the "Running" heartbeat status.
func TestE2E_PipelineWithUnregisteredProcessor_NeverReportsRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)
	defer env.Close()

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_broken_processor",
		Name:      "Broken Processor E2E Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{"broken_processor_e2e"},
		BatchSize: 1,
		BatchWait: 10 * time.Millisecond,
		Processors: []protocol.ProcessorConfig{
			{
				Name:           "does-not-exist",
				Type:           "this-transformer-type-is-not-registered",
				OperationTypes: []protocol.OperationType{protocol.OpInsert},
			},
		},
	}
	data, _ := json.Marshal(pipeCfg)

	env.SeedPostgres("broken_processor_e2e", 0)
	_, err := env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	require.NoError(t, err)
	env.StartWorker()

	// Give the supervisor several backoff/retry cycles to (mis)behave, then
	// assert it never once reported "Running": CreateWorker must return an
	// error on the unregistered processor, so startNewWorker(config/manager.go)
	// never registers a worker and monitorWorker stays on the nil-worker path
	// (heartbeat status "Retrying"), not the worker path (status "Running").
	deadline := time.Now().Add(15 * time.Second)
	sawRunning := false
	sawRetrying := false
	for time.Now().Before(deadline) {
		status, ok := heartbeatStatus(t, env, pipeCfg.ID)
		if ok {
			if status == "Running" {
				sawRunning = true
			}
			if status == "Retrying" {
				sawRetrying = true
			}
		}
		if sawRunning {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	require.False(t, sawRunning, "pipeline with an unregistered processor type must never report heartbeat status \"Running\"")
	require.True(t, sawRetrying, "pipeline with an unregistered processor type should be visibly retrying, not silently stuck")
}

// TestE2E_PipelineWithProcessorConstructorError_NeverReportsRunning covers the
// other half of the factory.go:232-236 fatal path: a *registered* transformer
// type whose constructor itself returns an error, as opposed to an
// unregistered type (covered above). This is exercised specifically because
// WS-8 item 4 (internal/transformer/nats/protobuf.go) made
// NewNatsProtoTransformer reject a config with neither 'schemas' nor 'tables'
// set, which makes this failure mode reachable in production for any
// already-deployed nats/protobuf processor missing that option -- previously
// this construction error path had no test coverage at all.
func TestE2E_PipelineWithProcessorConstructorError_NeverReportsRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := Setup(t)
	defer env.Close()

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_broken_processor_ctor",
		Name:      "Broken Processor Constructor E2E Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{"broken_processor_ctor_e2e"},
		BatchSize: 1,
		BatchWait: 10 * time.Millisecond,
		Processors: []protocol.ProcessorConfig{
			{
				Name: "nats-protobuf-missing-filter",
				Type: "nats/protobuf",
				// Deliberately omit 'schemas' and 'tables': NewNatsProtoTransformer
				// requires at least one to be set (WS-8 item 4) and otherwise
				// returns an error from tf(opts) in factory.go, not from the
				// transformer.GetTransformer lookup.
				Options: map[string]interface{}{
					"nats_url": "nats://127.0.0.1:4222",
					"subject":  "cdc.transform.broken",
				},
				OperationTypes: []protocol.OperationType{protocol.OpInsert},
			},
		},
	}
	data, _ := json.Marshal(pipeCfg)

	env.SeedPostgres("broken_processor_ctor_e2e", 0)
	_, err := env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	require.NoError(t, err)
	env.StartWorker()

	deadline := time.Now().Add(15 * time.Second)
	sawRunning := false
	sawRetrying := false
	for time.Now().Before(deadline) {
		status, ok := heartbeatStatus(t, env, pipeCfg.ID)
		if ok {
			if status == "Running" {
				sawRunning = true
			}
			if status == "Retrying" {
				sawRetrying = true
			}
		}
		if sawRunning {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	require.False(t, sawRunning, "pipeline whose processor constructor errors must never report heartbeat status \"Running\"")
	require.True(t, sawRetrying, "pipeline whose processor constructor errors should be visibly retrying, not silently stuck")
}

// heartbeatStatus reads the current worker heartbeat status for a pipeline
// directly out of KV, without asserting on it (unlike
// Environment.EventuallyAssertHeartbeat, which polls until a specific status
// is seen or times out). Returns ok=false if no heartbeat key exists yet.
func heartbeatStatus(t *testing.T, env *Environment, pipelineID string) (string, bool) {
	t.Helper()
	keys, err := env.KV.Keys()
	if err != nil {
		return "", false
	}

	prefix := protocol.WorkerHeartbeatKey(pipelineID)
	var workerKey string
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			workerKey = key
			break
		}
	}
	if workerKey == "" {
		return "", false
	}

	entry, err := env.KV.Get(workerKey)
	if err != nil {
		return "", false
	}

	var hb protocol.WorkerHeartbeat
	if err := json.Unmarshal(entry.Value(), &hb); err != nil {
		return "", false
	}
	return hb.Status, true
}
