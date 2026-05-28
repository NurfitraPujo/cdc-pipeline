package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	cdctransformv1 "bitbucket.org/daya-engineering/daya-contracts/gen/go/cdc/transform/v1"
)

func TestE2E_NatsProtobufTransformer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	defer func() {
		if r := recover(); r != nil {
			t.Skipf("Skipping E2E test due to Docker/Testcontainers panic: %v", r)
		}
	}()

	env := Setup(t)
	defer env.Teardown(env.Ctx)

	nc, err := nats.Connect(env.NatsURL)
	if err != nil {
		t.Skipf("Skipping test: cannot connect to NATS URL %s: %v", env.NatsURL, err)
	}
	defer nc.Close()

	sub, err := nc.Subscribe("transform.e2e", func(msg *nats.Msg) {
		var req cdctransformv1.TransformRequest
		if err := proto.Unmarshal(msg.Data, &req); err != nil {
			return
		}

		results := make([]*cdctransformv1.TransformRecordResult, len(req.Records))
		for i, rec := range req.Records {
			var data *structpb.Struct
			if rec.Op == string(protocol.OpSchemaChange) && rec.SchemaMetadata != nil {
				cols := make(map[string]string)
				for k, v := range rec.SchemaMetadata.Columns {
					if k == "name" {
						cols["enriched_name"] = v
					} else {
						cols[k] = v
					}
				}
				results[i] = &cdctransformv1.TransformRecordResult{
					Success: true,
					Keep:    true,
					TransformedSchema: &cdctransformv1.SchemaMetadata{
						Table:     rec.SchemaMetadata.Table,
						Schema:    rec.SchemaMetadata.Schema,
						Columns:   cols,
						PkColumns: rec.SchemaMetadata.PkColumns,
					},
				}
			} else if rec.Data != nil {
				m := rec.Data.AsMap()
				if val, ok := m["name"]; ok {
					m["enriched_name"] = "ENRICHED_" + val.(string)
					delete(m, "name")
				}
				data, _ = structpb.NewStruct(m)
				results[i] = &cdctransformv1.TransformRecordResult{
					Success:         true,
					Keep:            true,
					TransformedData: data,
				}
			}
		}

		resp := cdctransformv1.TransformResponse{Results: results}
		respBytes, _ := proto.Marshal(&resp)
		_ = msg.Respond(respBytes)
	})
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	pipeCfg := protocol.PipelineConfig{
		ID:        "p_nats_pb",
		Name:      "NATS Protobuf E2E Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{"users_nats_pb"},
		BatchSize: 1,
		BatchWait: 10 * time.Millisecond,
		Processors: []protocol.ProcessorConfig{
			{
				Name: "my-proto-transformer",
				Type: "nats/protobuf",
				OperationTypes: []protocol.OperationType{
					protocol.OpInsert,
					protocol.OpSchemaChange,
				},
				Options: map[string]interface{}{
					"nats_url":   env.NatsURL,
					"subject":    "transform.e2e",
					"timeout_ms": 1000.0,
				},
			},
		},
	}
	data, _ := json.Marshal(pipeCfg)

	env.SeedPostgres("users_nats_pb", 0)

	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	env.StartWorker()

	env.EventuallyAssertHeartbeat(pipeCfg.ID, "Running", 30*time.Second)

	env.Postgres.Exec("INSERT INTO users_nats_pb (name, age) VALUES ($1, $2)", "alice", 25)

	env.EventuallyCountDatabend("users_nats_pb", 1, 30*time.Second)
	env.EventuallyMatchDatabendRow("users_nats_pb", "enriched_name", "ENRICHED_alice", map[string]any{"age": 25}, 30*time.Second)
}
