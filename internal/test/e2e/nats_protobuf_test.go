package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	cdctransformv1 "bitbucket.org/daya-engineering/daya-contracts/v2/gen/go/cdc/transform/v1"
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
				// Round-2 finding #9: assert the *kind*, not just the
				// stringified value -- a Databend-string-only assertion
				// would still pass even if decimal_value routing broke
				// entirely and "price" fell back to string_value (the two
				// render identically once decoded back to a plain Go
				// string). This is the check that actually pins the
				// ColumnKinds side-channel end to end.
				// assert (not require): this callback runs on a NATS
				// subscription goroutine, not the test's own goroutine, and
				// testify's require.FailNow/t.FailNow is only supported from
				// the goroutine running the test itself.
				if priceVal, ok := rec.Data["price"]; ok {
					_, isDecimal := priceVal.GetKind().(*cdctransformv1.TypedValue_DecimalValue)
					assert.True(t, isDecimal, "price must arrive as TypedValue.decimal_value (ColumnKinds routing), got %T", priceVal.GetKind())
				}

				data := make(map[string]*cdctransformv1.TypedValue, len(rec.Data))
				for k, v := range rec.Data {
					if k == "name" {
						continue
					}
					data[k] = v
				}
				if nameVal, ok := rec.Data["name"]; ok {
					data["enriched_name"] = &cdctransformv1.TypedValue{
						Kind: &cdctransformv1.TypedValue_StringValue{StringValue: "ENRICHED_" + nameVal.GetStringValue()},
					}
				}
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
					// WS-1 regression guard: filter by *schema*, not table.
					// The original version of this test filtered by "tables",
					// which never exercises matchesFilter's schema branch --
					// exactly the branch that was silently broken (it read
					// m.Schema.Schema, always nil for data rows, instead of
					// m.TableSchema). A "tables" filter would still pass
					// with that bug present. "schemas" does not.
					"schemas": []interface{}{"public"},
				},
			},
		},
	}
	data, _ := json.Marshal(pipeCfg)

	// Bespoke schema (not env.SeedPostgres's fixed name/age/metadata shape)
	// so this test also exercises the encoder types WS-0's contract review
	// found broken: numeric (price -> decimal_value, previously a Go struct
	// dump because pgtype.Numeric is not a fmt.Stringer), uuid (ext_id ->
	// string_value, previously a JSON array of 16 ints because the real WAL
	// decode type is [16]byte, not uuid.UUID/[]byte), and an explicit NULL
	// (note -> null_value, must survive as a real NULL, not vanish or become
	// the string "<nil>").
	_, err = env.Postgres.Exec(`CREATE TABLE IF NOT EXISTS users_nats_pb (
		id SERIAL PRIMARY KEY,
		name TEXT,
		age INT,
		price NUMERIC(10,2),
		ext_id UUID,
		note TEXT,
		created_at TIMESTAMP DEFAULT NOW()
	)`)
	assert.NoError(t, err)

	env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	env.StartWorker()

	env.EventuallyAssertHeartbeat(pipeCfg.ID, "Running", 30*time.Second)

	_, err = env.Postgres.Exec(
		"INSERT INTO users_nats_pb (name, age, price, ext_id, note) VALUES ($1, $2, $3, $4, $5)",
		"alice", 25, "1500.50", "550e8400-e29b-41d4-a716-446655440000", nil,
	)
	assert.NoError(t, err)

	env.EventuallyCountDatabend("users_nats_pb", 1, 30*time.Second)
	env.EventuallyMatchDatabendRow("users_nats_pb", "enriched_name", "ENRICHED_alice", map[string]any{
		"age":    25,
		"price":  "1500.50",
		"ext_id": "550e8400-e29b-41d4-a716-446655440000",
		"note":   nil,
	}, 30*time.Second)
}

// TestE2E_NumericColumn_WithoutNatsProtobufProcessor is the regression guard
// for BLOCKERS 1 and 2 from the second review round: sanitizeValue's fix for
// NUMERIC-column decimal fidelity must be invisible to a pipeline that never
// runs the nats/protobuf processor at all. An earlier revision of that fix
// tagged the Data string itself with an in-band NUL-byte marker
// ("\x00cdc:decimal:..."), which every sink (and transformer/builtin.go)
// read unconditionally with no unmarking step -- so a pipeline configured
// WITHOUT nats/protobuf would have started writing the literal marker text
// into the sink, a silent regression against a configuration that works
// today, caused entirely by a feature it doesn't use. Every test added
// alongside that revision configured nats/protobuf, so none of them could
// have caught it -- this test exists specifically to cover the "processor
// not configured" path the review flagged as completely untested.
func TestE2E_NumericColumn_WithoutNatsProtobufProcessor(t *testing.T) {
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

	// No Processors at all -- a plain source -> sink pipeline, the
	// configuration every existing (non-nats/protobuf) deployment runs.
	pipeCfg := protocol.PipelineConfig{
		ID:        "p_numeric_no_transformer",
		Name:      "Numeric column without nats/protobuf E2E Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{env.DbConfig.ID},
		Tables:    []string{"numeric_no_transformer"},
		BatchSize: 1,
		BatchWait: 10 * time.Millisecond,
	}
	data, err := json.Marshal(pipeCfg)
	require.NoError(t, err)

	_, err = env.Postgres.Exec(`CREATE TABLE IF NOT EXISTS numeric_no_transformer (
		id SERIAL PRIMARY KEY,
		price NUMERIC(10,2)
	)`)
	require.NoError(t, err)

	_, err = env.KV.Put(protocol.PipelineConfigKey(pipeCfg.ID), data)
	require.NoError(t, err)
	env.StartWorker()

	env.EventuallyAssertHeartbeat(pipeCfg.ID, "Running", 30*time.Second)

	_, err = env.Postgres.Exec("INSERT INTO numeric_no_transformer (price) VALUES ($1)", "1500.50")
	require.NoError(t, err)

	env.EventuallyCountDatabend("numeric_no_transformer", 1, 30*time.Second)
	// Must be the exact, clean decimal text -- no marker, no NUL byte, no
	// "cdc:decimal:" prefix. sanitizeValue's ColumnKinds side-channel must
	// have zero effect on Data when nothing ever reads ColumnKinds.
	env.EventuallyMatchDatabendRow("numeric_no_transformer", "", nil, map[string]any{
		"price": "1500.50",
	}, 30*time.Second)
}
