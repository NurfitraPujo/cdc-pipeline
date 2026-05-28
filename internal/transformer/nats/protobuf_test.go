package nats

import (
	"context"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	cdctransformv1 "bitbucket.org/daya-engineering/daya-contracts/gen/go/cdc/transform/v1"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
)

func TestNatsProtoTransformer_RouterFiltering(t *testing.T) {
	tf := &NatsProtoTransformer{
		schemas: []string{"tenant_a"},
		tables:  []string{"orders"},
		conn:    nil,
	}

	msgs := []protocol.Message{
		{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}},
		{SourceID: "s1", Table: "users", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "public"}},
	}

	matching, passthrough := tf.filterMessages(msgs)
	assert.Equal(t, 1, len(matching))
	assert.Equal(t, "orders", matching[0].Table)

	assert.Equal(t, 1, len(passthrough))
	assert.Equal(t, "users", passthrough[0].Table)

	tfNoMatch := &NatsProtoTransformer{
		schemas: []string{"tenant_b"},
		conn:    nil,
	}
	result, err := tfNoMatch.TransformBatch(context.Background(), msgs)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result), "all messages bypassed since none match schema tenant_b")
}

func TestNatsProtoTransformer_ProtobufMapping(t *testing.T) {
	ctx := context.Background()

	defer func() {
		if r := recover(); r != nil {
			t.Skipf("Skipping integration test due to Docker/Testcontainers panic: %v", r)
		}
	}()

	natsC, err := tc_nats.Run(ctx, "nats:2.10-alpine")
	if err != nil {
		t.Skipf("Docker/Testcontainers not available: %v", err)
	}
	defer natsC.Terminate(ctx)

	url, _ := natsC.ConnectionString(ctx)

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    "daya.transform.test",
		"timeout_ms": 1000.0,
	})
	assert.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer tf.Stop()

	nc, err := nats.Connect(url)
	assert.NoError(t, err)
	defer nc.Close()

	sub, err := nc.Subscribe("daya.transform.test", func(msg *nats.Msg) {
		var req cdctransformv1.TransformRequest
		err := proto.Unmarshal(msg.Data, &req)
		if err != nil {
			return
		}

		results := make([]*cdctransformv1.TransformRecordResult, len(req.Records))
		for i, rec := range req.Records {
			var transformedData *structpb.Struct
			if rec.Data != nil {
				m := rec.Data.AsMap()
				m["enriched"] = "true"
				transformedData, _ = structpb.NewStruct(m)
			}

			results[i] = &cdctransformv1.TransformRecordResult{
				Success:         true,
				Keep:            true,
				TransformedData: transformedData,
			}
		}

		resp := cdctransformv1.TransformResponse{Results: results}
		respBytes, _ := proto.Marshal(&resp)
		_ = msg.Respond(respBytes)
	})
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	msg := protocol.Message{
		SourceID: "s1",
		Table:    "users",
		Op:       protocol.OpInsert,
		Data:     map[string]interface{}{"name": "alice"},
	}

	res, err := tf.TransformBatch(ctx, []protocol.Message{msg})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, "true", res[0].Data["enriched"])
	assert.Equal(t, "alice", res[0].Data["name"])
}

func TestNatsProtoTransformer_AllColumnTypesSanitization(t *testing.T) {
	tf := &NatsProtoTransformer{
		conn: nil,
	}

	type customStruct struct {
		name string
	}

	msg := protocol.Message{
		Data: map[string]interface{}{
			"c_uuid":      []byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00},
			"c_bytea":     []byte{0xde, 0xad, 0xbe, 0xef},
			"c_int_array": []int64{1, 2, 3},
			"c_str_array": []string{"a", "b"},
			"c_struct":    customStruct{name: "test"},
			"c_time":      time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}

	req, err := tf.buildTransformRequest([]protocol.Message{msg})
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Equal(t, 1, len(req.Records))

	rec := req.Records[0]
	assert.NotNil(t, rec.Data)

	fields := rec.Data.Fields
	// c_uuid should be formatted as a standard UUID string
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", fields["c_uuid"].GetStringValue())

	// c_bytea should be base64 string
	assert.Equal(t, "3q2+7w==", fields["c_bytea"].GetStringValue())

	// c_int_array should be a list value containing Number values
	list := fields["c_int_array"].GetListValue()
	assert.NotNil(t, list)
	assert.Equal(t, 3, len(list.Values))
	assert.Equal(t, 1.0, list.Values[0].GetNumberValue())

	// c_time should be RFC3339 formatted string
	assert.Equal(t, "2023-01-01T12:00:00Z", fields["c_time"].GetStringValue())

	// c_struct should fallback to string representation
	assert.Contains(t, fields["c_struct"].GetStringValue(), "test")
}
