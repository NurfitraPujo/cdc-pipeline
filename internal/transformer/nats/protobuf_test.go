package nats

import (
	"context"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
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
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
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

func TestTransformBatchPreservesOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
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
		"subject":    "daya.transform.order.test",
		"timeout_ms": 5000.0,
	})
	assert.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer tf.Stop()

	nc, err := nats.Connect(url)
	assert.NoError(t, err)
	defer nc.Close()

	sub, err := nc.Subscribe("daya.transform.order.test", func(msg *nats.Msg) {
		var req cdctransformv1.TransformRequest
		err := proto.Unmarshal(msg.Data, &req)
		if err != nil {
			return
		}

		results := make([]*cdctransformv1.TransformRecordResult, len(req.Records))
		for i, rec := range req.Records {
			m := rec.Data.AsMap()
			m["transformed"] = "true"
			transformedData, _ := structpb.NewStruct(m)

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

	// Adversarial input: alternating matching/non-matching messages with deterministic IDs
	// Schema "tenant_a" matches filter, "public" does not
	msgs := []protocol.Message{
		{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "msg-0001", Data: map[string]interface{}{"id": 1}},
		{SourceID: "s1", Table: "users", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "public"}, UUID: "msg-0002", Data: map[string]interface{}{"id": 2}},
		{SourceID: "s1", Table: "orders", Op: protocol.OpUpdate, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "msg-0003", Data: map[string]interface{}{"id": 3}},
		{SourceID: "s1", Table: "products", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "public"}, UUID: "msg-0004", Data: map[string]interface{}{"id": 4}},
		{SourceID: "s1", Table: "orders", Op: protocol.OpDelete, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "msg-0005", Data: map[string]interface{}{"id": 5}},
	}

	// Expected order: msg-0001 transformed, msg-0002 passthrough, msg-0003 transformed, msg-0004 passthrough, msg-0005 transformed
	result, err := tf.TransformBatch(ctx, msgs)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(result), "all messages should be in output")

	// Verify original order preserved
	assert.Equal(t, "msg-0001", result[0].UUID)
	assert.Equal(t, "msg-0002", result[1].UUID)
	assert.Equal(t, "msg-0003", result[2].UUID)
	assert.Equal(t, "msg-0004", result[3].UUID)
	assert.Equal(t, "msg-0005", result[4].UUID)

	// Verify transformed messages were actually transformed
	assert.Equal(t, "true", result[0].Data["transformed"], "msg-0001 should be transformed")
	assert.Nil(t, result[1].Data["transformed"], "msg-0002 should be passthrough (no transformed field)")
	assert.Equal(t, "true", result[2].Data["transformed"], "msg-0003 should be transformed")
	assert.Nil(t, result[3].Data["transformed"], "msg-0004 should be passthrough (no transformed field)")
	assert.Equal(t, "true", result[4].Data["transformed"], "msg-0005 should be transformed")
}

func TestTransformBatchDroppedMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
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
		"subject":    "daya.transform.drop.test",
		"timeout_ms": 5000.0,
	})
	assert.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer tf.Stop()

	nc, err := nats.Connect(url)
	assert.NoError(t, err)
	defer nc.Close()

	// Track which records the transformer drops
	dropIndex := 1 // Drop the second matching message

	sub, err := nc.Subscribe("daya.transform.drop.test", func(msg *nats.Msg) {
		var req cdctransformv1.TransformRequest
		err := proto.Unmarshal(msg.Data, &req)
		if err != nil {
			return
		}

		results := make([]*cdctransformv1.TransformRecordResult, len(req.Records))
		for i := range req.Records {
			keep := true
			if i == dropIndex {
				keep = false
			}
			results[i] = &cdctransformv1.TransformRecordResult{
				Success: true,
				Keep:    keep,
			}
		}

		resp := cdctransformv1.TransformResponse{Results: results}
		respBytes, _ := proto.Marshal(&resp)
		_ = msg.Respond(respBytes)
	})
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	// 3 matching messages, 1 non-matching
	// dropIndex=1 means second matching message (index 1 in matching array) is dropped
	msgs := []protocol.Message{
		{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "msg-0001"},
		{SourceID: "s1", Table: "users", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "public"}, UUID: "msg-0002"},
		{SourceID: "s1", Table: "orders", Op: protocol.OpUpdate, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "msg-0003"},
		{SourceID: "s1", Table: "orders", Op: protocol.OpDelete, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "msg-0004"},
	}

	// Expected: msg-0001 (match), msg-0002 (passthrough), msg-0003 (match), msg-0004 (match but dropped)
	result, err := tf.TransformBatch(ctx, msgs)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result), "one message should be dropped")

	// Verify order
	assert.Equal(t, "msg-0001", result[0].UUID)
	assert.Equal(t, "msg-0002", result[1].UUID)
	assert.Equal(t, "msg-0003", result[2].UUID)
}

func TestTransformBatch_PropertyBased(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
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

	nc, err := nats.Connect(url)
	assert.NoError(t, err)
	defer nc.Close()

	type testCase struct {
		name        string
		schemas     []interface{}
		msgs        []protocol.Message
		expectedLen int
		dropIndices []int
		description string
	}

	cases := []testCase{
		{
			name:    "all_matching_all_kept",
			schemas: []interface{}{"tenant_a"},
			msgs: []protocol.Message{
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "1"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "2"},
			},
			expectedLen: 2,
			dropIndices: nil,
			description: "all messages match filter and all are kept",
		},
		{
			name:    "all_matching_some_dropped",
			schemas: []interface{}{"tenant_a"},
			msgs: []protocol.Message{
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "1"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "2"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "3"},
			},
			expectedLen: 2,
			dropIndices: []int{1},
			description: "all match but one is dropped",
		},
		{
			name:    "mixed_matching_and_passthrough",
			schemas: []interface{}{"tenant_a"},
			msgs: []protocol.Message{
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "public"}, UUID: "1"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "2"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "public"}, UUID: "3"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "4"},
			},
			expectedLen: 4,
			dropIndices: nil,
			description: "interleaved matching and passthrough, all kept",
		},
		{
			name:    "none_match_returns_all",
			schemas: []interface{}{"tenant_b"},
			msgs: []protocol.Message{
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "1"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "2"},
			},
			expectedLen: 2,
			dropIndices: nil,
			description: "no messages match, all pass through",
		},
		{
			name:    "mixed_with_drops",
			schemas: []interface{}{"tenant_a"},
			msgs: []protocol.Message{
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "public"}, UUID: "1"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "2"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "tenant_a"}, UUID: "3"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, Schema: &protocol.SchemaMetadata{Schema: "public"}, UUID: "4"},
			},
			expectedLen: 3,
			dropIndices: []int{0},
			description: "mixed with some drops",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dropMap := make(map[int]bool)
			for _, d := range tc.dropIndices {
				dropMap[d] = true
			}

			schemasIF := tc.schemas

			tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
				"nats_url":   url,
				"subject":    "daya.transform.property.test",
				"timeout_ms": 5000.0,
				"schemas":    schemasIF,
			})
			assert.NoError(t, err)
			tf := tfRaw.(*NatsProtoTransformer)
			defer tf.Stop()

			sub, err := nc.Subscribe("daya.transform.property.test", func(msg *nats.Msg) {
				var req cdctransformv1.TransformRequest
				err := proto.Unmarshal(msg.Data, &req)
				if err != nil {
					return
				}

				results := make([]*cdctransformv1.TransformRecordResult, len(req.Records))
				for i := range req.Records {
					keep := true
					if dropMap[i] {
						keep = false
					}
					results[i] = &cdctransformv1.TransformRecordResult{
						Success: true,
						Keep:    keep,
					}
				}

				resp := cdctransformv1.TransformResponse{Results: results}
				respBytes, _ := proto.Marshal(&resp)
				_ = msg.Respond(respBytes)
			})
			assert.NoError(t, err)
			defer sub.Unsubscribe()

			result, err := tf.TransformBatch(context.Background(), tc.msgs)
			assert.NoError(t, err, tc.description)
			assert.Equal(t, tc.expectedLen, len(result), tc.description)

			// Verify order is preserved
			outIdx := 0
			for _, m := range tc.msgs {
				matched := false
				for _, s := range tc.schemas {
					if m.Schema != nil && m.Schema.Schema == s.(string) {
						matched = true
						break
					}
				}

				if matched {
					matchingIdx := 0
					for _, prevMsg := range tc.msgs {
						if prevMsg.UUID == m.UUID {
							break
						}
						for _, s := range tc.schemas {
							if prevMsg.Schema != nil && prevMsg.Schema.Schema == s.(string) {
								matchingIdx++
								break
							}
						}
					}

					if dropMap[matchingIdx] {
						continue
					}
				}

				assert.Equal(t, m.UUID, result[outIdx].UUID, tc.description)
				outIdx++
			}
		})
	}
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

func TestCloseableTransformer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
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
		"subject":    "daya.transform.close.test",
		"timeout_ms": 1000.0,
	})
	assert.NoError(t, err)

	tf := tfRaw.(*NatsProtoTransformer)
	assert.NotNil(t, tf.conn, "NATS connection should be established")
	assert.False(t, tf.conn.IsClosed(), "NATS connection should not be closed initially")

	// Type-assert to CloseableTransformer interface (defined in transformer package)
	closeable, ok := tfRaw.(transformer.CloseableTransformer)
	assert.True(t, ok, "NatsProtoTransformer should implement CloseableTransformer")

	// Call Close and verify connection is released
	err = closeable.Close()
	assert.NoError(t, err, "Close() should not return an error")
	assert.True(t, tf.conn.IsClosed(), "NATS connection should be closed after Close()")

	// Calling Close again should be idempotent
	err = closeable.Close()
	assert.NoError(t, err, "Close() should be idempotent")
}
