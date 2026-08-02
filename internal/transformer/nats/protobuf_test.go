package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/transformer"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	cdctransformv1 "bitbucket.org/daya-engineering/daya-contracts/v2/gen/go/cdc/transform/v1"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
)

func TestNatsProtoTransformer_RouterFiltering(t *testing.T) {
	tf := &NatsProtoTransformer{
		schemas: []string{"tenant_a"},
		tables:  []string{"orders"},
		conn:    nil,
	}

	// WS-1: the schema filter reads m.TableSchema (the sibling field), not
	// m.Schema -- m.Schema is *protocol.SchemaMetadata, populated only for
	// OpSchemaChange DDL events, and is nil for ordinary data rows.
	msgs := []protocol.Message{
		{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, TableSchema: "tenant_a"},
		{SourceID: "s1", Table: "users", Op: protocol.OpInsert, TableSchema: "public"},
	}

	assert.True(t, tf.matchesFilter(msgs[0]), "orders/tenant_a should match schema+table filter")
	assert.False(t, tf.matchesFilter(msgs[1]), "users/public should not match schema+table filter")

	tfNoMatch := &NatsProtoTransformer{
		schemas: []string{"tenant_b"},
		conn:    nil,
	}
	result, err := tfNoMatch.TransformBatch(context.Background(), msgs)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result), "all messages bypassed since none match schema tenant_b")
}

// TestMatchesFilter_SchemasAndTablesOR pins the WS-1B fix: when both
// `schemas` and `tables` are configured on the same processor, a message
// must match if it satisfies *either* -- not both. This is the shape WS-1B
// requires: `schemas: ["custom_objects"]` picks up every custom_objects
// table, and `tables: ["visitations"]` additionally admits the public-schema
// `visitations` table (transformed for its checked_in/checked_out
// enrichment) even though it isn't in the custom_objects schema. Before this
// fix, the two filters ANDed, so that combination matched nothing: no row is
// simultaneously in schema "custom_objects" and named "visitations".
func TestMatchesFilter_SchemasAndTablesOR(t *testing.T) {
	tf := &NatsProtoTransformer{
		schemas: []string{"custom_objects"},
		tables:  []string{"visitations"},
	}

	customObjectsRow := protocol.Message{Table: "_1_2_master_contacts", TableSchema: "custom_objects", Op: protocol.OpInsert}
	visitationsRow := protocol.Message{Table: "visitations", TableSchema: "public", Op: protocol.OpUpdate}
	unrelatedRow := protocol.Message{Table: "business_entities", TableSchema: "public", Op: protocol.OpInsert}

	assert.True(t, tf.matchesFilter(customObjectsRow), "a custom_objects-schema row must match via the schemas filter alone")
	assert.True(t, tf.matchesFilter(visitationsRow), "public.visitations must match via the tables filter even though its schema isn't in the schemas list")
	assert.False(t, tf.matchesFilter(unrelatedRow), "a public built-in that is neither in the schemas list nor the tables list must not match")

	// Single-filter configurations are unaffected: only one of the two
	// gates the result, matching pre-WS-1B behaviour exactly.
	schemaOnly := &NatsProtoTransformer{schemas: []string{"custom_objects"}}
	assert.False(t, schemaOnly.matchesFilter(visitationsRow), "with no tables filter configured, a public-schema row must not match on schema alone")

	tablesOnly := &NatsProtoTransformer{tables: []string{"visitations"}}
	assert.False(t, tablesOnly.matchesFilter(customObjectsRow), "with no schemas filter configured, a non-visitations table must not match on tables alone")
}

// TestTransform_FailsClosedOnError pins the WS-10 fix: Transform (the
// single-message fallback path used by the engine only for transformers
// that don't implement BatchTransformer) must fail closed on error --
// return (nil, false, err) -- instead of the old (m, true, err), which told
// callers to "keep" the original untransformed message on a hard failure.
// That contradicted TransformBatch/doTransform, which never passes matching
// rows through unchanged on error. Drives a real NATS round trip with no
// responder listening on the subject, so the request genuinely times out
// (not a mocked error), matching this suite's other real-transport tests.
func TestTransform_FailsClosedOnError(t *testing.T) {
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
	defer func() { _ = natsC.Terminate(ctx) }()

	url, _ := natsC.ConnectionString(ctx)

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    "daya.transform.no-responder.test",
		"timeout_ms": 200.0, // no responder is listening, so this must time out fast
		"tables":     []interface{}{"orders"},
	})
	require.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer func() { _ = tf.Close() }()

	msg := &protocol.Message{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, TableSchema: "public", UUID: "u1"}

	result, keep, err := tf.Transform(ctx, msg)
	require.Error(t, err, "no responder on the subject must surface as an error")
	assert.Nil(t, result, "on error, Transform must not return a message to keep (fail closed)")
	assert.False(t, keep, "on error, keep must be false -- WS-10: previously this was hardcoded true")
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
	defer func() { _ = natsC.Terminate(ctx) }()

	url, _ := natsC.ConnectionString(ctx)

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    "daya.transform.test",
		"timeout_ms": 1000.0,
		"tables":     []interface{}{"users"},
	})
	assert.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer func() { _ = tf.Stop() }()

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
			var transformedData map[string]*cdctransformv1.TypedValue
			if rec.Data != nil {
				transformedData = make(map[string]*cdctransformv1.TypedValue, len(rec.Data)+1)
				for k, v := range rec.Data {
					transformedData[k] = v
				}
				transformedData["enriched"] = &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: "true"}}
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
	assert.NoError(t, nc.Flush())
	defer func() { _ = sub.Unsubscribe() }()

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
	defer func() { _ = natsC.Terminate(ctx) }()

	url, _ := natsC.ConnectionString(ctx)

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    "daya.transform.order.test",
		"timeout_ms": 5000.0,
		"schemas":    []interface{}{"tenant_a"},
	})
	assert.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer func() { _ = tf.Stop() }()

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
			transformedData := make(map[string]*cdctransformv1.TypedValue, len(rec.Data)+1)
			for k, v := range rec.Data {
				transformedData[k] = v
			}
			transformedData["transformed"] = &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: "true"}}

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
	assert.NoError(t, nc.Flush())
	defer func() { _ = sub.Unsubscribe() }()

	// Adversarial input: alternating matching/non-matching messages with deterministic IDs
	// Schema "tenant_a" matches filter, "public" does not
	msgs := []protocol.Message{
		{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "msg-0001", Data: map[string]interface{}{"id": 1}},
		{SourceID: "s1", Table: "users", Op: protocol.OpInsert, TableSchema: "public", UUID: "msg-0002", Data: map[string]interface{}{"id": 2}},
		{SourceID: "s1", Table: "orders", Op: protocol.OpUpdate, TableSchema: "tenant_a", UUID: "msg-0003", Data: map[string]interface{}{"id": 3}},
		{SourceID: "s1", Table: "products", Op: protocol.OpInsert, TableSchema: "public", UUID: "msg-0004", Data: map[string]interface{}{"id": 4}},
		{SourceID: "s1", Table: "orders", Op: protocol.OpDelete, TableSchema: "tenant_a", UUID: "msg-0005", Data: map[string]interface{}{"id": 5}},
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
	defer func() { _ = natsC.Terminate(ctx) }()

	url, _ := natsC.ConnectionString(ctx)

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    "daya.transform.drop.test",
		"timeout_ms": 5000.0,
		"schemas":    []interface{}{"tenant_a"},
	})
	assert.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer func() { _ = tf.Stop() }()

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
	assert.NoError(t, nc.Flush())
	defer func() { _ = sub.Unsubscribe() }()

	// 3 matching messages, 1 non-matching
	// dropIndex=1 means second matching message (index 1 in matching array) is dropped
	msgs := []protocol.Message{
		{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "msg-0001"},
		{SourceID: "s1", Table: "users", Op: protocol.OpInsert, TableSchema: "public", UUID: "msg-0002"},
		{SourceID: "s1", Table: "orders", Op: protocol.OpUpdate, TableSchema: "tenant_a", UUID: "msg-0003"},
		{SourceID: "s1", Table: "orders", Op: protocol.OpDelete, TableSchema: "tenant_a", UUID: "msg-0004"},
	}

	// Matching messages, in filter order: msg-0001, msg-0003, msg-0004.
	// dropIndex=1 drops the second *matching* message, i.e. msg-0003 -- not
	// msg-0004. (The original assertion here expected msg-0003 to survive
	// and msg-0004 to be dropped -- backwards from what dropIndex=1 actually
	// selects. It "passed" only because the pre-WS-1 schema filter never
	// matched anything real, so this path was never exercised honestly; the
	// corrected assertion below, expecting msg-0004 to survive, is the one
	// that is arithmetically right.)
	// Expected: msg-0001 (match, kept), msg-0002 (passthrough), msg-0003 (match, dropped), msg-0004 (match, kept)
	result, err := tf.TransformBatch(ctx, msgs)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result), "one message should be dropped")

	// Verify order
	assert.Equal(t, "msg-0001", result[0].UUID)
	assert.Equal(t, "msg-0002", result[1].UUID)
	assert.Equal(t, "msg-0004", result[2].UUID)
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
	defer func() { _ = natsC.Terminate(ctx) }()

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
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "1"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "2"},
			},
			expectedLen: 2,
			dropIndices: nil,
			description: "all messages match filter and all are kept",
		},
		{
			name:    "all_matching_some_dropped",
			schemas: []interface{}{"tenant_a"},
			msgs: []protocol.Message{
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "1"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "2"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "3"},
			},
			expectedLen: 2,
			dropIndices: []int{1},
			description: "all match but one is dropped",
		},
		{
			name:    "mixed_matching_and_passthrough",
			schemas: []interface{}{"tenant_a"},
			msgs: []protocol.Message{
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "public", UUID: "1"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "2"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "public", UUID: "3"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "4"},
			},
			expectedLen: 4,
			dropIndices: nil,
			description: "interleaved matching and passthrough, all kept",
		},
		{
			name:    "none_match_returns_all",
			schemas: []interface{}{"tenant_b"},
			msgs: []protocol.Message{
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "1"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "2"},
			},
			expectedLen: 2,
			dropIndices: nil,
			description: "no messages match, all pass through",
		},
		{
			name:    "mixed_with_drops",
			schemas: []interface{}{"tenant_a"},
			msgs: []protocol.Message{
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "public", UUID: "1"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "2"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "tenant_a", UUID: "3"},
				{SourceID: "s1", Table: "t1", Op: protocol.OpInsert, TableSchema: "public", UUID: "4"},
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
			defer func() { _ = tf.Stop() }()

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
			assert.NoError(t, nc.Flush())
			defer func() { _ = sub.Unsubscribe() }()

			result, err := tf.TransformBatch(context.Background(), tc.msgs)
			assert.NoError(t, err, tc.description)
			assert.Equal(t, tc.expectedLen, len(result), tc.description)

			// Verify order is preserved
			outIdx := 0
			for _, m := range tc.msgs {
				matched := false
				for _, s := range tc.schemas {
					if m.TableSchema == s.(string) {
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
							if prevMsg.TableSchema == s.(string) {
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

	fields := rec.Data
	// c_uuid should be formatted as a standard UUID string
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", fields["c_uuid"].GetStringValue())

	// c_bytea should be base64 string
	assert.Equal(t, "3q2+7w==", fields["c_bytea"].GetStringValue())

	// c_int_array should round-trip as a JSON array (WS-0: complex/array
	// values travel as json_value, not a structpb ListValue).
	var intArray []int64
	assert.NoError(t, json.Unmarshal([]byte(fields["c_int_array"].GetJsonValue()), &intArray))
	assert.Equal(t, []int64{1, 2, 3}, intArray)

	var strArray []string
	assert.NoError(t, json.Unmarshal([]byte(fields["c_str_array"].GetJsonValue()), &strArray))
	assert.Equal(t, []string{"a", "b"}, strArray)

	// c_time should be a real timestamp_value, not a formatted string --
	// TypedValue carries timestamps typed (google.protobuf.Timestamp), which
	// is the whole point of retiring structpb.
	assert.NotNil(t, fields["c_time"].GetTimestampValue())
	assert.Equal(t, time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC), fields["c_time"].GetTimestampValue().AsTime())

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
	defer func() { _ = natsC.Terminate(ctx) }()

	url, _ := natsC.ConnectionString(ctx)

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    "daya.transform.close.test",
		"timeout_ms": 1000.0,
		"tables":     []interface{}{"any"},
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

// ----------------------------------------------------------------------------
// WS-3: payload-size guard and batch chunking
// ----------------------------------------------------------------------------

// TestTransformBatch_ChunksOversizedBatch drives the real transport path (a
// real NATS connection via testcontainers, real proto.Marshal/Unmarshal on
// both sides) with a batch whose encoded size exceeds an artificially small
// maxPayload -- set directly on the transformer rather than negotiated from
// the server, since the real NATS default (1MB) would require an
// impractically large test batch to exercise chunking. The responder counts
// how many distinct TransformRequest messages it receives and how many
// total records they carry, so this proves chunking actually happens (not
// just that chunkRequest's pure function looks right in isolation) and that
// every record is still transformed with its original order preserved.
func TestTransformBatch_ChunksOversizedBatch(t *testing.T) {
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
	defer func() { _ = natsC.Terminate(ctx) }()

	url, _ := natsC.ConnectionString(ctx)

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    "daya.transform.chunk.test",
		"timeout_ms": 5000.0,
		"schemas":    []interface{}{"tenant_a"},
	})
	assert.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer func() { _ = tf.Stop() }()

	// Force a small chunk budget so a batch of ordinary-sized records must
	// split into multiple requests. This is the one deliberate deviation
	// from "real conn.MaxPayload()" -- everything downstream of it
	// (marshal, NATS request/reply, unmarshal, reassembly) is the real path.
	tf.maxPayload = 512

	nc, err := nats.Connect(url)
	assert.NoError(t, err)
	defer nc.Close()

	var mu sync.Mutex
	var requestsSeen int
	var recordsSeen int

	sub, err := nc.Subscribe("daya.transform.chunk.test", func(msg *nats.Msg) {
		var req cdctransformv1.TransformRequest
		if err := proto.Unmarshal(msg.Data, &req); err != nil {
			return
		}

		mu.Lock()
		requestsSeen++
		recordsSeen += len(req.Records)
		mu.Unlock()

		results := make([]*cdctransformv1.TransformRecordResult, len(req.Records))
		for i, rec := range req.Records {
			transformedData := make(map[string]*cdctransformv1.TypedValue, len(rec.Data)+1)
			for k, v := range rec.Data {
				transformedData[k] = v
			}
			transformedData["chunked"] = &cdctransformv1.TypedValue{Kind: &cdctransformv1.TypedValue_StringValue{StringValue: "true"}}
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
	assert.NoError(t, nc.Flush())
	defer func() { _ = sub.Unsubscribe() }()

	const numRecords = 20
	msgs := make([]protocol.Message, numRecords)
	for i := 0; i < numRecords; i++ {
		msgs[i] = protocol.Message{
			SourceID:    "s1",
			Table:       "orders",
			Op:          protocol.OpInsert,
			TableSchema: "tenant_a",
			UUID:        fmt.Sprintf("chunk-msg-%04d", i),
			Data: map[string]interface{}{
				"id":          i,
				"description": strings.Repeat("x", 100), // padding so each record has real weight
			},
		}
	}

	result, err := tf.TransformBatch(ctx, msgs)
	assert.NoError(t, err)
	require.Equal(t, numRecords, len(result), "every record must survive chunking")

	mu.Lock()
	defer mu.Unlock()
	assert.Greater(t, requestsSeen, 1, "a %d-byte maxPayload budget must force more than one TransformRequest for %d padded records", tf.maxPayload, numRecords)
	assert.Equal(t, numRecords, recordsSeen, "the sum of records across all chunk requests must equal the original batch size, no more, no less")

	// Order and content must both be intact across the chunk boundary.
	for i, m := range result {
		assert.Equal(t, fmt.Sprintf("chunk-msg-%04d", i), m.UUID, "chunked records must come back in original order")
		assert.Equal(t, "true", m.Data["chunked"], "every record, regardless of which chunk it landed in, must have been transformed")
	}
}

// TestChunkRequest_SingleOversizedRecordGetsOwnChunk pins chunkRequest's
// documented edge case directly: a lone record whose own wire size already
// exceeds the budget must not be dropped or hang the loop -- it gets its own
// single-record chunk.
func TestChunkRequest_SingleOversizedRecordGetsOwnChunk(t *testing.T) {
	tf := &NatsProtoTransformer{pipelineID: "p1", maxPayload: 32}

	big := make(map[string]interface{}, 1)
	big["blob"] = strings.Repeat("y", 500)
	msgs := []protocol.Message{
		{SourceID: "s1", Table: "t", Op: protocol.OpInsert, Data: big},
		{SourceID: "s1", Table: "t", Op: protocol.OpInsert, Data: map[string]interface{}{"a": "b"}},
	}

	req, err := tf.buildTransformRequest(msgs)
	require.NoError(t, err)

	chunks := tf.chunkRequest(req)
	require.GreaterOrEqual(t, len(chunks), 2, "the oversized record must not be merged into the same chunk as the small one")

	total := 0
	for _, c := range chunks {
		total += len(c.Records)
	}
	assert.Equal(t, 2, total, "chunking must never drop a record")
}

// ----------------------------------------------------------------------------
// Carry-over from round 4 (untested but verified-correct paths). Round 4's
// validator exercised these with throwaway tests that were never committed --
// exactly the shape ("tests exercise the function, not the path") that
// produced three prior rounds of findings, per the remediation plan.
// ----------------------------------------------------------------------------

// TestParseResponseWithOrder_KindReattachment_ChainedHop pins
// decodeTypedValueMap's ColumnKinds side-channel: when a responder returns a
// DecimalValue in TransformedData, the decoded protocol.Message must carry
// protocol.ColumnKindDecimal in its ColumnKinds so that a *second*
// nats/protobuf processor later in the same pipeline (a "chained hop") sees
// the kind hint and re-encodes it as decimal_value again, rather than an
// ordinary string that would silently become string_value on the second
// hop. This exercises the real transport (proto marshal/unmarshal over a
// real NATS connection), not a hand-built TypedValue literal, then feeds
// the *decoded* protocol.Message back through encodeTypedValue directly
// (the same call buildTransformRequest would make on a second hop) to prove
// the round trip is closed, not just that ColumnKinds got populated.
func TestParseResponseWithOrder_KindReattachment_ChainedHop(t *testing.T) {
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
	defer func() { _ = natsC.Terminate(ctx) }()

	url, _ := natsC.ConnectionString(ctx)

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    "daya.transform.kind.test",
		"timeout_ms": 5000.0,
		"tables":     []interface{}{"invoices"},
	})
	assert.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer func() { _ = tf.Stop() }()

	nc, err := nats.Connect(url)
	assert.NoError(t, err)
	defer nc.Close()

	// The responder returns a DecimalValue for "amount" -- a real
	// TransformedData response shaped exactly as daya-core's would be.
	sub, err := nc.Subscribe("daya.transform.kind.test", func(msg *nats.Msg) {
		var req cdctransformv1.TransformRequest
		if err := proto.Unmarshal(msg.Data, &req); err != nil {
			return
		}
		results := make([]*cdctransformv1.TransformRecordResult, len(req.Records))
		for i := range req.Records {
			results[i] = &cdctransformv1.TransformRecordResult{
				Success: true,
				Keep:    true,
				TransformedData: map[string]*cdctransformv1.TypedValue{
					"amount": {Kind: &cdctransformv1.TypedValue_DecimalValue{DecimalValue: "1500.50"}},
				},
			}
		}
		resp := cdctransformv1.TransformResponse{Results: results}
		respBytes, _ := proto.Marshal(&resp)
		_ = msg.Respond(respBytes)
	})
	assert.NoError(t, err)
	assert.NoError(t, nc.Flush())
	defer func() { _ = sub.Unsubscribe() }()

	msgs := []protocol.Message{
		{SourceID: "s1", Table: "invoices", Op: protocol.OpInsert, UUID: "inv-1", Data: map[string]interface{}{"amount": "0"}},
	}

	result, err := tf.TransformBatch(ctx, msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	// The decoded value itself is the plain decimal-text string...
	assert.Equal(t, "1500.50", result[0].Data["amount"])
	// ...but the kind hint must have survived the round trip so a second hop
	// knows this is a decimal, not an ordinary string.
	require.NotNil(t, result[0].ColumnKinds)
	assert.Equal(t, protocol.ColumnKindDecimal, result[0].ColumnKinds["amount"], "the DecimalValue kind must be re-attached via ColumnKinds for a chained second hop")

	// Close the loop: feed the decoded value + kind hint through the same
	// encoder a second hop's buildTransformRequest would call, and confirm
	// it comes back out as decimal_value again -- not string_value, which is
	// exactly the silent-corruption failure mode this test guards against.
	reEncoded := encodeTypedValue(result[0].Data["amount"], result[0].ColumnKinds["amount"])
	dv, ok := reEncoded.Kind.(*cdctransformv1.TypedValue_DecimalValue)
	require.True(t, ok, "a second hop must re-encode the value as decimal_value, not string_value")
	assert.Equal(t, "1500.50", dv.DecimalValue)
}

// TestParseResponseWithOrder_PureFilter_PreservesDataAndColumnKinds pins the
// Keep:true / nil-TransformedData "pure filter" response shape: a responder
// that only decides Keep (no TransformedData at all) must leave both
// Data and ColumnKinds exactly as they were in the request -- nothing about
// the columns changed, so nothing about their routing hints should either.
// Driven through the real transport so the "response has no
// transformed_data field at all" case is the real wire shape (a genuinely
// absent map, decoded via proto.Unmarshal), not a Go nil constructed by
// hand.
func TestParseResponseWithOrder_PureFilter_PreservesDataAndColumnKinds(t *testing.T) {
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
	defer func() { _ = natsC.Terminate(ctx) }()

	url, _ := natsC.ConnectionString(ctx)

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    "daya.transform.purefilter.test",
		"timeout_ms": 5000.0,
		"tables":     []interface{}{"ledger"},
	})
	assert.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer func() { _ = tf.Stop() }()

	nc, err := nats.Connect(url)
	assert.NoError(t, err)
	defer nc.Close()

	// The responder makes a pure Keep decision and explicitly leaves
	// TransformedData nil -- a legal response shape (documented at
	// parseResponseWithOrder's TransformedData-nil branch) distinct from
	// "TransformedData present but empty".
	sub, err := nc.Subscribe("daya.transform.purefilter.test", func(msg *nats.Msg) {
		var req cdctransformv1.TransformRequest
		if err := proto.Unmarshal(msg.Data, &req); err != nil {
			return
		}
		results := make([]*cdctransformv1.TransformRecordResult, len(req.Records))
		for i := range req.Records {
			results[i] = &cdctransformv1.TransformRecordResult{
				Success:         true,
				Keep:            true,
				TransformedData: nil, // pure filter: no columns changed
			}
		}
		resp := cdctransformv1.TransformResponse{Results: results}
		respBytes, _ := proto.Marshal(&resp)
		_ = msg.Respond(respBytes)
	})
	assert.NoError(t, err)
	assert.NoError(t, nc.Flush())
	defer func() { _ = sub.Unsubscribe() }()

	// The request itself carries a decimal-kind column, so ColumnKinds is
	// non-empty going in -- this is what must survive untouched.
	msgs := []protocol.Message{{
		SourceID:    "s1",
		Table:       "ledger",
		Op:          protocol.OpInsert,
		UUID:        "ledger-1",
		Data:        map[string]interface{}{"balance": "42.00", "label": "checking"},
		ColumnKinds: map[string]string{"balance": protocol.ColumnKindDecimal},
	}}

	result, err := tf.TransformBatch(ctx, msgs)
	require.NoError(t, err)
	require.Len(t, result, 1)

	assert.Equal(t, "42.00", result[0].Data["balance"], "Data must be left exactly as the request's own value -- untouched by a pure-filter response")
	assert.Equal(t, "checking", result[0].Data["label"])
	require.NotNil(t, result[0].ColumnKinds, "ColumnKinds must not be dropped by a nil-TransformedData response")
	assert.Equal(t, protocol.ColumnKindDecimal, result[0].ColumnKinds["balance"], "the pre-existing decimal kind hint must be preserved, not cleared, when TransformedData is nil")
}

// ----------------------------------------------------------------------------
// WS-4C: measured pipeline-only throughput ceiling
// ----------------------------------------------------------------------------

// TestMeasure_PipelineOnlyTransformLatency is WS-4C's "measure before
// building" step. It does not simulate daya-core's own DB/Databend latency
// (there is no real daya-core to measure here) -- it isolates and measures
// what this repo alone contributes to per-batch wall-clock: proto marshal of
// the request, the real NATS round trip (loopback, real network stack via
// testcontainers), and proto unmarshal + reassembly of the response. That is
// the floor this repo imposes regardless of how fast daya-core answers,
// since (per WS-4C's own finding) there is at most one in-flight transform
// request per sink and TransformBatch blocks the whole consumer loop for its
// duration.
//
// The responder replies immediately (no artificial delay), so the measured
// numbers are a lower bound on real per-batch latency, not an estimate of
// end-to-end throughput -- the real daya-core+Databend round trip is
// necessarily larger. Reported as evidence for whether config.example.yaml's
// batch sizes (100/1000/2000) leave headroom over the ~100 rows/s peak
// figure in docs/todos/custom_object_cdc_followups.md:29, given a single
// in-flight request per sink.
func TestMeasure_PipelineOnlyTransformLatency(t *testing.T) {
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
	defer func() { _ = natsC.Terminate(ctx) }()

	url, _ := natsC.ConnectionString(ctx)
	subject := "daya.transform.throughput.test"

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var req cdctransformv1.TransformRequest
		if err := proto.Unmarshal(msg.Data, &req); err != nil {
			return
		}
		results := make([]*cdctransformv1.TransformRecordResult, len(req.Records))
		for i := range req.Records {
			results[i] = &cdctransformv1.TransformRecordResult{Success: true, Keep: true}
		}
		resp := cdctransformv1.TransformResponse{Results: results}
		respBytes, _ := proto.Marshal(&resp)
		_ = msg.Respond(respBytes)
	})
	require.NoError(t, err)
	require.NoError(t, nc.Flush())
	defer func() { _ = sub.Unsubscribe() }()

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   url,
		"subject":    subject,
		"timeout_ms": 15000.0,
		"tables":     []interface{}{"custom_objects_row"},
	})
	require.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer func() { _ = tf.Close() }()

	// Representative column shape for a custom-object row (matches the
	// remediation plan's "generated custom table" description: a handful of
	// CITEXT-ish columns).
	makeMsgs := func(n int) []protocol.Message {
		msgs := make([]protocol.Message, n)
		for i := 0; i < n; i++ {
			msgs[i] = protocol.Message{
				SourceID: "s1",
				Table:    "custom_objects_row",
				Op:       protocol.OpInsert,
				UUID:     fmt.Sprintf("row-%d", i),
				Data: map[string]interface{}{
					"field_a": "some text value",
					"field_b": "another field value",
					"field_c": "42.50",
					"field_d": "2026-08-02T10:00:00Z",
				},
			}
		}
		return msgs
	}

	// config.example.yaml ships batch_size: 100 (schema-scoped custom_objects
	// processor), 1000 (source default), and 2000 (a shipped example).
	for _, batchSize := range []int{100, 1000, 2000} {
		batchSize := batchSize
		t.Run(fmt.Sprintf("batch_size=%d", batchSize), func(t *testing.T) {
			msgs := makeMsgs(batchSize)

			// Warm-up: first call pays one-time connection/subscription
			// costs that would not recur in steady-state operation.
			_, err := tf.TransformBatch(ctx, msgs)
			require.NoError(t, err)

			const iterations = 5
			var total time.Duration
			for i := 0; i < iterations; i++ {
				start := time.Now()
				res, err := tf.TransformBatch(ctx, msgs)
				elapsed := time.Since(start)
				require.NoError(t, err)
				require.Len(t, res, batchSize)
				total += elapsed
			}
			avg := total / iterations
			rowsPerSec := float64(batchSize) / avg.Seconds()
			t.Logf("MEASURED batch_size=%d avg_batch_latency=%s pipeline_only_ceiling=%.0f rows/s (marshal+NATS RTT+unmarshal only, zero daya-core/Databend latency)", batchSize, avg, rowsPerSec)
		})
	}
}
