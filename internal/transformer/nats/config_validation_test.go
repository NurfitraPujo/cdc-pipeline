// Package nats contains the NATS JetStream transformer's config-validation
// test suite.
package nats

import (
	"context"
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	cdctransformv1 "bitbucket.org/daya-engineering/daya-contracts/v2/gen/go/cdc/transform/v1"
	tc_nats "github.com/testcontainers/testcontainers-go/modules/nats"
)

// WS-8 item 4 -- reject path: an unfiltered nats/protobuf instance would
// forward every table in the pipeline to daya-core. This must fail at
// construction time rather than start and silently misbehave. No NATS server
// is needed for this case: validation happens before nats.Connect.
func TestNewNatsProtoTransformer_RejectsMissingSchemasAndTables(t *testing.T) {
	_, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   "nats://127.0.0.1:4222",
		"subject":    "daya.transform.reject",
		"timeout_ms": 1000.0,
		// deliberately no "schemas" and no "tables"
	})
	require.Error(t, err, "expected construction to fail without a schemas/tables filter")
	assert.Contains(t, err.Error(), "schemas", "error should name the missing filter option")
}

// WS-8 item 3 -- reject/degrade path for malformed option types: a scalar
// `schemas: "custom_objects"` (instead of a list) or a stringly-typed
// `timeout_ms` must not silently succeed with a filter that matches nothing;
// construction should still fail overall here because, once the malformed
// `schemas` value is discarded, there is no other filter left.
func TestNewNatsProtoTransformer_RejectsScalarSchemasOption(t *testing.T) {
	_, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":   "nats://127.0.0.1:4222",
		"subject":    "daya.transform.reject.scalar",
		"timeout_ms": 1000.0,
		"schemas":    "custom_objects", // wrong shape: should be []interface{}
	})
	require.Error(t, err, "a scalar schemas option must not silently become an unfiltered instance")
}

// WS-8 item 4 -- accept path: a processor that declares either schemas or
// tables constructs successfully.
func TestNewNatsProtoTransformer_AcceptsSchemasOrTablesFilter(t *testing.T) {
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

	t.Run("schemas only", func(t *testing.T) {
		tf, err := NewNatsProtoTransformer(map[string]interface{}{
			"nats_url":   url,
			"subject":    "daya.transform.accept.schemas",
			"timeout_ms": 1000.0,
			"schemas":    []interface{}{"custom_objects"},
		})
		require.NoError(t, err)
		require.NoError(t, tf.(interface{ Close() error }).Close())
	})

	t.Run("tables only", func(t *testing.T) {
		tf, err := NewNatsProtoTransformer(map[string]interface{}{
			"nats_url":   url,
			"subject":    "daya.transform.accept.tables",
			"timeout_ms": 1000.0,
			"tables":     []interface{}{"users"},
		})
		require.NoError(t, err)
		require.NoError(t, tf.(interface{ Close() error }).Close())
	})
}

// WS-9 -- the observability additions must be exercised by a real test that
// asserts on them, not just declared. This drives a full TransformBatch
// round trip (matched + passed-through + dropped records, plus a hard
// transport failure) through a real NATS responder and asserts the
// Prometheus counters/histograms actually moved by the right amount, with
// the right labels.
func TestNatsProtoTransformer_Metrics(t *testing.T) {
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

	const pipelineID = "metrics-test-pipeline"
	const subject = "daya.transform.metrics.test"

	tfRaw, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":    url,
		"subject":     subject,
		"timeout_ms":  2000.0,
		"tables":      []interface{}{"orders"},
		"pipeline_id": pipelineID,
	})
	require.NoError(t, err)
	tf := tfRaw.(*NatsProtoTransformer)
	defer func() { _ = tf.Stop() }()

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	// Responder: keeps (transforms) the first record, drops the second.
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var req cdctransformv1.TransformRequest
		if err := proto.Unmarshal(msg.Data, &req); err != nil {
			return
		}
		results := make([]*cdctransformv1.TransformRecordResult, len(req.Records))
		for i := range req.Records {
			results[i] = &cdctransformv1.TransformRecordResult{
				Success: true,
				Keep:    i == 0, // drop every record after the first
			}
		}
		resp := cdctransformv1.TransformResponse{Results: results}
		respBytes, _ := proto.Marshal(&resp)
		_ = msg.Respond(respBytes)
	})
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	msgs := []protocol.Message{
		{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, UUID: "kept"},
		{SourceID: "s1", Table: "orders", Op: protocol.OpUpdate, UUID: "dropped"},
		{SourceID: "s1", Table: "users", Op: protocol.OpInsert, UUID: "passthrough"}, // filtered out by "tables": ["orders"]
	}

	before := testutil.ToFloat64(metrics.TransformRequestsTotal.WithLabelValues(pipelineID, tf.Name(), "success"))

	result, err := tf.TransformBatch(ctx, msgs)
	require.NoError(t, err)
	assert.Len(t, result, 2, "1 transformed + 1 passthrough; the dropped record is gone")

	after := testutil.ToFloat64(metrics.TransformRequestsTotal.WithLabelValues(pipelineID, tf.Name(), "success"))
	assert.Equal(t, before+1, after, "one successful transform RPC should have been recorded")

	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.TransformRecordsTotal.WithLabelValues(pipelineID, tf.Name(), "transformed")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.TransformRecordsTotal.WithLabelValues(pipelineID, tf.Name(), "dropped")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.TransformRecordsTotal.WithLabelValues(pipelineID, tf.Name(), "passthrough")))

	durationCount := testutil.CollectAndCount(metrics.TransformDurationSeconds)
	assert.Greater(t, durationCount, 0, "transform_duration_seconds should have at least one observation")

	reqBytesCount := testutil.CollectAndCount(metrics.TransformRequestBytes)
	respBytesCount := testutil.CollectAndCount(metrics.TransformResponseBytes)
	assert.Greater(t, reqBytesCount, 0, "transform_request_bytes should have been observed")
	assert.Greater(t, respBytesCount, 0, "transform_response_bytes should have been observed")

	// Now drive a transport failure (no responder listening on this subject)
	// and assert the "error"/"failed" outcome labels move too.
	tfFail, err := NewNatsProtoTransformer(map[string]interface{}{
		"nats_url":    url,
		"subject":     "daya.transform.metrics.nobody-listening",
		"timeout_ms":  200.0,
		"tables":      []interface{}{"orders"},
		"pipeline_id": pipelineID,
	})
	require.NoError(t, err)
	tfF := tfFail.(*NatsProtoTransformer)
	defer func() { _ = tfF.Stop() }()

	_, err = tfF.TransformBatch(ctx, []protocol.Message{{SourceID: "s1", Table: "orders", Op: protocol.OpInsert, UUID: "will-fail"}})
	require.Error(t, err, "expected the request to fail with no responder subscribed")

	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.TransformRequestsTotal.WithLabelValues(pipelineID, tfF.Name(), "error")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.TransformRecordsTotal.WithLabelValues(pipelineID, tfF.Name(), "failed")))
}
