package nats

import (
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	cdctransformv1 "bitbucket.org/daya-engineering/daya-contracts/v2/gen/go/cdc/transform/v1"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

// assertDecimalEqual compares want/got as exact numeric values, not as
// strings. daya-core's decimal encoder (shopspring/decimal's String())
// normalizes scale -- "1500.50" and "1500.5" are the same number and both
// are legitimate wire forms, since the column's actual display scale comes
// from the Databend sink's own decimal_precision/decimal_scale config, not
// from the value's text (see summaries/phase4_wire_contract.md). A prior
// version of this test asserted byte-for-byte string equality here and
// flagged a false positive over exactly this normalization. Comparing
// numerically (via math/big.Rat, which is exact and arbitrary-precision --
// no float64 involved) is what actually matters: it still fails loudly if a
// decimal is ever silently rounded or routed through a float, which
// string-equality was accidentally over-asserting past. Do not re-tighten
// this back to string equality.
func assertDecimalEqual(t *testing.T, want, got, what string) {
	t.Helper()
	wantR, ok := new(big.Rat).SetString(want)
	require.Truef(t, ok, "%s: expected value %q must parse as an exact decimal", what, want)
	gotR, ok := new(big.Rat).SetString(got)
	require.Truef(t, ok, "%s: observed value %q must parse as an exact decimal", what, got)
	assert.Truef(t, wantR.Cmp(gotR) == 0, "%s: decimal values differ numerically: want %s, got %s", what, want, got)
}

// Phase 4 golden wire fixture exchange, step 1 and step 3.
//
// Step 1 (this file's Test*RequestFixture tests): build a realistic
// protocol.Message set, run it through the REAL
// buildTransformRequest/encodeTypedValue path (unexported methods on
// NatsProtoTransformer, reachable because this test lives in package nats),
// proto.Marshal the result, and check it against a fixture committed at
// testdata/wire/ (repo root). The same fixture bytes are committed in
// daya-core's testdata/wire/ so daya-core's Step 2 test can consume them
// without ever talking to this pipeline over NATS -- see
// summaries/phase4_wire_contract.md for the full writeup and the NATS
// deployment blocker that motivated this approach.
//
// Step 3 (TestWireFixture_ResponseInsert_ConsumedByPipeline below): read the
// TransformResponse daya-core's Step 2 test produced (copied in from
// daya-core's testdata/wire/), run it through the REAL
// parseResponseWithOrder, and assert the decoded values/kinds/order are what
// the sink would actually write.
//
// Regeneration is explicit: set CDC_REGEN_WIRE_FIXTURES=1 to rewrite the
// request fixtures from the current encoder. Without it, a mismatch is a
// hard test failure (drift), never a silent overwrite.
const regenerateEnvVar = "CDC_REGEN_WIRE_FIXTURES"

func wireFixtureTable() string { return "cdc_wire_fixture_table" }

func wireFixtureDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	return filepath.Join(wd, "..", "..", "..", "testdata", "wire")
}

// buildFixtureInsertMessage is the single record this fixture exchange
// leans on to catch every wire hazard named in the Phase 4 spec:
//   - number/currency/percentage/date/datetime/boolean/multi_select/linked_record
//   - an explicit NULL (field_url) and an empty string (field_email)
//   - a toasted_unchanged column (field_long_text: no Data entry at all,
//     only a ColumnKinds marker -- see protocol.ColumnKindToastedUnchanged)
//   - a primary key above 2^53 (id), which a float64 hop would corrupt
func buildFixtureInsertMessage() protocol.Message {
	const bigPK = int64(9007199254740993) // 2^53 + 1

	return protocol.Message{
		SourceID:    "wire-fixture-source",
		SinkID:      "wire-fixture-sink",
		Table:       wireFixtureTable(),
		TableSchema: "custom_objects",
		Op:          protocol.OpUpdate,
		LSN:         12345,
		PK:          "9007199254740993",
		UUID:        "3f746f97-4000-4000-8000-000000000001",
		Timestamp:   time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC),
		Data: map[string]interface{}{
			"id":              bigPK,
			"field_full_name": "Alice Wire Fixture",
			"field_number":    int64(42),
			// pgtype.Numeric collapses to plain decimal text pre-transport.
			// Deliberately beyond float64's exact-integer range (2^53) AND
			// carries fractional digits, so any regression that routes a
			// decimal through a float anywhere on the path (encode, parse,
			// or re-encode) fails loudly on the integer part, the fractional
			// part, or both -- not just a display-scale nit.
			"field_currency":      "9007199254740993.123456789",
			"field_percentage":    "12.5",
			"field_date":          "2026-03-15", // custom_objects date columns are CITEXT: arrive as plain strings
			"field_datetime":      time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC),
			"field_boolean":       true,
			"field_multi_select":  []interface{}{"red", "green", "blue"},
			"field_linked_record": []interface{}{int64(10), int64(20)},
			"field_email":         "", // must survive distinct from NULL
			"field_url":           nil,
		},
		ColumnKinds: map[string]string{
			"field_currency":   protocol.ColumnKindDecimal,
			"field_percentage": protocol.ColumnKindDecimal,
			// No Data["field_long_text"] entry at all -- this is what a
			// genuine TOASTed-and-unchanged column looks like on the wire.
			"field_long_text": protocol.ColumnKindToastedUnchanged,
		},
	}
}

func buildFixtureSchemaChangeMessage() protocol.Message {
	return protocol.Message{
		SourceID:    "wire-fixture-source",
		Table:       wireFixtureTable(),
		TableSchema: "custom_objects",
		Op:          protocol.OpSchemaChange,
	}
}

func buildFixtureDeleteMessage() protocol.Message {
	return protocol.Message{
		SourceID:    "wire-fixture-source",
		Table:       wireFixtureTable(),
		TableSchema: "custom_objects",
		Op:          protocol.OpDelete,
		PK:          "9007199254740993",
		Data: map[string]interface{}{
			"id": int64(9007199254740993),
		},
	}
}

func TestWireFixture_RequestInsert(t *testing.T) {
	generateOrVerifyRequestFixture(t, "request_insert.pb", []protocol.Message{buildFixtureInsertMessage()})
}

func TestWireFixture_RequestSchemaChange(t *testing.T) {
	generateOrVerifyRequestFixture(t, "request_schema_change.pb", []protocol.Message{buildFixtureSchemaChangeMessage()})
}

func TestWireFixture_RequestDelete(t *testing.T) {
	generateOrVerifyRequestFixture(t, "request_delete.pb", []protocol.Message{buildFixtureDeleteMessage()})
}

// generateOrVerifyRequestFixture runs msgs through the real
// buildTransformRequest/encodeTypedValue path and either (a) writes the
// marshalled bytes to testdata/wire/<filename> when CDC_REGEN_WIRE_FIXTURES
// is set, or (b) fails loudly if the freshly-built request no longer matches
// the committed fixture.
func generateOrVerifyRequestFixture(t *testing.T, filename string, msgs []protocol.Message) {
	t.Helper()
	tf := &NatsProtoTransformer{pipelineID: "wire-fixture-pipeline"}
	req, err := tf.buildTransformRequest(msgs)
	require.NoError(t, err)
	got, err := proto.Marshal(req)
	require.NoError(t, err)

	path := filepath.Join(wireFixtureDir(t), filename)

	if os.Getenv(regenerateEnvVar) != "" {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		require.NoError(t, os.WriteFile(path, got, 0o600))
		t.Logf("regenerated fixture %s (%d bytes)", path, len(got))
		return
	}

	golden, err := os.ReadFile(path) //nolint:gosec // test-only fixture path built from a fixed testdata dir + a hardcoded filename, never from external input
	require.NoErrorf(t, err, "fixture %s missing; run with %s=1 to generate it", path, regenerateEnvVar)

	var gotReq, wantReq cdctransformv1.TransformRequest
	require.NoError(t, proto.Unmarshal(got, &gotReq))
	require.NoError(t, proto.Unmarshal(golden, &wantReq))
	assert.Truef(t, proto.Equal(&gotReq, &wantReq),
		"request fixture %s has drifted from the real buildTransformRequest/encodeTypedValue output -- "+
			"regenerate with %s=1 ONLY if this is an intended encoding change", filename, regenerateEnvVar)
}

// TestWireFixture_ResponseInsert_ConsumedByPipeline is Step 3: it reads the
// TransformResponse bytes daya-core's real HandleTransformRequest produced
// (committed at testdata/wire/response_insert.pb, copied over from
// daya-core's own copy of the same fixture) and runs them through the real
// parseResponseWithOrder, asserting what the sink would actually observe.
func TestWireFixture_ResponseInsert_ConsumedByPipeline(t *testing.T) {
	path := filepath.Join(wireFixtureDir(t), "response_insert.pb")
	b, err := os.ReadFile(path) //nolint:gosec // test-only fixture path built from a fixed testdata dir + a hardcoded filename, never from external input
	require.NoErrorf(t, err, "response fixture %s missing -- produced by daya-core's Step 2 test, copy it in", path)

	var resp cdctransformv1.TransformResponse
	require.NoError(t, proto.Unmarshal(b, &resp))

	tf := &NatsProtoTransformer{pipelineID: "wire-fixture-pipeline"}
	msgs := []protocol.Message{buildFixtureInsertMessage()}
	results, err := tf.parseResponseWithOrder(msgs, &resp)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	require.NotNil(t, r.msg, "daya-core must have kept the record (Keep:true)")

	assert.Equal(t, int64(42), r.msg.Data["field_number"], "number must decode to an int64, not a float")
	curGot, ok := r.msg.Data["field_currency"].(string)
	require.True(t, ok, "field_currency must decode to a string (decimal text), not some other Go type")
	assertDecimalEqual(t, "9007199254740993.123456789", curGot, "field_currency")
	assert.Equal(t, protocol.ColumnKindDecimal, r.msg.ColumnKinds["field_currency"], "decimal routing kind must round-trip")
	assert.Equal(t, "12.5", r.msg.Data["field_percentage"])
	assert.Equal(t, protocol.ColumnKindDecimal, r.msg.ColumnKinds["field_percentage"])
	assert.Equal(t, true, r.msg.Data["field_boolean"])
	assert.Equal(t, int64(9007199254740993), r.msg.Data["id"], "a >2^53 id must round-trip exactly, not lose precision through a float")

	v, ok := r.msg.Data["field_url"]
	assert.True(t, ok, "an explicit NULL must be a present key with a nil value, not silently dropped")
	assert.Nil(t, v)

	ev, eok := r.msg.Data["field_email"]
	assert.True(t, eok, "an empty string must not collapse into an absent key")
	assert.Equal(t, "", ev, "an empty string must not collapse into NULL")
}
