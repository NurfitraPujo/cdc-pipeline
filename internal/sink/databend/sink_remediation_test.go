package databend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// Test helpers
// ----------------------------------------------------------------------------

// fakeDB is a lightweight in-memory DBExec implementation for unit tests.
// It records every ExecContext call and can be configured to return canned
// results from QueryRowScan (used for SHOW CREATE TABLE) and QueryContext.
type fakeDB struct {
	mu sync.Mutex

	// execCalls records the queries invoked via ExecContext in order.
	execCalls []string

	// scanFn is invoked by QueryRowScan. If nil the call returns sql.ErrNoRows.
	scanFn func(query string, args []any, dest ...any) error

	// queryFn is invoked by QueryContext. If nil the call returns an empty
	// rows iterator.
	queryFn func(query string, args ...any) (DBRows, error)

	// closeErr is returned from Close, if set.
	closeErr error
}

func newFakeDB() *fakeDB {
	return &fakeDB{}
}

func (f *fakeDB) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execCalls = append(f.execCalls, query)
	return fakeResult{}, nil
}

func (f *fakeDB) QueryContext(_ context.Context, query string, args ...any) (DBRows, error) {
	if f.queryFn != nil {
		return f.queryFn(query, args...)
	}
	return emptyDBRows{}, nil
}

func (f *fakeDB) QueryRowScan(_ context.Context, query string, args []any, dest ...any) error {
	if f.scanFn == nil {
		return sql.ErrNoRows
	}
	return f.scanFn(query, args, dest...)
}

func (f *fakeDB) Close() error {
	return f.closeErr
}

func (f *fakeDB) ExecCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.execCalls)
}

func (f *fakeDB) ExecCountMatching(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, q := range f.execCalls {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(q)), strings.ToUpper(prefix)) {
			count++
		}
	}
	return count
}

type fakeResult struct{}

func (fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (fakeResult) RowsAffected() (int64, error) { return 0, nil }

// fakeDLQPublisher records every publish so tests can assert on the DLQ flow
// without depending on a real NATS connection.
type fakeDLQPublisher struct {
	mu       sync.Mutex
	calls    []dlqPublishCall
	publishErr error
}

type dlqPublishCall struct {
	Topic     string
	MessageID string
	Payload   []byte
}

func (p *fakeDLQPublisher) Publish(topic string, messages ...*message.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.publishErr != nil {
		return p.publishErr
	}
	for _, m := range messages {
		p.calls = append(p.calls, dlqPublishCall{
			Topic:     topic,
			MessageID: m.UUID,
			Payload:   append([]byte(nil), m.Payload...),
		})
	}
	return nil
}

func (p *fakeDLQPublisher) Calls() []dlqPublishCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]dlqPublishCall, len(p.calls))
	copy(out, p.calls)
	return out
}

func newTestSink(db DBExec, opts ...map[string]interface{}) *DatabendSink {
	options := map[string]interface{}{
		"max_placeholders":  10000,
		"decimal_precision": 38,
		"decimal_scale":     9,
	}
	for _, o := range opts {
		for k, v := range o {
			options[k] = v
		}
	}
	snk := &DatabendSink{
		name:             "test-sink",
		db:               db,
		pkCache:          make(map[string][]string),
		pkLoaded:         make(map[string]struct{}),
		maxPlaceholders:  DefaultMaxPlaceholders,
		decimalPrecision: DefaultDecimalPrecision,
		decimalScale:     DefaultDecimalScale,
		dlqSubject:       DefaultSinkDeadLetterSubject("test-sink"),
	}
	return snk.WithOptions(options)
}

func payloadJSON(v map[string]any) []byte {
	b, err := marshalJSONForTest(v)
	if err != nil {
		panic(err)
	}
	return b
}

// marshalJSONForTest is a tiny indirection so the test file does not need to
// import encoding/json directly when constructing payloads.
func marshalJSONForTest(v map[string]any) ([]byte, error) {
	return jsonMarshal(v)
}

// ----------------------------------------------------------------------------
// T2-4: type mapping
// ----------------------------------------------------------------------------

func TestMapPgTypeToDatabend_T2_4(t *testing.T) {
	snk := newTestSink(newFakeDB())

	tests := []struct {
		name string
		pg   string
		want string
	}{
		{name: "numeric default", pg: "numeric", want: "DECIMAL(38, 9)"},
		{name: "decimal default", pg: "decimal", want: "DECIMAL(38, 9)"},
		{name: "numeric with precision", pg: "numeric(10,2)", want: "DECIMAL(38, 9)"},
		{name: "decimal uppercase", pg: "DECIMAL", want: "DECIMAL(38, 9)"},
		{name: "int array", pg: "int[]", want: "VARIANT"},
		{name: "text array", pg: "text[]", want: "VARIANT"},
		{name: "numeric array", pg: "numeric[]", want: "VARIANT"},
		{name: "underscore array", pg: "_int4", want: "VARIANT"},
		{name: "boolean", pg: "bool", want: "BOOLEAN"},
		{name: "int", pg: "int", want: "INT64"},
		{name: "bigint", pg: "bigint", want: "INT64"},
		{name: "float", pg: "double precision", want: "FLOAT64"},
		{name: "timestamp", pg: "timestamp", want: "TIMESTAMP"},
		{name: "json", pg: "jsonb", want: "VARIANT"},
		{name: "text", pg: "varchar", want: "STRING"},
		{name: "oid int", pg: "23", want: "INT64"},
		{name: "oid string", pg: "1043", want: "STRING"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := snk.mapPgTypeToDatabend(tc.pg)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMapPgTypeToDatabend_CustomPrecision(t *testing.T) {
	snk := newTestSink(newFakeDB())
	snk.WithOptions(map[string]interface{}{
		"decimal_precision": 20,
		"decimal_scale":     4,
	})

	assert.Equal(t, "DECIMAL(20, 4)", snk.mapPgTypeToDatabend("numeric"))
	assert.Equal(t, "DECIMAL(20, 4)", snk.mapPgTypeToDatabend("decimal"))

	// If scale > precision we clamp scale to precision (Databend DECIMAL
	// requires scale <= precision). precision=10, scale=50 -> DECIMAL(10, 10).
	snk.WithOptions(map[string]interface{}{
		"decimal_precision": 10,
		"decimal_scale":     50,
	})
	assert.Equal(t, "DECIMAL(10, 10)", snk.mapPgTypeToDatabend("numeric"))

	// Scale == 0 emits the DECIMAL(p) shorthand.
	snk.WithOptions(map[string]interface{}{
		"decimal_precision": 15,
		"decimal_scale":     0,
	})
	assert.Equal(t, "DECIMAL(15)", snk.mapPgTypeToDatabend("numeric"))
}

// ----------------------------------------------------------------------------
// T2-4: expanded primitive switch
// ----------------------------------------------------------------------------

func TestNormalizeValue_ExpandedPrimitives(t *testing.T) {
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	cases := []struct {
		name string
		in   any
		// wantJSON indicates the value should be returned as-is (not encoded).
		wantJSON bool
	}{
		{name: "int8", in: int8(1), wantJSON: false},
		{name: "int16", in: int16(2), wantJSON: false},
		{name: "int32", in: int32(3), wantJSON: false},
		{name: "int64", in: int64(4), wantJSON: false},
		{name: "int", in: int(5), wantJSON: false},
		{name: "uint", in: uint(6), wantJSON: false},
		{name: "uint8", in: uint8(7), wantJSON: false},
		{name: "uint16", in: uint16(8), wantJSON: false},
		{name: "uint32", in: uint32(9), wantJSON: false},
		{name: "uint64", in: uint64(10), wantJSON: false},
		{name: "float32", in: float32(1.5), wantJSON: false},
		{name: "float64", in: 2.5, wantJSON: false},
		{name: "string", in: "hello", wantJSON: false},
		{name: "bool", in: true, wantJSON: false},
		{name: "time", in: now, wantJSON: false},
		{name: "map", in: map[string]any{"a": 1}, wantJSON: true},
		{name: "slice", in: []int{1, 2, 3}, wantJSON: true},
		{name: "struct", in: struct{ X int }{X: 1}, wantJSON: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			out := normalizeValue(tc.in)
			if tc.wantJSON {
				s, ok := out.(string)
				require.True(t, ok, "expected JSON-encoded string, got %T", out)
				require.NotEmpty(t, s)
				// Ensure it parses as JSON.
				var parsed any
				require.NoError(t, jsonUnmarshal([]byte(s), &parsed))
			} else {
				assert.Equal(t, tc.in, out)
			}
		})
	}
}

func TestNormalizeValue_Nil(t *testing.T) {
	assert.Nil(t, normalizeValue(nil))
}

// ----------------------------------------------------------------------------
// T1-14: chunked REPLACE INTO
// ----------------------------------------------------------------------------

func TestExecuteReplaceIntoChunks_T1_14(t *testing.T) {
	t.Run("splits into N/expected chunks", func(t *testing.T) {
		db := newFakeDB()
		snk := newTestSink(db, map[string]interface{}{
			"max_placeholders": 10000,
		})

		const numRecords = 300
		const colsPerRecord = 100
		records := make([]map[string]any, numRecords)
		for i := 0; i < numRecords; i++ {
			rec := make(map[string]any, colsPerRecord)
			for j := 0; j < colsPerRecord; j++ {
				rec[fmt.Sprintf("c%d", j)] = i*colsPerRecord + j
			}
			records[i] = rec
		}

		columns := make([]string, colsPerRecord)
		for i := 0; i < colsPerRecord; i++ {
			columns[i] = fmt.Sprintf("c%d", i)
		}

		err := snk.executeReplaceIntoChunks(
			context.Background(),
			"chunk_table",
			`"chunk_table"`,
			`"c0", "c1"`,
			`"id"`,
			columns,
			records,
		)
		require.NoError(t, err)

		expectedChunks := (numRecords * colsPerRecord) / 10000
		if numRecords*colsPerRecord%10000 != 0 {
			expectedChunks++
		}
		require.Equal(t, expectedChunks, db.ExecCountMatching("REPLACE INTO"))

		chunksMetric := testutil.ToFloat64(SinkChunksTotal.WithLabelValues(snk.name, "chunk_table"))
		assert.InDelta(t, float64(expectedChunks), chunksMetric, 0.0001)
	})

	t.Run("single chunk when below budget", func(t *testing.T) {
		db := newFakeDB()
		snk := newTestSink(db, map[string]interface{}{
			"max_placeholders": 10000,
		})

		records := []map[string]any{
			{"a": 1, "b": 2},
			{"a": 3, "b": 4},
		}
		columns := []string{"a", "b"}

		err := snk.executeReplaceIntoChunks(
			context.Background(),
			"small_table",
			`"small_table"`,
			`"a", "b"`,
			`"a"`,
			columns,
			records,
		)
		require.NoError(t, err)
		assert.Equal(t, 1, db.ExecCountMatching("REPLACE INTO"))
	})

	t.Run("propagates driver error", func(t *testing.T) {
		db := newFakeDB()
		snk := newTestSink(db, map[string]interface{}{
			"max_placeholders": 4, // forces chunkSize=2 for 2 columns
		})
		// Replace the db with one that always errors so we observe the
		// chunk-failure propagation path.
		snk.db = failingDB{err: errors.New("driver exploded")}

		records := []map[string]any{
			{"a": 1, "b": 2},
			{"a": 3, "b": 4},
			{"a": 5, "b": 6},
		}
		columns := []string{"a", "b"}

		err := snk.executeReplaceIntoChunks(
			context.Background(),
			"err_table",
			`"err_table"`,
			`"a", "b"`,
			`"a"`,
			columns,
			records,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "driver exploded")
	})
}

// TestExecuteReplaceIntoChunks_FailureSurfaces makes sure a chunk failure
// bubbles up as a typed error.
func TestExecuteReplaceIntoChunks_FailureSurfaces(t *testing.T) {
	db := newFakeDB()
	snk := newTestSink(db)
	snk.db = failingDB{err: errors.New("boom")}

	err := snk.executeReplaceIntoChunks(
		context.Background(),
		"t",
		`"t"`,
		`"a"`,
		`"a"`,
		[]string{"a"},
		[]map[string]any{{"a": 1}},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

type failingDB struct{ err error }

func (f failingDB) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, f.err
}
func (f failingDB) QueryContext(_ context.Context, _ string, _ ...any) (DBRows, error) {
	return emptyDBRows{}, nil
}
func (f failingDB) QueryRowScan(_ context.Context, _ string, _ []any, _ ...any) error {
	return f.err
}
func (f failingDB) Close() error { return nil }

// ----------------------------------------------------------------------------
// T1-17: PK cache
// ----------------------------------------------------------------------------

func TestParsePKFromDDL(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
		want []string
	}{
		{
			name: "simple PRIMARY KEY",
			ddl:  `CREATE TABLE t (id INT, name STRING, PRIMARY KEY (id))`,
			want: []string{"id"},
		},
		{
			name: "composite PRIMARY KEY",
			ddl:  `CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))`,
			want: []string{"a", "b"},
		},
		{
			name: "backtick quoted",
			ddl:  "CREATE TABLE t (id INT, PRIMARY KEY (`id`))",
			want: []string{"id"},
		},
		{
			name: "double quoted",
			ddl:  `CREATE TABLE t ("id" INT, PRIMARY KEY ("id"))`,
			want: []string{"id"},
		},
		{
			name: "case insensitive",
			ddl:  `create table t (id int, primary key (id))`,
			want: []string{"id"},
		},
		{
			name: "no PRIMARY KEY",
			ddl:  `CREATE TABLE t (id INT)`,
			want: nil,
		},
		{
			name: "empty",
			ddl:  ``,
			want: nil,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := parsePKFromDDL(tc.ddl)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRefreshPrimaryKey_RunsOnce_T1_17(t *testing.T) {
	db := newFakeDB()
	scanCalls := 0
	db.scanFn = func(query string, _ []any, dest ...any) error {
		if strings.Contains(strings.ToUpper(query), "SHOW CREATE TABLE") {
			scanCalls++
			if len(dest) > 0 {
				if p, ok := dest[0].(*string); ok {
					*p = "CREATE TABLE t (id INT, tenant_id STRING, PRIMARY KEY (id, tenant_id))"
				}
			}
			return nil
		}
		return sql.ErrNoRows
	}

	snk := newTestSink(db)

	// First call: should hit SHOW CREATE TABLE.
	err := snk.refreshPrimaryKey(context.Background(), "t")
	require.NoError(t, err)
	require.Equal(t, 1, scanCalls)
	assert.Equal(t, []string{"id", "tenant_id"}, snk.pkCache["t"])

	// Subsequent calls must NOT re-execute SHOW CREATE TABLE.
	for i := 0; i < 5; i++ {
		err := snk.refreshPrimaryKey(context.Background(), "t")
		require.NoError(t, err)
	}
	require.Equal(t, 1, scanCalls)
	assert.Equal(t, []string{"id", "tenant_id"}, snk.pkCache["t"])
	assert.InDelta(t, 1.0, testutil.ToFloat64(SinkPKResolved.WithLabelValues(snk.name, "t")), 0.0001)
}

func TestRefreshPrimaryKey_FallbackOnError(t *testing.T) {
	db := newFakeDB()
	db.scanFn = func(query string, _ []any, _ ...any) error {
		return errors.New("table not found")
	}

	snk := newTestSink(db)
	err := snk.refreshPrimaryKey(context.Background(), "missing")
	require.Error(t, err)
	assert.Equal(t, []string{"id"}, snk.pkCache["missing"])
	assert.InDelta(t, 0.0, testutil.ToFloat64(SinkPKResolved.WithLabelValues(snk.name, "missing")), 0.0001)
}

func TestRefreshPrimaryKey_NoPrimaryKeyFallsBack(t *testing.T) {
	db := newFakeDB()
	db.scanFn = func(_ string, _ []any, dest ...any) error {
		if len(dest) > 0 {
			if p, ok := dest[0].(*string); ok {
				*p = "CREATE TABLE t (id INT, val STRING)" // no PRIMARY KEY
			}
		}
		return nil
	}

	snk := newTestSink(db)
	require.NoError(t, snk.refreshPrimaryKey(context.Background(), "no_pk"))
	assert.Equal(t, []string{"id"}, snk.pkCache["no_pk"])
	assert.InDelta(t, 0.0, testutil.ToFloat64(SinkPKResolved.WithLabelValues(snk.name, "no_pk")), 0.0001)
}

func TestBatchUpload_UsesResolvedPK_T1_17(t *testing.T) {
	db := newFakeDB()
	db.scanFn = func(query string, _ []any, dest ...any) error {
		if strings.Contains(strings.ToUpper(query), "SHOW CREATE TABLE") {
			if len(dest) > 0 {
				if p, ok := dest[0].(*string); ok {
					*p = "CREATE TABLE t (tenant_id STRING, row_id STRING, PRIMARY KEY (tenant_id, row_id))"
				}
			}
			return nil
		}
		return sql.ErrNoRows
	}

	pub := &fakeDLQPublisher{}
	snk := newTestSink(db, map[string]interface{}{
		"dlq_publisher": DLQPublisher(pub),
	})

	// Build a batch with two messages for the same table.
	messages := []protocol.Message{
		{
			SourceID: "src1",
			Table:    "t",
			Op:       protocol.OpInsert,
			Payload:  payloadJSON(map[string]any{"tenant_id": "a", "row_id": "r1", "val": "x"}),
		},
		{
			SourceID: "src1",
			Table:    "t",
			Op:       protocol.OpInsert,
			Payload:  payloadJSON(map[string]any{"tenant_id": "b", "row_id": "r2", "val": "y"}),
		},
	}

	require.NoError(t, snk.BatchUpload(context.Background(), messages))

	// QueryRowScan is invoked once (SHOW CREATE TABLE on the first batch)
	// because refreshPrimaryKey is gated by pkLoaded. We assert cache state
	// and the rendered REPLACE INTO clause directly.
	assert.Equal(t, []string{"tenant_id", "row_id"}, snk.pkCache["t"])

	// The REPLACE INTO must use the resolved PK columns.
	require.NotEmpty(t, db.execCalls)
	last := db.execCalls[len(db.execCalls)-1]
	assert.Contains(t, last, `ON ("tenant_id", "row_id")`)
}

// ----------------------------------------------------------------------------
// T1-1: deserialization failure -> DLQ
// ----------------------------------------------------------------------------

func TestBatchUpload_DeserializationFailure_DLQ_T1_1(t *testing.T) {
	t.Run("MsgPack and JSON both fail -> DLQ", func(t *testing.T) {
		db := newFakeDB()
		pub := &fakeDLQPublisher{}
		snk := newTestSink(db, map[string]interface{}{
			"dlq_publisher": DLQPublisher(pub),
		})

		// Payload that is neither valid msgpack nor valid JSON.
		badPayload := []byte{0xff, 0xfe, 0xfd, 0xfc, 0xfb}

		goodPayload := payloadJSON(map[string]any{"id": 1, "val": "ok"})

		messages := []protocol.Message{
			{SourceID: "src", Table: "t", Op: protocol.OpInsert, UUID: "u-bad", Payload: badPayload},
			{SourceID: "src", Table: "t", Op: protocol.OpInsert, UUID: "u-good", Payload: goodPayload},
		}

		before := testutil.ToFloat64(SinkDLQTotal.WithLabelValues(snk.name, "t", reasonDeserializationFailed))

		require.NoError(t, snk.BatchUpload(context.Background(), messages))

		after := testutil.ToFloat64(SinkDLQTotal.WithLabelValues(snk.name, "t", reasonDeserializationFailed))
		assert.Equal(t, before+1, after, "DLQ counter must increment by exactly 1 per failed message")

		// Exactly one DLQ publish, for the bad message.
		calls := pub.Calls()
		require.Len(t, calls, 1)
		assert.Equal(t, "u-bad", calls[0].MessageID)
		assert.Equal(t, DefaultSinkDeadLetterSubject(snk.name), calls[0].Topic)
		assert.NotEmpty(t, calls[0].Payload)
	})

	t.Run("Only JSON path fails (msgpack succeeds) -> DLQ", func(t *testing.T) {
		// If msgpack can decode the payload, the JSON fallback is not reached.
		// We assert that valid msgpack payloads do NOT trigger the DLQ.
		db := newFakeDB()
		pub := &fakeDLQPublisher{}
		snk := newTestSink(db, map[string]interface{}{
			"dlq_publisher": DLQPublisher(pub),
		})

		// Encode with msgpack directly so we know the JSON fallback will not be
		// taken even though JSON could parse it.
		payload, err := msgpackMarshal(map[string]any{"id": 1})
		require.NoError(t, err)

		messages := []protocol.Message{
			{SourceID: "src", Table: "t", Op: protocol.OpInsert, UUID: "u-ok", Payload: payload},
		}
		require.NoError(t, snk.BatchUpload(context.Background(), messages))
		assert.Empty(t, pub.Calls())
	})

	t.Run("DLQ publish failure does not abort batch", func(t *testing.T) {
		db := newFakeDB()
		pub := &fakeDLQPublisher{publishErr: errors.New("nats down")}
		snk := newTestSink(db, map[string]interface{}{
			"dlq_publisher": DLQPublisher(pub),
		})

		bad := []byte{0x00, 0x01, 0x02}
		good := payloadJSON(map[string]any{"id": 1})

		messages := []protocol.Message{
			{SourceID: "src", Table: "t", Op: protocol.OpInsert, UUID: "u-bad", Payload: bad},
			{SourceID: "src", Table: "t", Op: protocol.OpInsert, UUID: "u-good", Payload: good},
		}

		require.NoError(t, snk.BatchUpload(context.Background(), messages))
		// Counter still increments regardless of publish success.
		after := testutil.ToFloat64(SinkDLQTotal.WithLabelValues(snk.name, "t", reasonDeserializationFailed))
		assert.GreaterOrEqual(t, after, 1.0)
	})
}

func TestBatchUpload_DeserializationFailure_NoPublisher(t *testing.T) {
	db := newFakeDB()
	snk := newTestSink(db) // no DLQ publisher wired

	bad := []byte{0xff, 0xff}
	messages := []protocol.Message{
		{SourceID: "src", Table: "t", Op: protocol.OpInsert, UUID: "u1", Payload: bad},
	}

	before := testutil.ToFloat64(SinkDLQTotal.WithLabelValues(snk.name, "t", reasonDeserializationFailed))
	require.NoError(t, snk.BatchUpload(context.Background(), messages))
	after := testutil.ToFloat64(SinkDLQTotal.WithLabelValues(snk.name, "t", reasonDeserializationFailed))
	assert.Equal(t, before+1, after, "counter must increment even without a publisher")
}

// ----------------------------------------------------------------------------
// T1-1: deletes also emit DLQ on bad payload
// ----------------------------------------------------------------------------

func TestDeleteTableBatch_DeserializationFailure_DLQ(t *testing.T) {
	db := newFakeDB()
	pub := &fakeDLQPublisher{}
	snk := newTestSink(db, map[string]interface{}{
		"dlq_publisher": DLQPublisher(pub),
	})

	bad := []byte{0xde, 0xad, 0xbe, 0xef}
	messages := []protocol.Message{
		{SourceID: "src", Table: "t", Op: protocol.OpDelete, UUID: "u-bad", Payload: bad},
	}

	require.NoError(t, snk.BatchUpload(context.Background(), messages))

	calls := pub.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, "u-bad", calls[0].MessageID)
}

// ----------------------------------------------------------------------------
// Misc helpers
// ----------------------------------------------------------------------------

func TestWithOptions_AppliesValues(t *testing.T) {
	snk := newTestSink(newFakeDB(), map[string]interface{}{
		"max_placeholders":  42,
		"decimal_precision": 10,
		"decimal_scale":     2,
		"dlq_subject":       "custom.subject",
	})
	assert.Equal(t, 42, snk.maxPlaceholders)
	assert.Equal(t, 10, snk.decimalPrecision)
	assert.Equal(t, 2, snk.decimalScale)
	assert.Equal(t, "custom.subject", snk.dlqSubject)
}

func TestWithOptions_IgnoresBadValues(t *testing.T) {
	snk := newTestSink(newFakeDB())
	defaultPH := snk.maxPlaceholders
	defaultDP := snk.decimalPrecision
	defaultDS := snk.decimalScale
	defaultSubject := snk.dlqSubject

	snk.WithOptions(map[string]interface{}{
		"max_placeholders":  -1,
		"decimal_precision": 0,
		"decimal_scale":     -5,
		"dlq_subject":       "",
	})

	assert.Equal(t, defaultPH, snk.maxPlaceholders)
	assert.Equal(t, defaultDP, snk.decimalPrecision)
	assert.Equal(t, defaultDS, snk.decimalScale)
	assert.Equal(t, defaultSubject, snk.dlqSubject)
}
