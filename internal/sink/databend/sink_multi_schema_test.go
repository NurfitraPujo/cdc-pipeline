package databend

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// Test helpers: a queryFn-backed DBRows that yields canned string rows.
// ----------------------------------------------------------------------------

type stringRows struct {
	values []string
	idx    int
}

func (r *stringRows) Next() bool {
	if r.idx >= len(r.values) {
		return false
	}
	r.idx++
	return true
}

func (r *stringRows) Scan(dest ...any) error {
	if len(dest) == 0 {
		return nil
	}
	p, ok := dest[0].(*string)
	if !ok {
		return nil
	}
	*p = r.values[r.idx-1]
	return nil
}

func (r *stringRows) Close() error { return nil }
func (r *stringRows) Err() error   { return nil }

// ----------------------------------------------------------------------------
// MULTI_SCHEMA_PLAN.md §7.4 item 5: pkCache keying must match between
// ApplySchema and the upload path, and BatchUpload must not merge rows for
// same-named tables in different schemas.
// ----------------------------------------------------------------------------

// TestBatchUpload_CrossSchemaCollision_PKIsolation is the regression guard
// for the "Cross-schema collision" test called for in MULTI_SCHEMA_PLAN.md
// §5: public.orders and sales.orders share a bare table name. This drives
// the real BatchUpload -> uploadTableBatch -> ensurePrimaryKey path (not a
// recomputed key) and asserts each REPLACE INTO uses its own qualified
// table name and its own independently-resolved primary key. Reverting
// either refFromMessage's qualification, or the getCurrentColumns/
// refreshPrimaryKey schema predicate, makes the two tables' PKs (or rows)
// bleed into each other.
func TestBatchUpload_CrossSchemaCollision_PKIsolation(t *testing.T) {
	db := newFakeDB()
	db.scanFn = func(query string, _ []any, dest ...any) error {
		p, ok := dest[0].(*string)
		require.True(t, ok)
		switch {
		case strings.Contains(query, `"public"."orders"`):
			*p = `CREATE TABLE orders (id INT, PRIMARY KEY (id))`
		case strings.Contains(query, `"sales"."orders"`):
			*p = `CREATE TABLE orders (tenant_id STRING, order_id STRING, PRIMARY KEY (tenant_id, order_id))`
		default:
			t.Fatalf("unexpected SHOW CREATE TABLE query: %s", query)
		}
		return nil
	}

	snk := newTestSink(db)

	messages := []protocol.Message{
		{
			SourceID:    "src1",
			Table:       "orders",
			TableSchema: "", // normalises to "public"
			Op:          protocol.OpInsert,
			Payload:     payloadJSON(map[string]any{"id": 1}),
		},
		{
			SourceID:    "src1",
			Table:       "orders",
			TableSchema: "sales",
			Op:          protocol.OpInsert,
			Payload:     payloadJSON(map[string]any{"tenant_id": "t1", "order_id": "o1"}),
		},
	}

	require.NoError(t, snk.BatchUpload(context.Background(), messages))

	assert.Equal(t, []string{"id"}, snk.pkCache["public.orders"])
	assert.Equal(t, []string{"tenant_id", "order_id"}, snk.pkCache["sales.orders"])

	var publicQuery, salesQuery string
	for _, q := range db.execCalls {
		if strings.Contains(q, `REPLACE INTO "public"."orders"`) {
			publicQuery = q
		}
		if strings.Contains(q, `REPLACE INTO "sales"."orders"`) {
			salesQuery = q
		}
	}
	require.NotEmpty(t, publicQuery, "expected a REPLACE INTO against public.orders")
	require.NotEmpty(t, salesQuery, "expected a REPLACE INTO against sales.orders")
	assert.Contains(t, publicQuery, `ON ("id")`)
	assert.Contains(t, salesQuery, `ON ("tenant_id", "order_id")`)
}

// ----------------------------------------------------------------------------
// MULTI_SCHEMA_PLAN.md §7.4 item 1/2: auto_create_schema, both settings.
// ----------------------------------------------------------------------------

// TestApplySchema_AutoCreateSchema_DefaultsTrue_CreatesDatabase asserts the
// default (no "auto_create_schema" option set) issues CREATE DATABASE IF NOT
// EXISTS before CREATE TABLE. This is the exact path attempt 1 broke: with
// auto-provisioning default-off, the e2e suite failed on the ordinary
// *public* path because nothing had ever created database "public".
func TestApplySchema_AutoCreateSchema_DefaultsTrue_CreatesDatabase(t *testing.T) {
	db := newFakeDB()
	snk := newTestSink(db) // no auto_create_schema option -> must default true
	require.True(t, snk.autoCreateSchema)

	schema := protocol.SchemaMetadata{
		Table:   "orders",
		Schema:  "public",
		Columns: map[string]string{"id": "23"},
	}
	require.NoError(t, snk.ApplySchema(context.Background(), protocol.Message{Op: protocol.OpSchemaChange, Schema: &schema}))

	require.NotEmpty(t, db.execCalls)
	assert.Contains(t, db.execCalls[0], `CREATE DATABASE IF NOT EXISTS "public"`,
		"CREATE DATABASE must be issued before CREATE TABLE when auto_create_schema is unset/true")
	assert.Equal(t, 1, db.ExecCountMatching("CREATE DATABASE"), "must not re-issue CREATE DATABASE on a cached database")
}

// TestApplySchema_AutoCreateSchemaFalse_MissingDatabase_PermanentError is the
// regression guard for MULTI_SCHEMA_PLAN.md §7.4 item 2: when
// auto_create_schema is false and the target database does not exist,
// ApplySchema must return a permanent, actionable error and must NEVER issue
// CREATE DATABASE. Calling the real ApplySchema (not a recomputed check) --
// reverting ensureDatabase's autoCreateSchema branch to always
// auto-provision would make this fail because a CREATE DATABASE call would
// appear in db.execCalls and the error would be nil.
func TestApplySchema_AutoCreateSchemaFalse_MissingDatabase_PermanentError(t *testing.T) {
	db := newFakeDB()
	db.queryFn = func(query string, _ ...any) (DBRows, error) {
		if strings.Contains(query, "system.databases") {
			return &stringRows{}, nil // empty: database does not exist
		}
		return emptyDBRows{}, nil
	}
	snk := newTestSink(db, map[string]interface{}{"auto_create_schema": false})

	schema := protocol.SchemaMetadata{
		Table:   "orders",
		Schema:  "public",
		Columns: map[string]string{"id": "23"},
	}
	err := snk.ApplySchema(context.Background(), protocol.Message{Op: protocol.OpSchemaChange, Schema: &schema})

	require.Error(t, err)
	assert.True(t, IsPermanentDDLError(err), "a missing database with auto-provisioning off must be a permanent error, not a retryable one")
	assert.Contains(t, err.Error(), "auto_create_schema")
	assert.Equal(t, 0, db.ExecCountMatching("CREATE DATABASE"), "must never auto-provision when auto_create_schema is false")
	assert.Equal(t, 0, db.ExecCountMatching("CREATE TABLE"), "must not attempt CREATE TABLE against a database confirmed missing")
}

// TestApplySchema_AutoCreateSchemaFalse_ExistingDatabase_Succeeds is the
// positive case for the same setting: when the database already exists,
// ApplySchema must succeed without ever issuing CREATE DATABASE.
func TestApplySchema_AutoCreateSchemaFalse_ExistingDatabase_Succeeds(t *testing.T) {
	db := newFakeDB()
	db.queryFn = func(query string, _ ...any) (DBRows, error) {
		if strings.Contains(query, "system.databases") {
			return &stringRows{values: []string{"public"}}, nil
		}
		return emptyDBRows{}, nil
	}
	snk := newTestSink(db, map[string]interface{}{"auto_create_schema": false})

	schema := protocol.SchemaMetadata{
		Table:   "orders",
		Schema:  "public",
		Columns: map[string]string{"id": "23"},
	}
	require.NoError(t, snk.ApplySchema(context.Background(), protocol.Message{Op: protocol.OpSchemaChange, Schema: &schema}))
	assert.Equal(t, 0, db.ExecCountMatching("CREATE DATABASE"))
	assert.Equal(t, 1, db.ExecCountMatching("CREATE TABLE"))
}

// ----------------------------------------------------------------------------
// MULTI_SCHEMA_PLAN.md §7.4 item 5: ApplySchema must not set pkLoaded when
// PKColumns is empty.
// ----------------------------------------------------------------------------

// TestApplySchema_EmptyPKColumns_DoesNotStickPKLoaded is the regression
// guard for the door attempt 1 reintroduced the sticky-wrong-PK bug through:
// a schema-change event reconstructed from a SchemaDiff has PKColumns ==
// nil. If ApplySchema still marked pkLoaded for that table, the later
// upload-path SHOW CREATE TABLE lookup (refreshPrimaryKey, gated by
// pkLoaded) would never run, and every REPLACE INTO would use the ["id"]
// fallback forever, even for a composite-PK table. This drives the real
// ApplySchema and then the real BatchUpload (which internally calls
// ensurePrimaryKey/refreshPrimaryKey) and asserts SHOW CREATE TABLE *was*
// invoked and the composite PK was picked up.
func TestApplySchema_EmptyPKColumns_DoesNotStickPKLoaded(t *testing.T) {
	db := newFakeDB()
	showCreateCalls := 0
	db.scanFn = func(query string, _ []any, dest ...any) error {
		if strings.Contains(query, "SHOW CREATE TABLE") {
			showCreateCalls++
			p, _ := dest[0].(*string)
			*p = `CREATE TABLE orders (tenant_id STRING, order_id STRING, PRIMARY KEY (tenant_id, order_id))`
			return nil
		}
		return sql.ErrNoRows
	}
	snk := newTestSink(db)

	// Schema-change event with no PK info, as consumer.go's diff-reconstructed
	// SchemaMetadata produces (MULTI_SCHEMA_PLAN.md §7.4 item 5).
	schema := protocol.SchemaMetadata{
		Table:     "orders",
		Schema:    "public",
		Columns:   map[string]string{"tenant_id": "25", "order_id": "25"},
		PKColumns: nil,
	}
	require.NoError(t, snk.ApplySchema(context.Background(), protocol.Message{Op: protocol.OpSchemaChange, Schema: &schema}))

	// ApplySchema must not have marked pkLoaded for this table.
	snk.pkMu.RLock()
	_, loaded := snk.pkLoaded["public.orders"]
	snk.pkMu.RUnlock()
	assert.False(t, loaded, "ApplySchema must not set pkLoaded when PKColumns is empty")

	// Now the upload path must still be able to resolve the real PK.
	messages := []protocol.Message{
		{
			SourceID: "src1",
			Table:    "orders",
			Op:       protocol.OpInsert,
			Payload:  payloadJSON(map[string]any{"tenant_id": "t1", "order_id": "o1"}),
		},
	}
	require.NoError(t, snk.BatchUpload(context.Background(), messages))

	assert.Equal(t, 1, showCreateCalls, "SHOW CREATE TABLE must have run because pkLoaded was not stuck")
	assert.Equal(t, []string{"tenant_id", "order_id"}, snk.pkCache["public.orders"])
}

// ----------------------------------------------------------------------------
// MULTI_SCHEMA_PLAN.md §7.4 item 6: swallowed ALTER failure.
// ----------------------------------------------------------------------------

// TestApplySchema_ALTERFailure_ReturnsError is the regression guard: the
// previous version logged a warning on ALTER failure and returned nil, so
// ApplySchema reported success while the missing column caused every
// subsequent write to fail forever with no visible signal at the schema-sync
// call site. Calling the real ApplySchema against a table that already has
// columns (forcing the ALTER branch, not the CREATE TABLE branch).
func TestApplySchema_ALTERFailure_ReturnsError(t *testing.T) {
	db := newFakeDB()
	db.queryFn = func(query string, _ ...any) (DBRows, error) {
		if strings.Contains(query, "information_schema.columns") {
			// Table already has "id" -- so the new "name" column goes
			// through ALTER TABLE, not CREATE TABLE.
			return &stringRows{values: []string{"id"}}, nil
		}
		return emptyDBRows{}, nil
	}
	db.execErrFn = func(query string) error {
		if strings.Contains(query, "ALTER TABLE") {
			return assertErr("1003 unknown database")
		}
		return nil
	}

	snk := newTestSink(db)
	schema := protocol.SchemaMetadata{
		Table:   "orders",
		Schema:  "public",
		Columns: map[string]string{"id": "23", "name": "25"},
	}
	err := snk.ApplySchema(context.Background(), protocol.Message{Op: protocol.OpSchemaChange, Schema: &schema})

	require.Error(t, err, "a failed ALTER TABLE must propagate as an error, not be swallowed")
	assert.True(t, IsPermanentDDLError(err))
}

// assertErr is a trivial error type so classifyDDLError has real error text
// to classify against.
type assertErr string

func (e assertErr) Error() string { return string(e) }

// ----------------------------------------------------------------------------
// MULTI_SCHEMA_PLAN.md §7.4 item 6 audit: deleteTableBatch's silent
// "continue" when no PK column is present in the payload.
// ----------------------------------------------------------------------------

// TestDeleteTableBatch_MissingPKColumns_DLQ is the regression guard: the
// previous version silently dropped a delete when none of the resolved PK
// columns were present in the decoded payload. Calling the real
// BatchUpload/deleteTableBatch and asserting the DLQ publisher saw the
// event and no DELETE statement was ever issued.
func TestDeleteTableBatch_MissingPKColumns_DLQ(t *testing.T) {
	db := newFakeDB()
	pub := &fakeDLQPublisher{}
	snk := newTestSink(db, map[string]interface{}{
		"dlq_publisher": DLQPublisher(pub),
	})

	// Payload decodes fine but does not contain "id" (the fallback PK, since
	// no SHOW CREATE TABLE result was primed).
	messages := []protocol.Message{
		{SourceID: "src", Table: "t", Op: protocol.OpDelete, UUID: "u-missing-pk", Payload: payloadJSON(map[string]any{"unrelated_col": "x"})},
	}

	require.NoError(t, snk.BatchUpload(context.Background(), messages))

	calls := pub.Calls()
	require.Len(t, calls, 1, "a delete with no resolvable PK column must be dead-lettered, not silently dropped")
	assert.Equal(t, "u-missing-pk", calls[0].MessageID)
	assert.Equal(t, 0, db.ExecCountMatching("DELETE FROM"), "no DELETE should be issued when no PK column is present")

	after := testutil.ToFloat64(SinkDLQTotal.WithLabelValues(snk.name, "public.t", reasonMissingPKColumns))
	assert.GreaterOrEqual(t, after, 1.0)
}
