package databend

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// WS-4.6: primary key durability across a sink process restart.
//
// The acceptance criterion (plans/cdc_custom_object_transform_remediation.md
// WS-4) is explicit: a test that only exercises the in-memory path (pkCache)
// does not meet it. persistentFakeDB below is a DBExec that survives being
// handed to a *second*, freshly-constructed DatabendSink -- exactly what
// simulates a restart, since pkCache/pkLoaded are process-local maps that a
// real restart wipes, but anything actually written to the (fake) Databend
// cluster persists. The real bug this guards is sink.go's pre-WS-4.6
// fallback: after a restart, refreshPrimaryKey's SHOW CREATE TABLE finds no
// PRIMARY KEY (Databend's CREATE TABLE never emits one) and both write paths
// silently used pks = []string{"id"} -- wrong for a sidecar keyed on
// record_id, causing REPLACE INTO ON ("id") to insert a duplicate row per
// update instead of replacing the existing one.
//
// This fake also models real Databend's column set per table (populated from
// CREATE TABLE / ALTER TABLE ADD COLUMN) and rejects an UPDATE that
// references an unknown column with an "unknown column" error -- this is
// what makes the round-5-review HIGH finding ("soft-delete UPDATE hard-fails
// on any table whose Databend column set has no deleted_at") reproducible
// and, after the fix, provably closed: ApplySchema must synthesize
// deleted_at on every synced table regardless of whether the source schema
// declares it.
// ----------------------------------------------------------------------------

// persistentFakeDB is a DBExec whose state -- unlike fakeDB in
// sink_remediation_test.go -- represents the *server-side* Databend cluster,
// not a single sink instance's view of it. Handing the same *persistentFakeDB
// to two separate DatabendSink values is the restart simulation: the second
// sink starts with empty pkCache/pkLoaded (a fresh process) but reads back
// whatever the first sink durably wrote (cdc_meta.pk_columns, and the data
// table's rows), exactly as a real restarted process would reconnect to the
// same Databend cluster.
type persistentFakeDB struct {
	mu sync.Mutex

	pkMeta map[string]string // table_ref -> comma-joined pk columns

	// columns: qualified table name -> lowercased column name -> present.
	// Populated by CREATE TABLE / ALTER TABLE ADD COLUMN, exactly like a
	// real Databend information_schema would reflect. An UPDATE/REPLACE
	// referencing a column not in this set fails with an "unknown column"
	// error, the same way real Databend would.
	columns map[string]map[string]bool

	// rows: qualified table name -> pk value -> column name -> value.
	rows map[string]map[string]map[string]any
	// deletedAt: qualified table name -> pk value -> deleted_at value, present
	// only once a soft delete has landed for that key.
	deletedAt map[string]map[string]any

	// updateStatements counts UPDATE ... SET deleted_at executions, so tests
	// can assert batching actually reduced statement count (WS-4 item 3).
	updateStatements int

	execCalls []string
}

func newPersistentFakeDB() *persistentFakeDB {
	return &persistentFakeDB{
		pkMeta:    make(map[string]string),
		columns:   make(map[string]map[string]bool),
		rows:      make(map[string]map[string]map[string]any),
		deletedAt: make(map[string]map[string]any),
	}
}

var (
	rePKMetaUpsert   = regexp.MustCompile(`(?is)REPLACE INTO\s+"cdc_meta"\."pk_columns".*VALUES`)
	rePKMetaSelect   = regexp.MustCompile(`(?is)SELECT pk_columns FROM\s+"cdc_meta"\."pk_columns"`)
	reReplaceInto    = regexp.MustCompile(`(?is)^REPLACE INTO\s+(\S+)\s+\(([^)]+)\)\s+ON\s+\(([^)]+)\)\s+VALUES`)
	reCreateTable    = regexp.MustCompile(`(?is)^CREATE TABLE IF NOT EXISTS\s+(\S+)\s+\((.+)\)\s*$`)
	reAlterAddColumn = regexp.MustCompile(`(?is)^ALTER TABLE\s+(\S+)\s+ADD COLUMN\s+"([^"]+)"`)
	reUpdateSet      = regexp.MustCompile(`(?is)^UPDATE\s+(\S+)\s+SET\s+"deleted_at"\s*=\s*\?\s+WHERE\s+(.+)$`)
	reQuoted         = regexp.MustCompile(`"([^"]+)"`)
)

func unquoteList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, `"`)
		out = append(out, p)
	}
	return out
}

func normalizeFakeTableName(raw string) string {
	table := strings.Trim(raw, `"`)
	return strings.ReplaceAll(table, `"."`, ".")
}

func (f *persistentFakeDB) addColumn(table, name string) {
	if f.columns[table] == nil {
		f.columns[table] = make(map[string]bool)
	}
	f.columns[table][strings.ToLower(name)] = true
}

func (f *persistentFakeDB) hasColumn(table, name string) bool {
	return f.columns[table] != nil && f.columns[table][strings.ToLower(name)]
}

//nolint:gocyclo // test fake dispatches on SQL statement shape (CREATE/ALTER/INSERT/UPDATE/DELETE variants); splitting would obscure the dispatch and isn't worth it for test-only code.
func (f *persistentFakeDB) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execCalls = append(f.execCalls, query)

	trimmed := strings.TrimSpace(query)

	switch {
	case strings.HasPrefix(strings.ToUpper(trimmed), "CREATE DATABASE"):
		return fakeResult{}, nil
	case strings.HasPrefix(strings.ToUpper(trimmed), "CREATE TABLE"):
		if m := reCreateTable.FindStringSubmatch(trimmed); m != nil {
			table := normalizeFakeTableName(m[1])
			// Column defs are "name TYPE" pairs separated by top-level
			// commas; none of this test's types contain a comma or paren,
			// so a plain split is safe.
			for _, def := range strings.Split(m[2], ",") {
				def = strings.TrimSpace(def)
				fields := reQuoted.FindStringSubmatch(def)
				if len(fields) == 2 {
					f.addColumn(table, fields[1])
				}
			}
		}
		return fakeResult{}, nil
	case strings.HasPrefix(strings.ToUpper(trimmed), "ALTER TABLE"):
		if m := reAlterAddColumn.FindStringSubmatch(trimmed); m != nil {
			table := normalizeFakeTableName(m[1])
			// Round-5 review LOW: real Databend has no ADD COLUMN IF NOT
			// EXISTS and rejects adding a column that already exists.
			// Modeling that here is what makes the double-ALTER regression
			// (round-5 MEDIUM) actually catchable by a test instead of
			// silently succeeding twice.
			if f.hasColumn(table, m[2]) {
				// Wording verified live against datafuselabs/databend:latest
				// (round-5 review caveat): real error code 1108 says
				// "add column X already exist".
				return nil, fmt.Errorf("add column %s already exist", m[2])
			}
			f.addColumn(table, m[2])
		}
		return fakeResult{}, nil
	case rePKMetaUpsert.MatchString(trimmed):
		if len(args) != 2 {
			return nil, fmt.Errorf("unexpected pk_columns REPLACE INTO arg count: %d", len(args))
		}
		tableRef, _ := args[0].(string)
		pkCols, _ := args[1].(string)
		f.pkMeta[tableRef] = pkCols
		return fakeResult{}, nil
	case strings.HasPrefix(strings.ToUpper(trimmed), "REPLACE INTO"):
		m := reReplaceInto.FindStringSubmatch(trimmed)
		if m == nil {
			return nil, fmt.Errorf("persistentFakeDB: could not parse REPLACE INTO: %s", trimmed)
		}
		table := normalizeFakeTableName(m[1])
		cols := unquoteList(m[2])
		pkCols := unquoteList(m[3])

		if len(pkCols) != 1 {
			return nil, fmt.Errorf("persistentFakeDB only supports single-column PKs for REPLACE INTO in this test, got %v", pkCols)
		}
		pkIdx := -1
		for i, c := range cols {
			if c == pkCols[0] {
				pkIdx = i
				break
			}
		}
		if pkIdx < 0 {
			return nil, fmt.Errorf("persistentFakeDB: pk column %q not found in column list %v", pkCols[0], cols)
		}

		if len(args)%len(cols) != 0 {
			return nil, fmt.Errorf("persistentFakeDB: arg count %d not a multiple of column count %d", len(args), len(cols))
		}
		if f.rows[table] == nil {
			f.rows[table] = make(map[string]map[string]any)
		}
		for start := 0; start < len(args); start += len(cols) {
			rowArgs := args[start : start+len(cols)]
			pkVal := fmt.Sprintf("%v", rowArgs[pkIdx])
			row := make(map[string]any, len(cols))
			for i, c := range cols {
				row[c] = rowArgs[i]
			}
			// REPLACE INTO ... ON (pk) semantics: this call, keyed correctly,
			// overwrites the existing row for that PK rather than appending
			// a duplicate. This is exactly the property that breaks when the
			// caller resolves the wrong PK column (e.g. "id" instead of
			// "record_id") -- two logically-identical updates land under two
			// different fake keys instead of one.
			f.rows[table][pkVal] = row

			// Round-5 review MEDIUM ("a later upsert resurrects a
			// soft-deleted row"): real Databend's REPLACE INTO fully
			// replaces the matched row, defaulting any column absent from
			// the statement's column list -- including deleted_at -- to
			// NULL. Model that precisely: if this REPLACE INTO's column
			// list includes deleted_at, it is authoritative for the
			// tombstone map; if it does not, the tombstone is wiped, the
			// same way real Databend would silently null it. This is what
			// makes the "upsert resurrects a soft-deleted row" bug
			// reproducible here at all -- without modeling the wipe, no
			// fake assertion could ever distinguish "the sink correctly
			// preserved the tombstone" from "the fake just never touched
			// it".
			hasDeletedAtCol := false
			for _, c := range cols {
				if c == deletedAtColumn {
					hasDeletedAtCol = true
					break
				}
			}
			if f.deletedAt[table] == nil {
				f.deletedAt[table] = make(map[string]any)
			}
			if hasDeletedAtCol {
				f.deletedAt[table][pkVal] = row[deletedAtColumn]
			} else {
				delete(f.deletedAt[table], pkVal)
			}
		}
		return fakeResult{}, nil
	case reUpdateSet.MatchString(trimmed):
		m := reUpdateSet.FindStringSubmatch(trimmed)
		table := normalizeFakeTableName(m[1])
		whereClause := m[2]

		// Round-5 review HIGH finding: real Databend rejects an UPDATE
		// referencing a column the table does not have. Reproduce that
		// here instead of unconditionally accepting the write -- without
		// this check, the fake cannot distinguish "sink correctly
		// synthesized deleted_at" from "sink is about to hard-fail every
		// delete against this table forever".
		//
		// Round-5c review LOW: use the exact wording verified live against
		// datafuselabs/databend:latest (code 1006), not a synthetic
		// approximation -- the earlier "unknown column ..." text happened to
		// match the "unknown column" marker too, so
		// TestWS4_UnknownDeletedAtColumn_IsClassifiedPermanent was passing
		// on an old marker rather than genuinely exercising the
		// "does not have a column with name" one that real Databend emits.
		if !f.hasColumn(table, deletedAtColumn) {
			return nil, fmt.Errorf("Table %q does not have a column with name %q", table, deletedAtColumn)
		}

		if f.deletedAt[table] == nil {
			f.deletedAt[table] = make(map[string]any)
		}
		deletedAtVal := args[0]
		f.updateStatements++

		if strings.Contains(whereClause, " IN (") {
			// Single-column IN form: "pk" IN (?, ?, ...).
			colMatch := reQuoted.FindStringSubmatch(whereClause)
			if colMatch == nil {
				return nil, fmt.Errorf("persistentFakeDB: could not find IN column in %q", whereClause)
			}
			values := args[1:]
			for _, v := range values {
				pkVal := fmt.Sprintf("%v", v)
				f.deletedAt[table][pkVal] = deletedAtVal
			}
			return fakeResult{}, nil
		}

		// Composite OR-of-AND form: ("pk1" = ? AND "pk2" = ?) OR (...).
		groups := strings.Split(whereClause, ") OR (")
		var pkCols []string
		argIdx := 1
		for gi, g := range groups {
			g = strings.TrimPrefix(g, "(")
			g = strings.TrimSuffix(g, ")")
			clauses := strings.Split(g, " AND ")
			if gi == 0 {
				for _, c := range clauses {
					cm := reQuoted.FindStringSubmatch(c)
					if cm == nil {
						return nil, fmt.Errorf("persistentFakeDB: could not parse AND clause %q", c)
					}
					pkCols = append(pkCols, cm[1])
				}
			}
			if argIdx+len(pkCols) > len(args) {
				return nil, fmt.Errorf("persistentFakeDB: not enough args for composite WHERE group %d", gi)
			}
			vals := args[argIdx : argIdx+len(pkCols)]
			argIdx += len(pkCols)
			parts := make([]string, len(vals))
			for i, v := range vals {
				parts[i] = fmt.Sprintf("%v", v)
			}
			pkVal := strings.Join(parts, "|")
			f.deletedAt[table][pkVal] = deletedAtVal
		}
		return fakeResult{}, nil
	default:
		return fakeResult{}, nil
	}
}

// reSelectPKAndDeletedAt (name kept for the round-5/WS-4 history in
// comments elsewhere in this file) now captures the full SELECT list, not
// just "we know it's pk-columns-then-deleted_at" -- WS-7's
// fetchCurrentColumns issues the same shape of query but with an arbitrary
// extra-column list, not just deleted_at, so QueryContext below derives
// which trailing columns were requested from the select list itself
// (unquoteList) rather than assuming deleted_at.
var reSelectPKAndDeletedAt = regexp.MustCompile(`(?is)^SELECT\s+(.+?)\s+FROM\s+(\S+)\s+WHERE\s+(.+)$`)

// parseFakeWhereTuples parses the same two WHERE-clause shapes the sink
// itself emits (single-column "pk" IN (?, ...), or composite
// ("pk1" = ? AND "pk2" = ?) OR (...)) against a flat args slice, returning
// the pk column names (in order) and one value-tuple per matched row. Used
// by both QueryContext (SELECT ... for fetchCurrentDeletedAt) and could be
// reused by ExecContext's UPDATE handling, but that handling predates this
// helper and is left as-is to avoid touching already-verified logic.
func parseFakeWhereTuples(whereClause string, args []any) (pkCols []string, tuples [][]any, err error) {
	if strings.Contains(whereClause, " IN (") {
		colMatch := reQuoted.FindStringSubmatch(whereClause)
		if colMatch == nil {
			return nil, nil, fmt.Errorf("could not find IN column in %q", whereClause)
		}
		pkCols = []string{colMatch[1]}
		tuples = make([][]any, len(args))
		for i, v := range args {
			tuples[i] = []any{v}
		}
		return pkCols, tuples, nil
	}

	groups := strings.Split(whereClause, ") OR (")
	argIdx := 0
	for gi, g := range groups {
		g = strings.TrimPrefix(g, "(")
		g = strings.TrimSuffix(g, ")")
		clauses := strings.Split(g, " AND ")
		if gi == 0 {
			for _, c := range clauses {
				cm := reQuoted.FindStringSubmatch(c)
				if cm == nil {
					return nil, nil, fmt.Errorf("could not parse AND clause %q", c)
				}
				pkCols = append(pkCols, cm[1])
			}
		}
		if argIdx+len(pkCols) > len(args) {
			return nil, nil, fmt.Errorf("not enough args for WHERE group %d", gi)
		}
		tuple := append([]any{}, args[argIdx:argIdx+len(pkCols)]...)
		tuples = append(tuples, tuple)
		argIdx += len(pkCols)
	}
	return pkCols, tuples, nil
}

// fakeGenericRows is a minimal DBRows over a static [][]any result set,
// scanned into whatever destination pointers the caller provides (the sink
// production code scans into *any slots via fetchCurrentDeletedAt).
type fakeGenericRows struct {
	rows [][]any
	i    int
}

func (r *fakeGenericRows) Next() bool { return r.i < len(r.rows) }
func (r *fakeGenericRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	if len(dest) != len(row) {
		return fmt.Errorf("fakeGenericRows: dest count %d != row width %d", len(dest), len(row))
	}
	for i, d := range dest {
		p, ok := d.(*any)
		if !ok {
			return fmt.Errorf("fakeGenericRows: dest[%d] is not *any", i)
		}
		*p = row[i]
	}
	r.i++
	return nil
}
func (r *fakeGenericRows) Close() error { return nil }
func (r *fakeGenericRows) Err() error   { return nil }

// QueryContext serves two query shapes: getCurrentColumns' information_schema
// probe (always empty in this fake -- ApplySchema's CREATE-vs-ALTER branch
// selection for these tests is exercised via the alterPathFakeDB wrapper
// below instead) and fetchCurrentDeletedAt's per-table SELECT, which needs
// real data back so the round-5 "upsert resurrects a soft-deleted row" fix
// is genuinely testable.
func (f *persistentFakeDB) QueryContext(_ context.Context, query string, args ...any) (DBRows, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	trimmed := strings.TrimSpace(query)
	if strings.Contains(strings.ToUpper(trimmed), "INFORMATION_SCHEMA.COLUMNS") {
		return emptyDBRows{}, nil
	}

	m := reSelectPKAndDeletedAt.FindStringSubmatch(trimmed)
	if m == nil {
		return emptyDBRows{}, nil
	}
	selectCols := unquoteList(m[1])
	table := normalizeFakeTableName(m[2])
	whereClause := m[3]

	pkCols, tuples, err := parseFakeWhereTuples(whereClause, args)
	if err != nil {
		return nil, err
	}

	// The select list is "pk cols..., extra cols..." in that order (both
	// fetchCurrentDeletedAt and WS-7's fetchCurrentColumns build it this
	// way) -- everything after the pk columns is the arbitrary extra
	// column set being read back (historically just deleted_at; now
	// potentially any TOAST-preserved column too).
	extraCols := selectCols
	if len(pkCols) <= len(selectCols) {
		extraCols = selectCols[len(pkCols):]
	}

	var out [][]any
	for _, tuple := range tuples {
		key := pkTupleKey(tuple)
		row, ok := f.rows[table][key]
		if !ok {
			// No row for this pk yet (a genuine first insert) -- matches
			// real SQL semantics: SELECT ... WHERE pk = <nonexistent>
			// returns zero rows, not a NULL row.
			continue
		}
		outRow := make([]any, 0, len(tuple)+len(extraCols))
		outRow = append(outRow, tuple...)
		for _, c := range extraCols {
			if c == deletedAtColumn {
				outRow = append(outRow, f.deletedAt[table][key])
			} else {
				outRow = append(outRow, row[c])
			}
		}
		out = append(out, outRow)
	}
	return &fakeGenericRows{rows: out}, nil
}

func (f *persistentFakeDB) QueryRowScan(_ context.Context, query string, args []any, dest ...any) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	trimmed := strings.TrimSpace(query)
	if rePKMetaSelect.MatchString(trimmed) {
		if len(args) != 1 {
			return fmt.Errorf("unexpected pk_columns SELECT arg count: %d", len(args))
		}
		tableRef, _ := args[0].(string)
		pkCols, ok := f.pkMeta[tableRef]
		if !ok {
			return sql.ErrNoRows
		}
		strPtr, ok := dest[0].(*string)
		if !ok {
			return fmt.Errorf("expected *string dest")
		}
		*strPtr = pkCols
		return nil
	}
	// SHOW CREATE TABLE fallback: the real Databend sink never emits a
	// PRIMARY KEY clause in its CREATE TABLE DDL (sink.go's ApplySchema), so
	// a real post-restart SHOW CREATE TABLE genuinely finds nothing. Model
	// that faithfully instead of handing the test an easy PK for free.
	return sql.ErrNoRows
}

func (f *persistentFakeDB) Close() error { return nil }

func (f *persistentFakeDB) rowCount(table string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows[table])
}

func (f *persistentFakeDB) rowByPK(table, pk string) (map[string]any, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rows[table][pk]
	return r, ok
}

// deletedAtFor returns the current deleted_at value for pk in table, and
// whether it is actually set. A present-but-nil map entry (a row whose
// column list included deleted_at with a nil/NULL value -- e.g. a fresh
// insert, or an upsert that correctly preserved "no tombstone") counts as
// NOT deleted, distinct from "no entry recorded yet at all"; callers only
// care about the semantic distinction, not the map-presence mechanics.
func (f *persistentFakeDB) deletedAtFor(table, pk string) (any, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.deletedAt[table][pk]
	return v, ok && v != nil
}

func (f *persistentFakeDB) updateStatementCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updateStatements
}

// newRestartSink builds a DatabendSink as a fresh process would: empty
// pkCache/pkLoaded/provisionedDB, wired to the given (possibly
// already-populated) DBExec. Two calls with the same persistentFakeDB
// simulate "before restart" / "after restart".
func newRestartSink(db DBExec) *DatabendSink {
	return newTestSink(db)
}

func TestWS4_6_PKDurability_SurvivesRestart_Sidecar(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()

	sidecarRef := protocol.TableRef{Schema: "custom_objects", Table: "_42_7_master_contacts"}
	qualified := sidecarRef.String()

	// --- Process instance #1: schema_change declares record_id as the PK,
	// then an initial row lands. ---
	sink1 := newRestartSink(db)
	err := sink1.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:  sidecarRef.Table,
			Schema: sidecarRef.Schema,
			Columns: map[string]string{
				"record_id": "int8",
				"name":      "text",
			},
			PKColumns: []string{"record_id"},
		},
	})
	require.NoError(t, err)

	err = sink1.BatchUpload(ctx, []protocol.Message{{
		Op:          protocol.OpInsert,
		Table:       sidecarRef.Table,
		TableSchema: sidecarRef.Schema,
		UUID:        "row-1-insert",
		Data:        map[string]any{"record_id": int64(42), "name": "first"},
	}})
	require.NoError(t, err)

	require.Equal(t, 1, db.rowCount(qualified), "exactly one row after the initial insert")
	row, ok := db.rowByPK(qualified, "42")
	require.True(t, ok)
	assert.Equal(t, "first", row["name"])

	// --- Simulate a restart: a brand-new DatabendSink, with empty
	// pkCache/pkLoaded, reconnects to the same (fake) Databend cluster. No
	// schema_change is replayed -- the PK must be recovered from durable
	// storage alone, not from an in-memory cache and not from SHOW CREATE
	// TABLE (which this fake correctly reports as empty, matching real
	// Databend). ---
	sink2 := newRestartSink(db)

	err = sink2.BatchUpload(ctx, []protocol.Message{{
		Op:          protocol.OpUpdate,
		Table:       sidecarRef.Table,
		TableSchema: sidecarRef.Schema,
		UUID:        "row-1-update",
		Data:        map[string]any{"record_id": int64(42), "name": "second"},
	}})
	require.NoError(t, err, "post-restart upload must succeed once the durable PK is resolved")

	// The critical assertion: still exactly ONE row for record_id=42, not
	// two. Pre-WS-4.6, this would fall back to pks=["id"], REPLACE INTO ON
	// ("id") would key on an "id" value that does not exist in this row's
	// data (record_id is the PK, "id" is absent), and the row would either
	// be silently dropped from the ON clause matching or duplicated under a
	// different fake key -- either way wrong. Post-fix, the row must be
	// found and replaced under the correct key.
	require.Equal(t, 1, db.rowCount(qualified), "post-restart update must replace the existing row, not duplicate it")
	row, ok = db.rowByPK(qualified, "42")
	require.True(t, ok)
	assert.Equal(t, "second", row["name"], "the row content must reflect the post-restart update")

	// A second post-restart write (a fresh record) also resolves correctly,
	// proving this isn't a one-shot accident of the first lazy resolution.
	err = sink2.BatchUpload(ctx, []protocol.Message{{
		Op:          protocol.OpInsert,
		Table:       sidecarRef.Table,
		TableSchema: sidecarRef.Schema,
		UUID:        "row-2-insert",
		Data:        map[string]any{"record_id": int64(43), "name": "third"},
	}})
	require.NoError(t, err)
	require.Equal(t, 2, db.rowCount(qualified))

	// --- Restart again (third process instance), this time exercise the
	// delete path -- deleteTableBatch has its own independent pks lookup and
	// its own pre-WS-4.6 ["id"] fallback (sink.go:960-962 in the plan). ---
	sink3 := newRestartSink(db)
	deleteTS := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	err = sink3.BatchUpload(ctx, []protocol.Message{{
		Op:          protocol.OpDelete,
		Table:       sidecarRef.Table,
		TableSchema: sidecarRef.Schema,
		UUID:        "row-1-delete",
		Timestamp:   deleteTS,
		Data:        map[string]any{"record_id": int64(42)},
	}})
	require.NoError(t, err)

	deletedAt, ok := db.deletedAtFor(qualified, "42")
	require.True(t, ok, "the correct row (record_id=42) must be soft-deleted")
	assert.Equal(t, deleteTS, deletedAt)
	_, otherDeleted := db.deletedAtFor(qualified, "43")
	assert.False(t, otherDeleted, "the other row must not be affected")

	// WS-4 (soft delete everywhere): the row itself must still exist with
	// all columns intact -- this is not a hard delete.
	require.Equal(t, 2, db.rowCount(qualified), "soft delete must not remove the row")
	row, ok = db.rowByPK(qualified, "42")
	require.True(t, ok)
	assert.Equal(t, "second", row["name"], "soft-deleted row keeps its data")
}

// TestWS4_6_MissingDurablePK_CustomObjects_IsHardError exercises "the
// configuration that does NOT use the feature": a custom_objects table for
// which no schema_change with PKColumns has ever been applied (so
// cdc_meta.pk_columns has no entry) and SHOW CREATE TABLE finds nothing
// either -- the exact pre-WS-4.6 condition that used to silently fall back
// to pks=["id"] and corrupt a sidecar. Post-fix this must be a hard, loud
// error instead, per the plan's "make a missing PK for a custom_objects
// table a hard error on both paths, not a warning".
func TestWS4_6_MissingDurablePK_CustomObjects_IsHardError(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "custom_objects", Table: "_9_1_never_applied"}

	uploadErr := snk.BatchUpload(ctx, []protocol.Message{{
		Op:          protocol.OpInsert,
		Table:       ref.Table,
		TableSchema: ref.Schema,
		UUID:        "u1",
		Data:        map[string]any{"record_id": int64(1), "name": "x"},
	}})
	require.Error(t, uploadErr, "upload against a custom_objects table with no resolvable PK must fail loudly, not default to [\"id\"]")
	assert.Contains(t, uploadErr.Error(), "no primary key resolved")
	require.Equal(t, 0, db.rowCount(ref.String()), "nothing should have been written under a wrong PK")

	deleteErr := snk.BatchUpload(ctx, []protocol.Message{{
		Op:          protocol.OpDelete,
		Table:       ref.Table,
		TableSchema: ref.Schema,
		UUID:        "u2",
		Data:        map[string]any{"record_id": int64(1)},
	}})
	require.Error(t, deleteErr, "delete against a custom_objects table with no resolvable PK must also fail loudly")
	assert.Contains(t, deleteErr.Error(), "no primary key resolved")
}

// TestWS4_6_NonCustomObjectsTable_StillDefaultsToID is the "does NOT use the
// feature" companion in the other direction: a direct-class table (public
// schema, e.g. master_contacts) that has never had PKColumns recorded still
// falls back to the legacy ["id"] default rather than erroring -- the hard
// error is scoped to custom_objects specifically, where "id" is known to be
// wrong for a sidecar, not a blanket behavior change for every table class.
func TestWS4_6_NonCustomObjectsTable_StillDefaultsToID(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "master_contacts"}

	err := snk.BatchUpload(ctx, []protocol.Message{{
		Op:          protocol.OpInsert,
		Table:       ref.Table,
		TableSchema: ref.Schema,
		UUID:        "u1",
		Data:        map[string]any{"id": int64(1), "name": "acme"},
	}})
	require.NoError(t, err)
	require.Equal(t, 1, db.rowCount(ref.String()))
	row, ok := db.rowByPK(ref.String(), "1")
	require.True(t, ok)
	assert.Equal(t, "acme", row["name"])
}

// TestWS4_6_PersistPKMetadata_IsAuthoritative_OverShowCreateTable pins the
// "make the declared PkColumns authoritative -- never let SHOW CREATE TABLE
// or the ["id"] default override it" requirement directly: even though this
// fake's SHOW CREATE TABLE always reports no PK (matching real Databend),
// a value durably recorded via ApplySchema's PKColumns must win.
func TestWS4_6_PersistPKMetadata_IsAuthoritative_OverShowCreateTable(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	sink1 := newRestartSink(db)

	ref := protocol.TableRef{Schema: "custom_objects", Table: "_1_1_business_entities"}
	err := sink1.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:     ref.Table,
			Schema:    ref.Schema,
			Columns:   map[string]string{"record_id": "int8"},
			PKColumns: []string{"record_id"},
		},
	})
	require.NoError(t, err)

	pks, found, err := sink1.loadPKMetadata(ctx, ref)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, []string{"record_id"}, pks)
}

// ----------------------------------------------------------------------------
// Round-5 review: soft-delete against a table with no deleted_at at the
// source, batched delete statements, and the DDL-error classification
// backstop.
// ----------------------------------------------------------------------------

// TestWS4_ApplySchema_SynthesizesDeletedAt_ForSourceTableWithout it covers
// the HIGH finding directly: business_entity_addresses,
// business_entity_contacts, visitation_contacts and business_entity_industry
// (PIPE-OQ-4's satellite table list) have NO deleted_at column at the
// source. schema.Columns for a schema_change against one of these therefore
// never includes it. Without ApplySchema synthesizing the column,
// deleteTableBatch's unconditional UPDATE ... SET deleted_at would hit this
// fake's "unknown column" simulation (and, on real Databend, an actual
// unknown-column DDL error) on every delete, forever. This test drives a
// schema_change with no deleted_at in Columns, then a delete, through the
// real ApplySchema/BatchUpload path and asserts it succeeds.
func TestWS4_ApplySchema_SynthesizesDeletedAt_ForSourceTableWithout(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "business_entity_addresses"}

	// Schema-change as it would really arrive for this table: no deleted_at
	// in Columns, because the source table genuinely has none.
	err := snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:  ref.Table,
			Schema: ref.Schema,
			Columns: map[string]string{
				"id":                 "int8",
				"business_entity_id": "int8",
				"address_line1":      "text",
			},
			PKColumns: []string{"id"},
		},
	})
	require.NoError(t, err)

	// The fake's CREATE TABLE parsing must have picked up the synthesized
	// column -- assert the precondition directly before exercising the
	// write path, so a failure here points straight at ApplySchema instead
	// of surfacing only as a mysterious delete failure below.
	require.True(t, db.hasColumn(ref.String(), "deleted_at"), "ApplySchema must synthesize deleted_at on every synced table, even when the source schema has none")

	err = snk.BatchUpload(ctx, []protocol.Message{{
		Op:          protocol.OpInsert,
		Table:       ref.Table,
		TableSchema: ref.Schema,
		UUID:        "addr-insert",
		Data:        map[string]any{"id": int64(1), "business_entity_id": int64(9), "address_line1": "1 Main St"},
	}})
	require.NoError(t, err)

	deleteTS := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	err = snk.BatchUpload(ctx, []protocol.Message{{
		Op:          protocol.OpDelete,
		Table:       ref.Table,
		TableSchema: ref.Schema,
		UUID:        "addr-delete",
		Timestamp:   deleteTS,
		Data:        map[string]any{"id": int64(1)},
	}})
	require.NoError(t, err, "soft delete against a table whose source schema has no deleted_at must still succeed, because the sink synthesizes the column")

	deletedAt, ok := db.deletedAtFor(ref.String(), "1")
	require.True(t, ok)
	assert.Equal(t, deleteTS, deletedAt)
}

// TestWS4_ApplySchema_BackfillsDeletedAt_OnExistingTable covers the ALTER
// path: a table that already exists (e.g. created by a pre-fix process
// instance, or any other reason its Databend columns lack deleted_at) gets
// deleted_at added via ALTER TABLE the next time ApplySchema runs, not just
// at CREATE TABLE time.
func TestWS4_ApplySchema_BackfillsDeletedAt_OnExistingTable(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()

	ref := protocol.TableRef{Schema: "public", Table: "legacy_table"}
	qualified := ref.String()

	// Simulate a table that predates the deleted_at synthesis fix: created
	// directly in the fake with no deleted_at column, bypassing ApplySchema.
	db.addColumn(qualified, "id")
	db.addColumn(qualified, "name")

	// getCurrentColumns' QueryContext stub in this fake always returns empty
	// rows, so ApplySchema's own "existingCols" view is empty regardless of
	// db.columns -- meaning ApplySchema will always take the CREATE TABLE
	// branch, not ALTER, against this particular fake. Use a QueryContext
	// override for this one test so the ALTER path is genuinely exercised.
	db2 := &alterPathFakeDB{persistentFakeDB: db, existingCols: []string{"id", "name"}}
	snk2 := newRestartSink(db2)

	err := snk2.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:     ref.Table,
			Schema:    ref.Schema,
			Columns:   map[string]string{"id": "int8", "name": "text"},
			PKColumns: []string{"id"},
		},
	})
	require.NoError(t, err)

	assert.True(t, db.hasColumn(qualified, "deleted_at"), "ApplySchema must ALTER TABLE ADD COLUMN deleted_at for an existing table that lacks it")
}

// alterPathFakeDB wraps persistentFakeDB to report existingCols from
// QueryContext (getCurrentColumns), forcing ApplySchema down its ALTER
// TABLE branch instead of CREATE TABLE.
type alterPathFakeDB struct {
	*persistentFakeDB
	existingCols []string
}

func (f *alterPathFakeDB) QueryContext(_ context.Context, query string, _ ...any) (DBRows, error) {
	if strings.Contains(strings.ToUpper(query), "INFORMATION_SCHEMA.COLUMNS") {
		return &fakeColumnRows{cols: f.existingCols}, nil
	}
	return emptyDBRows{}, nil
}

// fakeColumnRows is a minimal DBRows over a static column-name list.
type fakeColumnRows struct {
	cols []string
	i    int
}

func (r *fakeColumnRows) Next() bool { return r.i < len(r.cols) }
func (r *fakeColumnRows) Scan(dest ...any) error {
	p, ok := dest[0].(*string)
	if !ok {
		return fmt.Errorf("expected *string dest")
	}
	*p = r.cols[r.i]
	r.i++
	return nil
}
func (r *fakeColumnRows) Close() error { return nil }
func (r *fakeColumnRows) Err() error   { return nil }

// TestWS4_DeleteTableBatch_BatchesIntoSingleUpdate_SingleColumnPK pins WS-4
// item 3: multiple deletes sharing the same event timestamp against the
// same table must land in ONE UPDATE ... WHERE pk IN (...) statement, not
// one per row -- the write-amplification concern WS-4B flagged for
// merge-on-write (each UPDATE is a new copy-on-write snapshot).
func TestWS4_DeleteTableBatch_BatchesIntoSingleUpdate_SingleColumnPK(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "custom_objects", Table: "_1_2_orders"}
	err := snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:     ref.Table,
			Schema:    ref.Schema,
			Columns:   map[string]string{"record_id": "int8", "name": "text"},
			PKColumns: []string{"record_id"},
		},
	})
	require.NoError(t, err)

	deleteTS := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	msgs := make([]protocol.Message, 5)
	for i := 0; i < 5; i++ {
		msgs[i] = protocol.Message{
			Op:          protocol.OpDelete,
			Table:       ref.Table,
			TableSchema: ref.Schema,
			UUID:        fmt.Sprintf("del-%d", i),
			Timestamp:   deleteTS,
			Data:        map[string]any{"record_id": int64(100 + i)},
		}
	}

	require.NoError(t, snk.BatchUpload(ctx, msgs))

	assert.Equal(t, 1, db.updateStatementCount(), "5 same-timestamp deletes against the same table must batch into a single UPDATE statement")
	for i := 0; i < 5; i++ {
		deletedAt, ok := db.deletedAtFor(ref.String(), fmt.Sprintf("%d", 100+i))
		require.True(t, ok, "record_id=%d must be soft-deleted", 100+i)
		assert.Equal(t, deleteTS, deletedAt)
	}
}

// TestWS4_DeleteTableBatch_CompositePK_BatchesIntoSingleUpdate covers the
// composite-PK OR-of-AND batching form (visitation_contacts,
// business_entity_contacts -- PIPE-OQ-4's composite-key satellites).
func TestWS4_DeleteTableBatch_CompositePK_BatchesIntoSingleUpdate(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "visitation_contacts"}
	err := snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:     ref.Table,
			Schema:    ref.Schema,
			Columns:   map[string]string{"visitation_id": "int8", "contact_id": "int8"},
			PKColumns: []string{"visitation_id", "contact_id"},
		},
	})
	require.NoError(t, err)

	deleteTS := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	msgs := []protocol.Message{
		{Op: protocol.OpDelete, Table: ref.Table, TableSchema: ref.Schema, UUID: "d1", Timestamp: deleteTS, Data: map[string]any{"visitation_id": int64(1), "contact_id": int64(10)}},
		{Op: protocol.OpDelete, Table: ref.Table, TableSchema: ref.Schema, UUID: "d2", Timestamp: deleteTS, Data: map[string]any{"visitation_id": int64(1), "contact_id": int64(11)}},
	}

	require.NoError(t, snk.BatchUpload(ctx, msgs))

	assert.Equal(t, 1, db.updateStatementCount(), "composite-PK deletes sharing a timestamp must also batch into a single UPDATE")
	_, ok := db.deletedAtFor(ref.String(), "1|10")
	require.True(t, ok)
	_, ok = db.deletedAtFor(ref.String(), "1|11")
	require.True(t, ok)
}

// TestWS4_UnknownDeletedAtColumn_IsClassifiedPermanent is the defense-in-depth
// backstop: even if a table somehow reaches deleteTableBatch without a
// deleted_at column (this fake simulated directly, bypassing ApplySchema, to
// isolate the classification behavior from the synthesis fix above), the
// resulting DDL error must classify as permanent so it DLQs on attempt 1
// instead of Nack-looping forever against a frozen replication slot.
func TestWS4_UnknownDeletedAtColumn_IsClassifiedPermanent(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "no_deleted_at_column"}
	qualified := ref.String()

	// Bypass ApplySchema entirely -- seed the PK cache directly and give the
	// fake table columns with no deleted_at, modeling a table that reached
	// this state despite the synthesis fix (e.g. an operator-created table,
	// or a future regression).
	snk.pkMu.Lock()
	snk.pkCache[qualified] = []string{"id"}
	snk.pkLoaded[qualified] = struct{}{}
	snk.pkMu.Unlock()
	db.addColumn(qualified, "id")
	db.addColumn(qualified, "name")

	err := snk.BatchUpload(ctx, []protocol.Message{{
		Op:          protocol.OpDelete,
		Table:       ref.Table,
		TableSchema: ref.Schema,
		UUID:        "d1",
		Timestamp:   time.Now(),
		Data:        map[string]any{"id": int64(1)},
	}})
	require.Error(t, err)
	assert.True(t, IsPermanentDDLError(err), "an unknown-column error on the soft-delete UPDATE must classify as permanent, not transient -- transient means Nack-retry-forever against a frozen replication slot")
}

// ----------------------------------------------------------------------------
// Round-5 review follow-up: source-declares-deleted_at cases (the most
// common real configuration -- generated custom tables, sidecars,
// master_contacts, business_entities, visitations all have a real
// deleted_at column at the source), the double-ALTER regression, and the
// upsert-resurrects-a-tombstone fix.
// ----------------------------------------------------------------------------

// TestWS4_ApplySchema_CreateTable_SourceAlreadyDeclaresDeletedAt is the
// CREATE-path companion the round-5 review flagged as missing: when the
// source schema *does* declare deleted_at (the common case), ApplySchema's
// dedup (case-insensitive match against schema.Columns) must not emit a
// second column definition -- the table must end up with exactly one
// deleted_at column, not two.
func TestWS4_ApplySchema_CreateTable_SourceAlreadyDeclaresDeletedAt(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "custom_objects", Table: "_1_3_generated_table"}
	err := snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:  ref.Table,
			Schema: ref.Schema,
			Columns: map[string]string{
				"id":         "int8",
				"name":       "text",
				"deleted_at": "timestamptz", // real source column, as on every generated custom table
			},
			PKColumns: []string{"id"},
		},
	})
	require.NoError(t, err)

	require.True(t, db.hasColumn(ref.String(), "deleted_at"))
	// Exactly one CREATE TABLE, and its DDL must not contain "deleted_at"
	// twice -- a second, synthesized definition would be a Databend DDL
	// error (duplicate column) on the very first sync of this table.
	createCalls := 0
	for _, q := range db.execCalls {
		up := strings.ToUpper(strings.TrimSpace(q))
		// Scope to this test's own table -- ApplySchema also issues a
		// CREATE TABLE for cdc_meta.pk_columns, which is unrelated.
		if strings.HasPrefix(up, "CREATE TABLE") && strings.Contains(q, `"_1_3_generated_table"`) {
			createCalls++
			assert.Equal(t, 1, strings.Count(strings.ToLower(q), `"deleted_at"`), "deleted_at must appear exactly once in the CREATE TABLE DDL: %s", q)
		}
	}
	assert.Equal(t, 1, createCalls)
}

// TestWS4_ApplySchema_AlterTable_SourceAlreadyDeclaresDeletedAt_NoDoubleAdd
// is the direct regression test for the round-5 MEDIUM: an existing table
// missing deleted_at, whose source schema.Columns *does* declare it (the
// PIPE-OQ-4 remediation direction -- a satellite table gains the column and
// a schema_change carries it), must get exactly ONE ALTER TABLE ADD COLUMN
// deleted_at, not two. Before the fix, the main loop's ALTER and the
// backstop's ALTER both fired against the same stale existingCols snapshot;
// the fake's ALTER-rejects-duplicate behavior (added this round) makes that
// concretely fail instead of silently succeeding twice.
func TestWS4_ApplySchema_AlterTable_SourceAlreadyDeclaresDeletedAt_NoDoubleAdd(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()

	ref := protocol.TableRef{Schema: "public", Table: "satellite_gaining_deleted_at"}
	qualified := ref.String()

	// Table already exists in Databend (via a prior sync) with columns but
	// no deleted_at -- exactly PIPE-OQ-4's "add the column" remediation
	// starting state.
	db.addColumn(qualified, "id")
	db.addColumn(qualified, "contact_id")

	altered := &alterPathFakeDB{persistentFakeDB: db, existingCols: []string{"id", "contact_id"}}
	snk := newRestartSink(altered)

	err := snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:  ref.Table,
			Schema: ref.Schema,
			Columns: map[string]string{
				"id":         "int8",
				"contact_id": "int8",
				"deleted_at": "timestamptz", // now present in the source schema too
			},
			PKColumns: []string{"id"},
		},
	})
	require.NoError(t, err, "a double ALTER TABLE ADD COLUMN deleted_at must not happen -- if it does, the fake's duplicate-column rejection turns this into an error")

	assert.True(t, db.hasColumn(qualified, "deleted_at"))
	alterCalls := 0
	for _, q := range db.execCalls {
		up := strings.ToUpper(strings.TrimSpace(q))
		if strings.HasPrefix(up, "ALTER TABLE") && strings.Contains(strings.ToLower(q), `"deleted_at"`) {
			alterCalls++
		}
	}
	assert.Equal(t, 1, alterCalls, "exactly one ALTER TABLE ADD COLUMN deleted_at must be issued, not one from the main loop plus one from the backstop")
}

// TestWS4_UploadTableBatch_PreservesTombstoneAcrossUpsert is the direct
// regression test for the round-5 MEDIUM "a later upsert resurrects a
// soft-deleted row": a row is inserted, soft-deleted, and then a
// redelivered upsert for the same pk (whose payload -- realistically, from
// a table where deleted_at is synthesized rather than sourced -- omits
// deleted_at entirely) arrives. The tombstone must survive.
func TestWS4_UploadTableBatch_PreservesTombstoneAcrossUpsert(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "business_entity_addresses"}
	err := snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:  ref.Table,
			Schema: ref.Schema,
			Columns: map[string]string{
				"id":            "int8",
				"address_line1": "text",
				// no deleted_at -- this table's source genuinely has none,
				// so it is synthesized by ApplySchema (see the earlier
				// synthesis test). Every upsert payload for this table will
				// likewise omit deleted_at.
			},
			PKColumns: []string{"id"},
		},
	})
	require.NoError(t, err)

	// Insert.
	require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{{
		Op: protocol.OpInsert, Table: ref.Table, TableSchema: ref.Schema, UUID: "ins-1",
		Data: map[string]any{"id": int64(7), "address_line1": "1 Main St"},
	}}))
	_, deleted := db.deletedAtFor(ref.String(), "7")
	assert.False(t, deleted, "freshly inserted row must not be tombstoned")

	// Soft delete.
	deleteTS := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{{
		Op: protocol.OpDelete, Table: ref.Table, TableSchema: ref.Schema, UUID: "del-1",
		Timestamp: deleteTS, Data: map[string]any{"id": int64(7)},
	}}))
	deletedAt, ok := db.deletedAtFor(ref.String(), "7")
	require.True(t, ok)
	assert.Equal(t, deleteTS, deletedAt)

	// A redelivered/superseded upsert for the same pk arrives after the
	// delete -- its payload has no deleted_at key, exactly as every upsert
	// for this table's payload does (the column is synthesized, not
	// sourced). Pre-fix, REPLACE INTO would silently null deleted_at back
	// out because the column is absent from the payload's key set.
	require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{{
		Op: protocol.OpUpdate, Table: ref.Table, TableSchema: ref.Schema, UUID: "upd-1",
		Data: map[string]any{"id": int64(7), "address_line1": "1 Main St, Suite 2"},
	}}))

	// The row's own content is updated...
	row, ok := db.rowByPK(ref.String(), "7")
	require.True(t, ok)
	assert.Equal(t, "1 Main St, Suite 2", row["address_line1"])
	// ...but the tombstone must survive. This is the assertion that fails
	// without the fix.
	deletedAt, ok = db.deletedAtFor(ref.String(), "7")
	require.True(t, ok, "the tombstone must survive an upsert whose payload omits deleted_at")
	assert.Equal(t, deleteTS, deletedAt, "the preserved deleted_at value must be exactly what the prior delete set, not reset or cleared")

	// A genuinely new row (never deleted) for the same table must NOT be
	// spuriously tombstoned by the preservation logic -- it has no prior
	// deleted_at to preserve, so it must come back nil/absent.
	require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{{
		Op: protocol.OpInsert, Table: ref.Table, TableSchema: ref.Schema, UUID: "ins-2",
		Data: map[string]any{"id": int64(8), "address_line1": "2 Main St"},
	}}))
	_, deleted = db.deletedAtFor(ref.String(), "8")
	assert.False(t, deleted, "a genuinely new row must not be spuriously tombstoned by deleted_at-preservation logic")
}

// ----------------------------------------------------------------------------
// Round-5c review: intra-batch upsert/delete serialization for the same
// ref, and PK-key canonicalization across the write-side/read-side type
// divergence.
// ----------------------------------------------------------------------------

// TestBatchUpload_IntraBatchDeleteAndSupersededUpsert_DeleteWins is the
// direct regression test for the round-5c MEDIUM: a single BatchUpload call
// containing BOTH an upsert and a delete for the same PK in the same table
// must not race. Before the fix, upsertTableBatch and deleteTableBatch ran
// as independent errgroup goroutines for the same ref, so
// fetchCurrentDeletedAt (inside the upsert) could read pre-delete state and
// the REPLACE INTO could silently erase the delete this very batch was
// meant to apply. Post-fix, BatchUpload serializes the pair (upsert then
// delete) into one goroutine per ref, so the delete deterministically wins.
// Run several iterations -- serialization removes the race entirely, so
// this is about catching a regression back to independent goroutines, not
// about statistically dodging a race window.
func TestBatchUpload_IntraBatchDeleteAndSupersededUpsert_DeleteWins(t *testing.T) {
	ctx := context.Background()

	for iter := 0; iter < 20; iter++ {
		db := newPersistentFakeDB()
		snk := newRestartSink(db)

		ref := protocol.TableRef{Schema: "public", Table: "business_entity_addresses"}
		require.NoError(t, snk.ApplySchema(ctx, protocol.Message{
			Op: protocol.OpSchemaChange,
			Schema: &protocol.SchemaMetadata{
				Table:     ref.Table,
				Schema:    ref.Schema,
				Columns:   map[string]string{"id": "int8", "address_line1": "text"},
				PKColumns: []string{"id"},
			},
		}))

		// Seed an existing row so the upsert in the same batch below is a
		// genuine "superseded update", not a first insert -- it must go
		// through the tombstone-preservation fetch path (its payload omits
		// deleted_at) at the same time the delete for the same pk is being
		// applied.
		require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{{
			Op: protocol.OpInsert, Table: ref.Table, TableSchema: ref.Schema, UUID: "seed",
			Data: map[string]any{"id": int64(99), "address_line1": "orig"},
		}}))

		deleteTS := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
		// One BatchUpload call, one flush: an upsert AND a delete for the
		// same pk, exactly the scenario that used to race.
		err := snk.BatchUpload(ctx, []protocol.Message{
			{Op: protocol.OpUpdate, Table: ref.Table, TableSchema: ref.Schema, UUID: "upd",
				Data: map[string]any{"id": int64(99), "address_line1": "superseded-update"}},
			{Op: protocol.OpDelete, Table: ref.Table, TableSchema: ref.Schema, UUID: "del",
				Timestamp: deleteTS, Data: map[string]any{"id": int64(99)}},
		})
		require.NoError(t, err)

		deletedAt, ok := db.deletedAtFor(ref.String(), "99")
		require.True(t, ok, "iteration %d: the delete in the same batch as the superseded upsert must win -- the row must end up tombstoned", iter)
		assert.Equal(t, deleteTS, deletedAt, "iteration %d", iter)
	}
}

// TestCanonicalPKValueString pins the write-side/read-side rendering
// mismatch directly: a JSON-fallback-decoded PK (decodePayload's msgpack
// path failing over to JSON, per sink.go) yields a float64 for what is
// really an integer id, while the Databend driver's own INT64 column parser
// (verified live against datafuselabs/databend-go's valueparser.go
// intParser) yields a genuine Go int64. Both must canonicalize to the same
// key, including a bigint value that would visibly differ under %v's
// default formatting (float64's %g-like default vs int64's %d).
func TestCanonicalPKValueString(t *testing.T) {
	const bigID = 1234567890123 // exceeds float64's exact-integer boundary considerations enough to matter, and is a realistic bigint PK

	cases := []struct {
		name string
		a, b any
	}{
		{"float64 vs int64, large id", float64(bigID), int64(bigID)},
		{"float64 vs int, small id", float64(42), 42},
		{"float64 vs int32", float64(7), int32(7)},
		{"[]byte vs string", []byte("abc-uuid"), "abc-uuid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ka := canonicalPKValueString(tc.a)
			kb := canonicalPKValueString(tc.b)
			assert.Equal(t, kb, ka, "canonicalPKValueString(%#v) = %q must equal canonicalPKValueString(%#v) = %q", tc.a, ka, tc.b, kb)
		})
	}

	// Sanity: without canonicalization this pair would visibly diverge --
	// pin that fact so a future edit that weakens canonicalPKValueString
	// back toward a bare fmt.Sprintf("%v") is caught by this test failing
	// with an explanatory message, not just a silent behavior change.
	require.NotEqual(t, fmt.Sprintf("%v", float64(bigID)), fmt.Sprintf("%v", int64(bigID)),
		"if this ever becomes equal, %%v itself started agreeing across these types and canonicalPKValueString's float64 special-case may no longer be needed -- but verify before removing it")
}

// driverLikeDeletedAtRows is a minimal DBRows that always returns pk values
// as int64 (mimicking the real databend-go driver's INT64 column parser,
// verified live), decoupled from whatever Go type the query's own args
// happened to be -- unlike persistentFakeDB, which merely echoes back
// whatever it was given and so cannot, by construction, exercise a type
// mismatch between the query args and the returned row.
type driverLikeDeletedAtRows struct {
	pkInt64  int64
	deleted  any
	returned bool
}

func (r *driverLikeDeletedAtRows) Next() bool {
	if r.returned {
		return false
	}
	r.returned = true
	return true
}
func (r *driverLikeDeletedAtRows) Scan(dest ...any) error {
	if len(dest) != 2 {
		return fmt.Errorf("expected 2 dest, got %d", len(dest))
	}
	p0, ok := dest[0].(*any)
	if !ok {
		return fmt.Errorf("dest[0] not *any")
	}
	p1, ok := dest[1].(*any)
	if !ok {
		return fmt.Errorf("dest[1] not *any")
	}
	*p0 = r.pkInt64 // int64, like the real driver's INT64 column parser
	*p1 = r.deleted
	return nil
}
func (r *driverLikeDeletedAtRows) Close() error { return nil }
func (r *driverLikeDeletedAtRows) Err() error   { return nil }

// driverLikeDeletedAtDB is a DBExec whose QueryContext always answers with
// driverLikeDeletedAtRows, regardless of the query text or the Go type of
// the args it was called with -- the point is to decouple "what type the
// caller queried with" from "what type the driver hands back", which
// persistentFakeDB cannot do because it round-trips identical Go values.
type driverLikeDeletedAtDB struct {
	*persistentFakeDB
	pkInt64 int64
	deleted any
}

func (f *driverLikeDeletedAtDB) QueryContext(_ context.Context, query string, _ ...any) (DBRows, error) {
	if strings.Contains(strings.ToUpper(query), "INFORMATION_SCHEMA.COLUMNS") {
		return emptyDBRows{}, nil
	}
	return &driverLikeDeletedAtRows{pkInt64: f.pkInt64, deleted: f.deleted}, nil
}

// TestFetchCurrentDeletedAt_CanonicalizesAcrossDriverTypeDivergence drives
// fetchCurrentDeletedAt itself (not the higher-level upload path) with a
// float64 query tuple -- exactly what a JSON-fallback-decoded bigint PK
// produces -- against a fake driver that returns the PK as int64, matching
// real databend-go's INT64 parser. Without canonicalPKValueString, the
// returned map's key (built from the int64 the "driver" handed back) would
// never match a lookup keyed from the float64 tuple that was queried with,
// and the caller would silently treat every row as "no tombstone found".
func TestFetchCurrentDeletedAt_CanonicalizesAcrossDriverTypeDivergence(t *testing.T) {
	ctx := context.Background()
	const bigID = 1234567890123

	wantDeletedAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	db := &driverLikeDeletedAtDB{
		persistentFakeDB: newPersistentFakeDB(),
		pkInt64:          bigID,
		deleted:          wantDeletedAt,
	}
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "business_entity_addresses"}
	// The query tuple, as uploadTableBatch would build it from a
	// JSON-fallback-decoded payload: a float64, not an int64.
	tuples := [][]any{{float64(bigID)}}

	result, err := snk.fetchCurrentDeletedAt(ctx, ref, []string{"id"}, tuples)
	require.NoError(t, err)

	key := pkTupleKey(tuples[0])
	got, ok := result[key]
	require.True(t, ok, "fetchCurrentDeletedAt's result map must be keyed so a float64-tuple lookup finds the int64-driver-typed row -- without canonicalization this misses silently, with no error, reproducing the tombstone-resurrection bug for exactly the bigint-PK case JSON-fallback decode produces")
	assert.Equal(t, wantDeletedAt, got)
}
