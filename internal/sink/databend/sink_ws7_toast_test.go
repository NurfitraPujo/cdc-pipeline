package databend

import (
	"context"
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// WS-7: the TOAST hazard. Under REPLICA IDENTITY DEFAULT, an UPDATE that
// doesn't touch a wide (TOASTed) column omits it from the WAL tuple
// entirely; the pipeline-side fix (internal/vendor/go-pq-cdc's
// DecodeWithColumn, format.Update, source/postgres/source.go) surfaces this
// as protocol.ColumnKindToastedUnchanged in the message's ColumnKinds
// rather than letting the column vanish indistinguishably from a real
// NULL. This file proves the *sink* side of the contract: uploadTableBatch
// must fetch and carry forward the column's current value rather than
// letting REPLACE INTO default it to NULL.
//
// Uses persistentFakeDB (sink_ws4_pk_durability_test.go), the same
// server-state-persisting fake the WS-4 deleted_at-preservation tests use,
// driven through the real BatchUpload -> uploadTableBatch ->
// fetchCurrentColumns -> executeReplaceIntoChunks path -- not a hand-built
// call to a preservation helper in isolation.
// ----------------------------------------------------------------------------

func TestWS7_UploadTableBatch_PreservesToastedColumnAcrossUpdate(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "widgets"}
	require.NoError(t, snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:  ref.Table,
			Schema: ref.Schema,
			Columns: map[string]string{
				"id":       "int8",
				"name":     "text",
				"long_bio": "text",
			},
			PKColumns: []string{"id"},
		},
	}))

	// Insert with a large long_bio value.
	require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{{
		Op: protocol.OpInsert, Table: ref.Table, TableSchema: ref.Schema, UUID: "ins-1",
		Data: map[string]any{"id": int64(1), "name": "alice", "long_bio": "a very long biography..."},
	}}))
	row, ok := db.rowByPK(ref.String(), "1")
	require.True(t, ok)
	assert.Equal(t, "a very long biography...", row["long_bio"])

	// An UPDATE that only changed "name" -- long_bio is unchanged and
	// TOASTed, so its payload omits the key entirely and flags it via
	// ColumnKinds, exactly what source/postgres/source.go's buildMessage
	// now produces for this case (see source_toast_test.go).
	require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{{
		Op: protocol.OpUpdate, Table: ref.Table, TableSchema: ref.Schema, UUID: "upd-1",
		Data:        map[string]any{"id": int64(1), "name": "alice-renamed"},
		ColumnKinds: map[string]string{"long_bio": protocol.ColumnKindToastedUnchanged},
	}}))

	row, ok = db.rowByPK(ref.String(), "1")
	require.True(t, ok)
	assert.Equal(t, "alice-renamed", row["name"], "the actually-changed column must be updated")
	// This is the assertion that fails without the WS-7.2 sink fix: a
	// REPLACE INTO whose column list omits long_bio (because it was never
	// in Data) defaults it to NULL/empty on real Databend.
	assert.Equal(t, "a very long biography...", row["long_bio"],
		"an unchanged TOASTed column must survive an UPDATE that never touched it, not be nulled out")
}

// TestWS7_UploadTableBatch_RealNullStillWritesNull is the configuration
// that does NOT exercise TOAST at all: an UPDATE that genuinely sets a
// column to NULL (no ColumnKinds entry -- the pipeline never marks a real
// NULL as toasted-unchanged, see tuple.Data.DecodeWithColumn's DataTypeNull
// case) must still write NULL, proving the fix does not overreach and
// start preserving values for columns that were never TOASTed in the first
// place.
func TestWS7_UploadTableBatch_RealNullStillWritesNull(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "widgets"}
	require.NoError(t, snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:     ref.Table,
			Schema:    ref.Schema,
			Columns:   map[string]string{"id": "int8", "notes": "text"},
			PKColumns: []string{"id"},
		},
	}))

	require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{{
		Op: protocol.OpInsert, Table: ref.Table, TableSchema: ref.Schema, UUID: "ins-1",
		Data: map[string]any{"id": int64(2), "notes": "some notes"},
	}}))

	// A genuine NULL: notes is explicitly present in Data with a nil
	// value, not merely absent, and there is no ColumnKinds entry for it.
	require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{{
		Op: protocol.OpUpdate, Table: ref.Table, TableSchema: ref.Schema, UUID: "upd-1",
		Data: map[string]any{"id": int64(2), "notes": nil},
	}}))

	row, ok := db.rowByPK(ref.String(), "2")
	require.True(t, ok)
	assert.Nil(t, row["notes"], "an explicit NULL must be written as NULL, not preserved as the old value")
}

// TestWS7_UploadTableBatch_SamePK_TwiceInOneBatch_PrefersInBatchValue is the
// Opus-validation-review regression test: fetchCurrentColumns reads
// pre-flush database state, so if a single batch contains two updates to
// the same PK where the first establishes a real value for a column and
// the second toast-elides that same column, resolving purely from the DB
// read would silently preserve the *stale pre-batch* value instead of what
// this very batch already established -- strictly better than the pre-WS-7
// bug (which nulled it outright) but still wrong. The fix must resolve the
// second row's toast-need from the first row's in-batch value, not the
// (deliberately stale, older) value already sitting in Databend.
func TestWS7_UploadTableBatch_SamePK_TwiceInOneBatch_PrefersInBatchValue(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "widgets"}
	require.NoError(t, snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:     ref.Table,
			Schema:    ref.Schema,
			Columns:   map[string]string{"id": "int8", "name": "text", "long_bio": "text"},
			PKColumns: []string{"id"},
		},
	}))

	// Pre-batch state in Databend: long_bio = "STALE pre-batch value".
	require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{{
		Op: protocol.OpInsert, Table: ref.Table, TableSchema: ref.Schema, UUID: "ins-1",
		Data: map[string]any{"id": int64(5), "name": "alice", "long_bio": "STALE pre-batch value"},
	}}))

	// A single flush containing TWO updates for the same PK, in order:
	//  1. sets long_bio to a NEW value explicitly (a genuine TOAST update
	//     -- this is the row where the wide column actually changed).
	//  2. a later, different update for the same PK that changes only
	//     "name" -- long_bio is unchanged relative to update 1 and is
	//     TOAST-elided, so its payload omits it and flags it via
	//     ColumnKinds, exactly as source/postgres/source.go produces.
	// The correct preserved value for update 2's long_bio is update 1's
	// "FRESH in-batch value", never the pre-batch "STALE" one.
	require.NoError(t, snk.BatchUpload(ctx, []protocol.Message{
		{
			Op: protocol.OpUpdate, Table: ref.Table, TableSchema: ref.Schema, UUID: "upd-1",
			Data: map[string]any{"id": int64(5), "name": "alice", "long_bio": "FRESH in-batch value"},
		},
		{
			Op: protocol.OpUpdate, Table: ref.Table, TableSchema: ref.Schema, UUID: "upd-2",
			Data:        map[string]any{"id": int64(5), "name": "alice-renamed"},
			ColumnKinds: map[string]string{"long_bio": protocol.ColumnKindToastedUnchanged},
		},
	}))

	row, ok := db.rowByPK(ref.String(), "5")
	require.True(t, ok)
	assert.Equal(t, "alice-renamed", row["name"])
	assert.Equal(t, "FRESH in-batch value", row["long_bio"],
		"the second update's toast-elided long_bio must resolve to the FIRST update's in-batch value, not the stale pre-batch database value")
}

// TestWS7_UploadTableBatch_ToastPreservation_FirstWriteHasNothingToPreserve
// covers the pk-miss branch: a TOAST-unchanged column on a row that does
// not exist yet in Databend (should not happen for a genuine UPDATE, but
// must not corrupt the batch if it does -- e.g. a redelivered/reordered
// event) simply has nothing to preserve and must not error the batch.
func TestWS7_UploadTableBatch_ToastPreservation_FirstWriteHasNothingToPreserve(t *testing.T) {
	ctx := context.Background()
	db := newPersistentFakeDB()
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "widgets"}
	require.NoError(t, snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:     ref.Table,
			Schema:    ref.Schema,
			Columns:   map[string]string{"id": "int8", "name": "text", "long_bio": "text"},
			PKColumns: []string{"id"},
		},
	}))

	err := snk.BatchUpload(ctx, []protocol.Message{{
		Op: protocol.OpUpdate, Table: ref.Table, TableSchema: ref.Schema, UUID: "upd-orphan",
		Data:        map[string]any{"id": int64(999), "name": "ghost"},
		ColumnKinds: map[string]string{"long_bio": protocol.ColumnKindToastedUnchanged},
	}})
	require.NoError(t, err, "a toasted-unchanged column with nothing to preserve must not error the batch")

	row, ok := db.rowByPK(ref.String(), "999")
	require.True(t, ok)
	assert.Equal(t, "ghost", row["name"])
	_, hasLongBio := row["long_bio"]
	assert.False(t, hasLongBio, "with nothing to preserve, long_bio correctly stays out of this row's column set entirely")
}
