package databend

import (
	"context"
	"strings"
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatchUpload_SnapshotRowsRouteThroughReplaceIntoMerge is WS-6's
// verification that a forced re-snapshot (Snapshot.Resnapshot: true --
// source.go's shouldResnapshot) does not need a bespoke write path. Plan
// section: "implement a staging-table merge (load into a staging table,
// then merge/replace into the target by key)" is exactly what Databend's
// REPLACE INTO ... ON (key) VALUES already does under the hood -- the same
// statement uploadTableBatch already issues for OpInsert/OpUpdate rows.
// protocol.OpSnapshot messages are not special-cased in BatchUpload (only
// OpDelete and OpSchemaChange are), so re-snapshot rows land in exactly the
// same REPLACE INTO ON (pk) merge as ordinary CDC upserts -- which is what
// makes a re-snapshot idempotent against a target with no PRIMARY KEY in
// DDL: two OpSnapshot rows for the same pk collapse into a single merged
// row instead of duplicating.
func TestBatchUpload_SnapshotRowsRouteThroughReplaceIntoMerge(t *testing.T) {
	db := newFakeDB()
	snk := newTestSink(db)
	ref := protocol.TableRef{Schema: "public", Table: "orders"}
	snk.pkCache[ref.String()] = []string{"id"}
	snk.pkLoaded[ref.String()] = struct{}{}
	snk.provisionedDB[protocol.NormalizeSchema("public")] = struct{}{}

	batch := []protocol.Message{
		{
			SourceID: "s1", TableSchema: "public", Table: "orders",
			Op:   protocol.OpSnapshot,
			Data: map[string]interface{}{"id": float64(1), "name": "first-pass"},
		},
		// A second snapshot chunk re-observing the same pk (e.g. a
		// concurrent UPDATE landing mid-snapshot, or a retried/redelivered
		// chunk) must merge, not duplicate.
		{
			SourceID: "s1", TableSchema: "public", Table: "orders",
			Op:   protocol.OpSnapshot,
			Data: map[string]interface{}{"id": float64(1), "name": "second-pass"},
		},
	}

	err := snk.BatchUpload(context.Background(), batch)
	require.NoError(t, err)

	calls := db.execCalls
	require.Len(t, calls, 1, "both snapshot rows for the same pk must be issued as one REPLACE INTO statement, not per-op writes")
	assert.True(t, strings.HasPrefix(calls[0], `REPLACE INTO "public"."orders"`), "snapshot rows must use the same merge-by-key statement as ordinary upserts, got: %s", calls[0])
	assert.Contains(t, calls[0], `ON ("id")`, "REPLACE INTO must merge on the resolved primary key")
}
