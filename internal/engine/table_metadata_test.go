package engine

import (
	"encoding/json"
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTableMetadataWriteReadRoundTrip is the regression test for the
// TableMetadataKey cross-type defect (ADR-0017 "Consequences"). The discovery
// path wrote the message's *SchemaMetadata under this key while the API reads
// it as protocol.TableMetadata. The two disagree on `columns` -- a name->type
// object versus a []string -- so json.Unmarshal failed outright and the
// reader's `err == nil` guard dropped the table without a trace.
//
// This encodes exactly as the writer does and decodes exactly as
// api/handler.go ListSourceTables does.
func TestTableMetadataWriteReadRoundTrip(t *testing.T) {
	ref := protocol.TableRef{Schema: "sales", Table: "orders"}
	schema := &protocol.SchemaMetadata{
		Table:     "orders",
		Schema:    "sales",
		Columns:   map[string]string{"id": "int", "name": "text", "amount": "numeric"},
		PKColumns: []string{"id"},
	}

	// The pre-fix writer put the SchemaMetadata itself under this key. Assert
	// that still fails, so this test cannot quietly stop testing anything if
	// the two types ever converge.
	oldBytes, err := json.Marshal(schema)
	require.NoError(t, err)
	var oldDecoded protocol.TableMetadata
	require.Error(t, json.Unmarshal(oldBytes, &oldDecoded),
		"writing SchemaMetadata under a TableMetadata key must not decode")

	data, err := json.Marshal(tableMetadataFromSchema(ref, schema))
	require.NoError(t, err)

	var got protocol.TableMetadata
	require.NoError(t, json.Unmarshal(data, &got),
		"the API reader must be able to decode what the discovery path writes")

	assert.Equal(t, "sales=orders", got.ID, "ID is the KeyToken identity")
	assert.Equal(t, "orders", got.Name)
	assert.Equal(t, "sales", got.Schema)
	assert.Equal(t, []string{"id"}, got.PKColumns)

	// Sorted, and Types aligned positionally with Columns.
	require.Equal(t, []string{"amount", "id", "name"}, got.Columns)
	require.Equal(t, []string{"numeric", "int", "text"}, got.Types)
	for i, col := range got.Columns {
		assert.Equal(t, schema.Columns[col], got.Types[i], "Types[%d] must describe Columns[%d]", i, i)
	}
}

// A bare table must present as "public" rather than "", matching the mapper
// contract pinned by api.TestTableMetadata_SchemaDefaultsToPublic.
func TestTableMetadataDefaultsSchemaToPublic(t *testing.T) {
	meta := tableMetadataFromSchema(protocol.TableRef{Table: "orders"}, nil)
	assert.Equal(t, "public", meta.Schema)
	assert.Equal(t, "orders", meta.Name)
	assert.Empty(t, meta.Columns)
}

// Column ordering must not depend on map range order, or the blob churns on
// every discovery and Columns/Types can misalign.
func TestTableMetadataColumnOrderIsStable(t *testing.T) {
	ref := protocol.TableRef{Schema: "public", Table: "t"}
	schema := &protocol.SchemaMetadata{
		Columns: map[string]string{"z": "int", "a": "text", "m": "bool", "b": "date"},
	}

	first := tableMetadataFromSchema(ref, schema)
	for i := 0; i < 50; i++ {
		again := tableMetadataFromSchema(ref, schema)
		require.Equal(t, first.Columns, again.Columns, "column order must be deterministic")
		require.Equal(t, first.Types, again.Types, "type order must be deterministic")
	}
}
