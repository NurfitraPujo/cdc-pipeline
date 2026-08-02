package databend

import (
	"context"
	"strings"
	"testing"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// WS-6: schema evolution. ApplySchema is (and per the ratified plan,
// remains) add-only -- it never issues ALTER ... MODIFY COLUMN. What it
// must not do is stay silent when a schema_change redeclares an *existing*
// column with a type that maps to a different Databend type than what was
// previously applied: that divergence must be counted and logged loudly
// (cdc_sink_schema_type_divergence_total) rather than the table quietly
// drifting out of sync with the source forever.
// ----------------------------------------------------------------------------

func TestWS6_ApplySchema_TypeDivergence_DetectedAndCounted(t *testing.T) {
	ctx := context.Background()
	// alterPathFakeDB reports "id"/"amount" as already existing from the
	// very first ApplySchema call, forcing both calls down ApplySchema's
	// ALTER-branch code path (where the WS-6 divergence check lives)
	// instead of the CREATE TABLE branch, exactly the way a real second
	// schema_change for an already-synced table would.
	db := &alterPathFakeDB{persistentFakeDB: newPersistentFakeDB(), existingCols: []string{"id", "amount", "deleted_at"}}
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "orders"}

	// First schema_change: "amount" declared numeric -> DECIMAL(...). The
	// column "already exists" per existingCols, so this is itself an
	// ALTER-branch call, but with nothing yet recorded in colTypeCache --
	// establishes the baseline, must not itself count as a divergence.
	require.NoError(t, snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:     ref.Table,
			Schema:    ref.Schema,
			Columns:   map[string]string{"id": "int8", "amount": "numeric"},
			PKColumns: []string{"id"},
		},
	}))
	before := testutil.ToFloat64(SinkSchemaTypeDivergenceTotal.WithLabelValues(snk.name, ref.String(), "amount"))

	// Second schema_change for the SAME already-existing column declares a
	// different type (text -> STRING instead of numeric -> DECIMAL). This
	// is exactly the case ApplySchema cannot safely auto-remediate (custom
	// objects don't permit type changes app-side; a non-custom-object
	// change has no safe automatic ALTER) -- it must be surfaced, not
	// silently dropped.
	require.NoError(t, snk.ApplySchema(ctx, protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:     ref.Table,
			Schema:    ref.Schema,
			Columns:   map[string]string{"id": "int8", "amount": "text"},
			PKColumns: []string{"id"},
		},
	}))
	after := testutil.ToFloat64(SinkSchemaTypeDivergenceTotal.WithLabelValues(snk.name, ref.String(), "amount"))

	assert.Equal(t, before+1, after, "a genuine type change on an existing column must increment the divergence counter exactly once")

	// ApplySchema must remain add-only: since existingCols already reports
	// "amount" as present on both calls, the sink must never have issued
	// an ALTER TABLE for it at all (add-only means it is only ever ADDed
	// when absent, never MODIFYed when it diverges).
	amountAlterCount := 0
	for _, call := range db.execCalls {
		if strings.Contains(call, "ALTER TABLE") && strings.Contains(call, `"amount"`) {
			amountAlterCount++
		}
	}
	assert.Equal(t, 0, amountAlterCount, "ApplySchema must remain add-only: no ALTER/MODIFY for an existing column, even when its declared type diverges")
}

// TestWS6_ApplySchema_SameTypeRedeclared_NoSpuriousDivergence is the
// configuration that does NOT exercise a real divergence: the same column
// declared with the same type on a second schema_change (e.g. a redelivered
// message) must not increment the counter -- only an actual change should.
func TestWS6_ApplySchema_SameTypeRedeclared_NoSpuriousDivergence(t *testing.T) {
	ctx := context.Background()
	db := &alterPathFakeDB{persistentFakeDB: newPersistentFakeDB(), existingCols: []string{"id", "amount", "deleted_at"}}
	snk := newRestartSink(db)

	ref := protocol.TableRef{Schema: "public", Table: "invoices"}
	schemaMsg := protocol.Message{
		Op: protocol.OpSchemaChange,
		Schema: &protocol.SchemaMetadata{
			Table:     ref.Table,
			Schema:    ref.Schema,
			Columns:   map[string]string{"id": "int8", "amount": "numeric"},
			PKColumns: []string{"id"},
		},
	}
	require.NoError(t, snk.ApplySchema(ctx, schemaMsg))
	before := testutil.ToFloat64(SinkSchemaTypeDivergenceTotal.WithLabelValues(snk.name, ref.String(), "amount"))

	// Redelivered/replayed schema_change, identical declaration.
	require.NoError(t, snk.ApplySchema(ctx, schemaMsg))
	after := testutil.ToFloat64(SinkSchemaTypeDivergenceTotal.WithLabelValues(snk.name, ref.String(), "amount"))

	assert.Equal(t, before, after, "redelivering the exact same schema_change must not spuriously count as a divergence")
}
