package databend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSplitQualified codifies the fixed arity check (MULTI_SCHEMA_PLAN.md
// §7.4 item 8). Before the fix, splitQualified treated anything that was not
// exactly 2 dot-separated parts as "unqualified", so "a.b.c" silently became
// the single table name "a.b.c" -- quoteIdentifier then rendered that as a
// 3-part `"a"."b"."c"` DDL fragment, which Databend accepts as
// catalog.database.table (§6): syntactically valid, but never what the
// caller meant, and nothing ever errored. The fixed version rejects anything
// that is not exactly 1 or 2 non-empty components, including "a..b", a bare
// ".", and leading/trailing dots.
func TestSplitQualified(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantSchema string
		wantTable  string
		wantErr    bool
	}{
		{
			name:       "qualified name",
			input:      "mydb.mytable",
			wantSchema: "mydb",
			wantTable:  "mytable",
		},
		{
			name:      "unqualified name",
			input:     "mytable",
			wantTable: "mytable",
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "triple dotted is rejected, not silently unqualified",
			input:   "a.b.c",
			wantErr: true,
		},
		{
			name:    "empty middle component",
			input:   "a..b",
			wantErr: true,
		},
		{
			name:    "bare dot",
			input:   ".",
			wantErr: true,
		},
		{
			name:    "trailing dot",
			input:   "a.",
			wantErr: true,
		},
		{
			name:    "leading dot",
			input:   ".b",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSchema, gotTable, err := splitQualified(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSchema, gotSchema)
			assert.Equal(t, tt.wantTable, gotTable)
		})
	}
}

// TestValidateIdentifier_RejectsMalformedQualifiedForms exercises the bugs
// listed in MULTI_SCHEMA_PLAN.md §1.1: validateIdentifier used to accept
// "a..b", ".", and leading/trailing dots because its character-only loop
// allowed "." unconditionally, with no check on how many components the dots
// produced. It now reuses splitQualified, so it agrees with TestSplitQualified.
func TestValidateIdentifier_RejectsMalformedQualifiedForms(t *testing.T) {
	for _, bad := range []string{"", "a..b", ".", "a.", ".b", "a.b.c"} {
		t.Run(bad, func(t *testing.T) {
			assert.Error(t, validateIdentifier(bad))
		})
	}

	for _, good := range []string{"orders", "sales_orders", "sales.orders", "public.orders"} {
		t.Run(good, func(t *testing.T) {
			assert.NoError(t, validateIdentifier(good))
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "qualified name",
			input: "mydb.mytable",
			want:  `"mydb"."mytable"`,
		},
		{
			name:  "unqualified name",
			input: "mytable",
			want:  `"mytable"`,
		},
		{
			name:  "name with embedded quote",
			input: `my"table`,
			want:  `"my""table"`,
		},
		{
			name:  "qualified with embedded quote",
			input: `mydb.my"table`,
			want:  `"mydb"."my""table"`,
		},
		{
			name:  "empty string",
			input: "",
			want:  `""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quoteIdentifier(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}
