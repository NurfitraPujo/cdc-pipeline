package databend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitQualified(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantSchema string
		wantTable  string
	}{
		{
			name:      "qualified name",
			input:     "mydb.mytable",
			wantSchema: "mydb",
			wantTable:  "mytable",
		},
		{
			name:      "unqualified name",
			input:     "mytable",
			wantSchema: "",
			wantTable:  "mytable",
		},
		{
			name:      "empty string",
			input:     "",
			wantSchema: "",
			wantTable:  "",
		},
		{
			name:      "triple dotted (treated as unqualified)",
			input:     "a.b.c",
			wantSchema: "",
			wantTable:  "a.b.c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSchema, gotTable := splitQualified(tt.input)
			assert.Equal(t, tt.wantSchema, gotSchema)
			assert.Equal(t, tt.wantTable, gotTable)
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
