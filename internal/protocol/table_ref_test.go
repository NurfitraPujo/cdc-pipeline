package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeSchema(t *testing.T) {
	assert.Equal(t, "public", NormalizeSchema(""))
	assert.Equal(t, "public", NormalizeSchema("public"))
	assert.Equal(t, "sales", NormalizeSchema("sales"))
}

func TestTableRef_String(t *testing.T) {
	assert.Equal(t, "sales.orders", TableRef{Schema: "sales", Table: "orders"}.String())
	assert.Equal(t, "public.orders", TableRef{Schema: "public", Table: "orders"}.String())
	// Unnormalized (empty) schema still qualifies as "public" -- String()
	// always normalises.
	assert.Equal(t, "public.orders", TableRef{Schema: "", Table: "orders"}.String())
}

// TestTableRef_KeyToken_NoMigration locks the no-migration guarantee from
// plan §2.3: for the "public" schema, KeyToken() is exactly the bare table
// name, identical to today's pre-TableRef key format. If this test ever
// needs to change, every KV key and JetStream name for public-schema tables
// changes underneath every existing deployment.
func TestTableRef_KeyToken_NoMigration(t *testing.T) {
	assert.Equal(t, "orders", TableRef{Schema: "public", Table: "orders"}.KeyToken())
}

func TestTableRef_KeyToken(t *testing.T) {
	cases := []struct {
		name string
		ref  TableRef
		want string
	}{
		{"public bare", TableRef{Schema: "public", Table: "orders"}, "orders"},
		{"empty schema normalises to public", TableRef{Schema: "", Table: "orders"}, "orders"},
		{"non-public schema qualifies with =", TableRef{Schema: "sales", Table: "orders"}, "sales=orders"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.ref.KeyToken())
		})
	}
}

// TestTableRef_KeyToken_Injective asserts that distinct TableRefs never
// collide on KeyToken -- required so KV keys and JetStream names stay
// addressable per-table across schemas.
func TestTableRef_KeyToken_Injective(t *testing.T) {
	refs := []TableRef{
		{Schema: "public", Table: "orders"},
		{Schema: "public", Table: "users"},
		{Schema: "sales", Table: "orders"},
		{Schema: "inventory", Table: "orders"},
		{Schema: "sales", Table: "users"},
	}
	seen := make(map[string]TableRef, len(refs))
	for _, r := range refs {
		tok := r.KeyToken()
		if prior, ok := seen[tok]; ok {
			t.Fatalf("KeyToken collision: %+v and %+v both produce %q", prior, r, tok)
		}
		seen[tok] = r
	}
}

func TestParseTableRef(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want TableRef
	}{
		{"bare", "orders", TableRef{Schema: "public", Table: "orders"}},
		{"qualified", "sales.orders", TableRef{Schema: "sales", Table: "orders"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTableRef(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseTableRef_Rejects(t *testing.T) {
	cases := []string{
		"",                       // empty
		"sales.inventory.orders", // >2 components
		".orders",                // empty schema component
		"sales.",                 // empty table component
		"sales=orders",           // "=" reserved for KeyToken
		"=",                      // bare "="
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			_, err := ParseTableRef(in)
			require.Error(t, err, "expected ParseTableRef(%q) to be rejected", in)
		})
	}
}

// TestTableRef_ParseString_RoundTrip verifies String() and ParseTableRef are
// inverses for already-qualified names (String() always qualifies, so the
// round trip only holds starting from the qualified form -- a bare "orders"
// parses to {public, orders} but String() back out is "public.orders", not
// "orders").
func TestTableRef_ParseString_RoundTrip(t *testing.T) {
	cases := []string{"public.orders", "sales.orders", "inventory.customers"}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			ref, err := ParseTableRef(in)
			require.NoError(t, err)
			assert.Equal(t, in, ref.String())
		})
	}
}

// TestTableRefFromKeyToken_RoundTrip verifies TableRefFromKeyToken is the
// exact inverse of KeyToken() for every ref shape KeyToken() can produce --
// this is what lets engine code carry a KeyToken()-normalised string as its
// internal table identity and still recover a TableRef at a key-builder call
// site without re-deriving from a raw config/message string (§11.2
// requirement 3).
func TestTableRefFromKeyToken_RoundTrip(t *testing.T) {
	cases := []TableRef{
		{Schema: "public", Table: "orders"},
		{Schema: "sales", Table: "orders"},
		{Schema: "inventory", Table: "customers"},
	}
	for _, ref := range cases {
		t.Run(ref.String(), func(t *testing.T) {
			assert.Equal(t, ref, TableRefFromKeyToken(ref.KeyToken()))
		})
	}
}

func TestTableRefFromKeyToken_Bare(t *testing.T) {
	assert.Equal(t, TableRef{Schema: "public", Table: "orders"}, TableRefFromKeyToken("orders"))
}
