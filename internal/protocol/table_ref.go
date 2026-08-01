package protocol

import (
	"fmt"
	"strings"
)

// TableRef is the canonical identity of a table: the only way a table is
// named across config, wire, and key-builder boundaries. See
// MULTI_SCHEMA_PLAN.md §2.1-2.3.
type TableRef struct {
	Schema string // never empty after normalisation
	Table  string
}

// NormalizeSchema maps the empty schema to "public". This is the single rule
// used both for bare-configured tables and for in-flight/legacy messages
// that predate the TableSchema field.
func NormalizeSchema(s string) string {
	if s == "" {
		return "public"
	}
	return s
}

// String renders the always-qualified display form, e.g. "sales.orders" or
// "public.orders". Used for display, logs, and sink targets.
func (r TableRef) String() string {
	return NormalizeSchema(r.Schema) + "." + r.Table
}

// KeyToken renders the KV/JetStream-safe form (plan §2.3):
//
//	KeyToken() = table                when Schema == "public"
//	KeyToken() = schema + "=" + table otherwise
//
// The public branch coincides with today's bare format, so bare-configured
// deployments see identical keys before and after Stage 1. "=" is chosen
// because it is valid in a NATS KV key, is not a token separator, and is not
// legal in an unquoted Postgres identifier -- see ParseTableRef, which
// rejects "=" in either component to keep this encoding injective.
func (r TableRef) KeyToken() string {
	schema := NormalizeSchema(r.Schema)
	if schema == "public" {
		return r.Table
	}
	return schema + "=" + r.Table
}

// ParseTableRef parses a bare ("orders") or schema-qualified ("sales.orders")
// name into a TableRef. Bare names normalise to the "public" schema. Names
// with more than two dot-separated components, empty components, or a
// component containing "=" are rejected -- "=" is reserved for KeyToken.
func ParseTableRef(s string) (TableRef, error) {
	if strings.Contains(s, "=") {
		return TableRef{}, fmt.Errorf("protocol: invalid table reference %q: %q is not allowed", s, "=")
	}

	parts := strings.Split(s, ".")
	switch len(parts) {
	case 1:
		if parts[0] == "" {
			return TableRef{}, fmt.Errorf("protocol: invalid table reference %q: empty", s)
		}
		return TableRef{Schema: "public", Table: parts[0]}, nil
	case 2:
		if parts[0] == "" || parts[1] == "" {
			return TableRef{}, fmt.Errorf("protocol: invalid table reference %q: empty component", s)
		}
		return TableRef{Schema: parts[0], Table: parts[1]}, nil
	default:
		return TableRef{}, fmt.Errorf("protocol: invalid table reference %q: expected at most one \".\"", s)
	}
}

// TableRefFromKeyToken inverts KeyToken(): it is the only supported way back
// from the KV/JetStream-safe form to a TableRef. It is NOT a general parser
// like ParseTableRef -- KeyToken() uses "=" as its separator (never "."), so
// this simply splits on the first "=". Engine state (tableStates/evoStates
// maps, and any KV key builder fed from an already-KeyToken-normalised
// identity) carries KeyToken() as its canonical form as of Stage 1; this is
// how those call sites recover a TableRef to hand to a key builder without
// re-deriving from a raw, potentially-unnormalised string (see
// MULTI_SCHEMA_PLAN.md §11.2 requirement 3).
func TableRefFromKeyToken(token string) TableRef {
	if schema, table, ok := strings.Cut(token, "="); ok {
		return TableRef{Schema: schema, Table: table}
	}
	return TableRef{Schema: "public", Table: token}
}
