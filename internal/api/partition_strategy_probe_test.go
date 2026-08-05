package api

import (
	"database/sql"
	"testing"
)

// TestIsIntegerRangeStrategy is the table test for the validator finding:
// this must be an allowlist (only exactly "integer_range" is safe), not a
// denylist of the known-bad strategies -- a NULL column, an empty string, or
// an unrecognised/future strategy name must all degrade, never be silently
// treated as safe.
func TestIsIntegerRangeStrategy(t *testing.T) {
	cases := []struct {
		name     string
		strategy sql.NullString
		want     bool
	}{
		{"integer_range is safe", sql.NullString{String: "integer_range", Valid: true}, true},
		{"ctid_block is degraded", sql.NullString{String: "ctid_block", Valid: true}, false},
		{"offset is degraded", sql.NullString{String: "offset", Valid: true}, false},
		{"NULL is degraded, not silently safe", sql.NullString{Valid: false}, false},
		{"empty string is degraded", sql.NullString{String: "", Valid: true}, false},
		{"unknown/future strategy is degraded", sql.NullString{String: "hash_range", Valid: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isIntegerRangeStrategy(tc.strategy); got != tc.want {
				t.Errorf("isIntegerRangeStrategy(%+v) = %v, want %v", tc.strategy, got, tc.want)
			}
		})
	}
}

// TestResolveSnapshotMetadataSchema mirrors the vendored connector's own
// resolveMetadataSchema (internal/vendor/go-pq-cdc/pq/snapshot/snapshot.go):
// first configured schema wins, empty/unset falls back to "public".
func TestResolveSnapshotMetadataSchema(t *testing.T) {
	cases := []struct {
		name    string
		schemas []string
		want    string
	}{
		{"nil schemas defaults to public", nil, "public"},
		{"empty schemas defaults to public", []string{}, "public"},
		{"single schema is used verbatim", []string{"sales"}, "sales"},
		{"first of multiple schemas wins", []string{"sales", "public"}, "sales"},
		{"whitespace-only first schema defaults to public", []string{"  "}, "public"},
		{"leading/trailing whitespace is trimmed", []string{"  sales  "}, "sales"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveSnapshotMetadataSchema(tc.schemas); got != tc.want {
				t.Errorf("resolveSnapshotMetadataSchema(%v) = %q, want %q", tc.schemas, got, tc.want)
			}
		})
	}
}

// TestQueryNonIntegerRangeTables_NilInputs covers the guard clause without
// a database: a nil db or empty slot name must report ok=false ("skip this
// check"), never a false "not degraded".
func TestQueryNonIntegerRangeTables_NilInputs(t *testing.T) {
	ctx := t.Context()

	degraded, tables, ok := queryNonIntegerRangeTables(ctx, nil, "public", "some_slot")
	if ok {
		t.Errorf("expected ok=false for a nil db")
	}
	if degraded || tables != nil {
		t.Errorf("expected no degraded result for a nil db, got degraded=%v tables=%v", degraded, tables)
	}

	degraded, tables, ok = queryNonIntegerRangeTables(ctx, nil, "public", "")
	if ok {
		t.Errorf("expected ok=false for an empty slot name")
	}
	if degraded || tables != nil {
		t.Errorf("expected no degraded result for an empty slot name, got degraded=%v tables=%v", degraded, tables)
	}
}
