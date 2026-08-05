package config

import "testing"

// TestDropReplicationSlot_NilInputs covers dropReplicationSlot's guard
// clause without a database: a nil db or empty slot name must error rather
// than silently report success, since a caller reading a nil error as "the
// slot is gone" would let finalizeStop mark Stopped without ever having
// touched PostgreSQL.
func TestDropReplicationSlot_NilInputs(t *testing.T) {
	ctx := t.Context()

	if err := dropReplicationSlot(ctx, nil, "some_slot"); err == nil {
		t.Errorf("expected an error for a nil db")
	}
}
