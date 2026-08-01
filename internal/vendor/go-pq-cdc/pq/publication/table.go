package publication

import (
	"slices"
	"strings"

	"github.com/go-playground/errors"
)

// SnapshotPartitionStrategy defines how a table should be partitioned during snapshot.
// If empty, the strategy is auto-detected based on primary key type.
type SnapshotPartitionStrategy string

const (
	// SnapshotPartitionStrategyAuto lets the system decide based on PK type (default)
	SnapshotPartitionStrategyAuto SnapshotPartitionStrategy = ""
	// SnapshotPartitionStrategyIntegerRange uses MIN/MAX range for integer PKs
	SnapshotPartitionStrategyIntegerRange SnapshotPartitionStrategy = "integer_range"
	// SnapshotPartitionStrategyCTIDBlock uses PostgreSQL physical block locations
	SnapshotPartitionStrategyCTIDBlock SnapshotPartitionStrategy = "ctid_block"
	// SnapshotPartitionStrategyOffset uses LIMIT/OFFSET (slow, fallback)
	SnapshotPartitionStrategyOffset SnapshotPartitionStrategy = "offset"
)

// ValidSnapshotPartitionStrategies contains all valid partition strategy options
var ValidSnapshotPartitionStrategies = []SnapshotPartitionStrategy{
	SnapshotPartitionStrategyAuto,
	SnapshotPartitionStrategyIntegerRange,
	SnapshotPartitionStrategyCTIDBlock,
	SnapshotPartitionStrategyOffset,
}

type Table struct {
	Name            string `json:"name" yaml:"name"`
	ReplicaIdentity string `json:"replicaIdentity" yaml:"replicaIdentity"`
	Schema          string `json:"schema,omitempty" yaml:"schema,omitempty"`
	// SnapshotPartitionStrategy allows overriding the auto-detected partition strategy.
	// Useful when integer PKs are hash-based (not sequential) and range partitioning performs poorly.
	// Options: "" (auto), "integer_range", "ctid_block", "offset"
	SnapshotPartitionStrategy SnapshotPartitionStrategy `json:"snapshotPartitionStrategy,omitempty" yaml:"snapshotPartitionStrategy,omitempty"`
}

func (tc Table) Validate() error {
	if strings.TrimSpace(tc.Name) == "" {
		return errors.New("table name cannot be empty")
	}

	// vendored-patch: MS-2 (MULTI_SCHEMA_PLAN.md §3 Stage 4, task 4) - Schema is
	// required here rather than left to SetDefault()'s silent "" -> "public"
	// fallback (config/config.go). SetDefault() still runs first in the normal
	// connector.New() flow (connector.go) and always fills Schema before this
	// runs, so this check is inert on that path today -- the embedding
	// application (internal/source/postgres/source.go) already sets Schema
	// explicitly on every publication.Table via TableRef.NormalizeSchema. Its
	// purpose is to catch any *future* or *direct* caller (tests, a different
	// embedder, a call to Tables.Validate()/Table.Validate() that bypasses
	// SetDefault) that constructs a Table without Schema: such a caller now
	// fails loudly here instead of silently landing on "public" three call
	// frames away.
	if strings.TrimSpace(tc.Schema) == "" {
		return errors.New("table schema cannot be empty")
	}

	if !slices.Contains(ReplicaIdentityOptions, tc.ReplicaIdentity) {
		return errors.Newf("undefined replica identity option. valid identity options are: %v", ReplicaIdentityOptions)
	}

	return nil
}

type Tables []Table

func (ts Tables) Validate() error {
	if len(ts) == 0 {
		return errors.New("at least one table must be defined")
	}

	for _, t := range ts {
		if err := t.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (ts Tables) Diff(tss Tables) Tables {
	res := Tables{}
	tssMap := make(map[string]Table)

	// vendored-patch: MS-2 (MULTI_SCHEMA_PLAN.md §3 Stage 4, task 1) - the diff
	// key was Name+ReplicaIdentity only, which is schema-blind: "a.t" and "b.t"
	// hashed to the same key, so a table added or removed in one schema could
	// be masked entirely by a same-named table in another schema. Schema is now
	// part of the key so cross-schema same-named tables diff independently.
	for _, t := range tss {
		tssMap[t.Schema+"."+t.Name+t.ReplicaIdentity] = t
	}

	for _, t := range ts {
		if v, found := tssMap[t.Schema+"."+t.Name+t.ReplicaIdentity]; !found || v.ReplicaIdentity != t.ReplicaIdentity {
			res = append(res, t)
		}
	}

	return res
}
