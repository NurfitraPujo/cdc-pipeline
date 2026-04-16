# Dynamic Table Discovery Without Pipeline Restart

## Status

Proposed

## Date

2026-04-15

## Author

CDC Pipeline Team

---

## Summary

This document specifies a design for dynamic table discovery in the CDC pipeline that avoids full pipeline restarts when new tables are detected in the source database. Currently, when a new table is discovered, the pipeline triggers a drain → shutdown → restart cycle to update the replication publication. This design eliminates that restart latency while still ensuring new tables are properly tracked.

## Motivation

1. **Reduce latency**: Discovery → CDC capture for new tables currently takes ~10+ seconds due to pipeline restart overhead
2. **No disruption**: Existing CDC streams for current tables are interrupted during transition

## Constraints

1. The go-pq-cdc library creates the Postgres publication at startup and does not expose runtime table addition
2. The library does not support adding tables to its snapshot mechanism at runtime
3. Postgres replication publication CAN be altered at runtime via `ALTER PUBLICATION ADD TABLE`
4. Backfill of existing table data must be handled separately from go-pq-cdc

---

## Design

### Overview

When a new table is discovered:
1. Execute `ALTER PUBLICATION ADD TABLE` via direct SQL connection
2. Backfill existing rows via pgx COPY (emit as `snapshot` messages)
3. Add table to `DynamicTables` in SourceConfig (for restart recovery)
4. Emit `schema_change` message to trigger downstream processing
5. **No pipeline restart occurs**

### New Field in SourceConfig

```go
type SourceConfig struct {
    // ... existing fields ...
    
    DynamicTables []string `msg:"dynamic_tables" yaml:"dynamic_tables" json:"dynamic_tables"`
}
```

**Purpose**: Stores tables discovered at runtime. On restart, the source will:
1. Re-discover tables (including DynamicTables)
2. Issue `ALTER PUBLICATION ADD TABLE` for each dynamic table
3. Re-backfill existing data if needed

### Key Behavior

| Scenario | Action |
|----------|--------|
| New table discovered | Add to publication, backfill, update DynamicTables, emit schema_change |
| Pipeline restart | Source re-discovers all tables (static + dynamic), re-adds to publication |
| ConfigManager watch | Ignores DynamicTables changes - only triggers restart on static config changes |

### Components Modified

#### 1. `internal/source/postgres/source.go`

**New method: `AlterPublicationAddTable`**
```go
func (s *PostgresSource) AlterPublicationAddTable(ctx context.Context, db *sql.DB, tableName string) error {
    // Executes: ALTER PUBLICATION <name> ADD TABLE <tableName>
}
```

**New method: `SnapshotTable`**
```go
func (s *PostgresSource) SnapshotTable(ctx context.Context, db *sql.DB, tableName string, srcConfig SourceConfig, mu *sync.Mutex, msgs *[]Message, triggerFlush func()) error {
    // Uses pgx COPY to read existing rows
    // Emits snapshot messages with LSN=0 (handled by consumer)
    // Does NOT use go-pq-cdc snapshotter
}
```

**Modified: `discoverTables`**
- After detecting new table, call `AlterPublicationAddTable`
- Call `SnapshotTable` for backfill
- Append to `knownTables` and `srcConfig.DynamicTables` (caller must persist)
- Emit `schema_change` message

**Modified: `Start`**
- Initialize `srcConfig.DynamicTables` from stored config
- On startup, issue `ALTER PUBLICATION ADD TABLE` for each dynamic table

#### 2. `internal/protocol/config.go`

**New field in SourceConfig**
```go
type SourceConfig struct {
    // ... existing fields ...
    DynamicTables []string `msg:"dynamic_tables" yaml:"dynamic_tables" json:"dynamic_tables"`
}
```

#### 3. `internal/config/manager.go`

**Modified: `applyHierarchy`**
```go
func (m *ConfigManager) applyHierarchy(cfg *protocol.PipelineConfig) {
    // ... existing hierarchy application ...
    
    // Preserve dynamic tables - don't trigger restart on their addition
    if len(m.configs[cfg.ID].Sources) > 0 {
        // Get stored source config with dynamic tables
        storedSrc, _ := m.getStoredSourceConfig(cfg.Sources[0])
        if storedSrc != nil {
            // Keep dynamic tables from stored config
            cfg.DynamicTables = storedSrc.DynamicTables
        }
    }
}
```

**Alternative approach**: Add a `SkipRestartFields` or similar mechanism. The simpler approach is to store and restore DynamicTables from the existing config during applyHierarchy.

#### 4. `internal/engine/producer.go`

**Modified: `handleDiscovery`**
```go
func (p *Producer) handleDiscovery(m protocol.Message) {
    // ... existing new table check ...
    
    if isNew {
        // DO NOT trigger transitionWorker for dynamic table discovery
        // Just update the config for observability
        p.config.Tables = append(p.config.Tables, m.Schema.Table)
        
        // Also update dynamic tables in SourceConfig
        // ... update stored source config with new dynamic table ...
        
        // Note: No kv.Put to trigger ConfigManager watch restart
    }
}
```

**Key insight**: Discovery config updates happen, but they don't trigger restart because:
1. ConfigManager's `handlePipelineUpdates` only triggers restart on pipeline config changes
2. SourceConfig changes are handled separately and don't go through that path

### Data Flow

```
Discovery Interval (2s)
        │
        ▼
┌─────────────────────────────────┐
│  discoverTables()                │
│  1. Query information_schema     │
│  2. Find table NOT in knownTables│
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│  AlterPublicationAddTable()     │
│  SQL: ALTER PUBLICATION ADD TABLE│
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│  SnapshotTable()                 │
│  pgx COPY existing rows          │
│  Emit 'snapshot' messages        │
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│  Update knownTables +            │
│  srcConfig.DynamicTables          │
│  Persist SourceConfig to KV      │
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│  triggerFlush()                  │
│  schema_change message → producer │
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│  handleDiscovery()               │
│  - Adds to p.config.Tables       │
│  - Updates SourceConfig in KV     │
│  - NO restart triggered          │
└─────────────────────────────────┘
```

### Restart Recovery Flow

```
Pipeline Start
        │
        ▼
┌─────────────────────────────────┐
│  factory.CreateWorker()          │
│  1. Get SourceConfig from KV     │
│  2. Create PostgresSource        │
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│  PostgresSource.Start()          │
│  1. Get DynamicTables from config│
│  2. For each dynamic table:      │
│     - AlterPublicationAddTable() │
│     - SnapshotTable() (if needed)│
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│  Discovery goroutine starts      │
│  - knownTables pre-seeded        │
│  - Will re-detect dynamic tables │
│    (idempotent - no-op)          │
└─────────────────────────────────┘
```

### Error Handling

| Error | Handling |
|-------|----------|
| ALTER PUBLICATION fails | Log error, retry next discovery cycle, do not emit schema_change |
| Snapshot backfill fails | Log error, emit partial snapshot, mark table for retry |
| SourceConfig persist fails | Log error, table still added to publication, recovery may need manual intervention |

### Testing Considerations

1. **E2E Discovery Test**: Update `discovery_test.go` to:
   - Remove expectation of pipeline restart
   - Verify table appears in sink within reasonable time
   - Verify existing table CDC continues without interruption

2. **Unit Tests**:
   - Test `AlterPublicationAddTable` with mock DB
   - Test `SnapshotTable` with mock DB
   - Test idempotency when same table discovered twice

3. **Integration Tests**:
   - Test restart recovery: kill and restart worker, verify dynamic tables still work

---

## Files Affected

| File | Change |
|------|--------|
| `internal/protocol/config.go` | Add `DynamicTables` field |
| `internal/source/postgres/source.go` | Add `AlterPublicationAddTable`, `SnapshotTable`, modify discovery |
| `internal/config/manager.go` | Preserve DynamicTables during applyHierarchy |
| `internal/engine/producer.go` | Remove restart trigger from handleDiscovery |
| `internal/test/e2e/discovery_test.go` | Update test expectations |

---

## Open Questions

1. **Idempotency**: Should re-discovery of an already-dynamic table skip or re-snapshot?
   - Proposal: Skip ALTER PUBLICATION (already added), re-snapshot if `Resnapshot` enabled in config

2. **Remove dynamic table**: What if a dynamic table is dropped?
   - Proposal: Not handled in this version. Future enhancement could detect and log warning.

3. **Multiple sources**: How does this work with pipelines having multiple sources?
   - Proposal: Each SourceConfig manages its own DynamicTables independently

---

## Alternatives Considered

### Option B: Separate KV Key for DiscoveredTables
- Store discovered tables in separate KV key
- Pros: Cleaner separation
- Cons: More complex recovery logic, extra KV ops

### Option C: No Config Persistence
- Rely entirely on re-discovery on restart
- Pros: Simpler
- Cons: Lost observability, potential re-backfill noise

---

## Implementation Phases

### Phase 1: Core Discovery Without Restart
1. Add `DynamicTables` field to SourceConfig
2. Implement `AlterPublicationAddTable`
3. Implement `SnapshotTable` for backfill
4. Modify discovery to use new methods
5. Update config persistence (no restart trigger)

### Phase 2: Restart Recovery
1. Modify PostgresSource.Start to re-add dynamic tables
2. Modify manager.go to preserve DynamicTables

### Phase 3: Testing & Refinement
1. Update E2E tests
2. Add unit tests
3. Error handling refinement
