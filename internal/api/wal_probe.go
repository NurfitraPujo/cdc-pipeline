package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/crypto"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

// walGrowthSampleInterval is the gap between the two pg_replication_slots
// samples NewSlotLagRateSampler takes to estimate WAL growth rate. Long
// enough that pg_wal_lsn_diff moves a measurable amount on a real source,
// short enough not to noticeably delay the pause request it is embedded in.
const walGrowthSampleInterval = 250 * time.Millisecond

// openSourceDBByID is the c *gin.Context-free twin of openSourceDB, used by
// callers (the WAL probes below) that run outside a request handler's own
// source lookup. It duplicates openSourceDB's connection-string assembly
// rather than refactoring openSourceDB to take this as a helper, to avoid
// touching openSourceDB's existing gin.Context-shaped error responses.
func (h *Handler) openSourceDBByID(id string) (*sql.DB, protocol.SourceConfig, error) {
	entry, err := h.kv.Get(protocol.SourceConfigKey(id))
	if err != nil {
		return nil, protocol.SourceConfig{}, fmt.Errorf("source not found: %w", err)
	}

	var cfg protocol.SourceConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		return nil, protocol.SourceConfig{}, err
	}

	if cfg.PassEncrypted != "" {
		encKey, err := crypto.GetEncryptionKey()
		if err != nil {
			return nil, cfg, err
		}
		if decrypted, err := crypto.Decrypt(cfg.PassEncrypted, encKey); err == nil {
			cfg.PassEncrypted = decrypted
		}
	}

	u := &url.URL{
		Scheme: "postgres", Host: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		User: url.UserPassword(cfg.User, cfg.PassEncrypted), Path: cfg.Database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	q.Set("connect_timeout", "3")
	u.RawQuery = q.Encode()

	db, err := sql.Open("pgx", u.String())
	if err != nil {
		return nil, cfg, fmt.Errorf("failed to open connection: %w", err)
	}
	return db, cfg, nil
}

// NewSlotLagRateSampler builds the WS-3 SlotLagRateSampler (plan section 5)
// out of two samples of the same pg_replication_slots query
// querySlotLagBytes (internal/source/postgres/source.go) already runs for
// cdc_source_slot_lag_bytes, taken walGrowthSampleInterval apart. It opens
// its own short-lived connection to the pipeline's first source (the same
// "Sources[0]" convention internal/engine/factory.go uses) rather than
// sharing the running worker's *sql.DB, since a pipeline being paused may
// have no live worker to share one with.
func (h *Handler) NewSlotLagRateSampler() SlotLagRateSampler {
	return func(ctx context.Context, _ string, cfg protocol.PipelineConfig) (float64, bool) {
		if len(cfg.Sources) == 0 {
			return 0, false
		}
		db, srcCfg, err := h.openSourceDBByID(cfg.Sources[0])
		if err != nil {
			return 0, false
		}
		defer func() { _ = db.Close() }()

		if srcCfg.SlotName == "" {
			return 0, false
		}

		first, ok := queryRetainedWALBytes(ctx, db, srcCfg.SlotName)
		if !ok {
			return 0, false
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-time.After(walGrowthSampleInterval):
		}
		second, ok := queryRetainedWALBytes(ctx, db, srcCfg.SlotName)
		if !ok {
			return 0, false
		}

		deltaBytes := second - first
		if deltaBytes < 0 {
			// Slot advanced (consumer caught up) between samples; growth
			// rate is not negative, it is effectively zero.
			deltaBytes = 0
		}
		return float64(deltaBytes) / walGrowthSampleInterval.Seconds(), true
	}
}

// queryRetainedWALBytes reads the WAL bytes currently retained for a slot --
// the same pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) query
// querySlotLagBytes runs -- plus safe_wal_size when non-NULL (plan section
// 7: NULL whenever max_slot_wal_keep_size is unset). ok=false on any error,
// matching querySlotLagBytes's own "skip this tick, never report a fake
// zero" contract.
func queryRetainedWALBytes(ctx context.Context, db *sql.DB, slotName string) (int64, bool) {
	if db == nil || slotName == "" {
		return 0, false
	}
	qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var retained int64
	err := db.QueryRowContext(qctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
		 FROM pg_replication_slots WHERE slot_name = $1`,
		slotName,
	).Scan(&retained)
	if err != nil {
		return 0, false
	}
	return retained, true
}

// remainingWALBudgetBytes computes the WAL budget actually left before the
// slot is invalidated (plan section 5/7), rather than the constant full
// budget pauseResponse used to pass unconditionally: WALBudgetBytes minus
// the WAL already retained by the slot when safe_wal_size is unavailable
// (the common path here, since a second query would cost another
// round-trip this helper's caller does not otherwise need), floored at 0
// so an already-over-budget slot reports "no time left" rather than a
// negative duration.
func remainingWALBudgetBytes(ctx context.Context, db *sql.DB, slotName string) (int64, bool) {
	retained, ok := queryRetainedWALBytes(ctx, db, slotName)
	if !ok {
		return 0, false
	}
	remaining := protocol.WALBudgetBytes - retained
	if remaining < 0 {
		remaining = 0
	}
	return remaining, true
}
