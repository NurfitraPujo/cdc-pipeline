package config

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/crypto"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	// stdlib registers the "pgx" database/sql driver used to open source Postgres connections.
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// NewPostgresSlotHealthChecker builds the production SlotHealthChecker WS-3
// leaves as a seam (see defaultSlotHealthChecker's doc comment). It reuses
// the slot-status signal already decoded at
// internal/vendor/go-pq-cdc/pq/slot/slot.go:220 -- wal_status -- via the
// same short-lived pg_replication_slots probe querySlotLagBytes
// (internal/source/postgres/source.go) runs, rather than adding a new
// connector. cmd/pipeline/main.go installs this so the pause-expiry ticker
// actually consults slot health before resuming (plan section 4.3's
// "Paused | timer expiry | wal_status = lost | NeedsResnapshot" row).
//
// A probe failure (source not found, connection refused, query error) is
// treated as "do not resume": it returns SlotHealth{Alive: false,
// WALStatusLost: false}, which protocol.Transition rejects as an illegal
// (Paused, timer_expiry) pair rather than accepting it as Resuming --
// tickPauseExpiry logs and leaves the record untouched for the next tick to
// retry. That is the deliberately safe direction: an unreachable database
// must never be read as "the slot is fine".
func NewPostgresSlotHealthChecker(kv nats.KeyValue) SlotHealthChecker {
	return func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) SlotHealth {
		if len(cfg.Sources) == 0 {
			log.Warn().Str("pipeline_id", pipelineID).Msg("slot health probe: pipeline has no source configured")
			return SlotHealth{}
		}

		db, slotName, err := openSourceDBForHealthCheck(kv, cfg.Sources[0])
		if err != nil {
			log.Warn().Err(err).Str("pipeline_id", pipelineID).Msg("slot health probe: failed to open source connection")
			return SlotHealth{}
		}
		defer func() { _ = db.Close() }()

		health, ok := probeSlotHealth(ctx, db, slotName)
		if !ok {
			log.Warn().Str("pipeline_id", pipelineID).Str("slot", slotName).Msg("slot health probe: query failed")
			return SlotHealth{}
		}
		return health
	}
}

// openSourceDBForHealthCheck reads a source config from KV and opens a
// short-lived connection to it, mirroring the connection-string assembly
// internal/api.Handler.openSourceDB uses (host/port/user/password/database,
// sslmode disabled, a short connect timeout) -- duplicated here rather than
// exported from internal/api to avoid a config->api dependency neither
// package otherwise has.
func openSourceDBForHealthCheck(kv nats.KeyValue, sourceID string) (*sql.DB, string, error) {
	entry, err := kv.Get(protocol.SourceConfigKey(sourceID))
	if err != nil {
		return nil, "", fmt.Errorf("source not found: %w", err)
	}
	var cfg protocol.SourceConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		return nil, "", err
	}

	if cfg.PassEncrypted != "" {
		encKey, err := crypto.GetEncryptionKey()
		if err != nil {
			return nil, "", err
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
		return nil, cfg.SlotName, fmt.Errorf("failed to open connection: %w", err)
	}
	return db, cfg.SlotName, nil
}

// probeSlotHealth queries pg_replication_slots for wal_status, following
// the plan section 7 ladder (reserved -> extended -> unreserved -> lost).
// ok=false on any query error, including a missing driver/db -- callers
// treat that as "do not resume", never as healthy. A missing row (slot
// dropped/never created) reports Alive: false without WALStatusLost, which
// is also "do not resume" via the same illegal-transition path -- WS-5 is
// what teaches this probe to expect that shape routinely once stop starts
// dropping slots.
func probeSlotHealth(ctx context.Context, db *sql.DB, slotName string) (SlotHealth, bool) {
	if db == nil || slotName == "" {
		return SlotHealth{}, false
	}
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var walStatus sql.NullString
	err := db.QueryRowContext(qctx,
		`SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1`,
		slotName,
	).Scan(&walStatus)
	if err == sql.ErrNoRows {
		return SlotHealth{Alive: false}, true
	}
	if err != nil {
		return SlotHealth{}, false
	}

	lost := walStatus.Valid && walStatus.String == "lost"
	return SlotHealth{Alive: !lost, WALStatusLost: lost}, true
}
