package postgresdebug

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/google/uuid"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

type DebugSink struct {
	name   string
	config *Config
	db     *sql.DB
	stopCh chan struct{}

	mu           sync.RWMutex
	tableCreated bool

	statsMu         sync.RWMutex
	recordsCaptured uint64
	recordsFiltered uint64
	recordsSampled  uint64
}

func NewDebugSink(name string, dsn string, config *Config) (*DebugSink, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	s := &DebugSink{
		name:   name,
		config: config,
		db:     db,
		stopCh: make(chan struct{}),
	}

	if err := s.ensureTable(); err != nil {
		return nil, err
	}

	go s.runCleanup()

	return s, nil
}

func (s *DebugSink) Name() string {
	return s.name
}

func (s *DebugSink) ensureTable() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tableCreated {
		return nil
	}

	// Always quote table name to handle invalid characters/keywords
	quotedTable := pq.QuoteIdentifier(s.config.TableName)

	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			correlation_id UUID NOT NULL,
			pipeline_id VARCHAR(255) NOT NULL,
			source_id VARCHAR(255) NOT NULL,
			sink_id VARCHAR(255) NOT NULL,
			table_name VARCHAR(255) NOT NULL,
			schema_name VARCHAR(255),
			operation_type VARCHAR(20) NOT NULL,
			lsn BIGINT,
			primary_key TEXT,
			message_uuid VARCHAR(255),
			capture_stage VARCHAR(20) NOT NULL,
			filtered BOOLEAN DEFAULT FALSE,
			transformer_names TEXT[],
			payload JSONB NOT NULL,
			payload_hash VARCHAR(64),
			processing_latency_ms INTEGER,
			captured_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			message_timestamp TIMESTAMP WITH TIME ZONE,
			CONSTRAINT valid_stage CHECK (capture_stage IN ('before', 'after', 'schema_change'))
		);
	`, quotedTable)

	if _, err := s.db.Exec(createTable); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	indexBase := s.config.TableName
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(captured_at DESC)", pq.QuoteIdentifier("idx_"+indexBase+"_captured_at"), quotedTable),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(correlation_id)", pq.QuoteIdentifier("idx_"+indexBase+"_correlation"), quotedTable),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(pipeline_id, sink_id, table_name, captured_at DESC)", pq.QuoteIdentifier("idx_"+indexBase+"_pipeline_lookup"), quotedTable),
	}

	for _, idx := range indexes {
		if _, err := s.db.Exec(idx); err != nil {
			log.Warn().Err(err).Str("index", idx).Msg("Failed to create index")
		}
	}

	s.tableCreated = true
	log.Info().Str("table", s.config.TableName).Msg("Debug sink table created")
	return nil
}

func (s *DebugSink) BatchUpload(ctx context.Context, messages []protocol.Message) error {
	for _, m := range messages {
		if m.Op == "schema_change" {
			if err := s.captureSchemaChange(ctx, m); err != nil {
				log.Error().Err(err).Msg("Debug sink: failed to capture schema_change")
			}
		}
	}
	return nil
}

func (s *DebugSink) ApplySchema(ctx context.Context, schema protocol.SchemaMetadata) error {
	return nil
}

func (s *DebugSink) Stop() error {
	close(s.stopCh)
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *DebugSink) captureSchemaChange(ctx context.Context, msg protocol.Message) error {
	payload, err := extractPayload(msg)
	if err != nil {
		return err
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	correlationID := uuid.New().String()

	schemaName := ""
	if msg.Schema != nil {
		schemaName = msg.Schema.Schema
	}

	quotedTable := pq.QuoteIdentifier(s.config.TableName)
	query := fmt.Sprintf(`
		INSERT INTO %s (
			correlation_id, pipeline_id, source_id, sink_id, table_name, schema_name,
			operation_type, capture_stage, payload, message_timestamp, transformer_names
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, quotedTable)

	_, err = s.db.ExecContext(ctx, query,
		correlationID,
		msg.SourceID, // pipeline_id (best effort)
		msg.SourceID,
		s.name,
		msg.Table,
		schemaName,
		msg.Op,
		"schema_change",
		payloadJSON,
		msg.Timestamp,
		pq.Array([]string{}), // Use pq.Array for TEXT[]
	)

	return err
}

func (s *DebugSink) runCleanup() {
	if s.config.Retention.Mode == "disabled" {
		return
	}

	ticker := time.NewTicker(s.config.Retention.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			switch s.config.Retention.Mode {
			case "age":
				s.cleanupByAge()
			case "count":
				s.cleanupByCount()
			}
		}
	}
}

func (s *DebugSink) cleanupByAge() {
	quotedTable := pq.QuoteIdentifier(s.config.TableName)
	// Use interval syntax for PostgreSQL
	query := fmt.Sprintf(`
		DELETE FROM %s WHERE captured_at < NOW() - ($1 || ' hours')::INTERVAL
	`, quotedTable)

	maxAgeHours := int(s.config.Retention.MaxAge.Hours())
	_, err := s.db.Exec(query, fmt.Sprintf("%d", maxAgeHours))
	if err != nil {
		log.Error().Err(err).Msg("Debug sink: cleanup by age failed")
	}
}

func (s *DebugSink) cleanupByCount() {
	quotedTable := pq.QuoteIdentifier(s.config.TableName)
	// More efficient count-based cleanup
	query := fmt.Sprintf(`
		DELETE FROM %s WHERE id IN (
			SELECT id FROM %s ORDER BY captured_at DESC OFFSET $1
		)
	`, quotedTable, quotedTable)

	_, err := s.db.Exec(query, s.config.Retention.MaxCount)
	if err != nil {
		log.Error().Err(err).Msg("Debug sink: cleanup by count failed")
	}
}

func extractPayload(m protocol.Message) (map[string]interface{}, error) {
	if m.Data != nil {
		return m.Data, nil
	}
	if m.Payload != nil {
		var data map[string]interface{}
		if err := msgpack.Unmarshal(m.Payload, &data); err != nil {
			if err2 := json.Unmarshal(m.Payload, &data); err2 != nil {
				return nil, fmt.Errorf("failed to unmarshal payload: %w", err2)
			}
		}
		return data, nil
	}
	return nil, nil
}
