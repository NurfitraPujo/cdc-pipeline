package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/vmihailenco/msgpack/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog/log"
)

type PostgresSource struct {
	name      string
	connector cdc.Connector
	cancel    context.CancelFunc
	closeOnce sync.Once

	oidMu    sync.RWMutex
	oidCache map[uint32]string
}

func NewPostgresSource(name string) *PostgresSource {
	return &PostgresSource{
		name:     name,
		oidCache: make(map[uint32]string),
	}
}

func (s *PostgresSource) Name() string {
	return s.name
}

func (s *PostgresSource) Start(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint) (<-chan []protocol.Message, chan<- struct{}, error) {
	out := make(chan []protocol.Message, 1)
	ack := make(chan struct{})
	flushReq := make(chan []protocol.Message)

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	batchSize := srcConfig.BatchSize
	if batchSize <= 0 { batchSize = 100 }
	batchWait := srcConfig.BatchWait
	if batchWait <= 0 { batchWait = 5 * time.Second }
	discoveryInterval := srcConfig.DiscoveryInterval
	if discoveryInterval <= 0 { discoveryInterval = 30 * time.Second }

	var mu sync.Mutex
	var msgs []protocol.Message
	knownTables := make(map[string]bool)

	// Sequencer Goroutine
	go func() {
		defer close(flushReq) // dummy, but for completeness
		for {
			select {
			case <-ctx.Done(): return
			case m := <-flushReq:
				select {
				case out <- m:
					select {
					case <-ack:
					case <-ctx.Done(): return
					}
				case <-ctx.Done(): return
				}
			}
		}
	}()

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		srcConfig.User, srcConfig.PassEncrypted, srcConfig.Host, srcConfig.Port, srcConfig.Database)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("failed to open DB connection: %w", err)
	}

	if err := s.primeOIDCache(ctx, db); err != nil {
		log.Warn().Err(err).Msg("Failed to prime OID cache")
	}

	triggerFlush := func() {
		mu.Lock()
		if len(msgs) == 0 { mu.Unlock(); return }
		mCopy := make([]protocol.Message, len(msgs))
		copy(mCopy, msgs)
		msgs = msgs[:0]
		mu.Unlock()

		select {
		case flushReq <- mCopy:
		case <-ctx.Done():
		}
	}

	// Periodic flusher
	go func() {
		ticker := time.NewTicker(batchWait)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done(): return
			case <-ticker.C: triggerFlush()
			}
		}
	}()

	// Discovery & Initial Emission
	go func() {
		defer db.Close()
		for _, t := range srcConfig.Tables {
			cols, pks, err := s.getTableMetadata(ctx, db, "public", t)
			if err == nil {
				m := protocol.Message{
					SourceID: srcConfig.ID, Table: t, Op: "schema_change", Timestamp: time.Now(),
					Schema: &protocol.SchemaMetadata{Table: t, Schema: "public", Columns: cols, PKColumns: pks},
				}
				mu.Lock(); msgs = append(msgs, m); mu.Unlock(); triggerFlush()
			}
		}

		ticker := time.NewTicker(discoveryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done(): return
			case <-ticker.C: s.discoverTables(ctx, db, srcConfig, &mu, &msgs, knownTables, triggerFlush)
			}
		}
	}()

	pubTables := make(publication.Tables, len(srcConfig.Tables))
	for i, t := range srcConfig.Tables {
		pubTables[i] = publication.Table{Name: t, ReplicaIdentity: "DEFAULT"}
	}

	cfg := config.Config{
		Host: srcConfig.Host, Port: srcConfig.Port, Username: srcConfig.User, Password: srcConfig.PassEncrypted, Database: srcConfig.Database,
		Slot: slot.Config{Name: srcConfig.SlotName, CreateIfNotExists: true},
		Publication: publication.Config{
			Name: srcConfig.PublicationName, CreateIfNotExists: true, Tables: pubTables,
			Operations: publication.Operations{publication.OperationInsert, publication.OperationUpdate, publication.OperationDelete},
		},
		Snapshot: config.SnapshotConfig{Enabled: checkpoint.IngressLSN == 0, Mode: config.SnapshotModeInitial, ChunkSize: 8000},
	}

	handler := func(lc *replication.ListenerContext) {
		// Recovery from handler panics
		defer func() {
			if r := recover(); r != nil {
				log.Error().Str("slot", srcConfig.SlotName).Interface("recover", r).Msg("PostgresSource RECOVERED from handler panic")
			}
		}()

		mu.Lock()
		var m protocol.Message
		switch msg := lc.Message.(type) {
		case *format.Relation:
			if strings.HasPrefix(msg.Name, "cdc_snapshot_") { mu.Unlock(); lc.Ack(); return }
			if !s.isSchemaAllowed(msg.Namespace, srcConfig.Schemas) { mu.Unlock(); lc.Ack(); return }
			knownTables[msg.Namespace+"."+msg.Name] = true
			cols := make(map[string]string)
			for _, col := range msg.Columns { cols[col.Name] = s.resolveTypeName(col.DataType) }
			m = protocol.Message{
				SourceID: srcConfig.ID, Table: msg.Name, Op: "schema_change", Timestamp: time.Now(),
				Schema: &protocol.SchemaMetadata{Table: msg.Name, Schema: msg.Namespace, Columns: cols},
			}
		case *format.Insert:
			if strings.HasPrefix(msg.TableName, "cdc_snapshot_") { mu.Unlock(); lc.Ack(); return }
			payload, err := msgpack.Marshal(msg.Decoded)
			if err != nil { log.Error().Err(err).Str("table", msg.TableName).Msg("Error marshaling insert"); mu.Unlock(); lc.Ack(); return }
			m = protocol.Message{SourceID: srcConfig.ID, Table: msg.TableName, Op: "insert", Payload: payload, Data: msg.Decoded, Timestamp: msg.MessageTime}
		case *format.Update:
			if strings.HasPrefix(msg.TableName, "cdc_snapshot_") { mu.Unlock(); lc.Ack(); return }
			payload, err := msgpack.Marshal(msg.NewDecoded)
			if err != nil { log.Error().Err(err).Str("table", msg.TableName).Msg("Error marshaling update"); mu.Unlock(); lc.Ack(); return }
			m = protocol.Message{SourceID: srcConfig.ID, Table: msg.TableName, Op: "update", Payload: payload, Data: msg.NewDecoded, Timestamp: msg.MessageTime}
		case *format.Delete:
			if strings.HasPrefix(msg.TableName, "cdc_snapshot_") { mu.Unlock(); lc.Ack(); return }
			payload, err := msgpack.Marshal(msg.OldDecoded)
			if err != nil { log.Error().Err(err).Str("table", msg.TableName).Msg("Error marshaling delete"); mu.Unlock(); lc.Ack(); return }
			m = protocol.Message{SourceID: srcConfig.ID, Table: msg.TableName, Op: "delete", Payload: payload, Data: msg.OldDecoded, Timestamp: msg.MessageTime}
		case *format.Snapshot:
			if strings.HasPrefix(msg.Table, "cdc_snapshot_") { mu.Unlock(); lc.Ack(); return }
			if msg.EventType == format.SnapshotEventTypeData {
				payload, err := msgpack.Marshal(msg.Data)
				if err != nil { log.Error().Err(err).Str("table", msg.Table).Msg("Error marshaling snapshot"); mu.Unlock(); lc.Ack(); return }
				m = protocol.Message{SourceID: srcConfig.ID, Table: msg.Table, Op: "snapshot", LSN: uint64(msg.LSN), Payload: payload, Data: msg.Data, Timestamp: msg.ServerTime}
			}
		}

		if m.SourceID != "" {
			msgs = append(msgs, m)
			if len(msgs) >= batchSize || m.Op == "schema_change" {
				mu.Unlock()
				triggerFlush()
			} else {
				mu.Unlock()
			}
		} else {
			mu.Unlock()
		}

		_ = lc.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("failed to create connector: %w", err)
	}
	s.connector = connector

	go func() {
		defer close(out)
		s.connector.Start(ctx)
		// Internal safety: Ensure Close is called if Start returns unexpectedly
		s.Stop()
	}()

	return out, ack, nil
}

func (s *PostgresSource) getTableMetadata(ctx context.Context, db *sql.DB, schema, table string) (map[string]string, []string, error) {
	rows, err := db.QueryContext(ctx, "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2", schema, table)
	if err != nil { return nil, nil, err }
	defer rows.Close()
	cols := make(map[string]string)
	for rows.Next() {
		var name, dtype string
		if err := rows.Scan(&name, &dtype); err == nil { cols[name] = dtype }
	}
	pkQuery := `SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisprimary;`
	rowsPk, err := db.QueryContext(ctx, pkQuery, schema, table)
	if err != nil { return cols, []string{"id"}, nil }
	defer rowsPk.Close()
	var pks []string
	for rowsPk.Next() {
		var pk string
		if err := rowsPk.Scan(&pk); err == nil { pks = append(pks, pk) }
	}
	if len(pks) == 0 { pks = []string{"id"} }
	return cols, pks, nil
}

func (s *PostgresSource) isSchemaAllowed(namespace string, allowed []string) bool {
	if len(allowed) == 0 { return true }
	for _, a := range allowed { if a == namespace { return true } }
	return false
}

func (s *PostgresSource) resolveTypeName(oid uint32) string {
	s.oidMu.RLock(); defer s.oidMu.RUnlock()
	if name, ok := s.oidCache[oid]; ok { return name }
	return fmt.Sprintf("oid:%d", oid)
}

func (s *PostgresSource) primeOIDCache(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "SELECT oid, typname FROM pg_type")
	if err != nil { return err }
	defer rows.Close()
	s.oidMu.Lock(); defer s.oidMu.Unlock()
	for rows.Next() {
		var oid uint32; var name string
		if err := rows.Scan(&oid, &name); err == nil { s.oidCache[oid] = name }
	}
	return nil
}

func (s *PostgresSource) discoverTables(ctx context.Context, db *sql.DB, srcConfig protocol.SourceConfig, mu *sync.Mutex, msgs *[]protocol.Message, known map[string]bool, triggerFlush func()) {
	for _, schema := range srcConfig.Schemas {
		rows, err := db.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = $1", schema)
		if err != nil { continue }
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err == nil {
				if strings.HasPrefix(tableName, "cdc_snapshot_") { continue }
				fullKey := schema + "." + tableName
				mu.Lock()
				if !known[fullKey] {
					known[fullKey] = true
					cols, pks, _ := s.getTableMetadata(ctx, db, schema, tableName)
					m := protocol.Message{
						SourceID: srcConfig.ID, Table: tableName, Op: "schema_change", Timestamp: time.Now(),
						Schema: &protocol.SchemaMetadata{Table: tableName, Schema: schema, Columns: cols, PKColumns: pks},
					}
					*msgs = append(*msgs, m); mu.Unlock(); triggerFlush(); mu.Lock()
				}
				mu.Unlock()
			}
		}
		rows.Close()
	}
}

func (s *PostgresSource) Stop() error {
	s.closeOnce.Do(func() {
		if s.connector != nil {
			// FIRST PRINCIPLE FIX:
			// 1. Signal shutdown to our own context first so loops know to stop.
			if s.cancel != nil {
				log.Info().Msg("PostgresSource: Canceling context...")
				s.cancel()
			}
			
			// 2. Short sleep to allow ReceiveMessage loops (which have a 300ms deadline) 
			// to finish their current iteration and see the canceled context.
			time.Sleep(400 * time.Millisecond)

			// 3. Explicitly close the connector. 
			// Because the context is already done, the internal loops will see the EOF
			// and check s.closed. Since Close() is running, we MUST ensure the library 
			// sees 'closed' before it panics. 
			log.Info().Msg("PostgresSource: Signaling explicit close...")
			s.connector.Close()
		}
	})
	return nil
}
