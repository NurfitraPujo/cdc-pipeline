package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

var metricPortCounter = uint32(20000)

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

func sanitizePayload(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if valuer, ok := v.(driver.Valuer); ok {
			val, err := valuer.Value()
			if err == nil {
				out[k] = val
			} else {
				out[k] = nil
			}
		} else {
			out[k] = v
		}
	}
	return out
}

func (s *PostgresSource) Start(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint) (<-chan []protocol.Message, chan<- struct{}, error) {
	// Create a dedicated context for this source instance's background work
	// This ensures that even if the parent ctx is cancelled during a rapid reload,
	// we have control over the cleanup sequence.
	sourceCtx, sourceCancel := context.WithCancel(context.Background())
	s.cancel = sourceCancel

	out := make(chan []protocol.Message, 1)
	ack := make(chan struct{})
	flushReq := make(chan []protocol.Message)

	var mu sync.Mutex
	var msgs []protocol.Message
	knownTables := make(map[string]bool)
	for _, t := range srcConfig.Tables {
		knownTables["public."+t] = true
	}

	batchWait := srcConfig.BatchWait
	if batchWait == 0 {
		batchWait = 500 * time.Millisecond
	}

	discoveryInterval := srcConfig.DiscoveryInterval
	if discoveryInterval <= 0 {
		discoveryInterval = 30 * time.Second
	}

	// Use a separate setup context with timeout for initial connections
	// This prevents the "operation was canceled" error if a reload happens during dial.
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelSetup()

	// 1. Initial Data Stream (Bridge to out channel)
	go func() {
		for {
			select {
			case mBatch := <-flushReq:
				select {
				case out <- mBatch:
				case <-sourceCtx.Done():
					return
				}
			case <-sourceCtx.Done():
				return
			}
		}
	}()

	// Build DSN using url.URL for proper escaping of special characters
	u := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%d", srcConfig.Host, srcConfig.Port),
		User:   url.UserPassword(srcConfig.User, srcConfig.PassEncrypted),
		Path:   srcConfig.Database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	dsn := u.String()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		sourceCancel()
		return nil, nil, fmt.Errorf("failed to open DB connection: %w", err)
	}

	if err := s.primeOIDCache(setupCtx, db); err != nil {
		log.Warn().Err(err).Msg("Failed to prime OID cache")
	}

	triggerFlush := func() {
		mu.Lock()
		if len(msgs) == 0 {
			mu.Unlock()
			return
		}
		mCopy := make([]protocol.Message, len(msgs))
		copy(mCopy, msgs)
		msgs = msgs[:0]
		mu.Unlock()

		select {
		case flushReq <- mCopy:
		case <-sourceCtx.Done():
		}
	}

	// Periodic flusher
	go func() {
		ticker := time.NewTicker(batchWait)
		defer ticker.Stop()
		for {
			select {
			case <-sourceCtx.Done():
				return
			case <-ticker.C:
				triggerFlush()
			}
		}
	}()

	// Discovery & Initial Emission
	go func() {
		defer db.Close()
		for _, t := range srcConfig.Tables {
			cols, pks, err := s.getTableMetadata(sourceCtx, db, "public", t)
			if err == nil {
				m := protocol.Message{
					SourceID: srcConfig.ID, Table: t, Op: "schema_change", Timestamp: time.Now(),
					Schema: &protocol.SchemaMetadata{Table: t, Schema: "public", Columns: cols, PKColumns: pks},
				}
				mu.Lock()
				msgs = append(msgs, m)
				mu.Unlock()
				triggerFlush()
			}
		}

		ticker := time.NewTicker(discoveryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-sourceCtx.Done():
				return
			case <-ticker.C:
				s.discoverTables(sourceCtx, db, srcConfig, &mu, &msgs, knownTables, triggerFlush)
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
		Snapshot: config.SnapshotConfig{
			Enabled:           checkpoint.IngressLSN == 0,
			Mode:              config.SnapshotModeInitial,
			ChunkSize:         8000,
			ClaimTimeout:      30 * time.Second,
			HeartbeatInterval: 5 * time.Second,
		},
		Metric: config.MetricConfig{Port: int(atomic.AddUint32(&metricPortCounter, 1))}, // Unique port to avoid collision
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
			log.Debug().Str("table", msg.Name).Msg("PostgresSource: Received Relation")
			if strings.HasPrefix(msg.Name, "cdc_snapshot_") {
				mu.Unlock()
				lc.Ack()
				return
			}
			if !s.isSchemaAllowed(msg.Namespace, srcConfig.Schemas) {
				mu.Unlock()
				lc.Ack()
				return
			}
			knownTables[msg.Namespace+"."+msg.Name] = true
			cols := make(map[string]string)
			for _, col := range msg.Columns {
				cols[col.Name] = s.resolveTypeName(col.DataType)
			}
			m = protocol.Message{
				SourceID: srcConfig.ID, Table: msg.Name, Op: "schema_change", Timestamp: time.Now(),
				Schema: &protocol.SchemaMetadata{Table: msg.Name, Schema: msg.Namespace, Columns: cols},
			}
		case *format.Insert:
			log.Debug().Str("table", msg.TableName).Msg("PostgresSource: Received Insert")
			if strings.HasPrefix(msg.TableName, "cdc_snapshot_") {
				mu.Unlock()
				lc.Ack()
				return
			}
			sani := sanitizePayload(msg.Decoded)
			payload, err := msgpack.Marshal(sani)
			if err != nil {
				log.Error().Err(err).Str("table", msg.TableName).Msg("Error marshaling insert")
				mu.Unlock()
				lc.Ack()
				return
			}
			m = protocol.Message{SourceID: srcConfig.ID, Table: msg.TableName, Op: "insert", Payload: payload, Data: sani, Timestamp: msg.MessageTime}
		case *format.Update:
			log.Debug().Str("table", msg.TableName).Msg("PostgresSource: Received Update")
			if strings.HasPrefix(msg.TableName, "cdc_snapshot_") {
				mu.Unlock()
				lc.Ack()
				return
			}
			sani := sanitizePayload(msg.NewDecoded)
			payload, err := msgpack.Marshal(sani)
			if err != nil {
				log.Error().Err(err).Str("table", msg.TableName).Msg("Error marshaling update")
				mu.Unlock()
				lc.Ack()
				return
			}
			m = protocol.Message{SourceID: srcConfig.ID, Table: msg.TableName, Op: "update", Payload: payload, Data: sani, Timestamp: msg.MessageTime}
		case *format.Delete:
			if strings.HasPrefix(msg.TableName, "cdc_snapshot_") {
				mu.Unlock()
				lc.Ack()
				return
			}
			sani := sanitizePayload(msg.OldDecoded)
			payload, err := msgpack.Marshal(sani)
			if err != nil {
				log.Error().Err(err).Str("table", msg.TableName).Msg("Error marshaling delete")
				mu.Unlock()
				lc.Ack()
				return
			}
			m = protocol.Message{SourceID: srcConfig.ID, Table: msg.TableName, Op: "delete", Payload: payload, Data: sani, Timestamp: msg.MessageTime}
		case *format.Snapshot:
			if strings.HasPrefix(msg.Table, "cdc_snapshot_") {
				mu.Unlock()
				lc.Ack()
				return
			}
			if msg.EventType == format.SnapshotEventTypeData {
				sani := sanitizePayload(msg.Data)
				payload, err := msgpack.Marshal(sani)
				if err != nil {
					log.Error().Err(err).Str("table", msg.Table).Msg("Error marshaling snapshot")
					mu.Unlock()
					lc.Ack()
					return
				}
				m = protocol.Message{SourceID: srcConfig.ID, Table: msg.Table, Op: "snapshot", LSN: uint64(msg.LSN), Payload: payload, Data: sani, Timestamp: msg.ServerTime}
			}
		}

		if m.SourceID != "" {
			msgs = append(msgs, m)
			mu.Unlock()
			// For high speed ETL, we allow batching to collect messages until ticker flush.
			// But for snapshot events, we may want to trigger faster.
			if m.Op == "snapshot" && len(msgs) >= 1000 {
				triggerFlush()
			}
		} else {
			mu.Unlock()
		}

		// Acknowledge source library
		select {
		case <-ack:
			lc.Ack()
		case <-sourceCtx.Done():
			// Don't ack if shutting down
		}
	}

	var connectorErr error
	s.connector, connectorErr = cdc.NewConnector(setupCtx, cfg, handler)
	if connectorErr != nil {
		sourceCancel()
		return nil, nil, fmt.Errorf("failed to create connector: %w", connectorErr)
	}

	go s.connector.Start(sourceCtx)

	// Propagate parent cancellation to our local context
	go func() {
		select {
		case <-ctx.Done():
			s.Stop()
		case <-sourceCtx.Done():
		}
	}()

	return out, ack, nil
}

func (s *PostgresSource) Stop() error {
	s.closeOnce.Do(func() {
		log.Info().Str("source", s.name).Msg("PostgresSource: Stopping...")

		// 1. Cancel local context first to stop all internal goroutines
		if s.cancel != nil {
			s.cancel()
		}

		// 2. Short sleep to allow ReceiveMessage loops (which have a 300ms deadline)
		// to finish their current iteration and see the canceled context.
		time.Sleep(400 * time.Millisecond)

		// 3. Explicitly close the connector.
		if s.connector != nil {
			log.Info().Msg("PostgresSource: Signaling explicit close...")
			s.connector.Close()
		}
	})
	return nil
}

func (s *PostgresSource) primeOIDCache(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "SELECT oid, typname FROM pg_type")
	if err != nil {
		return err
	}
	defer rows.Close()

	s.oidMu.Lock()
	defer s.oidMu.Unlock()
	for rows.Next() {
		var oid uint32
		var name string
		if err := rows.Scan(&oid, &name); err == nil {
			s.oidCache[oid] = name
		}
	}
	return nil
}

func (s *PostgresSource) resolveTypeName(oid uint32) string {
	s.oidMu.RLock()
	defer s.oidMu.RUnlock()
	if name, ok := s.oidCache[oid]; ok {
		return name
	}
	return fmt.Sprintf("oid_%d", oid)
}

func (s *PostgresSource) getTableMetadata(ctx context.Context, db *sql.DB, schema, table string) (map[string]string, []string, error) {
	cols := make(map[string]string)
	colQuery := `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`
	rows, err := db.QueryContext(ctx, colQuery, schema, table)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name, dtype string
		if err := rows.Scan(&name, &dtype); err == nil {
			cols[name] = dtype
		}
	}

	var pks []string
	pkQuery := `SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisprimary;`
	rowsPk, err := db.QueryContext(ctx, pkQuery, schema, table)
	if err == nil {
		defer rowsPk.Close()
		for rowsPk.Next() {
			var name string
			if err := rowsPk.Scan(&name); err == nil {
				pks = append(pks, name)
			}
		}
	}

	return cols, pks, nil
}

func (s *PostgresSource) isSchemaAllowed(schema string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, a := range allowed {
		if a == schema {
			return true
		}
	}
	return false
}

func (s *PostgresSource) discoverTables(ctx context.Context, db *sql.DB, srcConfig protocol.SourceConfig, mu *sync.Mutex, msgs *[]protocol.Message, knownTables map[string]bool, triggerFlush func()) {
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg("Failed to discover tables")
		return
	}
	defer rows.Close()

	foundNew := false
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}

		if !knownTables["public."+tableName] {
			log.Info().Str("table", tableName).Msg("New table discovered")
			cols, pks, err := s.getTableMetadata(ctx, db, "public", tableName)
			if err != nil {
				log.Error().Err(err).Str("table", tableName).Msg("Failed to get metadata for discovered table")
				continue
			}

			m := protocol.Message{
				SourceID: srcConfig.ID, Table: tableName, Op: "schema_change", Timestamp: time.Now(),
				Schema: &protocol.SchemaMetadata{Table: tableName, Schema: "public", Columns: cols, PKColumns: pks},
			}
			mu.Lock()
			*msgs = append(*msgs, m)
			knownTables["public."+tableName] = true
			mu.Unlock()
			foundNew = true
		}
	}

	if foundNew {
		triggerFlush()
	}
}
