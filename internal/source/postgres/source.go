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
	name           string
	connector      cdc.Connector
	cancel         context.CancelFunc
	closeOnce      sync.Once
	dsn            string
	db             *sql.DB
	lastCheckpoint protocol.Checkpoint

	// mu protects the config and connector during restarts
	mu       sync.RWMutex
	config   protocol.SourceConfig
	oidMu    sync.RWMutex
	oidCache map[uint32]string

	// Internal channels for goroutine communication
	msgChan chan []protocol.Message
	ackChan chan struct{}
	// runWg   sync.WaitGroup
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
	s.mu.Lock()
	s.config = srcConfig
	s.msgChan = make(chan []protocol.Message, 1)
	s.ackChan = make(chan struct{})
	s.mu.Unlock()

	go func() {
		s.startConnector(ctx, checkpoint)
	}()

	return s.msgChan, s.ackChan, nil
}

func (s *PostgresSource) startConnector(ctx context.Context, checkpoint protocol.Checkpoint) {
	s.mu.RLock()
	srcConfig := s.config
	s.mu.RUnlock()

	log.Info().Int64("lsn", int64(checkpoint.IngressLSN)).Msg("Starting connector with checkpoint")

	sourceCtx, sourceCancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancel = sourceCancel
	s.mu.Unlock()

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

	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelSetup()

	u := &url.URL{
		Scheme: "postgres", Host: fmt.Sprintf("%s:%d", srcConfig.Host, srcConfig.Port),
		User: url.UserPassword(srcConfig.User, srcConfig.PassEncrypted), Path: srcConfig.Database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	dsn := u.String()
	s.dsn = dsn

	var err error
	s.db, err = sql.Open("pgx", dsn)
	if err != nil {
		log.Error().Err(err).Msg("Failed to open DB for source")
		return
	}
	defer s.db.Close()

	if err := s.primeOIDCache(setupCtx, s.db); err != nil {
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
		case s.msgChan <- mCopy:
		case <-sourceCtx.Done():
		}
	}

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

	go func() {
		for _, t := range srcConfig.Tables {
			cols, pks, err := s.getTableMetadata(sourceCtx, s.db, "public", t)
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
				s.discoverTables(sourceCtx, s.db, srcConfig, &mu, &msgs, knownTables, triggerFlush)
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
		Metric: config.MetricConfig{Port: int(atomic.AddUint32(&metricPortCounter, 1))},
	}

	handler := func(lc *replication.ListenerContext) {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Str("slot", srcConfig.SlotName).Interface("recover", r).Msg("PostgresSource RECOVERED from handler panic")
			}
		}()

		mu.Lock()
		var m protocol.Message
		switch msg := lc.Message.(type) {
		case *format.Relation:
			log.Debug().
				Str("table", msg.Name).
				Uint32("oid", msg.OID).
				Int("cols", len(msg.Columns)).
				Msg("PostgresSource: Received Relation")
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
			log.Debug().
				Str("table", msg.TableName).
				Interface("data", msg.Decoded).
				Msg("PostgresSource: Received Insert")
			if strings.HasPrefix(msg.TableName, "cdc_snapshot_") {
				mu.Unlock()
				lc.Ack()
				return
			}
			sani := sanitizePayload(msg.Decoded)
			payload, err := msgpack.Marshal(sani)
			if err != nil {
				log.Error().Err(err).Str("table", msg.TableName).Interface("raw_data", msg.Decoded).Msg("Error marshaling insert")
				mu.Unlock()
				lc.Ack()
				return
			}
			m = protocol.Message{SourceID: srcConfig.ID, Table: msg.TableName, Op: "insert", Payload: payload, Data: sani, Timestamp: msg.MessageTime}
		case *format.Update:
			log.Debug().
				Str("table", msg.TableName).
				Interface("old_data", msg.OldDecoded).
				Interface("new_data", msg.NewDecoded).
				Msg("PostgresSource: Received Update")
			if strings.HasPrefix(msg.TableName, "cdc_snapshot_") {
				mu.Unlock()
				lc.Ack()
				return
			}
			sani := sanitizePayload(msg.NewDecoded)
			payload, err := msgpack.Marshal(sani)
			if err != nil {
				log.Error().Err(err).Str("table", msg.TableName).Interface("raw_new_data", msg.NewDecoded).Msg("Error marshaling update")
				mu.Unlock()
				lc.Ack()
				return
			}
			m = protocol.Message{SourceID: srcConfig.ID, Table: msg.TableName, Op: "update", Payload: payload, Data: sani, Timestamp: msg.MessageTime}
		case *format.Delete:
			log.Debug().
				Str("table", msg.TableName).
				Interface("old_data", msg.OldDecoded).
				Msg("PostgresSource: Received Delete")
			if strings.HasPrefix(msg.TableName, "cdc_snapshot_") {
				mu.Unlock()
				lc.Ack()
				return
			}
			sani := sanitizePayload(msg.OldDecoded)
			payload, err := msgpack.Marshal(sani)
			if err != nil {
				log.Error().Err(err).Str("table", msg.TableName).Interface("raw_old_data", msg.OldDecoded).Msg("Error marshaling delete")
				mu.Unlock()
				lc.Ack()
				return
			}
			m = protocol.Message{SourceID: srcConfig.ID, Table: msg.TableName, Op: "delete", Payload: payload, Data: sani, Timestamp: msg.MessageTime}
		case *format.Snapshot:
			log.Debug().
				Str("table", msg.Table).
				Uint64("lsn", uint64(msg.LSN)).
				Str("event", string(msg.EventType)).
				Msg("PostgresSource: Received Snapshot")
			if strings.HasPrefix(msg.Table, "cdc_snapshot_") {
				mu.Unlock()
				lc.Ack()
				return
			}
			if msg.EventType == format.SnapshotEventTypeData {
				sani := sanitizePayload(msg.Data)
				payload, err := msgpack.Marshal(sani)
				if err != nil {
					log.Error().Err(err).Str("table", msg.Table).Interface("raw_data", msg.Data).Msg("Error marshaling snapshot")
					mu.Unlock()
					lc.Ack()
					return
				}
				m = protocol.Message{SourceID: srcConfig.ID, Table: msg.Table, Op: "snapshot", LSN: uint64(msg.LSN), Payload: payload, Data: sani, Timestamp: msg.ServerTime}
			}
		}

		if m.SourceID != "" {
			s.lastCheckpoint = protocol.Checkpoint{
				IngressLSN: m.LSN,
			}
			msgs = append(msgs, m)
			mu.Unlock()
			if m.Op == "snapshot" && len(msgs) >= 1000 {
				triggerFlush()
			}

			select {
			case <-s.ackChan:
				lc.Ack()
			case <-sourceCtx.Done():
			}
		} else {
			mu.Unlock()
			lc.Ack()
		}
	}

	var connectorErr error
	s.mu.Lock()
	s.connector, connectorErr = cdc.NewConnector(setupCtx, cfg, handler)
	s.mu.Unlock()
	if connectorErr != nil {
		log.Error().Err(connectorErr).Msg("Failed to create CDC connector")
		s.mu.Lock()
		if s.cancel != nil {
			s.cancel()
		}
		s.mu.Unlock()
		return
	}

	s.connector.Start(sourceCtx)
}

func (s *PostgresSource) Stop() error {
	s.closeOnce.Do(func() {
		log.Info().Str("source", s.name).Msg("PostgresSource: Stopping...")
		s.mu.Lock()
		if s.cancel != nil {
			s.cancel()
		}
		s.mu.Unlock()
		// s.runWg.Wait()

		s.mu.Lock()
		if s.connector != nil {
			s.connector.Close()
		}
		if s.db != nil {
			s.db.Close()
		}
		s.mu.Unlock()
	})
	return nil
}

func (s *PostgresSource) RestartWithNewTables(ctx context.Context, newTables []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Info().Strs("tables", newTables).Msg("Gracefully restarting source with new tables")

	if s.cancel != nil {
		go func() {
			s.cancel()
			s.connector.Close()
		}()
	}
	// s.runWg.Wait()

	db, err := sql.Open("pgx", s.dsn)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create connection for restart")
		return err
	}

	s.db = db
	log.Info().Strs("tables", newTables).Msg("Altering the publication")

	for _, table := range newTables {
		if err := s.AlterPublication(ctx, table); err != nil {
			log.Error().Err(err).Str("table", table).Msg("Failed to add new table to publication during restart")
		}
	}

	s.config.Tables = append(s.config.Tables, newTables...)

	// s.runWg.Add(1)
	log.Info().Strs("tables", newTables).Msg("Restarting the connector")

	go func() {
		// defer s.runWg.Done()

		if s.lastCheckpoint.IngressLSN != 0 {
			s.startConnector(ctx, s.lastCheckpoint)
		} else {
			s.startConnector(ctx, protocol.Checkpoint{IngressLSN: 0})
		}
	}()

	log.Info().Strs("tables", newTables).Msg("Source restart complete")
	return nil
}

// ... (Other helper functions like primeOIDCache, resolveTypeName, etc. go here)
func (s *PostgresSource) formatDataMessage(msg any, sourceID string) protocol.Message {
	var tableName, op string
	var payload []byte
	var data map[string]any
	var msgTime time.Time
	var lsn uint64

	switch m := msg.(type) {
	case *format.Insert:
		tableName, op, data, msgTime = m.TableName, "insert", m.Decoded, m.MessageTime
	case *format.Update:
		tableName, op, data, msgTime = m.TableName, "update", m.NewDecoded, m.MessageTime
	case *format.Delete:
		tableName, op, data, msgTime = m.TableName, "delete", m.OldDecoded, m.MessageTime
	case *format.Snapshot:
		if m.EventType != format.SnapshotEventTypeData {
			return protocol.Message{}
		}
		tableName, op, data, msgTime, lsn = m.Table, "snapshot", m.Data, m.ServerTime, uint64(m.LSN)
	default:
		return protocol.Message{}
	}

	if strings.HasPrefix(tableName, "cdc_snapshot_") {
		return protocol.Message{}
	}

	saniData := sanitizePayload(data)
	payload, err := msgpack.Marshal(saniData)
	if err != nil {
		log.Error().Err(err).Str("table", tableName).Interface("raw_data", data).Msg("Error marshaling data message")
		return protocol.Message{}
	}

	return protocol.Message{SourceID: sourceID, Table: tableName, Op: op, Payload: payload, Data: saniData, Timestamp: msgTime, LSN: lsn}
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
			if strings.Contains(tableName, "cdc_snapshot") {
				knownTables["public."+tableName] = true
				continue
			}

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

func (s *PostgresSource) AlterPublication(ctx context.Context, tableName string) error {
	if s.db == nil {
		return fmt.Errorf("database connection not initialized")
	}
	query := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", s.config.PublicationName, tableName)
	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Str("table", tableName).Msg("Failed to add table to publication")
		return err
	}
	log.Info().Str("table", tableName).Msg("Table added to publication via AlterPublication")
	return nil
}
