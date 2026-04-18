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
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog/log"
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
	runWg   sync.WaitGroup
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

func (s *PostgresSource) createHandler(mu *sync.Mutex, msgs *[]protocol.Message, knownTables map[string]bool, triggerFlush func()) func(lc *replication.ListenerContext) {
	return func(lc *replication.ListenerContext) {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Str("source", s.name).Interface("recover", r).Msg("PostgresSource RECOVERED from handler panic")
			}
		}()

		mu.Lock()
		var m protocol.Message
		var tableName string

		switch msg := lc.Message.(type) {
		case *format.Relation:
			s.oidMu.Lock()
			s.oidCache[msg.OID] = msg.Name
			s.oidMu.Unlock()
			log.Info().Str("table", msg.Name).Uint32("oid", msg.OID).Msg("PostgresSource: Received relation")
			mu.Unlock()
			lc.Ack()
			return

		case *format.Insert:
			tableName = msg.TableName
			if tableName == "" {
				s.oidMu.RLock()
				tableName = s.oidCache[msg.OID]
				s.oidMu.RUnlock()
			}
			
			cleanName := strings.TrimPrefix(tableName, "public.")
			if tableName == "" || !knownTables[cleanName] {
				mu.Unlock()
				lc.Ack()
				return
			}
			sani := sanitizePayload(msg.Decoded)
			m = protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: "insert", Data: sani, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}

		case *format.Update:
			tableName = msg.TableName
			if tableName == "" {
				s.oidMu.RLock()
				tableName = s.oidCache[msg.OID]
				s.oidMu.RUnlock()
			}

			cleanName := strings.TrimPrefix(tableName, "public.")
			if tableName == "" || !knownTables[cleanName] {
				mu.Unlock()
				lc.Ack()
				return
			}
			sani := sanitizePayload(msg.NewDecoded)
			m = protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: "update", Data: sani, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}

		case *format.Snapshot:
			if msg.EventType != format.SnapshotEventTypeData {
				mu.Unlock()
				lc.Ack()
				return
			}
			tableName = msg.Table
			cleanName := strings.TrimPrefix(tableName, "public.")
			if tableName == "" || !knownTables[cleanName] {
				mu.Unlock()
				lc.Ack()
				return
			}
			sani := sanitizePayload(msg.Data)
			m = protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: "snapshot", Data: sani, Timestamp: msg.ServerTime, LSN: uint64(msg.LSN), UUID: uuid.New().String()}

		case *format.Delete:
			tableName = msg.TableName
			if tableName == "" {
				s.oidMu.RLock()
				tableName = s.oidCache[msg.OID]
				s.oidMu.RUnlock()
			}

			cleanName := strings.TrimPrefix(tableName, "public.")
			if tableName == "" || !knownTables[cleanName] {
				mu.Unlock()
				lc.Ack()
				return
			}
			sani := sanitizePayload(msg.OldDecoded)
			m = protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: "delete", Data: sani, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}
		}

		if m.SourceID != "" {
			*msgs = append(*msgs, m)
			mu.Unlock()
			
			triggerFlush()

			select {
			case <-s.ackChan:
				lc.Ack()
			default:
				lc.Ack()
			}
		} else {
			mu.Unlock()
			lc.Ack()
		}
	}
}

func (s *PostgresSource) Start(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint) (<-chan []protocol.Message, chan<- struct{}, error) {
	sourceCtx, sourceCancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.config = srcConfig
	s.cancel = sourceCancel
	s.msgChan = make(chan []protocol.Message, 1)
	s.ackChan = make(chan struct{}, 1000)
	s.mu.Unlock()

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
		sourceCancel()
		return nil, nil, fmt.Errorf("failed to open DB: %w", err)
	}

	if err := s.primeOIDCache(setupCtx, s.db); err != nil {
		log.Warn().Err(err).Msg("Failed to prime OID cache")
	}

	var mu sync.Mutex
	var msgs []protocol.Message
	knownTables := make(map[string]bool)
	for _, t := range srcConfig.Tables {
		cleanTable := strings.TrimPrefix(t, "public.")
		knownTables["public."+cleanTable] = true
		knownTables[cleanTable] = true
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
			log.Debug().Any("data", mCopy).Msg("Source data sent to message channel")
		case <-sourceCtx.Done():
		}
	}

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

	if checkpoint.IngressLSN > 0 {
		cfg.StartLSN = pq.LSN(checkpoint.IngressLSN)
	}

	handler := s.createHandler(&mu, &msgs, knownTables, triggerFlush)

	var connectorErr error
	s.mu.Lock()
	s.connector, connectorErr = cdc.NewConnector(setupCtx, cfg, handler)
	s.mu.Unlock()
	if connectorErr != nil || s.connector == nil {
		sourceCancel()
		return nil, nil, fmt.Errorf("failed to create connector: %w", connectorErr)
	}

	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		s.startConnector(sourceCtx, checkpoint, &mu, &msgs, knownTables, triggerFlush)
	}()

	return s.msgChan, s.ackChan, nil
}

func (s *PostgresSource) startConnector(sourceCtx context.Context, checkpoint protocol.Checkpoint, mu *sync.Mutex, msgs *[]protocol.Message, knownTables map[string]bool, triggerFlush func()) {
	log.Info().Uint64("lsn", checkpoint.IngressLSN).Msg("Starting connector loop")

	s.mu.RLock()
	batchWait := s.config.BatchWait
	discoveryInterval := s.config.DiscoveryInterval
	srcConfig := s.config
	s.mu.RUnlock()

	if batchWait == 0 {
		batchWait = 500 * time.Millisecond
	}
	if discoveryInterval <= 0 {
		discoveryInterval = 30 * time.Second
	}

	// Prime initial schemas synchronously BEFORE starting connector to prevent race with first data messages
	for _, t := range srcConfig.Tables {
		cleanTable := strings.TrimPrefix(t, "public.")
		cols, pks, err := s.getTableMetadata(sourceCtx, s.db, "public", cleanTable)
		if err == nil {
			m := protocol.Message{
				SourceID: srcConfig.ID, Table: cleanTable, Op: "schema_change", Timestamp: time.Now(),
				Schema: &protocol.SchemaMetadata{Table: cleanTable, Schema: "public", Columns: cols, PKColumns: pks},
			}
			mu.Lock()
			*msgs = append(*msgs, m)
			mu.Unlock()
			triggerFlush()
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
		ticker := time.NewTicker(discoveryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-sourceCtx.Done():
				return
			case <-ticker.C:
				s.discoverTables(sourceCtx, s.db, srcConfig, mu, msgs, knownTables, triggerFlush)
			}
		}
	}()

	if checkpoint.IngressLSN > 0 {
		s.connector.UpdateXLogPos(pq.LSN(checkpoint.IngressLSN))
	}

	s.connector.Start(sourceCtx)

	go func() {
		<-sourceCtx.Done()
		log.Info().Str("source", s.name).Msg("PostgresSource: Context canceled, closing message channel")
		triggerFlush()
		time.Sleep(100 * time.Millisecond)
		s.mu.Lock()
		close(s.msgChan)
		s.mu.Unlock()
	}()
}

func (s *PostgresSource) UpdateXLogPos(ctx context.Context, lsn uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.connector != nil {
		s.connector.UpdateXLogPos(pq.LSN(lsn))
	}
	return nil
}

func (s *PostgresSource) Stop() error {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()

	s.runWg.Wait()

	s.closeOnce.Do(func() {
		log.Info().Str("source", s.name).Msg("PostgresSource: Closing resources")
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

	log.Info().Strs("tables", newTables).Msg("Restarting source with new tables")

	if s.cancel != nil {
		s.cancel()
		s.connector.Close()
	}

	s.config.Tables = append(s.config.Tables, newTables...)

	pubTables := make(publication.Tables, len(s.config.Tables))
	for i, t := range s.config.Tables {
		pubTables[i] = publication.Table{Name: t, ReplicaIdentity: "DEFAULT"}
	}

	cfg := config.Config{
		Host: s.config.Host, Port: s.config.Port, Username: s.config.User, Password: s.config.PassEncrypted, Database: s.config.Database,
		Slot: slot.Config{Name: s.config.SlotName, CreateIfNotExists: true},
		Publication: publication.Config{
			Name: s.config.PublicationName, CreateIfNotExists: true, Tables: pubTables,
			Operations: publication.Operations{publication.OperationInsert, publication.OperationUpdate, publication.OperationDelete},
		},
		Snapshot: config.SnapshotConfig{
			Enabled: false,
		},
		Metric: config.MetricConfig{Port: int(atomic.AddUint32(&metricPortCounter, 1))},
	}

	var mu sync.Mutex
	var msgs []protocol.Message
	knownTables := make(map[string]bool)
	for _, t := range s.config.Tables {
		cleanTable := strings.TrimPrefix(t, "public.")
		knownTables["public."+cleanTable] = true
		knownTables[cleanTable] = true
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
		default:
		}
	}

	handler := s.createHandler(&mu, &msgs, knownTables, triggerFlush)
	
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelSetup()
	
	conn, err := cdc.NewConnector(setupCtx, cfg, handler)
	if err != nil {
		return err
	}
	s.connector = conn
	
	ctxWithCancel, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	
	go func() {
		s.startConnector(ctxWithCancel, s.lastCheckpoint, &mu, &msgs, knownTables, triggerFlush)
	}()

	return nil
}

func (s *PostgresSource) AlterPublication(ctx context.Context, tableName string) error {
	s.mu.RLock()
	db := s.db
	pubName := s.config.PublicationName
	s.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("database connection not initialized")
	}

	// Retry logic for "publication does not exist" which can happen due to replication lag or race
	var lastErr error
	for i := 0; i < 10; i++ {
		execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		query := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", pubName, tableName)
		_, err := db.ExecContext(execCtx, query)
		cancel()

		if err == nil {
			log.Info().Str("table", tableName).Msg("Table added to publication")
			return nil
		}

		lastErr = err
		// 42704: undefined_object (publication does not exist)
		if strings.Contains(err.Error(), "42704") {
			log.Warn().Err(err).Str("table", tableName).Int("attempt", i+1).Msg("Publication not found, retrying...")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				continue
			}
		}

		// 42710: duplicate_object (already member)
		if strings.Contains(err.Error(), "42710") || strings.Contains(err.Error(), "already member") {
			return nil
		}

		break
	}

	log.Error().Err(lastErr).Str("table", tableName).Msg("Failed to add table to publication after retries")
	return fmt.Errorf("failed to add table to publication after retries: %w", lastErr)
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
