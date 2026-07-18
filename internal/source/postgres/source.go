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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
)

// metricPortCounter is the package-level fallback used only when a
// PostgresSource is not configured with an explicit metric port
// (PostgresSource.metricPort == 0). It is incremented atomically to keep
// the dynamic-port behaviour backwards compatible for callers that have
// not opted into a static port.
var metricPortCounter = uint32(20000)

// sourceRestartTotal counts every successful call to RestartWithNewTables.
// Operators rely on this metric to correlate hot-restart activity (e.g.
// dynamic table additions) with downstream behaviour. It is exported via
// the Prometheus global registry so existing scrapers pick it up
// automatically.
var sourceRestartTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "cdc_source_restart_total",
	Help: "The total number of times a PostgresSource has been restarted with new tables",
})

// connectorFactoryFunc is the pluggable factory used by PostgresSource to
// build a new cdc.Connector. The default implementation calls
// cdc.NewConnector. Tests can swap in a stub via SetConnectorFactory to
// exercise restart logic without spinning up a real PostgreSQL
// connection.
type connectorFactoryFunc func(ctx context.Context, cfg config.Config, handler replication.ListenerFunc) (cdc.Connector, error)

func defaultConnectorFactory(ctx context.Context, cfg config.Config, handler replication.ListenerFunc) (cdc.Connector, error) {
	return cdc.NewConnector(ctx, cfg, handler)
}

// PostgresSource implements the source.Source contract for PostgreSQL using
// logical replication via go-pq-cdc.
//
// Delivery contract: at-least-once.
//
// PostgresSource never advances the PostgreSQL replication slot past an LSN
// until the corresponding CDC event has been durably handed off to the
// downstream pipeline. The internal AckManager records every LSN observed
// by the replication callback and only lets the watermark advance once the
// engine has signalled that the batch carrying that LSN has been published
// to NATS (and persisted to the sink). A coordinator goroutine periodically
// flushes the watermark back to PostgreSQL via SendStandbyStatusUpdate so
// that a crash mid-batch replays the unconfirmed batch on restart rather
// than dropping it silently.
type PostgresSource struct {
	name           string
	connector      cdc.Connector
	cancel         context.CancelFunc
	ctx            context.Context
	closeOnce      sync.Once
	dsn            string
	db             *sql.DB
	lastCheckpoint protocol.Checkpoint

	// mu protects the config and connector during restarts
	mu       sync.RWMutex
	config   protocol.SourceConfig
	oidMu    sync.RWMutex
	oidCache map[uint32]string

	// metricPort is the static Prometheus metrics port used by the
	// underlying go-pq-cdc connector. When > 0 the same port is reused
	// across restarts so external scrapers continue to find the metrics
	// endpoint at a predictable address. When 0 (the default), the
	// package-level counter is incremented atomically to allocate a new
	// port on every Start, preserving the legacy behaviour.
	metricPort int

	// connectorFactory builds a new cdc.Connector. Defaults to
	// defaultConnectorFactory; tests may swap it via SetConnectorFactory
	// to avoid the real PostgreSQL connection path.
	connectorFactory connectorFactoryFunc

	// Internal channels for goroutine communication
	msgChan chan []protocol.Message
	// ackChan is the public channel returned from Start. The engine sends
	// struct{}{} on it to signal "the batch carrying the most recent LSN
	// has been durably published downstream". The replication callback
	// blocks on ackChan (instead of auto-acking) so the slot LSN never
	// advances past an unconfirmed event.
	ackChan chan struct{}
	// lsnChan is the internal channel used by the replication callback to
	// hand observed LSNs to the coordinator goroutine for watermark
	// tracking. It is not exposed to callers.
	lsnChan chan uint64
	// ackMgr owns the in-memory checkpoint watermark. The coordinator
	// goroutine is its sole Confirmer; the replication callback is its
	// sole Observer.
	ackMgr *AckManager
	runWg  sync.WaitGroup
}

func NewPostgresSource(name string) *PostgresSource {
	return &PostgresSource{
		name:             name,
		oidCache:         make(map[uint32]string),
		ackMgr:           NewAckManager(),
		connectorFactory: defaultConnectorFactory,
	}
}

// WithMetricPort configures a static Prometheus metrics port for the
// underlying go-pq-cdc connector and returns the receiver to allow
// chaining. When port > 0, every (re)start of this PostgresSource will
// expose metrics on the same port; external scrapers can therefore use a
// stable scrape target. When port == 0, the source falls back to the
// legacy behaviour of allocating a new port per restart. The default
// after NewPostgresSource is 0 (legacy behaviour).
func (s *PostgresSource) WithMetricPort(port int) *PostgresSource {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metricPort = port
	return s
}

// SetConnectorFactory replaces the factory used to build cdc.Connector
// instances. It is intended for tests that need to exercise the restart
// logic without a live PostgreSQL connection. The default factory calls
// cdc.NewConnector. Passing nil restores the default factory.
func (s *PostgresSource) SetConnectorFactory(factory connectorFactoryFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if factory == nil {
		s.connectorFactory = defaultConnectorFactory
		return
	}
	s.connectorFactory = factory
}

// resolveMetricPort returns the port that the underlying connector
// should bind to for the metrics endpoint. When s.metricPort > 0 the
// configured static port is returned unchanged on every call (the
// expected behaviour for stable Prometheus scraping). When s.metricPort
// == 0 the package-level counter is incremented atomically, matching the
// legacy dynamic-allocation behaviour.
//
// NOTE: resolveMetricPort is intentionally lock-free with respect to
// s.mu so that callers that already hold s.mu (e.g. RestartWithNewTables
// and Start) can invoke it without deadlocking on a recursive lock
// acquisition. Callers that mutate s.metricPort (WithMetricPort) take
// s.mu for synchronisation, and the read here is safe because Go's
// memory model guarantees that an int read is atomic on all supported
// architectures. The race detector would still flag a data race if a
// concurrent write happened, so WithMetricPort must be called BEFORE
// the source is shared with other goroutines (the intended usage).
func (s *PostgresSource) resolveMetricPort() int {
	if p := s.metricPort; p > 0 {
		return p
	}
	return int(atomic.AddUint32(&metricPortCounter, 1))
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
			m = protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: protocol.OpInsert, Data: sani, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}

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
			m = protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: protocol.OpUpdate, Data: sani, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}

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
			m = protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: protocol.OpSnapshot, Data: sani, Timestamp: msg.ServerTime, LSN: uint64(msg.LSN), UUID: uuid.New().String()}

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
			m = protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: protocol.OpDelete, Data: sani, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}
		}

		if m.SourceID != "" {
			*msgs = append(*msgs, m)
			mu.Unlock()

			triggerFlush()

			// Register the LSN with the in-memory checkpoint before
			// waiting for the downstream pipeline to confirm publication.
			// This guarantees the watermark can never advance past an LSN
			// the replication stream has produced but the consumer has
			// not yet acknowledged, preserving the at-least-once contract.
			lsn := uint64(lc.LSN)
			s.ackMgr.Observe(lsn)

			// Hand the observed LSN to the coordinator goroutine for
			// tracking. Use a non-blocking send: if the coordinator is
			// temporarily backed up the callback must not stall message
			// processing, and the AckManager (already updated above) is
			// the authoritative source of truth for the watermark.
			select {
			case s.lsnChan <- lsn:
			default:
			}

			// Non-blocking ack: always advance the replication stream so
			// the next event is delivered promptly, and let the
			// watermark coordinator (runAckCoordinator) own the slot
			// advancement via UpdateXLogPos. The watermark only advances
			// once the downstream pipeline has confirmed publication, so
			// the slot never gets told to fast-forward past unconfirmed
			// events.
			//
			// We still drain s.ackChan opportunistically so any signal the
			// producer managed to publish before our snapshot is consumed
			// here (otherwise the next event would re-read it and the
			// coordinator would see a stale watermark).
			select {
			case <-s.ackChan:
			default:
			}
			lc.Ack()
		} else {
			mu.Unlock()
			select {
			case <-s.ackChan:
			default:
			}
			lc.Ack()
		}
	}
}

func (s *PostgresSource) Start(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint) (<-chan []protocol.Message, chan<- struct{}, error) {
	sourceCtx, sourceCancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.config = srcConfig
	s.cancel = sourceCancel
	s.ctx = sourceCtx
	s.msgChan = make(chan []protocol.Message, 1)
	s.ackChan = make(chan struct{}, 1000)
	s.lsnChan = make(chan uint64, 1000)
	// Reset the AckManager so each Start cycle begins with a fresh
	// watermark. The watermark is hydrated from the persisted checkpoint
	// before the coordinator observes any new LSNs so resumes continue
	// from the last durable position.
	s.ackMgr = NewAckManager()
	s.lastCheckpoint = checkpoint
	if checkpoint.IngressLSN > 0 {
		// Hydrate fast-forwards the watermark past the persisted
		// checkpoint so the first UpdateXLogPos tells PostgreSQL the
		// correct resume position rather than the zero value.
		s.ackMgr.Hydrate(checkpoint.IngressLSN)
	}
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
		Metric: config.MetricConfig{Port: s.resolveMetricPort()},
	}

	if checkpoint.IngressLSN > 0 {
		cfg.StartLSN = pq.LSN(checkpoint.IngressLSN)
	}

	handler := s.createHandler(&mu, &msgs, knownTables, triggerFlush)

	var connectorErr error
	s.mu.Lock()
	connectorFactory := s.connectorFactory
	s.mu.Unlock()
	conn, connectorErr := connectorFactory(setupCtx, cfg, handler)
	s.mu.Lock()
	if connectorErr == nil {
		s.connector = conn
	}
	s.mu.Unlock()
	if connectorErr != nil || s.connector == nil {
		sourceCancel()
		return nil, nil, fmt.Errorf("failed to create connector: %w", connectorErr)
	}

	// T1-3: Capture config-derived values UNDER s.mu before spawning the
	// startConnector goroutine. The goroutine itself now takes these
	// values as parameters and never re-enters s.mu (see startConnector
	// signature). This avoids a deadlock where the spawned goroutine
	// blocks on RLock while a subsequent concurrent RestartWithNewTables
	// holds the write lock.
	batchWait := srcConfig.BatchWait
	discoveryInterval := srcConfig.DiscoveryInterval

	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		s.startConnector(conn, sourceCtx, checkpoint, &mu, &msgs, knownTables, triggerFlush, batchWait, discoveryInterval, srcConfig)
	}()

	// Spawn the ack coordinator goroutine. It is the SOLE Confirmer of the
	// AckManager: it receives LSNs handed off by the replication callback
	// over s.lsnChan and confirms them, allowing the watermark to advance.
	// It also periodically flushes the current watermark back to PostgreSQL
	// via SendStandbyStatusUpdate so the slot LSN stays in sync with the
	// actual progress. The ticker is a KEEPALIVE ONLY: it must never
	// auto-advance the watermark on its own (doing so would defeat the
	// at-least-once contract and re-introduce silent data loss).
	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		s.runAckCoordinator(sourceCtx)
	}()

	return s.msgChan, s.ackChan, nil
}

// runAckCoordinator is the long-lived goroutine that owns the AckManager
// watermark. It does two things, and only two things:
//
//  1. Consume LSNs observed by the replication callback from s.lsnChan and
//     confirm them with the AckManager. Confirming may advance the
//     contiguous watermark; the watermark is the single source of truth
//     that downstream callers (PostgreSQL SendStandbyStatusUpdate) should
//     use to advance the replication slot.
//
//  2. Every 500ms, flush the current watermark back to PostgreSQL via
//     connector.UpdateXLogPos. This is purely a keepalive / liveness
//     signal: the ticker MUST NOT auto-advance the watermark on its own.
//     Auto-advancing the watermark would silently drop any in-flight batch
//     whose ack has not yet been received from the engine, re-introducing
//     the data loss bug fixed in T0-1.
func (s *PostgresSource) runAckCoordinator(ctx context.Context) {
	const keepaliveInterval = 500 * time.Millisecond
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()

	// lastFlushedWatermark remembers the last watermark we successfully
	// pushed to PostgreSQL so we avoid sending the same standby status
	// update on every tick when there is no progress (which would be a
	// no-op for the upstream library but still wastes a network round
	// trip and clutters the postgres log).
	var lastFlushedWatermark uint64

	for {
		select {
		case <-ctx.Done():
			return
		case lsn, ok := <-s.lsnChan:
			if !ok {
				return
			}
			s.ackMgr.Confirm(lsn)
		case <-ticker.C:
			wm := s.ackMgr.Watermark()
			if wm == 0 || wm == lastFlushedWatermark {
				continue
			}
			s.mu.RLock()
			conn := s.connector
			s.mu.RUnlock()
			if conn == nil {
				continue
			}
			conn.UpdateXLogPos(pq.LSN(wm))
			lastFlushedWatermark = wm
		}
	}
}

func (s *PostgresSource) startConnector(conn cdc.Connector, sourceCtx context.Context, checkpoint protocol.Checkpoint, mu *sync.Mutex, msgs *[]protocol.Message, knownTables map[string]bool, triggerFlush func(), batchWait time.Duration, discoveryInterval time.Duration, srcConfig protocol.SourceConfig) {
	log.Info().Uint64("lsn", checkpoint.IngressLSN).Msg("Starting connector loop")

	if batchWait == 0 {
		batchWait = 500 * time.Millisecond
	}
	if discoveryInterval <= 0 {
		discoveryInterval = 30 * time.Second
	}

	// Prime initial schemas synchronously BEFORE starting connector to prevent race with first data messages
	for _, t := range srcConfig.Tables {
		cleanTable := strings.TrimPrefix(t, "public.")
		// T1-24 / T1-3 hardening: s.db may be nil when startConnector is
		// invoked outside the Start path (e.g. by a test that swaps the
		// connector factory). In that case there is no live database to
		// query, so schema priming is a no-op — the caller is responsible
		// for seeding any metadata it needs.
		if s.db == nil {
			break
		}
		cols, pks, err := s.getTableMetadata(sourceCtx, s.db, "public", cleanTable)
		if err == nil {
			m := protocol.Message{
				SourceID: srcConfig.ID, Table: cleanTable, Op: protocol.OpSchemaChange, Timestamp: time.Now(),
				Schema: &protocol.SchemaMetadata{Table: cleanTable, Schema: "public", Columns: cols, PKColumns: pks},
			}
			mu.Lock()
			*msgs = append(*msgs, m)
			mu.Unlock()
			triggerFlush()
		}
	}

	// T1-24: Register the batch-wait ticker with s.runWg so Stop() and
	// RestartWithNewTables can wait for it before tearing down shared
	// resources (e.g. the *sql.DB handle) that the ticker may be about to
	// touch. Previously this goroutine was detached, so Stop() could close
	// the DB out from under it while a ticker iteration was in flight,
	// producing a data-race (and the occasional nil-pointer dereference).
	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
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

	// T1-24: Same rationale as the batch-wait ticker above — the table
	// discovery goroutine issues QueryContext against s.db, so it must
	// exit before Stop() closes the DB.
	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		ticker := time.NewTicker(discoveryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-sourceCtx.Done():
				return
			case <-ticker.C:
				// T1-24 / T1-3 hardening: s.db may be nil in tests
				// (see schema priming comment above). Skip the
				// iteration rather than panic on a nil DB handle.
				if s.db == nil {
					continue
				}
				s.discoverTables(sourceCtx, s.db, srcConfig, mu, msgs, knownTables, triggerFlush)
			}
		}
	}()

	if checkpoint.IngressLSN > 0 {
		conn.UpdateXLogPos(pq.LSN(checkpoint.IngressLSN))
	}

	conn.Start(sourceCtx)

	// T1-3: Cleanup goroutine. Closes the channel that THIS session owned
	// (captured at launch time), not whatever value s.msgChan holds now
	// — by the time we wake up, RestartWithNewTables may have reallocated
	// a fresh channel and we must NOT close it (that would panic the new
	// session). The goroutine is registered in s.runWg so callers
	// (RestartWithNewTables / Stop) can wait for it to exit before
	// reallocating new channels or closing shared resources. We also do
	// NOT take s.mu here: holding it would deadlock with the
	// runWg.Wait() that RestartWithNewTables issues while holding s.mu.
	msgChan := s.msgChan
	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		<-sourceCtx.Done()
		log.Info().Str("source", s.name).Msg("PostgresSource: Context canceled, closing message channel")
		triggerFlush()
		time.Sleep(100 * time.Millisecond)
		close(msgChan)
	}()
}

func (s *PostgresSource) UpdateXLogPos(ctx context.Context, lsn uint64) error {
	// T1-2: persist the in-memory checkpoint BEFORE forwarding the new
	// LSN to the connector. Without this assignment, lastCheckpoint is
	// never advanced from the value passed into Start, so on restart the
	// pipeline would replay from a stale LSN and the in-memory
	// "ingress LSN" diverges from the value that was actually forwarded
	// to PostgreSQL. Holding the write lock here also serialises the
	// checkpoint update against concurrent UpdateXLogPos calls.
	s.mu.Lock()
	s.lastCheckpoint.IngressLSN = lsn
	s.lastCheckpoint.UpdatedAt = time.Now().UTC()
	connector := s.connector
	s.mu.Unlock()
	if connector != nil {
		connector.UpdateXLogPos(pq.LSN(lsn))
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
	// T1-25: Bump the restart counter up front so that an early failure
	// (e.g. invalid cfg) is still visible to operators. The counter is
	// package-level so a single Prometheus scrape covers all sources.
	sourceRestartTotal.Inc()

	// T1-3: Acquire s.mu as a write lock for the entire reallocation
	// sequence. We MUST serialise this against concurrent UpdateXLogPos
	// calls (which also take s.mu) so that the new channels and the
	// new cancel func are published atomically to other observers.
	// The lock is released before spawning the new startConnector
	// goroutine (see comment near the spawn).
	s.mu.Lock()
	deferMu := true
	defer func() {
		if deferMu {
			s.mu.Unlock()
		}
	}()

	log.Info().Strs("tables", newTables).Msg("Restarting source with new tables")

	// T1-3: Tear down the previous session. Order matters:
	//   1. Cancel the old context so the old connector's Start() returns
	//      and the goroutines inside startConnector() begin to wind down.
	//   2. Close the old connector so any in-flight replication connection
	//      is released before we reallocate state.
	//   3. Wait for runWg to drain. This is the critical step: the old
	//      session's cleanup goroutine sleeps for 100 ms and then closes
	//      the channel it captured at launch. If we did NOT wait, the
	//      old goroutine could close the freshly-allocated msgChan and
	//      panic the new session.
	if s.cancel != nil {
		s.cancel()
	}
	if s.connector != nil {
		s.connector.Close()
	}
	s.runWg.Wait()

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
		// T1-25: Use the configured static port when available so
		// external Prometheus scrapers continue to find the endpoint at
		// the same address across hot-restarts. When unset (0), fall
		// back to the package-level dynamic counter.
		Metric: config.MetricConfig{Port: s.resolveMetricPort()},
	}

	// T1-3: Allocate a fresh msgChan for the new session. The cleanup
	// goroutine of the previous session captured the old channel by
	// value and is already on its way out (runWg.Wait above returned),
	// so this reallocation is safe and races against no one.
	s.msgChan = make(chan []protocol.Message, 1)

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

	// T1-25: Route connector creation through the (potentially swapped)
	// factory so tests can stub the real cdc.NewConnector.
	conn, err := s.connectorFactory(setupCtx, cfg, handler)
	if err != nil {
		return err
	}
	s.connector = conn

	ctxWithCancel, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.ctx = ctxWithCancel

	// T1-3: Capture config-derived values under s.mu (which we still
	// hold) so the spawned startConnector goroutine does NOT need to
	// re-enter s.mu. This eliminates a deadlock where the goroutine
	// would block on RLock while a subsequent concurrent
	// RestartWithNewTables is holding the write lock.
	batchWait := s.config.BatchWait
	discoveryInterval := s.config.DiscoveryInterval
	srcConfigCopy := s.config

	// T1-3: Release s.mu BEFORE spawning the new startConnector goroutine
	// so the goroutine can begin running without contending on the lock
	// (even though startConnector no longer takes the lock, this keeps
	// the lock held window minimal for the common Stop/Restart race).
	s.mu.Unlock()
	deferMu = false

	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		s.startConnector(conn, ctxWithCancel, s.lastCheckpoint, &mu, &msgs, knownTables, triggerFlush, batchWait, discoveryInterval, srcConfigCopy)
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
				SourceID: srcConfig.ID, Table: tableName, Op: protocol.OpSchemaChange, Timestamp: time.Now(),
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
