package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/NurfitraPujo/cdc-pipeline/internal/source"
	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
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

// pendingLSNsGauge exports AckManager.PendingCount() per source. A
// permanent non-zero value indicates the watermark is stalled: either a
// downstream sink is not confirming, or a "ghost" entry exists (a Confirm
// for an LSN that was never Observed) which pins the watermark and blocks
// IdleAdvance forever. Full slot-lag alerting on top of this is WI-5a.
var pendingLSNsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cdc_source_pending_lsns",
	Help: "Number of observed-or-confirmed LSNs not yet folded into the AckManager watermark",
}, []string{"source"})

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

	// kv and pipelineID are optional observability plumbing for
	// persistWatermark (WI-7 §3): when kv is non-nil, runAckCoordinator
	// best-effort writes the current watermark to
	// protocol.SourceWatermarkKey(pipelineID, s.config.ID), rate-limited
	// to once per second. Neither is required for correctness -- when kv
	// is nil (the default), persistWatermark is a no-op. Set via WithKV.
	kv         nats.KeyValue
	pipelineID string

	// slotConfirmedFlushLSN resolves the given slot's confirmed_flush_lsn
	// (WI-7 B3 seed). Defaults to queryConfirmedFlushLSN (a real,
	// short-lived query against s.db); tests override it to exercise the
	// seed logic without a live PostgreSQL connection. ok is false
	// whenever the value could not be determined (no rows yet, query
	// error, parse error) -- callers treat that as "leave unseeded",
	// never as "seed with zero".
	slotConfirmedFlushLSN func(ctx context.Context, db *sql.DB, slotName string) (lsn pq.LSN, ok bool)

	// Internal channels for goroutine communication
	msgChan chan []protocol.Message
	// ackChan is the public channel returned from Start. The engine sends
	// source.SourceAck{SinkID, LSNs} on it once a sink has durably written
	// a batch of LSNs. runAckCoordinator is the SOLE reader: it is the only
	// path that feeds AckManager.Confirm, which is what allows the
	// watermark (and therefore the replication slot) to advance.
	ackChan chan source.SourceAck
	// ackMgr owns the in-memory checkpoint watermark. The coordinator
	// goroutine is its sole Confirmer; the replication callback is its
	// sole Observer.
	ackMgr *AckManager
	runWg  sync.WaitGroup
}

func NewPostgresSource(name string) *PostgresSource {
	return &PostgresSource{
		name:                  name,
		oidCache:              make(map[uint32]string),
		ackMgr:                NewAckManager(nil),
		connectorFactory:      defaultConnectorFactory,
		slotConfirmedFlushLSN: queryConfirmedFlushLSN,
	}
}

// queryConfirmedFlushLSN is the real (non-test) implementation of
// PostgresSource.slotConfirmedFlushLSN: it queries pg_replication_slots
// for the given slot's confirmed_flush_lsn over an existing *sql.DB. It
// is used both by the pre-Start fast path (slot already existed from a
// prior process) and by the post-WaitUntilReady path (freshly-created
// slot, WI-7 B3). Any error (nil db, query, scan, parse, or a
// zero/unparseable LSN) reports ok=false; callers treat that as "leave
// unseeded", since seeding 0 would be a no-op and this must never block
// or fail startup.
func queryConfirmedFlushLSN(ctx context.Context, db *sql.DB, slotName string) (pq.LSN, bool) {
	if db == nil || slotName == "" {
		return 0, false
	}

	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var confirmedFlushLSN string
	err := db.QueryRowContext(qctx,
		`SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1`,
		slotName,
	).Scan(&confirmedFlushLSN)
	if err != nil {
		log.Debug().Err(err).Str("slot", slotName).Msg("queryConfirmedFlushLSN: failed to read confirmed_flush_lsn")
		return 0, false
	}

	lsn, err := pq.ParseLSN(confirmedFlushLSN)
	if err != nil {
		log.Debug().Err(err).Str("confirmed_flush_lsn", confirmedFlushLSN).Msg("queryConfirmedFlushLSN: failed to parse confirmed_flush_lsn")
		return 0, false
	}
	if lsn == 0 {
		return 0, false
	}

	return lsn, true
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

// WithKV configures the NATS KV bucket and pipeline ID used by
// persistWatermark (WI-7 §3) to write a best-effort observability record
// of the current watermark. It is optional: callers that never invoke
// WithKV get the pre-WI-7 behaviour (no watermark KV write). Intended to
// be called before the source is shared with other goroutines, matching
// WithMetricPort's usage contract.
func (s *PostgresSource) WithKV(pipelineID string, kv nats.KeyValue) *PostgresSource {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pipelineID = pipelineID
	s.kv = kv
	return s
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

// handlerKind classifies what buildMessage did with a single replication
// event, so the (unlocked) outer closure knows what follow-up action —
// triggerFlush / AckManager bookkeeping — to take.
type handlerKind int

const (
	// handlerKindFiltered covers every early-return branch: Relation
	// messages, unmatched tables, non-data snapshot events, and any
	// message type the switch below does not recognise. These self-ack
	// through AckManager.ObserveConfirmed so they can never stall the
	// watermark waiting on a downstream sink that will never see them.
	handlerKindFiltered handlerKind = iota
	// handlerKindData is an Insert/Update/Delete event appended to the
	// pending batch; it must be Observe'd so the watermark waits for a
	// matching downstream confirm before advancing past it.
	handlerKindData
	// handlerKindSnapshot is a snapshot data row appended to the pending
	// batch with LSN zeroed. Snapshot rows are checkpointed by chunk, not
	// by LSN, so they are deliberately never Observed.
	handlerKindSnapshot
)

// handlerResult is buildMessage's report back to the outer, unlocked
// closure. lsn is only meaningful for handlerKindFiltered/handlerKindData.
type handlerResult struct {
	kind handlerKind
	lsn  uint64
}

// cacheRelation records an OID -> table-name mapping. It exists so the
// write happens under a DEFERRED unlock: a nil-map assignment (or any other
// panic) must not strand s.oidMu, which would deadlock every subsequent
// handler invocation and silently wedge the source. Same reasoning as
// buildMessage's own deferred unlock.
func (s *PostgresSource) cacheRelation(oid uint32, name string) {
	s.oidMu.Lock()
	defer s.oidMu.Unlock()
	// Deliberately NOT nil-guarded: the map is initialised in
	// NewPostgresSource, and TestHandler_PanicSafety_MuNotStranded forces a
	// nil map here precisely to prove that a panic under this lock is
	// survivable. Adding a guard would make that test vacuous.
	s.oidCache[oid] = name
}

// lookupRelationName resolves an OID to a cached table name under a
// deferred read-unlock, for the same panic-safety reason as cacheRelation.
func (s *PostgresSource) lookupRelationName(oid uint32) string {
	s.oidMu.RLock()
	defer s.oidMu.RUnlock()
	return s.oidCache[oid]
}

// buildMessage performs the entire message-construction critical section
// under mu, guarded by a deferred Unlock so that a panic anywhere inside
// (sanitizePayload, the OID cache, message construction) unwinds through
// the deferred Unlock before it reaches the outer closure's recover().
// Before this split, a panic between an explicit Lock and Unlock left mu
// permanently held, wedging every subsequent event (including the
// batch-wait ticker's triggerFlush, which blocks on the same mutex).
//
// buildMessage must never perform a blocking operation (triggerFlush,
// AckManager calls, channel sends) — those happen in the caller, after
// this method returns and mu has been released.
func (s *PostgresSource) buildMessage(lc *replication.ListenerContext, mu *sync.Mutex, msgs *[]protocol.Message, knownTables map[string]bool) handlerResult {
	mu.Lock()
	defer mu.Unlock()

	switch msg := lc.Message.(type) {
	case *format.Relation:
		s.cacheRelation(msg.OID, msg.Name)
		log.Info().Str("table", msg.Name).Uint32("oid", msg.OID).Msg("PostgresSource: Received relation")
		return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}

	case *format.Insert:
		tableName := msg.TableName
		if tableName == "" {
			tableName = s.lookupRelationName(msg.OID)
		}

		cleanName := strings.TrimPrefix(tableName, "public.")
		if tableName == "" || !knownTables[cleanName] {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		sani := sanitizePayload(msg.Decoded)
		m := protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: protocol.OpInsert, Data: sani, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}
		*msgs = append(*msgs, m)
		return handlerResult{kind: handlerKindData, lsn: uint64(lc.LSN)}

	case *format.Update:
		tableName := msg.TableName
		if tableName == "" {
			tableName = s.lookupRelationName(msg.OID)
		}

		cleanName := strings.TrimPrefix(tableName, "public.")
		if tableName == "" || !knownTables[cleanName] {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		sani := sanitizePayload(msg.NewDecoded)
		m := protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: protocol.OpUpdate, Data: sani, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}
		*msgs = append(*msgs, m)
		return handlerResult{kind: handlerKindData, lsn: uint64(lc.LSN)}

	case *format.Snapshot:
		if msg.EventType != format.SnapshotEventTypeData {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		tableName := msg.Table
		cleanName := strings.TrimPrefix(tableName, "public.")
		if tableName == "" || !knownTables[cleanName] {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		sani := sanitizePayload(msg.Data)
		// Snapshot rows bypass the watermark entirely: LSN is zeroed on
		// the emitted message and this kind is never Observed. Their
		// durability story is JetStream + the vendored chunk-job state,
		// not the replication watermark.
		m := protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: protocol.OpSnapshot, Data: sani, Timestamp: msg.ServerTime, LSN: 0, UUID: uuid.New().String()}
		*msgs = append(*msgs, m)
		return handlerResult{kind: handlerKindSnapshot}

	case *format.Delete:
		tableName := msg.TableName
		if tableName == "" {
			tableName = s.lookupRelationName(msg.OID)
		}

		cleanName := strings.TrimPrefix(tableName, "public.")
		if tableName == "" || !knownTables[cleanName] {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		sani := sanitizePayload(msg.OldDecoded)
		m := protocol.Message{SourceID: s.config.ID, Table: cleanName, Op: protocol.OpDelete, Data: sani, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}
		*msgs = append(*msgs, m)
		return handlerResult{kind: handlerKindData, lsn: uint64(lc.LSN)}
	}

	// Unmatched message type (e.g. Begin/Commit/Truncate): treat exactly
	// like a filtered event so it self-acks through AckManager rather
	// than stalling the watermark.
	return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
}

func (s *PostgresSource) createHandler(mu *sync.Mutex, msgs *[]protocol.Message, knownTables map[string]bool, triggerFlush func()) func(lc *replication.ListenerContext) {
	return func(lc *replication.ListenerContext) {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Str("source", s.name).Interface("recover", r).Msg("PostgresSource RECOVERED from handler panic")
			}
		}()

		// buildMessage takes and releases mu internally (deferred Unlock,
		// so a panic inside cannot strand the lock). Everything below runs
		// unlocked: triggerFlush and the AckManager calls must never run
		// while mu is held, since triggerFlush itself takes mu again and
		// nothing here may block the batch-wait ticker.
		res := s.buildMessage(lc, mu, msgs, knownTables)

		switch res.kind {
		case handlerKindData:
			triggerFlush()
			// Register the LSN with the AckManager before waiting for the
			// downstream pipeline to confirm publication. This guarantees
			// the watermark can never advance past an LSN the replication
			// stream has produced but no sink has yet acknowledged,
			// preserving the at-least-once contract. There is no lc.Ack()
			// call anywhere in this handler any more: under ManualCommit
			// the only thing that may advance the slot is
			// runAckCoordinator's UpdateXLogPos call, fed exclusively by
			// s.ackChan (see Start/runAckCoordinator).
			s.ackMgr.Observe(res.lsn)

		case handlerKindSnapshot:
			// Snapshot rows are excluded from the LSN/watermark machinery
			// entirely (checkpointed by chunk, not by LSN) — no Observe.
			triggerFlush()

		case handlerKindFiltered:
			if res.lsn > 0 {
				s.ackMgr.ObserveConfirmed(res.lsn)
			}
		}
	}
}

func (s *PostgresSource) Start(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint, ackers []string) (<-chan []protocol.Message, chan<- source.SourceAck, error) {
	sourceCtx, sourceCancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.config = srcConfig
	s.cancel = sourceCancel
	s.ctx = sourceCtx
	s.msgChan = make(chan []protocol.Message, 1)
	s.ackChan = make(chan source.SourceAck, 1024)
	// Reset the AckManager so each Start cycle begins with a fresh
	// watermark, gated on confirmation from every sink ID in ackers. The
	// watermark is hydrated from the persisted checkpoint before the
	// coordinator observes any new LSNs so resumes continue from the
	// last durable position.
	s.ackMgr = NewAckManager(ackers)
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
			// Critical 11 (source half): Enabled is now unconditional —
			// no longer keyed on checkpoint.IngressLSN == 0. The vendored
			// LoadJob (connector.go) is what decides skip/resume/fresh
			// against the cdc_snapshot_job/cdc_snapshot_chunks state, so
			// gating it here just prevented resume from ever being tried
			// after the first snapshot chunk had been published.
			Enabled:           true,
			Mode:              config.SnapshotModeInitial,
			ChunkSize:         8000,
			ClaimTimeout:      30 * time.Second,
			HeartbeatInterval: 5 * time.Second,
		},
		Metric: config.MetricConfig{Port: s.resolveMetricPort()},
		// ManualCommit moves position ownership entirely to
		// runAckCoordinator's UpdateXLogPos call: neither lc.Ack() (a
		// no-op call site that no longer exists) nor keepalives may
		// advance the slot any more. This MUST land together with the
		// coordinator rewrite below — enabling ManualCommit without a
		// coordinator that actually confirms real SourceAck.SinkID values
		// freezes the slot on the very first event.
		ManualCommit: true,
		// KeepaliveFunc reinstates keepalive-driven advancement for idle
		// streams (WAL-bloat protection) via the ONLY sanctioned
		// fast-forward, AckManager.IdleAdvance, which refuses whenever
		// anything is still pending confirmation.
		KeepaliveFunc: func(lsn pq.LSN) { s.ackMgr.IdleAdvance(uint64(lsn)) },
	}

	// WI-7: cfg.StartLSN is deliberately left at its zero value. The
	// PostgreSQL replication slot's own confirmed_flush_lsn is now the
	// sole resume authority (vendored stream.go: lastXLogPos==0 means
	// "start from confirmed_flush_lsn"). By construction the slot only
	// advances after every configured sink has durably written the LSN
	// (WI-4/WI-5), so it is always <= every sink's durable position and
	// safe to trust directly. Hydrate(checkpoint.IngressLSN) above still
	// applies the KV watermark as a floor so the coordinator's first
	// UpdateXLogPos call can never regress below what KV already knows.

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
	// AckManager: it receives SourceAcks from the engine over s.ackChan —
	// each naming the sink that durably wrote a set of LSNs — and confirms
	// them, allowing the watermark to advance only once every required sink
	// has reported.
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

// updateXLogPosTimeout bounds the network round trip of the coordinator's
// UpdateXLogPos call. See the B2 note in plan 01a: the vendored
// SendStandbyStatusUpdate discards its context argument, so this timeout
// only bounds how long the coordinator itself waits before moving on to
// the next tick — it does not abort the underlying socket write, which may
// still complete afterward.
const updateXLogPosTimeout = 5 * time.Second

// runAckCoordinator is the long-lived goroutine that owns the AckManager
// watermark, and the ONLY call site that may advance the PostgreSQL
// replication slot. It does two things, and only two things:
//
//  1. Consume source.SourceAck values published by the engine (one sink's
//     durable write of a batch of LSNs) from s.ackChan and Confirm every
//     LSN in the batch against the AckManager. Confirming may advance the
//     contiguous watermark; the watermark is the single source of truth
//     for how far it is safe to advance the replication slot.
//
//  2. Every 500ms, if the watermark has advanced since the last flush,
//     push it to PostgreSQL via connector.UpdateXLogPos. A failed or
//     timed-out call is retried on the next tick; the watermark itself is
//     never rolled back, so nothing is lost by retrying.
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

	// lastPersistedAt rate-limits persistWatermark (observability only,
	// never correctness) to once per second.
	var lastPersistedAt time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case ack, ok := <-s.ackChan:
			if !ok {
				return
			}
			for _, lsn := range ack.LSNs {
				s.ackMgr.Confirm(lsn, ack.SinkID)
			}
		case <-ticker.C:
			// Export the pending-LSN gauge on every tick regardless of
			// whether the watermark advanced: a permanently non-zero
			// value (a "ghost" Confirm for an LSN never Observed, or a
			// downed sink) is exactly the silently-frozen-slot condition
			// this metric exists to surface.
			pendingLSNsGauge.WithLabelValues(s.name).Set(float64(s.ackMgr.PendingCount()))

			wm := s.ackMgr.Watermark()

			if now := time.Now(); wm > 0 && now.Sub(lastPersistedAt) >= time.Second {
				s.persistWatermark(wm)
				lastPersistedAt = now
			}

			if wm == 0 || wm == lastFlushedWatermark {
				continue
			}

			// Snapshot the connector pointer under RLock, then release
			// the lock BEFORE the network call. Holding s.mu/RLock across
			// UpdateXLogPos would let a hung standby-status write stall
			// this goroutine indefinitely, which (once the producer's
			// ackChan send is blocking, per WI-5) applies backpressure
			// all the way back through AcksTopic into the consumer.
			s.mu.RLock()
			conn := s.connector
			s.mu.RUnlock()
			if conn == nil {
				continue
			}

			cctx, cancel := context.WithTimeout(ctx, updateXLogPosTimeout)
			err := conn.UpdateXLogPos(cctx, pq.LSN(wm))
			cancel()

			if err != nil {
				switch {
				case errors.Is(err, replication.ErrStreamClosed), errors.Is(err, replication.ErrStandbyWriteInFlight):
					// EXPECTED, not failures: the monotonic lastXLogPos
					// store in the vendored stream happens before either
					// of these conditions is even checked, so the
					// in-memory position did not fail to advance — only
					// the wire send was skipped (no live connection yet)
					// or superseded by another in-flight write. Do not
					// log as an error and do not count toward a failure
					// metric; just retry on the next tick.
				case errors.Is(err, context.DeadlineExceeded):
					// Abandoned, NOT proven failed: the underlying socket
					// write may still complete after we gave up waiting
					// (see updateXLogPosTimeout doc above). This is only
					// grounds to skip recording lastFlushedWatermark, never
					// grounds to treat the slot as stuck.
					log.Debug().Uint64("watermark", wm).Msg("UpdateXLogPos timed out waiting for the standby status write; it may still complete, retrying next tick")
				default:
					// A genuine failure worth logging.
					log.Warn().Err(err).Uint64("watermark", wm).Msg("Failed to advance replication slot position")
				}
				continue
			}

			lastFlushedWatermark = wm
		}
	}
}

// persistWatermark is a best-effort, non-blocking KV write of the current
// AckManager watermark for dashboards/operators (WI-7 §3). It is NEVER on
// the correctness path: the replication slot's own confirmed_flush_lsn
// (advanced exclusively via UpdateXLogPos above) is what actually gates
// resume safety. Any failure here is logged and otherwise ignored.
func (s *PostgresSource) persistWatermark(wm uint64) {
	s.mu.RLock()
	kv := s.kv
	pipelineID := s.pipelineID
	sourceID := s.config.ID
	s.mu.RUnlock()

	if kv == nil || pipelineID == "" || sourceID == "" {
		return
	}

	cp := protocol.Checkpoint{IngressLSN: wm, Status: "ACTIVE", UpdatedAt: time.Now().UTC()}
	data, err := cp.MarshalMsg(nil)
	if err != nil {
		log.Debug().Err(err).Msg("persistWatermark: failed to marshal checkpoint")
		return
	}

	key := protocol.SourceWatermarkKey(pipelineID, sourceID)
	if _, err := kv.Put(key, data); err != nil {
		log.Debug().Err(err).Str("key", key).Msg("persistWatermark: failed to write watermark to KV")
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

	// WI-7 / B3: cfg.StartLSN no longer seeds the vendored stream's
	// lastXLogPos, so both keepalive reply paths (LoadXLogPos() > 0
	// guarded) stay silent until the coordinator's first flush. On a
	// resume (hydrated watermark > 0) that would reopen a window WI-4
	// closed: a fresh session whose downstream sink is down from the very
	// first event would send NO standby status update at all while data
	// LSNs are pending (IdleAdvance correctly refuses), risking
	// wal_sender_timeout on the primary.
	//
	// Mitigation: seed lastXLogPos once, here, from the AckManager's
	// hydrated watermark -- NOT from checkpoint.IngressLSN directly, so
	// this stays in sync with the same floor Hydrate() applied in Start.
	// This is deliberately scoped to the resume case only (watermark > 0).
	// A genuinely fresh slot (watermark == 0, no prior checkpoint) is not
	// seeded: seeding 0 would be a no-op anyway (lastXLogPos already
	// starts at 0, meaning "use confirmed_flush_lsn"), and a fresh slot
	// with a sink down from event one is exactly the condition the WI-5a
	// slot-lag alert (cdc_source_pending_lsns / replication-slot lag
	// metrics) exists to catch -- an operator is expected to act on that
	// alert before wal_sender_timeout fires, rather than this code path
	// silently working around it.
	if wm := s.ackMgr.Watermark(); wm > 0 {
		// ErrStreamClosed is EXPECTED and deliberately not logged: this runs
		// before conn.Start below, so the stream has no socket yet. The
		// in-memory position is still stored, so the seed succeeded; only
		// the pre-connect send was skipped.
		if err := conn.UpdateXLogPos(sourceCtx, pq.LSN(wm)); err != nil &&
			!errors.Is(err, replication.ErrStreamClosed) {
			log.Warn().Err(err).Uint64("lsn", wm).Msg("Failed to seed xlog position from hydrated watermark")
		}
	} else {
		// B3, fully closed rather than left to the WI-5a alert: on a
		// genuinely fresh slot (hydrated watermark == 0) the vendored
		// lastXLogPos stays 0 until the coordinator's first flush, which
		// means both LoadXLogPos()>0-guarded keepalive reply paths stay
		// silent. If the downstream sink is down from event one, that is
		// not merely a slow reconnect: NO standby status update is ever
		// sent while data LSNs are pending (IdleAdvance correctly
		// refuses), wal_sender_timeout (default 60s) kills the walsender,
		// and the vendored connector's Start loop parks on cancelCh with
		// no reconnect -- ingestion stalls permanently until a human
		// restarts the pipeline, even after the sink recovers. No data is
		// lost (the slot never advanced), but it does not self-heal.
		//
		// Close it by querying the slot's OWN confirmed_flush_lsn and
		// seeding lastXLogPos with it. Semantically this is a no-op
		// position -- it is exactly where replication is about to start
		// from anyway -- but it makes lastXLogPos non-zero, which is all
		// either keepalive reply path checks.
		//
		// TWO attempts, because the slot may not exist yet at this point:
		// on a genuinely first-ever deployment the slot is created INSIDE
		// conn.Start (slot.Config{CreateIfNotExists: true}, materialised
		// by the vendored connector's prepareSnapshotAndSlot/slot.Create),
		// so a query issued here, before Start, would just get
		// sql.ErrNoRows and leave lastXLogPos unseeded -- exactly the
		// hole B3 describes, unmitigated.
		//
		//  1. Fast path, right here, pre-Start: correctly handles the
		//     slot-already-exists case (a prior process created it, this
		//     is a restart of a fresh-watermark session against it).
		//  2. Fallback, spawned below as a goroutine gated on
		//     conn.WaitUntilReady (which the vendored connector only
		//     unblocks after slot.Connect/stream.Connect/stream.Open have
		//     all succeeded, i.e. strictly after the slot exists): re-
		//     checks the watermark is still 0 (nothing else seeded it in
		//     the meantime) and retries the same query/seed.
		//
		// Deliberately NOT falling back to pg_current_wal_lsn(): that
		// reports a position ahead of delivery and would genuinely
		// advance the slot past undelivered data -- the exact bug this
		// whole plan exists to eliminate. Every path here only ever seeds
		// from confirmed_flush_lsn.
		if s.slotConfirmedFlushLSN != nil {
			if lsn, ok := s.slotConfirmedFlushLSN(sourceCtx, s.db, srcConfig.SlotName); ok {
				// Same ErrStreamClosed rationale as the resume-path seed above.
				if err := conn.UpdateXLogPos(sourceCtx, lsn); err != nil &&
					!errors.Is(err, replication.ErrStreamClosed) {
					log.Warn().Err(err).Str("lsn", lsn.String()).Msg("B3: failed to seed lastXLogPos from fresh-slot confirmed_flush_lsn (fast path)")
				}
			} else {
				log.Debug().Str("slot", srcConfig.SlotName).Msg("B3: confirmed_flush_lsn not available before conn.Start (slot likely does not exist yet); deferring to the post-WaitUntilReady seed")
			}

			s.runWg.Add(1)
			go func() {
				defer s.runWg.Done()
				// WaitUntilReady blocks until the connector's internal
				// slot/publication/stream setup has completed, or returns
				// early with an error if sourceCtx is cancelled first
				// (shutdown) or setup itself fails. Either way this
				// goroutine exits promptly and is drained by s.runWg --
				// it never blocks Stop()/Restart() from proceeding.
				if err := conn.WaitUntilReady(sourceCtx); err != nil {
					log.Debug().Err(err).Msg("B3: WaitUntilReady did not succeed; skipping post-ready fresh-slot seed")
					return
				}
				// Re-check: the resume-path branch above cannot run
				// concurrently with this goroutine (they are mutually
				// exclusive on wm==0 at spawn time), but another source
				// of truth could have advanced the watermark by now (a
				// real event was observed and confirmed before setup
				// finished). Only seed if it is STILL the fresh-slot case.
				if s.ackMgr.Watermark() != 0 {
					return
				}
				lsn, ok := s.slotConfirmedFlushLSN(sourceCtx, s.db, srcConfig.SlotName)
				if !ok {
					log.Debug().Str("slot", srcConfig.SlotName).Msg("B3: confirmed_flush_lsn still not available after WaitUntilReady; leaving lastXLogPos unseeded, relying on WI-5a slot-lag alerting")
					return
				}
				if err := conn.UpdateXLogPos(sourceCtx, lsn); err != nil &&
					!errors.Is(err, replication.ErrStreamClosed) {
					log.Warn().Err(err).Str("lsn", lsn.String()).Msg("B3: failed to seed lastXLogPos from fresh-slot confirmed_flush_lsn (post-ready path)")
				}
			}()
		}
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
		// T0-2: propagate the error instead of discarding it. This method's own signature
		// already promised an error return but always returned nil, so a failed slot
		// advance was invisible to callers.
		if err := connector.UpdateXLogPos(ctx, pq.LSN(lsn)); err != nil {
			return fmt.Errorf("advance replication slot to %d: %w", lsn, err)
		}
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

// Restart replaces the broken in-place RestartWithNewTables. It currently
// keeps the pre-existing tear-down/rebuild body (renamed and reshaped to
// return the source.Source-interface channel pair) but does NOT yet
// implement the rebind protocol specified in plan §3 WI-6: the ack
// coordinator is not respawned, the returned ackChan is the OLD (already
// orphaned, per the plan's own diagnosis) channel rather than a fresh one,
// and callers get back channels that are not wired into a live producer
// rebind loop.
//
// TODO(WI-6): implement the shared startSession(...) extraction, respawn
// runAckCoordinator against the new session, hydrate the new coordinator
// from the live (not reconstructed) AckManager, and give the caller a
// genuinely fresh ackChan. Tracked in plan §3 WI-6 / §7 Q5.
func (s *PostgresSource) Restart(ctx context.Context, newTables []string) (<-chan []protocol.Message, chan<- source.SourceAck, error) {
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
		return nil, nil, err
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

	return s.msgChan, s.ackChan, nil
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
