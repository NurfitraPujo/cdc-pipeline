package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
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
	"github.com/jackc/pgx/v5/pgtype"
	_ "github.com/jackc/pgx/v5/stdlib"
	// libpq is aliased (not "pq") because that name is already taken by the
	// vendored github.com/Trendyol/go-pq-cdc/pq package above; only
	// QuoteIdentifier is used, for AlterPublication's DDL string.
	libpq "github.com/lib/pq"
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

// pendingLSNsGauge exports AckManager.PendingCount() per source. A
// permanent non-zero value indicates the watermark is stalled: either a
// downstream sink is not confirming, or a "ghost" entry exists (a Confirm
// for an LSN that was never Observed) which pins the watermark and blocks
// IdleAdvance forever. Full slot-lag alerting on top of this is WI-5a.
//
// WI-5a review fix (Defect 1): the label set is deliberately
// {"pipeline","source","slot"} -- IDENTICAL to slotLagBytesGauge and
// ackWatermarkGauge below, not just {"source"}. PromQL's binary `and`
// matches on the full label set by default; a mismatched set (the original
// bug here) means `cdc_source_pending_lsns > 0 and delta(cdc_source_ack_watermark[10m]) == 0`
// can NEVER match any series, so the CDCSourcePendingLSNsStuck alert would
// silently never fire -- exactly the failure modes ((b) wedged connector,
// (c) pinned LSN) that do not self-heal and most need alerting. Any future
// gauge meant to be joined against these two in an alert expression MUST
// use this same three-label set.
var pendingLSNsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cdc_source_pending_lsns",
	Help: "Number of observed-or-confirmed LSNs not yet folded into the AckManager watermark",
}, []string{"pipeline", "source", "slot"})

// slotLagBytesGauge exports the byte gap between the source primary's
// current WAL position and the replication slot's confirmed_flush_lsn
// (WI-5a). Under the ManualCommit contract the slot only advances after
// every configured sink has durably written an LSN, so a dead/slow sink
// freezes confirmed_flush_lsn and this gauge grows without bound while
// PostgreSQL retains WAL on the source primary. This is the correct
// at-least-once trade (loss -> visible backpressure), but it is only safe
// to operate with an alert on this metric -- see the WI-5a runbook.
var slotLagBytesGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cdc_source_slot_lag_bytes",
	Help: "Bytes between pg_current_wal_lsn() and the replication slot's confirmed_flush_lsn",
}, []string{"pipeline", "source", "slot"})

// ackWatermarkGauge exports AckManager.Watermark() (WI-5a, required by the
// §6 bake period): with strict_ack off this is expected to lag
// confirmed_flush_lsn by a small, stable gap; a growing gap under
// production load means the watermark plumbing itself is not keeping up
// and the flag must not be flipped for that pipeline yet.
var ackWatermarkGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cdc_source_ack_watermark",
	Help: "Current AckManager watermark: the highest LSN confirmed durably written by every required sink",
}, []string{"pipeline", "source", "slot"})

// slotLagProbeLastSuccessGauge exports the unix timestamp of the last
// successful querySlotLagBytes call per source (WI-5a review Defect 3).
// slotLagBytesGauge is a Prometheus gauge: on a probe failure (query error,
// DB down) runSlotLagProbe deliberately leaves it at its last value rather
// than clearing it, so a degraded database connection -- the failure most
// likely to co-occur with a real source-primary problem -- would otherwise
// scrape as a stale, healthy-looking number forever, silencing both
// CDCSourceSlotLagWarning and CDCSourceSlotLagCritical during exactly the
// incident they exist for. This gauge makes that staleness independently
// observable and alertable (CDCSourceSlotLagProbeStale).
var slotLagProbeLastSuccessGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cdc_source_slot_lag_probe_last_success_timestamp_seconds",
	Help: "Unix timestamp of the last successful slot-lag probe query for this source",
}, []string{"pipeline", "source", "slot"})

// idleAdvanceRefusedCounter is the OPS-2 canary for a T0-3 regression.
//
// T0-3 (f192fe3) fixed a data-loss bug where IdleAdvance fast-forwarded the
// watermark past a replay backlog that had not yet reached Observe(), because
// the keepalive was delivered inline on the sink goroutine and bypassed the
// messageCH queue. The defining nastiness of that bug was that NOTHING moved:
// cdc_source_pending_lsns read a healthy 0 for the entire loss window, so no
// alert could fire and it took a full e2e investigation to find.
//
// AckManager's highestSeen/idleTrusted guard is the application-side backstop,
// but it is deliberately latching -- it refuses once, logs, then trusts. That
// makes it useless as a monitoring signal on its own. This counter turns each
// refusal into something alertable (CDCSourceIdleAdvanceRefused), so a
// recurrence is loud rather than silent. Labels match the four gauges above so
// it joins them in PromQL.
var idleAdvanceRefusedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "cdc_source_idle_advance_refused_total",
	Help: "Times IdleAdvance refused to fast-forward past the highest observed LSN (T0-3 regression canary)",
}, []string{"pipeline", "source", "slot"})

// strictAckEnvVar is the env var that gates the strict_ack feature flag
// (plan 01a §6). It is source-scoped in name only -- every PostgresSource
// in this process reads the same process-wide env var, matching how every
// other env-driven switch in this repo works (see logger.Init's "ENV" read
// and cmd/pipeline/main.go's POSTGRES_*/DATABEND_* overrides). Read directly
// in PostgresSource.Start rather than plumbed through protocol.SourceConfig
// because SourceConfig is msgp-serialized and persisted in KV per source --
// turning a temporary, release-scoped rollout switch into persisted deploy
// state would survive past the flag's own removal (§6 step 3) and require a
// migration to clean up. A plain env var needs neither.
const strictAckEnvVar = "CDC_STRICT_ACK"

// resolveStrictAck resolves the strict_ack flag (§6): an explicit
// CDC_STRICT_ACK=<bool> always wins, in either direction. With no (or an
// unparseable) value the default is by ENV:
//
//	ENV=production          => OFF (legacy per-event lc.Ack)
//	ENV=staging             => ON  (the new contract)
//	anything else / unset   => ON  (dev, test)
//
// Staging is deliberately grouped with dev, NOT with production: it is the
// intended bake environment for this flag and ships with the WI-5a alerts
// enabled, so it is where the new contract should be exercised first.
//
// Note this is deliberately NOT the same split logger.Init uses -- that one
// groups staging WITH production (logger.go: `env != "production" && env !=
// "staging"`). Do not "align" the two on the assumption they should match;
// they encode different intents.
//
// ON is the new no-lc.Ack() contract (the whole point of plan 01a); OFF is
// the legacy per-event ack this flag exists to fall back to, which re-opens
// the data-loss window -- an availability escape hatch, not a correctness
// one.
func resolveStrictAck() bool {
	if raw := os.Getenv(strictAckEnvVar); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err == nil {
			return v
		}
		log.Warn().Str(strictAckEnvVar, raw).Msg("PostgresSource: unrecognized CDC_STRICT_ACK value, falling back to the ENV-based default")
	}
	return os.Getenv("ENV") != "production"
}

// vendorDefaultSnapshotChunkSize mirrors the vendored connector's own
// fallback (internal/vendor/go-pq-cdc/config/config.go: c.Snapshot.ChunkSize
// defaults to 8_000 when unset) so that leaving SourceConfig.SnapshotChunkSize
// at its zero value reproduces exactly today's behaviour.
const vendorDefaultSnapshotChunkSize = 8000

// snapshotChunkSize resolves the vendored snapshot ChunkSize from
// SourceConfig.SnapshotChunkSize (WS-2B item 3): previously this field was
// parsed and validated but never reached the connector, which always used a
// hardcoded 8000 regardless of what was configured. A non-positive value
// (the common case: field left unset in existing configs) falls back to the
// same 8000 the vendored default would have applied, so this is a pure
// wiring fix with no behavioural change for configs that don't set it.
func snapshotChunkSize(configured int) int64 {
	if configured <= 0 {
		return vendorDefaultSnapshotChunkSize
	}
	return int64(configured)
}

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

	// startMu serialises entire Start() invocations end-to-end (HIGH-2 part
	// 2). The re-entry guard at the top of Start cancels and awaits a
	// PREVIOUS session before mutating shared fields, but that guard alone
	// does not stop two CONCURRENT Start() calls from both reading the same
	// prevCancel/prevDB/prevConnector, both proceeding past the guard, and
	// racing each other's writes to s.db/s.connector/s.msgChan/s.ackChan --
	// each such race can leak a *sql.DB/connector or hand a caller a
	// channel that a losing goroutine still holds a reference to. Locking
	// startMu for the whole method body makes Start calls strictly
	// sequential, so by the time a second call's re-entry guard runs, the
	// first call's session is fully constructed (or fully failed) with no
	// window for concurrent mutation.
	startMu sync.Mutex

	// mu protects the config and connector during restarts
	mu       sync.RWMutex
	config   protocol.SourceConfig
	oidMu    sync.RWMutex
	oidCache map[uint32]string // pg_type OID -> typname (primeOIDCache); unrelated to relationCache below
	// relationCache maps a WAL relation OID -> its (schema, table) TableRef,
	// populated from format.Relation.Namespace/Name (cacheRelation). Kept
	// separate from oidCache (a different OID namespace, pg_type vs
	// pg_class) rather than folding schema into that existing map, which
	// would conflate two unrelated caches. Guarded by the same oidMu.
	relationCache map[uint32]protocol.TableRef

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

	// slotLagBytes resolves the current byte gap between
	// pg_current_wal_lsn() and the slot's confirmed_flush_lsn (WI-5a).
	// Defaults to querySlotLagBytes (a real, short-lived query against
	// s.db); tests override it to exercise the probe loop without a live
	// PostgreSQL connection. ok is false whenever the value could not be
	// determined (slot does not exist yet, query error, parse error) --
	// callers treat that as "skip this tick", never as "lag is zero".
	slotLagBytes func(ctx context.Context, db *sql.DB, slotName string) (bytes int64, ok bool)

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
		relationCache:         make(map[uint32]protocol.TableRef),
		ackMgr:                NewAckManager(nil),
		connectorFactory:      defaultConnectorFactory,
		slotConfirmedFlushLSN: queryConfirmedFlushLSN,
		slotLagBytes:          querySlotLagBytes,
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

// querySlotLagBytes is the real (non-test) implementation of
// PostgresSource.slotLagBytes (WI-5a): it queries pg_replication_slots for
// the given slot's byte gap between the primary's current WAL position and
// its confirmed_flush_lsn. Any error (nil db, no such slot, query error,
// scan error) reports ok=false; callers treat that as "skip this tick",
// never as "lag is zero" -- this must never block or crash the probe loop.
// A missing slot is the expected, common case on a fresh deployment before
// conn.Start has created it.
func querySlotLagBytes(ctx context.Context, db *sql.DB, slotName string) (int64, bool) {
	if db == nil || slotName == "" {
		return 0, false
	}

	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var lagBytes int64
	err := db.QueryRowContext(qctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
		 FROM pg_replication_slots WHERE slot_name = $1`,
		slotName,
	).Scan(&lagBytes)
	if err != nil {
		log.Debug().Err(err).Str("slot", slotName).Msg("querySlotLagBytes: failed to read slot lag (slot may not exist yet)")
		return 0, false
	}

	return lagBytes, true
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

// tableRefFromConfigEntry parses a srcConfig.Tables entry (bare "orders" or
// schema-qualified "sales.orders") into a TableRef, falling back to a bare
// public ref rather than silently dropping identity -- protocol.SourceConfig
// .Validate() should already reject anything that reaches here with "=" or
// more than one ".". This is the ONLY place a config.Tables string is turned
// into a TableRef in this package; every consumer below (knownTables,
// pubTables, the schema-priming loop) threads the resulting ref rather than
// re-deriving it from the raw string (MULTI_SCHEMA_PLAN.md §11.2 requirement
// 3 -- mirrors internal/engine/producer.go's tableRefFromConfigEntry).
func tableRefFromConfigEntry(s string) protocol.TableRef {
	ref, err := protocol.ParseTableRef(s)
	if err != nil {
		return protocol.TableRef{Schema: "public", Table: s}
	}
	return ref
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

// cacheRelation records an OID -> TableRef mapping (schema AND bare table
// name -- a Relation message is the only place the namespace is carried
// explicitly for a given OID, so both must be cached together; caching only
// the bare name here would silently lose the schema for any DML message
// that arrives with an empty TableName/TableNamespace and has to fall back
// to this cache, see buildMessage). It exists so the write happens under a
// DEFERRED unlock: a nil-map assignment (or any other panic) must not
// strand s.oidMu, which would deadlock every subsequent handler invocation
// and silently wedge the source. Same reasoning as buildMessage's own
// deferred unlock.
func (s *PostgresSource) cacheRelation(oid uint32, schema, name string) {
	s.oidMu.Lock()
	defer s.oidMu.Unlock()
	// Deliberately NOT nil-guarded: the map is initialised in
	// NewPostgresSource, and TestHandler_PanicSafety_MuNotStranded forces a
	// nil map here precisely to prove that a panic under this lock is
	// survivable. Adding a guard would make that test vacuous.
	s.relationCache[oid] = protocol.TableRef{Schema: protocol.NormalizeSchema(schema), Table: name}
}

// lookupRelationName resolves an OID to a cached TableRef under a
// deferred read-unlock, for the same panic-safety reason as cacheRelation.
// The zero value (TableRef{}) means "not cached".
func (s *PostgresSource) lookupRelationName(oid uint32) protocol.TableRef {
	s.oidMu.RLock()
	defer s.oidMu.RUnlock()
	return s.relationCache[oid]
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
func (s *PostgresSource) buildMessage(lc *replication.ListenerContext, mu *sync.Mutex, msgs *[]protocol.Message, knownTables map[protocol.TableRef]bool) handlerResult {
	mu.Lock()
	defer mu.Unlock()

	switch msg := lc.Message.(type) {
	case *format.Relation:
		s.cacheRelation(msg.OID, msg.Namespace, msg.Name)
		log.Info().Str("schema", msg.Namespace).Str("table", msg.Name).Uint32("oid", msg.OID).Msg("PostgresSource: Received relation")
		return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}

	case *format.Insert:
		ref, ok := s.resolveDMLTableRef(msg.OID, msg.TableNamespace, msg.TableName, knownTables)
		if !ok {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		sani, kinds := sanitizePayload(msg.Decoded)
		m := protocol.Message{SourceID: s.config.ID, Table: ref.Table, TableSchema: ref.Schema, Op: protocol.OpInsert, Data: sani, ColumnKinds: kinds, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}
		*msgs = append(*msgs, m)
		return handlerResult{kind: handlerKindData, lsn: uint64(lc.LSN)}

	case *format.Update:
		ref, ok := s.resolveDMLTableRef(msg.OID, msg.TableNamespace, msg.TableName, knownTables)
		if !ok {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		sani, kinds := sanitizePayload(msg.NewDecoded)
		// WS-7: any column Postgres omitted from this UPDATE's new tuple
		// because it is an unchanged TOASTed value (never sent, never
		// decoded, genuinely absent from sani/Data) must be flagged via
		// ColumnKinds so a downstream consumer doing a wholesale row
		// replace does not treat the absence as NULL. sanitizePayload never
		// produces these keys (they were never in msg.NewDecoded to begin
		// with), so merge them in here.
		if len(msg.NewToastedColumns) > 0 {
			if kinds == nil {
				kinds = make(map[string]string, len(msg.NewToastedColumns))
			}
			for _, col := range msg.NewToastedColumns {
				kinds[col] = protocol.ColumnKindToastedUnchanged
			}
		}
		m := protocol.Message{SourceID: s.config.ID, Table: ref.Table, TableSchema: ref.Schema, Op: protocol.OpUpdate, Data: sani, ColumnKinds: kinds, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}
		*msgs = append(*msgs, m)
		return handlerResult{kind: handlerKindData, lsn: uint64(lc.LSN)}

	case *format.Snapshot:
		if msg.EventType != format.SnapshotEventTypeData {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		// Snapshot carries schema and table as separate fields already
		// (unlike the pre-multi-schema TrimPrefix("public.") assumption);
		// no OID/relation-cache fallback exists for snapshot rows.
		if msg.Table == "" {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		ref := protocol.TableRef{Schema: protocol.NormalizeSchema(msg.Schema), Table: msg.Table}
		if !knownTables[ref] {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		sani, kinds := sanitizePayload(msg.Data)
		// Snapshot rows bypass the watermark entirely: LSN is zeroed on
		// the emitted message and this kind is never Observed. Their
		// durability story is JetStream + the vendored chunk-job state,
		// not the replication watermark.
		m := protocol.Message{SourceID: s.config.ID, Table: ref.Table, TableSchema: ref.Schema, Op: protocol.OpSnapshot, Data: sani, ColumnKinds: kinds, Timestamp: msg.ServerTime, LSN: 0, UUID: uuid.New().String()}
		*msgs = append(*msgs, m)
		return handlerResult{kind: handlerKindSnapshot}

	case *format.Delete:
		ref, ok := s.resolveDMLTableRef(msg.OID, msg.TableNamespace, msg.TableName, knownTables)
		if !ok {
			return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
		}
		sani, kinds := sanitizePayload(msg.OldDecoded)
		m := protocol.Message{SourceID: s.config.ID, Table: ref.Table, TableSchema: ref.Schema, Op: protocol.OpDelete, Data: sani, ColumnKinds: kinds, Timestamp: msg.MessageTime, LSN: uint64(lc.LSN), UUID: uuid.New().String()}
		*msgs = append(*msgs, m)
		return handlerResult{kind: handlerKindData, lsn: uint64(lc.LSN)}
	}

	// Unmatched message type (e.g. Begin/Commit/Truncate): treat exactly
	// like a filtered event so it self-acks through AckManager rather
	// than stalling the watermark.
	return handlerResult{kind: handlerKindFiltered, lsn: uint64(lc.LSN)}
}

// resolveDMLTableRef derives the TableRef for an Insert/Update/Delete event
// and reports whether it is present in knownTables. This is the
// MULTI_SCHEMA_PLAN.md §3 Stage 2 "highest-risk edit": a partial change here
// makes every non-matching event handlerKindFiltered -- self-acked,
// watermark advanced, row dropped forever, no error, invisible to any
// public-only test -- so both the schema and table components MUST come
// from the same source (never table-only) before being checked against
// knownTables (itself keyed by TableRef, not a bare string).
//
// tableNamespace/tableName come directly off the wire message; the vendored
// library populates both from the cached Relation on every DML message
// (format.Insert/Update/Delete.decode), but an empty tableName falls back
// to the OID relation cache (cacheRelation) rather than being treated as
// having no schema -- discarding the cached namespace here would silently
// misfile a non-public table into the "public" bucket instead of correctly
// filtering or matching it.
func (s *PostgresSource) resolveDMLTableRef(oid uint32, tableNamespace, tableName string, knownTables map[protocol.TableRef]bool) (protocol.TableRef, bool) {
	if tableName == "" {
		ref := s.lookupRelationName(oid)
		if ref.Table == "" {
			return protocol.TableRef{}, false
		}
		return ref, knownTables[ref]
	}
	ref := protocol.TableRef{Schema: protocol.NormalizeSchema(tableNamespace), Table: tableName}
	return ref, knownTables[ref]
}

// createHandler builds the replication callback. strictAck is the
// resolved §6 feature flag, snapshotted once per Start call (a mid-session
// flip is not supported -- see the flag doc on resolveStrictAck):
//
//   - ON (current contract): no lc.Ack() call anywhere below. The only
//     thing that may advance the slot is runAckCoordinator's UpdateXLogPos,
//     fed exclusively by s.ackChan.
//   - OFF (legacy escape hatch, §6): every one of WI-4's 8 deleted lc.Ack()
//     call sites is restored, one per handlerKind branch below, exactly
//     mirroring the pre-WI-4 handler (see commit 0dbb895). Because
//     cfg.ManualCommit is false in this mode, the vendored stream.Ack
//     itself advances lastXLogPos and sends the standby status update --
//     runAckCoordinator's own UpdateXLogPos calls keep running (metrics
//     stay live for the §6 bake period) but are harmless no-ops here
//     because they can only ever report vendored stream.go's monotonic
//     max(lastXLogPos, lsn), never regress it.
func (s *PostgresSource) createHandler(mu *sync.Mutex, msgs *[]protocol.Message, knownTables map[protocol.TableRef]bool, triggerFlush func(), strictAck bool) func(lc *replication.ListenerContext) {
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
			// preserving the at-least-once contract. Kept live in BOTH
			// modes: the §6 bake period requires cdc_source_ack_watermark
			// to be observable before the flag is ever flipped.
			s.ackMgr.Observe(res.lsn)
			if !strictAck {
				// §6 OFF: legacy per-event ack. cfg.ManualCommit is false
				// in this mode, so the vendored stream.Ack advances the
				// slot straight to this event's LSN itself.
				lc.Ack() //nolint:errcheck // matches pre-WI-4 behaviour: return value was never checked
			}

		case handlerKindSnapshot:
			// Snapshot rows are excluded from the LSN/watermark machinery
			// entirely (checkpointed by chunk, not by LSN) — no Observe,
			// in both modes.
			triggerFlush()
			if !strictAck {
				lc.Ack() //nolint:errcheck // matches pre-WI-4 behaviour: return value was never checked
			}

		case handlerKindFiltered:
			if res.lsn > 0 {
				s.ackMgr.ObserveConfirmed(res.lsn)
			}
			if !strictAck {
				lc.Ack() //nolint:errcheck // matches pre-WI-4 behaviour: return value was never checked
			}
		}
	}
}

func (s *PostgresSource) Start(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint, ackers []string) (<-chan []protocol.Message, chan<- source.SourceAck, error) {
	// HIGH-2: serialise re-entry. If a previous session is still live (its
	// goroutines have not yet observed cancellation and exited), signal it
	// and wait for it to fully wind down BEFORE this call mutates any
	// shared field (s.dsn, s.db, s.connector, s.msgChan, s.ackChan, ...).
	// Without this, a caller that invokes Start again while the previous
	// session's coordinator/probe/cleanup goroutines are still running
	// (e.g. Producer.recoverProducer after errPublishRetriesExhausted)
	// races those goroutines' unlocked reads of s.db against this
	// function's writes, leaks the previous *sql.DB/connector, and lets
	// the previous session's closures keep writing to fields this call
	// reassigns out from under them.
	//
	// startMu covers the whole method body, not just the snapshot below:
	// the teardown-then-rebuild sequence is only safe if it is atomic with
	// respect to another Start. Two concurrent callers would otherwise both
	// snapshot prevCancel (possibly nil) and both proceed to build a
	// session, which is exactly the double-live-session state this guard
	// exists to make impossible.
	s.startMu.Lock()
	defer s.startMu.Unlock()

	s.mu.Lock()
	prevCancel := s.cancel
	prevDB := s.db
	prevConnector := s.connector
	s.mu.Unlock()

	if prevCancel != nil {
		prevCancel()
		s.runWg.Wait()
	}
	if prevConnector != nil {
		prevConnector.Close()
	}
	if prevDB != nil {
		prevDB.Close()
	}

	sourceCtx, sourceCancel := context.WithCancel(ctx)
	s.mu.Lock()
	// A fresh session begins: closeOnce must be reset so that a later
	// Stop() call actually closes THIS session's resources rather than
	// silently no-op'ing because an earlier session's Stop() already fired
	// it once for the lifetime of this PostgresSource.
	s.closeOnce = sync.Once{}
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
	// OPS-2: make an IdleAdvance refusal alertable, not just loggable. The
	// AckManager's guard latches after one refusal, so without this counter a
	// T0-3 regression would look exactly as it did before the fix: every
	// metric healthy, one Error line in the log. Labels are captured here so
	// they match the gauges; slot name comes from the config being started.
	pipelineIDForCanary, slotForCanary := s.pipelineID, srcConfig.SlotName
	sourceForCanary := s.name
	s.ackMgr.SetIdleAdvanceRefusedHook(func() {
		idleAdvanceRefusedCounter.WithLabelValues(pipelineIDForCanary, sourceForCanary, slotForCanary).Inc()
	})
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

	db, err := sql.Open("pgx", dsn)
	s.mu.Lock()
	s.dsn = dsn
	s.db = db
	s.mu.Unlock()
	if err != nil {
		sourceCancel()
		return nil, nil, fmt.Errorf("failed to open DB: %w", err)
	}

	if err := s.primeOIDCache(setupCtx, s.db); err != nil {
		log.Warn().Err(err).Msg("Failed to prime OID cache")
	}

	var mu sync.Mutex
	var msgs []protocol.Message
	// R9: flushWg tracks in-flight triggerFlush senders that are between
	// releasing mu and completing their send on s.msgChan. flushClosed,
	// guarded by the same mu, is set by the cleanup goroutine below before
	// it closes msgChan; every triggerFlush call checks it while still
	// holding mu, so once it flips no new sender can be admitted. The
	// cleanup goroutine then waits on flushWg to drain any senders that
	// were already admitted, guaranteeing none of them can still be
	// sitting in the msgChan-send case of their select when close() runs
	// -- without this, a sender parked in that select could pick the send
	// case after close(msgChan), which panics (send on closed channel).
	var flushWg sync.WaitGroup
	var flushClosed bool
	// knownTables is keyed by TableRef -- MULTI_SCHEMA_PLAN.md §3 Stage 2:
	// the dual bare/qualified string entries this replaced could not tell
	// "sales.users" and "public.users" apart, and could not represent
	// "sales.users" at all (see buildMessage/resolveDMLTableRef, the only
	// readers of this map).
	knownTables := make(map[protocol.TableRef]bool)
	for _, t := range srcConfig.Tables {
		knownTables[tableRefFromConfigEntry(t)] = true
	}

	triggerFlush := func() {
		mu.Lock()
		if flushClosed || len(msgs) == 0 {
			mu.Unlock()
			return
		}
		mCopy := make([]protocol.Message, len(msgs))
		copy(mCopy, msgs)
		msgs = msgs[:0]
		flushWg.Add(1)
		mu.Unlock()
		defer flushWg.Done()

		select {
		case s.msgChan <- mCopy:
			log.Debug().Any("data", mCopy).Msg("Source data sent to message channel")
		case <-sourceCtx.Done():
		}
	}

	// Schema MUST be set explicitly on every entry (MULTI_SCHEMA_PLAN.md §3
	// Stage 2 item 3 / §1 defect 3): an empty Schema falls through to the
	// vendored default at internal/vendor/go-pq-cdc/pq/publication/config.go
	// ("public", hardcoded), so a config entry naming "sales.orders" would
	// silently ask PostgreSQL to publish "public.orders" instead.
	pubTables := make(publication.Tables, len(srcConfig.Tables))
	for i, t := range srcConfig.Tables {
		ref := tableRefFromConfigEntry(t)
		pubTables[i] = publication.Table{Name: ref.Table, Schema: ref.Schema, ReplicaIdentity: "DEFAULT"}
	}

	// strictAck resolves the §6 feature flag once per Start call. See
	// resolveStrictAck for the CDC_STRICT_ACK / dev-vs-prod default
	// resolution; it is snapshotted here rather than re-read per event so a
	// single replication session never straddles both handler behaviours.
	strictAck := resolveStrictAck()

	// searchPath pins the vendored connector's regular (non-replication)
	// connection to exactly the configured schema whitelist (vendored-patch
	// MS-1, config.Config.SearchPath -- see that field's doc for why
	// ReplicationDSN is deliberately excluded and for the Stage 4 follow-up
	// this creates). Same "empty means public only" semantics as
	// discoverTables, for the same reason (§8 item 4): every existing
	// deployment has Schemas empty today.
	searchSchemas := srcConfig.Schemas
	if len(searchSchemas) == 0 {
		searchSchemas = []string{"public"}
	}
	searchPath := strings.Join(searchSchemas, ",")

	cfg := config.Config{
		Host: srcConfig.Host, Port: srcConfig.Port, Username: srcConfig.User, Password: srcConfig.PassEncrypted, Database: srcConfig.Database,
		SearchPath: searchPath,
		Slot:       slot.Config{Name: srcConfig.SlotName, CreateIfNotExists: true},
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
			Enabled: true,
			Mode:    config.SnapshotModeInitial,
			// ChunkSize (WS-2B item 3 / docs/todos/custom_object_cdc_followups.md
			// item 5): previously hardcoded 8000 regardless of
			// srcConfig.SnapshotChunkSize, so the config field existed but
			// silently did nothing for the snapshot path (it only affected
			// the producer's dynamic-table path). Falls back to the
			// vendored connector's own default (config.go: 8_000) when
			// unset, preserving today's behaviour for every existing
			// deployment that doesn't set snapshot_chunk_size.
			ChunkSize:         snapshotChunkSize(srcConfig.SnapshotChunkSize),
			ClaimTimeout:      30 * time.Second,
			HeartbeatInterval: 5 * time.Second,
		},
		Metric: config.MetricConfig{Port: s.resolveMetricPort()},
		// ManualCommit is gated by the §6 strict_ack flag (CDC_STRICT_ACK,
		// see resolveStrictAck). ON: position ownership moves entirely to
		// runAckCoordinator's UpdateXLogPos call -- neither lc.Ack() (a
		// no-op call site under ManualCommit) nor keepalives may advance
		// the slot. This MUST land together with the coordinator rewrite
		// below — enabling ManualCommit without a coordinator that
		// actually confirms real SourceAck.SinkID values freezes the slot
		// on the very first event. OFF (§6 rollback path): the vendored
		// library reverts to advancing the slot per-event via lc.Ack(),
		// exactly as before plan 01a -- see createHandler.
		ManualCommit: strictAck,
		// KeepaliveFunc reinstates keepalive-driven advancement for idle
		// streams (WAL-bloat protection) via the ONLY sanctioned
		// fast-forward, AckManager.IdleAdvance, which refuses whenever
		// anything is still pending confirmation. Left wired even when
		// strictAck is false: the vendored stream only ever invokes it
		// when config.ManualCommit is true (pq/replication/stream.go), so
		// it is simply unreachable, harmless dead wiring under the legacy
		// path rather than a second behaviour to maintain.
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

	handler := s.createHandler(&mu, &msgs, knownTables, triggerFlush, strictAck)

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
		s.startConnector(conn, sourceCtx, checkpoint, &mu, &msgs, knownTables, triggerFlush, batchWait, discoveryInterval, srcConfig, &flushWg, &flushClosed)
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
		s.runAckCoordinator(sourceCtx, srcConfig.SlotName, strictAck)
	}()

	// WI-5a: periodic slot-lag probe. This is pure observability -- it
	// never feeds AckManager or gates the slot -- but it is what makes the
	// ManualCommit trade (freeze the slot rather than lose data) safe to
	// operate: without it, a dead sink silently retains unbounded WAL on
	// the source primary with no operator-visible signal.
	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		s.runSlotLagProbe(sourceCtx, srcConfig.SlotName)
	}()

	return s.msgChan, s.ackChan, nil
}

// slotLagProbeInterval is how often runSlotLagProbe re-queries
// pg_replication_slots for the current WAL-vs-confirmed_flush_lsn gap
// (WI-5a). A var, not a const, so tests can shrink it to avoid a
// multi-second sleep; production code never reassigns it.
var slotLagProbeInterval = 15 * time.Second

// runSlotLagProbe periodically exports cdc_source_slot_lag_bytes and
// cdc_source_ack_watermark for this source (WI-5a). It is best-effort and
// deliberately never fatal: a missing slot (fresh deployment, before
// conn.Start has created it) or a query error is logged at debug level and
// the loop simply continues to the next tick. It exits promptly on ctx
// cancellation, mirroring the other s.runWg-registered goroutines above.
func (s *PostgresSource) runSlotLagProbe(ctx context.Context, slotName string) {
	ticker := time.NewTicker(slotLagProbeInterval)
	defer ticker.Stop()

	s.mu.RLock()
	pipelineID := s.pipelineID
	s.mu.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Always export the watermark: it requires no I/O and is
			// needed by the §6 bake period regardless of whether the lag
			// query below succeeds.
			ackWatermarkGauge.WithLabelValues(pipelineID, s.name, slotName).Set(float64(s.ackMgr.Watermark()))

			s.mu.RLock()
			db := s.db
			s.mu.RUnlock()

			fn := s.slotLagBytes
			if fn == nil || db == nil {
				continue
			}

			lagBytes, ok := fn(ctx, db, slotName)
			if !ok {
				// Non-fatal: a fresh deployment has no slot yet, or the
				// query hit a transient error. Never crash or spam above
				// debug -- just skip this tick and retry in 15s.
				//
				// Defect 3 fix: deliberately do NOT touch
				// slotLagProbeLastSuccessGauge here. slotLagBytesGauge keeps
				// its last value on failure (a Prometheus gauge cannot
				// distinguish "unchanged" from "stale"), so a degraded DB
				// connection would otherwise scrape as a stale, healthy-
				// looking lag number forever and silence both slot-lag
				// alerts. Leaving the success-timestamp gauge un-updated is
				// what lets CDCSourceSlotLagProbeStale detect exactly that.
				continue
			}

			slotLagBytesGauge.WithLabelValues(pipelineID, s.name, slotName).Set(float64(lagBytes))
			slotLagProbeLastSuccessGauge.WithLabelValues(pipelineID, s.name, slotName).Set(float64(time.Now().Unix()))
		}
	}
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
//
// strictAck is the §6 flag snapshotted once by the caller (Start) from
// resolveStrictAck, and MUST be threaded through rather than re-read here:
// a mid-life env change must never desync the coordinator from the
// cfg.ManualCommit value the connector was actually built with.
//
// When strictAck is false (the production default, see resolveStrictAck),
// ManualCommit is false and the vendored stream's lc.Ack closure is live —
// it already performs an un-semaphored SendStandbyStatusUpdate on every
// event (pq/replication/stream.go). If this coordinator's UpdateXLogPos
// call ran concurrently with that, two unsynchronized standby-status writes
// could interleave on the wire and corrupt the replication protocol stream
// (standbySem only serializes UpdateXLogPos against itself, not against the
// legacy lc.Ack send). So under strictAck==false this coordinator MUST NOT
// call UpdateXLogPos at all — the slot is advanced exclusively by lc.Ack in
// that mode. Everything else (watermark computation, PendingCount/
// ack_watermark gauges, persistWatermark, and draining s.ackChan) keeps
// running regardless of strictAck, so operators can compare the coordinator's
// watermark against confirmed_flush_lsn during the bake period before
// flipping the flag to true.
func (s *PostgresSource) runAckCoordinator(ctx context.Context, slotName string, strictAck bool) {
	const keepaliveInterval = 500 * time.Millisecond
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()

	// pipelineID is captured once, like runSlotLagProbe does, so
	// pendingLSNsGauge shares an IDENTICAL label set with slotLagBytesGauge
	// and ackWatermarkGauge (WI-5a review Defect 1) -- required for the
	// CDCSourcePendingLSNsStuck alert's `and` join across
	// cdc_source_pending_lsns and cdc_source_ack_watermark to ever match.
	s.mu.RLock()
	pipelineID := s.pipelineID
	s.mu.RUnlock()

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
			pendingLSNsGauge.WithLabelValues(pipelineID, s.name, slotName).Set(float64(s.ackMgr.PendingCount()))

			wm := s.ackMgr.Watermark()

			if now := time.Now(); wm > 0 && now.Sub(lastPersistedAt) >= time.Second {
				s.persistWatermark(wm)
				lastPersistedAt = now
			}

			if wm == 0 || wm == lastFlushedWatermark {
				continue
			}

			if !strictAck {
				// strict_ack OFF: the slot is advanced exclusively by the
				// vendored lc.Ack per-event send (ManualCommit is false).
				// Calling UpdateXLogPos here too would race that unsynchronized
				// SendStandbyStatusUpdate on the wire (see doc above). Track
				// lastFlushedWatermark as if we had flushed so the bake-period
				// watermark/gauges above still reflect real progress without
				// ever touching the wire ourselves.
				lastFlushedWatermark = wm
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
	data, err := protocol.MarshalState(&cp)
	if err != nil {
		log.Debug().Err(err).Msg("persistWatermark: failed to marshal checkpoint")
		return
	}

	key := protocol.SourceWatermarkKey(pipelineID, sourceID)
	if _, err := kv.Put(key, data); err != nil {
		log.Debug().Err(err).Str("key", key).Msg("persistWatermark: failed to write watermark to KV")
	}
}

func (s *PostgresSource) startConnector(conn cdc.Connector, sourceCtx context.Context, checkpoint protocol.Checkpoint, mu *sync.Mutex, msgs *[]protocol.Message, knownTables map[protocol.TableRef]bool, triggerFlush func(), batchWait time.Duration, discoveryInterval time.Duration, srcConfig protocol.SourceConfig, flushWg *sync.WaitGroup, flushClosed *bool) {
	log.Info().Uint64("lsn", checkpoint.IngressLSN).Msg("Starting connector loop")

	if batchWait == 0 {
		batchWait = 500 * time.Millisecond
	}
	if discoveryInterval <= 0 {
		discoveryInterval = 30 * time.Second
	}

	// Prime initial schemas synchronously BEFORE starting connector to prevent race with first data messages
	for _, t := range srcConfig.Tables {
		ref := tableRefFromConfigEntry(t)
		// T1-24 / T1-3 hardening: s.db may be nil when startConnector is
		// invoked outside the Start path (e.g. by a test that swaps the
		// connector factory). In that case there is no live database to
		// query, so schema priming is a no-op — the caller is responsible
		// for seeding any metadata it needs.
		if s.db == nil {
			break
		}
		cols, pks, err := s.getTableMetadata(sourceCtx, s.db, ref.Schema, ref.Table)
		if err == nil {
			m := protocol.Message{
				SourceID: srcConfig.ID, Table: ref.Table, TableSchema: ref.Schema, Op: protocol.OpSchemaChange, Timestamp: time.Now(),
				Schema: &protocol.SchemaMetadata{Table: ref.Table, Schema: ref.Schema, Columns: cols, PKColumns: pks},
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
					// HA-1: a setup failure here means this source finished starting up
					// without capturing anything. Shutdown (context cancelled) is the one
					// benign case and stays at Debug; everything else is a real fault that
					// used to be invisible -- the connector's own log was the only trace,
					// and nothing counted it. Report it loudly and count it so
					// cdc_source_capture_setup_failures_total can alert.
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						log.Debug().Err(err).Msg("B3: WaitUntilReady did not succeed (shutdown); skipping post-ready fresh-slot seed")
						return
					}
					reason := "other"
					if errors.Is(err, replication.ErrorSlotInUse) {
						reason = "slot_in_use"
					}
					metrics.SourceCaptureSetupFailures.
						WithLabelValues(s.name, srcConfig.SlotName, reason).Inc()
					log.Error().Err(err).
						Str("source", s.name).
						Str("slot", srcConfig.SlotName).
						Str("reason", reason).
						Msg("source failed to start capturing: this worker is running but ingesting nothing")
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
		// R9: block any further triggerFlush calls from being admitted,
		// then wait for whatever was already admitted (including the
		// final flush just above) to finish its send before closing. See
		// the flushWg/flushClosed comment above for why the old
		// time.Sleep(100ms) band-aid was not actually sufficient.
		mu.Lock()
		*flushClosed = true
		mu.Unlock()
		flushWg.Wait()
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
		pipelineID, slotName := s.pipelineID, s.config.SlotName
		s.mu.Unlock()

		// WI-5a: drop this source's gauge series. The process outlives an
		// individual pipeline (cmd/pipeline starts and stops them from KV
		// config), so without this a deliberately stopped pipeline leaves
		// slot_lag_probe_last_success frozen while time() advances —
		// CDCSourceSlotLagProbeStale then fires forever and never resolves,
		// and a non-zero pending_lsns at stop latches
		// CDCSourcePendingLSNsStuck the same way. Alert fatigue on exactly
		// these alerts would defeat the purpose of shipping them.
		labels := prometheus.Labels{"pipeline": pipelineID, "source": s.name, "slot": slotName}
		pendingLSNsGauge.DeletePartialMatch(labels)
		slotLagBytesGauge.DeletePartialMatch(labels)
		ackWatermarkGauge.DeletePartialMatch(labels)
		slotLagProbeLastSuccessGauge.DeletePartialMatch(labels)
	})
	return nil
}

// AlterPublication issues ALTER PUBLICATION ... ADD TABLE for tableName,
// which may be bare ("orders") or schema-qualified ("sales.orders") --
// MULTI_SCHEMA_PLAN.md §3 Stage 2 item 3. It is always emitted as a quoted,
// schema-qualified identifier ("schema"."table") rather than the previous
// raw %s interpolation of the caller's string: unquoted, an unqualified
// name resolves against whatever search_path this DB connection happens to
// have, which becomes actively wrong once the replication connection's
// search_path is pinned to a non-public schema (see Start's search_path
// pinning below) -- and raw interpolation of an operator-controlled table
// name is also a SQL-injection surface this closes as a side effect.
func (s *PostgresSource) AlterPublication(ctx context.Context, tableName string) error {
	s.mu.RLock()
	db := s.db
	pubName := s.config.PublicationName
	s.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("database connection not initialized")
	}

	ref := tableRefFromConfigEntry(tableName)
	quotedTable := libpq.QuoteIdentifier(ref.Schema) + "." + libpq.QuoteIdentifier(ref.Table)

	// Retry logic for "publication does not exist" which can happen due to replication lag or race
	var lastErr error
	for i := 0; i < 10; i++ {
		execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		query := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", pubName, quotedTable)
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

// discoverTables polls information_schema for new tables in the
// whitelisted schemas (MULTI_SCHEMA_PLAN.md §3 Stage 2).
//
// CRITICAL SEMANTIC (plan §8 item 4): an empty/nil srcConfig.Schemas means
// "public" ONLY, NOT all schemas. Every existing config has Schemas empty
// today (the field was collected by the UI and stored in KV but never read
// by any backend code path before this), so treating empty as "all schemas"
// would silently start replicating every schema in the database the moment
// this ships -- see the (now corrected) doc comment on the generated
// SourceConfig.Schemas field (internal/api/generated.go, sourced from
// docs/openapi.yaml).
func (s *PostgresSource) discoverTables(ctx context.Context, db *sql.DB, srcConfig protocol.SourceConfig, mu *sync.Mutex, msgs *[]protocol.Message, knownTables map[protocol.TableRef]bool, triggerFlush func()) {
	schemas := srcConfig.Schemas
	if len(schemas) == 0 {
		schemas = []string{"public"}
	}

	// table_type = 'BASE TABLE' excludes views/foreign tables, which cannot
	// be added to a logical replication publication. The schema exclusions
	// are a defence-in-depth belt: table_schema = ANY($1) already scopes to
	// the configured whitelist, but a misconfigured Schemas entry naming a
	// system schema must never be able to pull catalog/temp tables into
	// discovery.
	query := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema = ANY($1)
		  AND table_type = 'BASE TABLE'
		  AND table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		  AND table_schema NOT LIKE 'pg_temp_%'
	`
	rows, err := db.QueryContext(ctx, query, schemas)
	if err != nil {
		log.Error().Err(err).Msg("Failed to discover tables")
		return
	}
	defer rows.Close()

	foundNew := false
	for rows.Next() {
		var schema, tableName string
		if err := rows.Scan(&schema, &tableName); err != nil {
			continue
		}
		ref := protocol.TableRef{Schema: protocol.NormalizeSchema(schema), Table: tableName}

		if !knownTables[ref] {
			log.Info().Str("schema", ref.Schema).Str("table", ref.Table).Msg("New table discovered")
			if strings.Contains(tableName, "cdc_snapshot") {
				knownTables[ref] = true
				continue
			}

			cols, pks, err := s.getTableMetadata(ctx, db, ref.Schema, ref.Table)
			if err != nil {
				log.Error().Err(err).Str("schema", ref.Schema).Str("table", ref.Table).Msg("Failed to get metadata for discovered table")
				continue
			}

			m := protocol.Message{
				SourceID: srcConfig.ID, Table: ref.Table, TableSchema: ref.Schema, Op: protocol.OpSchemaChange, Timestamp: time.Now(),
				Schema: &protocol.SchemaMetadata{Table: ref.Table, Schema: ref.Schema, Columns: cols, PKColumns: pks},
			}
			mu.Lock()
			*msgs = append(*msgs, m)
			knownTables[ref] = true
			mu.Unlock()
			foundNew = true
		}
	}

	if foundNew {
		triggerFlush()
	}
}

// sanitizePayload normalizes a decoded row into (a) a plain,
// transport-and-sink-safe Data map -- byte-identical in shape and content to
// what every sink reads today, whether or not the pipeline runs the
// nats/protobuf processor -- and (b) an optional ColumnKinds side-channel
// naming which Data entries carry a source type that can't itself survive
// protocol.Message's internal NATS JetStream transport (msgpack via
// generated WriteIntf/ReadIntf, whose reflection fallback only supports
// Ptr/Slice/Map -- a struct like pgtype.Numeric is msgp.ErrUnsupportedType
// and hard-fails MarshalMsg outright, not "silently corrupts").
//
// Earlier revisions of this fix tried tagging the decimal text itself with
// an in-band string marker (a NUL-prefixed prefix baked into the Data
// value). That was rejected on review: Data is read by every sink
// unconditionally (sink/databend, sink/postgresdebug), by
// transformer/builtin.go's masking, and by the delete-PK WHERE-clause path
// -- none of which know about a transformer-private encoding, so the marker
// leaked a literal NUL byte into every consumer that isn't the
// nats/protobuf transformer, which is a hard Postgres encoding error for
// sink/postgresdebug and a silent regression (wrong stored text, or a
// PK WHERE clause that matches zero rows) everywhere else. ColumnKinds
// keeps Data exactly as every existing consumer already reads it and
// carries the extra information the *few* consumers that need it
// (currently just internal/transformer/nats/protobuf.go's encodeTypedValue)
// out-of-band instead.
func sanitizePayload(in map[string]any) (map[string]any, map[string]string) {
	if in == nil {
		return nil, nil
	}
	out := make(map[string]any, len(in))
	var kinds map[string]string
	for k, v := range in {
		val, kind := sanitizeValue(v)
		out[k] = val
		if kind != "" {
			if kinds == nil {
				kinds = make(map[string]string, len(in))
			}
			kinds[k] = kind
		}
	}
	return out, kinds
}

// sanitizeValue normalizes one decoded column value into a form that
// survives protocol.Message's internal NATS JetStream transport (see
// sanitizePayload's doc comment), returning the transport/sink-safe value
// and an optional kind hint (currently only protocol.ColumnKindDecimal) for
// callers that want to populate ColumnKinds.
//
// Two scalar shapes need help:
//
//   - [16]byte (pgtype.UUIDCodec.DecodeValue's TextFormatCode result,
//     pgtype.UUID.Bytes verbatim -- not uuid.UUID, not driver.Valuer) is a
//     fixed-size array: msgp.WriteIntf has no case for reflect.Array at all,
//     so this used to hard-fail message encoding for every uuid column
//     (batch stall, not corruption). Converted here to its canonical UUID
//     string, which is both msgpack-safe and what encodeTypedValue's
//     string_value case expects. No kind hint needed -- a UUID string is
//     exactly what every consumer, including encodeTypedValue, already
//     wants for this column.
//
//   - pgtype.Numeric *does* implement driver.Valuer (so the generic branch
//     below would handle it without crashing), but its Value() collapses
//     straight to a plain Go string -- indistinguishable, once it crosses
//     the msgpack hop, from a genuine text/CITEXT column. Converted here to
//     its exact decimal text via Value() (the codec's own TextFormatCode
//     encoder, never a float) -- identical to what every sink already
//     stores today -- with protocol.ColumnKindDecimal returned as the kind
//     hint so a kind-aware encoder can still route it to decimal_value
//     without the value itself needing to change.
//
// pgtype.Interval and pgtype.Bits are deliberately NOT special-cased the
// same way as Numeric: TypedValue has no interval_value/bits_value oneof
// kind, so there is no routing distinction worth preserving, and their
// driver.Valuer.Value() (also exact canonical text, not lossy) is exactly
// the right msgpack-safe form to send as string_value. They fall through to
// the generic driver.Valuer branch below like every other pgtype scalar
// (timestamptz, date, etc.).
//
// Also recurses into slices/maps, since pgx decodes an array-typed column
// (uuid[], numeric[], ...) as []interface{} of these same scalar types, and
// they need identical transport-safety treatment one level down. Per-element
// kind hints are not propagated (ColumnKinds is keyed by top-level column
// name only) -- an array-typed decimal column already goes through
// encodeTypedValue's json_value path today, which is unaffected by this
// fix either way; only the scalar decimal_value routing is in scope here.
// sanitizeNumeric renders a pgtype.Numeric as its canonical decimal text (or
// nil for SQL NULL / an unrepresentable value), tagged with
// protocol.ColumnKindDecimal so the encoder can route it to decimal_value.
// Split out of sanitizeValue purely to keep that function's dispatch
// readable -- see the sanitizeValue doc comment above for why this case
// exists.
func sanitizeNumeric(n pgtype.Numeric) (any, string) {
	if !n.Valid {
		return nil, ""
	}
	val, err := n.Value()
	if err != nil {
		return nil, ""
	}
	text, ok := val.(string)
	if !ok {
		return fmt.Sprintf("%v", val), ""
	}
	return text, protocol.ColumnKindDecimal
}

func sanitizeValue(v any) (any, string) {
	if v == nil {
		return nil, ""
	}

	if n, ok := v.(pgtype.Numeric); ok {
		return sanitizeNumeric(n)
	}

	if valuer, ok := v.(driver.Valuer); ok {
		val, err := valuer.Value()
		if err != nil {
			return nil, ""
		}
		return val, ""
	}

	if b, ok := v.([16]byte); ok {
		return uuid.UUID(b).String(), ""
	}

	if _, ok := v.([]byte); ok {
		// bytea and similar -- leave intact. encodeTypedValue's own []byte
		// case handles UUID-shaped ([16]byte-length) and general binary
		// data; recursing into it here would explode it into a []any of
		// individual byte values instead.
		return v, ""
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice:
		if rv.IsNil() {
			return v, ""
		}
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			elem, _ := sanitizeValue(rv.Index(i).Interface())
			out[i] = elem
		}
		return out, ""
	case reflect.Map:
		if rv.IsNil() {
			return v, ""
		}
		out := make(map[string]any, rv.Len())
		for _, key := range rv.MapKeys() {
			elem, _ := sanitizeValue(rv.MapIndex(key).Interface())
			out[fmt.Sprintf("%v", key.Interface())] = elem
		}
		return out, ""
	default:
		return v, ""
	}
}
