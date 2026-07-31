package e2e

// Plan 01a (summaries/holistic_review_result/plans/01a_delivery_source_ack.md),
// WI-10 / §5 tests 20-27: e2e proof that the replication slot's
// confirmed_flush_lsn never advances past an LSN no sink has durably
// written. Every test here runs with CDC_STRICT_ACK explicitly set (never
// relying on the ambient default) so the suite can't silently start
// exercising legacy behavior later.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/Trendyol/go-pq-cdc/pq"
	go_nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// realSlotName reproduces pipeline.go's slot-name suffixing
// (srcCfg.SlotName = "<configured>_<pipelineID-with-dashes-as-underscores>").
func realSlotName(base, pipelineID string) string {
	return fmt.Sprintf("%s_%s", base, sanitizeForSlot(pipelineID))
}

func sanitizeForSlot(id string) string {
	out := make([]rune, 0, len(id))
	for _, r := range id {
		if r == '-' {
			out = append(out, '_')
		} else {
			out = append(out, r)
		}
	}
	return string(out)
}

// confirmedFlushLSN queries pg_replication_slots for the given slot's
// confirmed_flush_lsn, parsed to a comparable uint64. Precedent:
// pressure_test.go:99.
func confirmedFlushLSN(t *testing.T, e *Environment, slotName string) (uint64, bool) {
	t.Helper()
	var lsnStr *string
	err := e.Postgres.QueryRow(
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", slotName,
	).Scan(&lsnStr)
	if err != nil || lsnStr == nil {
		return 0, false
	}
	lsn, err := pq.ParseLSN(*lsnStr)
	if err != nil {
		return 0, false
	}
	return uint64(lsn), true
}

func currentWalLSN(t *testing.T, e *Environment) uint64 {
	t.Helper()
	var lsnStr string
	err := e.Postgres.QueryRow("SELECT pg_current_wal_lsn()").Scan(&lsnStr)
	require.NoError(t, err)
	lsn, err := pq.ParseLSN(lsnStr)
	require.NoError(t, err)
	return uint64(lsn)
}

// putGateSinkConfig registers a "gate" sink (internal/test/e2e/gate_sink.go)
// in KV under sinkID and returns it once the worker has constructed it.
func setupGateSink(t *testing.T, e *Environment, sinkID string) {
	t.Helper()
	cfg := protocol.SinkConfig{
		ID:   sinkID,
		Type: "gate",
		DSN:  encryptForKV(t, "unused"),
	}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	_, err = e.KV.Put(protocol.SinkConfigKey(sinkID), data)
	require.NoError(t, err)
}

func waitForGateSink(t *testing.T, sinkID string, timeout time.Duration) *gateSink {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if gs := GetGateSink(sinkID); gs != nil {
			return gs
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("gate sink %s was never constructed by the worker", sinkID)
	return nil
}

// ---------------------------------------------------------------------
// Test 20 (headline invariant): the slot never advances past an LSN whose
// event has not been durably written by every configured sink.
// ---------------------------------------------------------------------
func TestSlotNeverAdvancesBeforeSinkAck(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}
	t.Setenv("CDC_STRICT_ACK", "true")

	env := Setup(t)
	defer env.Cleanup()

	const table = "slot_gate_table"
	const sinkID = "gate1"
	const pipelineID = "p_slot_gate"

	setupGateSink(t, env, sinkID)

	pipeCfg := protocol.PipelineConfig{
		ID:        pipelineID,
		Name:      "Slot Gate Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{sinkID},
		Tables:    []string{table},
		BatchSize: 5,
		BatchWait: 100 * time.Millisecond,
	}
	require.NoError(t, env.SetPipelineConfig(pipelineID, pipeCfg))

	env.SeedPostgres(table, 0)

	// Block the sink BEFORE starting the worker so the very first batch is
	// held.
	BlockOnConstruct(sinkID)

	env.StartWorker()

	realSlot := realSlotName(env.PgConfig.SlotName, pipelineID)
	require.Eventually(t, func() bool {
		_, ok := confirmedFlushLSN(t, env, realSlot)
		return ok
	}, 30*time.Second, 500*time.Millisecond, "replication slot never appeared")

	// Insert rows and capture the commit LSN.
	for i := 1; i <= 20; i++ {
		_, err := env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1,$2)", table), fmt.Sprintf("gate-%d", i), 20+i)
		require.NoError(t, err)
	}
	commitLSN := currentWalLSN(t, env)

	// Wait until the ingest stream has actually carried the rows so we know
	// the coordinator has real pending LSNs to (not) confirm, not just an
	// empty/idle window.
	realGate := waitForGateSink(t, sinkID, 15*time.Second)

	// Gate on a POSITIVE signal that all 20 rows are genuinely in flight --
	// decoded by the source, Observed by the AckManager, and published --
	// before asserting anything about the slot.
	//
	// The previous gate here polled for `confirmed_flush_lsn > 0` and
	// admitted, in its own comment, that this was an approximation ("slot
	// existing and some time having passed"). That is satisfiable long
	// before the rows are pending: the slot is already non-zero from
	// creation/the B3 seed. Under load the assertion loop could therefore
	// start while the AckManager was still empty, at which point IdleAdvance
	// may legitimately fast-forward toward the WAL end -- and commitLSN was
	// captured from pg_current_wal_lsn(), so the slot could pass it without
	// the invariant being violated at all. That is the flake: a real
	// invariant, asserted in a window where it did not yet apply.
	//
	// Counting the ingest stream is the same technique the sibling
	// TestKeepaliveDoesNotConfirmInflight uses, and it is a fact about the
	// system rather than about elapsed time.
	nc, err := go_nats.Connect(env.NatsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := nc.JetStream()
	require.NoError(t, err)
	ingestStream := fmt.Sprintf("cdc_pipeline_%s_ingest", pipelineID)
	require.Eventually(t, func() bool {
		si, err := js.StreamInfo(ingestStream)
		if err != nil {
			return false
		}
		return si.State.Msgs >= 20
	}, 30*time.Second, 250*time.Millisecond,
		"ingest stream never carried all 20 rows -- the slot invariant cannot be proven until they are provably pending")

	// The sink is blocked and stays blocked: poll confirmed_flush_lsn across
	// several coordinator ticks (500ms cadence) and assert it never reaches
	// commitLSN.
	for i := 0; i < 8; i++ {
		lsn, ok := confirmedFlushLSN(t, env, realSlot)
		require.True(t, ok)
		require.Lessf(t, lsn, commitLSN,
			"confirmed_flush_lsn (%d) reached/passed the commit LSN (%d) while the sink was still blocked -- "+
				"this is exactly the pre-fix per-event-ack / keepalive-fast-forward bug", lsn, commitLSN)
		time.Sleep(600 * time.Millisecond)
	}

	// Unblock and assert the slot catches up.
	realGate.Unblock()
	require.Eventually(t, func() bool {
		lsn, ok := confirmedFlushLSN(t, env, realSlot)
		return ok && lsn >= commitLSN
	}, 30*time.Second, 500*time.Millisecond, "confirmed_flush_lsn never advanced past the commit LSN after unblocking the sink")

	require.GreaterOrEqual(t, realGate.Count(), 20, "gate sink should have received all 20 rows once unblocked")
}

// ---------------------------------------------------------------------
// Test 21: keepalives must not confirm in-flight (unacked) LSNs, even under
// WAL churn on a second table. This is the interim owner of vendored unit
// tests 11-13 (impossible to run standalone -- the vendored module can't
// build outside go-pq-cdc, and LoadXLogPos isn't exported), so it also
// stands in for those. It also proves the flip side: AckManager.IdleAdvance
// still lets a fully-acked, idle pipeline follow ServerWALEnd so the slot
// doesn't bloat WAL retention forever.
//
// Note: churnTable lives in a schema outside the pipeline's configured
// Tables/Schemas, so its transactions contribute pure WAL/keepalive traffic
// and are never decoded into a CDC event at all (see the comment at
// churnSchema's declaration below for why an earlier version of this test,
// which let auto-discovery pull the churn table into CDC, produced a false
// failure).
//
// PREVIOUSLY KNOWN FAILING, now fixed by vendored patch T0-3
// (internal/vendor/go-pq-cdc/pq/replication/stream.go): confirmed_flush_lsn
// was observed to cross commitLSN by a small, consistent margin shortly
// after churn started, while the sink was still provably blocked and
// gate2.Count() == 0. Instrumentation traced this to AckManager.IdleAdvance,
// not to ObserveConfirmed self-acking (an earlier theory recorded here was
// wrong on the mechanism: instrumented runs showed *zero* ObserveConfirmed
// calls for the churn table's BEGIN/COMMIT records -- PG16's pgoutput skips
// emitting anything at all for empty/unpublished transactions, so there was
// no self-ack to blame). The real cause was a stream-ordering race in the
// vendored replication library: handleKeepalive invoked KeepaliveFunc
// inline on the sink goroutine, bypassing messageCH (the queue that
// actually delivers decoded messages to Observe). On this test's very first
// keepalive after the 10 gated rows were decoded, "nothing pending" in
// AckManager could still be true while a decoded row sat in messageCH not
// yet Observe()'d, so IdleAdvance's len(lsns)==0 guard fast-forwarded the
// watermark past it. T0-3 fixes this by delivering the keepalive in band
// through messageCH itself, so KeepaliveFunc only fires once every
// previously decoded message has already reached Observe. AckManager also
// gained a highestSeen-based defence-in-depth check (see IdleAdvance and
// the idleTrusted field) so a future regression of that ordering guarantee
// would be refused (and logged loudly) rather than silently losing data.
// ---------------------------------------------------------------------
func TestKeepaliveDoesNotConfirmInflight(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}
	t.Setenv("CDC_STRICT_ACK", "true")

	env := Setup(t)
	defer env.Cleanup()

	const table = "keepalive_gate_table"
	// churnTable lives in a schema NOT in env.PgConfig.Schemas (["public"]
	// only), so this pipeline's discovery loop never sees it and never
	// dynamically adds it to CDC. That matters: an earlier version of this
	// test put the churn table in "public" and it got auto-discovered and
	// added to CDC mid-test (observed live: "New table discovered via CDC,
	// starting dynamic addition"), which then self-acked its own snapshot/
	// schema-change bookkeeping LSNs independently of the gated sink and
	// intermittently landed exactly on (or past) the captured commitLSN --
	// a false failure caused by the test's own dynamic-discovery side
	// effect, not a real keepalive-fast-forward bug. Keeping churnTable
	// entirely outside the source's configured schemas guarantees it can
	// only ever contribute physical WAL/keepalive traffic, never decoded,
	// self-acking CDC events.
	const churnSchema = "churn_schema"
	const churnTable = churnSchema + ".keepalive_churn_table"
	const sinkID = "gate2"
	const pipelineID = "p_keepalive_gate"

	setupGateSink(t, env, sinkID)

	pipeCfg := protocol.PipelineConfig{
		ID:        pipelineID,
		Name:      "Keepalive Gate Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{sinkID},
		Tables:    []string{table}, // churnTable deliberately excluded from CDC tables
		BatchSize: 5,
		BatchWait: 100 * time.Millisecond,
	}
	require.NoError(t, env.SetPipelineConfig(pipelineID, pipeCfg))

	env.SeedPostgres(table, 0)
	_, err := env.Postgres.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", churnSchema))
	require.NoError(t, err)
	_, err = env.Postgres.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, name TEXT, age INT)", churnTable))
	require.NoError(t, err)

	// Block the sink before the worker starts so the first batch is held.
	BlockOnConstruct(sinkID)

	env.StartWorker()

	realSlot := realSlotName(env.PgConfig.SlotName, pipelineID)
	require.Eventually(t, func() bool {
		_, ok := confirmedFlushLSN(t, env, realSlot)
		return ok
	}, 30*time.Second, 500*time.Millisecond, "replication slot never appeared")

	// Insert rows into the gated table and capture the commit LSN.
	for i := 1; i <= 10; i++ {
		_, err := env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1,$2)", table), fmt.Sprintf("ka-%d", i), 20+i)
		require.NoError(t, err)
	}
	commitLSN := currentWalLSN(t, env)

	waitForGateSink(t, sinkID, 15*time.Second)
	require.Eventually(t, func() bool {
		lsn, ok := confirmedFlushLSN(t, env, realSlot)
		return ok && lsn > 0
	}, 20*time.Second, 500*time.Millisecond, "slot never advanced past its initial position at all (nothing flowing)")

	// Before generating churn, make certain the source has actually decoded
	// and published all 10 rows -- i.e. that AckManager has real pending
	// entries for them, not just that the slot exists. Otherwise a keepalive
	// racing ahead of logical decode (rather than a fast-forward bug) could
	// produce a false positive/negative. Precedent for ingest-stream
	// introspection: pressure_test.go.
	nc, err := go_nats.Connect(env.NatsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := nc.JetStream()
	require.NoError(t, err)
	ingestStream := fmt.Sprintf("cdc_pipeline_%s_ingest", pipelineID)
	require.Eventually(t, func() bool {
		si, err := js.StreamInfo(ingestStream)
		if err != nil {
			return false
		}
		return si.State.Msgs >= 10
	}, 20*time.Second, 500*time.Millisecond, "ingest stream never received all 10 rows -- can't yet prove the keepalive invariant")

	// Generate WAL churn on a table NOT tracked by this pipeline's CDC
	// config, so replication ServerWALEnd keeps climbing well past
	// commitLSN while the pipeline's own in-flight LSN sits unacked. This
	// reproduces exactly the scenario a keepalive-driven fast-forward bug
	// would fail: high ServerWALEnd + blocked sink.
	stopChurn := make(chan struct{})
	churnDone := make(chan struct{})
	go func() {
		defer close(churnDone)
		i := 0
		for {
			select {
			case <-stopChurn:
				return
			default:
			}
			i++
			env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1,$2)", churnTable), fmt.Sprintf("churn-%d", i), i)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Poll across several coordinator ticks (500ms cadence) with churn
	// running; confirmed_flush_lsn must stay strictly below commitLSN.
	for i := 0; i < 8; i++ {
		lsn, ok := confirmedFlushLSN(t, env, realSlot)
		require.True(t, ok)
		require.Lessf(t, lsn, commitLSN,
			"confirmed_flush_lsn (%d) reached/passed the commit LSN (%d) under WAL churn while the sink was still "+
				"blocked -- this is exactly the pre-fix keepalive-fast-forward bug (ServerWALEnd from unrelated "+
				"WAL activity confirming in-flight, unacked LSNs)", lsn, commitLSN)
		time.Sleep(600 * time.Millisecond)
	}

	close(stopChurn)
	<-churnDone

	// Unblock the sink: the pipeline's own LSN should now get acked and the
	// slot should catch up past commitLSN.
	gs := GetGateSink(sinkID)
	require.NotNil(t, gs)
	gs.Unblock()

	require.Eventually(t, func() bool {
		lsn, ok := confirmedFlushLSN(t, env, realSlot)
		return ok && lsn >= commitLSN
	}, 30*time.Second, 500*time.Millisecond, "confirmed_flush_lsn never advanced past the commit LSN after unblocking the sink")

	// Second half: with a fully-acked, now-idle pipeline, further keepalives
	// (driven by more churn on the untracked table) SHOULD advance the slot
	// past the churn activity's WAL position -- proving IdleAdvance still
	// prevents WAL bloat once there is nothing pending. Capture a fresh WAL
	// position after more churn and assert the slot eventually reaches it
	// with no further Postgres inserts on the tracked table.
	for i := 1; i <= 5; i++ {
		env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1,$2)", churnTable), fmt.Sprintf("idle-churn-%d", i), i)
	}
	idleTargetLSN := currentWalLSN(t, env)

	require.Eventually(t, func() bool {
		lsn, ok := confirmedFlushLSN(t, env, realSlot)
		return ok && lsn >= idleTargetLSN
	}, 30*time.Second, 500*time.Millisecond,
		"idle, fully-acked pipeline never followed ServerWALEnd via IdleAdvance -- slot would retain WAL forever")
}

// ---------------------------------------------------------------------
// Test 22: crashing the worker between "message published to the ingest
// stream" and "sink durably wrote it" must not lose the message. This is
// the exact Critical-1 crash window from the plan: a producer can publish
// to NATS and crash before the sink (or the source's ack coordinator) ever
// confirms, and on restart every such message must still reach the sink
// (duplicates are fine -- the guarantee is at-least-once delivery to the
// sink, not exactly-once redelivery).
// ---------------------------------------------------------------------
func TestCrashBetweenPublishAndSinkReplays(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}
	t.Setenv("CDC_STRICT_ACK", "true")

	env := Setup(t)
	defer env.Cleanup()

	const table = "crash_replay_table"
	const sinkID = "gate3"
	const pipelineID = "p_crash_replay"
	const rowCount = 30

	setupGateSink(t, env, sinkID)

	pipeCfg := protocol.PipelineConfig{
		ID:        pipelineID,
		Name:      "Crash Replay Test",
		Sources:   []string{env.PgConfig.ID},
		Sinks:     []string{sinkID},
		Tables:    []string{table},
		BatchSize: 5,
		BatchWait: 100 * time.Millisecond,
	}
	require.NoError(t, env.SetPipelineConfig(pipelineID, pipeCfg))
	env.SeedPostgres(table, 0)

	// Block the sink so messages accumulate in the ingest stream, delivered
	// to the consumer but not yet durably written -- exactly the crash
	// window under test.
	BlockOnConstruct(sinkID)
	env.StartWorker()

	realSlot := realSlotName(env.PgConfig.SlotName, pipelineID)
	require.Eventually(t, func() bool {
		_, ok := confirmedFlushLSN(t, env, realSlot)
		return ok
	}, 30*time.Second, 500*time.Millisecond, "replication slot never appeared")

	for i := 1; i <= rowCount; i++ {
		_, err := env.Postgres.Exec(fmt.Sprintf("INSERT INTO %s (name, age) VALUES ($1,$2)", table), fmt.Sprintf("crash-%d", i), 20+i)
		require.NoError(t, err)
	}

	// Wait until the ingest stream has actually carried all the rows to the
	// consumer (i.e. delivered, not yet acked) before "crashing".
	nc, err := go_nats.Connect(env.NatsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := nc.JetStream()
	require.NoError(t, err)
	ingestStream := fmt.Sprintf("cdc_pipeline_%s_ingest", pipelineID)
	require.Eventually(t, func() bool {
		si, err := js.StreamInfo(ingestStream)
		if err != nil {
			return false
		}
		return si.State.Msgs >= rowCount
	}, 30*time.Second, 500*time.Millisecond, "ingest stream never received all rows before the simulated crash")

	// Simulated crash: stop the worker (and hence the source/consumer)
	// while the sink is still blocked and nothing has been durably written.
	// Mgr.Stop is the harness's only available "kill" primitive (no OS
	// process boundary to SIGKILL in this in-process test harness), but it
	// exercises the same code path a real crash-then-restart would need to
	// recover through: an unclean stop with in-flight, unacked messages.
	env.Mgr.Stop(context.Background())
	time.Sleep(2 * time.Second)

	// Restart against the same slot/pipeline. The worker constructs a fresh
	// gate sink instance under the same sinkID; it is not registered via
	// BlockOnConstruct this time, so it starts unblocked and replay can
	// actually complete.
	env.StartWorker()
	env.EventuallyAssertHeartbeat(pipelineID, "Running", 30*time.Second)

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if gs := GetGateSink(sinkID); gs != nil && gs.Count() >= rowCount {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	gs := GetGateSink(sinkID)
	require.NotNil(t, gs, "gate sink was never reconstructed after restart")
	require.GreaterOrEqual(t, gs.Count(), rowCount,
		"not every row reached the sink after crash+restart -- messages were lost in the publish-but-not-acked window")

	// Duplicates are permitted (at-least-once), but every one of the
	// rowCount distinct names inserted must appear at least once.
	seen := make(map[string]bool)
	for _, m := range gs.Rows() {
		if name, ok := m.Data["name"].(string); ok {
			seen[name] = true
		}
	}
	require.GreaterOrEqual(t, len(seen), rowCount,
		"only %d distinct rows out of %d reached the sink after crash+restart -- data was lost, not just duplicated", len(seen), rowCount)
}
