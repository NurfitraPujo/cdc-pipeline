package replication

import (
	"context"
	"encoding/binary"
	goerrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/avast/retry-go/v4"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// vendored-patch: T0-2 - returned by UpdateXLogPos when there is no usable connection to send
// a standby status update on. Distinguishable so callers can treat it as expected during
// shutdown instead of as a failed slot advance.
var ErrStreamClosed = goerrors.New("replication stream connection is closed")

// vendored-patch: T0-2 - returned by UpdateXLogPos when a previous standby status write is
// still blocked on the socket. The in-memory position was still advanced (the monotonic store
// happens first); only the network send was skipped, so the caller should simply retry later
// rather than treat this as a failed advance.
var ErrStandbyWriteInFlight = goerrors.New("standby status update already in flight")

var (
	ErrorSlotInUse    = errors.New("replication slot in use")
	ErrorNotConnected = errors.New("stream is not connected")
)

const (
	StandbyStatusUpdateByteID = 'r'
)

type ListenerContext struct {
	Message any
	Ack     func() error
	LSN     pq.LSN
}

type ListenerFunc func(ctx *ListenerContext)

type Message struct {
	message  any
	walStart int64
}

// vendored-patch: T0-3 - keepaliveMarker is a distinguishable payload type enqueued onto
// messageCH so a keepalive-driven WAL-end advance is delivered to KeepaliveFunc strictly
// after every preceding decoded message has already passed through listenerFunc/Observe.
// process() recognizes this type and must never let it reach listenerFunc.
type keepaliveMarker struct{}

type Streamer interface {
	Connect(ctx context.Context) error
	Open(ctx context.Context) error
	Close(ctx context.Context)
	GetSystemInfo() *pq.IdentifySystemResult
	GetMetric() metric.Metric
	OpenFromSnapshotLSN()
	// vendored-patch: T0-2 - ctx bounds the standby-status write; error lets the caller
	// detect that the replication slot did not advance.
	UpdateXLogPos(ctx context.Context, lsn pq.LSN) error
	AddRelation(rel *format.Relation)
}

type stream struct {
	conn                pq.Connection
	metric              metric.Metric
	system              *pq.IdentifySystemResult
	relation            map[uint32]*format.Relation
	messageCH           chan *Message
	listenerFunc        ListenerFunc
	sinkEnd             chan struct{}
	closeSinkEndOnce    sync.Once // vendored-patch: T1-5 - Prevent double-close of sinkEnd channel
	mu                  *sync.RWMutex
	config              config.Config
	lastXLogPos         pq.LSN
	snapshotLSN         pq.LSN
	openFromSnapshotLSN bool
	closed              atomic.Bool
	// vendored-patch: T0-2 - capacity-1 semaphore serialising standby status writes issued by
	// UpdateXLogPos. Without it, a write that blocks on a full TCP send buffer plus a caller
	// that retries on the next tick would put two goroutines into
	// Frontend().SendUnbufferedEncodedCopyData concurrently and interleave protocol frames.
	standbySem chan struct{}
}

func NewStream(dsn string, cfg config.Config, m metric.Metric, listenerFunc ListenerFunc) Streamer {
	return &stream{
		conn:         pq.NewConnectionTemplate(dsn),
		metric:       m,
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		messageCH:    make(chan *Message, 1000),
		listenerFunc: listenerFunc,
		// lastXLogPos:0 is not magical, 0 means, create replication starts with confirmed_flush_lsn
		// https://github.com/postgres/postgres/blob/master/src/include/access/xlogdefs.h#L28
		// https://github.com/postgres/postgres/blob/master/src/backend/replication/logical/logical.c#L540
		lastXLogPos: 0,
		sinkEnd:     make(chan struct{}, 1),
		mu:          &sync.RWMutex{},
		// vendored-patch: T0-2
		standbySem: make(chan struct{}, 1),
	}
}

func (s *stream) Connect(ctx context.Context) error {
	if err := s.conn.Connect(ctx); err != nil {
		return errors.Wrap(err, "stream connection")
	}

	system, err := pq.IdentifySystem(ctx, s.conn)
	if err != nil {
		_ = s.conn.Close(ctx)
		return errors.Wrap(err, "identify system")
	}

	s.system = &system
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database:", system.Database)
	return nil
}

func (s *stream) Open(ctx context.Context) error {
	if s.conn.IsClosed() {
		return ErrorNotConnected
	}

	if err := s.setup(ctx); err != nil {
		s.sinkEnd <- struct{}{}

		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "55006" {
			return ErrorSlotInUse
		}
		return errors.Wrap(err, "replication setup")
	}

	go s.sink(ctx)

	go s.process(ctx)

	logger.Info("cdc stream started")

	return nil
}

func (s *stream) setup(ctx context.Context) error {
	replication := New(s.conn)

	replicationStartLsn := s.lastXLogPos
	if s.openFromSnapshotLSN {
		snapshotLSN, err := s.fetchSnapshotLSN(ctx)
		if err != nil {
			return errors.Wrap(err, "fetch snapshot LSN")
		}
		replicationStartLsn = snapshotLSN
	}

	if err := replication.Start(s.config.Publication.Name, s.config.Slot.Name, replicationStartLsn); err != nil {
		return err
	}

	if err := replication.Test(ctx); err != nil {
		return err
	}

	if s.openFromSnapshotLSN {
		logger.Info("replication started from snapshot LSN", "slot", s.config.Slot.Name, "lsn", replicationStartLsn.String())
	} else {
		logger.Info("replication started from confirmed_flush_lsn", "slot", s.config.Slot.Name)
	}

	return nil
}

// messageBuffer manages a one-message look-ahead buffer.
//
// The last DML message in each transaction is held back so its WAL position
// can be rewritten to the transaction-end LSN (from COMMIT / STREAM COMMIT).
// All preceding messages are emitted immediately with their original position.
// This keeps memory usage O(1) regardless of transaction size.
type messageBuffer struct {
	pending *Message
	outCh   chan<- *Message
}

// flush emits the pending message (if any) with its original WAL position.
// Used at STREAM STOP boundaries where the final commit LSN is not yet known.
func (b *messageBuffer) flush() {
	if b.pending != nil {
		b.outCh <- b.pending
		b.pending = nil
	}
}

// flushWithLSN emits the pending message (if any), rewriting its WAL position
// to the given transaction-end LSN. Used at COMMIT and STREAM COMMIT.
func (b *messageBuffer) flushWithLSN(lsn pq.LSN) {
	if b.pending != nil {
		b.outCh <- &Message{
			message:  b.pending.message,
			walStart: int64(lsn),
		}
		b.pending = nil
	}
}

// discard drops the pending message without emitting.
// Used at BEGIN (reset state) and STREAM ABORT (transaction rolled back).
func (b *messageBuffer) discard() {
	b.pending = nil
}

// buffer stores a new DML message, first flushing any previously pending one.
func (b *messageBuffer) buffer(msg *Message) {
	b.flush()
	b.pending = msg
}

func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")

	buf := &messageBuffer{outCh: s.messageCH}
	corrupted := s.sinkLoop(ctx, buf)

	s.sinkEnd <- struct{}{}
	if !s.closed.Load() {
		s.Close(ctx)
		if corrupted {
			logger.Error("corrupted connection")
		}
	}
}

// sinkLoop reads raw replication messages and dispatches them until the
// connection is closed or a fatal error occurs. It returns true when the
// connection is in a corrupted state and the caller should log an error.
func (s *stream) sinkLoop(ctx context.Context, buf *messageBuffer) (corrupted bool) {
	for {
		select {
		case <-ctx.Done():
			logger.Info("sink loop: context canceled")
			return false
		default:
		}

		msgCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(300*time.Millisecond))
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		cancel()

		if err != nil {
			if s.closed.Load() {
				logger.Info("stream stopped")
				return false
			}
			if pgconn.Timeout(err) {
				if s.LoadXLogPos() > 0 {
					if err = SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos())); err != nil {
						logger.Error("send stand by status update", "error", err)
						return true
					}
					logger.Debug("send stand by status update")
				}
				continue
			}
			logger.Error("receive message error", "error", err)
			return true
		}

		copyData, ok := s.extractCopyData(rawMsg)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case message.PrimaryKeepaliveMessageByteID:
			if err := s.handleKeepalive(ctx, copyData.Data[1:], buf); err != nil {
				return true
			}
		case message.XLogDataByteID:
			s.handleXLogData(copyData.Data[1:], buf)
		}
	}
}

// extractCopyData validates a raw backend message. It returns the CopyData
// payload and true on success, or (nil, false) for protocol-level errors and
// unexpected message types which are logged and skipped.
func (s *stream) extractCopyData(rawMsg pgproto3.BackendMessage) (*pgproto3.CopyData, bool) {
	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		res, _ := errMsg.MarshalJSON()
		logger.Error("receive postgres wal error: " + string(res))
		return nil, false
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		logger.Warn(fmt.Sprintf("received unexpected message: %T", rawMsg))
		return nil, false
	}

	return msg, true
}

// handleKeepalive processes a primary keepalive message, updating the WAL
// position and responding with a standby status update when requested.
// A non-nil return signals a corrupted connection.
func (s *stream) handleKeepalive(ctx context.Context, data []byte, buf *messageBuffer) error {
	pkm, err := format.NewPrimaryKeepaliveMessage(data)
	if err != nil {
		logger.Error("decode primary keepalive message", "error", err)
		return nil // non-fatal, skip
	}

	if pkm.ServerWALEnd > 0 {
		// vendored-patch: T0-1 - under ManualCommit, route the server's WAL end to the
		// embedder's KeepaliveFunc instead of fast-forwarding lastXLogPos ourselves.
		if s.config.ManualCommit {
			if s.config.KeepaliveFunc != nil {
				// vendored-patch: T0-3 - deliver the keepalive IN BAND through messageCH
				// instead of calling KeepaliveFunc inline on the sink goroutine.
				//
				// Why this matters: KeepaliveFunc ultimately drives AckManager.IdleAdvance,
				// whose soundness depends entirely on "nothing pending" meaning "nothing
				// has been Observe()'d that isn't yet accounted for". But Observe() only
				// happens inside process()'s call to listenerFunc, reached by draining
				// messageCH. Calling KeepaliveFunc directly here - on the sink goroutine -
				// races that queue: a decoded message can be sitting in messageCH (or still
				// held in buf's one-message look-ahead) when the keepalive fires, so
				// IdleAdvance sees an empty backlog and fast-forwards the watermark past
				// data that hasn't been Observe()'d yet. On a fresh start where WAL already
				// has a replay backlog, this happens on essentially the first keepalive and
				// silently drops the entire backlog.
				//
				// The fix: flush buf's look-ahead message first (the same flush() used at
				// STREAM STOP boundaries - it emits the pending message with its original,
				// not yet commit-rewritten, LSN, which is fine: it is at worst conservative,
				// never wrong, and it must not be silently discarded), then enqueue a
				// keepaliveMarker carrying pkm.ServerWALEnd onto s.messageCH itself, in the
				// same position in the queue any decoded message would occupy. Because
				// messageCH is single-consumer FIFO (process() is the only reader) and buf
				// is only ever written to by this same sink goroutine, every message that
				// was decoded before this keepalive was received is *already* enqueued
				// ahead of the marker by the time we push it. process() then calls
				// KeepaliveFunc only once every prior message has been through
				// listenerFunc/Observe, which is exactly the ordering IdleAdvance's
				// len(a.lsns)==0 guard assumes.
				buf.flush()
				marker := &Message{message: &keepaliveMarker{}, walStart: int64(pkm.ServerWALEnd)}
				select {
				case s.messageCH <- marker:
				default:
					// messageCH (cap 1000) is full. Do NOT block: this goroutine is also
					// the only reader of the replication socket, and a keepalive still
					// needs `pkm.ReplyRequested` handled below to keep the standby-status
					// heartbeat alive - blocking here risks a wal_receiver_timeout
					// disconnect, which is strictly worse than a delayed idle-advance.
					// Dropping the marker only means this particular idle-advance
					// opportunity is skipped; the *next* keepalive (they arrive on a
					// steady timer) retries it once the backlog has drained under
					// process()'s own pace. Silently losing an IdleAdvance is always
					// safe - the failure mode this patch closes is exactly the reverse
					// (advancing too eagerly), so erring toward "advance later" here is
					// deliberate, not an oversight.
					logger.Warn("keepalive marker dropped: messageCH full", "serverWALEnd", pkm.ServerWALEnd.String())
				}
			}
		} else {
			// vendored-patch: T0-2 - log rather than propagate: a failed keepalive-driven
			// advance is not fatal to the stream loop (the next keepalive retries), and
			// returning here would tear down replication on a transient write error.
			if err := s.UpdateXLogPos(ctx, pkm.ServerWALEnd); err != nil && !goerrors.Is(err, ErrStreamClosed) {
				logger.Warn("keepalive xlog position update failed", "error", err, "serverWALEnd", pkm.ServerWALEnd.String())
			}
			logger.Debug("updated xlog position from keepalive", "serverWALEnd", pkm.ServerWALEnd.String())
		}
	}

	// vendored-patch: T0-1 - guard reply on a confirmed position (mirrors the guard at
	// the receive-timeout branch above) so we never report LSN 0 to the primary.
	if pkm.ReplyRequested && s.LoadXLogPos() > 0 {
		if err = SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos())); err != nil {
			logger.Error("standby status update", "error", err)
			return err
		}
		logger.Debug("standby status update sent on keepalive request")
	}

	return nil
}

// handleXLogData parses a WAL data message, decodes the logical replication
// event, and dispatches it through the message buffer.
func (s *stream) handleXLogData(data []byte, buf *messageBuffer) {
	xld, err := ParseXLogData(data)
	if err != nil {
		logger.Error("parse xLog data", "error", err)
		return
	}

	s.metric.SetCDCLatency(time.Now().UTC().Sub(xld.ServerTime).Nanoseconds())

	s.mu.Lock()
	decodedMsg, err := message.New(xld.WALData, xld.ServerTime, s.relation)
	s.mu.Unlock()

	if err != nil || decodedMsg == nil {
		if err != nil {
			logger.Debug("wal data message parsing error", "error", err)
		}
		// vendored-patch: T0-1 - under ManualCommit, undecodable messages must not
		// advance the position; unobserved LSNs cannot stall the AckManager since the
		// next confirmed event advances past them.
		if !s.config.ManualCommit {
			// vendored-patch: T0-2 - handleXLogData has no context in scope. This path runs
			// only with ManualCommit off, so context.Background() deliberately preserves
			// upstream behavior: threading a bounded ctx here would *introduce* a write
			// deadline where upstream had none. Error is logged, not propagated (this
			// function returns nothing and an undecodable message is already non-fatal).
			if err := s.UpdateXLogPos(context.Background(), xld.WALStart); err != nil && !goerrors.Is(err, ErrStreamClosed) {
				logger.Warn("xlog position update failed for undecodable message", "error", err, "walStart", xld.WALStart.String())
			}
		}
		return
	}

	s.dispatchMessage(decodedMsg, xld, buf)
}

// dispatchMessage routes a decoded logical replication event to the correct
// buffer action.
//
// Transaction boundaries (BEGIN, COMMIT, STREAM COMMIT, STREAM STOP, STREAM ABORT)
// control the buffer lifecycle. DML events (INSERT, UPDATE, DELETE) are buffered
// with a one-message look-ahead so the last message in each transaction can have
// its WAL position rewritten to the transaction-end LSN.
func (s *stream) dispatchMessage(decodedMsg any, xld XLogData, buf *messageBuffer) {
	switch msg := decodedMsg.(type) {
	case *format.Begin:
		buf.discard()

	case *format.Commit:
		buf.flushWithLSN(msg.TransactionEndLSN)

	case *format.StreamStop:
		// End of a streaming chunk – flush so messages are not lost when
		// other transactions are interleaved between chunks.
		buf.flush()

	case *format.StreamCommit:
		// Final commit of a streamed transaction – rewrite LSN like Commit.
		buf.flushWithLSN(msg.TransactionEndLSN)

	case *format.StreamAbort:
		// Streamed transaction rolled back – discard buffered message.
		buf.discard()

	default:
		// DML event (Insert, Update, Delete, Relation, …)
		buf.buffer(&Message{
			message:  decodedMsg,
			walStart: int64(xld.WALStart),
		})
	}
}

func (s *stream) process(ctx context.Context) {
	logger.Info("postgres message process started")

	for {
		var msg *Message
		var ok bool
		select {
		case <-ctx.Done():
			logger.Info("message process: context canceled")
			return
		case msg, ok = <-s.messageCH:
			if !ok {
				return
			}
		}

		// vendored-patch: T0-3 - a keepaliveMarker carries a WAL-end position enqueued in
		// band by handleKeepalive; it must be invisible to the application. Invoke
		// KeepaliveFunc here, after every message ahead of it has already gone through
		// listenerFunc, and skip building a ListenerContext entirely.
		if _, ok := msg.message.(*keepaliveMarker); ok {
			if s.config.KeepaliveFunc != nil {
				s.config.KeepaliveFunc(pq.LSN(msg.walStart))
			}
			continue
		}

		lCtx := &ListenerContext{
			Message: msg.message,
			LSN:     pq.LSN(msg.walStart),
			Ack: func() error {
				// vendored-patch: T0-1 - under ManualCommit, position ownership moves
				// entirely to the embedder's explicit UpdateXLogPos calls; Ack becomes a
				// no-op that neither advances lastXLogPos nor talks to the primary.
				if s.config.ManualCommit {
					return nil
				}
				pos := pq.LSN(msg.walStart)
				// vendored-patch: T0-2 - log rather than propagate so the legacy Ack keeps
				// its upstream contract (the explicit send below is the value it returns).
				// Note the redundant double-send is pre-existing upstream behavior, retained
				// here deliberately: this branch is unreachable under ManualCommit.
				if err := s.UpdateXLogPos(ctx, pos); err != nil && !goerrors.Is(err, ErrStreamClosed) {
					logger.Warn("ack xlog position update failed", "error", err, "lsn", pos.String())
				}
				logger.Debug("send stand by status update", "xLogPos", s.LoadXLogPos().String())
				return SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos()))
			},
		}

		switch lCtx.Message.(type) {
		case *format.Insert:
			s.metric.InsertOpIncrement(1)
		case *format.Delete:
			s.metric.DeleteOpIncrement(1)
		case *format.Update:
			s.metric.UpdateOpIncrement(1)
		}

		start := time.Now().UTC()
		s.listenerFunc(lCtx)
		s.metric.SetProcessLatency(time.Since(start).Nanoseconds())
	}
}

func (s *stream) Close(ctx context.Context) {
	s.closed.Store(true)

	<-s.sinkEnd
	// vendored-patch: T1-5 - Use sync.Once to safely close sinkEnd without draining signals
	s.closeSinkEndOnce.Do(func() {
		close(s.sinkEnd)
	})
	logger.Info("postgres message sink stopped")

	if !s.conn.IsClosed() {
		_ = s.conn.Close(ctx)
		logger.Info("postgres connection closed")
	}
}

func (s *stream) GetSystemInfo() *pq.IdentifySystemResult {
	return s.system
}

func (s *stream) GetMetric() metric.Metric {
	return s.metric
}

func (s *stream) SetSnapshotLSN(lsn pq.LSN) {
	s.snapshotLSN = lsn
}

// vendored-patch: T0-2 - takes a context and returns an error. Under ManualCommit this is the
// ONLY path that advances the replication slot, so the caller must be able to bound the write
// and learn whether it succeeded; silently failing here would stall the slot invisibly.
// Returns ErrStreamClosed when there is no usable connection (normal during shutdown — callers
// should check with errors.Is rather than treating it as a hard failure).
func (s *stream) UpdateXLogPos(ctx context.Context, lsn pq.LSN) error {
	// vendored-patch: T0-1 - monotonic guard: the *stored/reported* position must never
	// regress, but the standby status update must still be sent every call (including when
	// lsn <= lastXLogPos) so PostgreSQL keeps seeing liveness from this replica on an idle
	// stream (wal_receiver_timeout). Clamp what we report, don't skip the send.
	s.mu.Lock()
	if lsn > s.lastXLogPos {
		s.lastXLogPos = lsn
	}
	pos := s.lastXLogPos
	s.mu.Unlock()

	if s.conn == nil || s.conn.IsClosed() {
		return ErrStreamClosed
	}

	// vendored-patch: T0-2 - bound the write WITHOUT a socket deadline.
	//
	// The obvious implementation — push ctx's deadline onto the underlying net.Conn via
	// SetWriteDeadline — is WRONG on this connection, for three reasons:
	//  1. pgconn installs a DeadlineContextWatcherHandler by default, and sinkLoop calls
	//     ReceiveMessage with a 300ms deadline on every iteration forever, deliberately
	//     letting it expire. Each expiry runs SetDeadline(now) then SetDeadline(zero),
	//     which clears any write deadline we set from another goroutine — so the bound is
	//     silently defeated in exactly the "write is blocked" case it was meant to cover.
	//  2. Symmetrically, clearing the deadline afterwards would stomp a deadline pgx set
	//     for its own in-flight cancellation.
	//  3. Worst: a write deadline firing mid-frame inside SendUnbufferedEncodedCopyData
	//     leaves a TRUNCATED CopyData frame on the wire. That path bypasses PgConn's
	//     locking and status machinery, so nothing marks the connection broken and the
	//     next update writes a fresh frame onto a corrupted stream.
	//
	// Instead we run the write on its own goroutine and let the CALLER stop waiting. The
	// write itself is not cancelled — it completes or fails whenever the socket drains —
	// but the caller gets a bounded wait, which is what it actually needs to avoid stalling
	// the ack coordinator. The semaphore guarantees at most one standby write is ever in
	// flight, so a slow write plus a retrying caller cannot interleave protocol frames.
	select {
	case s.standbySem <- struct{}{}:
	default:
		// A previous standby write is still blocked on the socket. Piling on would risk
		// frame interleaving; report it so the caller can retry on its next tick.
		return ErrStandbyWriteInFlight
	}

	done := make(chan error, 1) // buffered: the goroutine must never block on an abandoned caller
	go func() {
		defer func() { <-s.standbySem }()
		done <- SendStandbyStatusUpdate(context.Background(), s.conn, uint64(pos))
	}()

	select {
	case err := <-done:
		if err != nil {
			logger.Error("failed to send manual standby status update", "error", err, "lsn", pos.String())
		}
		return err
	case <-ctx.Done():
		// The write is still running; the semaphore is released when it finishes.
		return ctx.Err()
	}
}

func (s *stream) AddRelation(rel *format.Relation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.relation != nil {
		s.relation[rel.OID] = rel
	}
}

func (s *stream) LoadXLogPos() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastXLogPos
}

func (s *stream) OpenFromSnapshotLSN() {
	s.openFromSnapshotLSN = true
}

// fetchSnapshotLSN queries the database to get the snapshot LSN from cdc_snapshot_job table
// Uses infinite retry with exponential backoff for resilience against transient database errors
func (s *stream) fetchSnapshotLSN(ctx context.Context) (pq.LSN, error) {
	logger.Info("fetching snapshot LSN from database", "slotName", s.config.Slot.Name)

	var snapshotLSN pq.LSN

	err := retry.Do(
		func() error {
			// Create a separate connection for querying metadata
			// Use regular DSN (not replication DSN) for normal SQL queries
			conn, err := pq.NewConnection(ctx, s.config.DSN())
			if err != nil {
				return errors.Wrap(err, "create connection for snapshot LSN query")
			}
			defer conn.Close(ctx)

			query := fmt.Sprintf(`
				SELECT snapshot_lsn, completed 
				FROM cdc_snapshot_job 
				WHERE slot_name = '%s'
			`, s.config.Slot.Name)

			resultReader := conn.Exec(ctx, query)
			results, err := resultReader.ReadAll()
			if err != nil {
				resultReader.Close()
				return errors.Wrap(err, "execute snapshot LSN query")
			}

			if err = resultReader.Close(); err != nil {
				return errors.Wrap(err, "close result reader")
			}

			if len(results) == 0 || len(results[0].Rows) == 0 {
				return retry.Unrecoverable(errors.New("no snapshot job found for slot: " + s.config.Slot.Name))
			}

			row := results[0].Rows[0]

			completed := string(row[1]) == "true" || string(row[1]) == "t"
			if !completed {
				return errors.New("snapshot job not completed yet for slot: " + s.config.Slot.Name)
			}

			lsnStr := string(row[0])
			if lsnStr == "" {
				return retry.Unrecoverable(errors.New("empty snapshot LSN result"))
			}

			snapshotLSN, err = pq.ParseLSN(lsnStr)
			if err != nil {
				return retry.Unrecoverable(errors.Wrap(err, "parse snapshot LSN: "+lsnStr))
			}

			return nil
		},
		retry.Attempts(0),                   // 0 means infinite retries
		retry.DelayType(retry.BackOffDelay), // Exponential backoff
		retry.OnRetry(func(n uint, err error) {
			logger.Error("error in snapshot LSN fetch, retrying",
				"attempt", n+1,
				"error", err,
				"slotName", s.config.Slot.Name)
		}),
	)
	if err != nil {
		return 0, errors.Wrap(err, "failed to fetch snapshot LSN")
	}

	logger.Info("fetched snapshot LSN from database", "slotName", s.config.Slot.Name, "snapshotLSN", snapshotLSN.String())
	return snapshotLSN, nil
}

// NOTE (vendored-patch: T0-2): this function is deliberately left in its upstream form, with
// the context ignored. Bounding the write happens one level up in stream.UpdateXLogPos — see
// the long comment there for why a socket write deadline is NOT usable on this connection.
func SendStandbyStatusUpdate(_ context.Context, conn pq.Connection, walWritePosition uint64) error {
	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, timeToPgTime(time.Now()))
	data = append(data, 0)

	cd := &pgproto3.CopyData{Data: data}
	buf, err := cd.Encode(nil)
	if err != nil {
		return err
	}

	return conn.Frontend().SendUnbufferedEncodedCopyData(buf)
}

func AppendUint64(buf []byte, n uint64) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[wp:], n)
	return buf
}

func timeToPgTime(t time.Time) uint64 {
	return uint64(t.Unix()*1000000 + int64(t.Nanosecond())/1000 - microSecFromUnixEpochToY2K)
}
