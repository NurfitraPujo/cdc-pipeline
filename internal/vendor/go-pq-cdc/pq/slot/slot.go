package slot

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrorSlotIsNotExists = errors.New("replication slot is not exists")
	ErrorSlotClosed      = errors.New("replication slot is closed")
)

type XLogUpdater interface {
	UpdateXLogPos(lsn pq.LSN)
}

type Slot struct {
	cfg             Config
	conn            *pgconn.PgConn
	replicationConn *pgconn.PgConn
	statusSQL       string
	ticker          *time.Ticker
	metrics         MetricsStore
	closed          atomic.Bool
	mu              sync.Mutex
	repDSN          string
	dsn             string
}

type MetricsStore interface {
	SetSlotActivity(active bool)
	SetSlotCurrentLSN(lsn float64)
	SetSlotConfirmedFlushLSN(lsn float64)
	SetSlotRetainedWALSize(size float64)
	SetSlotLag(lag float64)
}

func NewSlot(repDSN string, dsn string, cfg Config, metrics MetricsStore, updater XLogUpdater) *Slot {
	return &Slot{
		cfg:       cfg,
		repDSN:    repDSN,
		dsn:       dsn,
		statusSQL: fmt.Sprintf("SELECT * FROM pg_replication_slots WHERE slot_name = '%s'", cfg.Name),
		ticker:    time.NewTicker(time.Duration(cfg.SlotActivityCheckerInterval) * time.Millisecond),
		metrics:   metrics,
	}
}

func (s *Slot) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn != nil && !s.conn.IsClosed() {
		return nil
	}

	conn, err := pgconn.Connect(ctx, s.dsn)
	if err != nil {
		return errors.Wrap(err, "slot connect")
	}
	s.conn = conn

	repConn, err := pgconn.Connect(ctx, s.repDSN)
	if err != nil {
		_ = conn.Close(ctx)
		return errors.Wrap(err, "slot replication connect")
	}
	s.replicationConn = repConn

	return nil
}

func (s *Slot) Create(ctx context.Context) (*Info, error) {
	// Ensure connected
	if err := s.Connect(ctx); err != nil {
		return nil, err
	}

	info, err := s.Info(ctx)
	if err == nil {
		return info, nil
	}

	if err.Error() == ErrorSlotIsNotExists.Error() {
		if err := s.createSlotWithReplicationConn(ctx); err != nil {
			return nil, err
		}
		return s.Info(ctx)
	}

	return nil, err
}

func (s *Slot) Info(ctx context.Context) (*Info, error) {
	if s.closed.Load() {
		return nil, ErrorSlotClosed
	}

	return s.infoLocked(ctx)
}

func (s *Slot) infoLocked(ctx context.Context) (*Info, error) {
	if s.conn == nil || s.conn.IsClosed() {
		return nil, errors.New("connection closed")
	}

	resultReader := s.conn.Exec(ctx, s.statusSQL)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "replication slot info result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrorSlotIsNotExists
	}

	slotInfo, err := decodeSlotInfoResult(results[0])
	if err != nil {
		return nil, errors.Wrap(err, "replication slot info result decode")
	}

	if slotInfo.Type != Logical {
		return nil, errors.New(fmt.Sprintf("'%s' replication slot must be logical but it is %s", slotInfo.Name, slotInfo.Type))
	}

	return slotInfo, nil
}

func (s *Slot) Metrics(ctx context.Context) {
	if s.ticker == nil {
		return
	}
	defer s.ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.ticker.C:
			if s.closed.Load() {
				return
			}

			slotInfo, err := s.Info(ctx)
			if err != nil {
				if err.Error() == ErrorSlotClosed.Error() {
					return
				}
				// Only log if not cancelled
				if ctx.Err() == nil {
					logger.Error("slot metrics", "error", err)
				}
				continue
			}

			s.metrics.SetSlotActivity(slotInfo.Active)
			s.metrics.SetSlotCurrentLSN(float64(slotInfo.CurrentLSN))
			s.metrics.SetSlotConfirmedFlushLSN(float64(slotInfo.ConfirmedFlushLSN))
			s.metrics.SetSlotRetainedWALSize(float64(slotInfo.RetainedWALSize))
			s.metrics.SetSlotLag(float64(slotInfo.Lag))
		}
	}
}

func (s *Slot) Close(ctx context.Context) {
	s.closed.Store(true)
	if s.ticker != nil {
		s.ticker.Stop()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn != nil && !s.conn.IsClosed() {
		_ = s.conn.Close(ctx)
	}
	if s.replicationConn != nil && !s.replicationConn.IsClosed() {
		_ = s.replicationConn.Close(ctx)
	}
}

var typeMap = pgtype.NewMap()

func decodeSlotInfoResult(result *pgconn.Result) (*Info, error) {
	var slotInfo Info
	for i, fd := range result.FieldDescriptions {
		v, err := decodeTextColumnData(result.Rows[0][i], fd.DataTypeOID)
		if err != nil {
			return nil, err
		}

		if v == nil {
			continue
		}

		switch fd.Name {
		case "slot_name":
			slotInfo.Name = v.(string)
		case "slot_type":
			slotInfo.Type = Type(v.(string))
		case "active":
			slotInfo.Active = v.(bool)
		case "active_pid":
			slotInfo.ActivePID = v.(int32)
		case "restart_lsn":
			slotInfo.RestartLSN, _ = pq.ParseLSN(v.(string))
		case "confirmed_flush_lsn":
			slotInfo.ConfirmedFlushLSN, _ = pq.ParseLSN(v.(string))
		case "wal_status":
			slotInfo.WalStatus = v.(string)
		case "current_lsn":
			slotInfo.CurrentLSN, _ = pq.ParseLSN(v.(string))
		}
	}

	slotInfo.RetainedWALSize = slotInfo.CurrentLSN - slotInfo.RestartLSN
	slotInfo.Lag = slotInfo.CurrentLSN - slotInfo.ConfirmedFlushLSN

	return &slotInfo, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func (s *Slot) createSlotWithReplicationConn(ctx context.Context) error {
	if s.replicationConn == nil || s.replicationConn.IsClosed() {
		return errors.New("replication connection closed")
	}

	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput", s.cfg.Name)
	resultReader := s.replicationConn.Exec(ctx, sql)
	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "replication slot create result")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "replication slot create result reader close")
	}

	return nil
}
