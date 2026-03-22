package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
)

type PostgresSource struct {
	name      string
	connector cdc.Connector
	cancel    context.CancelFunc
}

func NewPostgresSource(name string) *PostgresSource {
	return &PostgresSource{name: name}
}

func (s *PostgresSource) Name() string {
	return s.name
}

func (s *PostgresSource) Start(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint) (<-chan []protocol.Message, chan<- struct{}, error) {
	out := make(chan []protocol.Message, 1)
	ack := make(chan struct{})

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Bound for batching, default to 100 if not specified
	batchSize := srcConfig.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	// Pre-allocated slice for zero-allocation batching
	msgs := make([]protocol.Message, 0, batchSize)

	// Determine snapshot mode: if we have an LSN, we resume from there and skip initial snapshot
	snapshotEnabled := checkpoint.IngressLSN == 0

	cfg := config.Config{
		Host:     srcConfig.Host,
		Port:     srcConfig.Port,
		Username: srcConfig.User,
		Password: srcConfig.PassEncrypted,
		Database: srcConfig.Database,
		Slot: slot.Config{
			Name:              srcConfig.SlotName,
			CreateIfNotExists: true,
		},
		Publication: publication.Config{
			Name:              srcConfig.PublicationName,
			CreateIfNotExists: true,
		},
		Snapshot: config.SnapshotConfig{
			Enabled: snapshotEnabled,
			Mode:    config.SnapshotModeInitial,
		},
	}

	handler := func(lc *replication.ListenerContext) {
		switch msg := lc.Message.(type) {
		case *format.Insert:
			payload, _ := json.Marshal(msg.Decoded)
			msgs = append(msgs, protocol.Message{
				SourceID:  srcConfig.ID,
				Table:     msg.TableName,
				Op:        "insert",
				Payload:   payload,
				Timestamp: msg.MessageTime,
			})
		case *format.Update:
			payload, _ := json.Marshal(msg.NewDecoded)
			msgs = append(msgs, protocol.Message{
				SourceID:  srcConfig.ID,
				Table:     msg.TableName,
				Op:        "update",
				Payload:   payload,
				Timestamp: msg.MessageTime,
			})
		case *format.Delete:
			payload, _ := json.Marshal(msg.OldDecoded)
			msgs = append(msgs, protocol.Message{
				SourceID:  srcConfig.ID,
				Table:     msg.TableName,
				Op:        "delete",
				Payload:   payload,
				Timestamp: msg.MessageTime,
			})
		case *format.Snapshot:
			if msg.EventType == format.SnapshotEventTypeData {
				payload, _ := json.Marshal(msg.Data)
				msgs = append(msgs, protocol.Message{
					SourceID:  srcConfig.ID,
					Table:     msg.Table,
					Op:        "snapshot",
					LSN:       uint64(msg.LSN),
					Payload:   payload,
					Timestamp: msg.ServerTime,
				})
			}
		}

		// Flush if we reached the batch limit OR if it's a Snapshot control event
		// or if we have at least one message and it's a "COMMIT" equivalent in the library.
		// For now, let's stick to the requested bound check logic.
		if len(msgs) >= batchSize {
			select {
			case out <- msgs:
				select {
				case <-ack:
					// IMPORTANT: Reset length while keeping underlying array for reuse
					msgs = msgs[:0]
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
		lc.Ack()
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
	}()

	return out, ack, nil
}

func (s *PostgresSource) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.connector != nil {
		s.connector.Close()
	}
	return nil
}
