package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
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

	batchSize := srcConfig.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	batchWait := srcConfig.BatchWait
	if batchWait <= 0 {
		batchWait = 5 * time.Second
	}

	var mu sync.Mutex
	msgs := make([]protocol.Message, 0, batchSize)

	flush := func() {
		mu.Lock()
		if len(msgs) == 0 {
			mu.Unlock()
			return
		}

		select {
		case out <- msgs:
			mu.Unlock()
			// Wait for producer to signal that it has published the batch
			select {
			case <-ack:
				mu.Lock()
				msgs = msgs[:0]
				mu.Unlock()
			case <-ctx.Done():
			}
		case <-ctx.Done():
			mu.Unlock()
		}
	}

	// Periodic flusher for low-traffic scenarios
	go func() {
		ticker := time.NewTicker(batchWait)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				flush()
			}
		}
	}()

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
		mu.Lock()

		var m protocol.Message
		switch msg := lc.Message.(type) {
		case *format.Insert:
			payload, _ := json.Marshal(msg.Decoded)
			m = protocol.Message{
				SourceID:  srcConfig.ID,
				Table:     msg.TableName,
				Op:        "insert",
				Payload:   payload,
				Timestamp: msg.MessageTime,
			}
		case *format.Update:
			payload, _ := json.Marshal(msg.NewDecoded)
			m = protocol.Message{
				SourceID:  srcConfig.ID,
				Table:     msg.TableName,
				Op:        "update",
				Payload:   payload,
				Timestamp: msg.MessageTime,
			}
		case *format.Delete:
			payload, _ := json.Marshal(msg.OldDecoded)
			m = protocol.Message{
				SourceID:  srcConfig.ID,
				Table:     msg.TableName,
				Op:        "delete",
				Payload:   payload,
				Timestamp: msg.MessageTime,
			}
		case *format.Snapshot:
			if msg.EventType == format.SnapshotEventTypeData {
				payload, _ := json.Marshal(msg.Data)
				m = protocol.Message{
					SourceID:  srcConfig.ID,
					Table:     msg.Table,
					Op:        "snapshot",
					LSN:       uint64(msg.LSN),
					Payload:   payload,
					Timestamp: msg.ServerTime,
				}
			}
		}

		if m.SourceID != "" {
			msgs = append(msgs, m)
		}

		if len(msgs) >= batchSize {
			mu.Unlock()
			flush()
		} else {
			mu.Unlock()
		}

		if err := lc.Ack(); err != nil {
			log.Printf("Warning: Failed to ack LSN: %v", err)
		}
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
