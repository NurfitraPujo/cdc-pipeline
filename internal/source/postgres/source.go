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

func (s *PostgresSource) Start(ctx context.Context, srcConfig protocol.SourceConfig, checkpoint protocol.Checkpoint) (<-chan []protocol.Message, error) {
	out := make(chan []protocol.Message, 100)

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	cfg := config.Config{
		Host:     srcConfig.Host,
		Port:     srcConfig.Port,
		Username: srcConfig.User,
		Password: srcConfig.PassEncrypted,
		Database: "postgres", // Should be part of srcConfig
		Slot: slot.Config{
			Name:              "daya_cdc_slot_" + srcConfig.ID,
			CreateIfNotExists: true,
		},
		Publication: publication.Config{
			Name:              "daya_cdc_pub_" + srcConfig.ID,
			CreateIfNotExists: true,
		},
		Snapshot: config.SnapshotConfig{
			Enabled: true,
			Mode:    config.SnapshotModeInitial,
		},
	}

	handler := func(lc *replication.ListenerContext) {
		var msgs []protocol.Message

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
			// For now just take NewDecoded
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

		if len(msgs) > 0 {
			select {
			case out <- msgs:
			case <-ctx.Done():
				return
			}
		}
		lc.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create connector: %w", err)
	}
	s.connector = connector

	go func() {
		defer close(out)
		s.connector.Start(ctx)
	}()

	return out, nil
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
