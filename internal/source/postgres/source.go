package postgres

import (
	"context"
	"database/sql"
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
	_ "github.com/jackc/pgx/v5/stdlib"
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
	discoveryInterval := srcConfig.DiscoveryInterval
	if discoveryInterval <= 0 {
		discoveryInterval = 30 * time.Second
	}

	var mu sync.Mutex
	msgs := make([]protocol.Message, 0, batchSize)
	knownTables := make(map[string]bool) // internal cache

	flush := func() {
		mu.Lock()
		if len(msgs) == 0 {
			mu.Unlock()
			return
		}

		select {
		case out <- msgs:
			mu.Unlock()
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

	// Periodic flusher
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

	// Periodic Discovery Poller
	go func() {
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
			srcConfig.User, srcConfig.PassEncrypted, srcConfig.Host, srcConfig.Port, srcConfig.Database)
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			log.Printf("Poller: Failed to open DB connection: %v", err)
			return
		}
		defer db.Close()

		ticker := time.NewTicker(discoveryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Query for tables in allowed schemas
				for _, schema := range srcConfig.Schemas {
					rows, err := db.QueryContext(ctx, 
						"SELECT table_name FROM information_schema.tables WHERE table_schema = $1", schema)
					if err != nil {
						log.Printf("Poller: Error querying tables for schema %s: %v", schema, err)
						continue
					}
					for rows.Next() {
						var tableName string
						if err := rows.Scan(&tableName); err == nil {
							fullKey := schema + "." + tableName
							mu.Lock()
							if !knownTables[fullKey] {
								log.Printf("Poller: Discovered new table %s", fullKey)
								// Emitting a schema_change event for discovery
								m := protocol.Message{
									SourceID:  srcConfig.ID,
									Table:     tableName,
									Op:        "schema_change",
									Timestamp: time.Now(),
									Schema: &protocol.SchemaMetadata{
										Table:  tableName,
										Schema: schema,
									},
								}
								if len(msgs) > 0 {
									mu.Unlock()
									flush()
									mu.Lock()
								}
								msgs = append(msgs, m)
								mu.Unlock()
								flush()
								mu.Lock()
								knownTables[fullKey] = true
							}
							mu.Unlock()
						}
					}
					rows.Close()
				}
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
		case *format.Relation:
			isAllowedSchema := false
			for _, s := range srcConfig.Schemas {
				if s == msg.Namespace {
					isAllowedSchema = true
					break
				}
			}
			if !isAllowedSchema && len(srcConfig.Schemas) > 0 {
				mu.Unlock()
				lc.Ack()
				return
			}

			fullKey := msg.Namespace + "." + msg.Name
			knownTables[fullKey] = true

			cols := make(map[string]string)
			for _, col := range msg.Columns {
				cols[col.Name] = fmt.Sprintf("%d", col.DataType)
			}

			m = protocol.Message{
				SourceID:  srcConfig.ID,
				Table:     msg.Name,
				Op:        "schema_change",
				Timestamp: time.Now(),
				Schema: &protocol.SchemaMetadata{
					Table:   msg.Name,
					Schema:  msg.Namespace,
					Columns: cols,
				},
			}

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
			if m.Op == "schema_change" {
				if len(msgs) > 0 {
					mu.Unlock()
					flush()
					mu.Lock()
				}
				msgs = append(msgs, m)
				mu.Unlock()
				flush()
			} else {
				msgs = append(msgs, m)
				if len(msgs) >= batchSize {
					mu.Unlock()
					flush()
				} else {
					mu.Unlock()
				}
			}
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
