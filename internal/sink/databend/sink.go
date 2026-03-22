package databend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/datafuselabs/databend-go"
	"bitbucket.com/daya-engineering/daya-data-pipeline/internal/protocol"
	"golang.org/x/sync/errgroup"
)

type DatabendSink struct {
	name string
	db   *sql.DB

	pkMu    sync.RWMutex
	pkCache map[string][]string // table -> pk columns
}

func NewDatabendSink(name string, dsn string) (*DatabendSink, error) {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)
	
	return &DatabendSink{
		name:    name, 
		db:      db,
		pkCache: make(map[string][]string),
	}, nil
}

func (s *DatabendSink) Name() string {
	return s.name
}

func (s *DatabendSink) BatchUpload(ctx context.Context, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	upserts := make(map[string][]protocol.Message)
	deletes := make(map[string][]protocol.Message)

	for _, m := range messages {
		if m.Op == "schema_change" || m.Op == "drain_marker" {
			continue
		}
		if m.Op == "delete" {
			deletes[m.Table] = append(deletes[m.Table], m)
		} else {
			upserts[m.Table] = append(upserts[m.Table], m)
		}
	}

	g, gCtx := errgroup.WithContext(ctx)

	for table, msgs := range upserts {
		t, m := table, msgs
		g.Go(func() error {
			return s.uploadTableBatch(gCtx, t, m)
		})
	}

	for table, msgs := range deletes {
		t, m := table, msgs
		g.Go(func() error {
			return s.deleteTableBatch(gCtx, t, m)
		})
	}

	return g.Wait()
}

func (s *DatabendSink) ApplySchema(ctx context.Context, schema protocol.SchemaMetadata) error {
	log.Printf("Syncing schema for table %s in Databend", schema.Table)

	s.pkMu.Lock()
	s.pkCache[schema.Table] = schema.PKColumns
	s.pkMu.Unlock()

	existingCols, err := s.getCurrentColumns(ctx, schema.Table)
	if err != nil {
		return fmt.Errorf("failed to check existing columns: %w", err)
	}

	if len(existingCols) == 0 {
		var colDefs []string
		for name, pgType := range schema.Columns {
			dbType := mapPgTypeToDatabend(pgType)
			colDefs = append(colDefs, fmt.Sprintf("\"%s\" %s", name, dbType))
		}
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (%s)", 
			schema.Table, strings.Join(colDefs, ", "))
		
		log.Printf("Executing DDL: %s", query)
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		return nil
	}

	for name, pgType := range schema.Columns {
		if !existingCols[strings.ToLower(name)] {
			dbType := mapPgTypeToDatabend(pgType)
			query := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN \"%s\" %s", 
				schema.Table, name, dbType)
			
			log.Printf("Executing Evolution DDL: %s", query)
			if _, err := s.db.ExecContext(ctx, query); err != nil {
				log.Printf("Warning: Failed to add column %s to %s: %v", name, schema.Table, err)
			}
		}
	}

	return nil
}

func (s *DatabendSink) getCurrentColumns(ctx context.Context, table string) (map[string]bool, error) {
	query := "SELECT column_name FROM information_schema.columns WHERE table_name = ?"
	rows, err := s.db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			cols[strings.ToLower(name)] = true
		}
	}
	return cols, nil
}

func mapPgTypeToDatabend(pgType string) string {
	t := strings.ToLower(pgType)
	switch {
	case strings.Contains(t, "bool"): return "BOOLEAN"
	case strings.Contains(t, "int"): return "INT64"
	case strings.Contains(t, "float") || strings.Contains(t, "numeric") || strings.Contains(t, "decimal"): return "FLOAT64"
	case strings.Contains(t, "timestamp"): return "TIMESTAMP"
	case strings.Contains(t, "date"): return "DATE"
	case strings.Contains(t, "json") || strings.Contains(t, "variant"): return "VARIANT"
	case strings.Contains(t, "bytea") || strings.Contains(t, "blob"): return "BINARY"
	case strings.Contains(t, "uuid") || strings.Contains(t, "text") || strings.Contains(t, "varchar") || strings.Contains(t, "char"): return "STRING"
	default:
		switch pgType {
		case "16": return "BOOLEAN"
		case "23", "20": return "INT64"
		case "1043", "25": return "STRING"
		case "1114", "1184": return "TIMESTAMP"
		case "3802": return "VARIANT"
		default: return "STRING"
		}
	}
}

func (s *DatabendSink) uploadTableBatch(ctx context.Context, table string, messages []protocol.Message) error {
	if len(messages) == 0 { return nil }

	var firstData map[string]any
	if err := json.Unmarshal(messages[0].Payload, &firstData); err != nil {
		return fmt.Errorf("failed to unmarshal first message: %w", err)
	}

	columns := make([]string, 0, len(firstData))
	for k := range firstData { columns = append(columns, k) }

	s.pkMu.RLock()
	pks := s.pkCache[table]
	s.pkMu.RUnlock()
	if len(pks) == 0 { pks = []string{"id"} }

	quotedColumns := make([]string, len(columns))
	for i, col := range columns { quotedColumns[i] = "\"" + col + "\"" }
	colList := strings.Join(quotedColumns, ", ")
	
	quotedPks := make([]string, len(pks))
	for i, pk := range pks { quotedPks[i] = "\"" + pk + "\"" }
	pkList := strings.Join(quotedPks, ", ")
	
	query := fmt.Sprintf("REPLACE INTO \"%s\" (%s) ON (%s) VALUES ", table, colList, pkList)
	
	valueStrings := make([]string, 0, len(messages))
	valueArgs := make([]any, 0, len(messages)*len(columns))

	for _, m := range messages {
		var data map[string]any
		json.Unmarshal(m.Payload, &data)

		placeholders := make([]string, len(columns))
		for j, col := range columns {
			placeholders[j] = "?"
			val := data[col]
			if val != nil {
				switch v := val.(type) {
				case string, int, int64, float64, bool, time.Time:
				default:
					b, _ := json.Marshal(v)
					val = string(b)
				}
			}
			valueArgs = append(valueArgs, val)
		}
		valueStrings = append(valueStrings, "("+strings.Join(placeholders, ", ")+")")
	}

	query += strings.Join(valueStrings, ", ")

	// #nosec G201
	_, err := s.db.ExecContext(ctx, query, valueArgs...)
	return err
}

func (s *DatabendSink) deleteTableBatch(ctx context.Context, table string, messages []protocol.Message) error {
	if len(messages) == 0 { return nil }

	s.pkMu.RLock()
	pks := s.pkCache[table]
	s.pkMu.RUnlock()
	if len(pks) == 0 { pks = []string{"id"} }

	for _, m := range messages {
		var data map[string]any
		json.Unmarshal(m.Payload, &data)

		var whereClauses []string
		var args []any
		for _, pk := range pks {
			whereClauses = append(whereClauses, fmt.Sprintf("\"%s\" = ?", pk))
			args = append(args, data[pk])
		}

		query := fmt.Sprintf("DELETE FROM \"%s\" WHERE %s", table, strings.Join(whereClauses, " AND "))
		// #nosec G201
		_, err := s.db.ExecContext(ctx, query, args...)
		if err != nil { return err }
	}
	return nil
}

func (s *DatabendSink) Stop() error {
	if s.db != nil { return s.db.Close() }
	return nil
}
