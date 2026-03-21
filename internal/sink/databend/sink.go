package databend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/datafuselabs/databend-go"
	"github.com/fitrapujo/daya-data-pipeline/internal/protocol"
)

type DatabendSink struct {
	name string
	db   *sql.DB
}

func NewDatabendSink(name string, dsn string) (*DatabendSink, error) {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		return nil, err
	}
	return &DatabendSink{name: name, db: db}, nil
}

func (s *DatabendSink) Name() string {
	return s.name
}

func (s *DatabendSink) BatchUpload(ctx context.Context, messages []protocol.Message) error {
	if len(messages) == 0 {
		return nil
	}

	// Assuming all messages in a batch belong to the same table for now
	// If not, we should group them.
	table := messages[0].Table

	// 1. Prepare SQL Insert
	// Databend supports INSERT INTO table (cols) VALUES (...), (...);
	// We need to parse the payload to extract column values.
	// This is highly dependent on how we want to handle schema evolution.
	
	// Example simplified logic:
	for _, m := range messages {
		var data map[string]any
		if err := json.Unmarshal(m.Payload, &data); err != nil {
			return fmt.Errorf("failed to unmarshal payload: %w", err)
		}
		
		// 2. Perform Idempotent Insert
		// Databend supports REPLACE INTO or unique constraints
		// For now, let's assume simple INSERT and handle dedup elsewhere
		
		// TODO: Implement actual batched SQL execution
		_ = table
		_ = data
	}

	return nil
}

func (s *DatabendSink) Stop() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
