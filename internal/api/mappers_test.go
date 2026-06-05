package api

import (
	"testing"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

func TestRoundTrip_LoginRequest(t *testing.T) {
	p := protocol.LoginRequest{Username: "admin", Password: "admin"}
	a := LoginRequestFromProtocol(p)
	back := LoginRequestToProtocol(a)
	if back.Username != p.Username || back.Password != p.Password {
		t.Fatalf("roundtrip mismatch: %+v -> %+v -> %+v", p, a, back)
	}
}

func TestRoundTrip_LoginResponse(t *testing.T) {
	p := protocol.LoginResponse{Token: "abc"}
	a := LoginResponseFromProtocol(p)
	back := LoginResponseToProtocol(a)
	if back.Token != a.Token || back.Token != p.Token {
		t.Fatalf("roundtrip mismatch: %+v -> %+v -> %+v", p, a, back)
	}
}

func TestRoundTrip_ErrorResponse(t *testing.T) {
	now := time.Now()
	p := protocol.ErrorResponse{Error: "x", StartedAt: now}
	a := ErrorResponseFromProtocol(p)
	if a.StartedAt == nil || !a.StartedAt.Equal(now) {
		t.Fatalf("started_at mismatch: %v vs %v", a.StartedAt, now)
	}
	back := ErrorResponseToProtocol(a)
	if back.Error != p.Error || !back.StartedAt.Equal(now) {
		t.Fatalf("roundtrip mismatch: %+v -> %+v -> %+v", p, a, back)
	}

	zero := protocol.ErrorResponse{Error: "y"}
	a2 := ErrorResponseFromProtocol(zero)
	if a2.StartedAt != nil {
		t.Fatalf("zero started_at should map to nil, got %v", a2.StartedAt)
	}
}

func TestRoundTrip_AcceptedResponse(t *testing.T) {
	p := protocol.AcceptedResponse{Status: "ok"}
	a := AcceptedResponseFromProtocol(p)
	back := AcceptedResponseToProtocol(a)
	if back.Status != "ok" {
		t.Fatalf("roundtrip mismatch")
	}
}

func TestRoundTrip_RetryConfig(t *testing.T) {
	p := protocol.RetryConfig{
		MaxRetries: 3, InitialInterval: time.Second, MaxInterval: 30 * time.Second, EnableDLQ: true,
	}
	a := RetryConfigFromProtocol(p)
	if a.MaxRetries == nil || *a.MaxRetries != 3 {
		t.Fatalf("max_retries: %v", a.MaxRetries)
	}
	if a.InitialInterval == nil || *a.InitialInterval != "1s" {
		t.Fatalf("initial_interval: %v", a.InitialInterval)
	}
	if a.MaxInterval == nil || *a.MaxInterval != "30s" {
		t.Fatalf("max_interval: %v", a.MaxInterval)
	}
	if a.EnableDlq == nil || !*a.EnableDlq {
		t.Fatalf("enable_dlq: %v", a.EnableDlq)
	}
	back := RetryConfigToProtocol(a)
	if back.MaxRetries != 3 || back.InitialInterval != time.Second || back.MaxInterval != 30*time.Second || !back.EnableDLQ {
		t.Fatalf("roundtrip mismatch: %+v", back)
	}
}

func TestRoundTrip_GlobalConfig(t *testing.T) {
	p := protocol.GlobalConfig{
		BatchSize: 100, BatchWait: 5 * time.Second, Retry: protocol.RetryConfig{MaxRetries: 1, InitialInterval: time.Second, MaxInterval: 10 * time.Second},
		DrainTimeout: 30 * time.Second, ShutdownTimeout: 30 * time.Second,
		StabilizationDelay: 2 * time.Second, CrashRecoveryDelay: 5 * time.Second, GlobalReloadDelay: 2 * time.Second,
	}
	a := GlobalConfigFromProtocol(p)
	if a.BatchWait != "5s" {
		t.Fatalf("batch_wait: %v", a.BatchWait)
	}
	if a.DrainTimeout == nil || *a.DrainTimeout != "30s" {
		t.Fatalf("drain_timeout: %v", a.DrainTimeout)
	}
	back := GlobalConfigToProtocol(a)
	if back.BatchSize != 100 || back.BatchWait != 5*time.Second || back.DrainTimeout != 30*time.Second {
		t.Fatalf("roundtrip mismatch: %+v", back)
	}
}

func TestRoundTrip_ProcessorConfig(t *testing.T) {
	p := protocol.ProcessorConfig{
		Name: "p1", Type: "mask",
		Options: map[string]interface{}{"k": "v"},
		OperationTypes: []protocol.OperationType{protocol.OperationType("insert"), protocol.OperationType("update")},
	}
	a := ProcessorConfigFromProtocol(p)
	if a.Name != "p1" || a.Type != "mask" {
		t.Fatalf("name/type mismatch")
	}
	if a.Options == nil || (*a.Options)["k"] != "v" {
		t.Fatalf("options mismatch: %v", a.Options)
	}
	if a.OperationTypes == nil || len(*a.OperationTypes) != 2 {
		t.Fatalf("operation_types mismatch")
	}
	back := ProcessorConfigToProtocol(a)
	if back.Name != "p1" || back.Type != "mask" || len(back.OperationTypes) != 2 {
		t.Fatalf("roundtrip mismatch: %+v", back)
	}
}

func TestRoundTrip_PipelineConfig(t *testing.T) {
	p := protocol.PipelineConfig{
		ID: "p1", Name: "Pipe 1",
		Sources: []string{"s1"}, Sinks: []string{"snk1"}, Tables: []string{"t1"},
		BatchSize: 50, BatchWait: 10 * time.Second,
		Processors: []protocol.ProcessorConfig{{Name: "p", Type: "mask"}},
	}
	a := PipelineConfigFromProtocol(p)
	if a.Id != "p1" || a.Name != "Pipe 1" {
		t.Fatalf("id/name mismatch")
	}
	if a.BatchSize == nil || *a.BatchSize != 50 {
		t.Fatalf("batch_size: %v", a.BatchSize)
	}
	if a.BatchWait == nil || *a.BatchWait != "10s" {
		t.Fatalf("batch_wait: %v", a.BatchWait)
	}
	if a.Processors == nil || len(*a.Processors) != 1 {
		t.Fatalf("processors mismatch")
	}
	back := PipelineConfigToProtocol(a)
	if back.ID != "p1" || back.BatchSize != 50 || back.BatchWait != 10*time.Second {
		t.Fatalf("roundtrip mismatch: %+v", back)
	}
}

func TestRoundTrip_SourceConfig(t *testing.T) {
	p := protocol.SourceConfig{
		ID: "s1", Type: "postgres", Host: "h", Port: 5432,
		User: "u", PassEncrypted: "p", Database: "d",
		SlotName: "slot", PublicationName: "pub",
		BatchSize: 10, BatchWait: 5 * time.Second,
		DiscoveryInterval: 30 * time.Second, SnapshotChunkSize: 100, SnapshotInterval: time.Second,
		Schemas: []string{"public"}, Tables: []string{"t"},
	}
	a := SourceConfigFromProtocol(p)
	if a.Id != "s1" || string(a.Type) != "postgres" || a.Port != 5432 {
		t.Fatalf("id/type/port mismatch")
	}
	if a.Pass == nil || *a.Pass != "p" {
		t.Fatalf("pass: %v", a.Pass)
	}
	if a.Schemas == nil || len(*a.Schemas) != 1 {
		t.Fatalf("schemas: %v", a.Schemas)
	}
	back := SourceConfigToProtocol(a)
	if back.ID != "s1" || back.PassEncrypted != "p" || back.BatchSize != 10 {
		t.Fatalf("roundtrip mismatch: %+v", back)
	}
}

func TestRoundTrip_SinkConfig(t *testing.T) {
	p := protocol.SinkConfig{
		ID: "snk1", Type: "databend", DSN: "dsn", MaxAckPending: 100,
		Options: map[string]interface{}{"a": 1.0},
	}
	a := SinkConfigFromProtocol(p)
	if a.Id != "snk1" || string(a.Type) != "databend" || a.Dsn != "dsn" {
		t.Fatalf("id/type/dsn mismatch")
	}
	if a.MaxAckPending == nil || *a.MaxAckPending != 100 {
		t.Fatalf("max_ack_pending: %v", a.MaxAckPending)
	}
	back := SinkConfigToProtocol(a)
	if back.ID != "snk1" || back.DSN != "dsn" || back.MaxAckPending != 100 {
		t.Fatalf("roundtrip mismatch: %+v", back)
	}
}

func TestRoundTrip_TableStats(t *testing.T) {
	now := time.Now()
	p := protocol.TableStats{
		Status: "ACTIVE", RPS: 1.5, TotalSynced: 100, ErrorCount: 0,
		LastSourceTS: now, LastProcessedTS: now, LagMS: 50, UpdatedAt: now,
	}
	a := TableStatsFromProtocol(p)
	if a.Status == nil || *a.Status != "ACTIVE" {
		t.Fatalf("status: %v", a.Status)
	}
	if a.Rps == nil || *a.Rps != 1.5 {
		t.Fatalf("rps: %v", a.Rps)
	}
	if a.TotalSynced == nil || *a.TotalSynced != 100 {
		t.Fatalf("total_synced: %v", a.TotalSynced)
	}
	back := TableStatsToProtocol(a)
	if back.Status != "ACTIVE" || back.RPS != 1.5 || back.TotalSynced != 100 || back.LagMS != 50 {
		t.Fatalf("roundtrip mismatch: %+v", back)
	}
}

func TestRoundTrip_TableMetadata(t *testing.T) {
	p := protocol.TableMetadata{
		ID: "t1", Name: "users",
		Columns: []string{"id", "name"},
		Types: []string{"int", "text"},
		PKColumns: []string{"id"},
	}
	a := TableMetadataFromProtocol(p)
	if a.Id == nil || *a.Id != "t1" {
		t.Fatalf("id: %v", a.Id)
	}
	back := TableMetadataToProtocol(a)
	if back.ID != "t1" || back.Name != "users" || len(back.PKColumns) != 1 {
		t.Fatalf("roundtrip mismatch: %+v", back)
	}
}

func TestRoundTrip_PipelineListResponse(t *testing.T) {
	p := protocol.PipelineListResponse{
		Pipelines: []protocol.PipelineConfig{{ID: "p1", Name: "P", Sources: []string{"s"}, Sinks: []string{"snk"}, Tables: []string{"t"}}},
		Total: 1, Page: 1, Limit: 10,
	}
	a := PipelineListResponseFromProtocol(p)
	if len(a.Pipelines) != 1 || a.Total != 1 || a.Page != 1 || a.Limit != 10 {
		t.Fatalf("list mismatch: %+v", a)
	}
}

func TestRoundTrip_SourceListResponse(t *testing.T) {
	p := protocol.SourceListResponse{Sources: []protocol.SourceConfig{{ID: "s1", Type: "postgres", Host: "h", Port: 5432, Database: "d"}}}
	a := SourceListResponseFromProtocol(p)
	if len(a.Sources) != 1 || a.Sources[0].Id != "s1" {
		t.Fatalf("list mismatch")
	}
}

func TestRoundTrip_SinkListResponse(t *testing.T) {
	p := protocol.SinkListResponse{Sinks: []protocol.SinkConfig{{ID: "snk1", Type: "databend", DSN: "d"}}}
	a := SinkListResponseFromProtocol(p)
	if len(a.Sinks) != 1 || a.Sinks[0].Id != "snk1" {
		t.Fatalf("list mismatch")
	}
}

func TestRoundTrip_PipelineStatusResponse(t *testing.T) {
	now := time.Now()
	p := protocol.PipelineStatusResponse{
		PipelineID: "p1",
		Status:     map[string]any{"k": "v"},
		Tables:     map[string]protocol.TableStats{"t1": {Status: "ACTIVE", TotalSynced: 1, UpdatedAt: now}},
		Sinks:      map[string]map[string]protocol.TableStats{"snk1": {"t1": {Status: "ACTIVE", TotalSynced: 1, UpdatedAt: now}}},
	}
	a := PipelineStatusResponseFromProtocol(p)
	if a.PipelineId != "p1" {
		t.Fatalf("pipeline_id: %v", a.PipelineId)
	}
	if len(a.Tables) != 1 || a.Tables["t1"].Status == nil {
		t.Fatalf("tables mismatch")
	}
	if len(a.Sinks) != 1 || len(a.Sinks["snk1"]) != 1 {
		t.Fatalf("sinks mismatch")
	}
}

func TestRoundTrip_SourceSchemaResponse(t *testing.T) {
	p := protocol.SourceSchemaResponse{SourceID: "s1", AvailableSchemas: []string{"public"}, DiscoveryStatus: "ready"}
	a := SourceSchemaResponseFromProtocol(p)
	if a.SourceId != "s1" || len(a.AvailableSchemas) != 1 || a.DiscoveryStatus != "ready" {
		t.Fatalf("schema mismatch: %+v", a)
	}
}

func TestRoundTrip_SourceTablesResponse(t *testing.T) {
	p := protocol.SourceTablesResponse{SourceID: "s1", Tables: []protocol.TableMetadata{{ID: "t1"}}}
	a := SourceTablesResponseFromProtocol(p)
	if a.SourceId != "s1" || len(a.Tables) != 1 {
		t.Fatalf("tables mismatch: %+v", a)
	}
}
