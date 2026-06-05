package api

import (
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
)

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }
func i64Ptr(i int64) *int64   { return &i }
func f64Ptr(f float64) *float64 { return &f }
func tsPtr(t time.Time) *time.Time { return &t }
func strSlicePtr(s []string) *[]string { return &s }
func mapPtr(m map[string]interface{}) *map[string]interface{} { return &m }

func timePtrOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func durationToStringPtr(d time.Duration) *string {
	if d == 0 {
		return nil
	}
	return strPtr(d.String())
}

// --- LoginRequest ---

func LoginRequestFromProtocol(p protocol.LoginRequest) LoginRequest {
	return LoginRequest{Username: p.Username, Password: p.Password}
}

func LoginRequestToProtocol(a LoginRequest) protocol.LoginRequest {
	return protocol.LoginRequest{Username: a.Username, Password: a.Password}
}

// --- LoginResponse ---

func LoginResponseFromProtocol(p protocol.LoginResponse) LoginResponse {
	return LoginResponse{Token: p.Token}
}

func LoginResponseToProtocol(a LoginResponse) protocol.LoginResponse {
	return protocol.LoginResponse{Token: a.Token}
}

// --- ErrorResponse ---

func ErrorResponseFromProtocol(p protocol.ErrorResponse) ErrorResponse {
	return ErrorResponse{Error: p.Error, StartedAt: timePtrOrNil(p.StartedAt)}
}

func ErrorResponseToProtocol(a ErrorResponse) protocol.ErrorResponse {
	var started time.Time
	if a.StartedAt != nil {
		started = *a.StartedAt
	}
	return protocol.ErrorResponse{Error: a.Error, StartedAt: started}
}

// --- AcceptedResponse ---

func AcceptedResponseFromProtocol(p protocol.AcceptedResponse) AcceptedResponse {
	return AcceptedResponse{Status: p.Status}
}

func AcceptedResponseToProtocol(a AcceptedResponse) protocol.AcceptedResponse {
	return protocol.AcceptedResponse{Status: a.Status}
}

// --- RetryConfig ---

func RetryConfigFromProtocol(p protocol.RetryConfig) RetryConfig {
	return RetryConfig{
		MaxRetries:      intPtrOrNil(p.MaxRetries),
		InitialInterval: durationToStringPtr(p.InitialInterval),
		MaxInterval:     durationToStringPtr(p.MaxInterval),
		EnableDlq:       boolPtrOrNil(p.EnableDLQ),
	}
}

func RetryConfigToProtocol(a RetryConfig) protocol.RetryConfig {
	return protocol.RetryConfig{
		MaxRetries:      intValueOrZero(a.MaxRetries),
		InitialInterval: durationValueOrZero(a.InitialInterval),
		MaxInterval:     durationValueOrZero(a.MaxInterval),
		EnableDLQ:       boolValueOrFalse(a.EnableDlq),
	}
}

func intPtrOrNil(v int) *int {
	if v == 0 {
		return nil
	}
	return intPtr(v)
}

func boolPtrOrNil(v bool) *bool {
	if !v {
		return nil
	}
	b := v
	return &b
}

func boolValueOrFalse(p *bool) bool {
	if p == nil {
		return false
	}
	return *p
}

func intValueOrZero(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

func durationValueOrZero(p *string) time.Duration {
	if p == nil {
		return 0
	}
	d, err := time.ParseDuration(*p)
	if err != nil {
		return 0
	}
	return d
}

// --- GlobalConfig ---

func GlobalConfigFromProtocol(p protocol.GlobalConfig) GlobalConfig {
	return GlobalConfig{
		BatchSize:          p.BatchSize,
		BatchWait:          p.BatchWait.String(),
		Retry:              RetryConfigFromProtocol(p.Retry),
		DrainTimeout:       durationToStringPtr(p.DrainTimeout),
		ShutdownTimeout:    durationToStringPtr(p.ShutdownTimeout),
		StabilizationDelay: durationToStringPtr(p.StabilizationDelay),
		CrashRecoveryDelay: durationToStringPtr(p.CrashRecoveryDelay),
		GlobalReloadDelay:  durationToStringPtr(p.GlobalReloadDelay),
	}
}

func GlobalConfigToProtocol(a GlobalConfig) protocol.GlobalConfig {
	return protocol.GlobalConfig{
		BatchSize:          a.BatchSize,
		BatchWait:          durationValueOrZero(&a.BatchWait),
		Retry:              RetryConfigToProtocol(a.Retry),
		DrainTimeout:       durationValueOrZero(a.DrainTimeout),
		ShutdownTimeout:    durationValueOrZero(a.ShutdownTimeout),
		StabilizationDelay: durationValueOrZero(a.StabilizationDelay),
		CrashRecoveryDelay: durationValueOrZero(a.CrashRecoveryDelay),
		GlobalReloadDelay:  durationValueOrZero(a.GlobalReloadDelay),
	}
}

// --- PipelineConfig ---

func PipelineConfigFromProtocol(p protocol.PipelineConfig) PipelineConfig {
	out := PipelineConfig{
		Id:        p.ID,
		Name:      p.Name,
		Sources:   append([]string(nil), p.Sources...),
		Sinks:     append([]string(nil), p.Sinks...),
		BatchSize: intPtrOrNil(p.BatchSize),
		BatchWait: durationToStringPtr(p.BatchWait),
	}
	if len(p.Processors) > 0 {
		ps := make([]ProcessorConfig, len(p.Processors))
		for i, pp := range p.Processors {
			ps[i] = ProcessorConfigFromProtocol(pp)
		}
		out.Processors = &ps
	}
	if p.Tables != nil {
		out.Tables = append([]string(nil), p.Tables...)
	}
	if p.Retry != nil {
		r := RetryConfigFromProtocol(*p.Retry)
		out.Retry = &r
	}
	return out
}

func PipelineConfigToProtocol(a PipelineConfig) protocol.PipelineConfig {
	out := protocol.PipelineConfig{
		ID:       a.Id,
		Name:     a.Name,
		Sources:  append([]string(nil), a.Sources...),
		Sinks:    append([]string(nil), a.Sinks...),
		Tables:   append([]string(nil), a.Tables...),
		BatchSize: intValueOrZero(a.BatchSize),
		BatchWait: durationValueOrZero(a.BatchWait),
	}
	if a.Processors != nil {
		ps := make([]protocol.ProcessorConfig, len(*a.Processors))
		for i, ap := range *a.Processors {
			ps[i] = ProcessorConfigToProtocol(ap)
		}
		out.Processors = ps
	}
	if a.Retry != nil {
		rc := RetryConfigToProtocol(*a.Retry)
		out.Retry = &rc
	}
	return out
}

// --- ProcessorConfig ---

func ProcessorConfigFromProtocol(p protocol.ProcessorConfig) ProcessorConfig {
	out := ProcessorConfig{Name: p.Name, Type: p.Type}
	if p.OperationTypes != nil {
		ops := make([]string, len(p.OperationTypes))
		for i, op := range p.OperationTypes {
			ops[i] = string(op)
		}
		out.OperationTypes = &ops
	}
	if p.Options != nil {
		out.Options = mapPtr(p.Options)
	}
	return out
}

func ProcessorConfigToProtocol(a ProcessorConfig) protocol.ProcessorConfig {
	out := protocol.ProcessorConfig{Name: a.Name, Type: a.Type}
	if a.Options != nil {
		out.Options = *a.Options
	}
	if a.OperationTypes != nil {
		ops := make([]protocol.OperationType, len(*a.OperationTypes))
		for i, op := range *a.OperationTypes {
			ops[i] = protocol.OperationType(op)
		}
		out.OperationTypes = ops
	}
	return out
}

// --- SourceConfig ---

func SourceConfigFromProtocol(p protocol.SourceConfig) SourceConfig {
	return SourceConfig{
		Id:                p.ID,
		Type:              SourceConfigType(p.Type),
		Host:              p.Host,
		Port:              p.Port,
		User:              strPtrOrNil(p.User),
		Pass:              strPtrOrNil(p.PassEncrypted),
		Database:          p.Database,
		SlotName:          strPtrOrNil(p.SlotName),
		PublicationName:   strPtrOrNil(p.PublicationName),
		BatchSize:         intPtrOrNil(p.BatchSize),
		BatchWait:         durationToStringPtr(p.BatchWait),
		DiscoveryInterval: durationToStringPtr(p.DiscoveryInterval),
		SnapshotChunkSize: intPtrOrNil(p.SnapshotChunkSize),
		SnapshotInterval:  durationToStringPtr(p.SnapshotInterval),
		Schemas:           strSlicePtrOrNil(p.Schemas),
		Tables:            strSlicePtrOrNil(p.Tables),
	}
}

func SourceConfigToProtocol(a SourceConfig) protocol.SourceConfig {
	return protocol.SourceConfig{
		ID:                a.Id,
		Type:              string(a.Type),
		Host:              a.Host,
		Port:              a.Port,
		User:              stringValueOrEmpty(a.User),
		PassEncrypted:     stringValueOrEmpty(a.Pass),
		Database:          a.Database,
		SlotName:          stringValueOrEmpty(a.SlotName),
		PublicationName:   stringValueOrEmpty(a.PublicationName),
		BatchSize:         intValueOrZero(a.BatchSize),
		BatchWait:         durationValueOrZero(a.BatchWait),
		DiscoveryInterval: durationValueOrZero(a.DiscoveryInterval),
		SnapshotChunkSize: intValueOrZero(a.SnapshotChunkSize),
		SnapshotInterval:  durationValueOrZero(a.SnapshotInterval),
		Schemas:           stringSliceValueOrNil(a.Schemas),
		Tables:            stringSliceValueOrNil(a.Tables),
	}
}

func strPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return strPtr(s)
}

func stringValueOrEmpty(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func strSlicePtrOrNil(s []string) *[]string {
	if len(s) == 0 {
		return nil
	}
	return strSlicePtr(s)
}

func stringSliceValueOrNil(p *[]string) []string {
	if p == nil {
		return nil
	}
	return *p
}

// --- SinkConfig ---

func SinkConfigFromProtocol(p protocol.SinkConfig) SinkConfig {
	return SinkConfig{
		Id:            p.ID,
		Type:          SinkConfigType(p.Type),
		Dsn:           p.DSN,
		MaxAckPending: intPtrOrNil(p.MaxAckPending),
		Options:       mapPtrOrNil(p.Options),
	}
}

func SinkConfigToProtocol(a SinkConfig) protocol.SinkConfig {
	return protocol.SinkConfig{
		ID:            a.Id,
		Type:          string(a.Type),
		DSN:           a.Dsn,
		MaxAckPending: intValueOrZero(a.MaxAckPending),
		Options:       mapValueOrNil(a.Options),
	}
}

func mapPtrOrNil(m map[string]interface{}) *map[string]interface{} {
	if len(m) == 0 {
		return nil
	}
	return mapPtr(m)
}

func mapValueOrNil(p *map[string]interface{}) map[string]interface{} {
	if p == nil {
		return nil
	}
	return *p
}

// --- TableStats ---

func TableStatsFromProtocol(p protocol.TableStats) TableStats {
	return TableStats{
		Status:          strPtrOrNil(p.Status),
		Rps:             f64PtrOrZero(p.RPS),
		TotalSynced:     i64PtrOrZero(int64(p.TotalSynced)),
		ErrorCount:      i64PtrOrZero(int64(p.ErrorCount)),
		LastSourceTs:    timePtrOrNil(p.LastSourceTS),
		LastProcessedTs: timePtrOrNil(p.LastProcessedTS),
		LagMs:           i64PtrOrZero(p.LagMS),
		UpdatedAt:       timePtrOrNil(p.UpdatedAt),
	}
}

func TableStatsToProtocol(a TableStats) protocol.TableStats {
	return protocol.TableStats{
		Status:          stringValueOrEmpty(a.Status),
		RPS:             f64ValueOrZero(a.Rps),
		TotalSynced:     uint64(i64ValueOrZero(a.TotalSynced)),
		ErrorCount:      uint64(i64ValueOrZero(a.ErrorCount)),
		LastSourceTS:    timeValueOrZero(a.LastSourceTs),
		LastProcessedTS: timeValueOrZero(a.LastProcessedTs),
		LagMS:           i64ValueOrZero(a.LagMs),
		UpdatedAt:       timeValueOrZero(a.UpdatedAt),
	}
}

func f64PtrOrZero(f float64) *float64 {
	if f == 0 {
		return nil
	}
	return f64Ptr(f)
}

func i64PtrOrZero(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return i64Ptr(v)
}

func f64ValueOrZero(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

func i64ValueOrZero(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func timeValueOrZero(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return *p
}

// --- TableMetadata ---

func TableMetadataFromProtocol(p protocol.TableMetadata) TableMetadata {
	return TableMetadata{
		Id:        strPtrOrNil(p.ID),
		Name:      strPtrOrNil(p.Name),
		Columns:   strSlicePtrOrNil(p.Columns),
		Types:     strSlicePtrOrNil(p.Types),
		PkColumns: strSlicePtrOrNil(p.PKColumns),
	}
}

func TableMetadataToProtocol(a TableMetadata) protocol.TableMetadata {
	return protocol.TableMetadata{
		ID:        stringValueOrEmpty(a.Id),
		Name:      stringValueOrEmpty(a.Name),
		Columns:   stringSliceValueOrNil(a.Columns),
		Types:     stringSliceValueOrNil(a.Types),
		PKColumns: stringSliceValueOrNil(a.PkColumns),
	}
}

// --- List/Status/Schema response wrappers ---

func PipelineListResponseFromProtocol(p protocol.PipelineListResponse) PipelineListResponse {
	ps := make([]PipelineConfig, len(p.Pipelines))
	for i, pp := range p.Pipelines {
		ps[i] = PipelineConfigFromProtocol(pp)
	}
	return PipelineListResponse{
		Pipelines: ps,
		Total:     p.Total,
		Page:      p.Page,
		Limit:     p.Limit,
	}
}

func SourceListResponseFromProtocol(p protocol.SourceListResponse) SourceListResponse {
	ss := make([]SourceConfig, len(p.Sources))
	for i, sp := range p.Sources {
		ss[i] = SourceConfigFromProtocol(sp)
	}
	return SourceListResponse{Sources: ss}
}

func SinkListResponseFromProtocol(p protocol.SinkListResponse) SinkListResponse {
	ss := make([]SinkConfig, len(p.Sinks))
	for i, sp := range p.Sinks {
		ss[i] = SinkConfigFromProtocol(sp)
	}
	return SinkListResponse{Sinks: ss}
}

func PipelineStatusResponseFromProtocol(p protocol.PipelineStatusResponse) PipelineStatusResponse {
	tables := make(map[string]TableStats, len(p.Tables))
	for k, v := range p.Tables {
		tables[k] = TableStatsFromProtocol(v)
	}
	sinks := make(map[string]map[string]TableStats, len(p.Sinks))
	for k, v := range p.Sinks {
		inner := make(map[string]TableStats, len(v))
		for k2, v2 := range v {
			inner[k2] = TableStatsFromProtocol(v2)
		}
		sinks[k] = inner
	}
	return PipelineStatusResponse{
		PipelineId: p.PipelineID,
		Status:     p.Status,
		Tables:     tables,
		Sinks:      sinks,
	}
}

func SourceSchemaResponseFromProtocol(p protocol.SourceSchemaResponse) SourceSchemaResponse {
	return SourceSchemaResponse{
		SourceId:        p.SourceID,
		AvailableSchemas: append([]string(nil), p.AvailableSchemas...),
		DiscoveryStatus: p.DiscoveryStatus,
	}
}

func SourceTablesResponseFromProtocol(p protocol.SourceTablesResponse) SourceTablesResponse {
	ts := make([]TableMetadata, len(p.Tables))
	for i, t := range p.Tables {
		ts[i] = TableMetadataFromProtocol(t)
	}
	return SourceTablesResponse{
		SourceId: p.SourceID,
		Tables:   ts,
	}
}
