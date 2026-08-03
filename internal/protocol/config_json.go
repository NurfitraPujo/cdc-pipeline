package protocol

import (
	"encoding/json"
	"time"
)

// JSON marshalling for the config structs that carry time.Duration fields.
//
// Each pair below uses the standard "alias + shadow" pattern: an anonymous
// struct embeds a type alias of the config struct (which drops its methods,
// avoiding infinite recursion) and re-declares just the duration fields as
// protocol.Duration. encoding/json resolves conflicting names by depth, so the
// depth-0 shadow fields win over the depth-1 promoted ones, and every
// non-duration field is carried by the embedded alias without being restated.
//
// This keeps the Go field types as time.Duration -- see duration.go for why --
// while making "5s" the wire format on both directions. Reads still accept the
// legacy nanosecond-integer form.
//
// Note: only JSON is affected. The msgp encoding (config_gen.go) and the YAML
// tags are untouched, so NATS KV state blobs and config.example.yaml keep
// working exactly as before.

// --- GlobalConfig ---

// MarshalJSON renders GlobalConfig with its six duration fields as Go duration
// strings ("5s") rather than nanosecond integers.
func (g GlobalConfig) MarshalJSON() ([]byte, error) {
	type alias GlobalConfig
	return json.Marshal(&struct {
		BatchWait          Duration `json:"batch_wait"`
		DrainTimeout       Duration `json:"drain_timeout"`
		ShutdownTimeout    Duration `json:"shutdown_timeout"`
		StabilizationDelay Duration `json:"stabilization_delay"`
		CrashRecoveryDelay Duration `json:"crash_recovery_delay"`
		GlobalReloadDelay  Duration `json:"global_reload_delay"`
		*alias
	}{
		BatchWait:          Duration(g.BatchWait),
		DrainTimeout:       Duration(g.DrainTimeout),
		ShutdownTimeout:    Duration(g.ShutdownTimeout),
		StabilizationDelay: Duration(g.StabilizationDelay),
		CrashRecoveryDelay: Duration(g.CrashRecoveryDelay),
		GlobalReloadDelay:  Duration(g.GlobalReloadDelay),
		alias:              (*alias)(&g),
	})
}

// UnmarshalJSON accepts duration strings and the legacy nanosecond-integer
// form, so configs written before Duration existed still load.
func (g *GlobalConfig) UnmarshalJSON(data []byte) error {
	type alias GlobalConfig
	aux := &struct {
		BatchWait          Duration `json:"batch_wait"`
		DrainTimeout       Duration `json:"drain_timeout"`
		ShutdownTimeout    Duration `json:"shutdown_timeout"`
		StabilizationDelay Duration `json:"stabilization_delay"`
		CrashRecoveryDelay Duration `json:"crash_recovery_delay"`
		GlobalReloadDelay  Duration `json:"global_reload_delay"`
		*alias
	}{alias: (*alias)(g)}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	g.BatchWait = time.Duration(aux.BatchWait)
	g.DrainTimeout = time.Duration(aux.DrainTimeout)
	g.ShutdownTimeout = time.Duration(aux.ShutdownTimeout)
	g.StabilizationDelay = time.Duration(aux.StabilizationDelay)
	g.CrashRecoveryDelay = time.Duration(aux.CrashRecoveryDelay)
	g.GlobalReloadDelay = time.Duration(aux.GlobalReloadDelay)
	return nil
}

// --- RetryConfig ---

// MarshalJSON renders the retry intervals as Go duration strings.
func (r RetryConfig) MarshalJSON() ([]byte, error) {
	type alias RetryConfig
	return json.Marshal(&struct {
		InitialInterval Duration `json:"initial_interval"`
		MaxInterval     Duration `json:"max_interval"`
		*alias
	}{
		InitialInterval: Duration(r.InitialInterval),
		MaxInterval:     Duration(r.MaxInterval),
		alias:           (*alias)(&r),
	})
}

// UnmarshalJSON accepts retry intervals as duration strings or as legacy
// nanosecond integers.
func (r *RetryConfig) UnmarshalJSON(data []byte) error {
	type alias RetryConfig
	aux := &struct {
		InitialInterval Duration `json:"initial_interval"`
		MaxInterval     Duration `json:"max_interval"`
		*alias
	}{alias: (*alias)(r)}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	r.InitialInterval = time.Duration(aux.InitialInterval)
	r.MaxInterval = time.Duration(aux.MaxInterval)
	return nil
}

// --- PipelineConfig ---

// MarshalJSON renders the pipeline's batch_wait override as a Go duration
// string.
func (p PipelineConfig) MarshalJSON() ([]byte, error) {
	type alias PipelineConfig
	return json.Marshal(&struct {
		BatchWait Duration `json:"batch_wait"`
		*alias
	}{
		BatchWait: Duration(p.BatchWait),
		alias:     (*alias)(&p),
	})
}

// UnmarshalJSON accepts batch_wait as a duration string or a legacy
// nanosecond integer.
func (p *PipelineConfig) UnmarshalJSON(data []byte) error {
	type alias PipelineConfig
	aux := &struct {
		BatchWait Duration `json:"batch_wait"`
		*alias
	}{alias: (*alias)(p)}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	p.BatchWait = time.Duration(aux.BatchWait)
	return nil
}

// --- SourceConfig ---

// MarshalJSON renders the source's three interval fields as Go duration
// strings.
func (s SourceConfig) MarshalJSON() ([]byte, error) {
	type alias SourceConfig
	return json.Marshal(&struct {
		BatchWait         Duration `json:"batch_wait"`
		DiscoveryInterval Duration `json:"discovery_interval"`
		SnapshotInterval  Duration `json:"snapshot_interval"`
		*alias
	}{
		BatchWait:         Duration(s.BatchWait),
		DiscoveryInterval: Duration(s.DiscoveryInterval),
		SnapshotInterval:  Duration(s.SnapshotInterval),
		alias:             (*alias)(&s),
	})
}

// UnmarshalJSON accepts the source's intervals as duration strings or as
// legacy nanosecond integers.
func (s *SourceConfig) UnmarshalJSON(data []byte) error {
	type alias SourceConfig
	aux := &struct {
		BatchWait         Duration `json:"batch_wait"`
		DiscoveryInterval Duration `json:"discovery_interval"`
		SnapshotInterval  Duration `json:"snapshot_interval"`
		*alias
	}{alias: (*alias)(s)}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	s.BatchWait = time.Duration(aux.BatchWait)
	s.DiscoveryInterval = time.Duration(aux.DiscoveryInterval)
	s.SnapshotInterval = time.Duration(aux.SnapshotInterval)
	return nil
}
