package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NurfitraPujo/cdc-pipeline/internal/config"
	"github.com/NurfitraPujo/cdc-pipeline/internal/crypto"
	"github.com/NurfitraPujo/cdc-pipeline/internal/metrics"
	"github.com/NurfitraPujo/cdc-pipeline/internal/protocol"
	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"

	// Database drivers for test connections
	_ "github.com/datafuselabs/databend-go"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
)

// isPrivateHost checks if the given IP address is in a private/reserved range.
// This includes loopback (127.0.0.0/8), link-local (169.254.0.0/16), and RFC 1918 private ranges.
func isPrivateHost(ip net.IP) bool {
	// Check loopback
	if ip.IsLoopback() {
		return true
	}
	// Check link-local
	if ip.IsLinkLocalUnicast() {
		return true
	}
	// Check RFC 1918 private ranges: 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
	// Also include 100.64.0.0/10 (Carrier-grade NAT)
	// And 192.0.0.0/24 (IETF Protocol assignments)
	privateBlocks := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"100.64.0.0/10",
		"192.0.0.0/24",
	}
	for _, block := range privateBlocks {
		_, cidr, _ := net.ParseCIDR(block)
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

// validateHost resolves the hostname and checks if the resulting IP is allowed.
// Returns an error message if the host resolves to a private/reserved IP.
func validateHost(host string) string {
	ips, err := net.LookupIP(host)
	if err != nil {
		// If DNS resolution fails, we allow the connection attempt (it may fail for other reasons)
		return ""
	}
	for _, ip := range ips {
		if isPrivateHost(ip) {
			return fmt.Sprintf("host %s resolved to private IP %s not allowed", host, ip.String())
		}
	}
	return ""
}

type Handler struct {
	kv nats.KeyValue
	sf singleflight.Group

	// lagRateSampler is WS-3's optional hook for the time-to-breach warning
	// (plan section 5): when set, PausePipeline uses it to estimate the
	// current WAL growth rate and warns if projected time-to-breach is
	// shorter than the requested (or, absent a ttl, the maximum) TTL. Nil
	// by default -- WS-4 owns wiring a real sampler against the existing
	// cdc_source_slot_lag_bytes probe (querySlotLagBytes in
	// internal/source/postgres/source.go); until then pauses proceed
	// without a projection rather than block on one.
	lagRateSampler SlotLagRateSampler

	// partitionStrategyChecker is WS-5's seam for the Paused -> Resuming
	// per-table strategy check (plan section 10, OQ-5/OQ-7): the operator
	// states every prioritised table has a single integer PK, so resuming
	// with Resnapshot: false (existing cdc_snapshot_chunks retained) is
	// assumed safe -- but that is an assumption to DETECT, not trust. Nil
	// by default, meaning StartPipeline resumes without the check (the
	// pre-WS-5 behaviour); cmd/api/main.go installs the real probe
	// (NewPartitionStrategyChecker) against cdc_snapshot_chunks.
	partitionStrategyChecker PartitionStrategyChecker

	// slotHealthChecker is StartPipeline's seam for the (Paused, start) ->
	// Resuming guard (plan section 4.3): the same replication-slot health
	// probe the pause-expiry ticker already consults on timer expiry
	// (config.SlotHealthChecker / config.NewPostgresSlotHealthChecker),
	// reused rather than reinvented so the operator-driven resume path and
	// the timer-driven one can never disagree about what "the slot is
	// alive" means. Nil by default, meaning StartPipeline resumes
	// optimistically (SlotAlive: true) -- the pre-WS-5 behaviour;
	// cmd/api/main.go installs the real probe.
	slotHealthChecker config.SlotHealthChecker
}

// SlotLagRateSampler estimates a pipeline's current WAL growth rate in
// bytes/sec, typically from two samples of the existing
// cdc_source_slot_lag_bytes probe taken apart in time. See Handler.lagRateSampler.
type SlotLagRateSampler func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) (bytesPerSec float64, ok bool)

// PartitionStrategyChecker inspects a pipeline's recorded snapshot chunks
// (cdc_snapshot_chunks.partition_strategy) and reports whether any belong
// to a table chunked with a strategy resume cannot guarantee coverage for
// (plan section 10, OQ-5: only integer_range is stable across a resume;
// ctid_block and offset address physical row position, which drifts under
// concurrent UPDATE/DELETE/VACUUM). ok is false when the probe itself could
// not run (no source, connection failure, missing chunks table on a
// pipeline that has never snapshotted) -- callers must treat that the same
// as "nothing to report", never as "degraded confirmed". tables lists the
// distinct non-integer_range table names found, for logging/Reason text.
type PartitionStrategyChecker func(ctx context.Context, pipelineID string, cfg protocol.PipelineConfig) (degraded bool, tables []string, ok bool)

func NewHandler(kv nats.KeyValue) *Handler {
	return &Handler{kv: kv}
}

// SetSlotLagRateSampler installs the WAL-growth-rate sampler PausePipeline
// consults for its time-to-breach warning (plan section 5). Pass nil to go
// back to "no projection available".
func (h *Handler) SetSlotLagRateSampler(sampler SlotLagRateSampler) {
	h.lagRateSampler = sampler
}

// SetPartitionStrategyChecker installs the per-table partition-strategy
// probe StartPipeline consults when resuming from Paused (plan section 10,
// OQ-5/OQ-7). Pass nil to resume without the check.
func (h *Handler) SetPartitionStrategyChecker(checker PartitionStrategyChecker) {
	h.partitionStrategyChecker = checker
}

// SetSlotHealthChecker installs the replication-slot health probe
// StartPipeline consults for the (Paused, start) -> Resuming guard (plan
// section 4.3). Pass nil to resume optimistically (SlotAlive: true), the
// pre-WS-5 behaviour.
func (h *Handler) SetSlotHealthChecker(checker config.SlotHealthChecker) {
	h.slotHealthChecker = checker
}

// --- Global Config ---

// GetGlobalConfig retrieves global defaults.
// @Summary      Get global configuration
// @Description  Retrieve global batching settings
// @Tags         config
// @Produce      json
// @Security     Bearer
// @Success      200  {object}  protocol.GlobalConfig
// @Failure      404  {object}  map[string]string "not found"
// @Router       /global [get]
func (h *Handler) GetGlobalConfig(c *gin.Context) {
	entry, err := h.kv.Get(protocol.KeyGlobalConfig)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "global config not found"})
		return
	}

	var cfg protocol.GlobalConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// UpdateGlobalConfig updates global defaults.
// @Summary      Update global configuration
// @Description  Update global batching settings and reload all pipelines
// @Tags         config
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        config  body      protocol.GlobalConfig  true  "Global Config"
// @Success      200     {object}  protocol.GlobalConfig
// @Failure      429     {object}  map[string]string "too many requests"
// @Router       /global [put]
func (h *Handler) UpdateGlobalConfig(c *gin.Context) {
	var cfg protocol.GlobalConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Dynamic Rate Limit for Global: Check if ANY pipeline is currently transitioning
	// since global update triggers a reload for all.
	keys, _ := h.kv.Keys()
	for _, key := range keys {
		if strings.HasSuffix(key, ".transition") {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "cannot update global config while pipelines are transitioning",
			})
			return
		}
	}

	data, _ := json.Marshal(cfg)
	if _, err := h.kv.Put(protocol.KeyGlobalConfig, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// --- Stats & Dashboard ---

// GetStatsSummary reads the pre-aggregated metrics from the global summary key.
// If the cache is older than 30 seconds (or missing), it triggers a re-computation.
// @Summary      Get dashboard summary
// @Description  Retrieve multi-pipeline totals with lazy refresh (Optimized)
// @Tags         stats
// @Produce      json
// @Security     Bearer
// @Success      200  {object}  protocol.StatsSummary
// @Router       /stats/summary [get]
func (h *Handler) GetStatsSummary(c *gin.Context) {
	entry, err := h.kv.Get(protocol.KeyGlobalSummary)

	// If entry exists and is "fresh" (< 30s), return it
	if err == nil && time.Since(entry.Created()) < 30*time.Second {
		var summary protocol.StatsSummary
		if err := json.Unmarshal(entry.Value(), &summary); err == nil {
			c.JSON(http.StatusOK, summary)
			return
		}
	}

	// Else: Stale or missing. Recompute (O(N)) and update cache
	// Use singleflight to ensure only one request performs the computation
	val, err, _ := h.sf.Do("global_summary", func() (any, error) {
		return h.computeAndStoreSummary(), nil
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to compute stats"})
		return
	}

	c.JSON(http.StatusOK, val.(protocol.StatsSummary))
}

// GetStatsHistory returns empty metrics history (Time-series requirement removed).
// @Summary      Get metrics history
// @Description  Retrieve time-series data (Deprecated/Ditched)
// @Tags         stats
// @Produce      json
// @Security     Bearer
// @Success      200  {array}  protocol.HistoryPoint
// @Router       /stats/history [get]
func (h *Handler) GetStatsHistory(c *gin.Context) {
	c.JSON(http.StatusOK, []protocol.HistoryPoint{})
}

func (h *Handler) computeAndStoreSummary() protocol.StatsSummary {
	keys, err := h.kv.Keys()
	summary := protocol.StatsSummary{}
	if err != nil {
		return summary
	}

	// 1. Identify debug sinks to exclude them from production metrics
	debugSinks := make(map[string]bool)
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSinkConfig) {
			if entry, err := h.kv.Get(key); err == nil {
				var sc protocol.SinkConfig
				if err := json.Unmarshal(entry.Value(), &sc); err == nil && sc.Type == "postgres_debug" {
					debugSinks[sc.ID] = true
				}
			}
		}
	}

	// 2. Group production stats by pipeline and table to deduplicate counts
	// pipelineID -> tableName -> max stats found across production sinks
	aggStats := make(map[string]map[string]protocol.TableStats)

	for _, key := range keys {
		info := protocol.ParseTableStatsKey(key)
		if info == nil {
			continue
		}

		// Exclude debug sinks from production summary
		if debugSinks[info.SinkID] {
			continue
		}

		if entry, err := h.kv.Get(key); err == nil {
			var st protocol.TableStats
			if err := protocol.UnmarshalState(entry.Value(), &st); err != nil {
				log.Error().Err(err).Str("key", key).Msg("Failed to decode table stats; excluded from summary totals")
			} else {
				if _, ok := aggStats[info.PipelineID]; !ok {
					aggStats[info.PipelineID] = make(map[string]protocol.TableStats)
				}

				current := aggStats[info.PipelineID][info.Table]
				// Aggregate: Max for count and lag
				if st.TotalSynced > current.TotalSynced {
					current.TotalSynced = st.TotalSynced
				}
				if st.LagMS > current.LagMS {
					current.LagMS = st.LagMS
				}
				aggStats[info.PipelineID][info.Table] = current
			}
		}
	}

	// 3. Compute final summary using aggregated data
	var totalLag int64
	var lagCount int64

	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixPipelineConfig) {
			summary.TotalPipelines++
			id := strings.TrimPrefix(key, protocol.PrefixPipelineConfig)

			// Check status/health
			tsKey := protocol.TransitionStateKey(id)
			if entry, err := h.kv.Get(tsKey); err == nil {
				var ts protocol.PipelineTransitionState
				if err := json.Unmarshal(entry.Value(), &ts); err == nil && ts.Status == "Transitioning" {
					summary.TransitioningCount++
				}
			}

			switch h.getPipelineStatusString(id) {
			case "healthy":
				summary.HealthyCount++
			case "error":
				summary.ErrorCount++
			}

			// Add this pipeline's aggregated production stats
			if pStats, ok := aggStats[id]; ok {
				for _, st := range pStats {
					summary.TotalRowsSynchronized += st.TotalSynced
					totalLag += st.LagMS
					lagCount++
				}
			}
		}
	}

	if lagCount > 0 {
		summary.AvgLagMS = totalLag / lagCount
	}

	data, _ := json.Marshal(summary)
	_, _ = h.kv.Put(protocol.KeyGlobalSummary, data)
	return summary
}

// --- Pipelines ---

// ListPipelines returns all pipelines.
// @Summary      List pipelines
// @Description  Retrieve pipeline configurations with search, status filtering, and pagination
// @Tags         pipelines
// @Produce      json
// @Security     Bearer
// @Param        search  query     string  false  "Search by name or ID"
// @Param        status  query     string  false  "Filter by status (Healthy, Error, Transitioning)"
// @Param        page    query     int     false  "Page number (default: 1)"
// @Param        limit   query     int     false  "Items per page (default: 10)"
// @Success      200  {object}  map[string]any
// @Router       /pipelines [get]
func (h *Handler) ListPipelines(c *gin.Context) {
	// Cleanup stale worker heartbeats while we're at it.
	// Use singleflight to deduplicate concurrent cleanup calls.
	h.sf.Do("cleanup", func() (any, error) {
		h.cleanupStaleHeartbeats()
		return nil, nil
	})

	search := strings.ToLower(c.Query("search"))
	statusFilter := c.Query("status")

	page := 1
	limit := 10
	if p := c.Query("page"); p != "" {
		fmt.Sscanf(p, "%d", &page)
	}
	if l := c.Query("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}

	// T2-7: Clamp pagination values to safe ranges to prevent slice index overflow.
	// Large page values could cause (page-1)*limit to wrap negative.
	if page < 1 {
		page = 1
	}
	if page > 10000 {
		page = 10000
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 100 {
		limit = 100
	}

	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var pipelines []protocol.PipelineConfig
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixPipelineConfig) {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}
			var cfg protocol.PipelineConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
				// Search filter
				if search != "" && !strings.Contains(strings.ToLower(cfg.Name), search) && !strings.Contains(strings.ToLower(cfg.ID), search) {
					continue
				}

				// Status filter (expensive but necessary if requested)
				actualStatus := h.getPipelineStatusString(cfg.ID)
				if statusFilter != "" && !strings.EqualFold(actualStatus, statusFilter) {
					continue
				}

				pipelines = append(pipelines, cfg)
			}
		}
	}

	total := len(pipelines)
	start := (page - 1) * limit
	end := start + limit

	// T2-7: Reject negative start values and handle out-of-bounds gracefully.
	// After clamping page and limit above, start should always be >= 0,
	// but we keep this check for defensive programming.
	if start < 0 || start >= total {
		pipelines = []protocol.PipelineConfig{}
	} else {
		if end > total {
			end = total
		}
		pipelines = pipelines[start:end]
	}

	items := make([]json.RawMessage, 0, len(pipelines))
	for _, pipe := range pipelines {
		item, err := h.pipelineWithStatus(pipe)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		items = append(items, item)
	}

	c.JSON(http.StatusOK, gin.H{
		"pipelines": items,
		"total":     total,
		"page":      page,
		"limit":     limit,
	})
}

// pipelineWithStatus renders a pipeline config with its computed status
// fields spliced in alongside the config fields: the legacy "status" string
// (kept for backward compatibility -- see getPipelineStatusString's doc
// comment for the one place it is not fully compatible) plus the new
// "lifecycle_state" / "health" pair from PipelineLifecycleStatus (plan
// section 4.1).
//
// This deliberately does not use a struct embedding protocol.PipelineConfig.
// PipelineConfig has a MarshalJSON method (internal/protocol/config_json.go,
// which renders durations as "10s" rather than as nanosecond integers), and an
// embedded type's MarshalJSON is promoted to the outer struct -- so an
// embedding wrapper serialises as a bare PipelineConfig and silently drops
// every sibling field, including Status. See list_pipelines_json_test.go.
func (h *Handler) pipelineWithStatus(cfg protocol.PipelineConfig) (json.RawMessage, error) {
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	lifecycle := h.getPipelineLifecycleStatus(cfg.ID)
	extra, err := json.Marshal(struct {
		Status         string                        `json:"status"`
		LifecycleState string                        `json:"lifecycle_state"`
		Health         string                        `json:"health"`
		PausedUntil    *time.Time                     `json:"paused_until,omitempty"`
		Reason         string                        `json:"reason,omitempty"`
		Reconciliation protocol.ReconciliationStatus `json:"reconciliation,omitempty"`
	}{
		Status:         legacyStatusString(lifecycle),
		LifecycleState: lifecycle.Lifecycle,
		Health:         lifecycle.Health,
		PausedUntil:    lifecycle.PausedUntil,
		Reason:         lifecycle.Reason,
		Reconciliation: lifecycle.Reconciliation,
	})
	if err != nil {
		return nil, err
	}

	// {"a":1} + {"status":"healthy",...} -> {"a":1,"status":"healthy",...}
	merged := make([]byte, 0, len(encoded)+len(extra))
	merged = append(merged, encoded[:len(encoded)-1]...)
	merged = append(merged, ',')
	merged = append(merged, extra[1:]...)
	return merged, nil
}

// PipelineLifecycleStatus splits "what is this pipeline doing" from "is it
// doing it well" per plan section 4.1: Lifecycle is the system's view of
// what is actually happening (Running/Paused/Stopped/Transitioning for now
// -- the fuller state machine in internal/protocol/lifecycle.go lands in
// WS-2+), and Health is only meaningful while Lifecycle is "Running". A
// deliberately paused or stopped pipeline is neither healthy nor unhealthy,
// so Health is empty for it -- reporting "error" there, as the previous
// single-string status did for "no worker", is exactly the conflation this
// plan exists to fix.
type PipelineLifecycleStatus struct {
	Lifecycle   string     `json:"lifecycle_state"`
	Health      string     `json:"health"`
	PausedUntil *time.Time `json:"paused_until,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	// Reconciliation carries WS-5/WS-7's degrade signal (plan invariant 5:
	// "stale must be visible in the UI ... hiding it would recreate the
	// 'reports healthy while diverging' failure this plan exists to
	// prevent"). omitempty relies on ReconciliationOK being "" (see its doc
	// comment), the same convention protocol.PipelineLifecycleRecord already
	// uses for this field.
	Reconciliation protocol.ReconciliationStatus `json:"reconciliation,omitempty"`
}

// getPipelineLifecycleStatus computes the split lifecycle/health view for a
// pipeline. It fetches the pipeline's config to read desired_state --
// getPipelineStatusString (the pre-existing, still-used single-string form)
// wraps this rather than duplicating the KV reads.
//
// The persisted lifecycle record (written only by PausePipeline/
// StartPipeline via protocol.Transition, see getLifecycleRecord) is
// authoritative whenever one exists: it is the only place paused_until
// lives, and it is the only source that can distinguish e.g.
// NeedsResnapshot/Snapshotting/Failed, none of which desired_state alone
// can express. desired_state is used only as the fallback for pipelines
// that predate WS-2 or have never been paused/started, mirroring
// getLifecycleRecord's own "no record" default of Running.
func (h *Handler) getPipelineLifecycleStatus(id string) PipelineLifecycleStatus {
	tsKey := protocol.TransitionStateKey(id)
	if tsEntry, err := h.kv.Get(tsKey); err == nil {
		var ts protocol.PipelineTransitionState
		if err := json.Unmarshal(tsEntry.Value(), &ts); err == nil && ts.Status == "Transitioning" {
			return PipelineLifecycleStatus{Lifecycle: "Transitioning"}
		}
	}

	if recEntry, err := h.kv.Get(protocol.LifecycleStateKey(id)); err == nil {
		var rec protocol.PipelineLifecycleRecord
		if err := json.Unmarshal(recEntry.Value(), &rec); err == nil && rec.State != "" {
			st := PipelineLifecycleStatus{
				Lifecycle:      string(rec.State),
				PausedUntil:    rec.PausedUntil,
				Reason:         rec.Reason,
				Reconciliation: rec.Reconciliation,
			}
			if rec.State == protocol.StateRunning {
				st.Health = h.workerHealth(id)
			}
			return st
		}
	}

	desired := protocol.DesiredStateRunning
	if entry, err := h.kv.Get(protocol.PipelineConfigKey(id)); err == nil {
		var cfg protocol.PipelineConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
			desired = cfg.EffectiveDesiredState()
		}
	}

	switch desired {
	case protocol.DesiredStatePaused:
		return PipelineLifecycleStatus{Lifecycle: "Paused"}
	case protocol.DesiredStateStopped:
		return PipelineLifecycleStatus{Lifecycle: "Stopped"}
	}

	return PipelineLifecycleStatus{Lifecycle: "Running", Health: h.workerHealth(id)}
}

// workerHealth reports the running-worker heartbeat health used whenever
// lifecycle state is (effectively) "Running" -- the only case where a
// missing or stale heartbeat is meaningful, since a paused/stopped/
// transitioning pipeline has no worker by design.
func (h *Handler) workerHealth(id string) string {
	health := "healthy"
	hbKey := protocol.WorkerHeartbeatKey(id)
	if hbEntry, err := h.kv.Get(hbKey); err == nil {
		var hb protocol.WorkerHeartbeat
		if err := json.Unmarshal(hbEntry.Value(), &hb); err == nil {
			if time.Since(hb.UpdatedAt) > 60*time.Second {
				health = "error"
			} else if hb.Status != "" && hb.Status != "Running" {
				// A fresh heartbeat that reports a non-"Running" status (e.g. a
				// worker that failed to start and is looping in the retry
				// backoff, see config/manager.go monitorWorker) must not be
				// reported "healthy" just because it is being updated on
				// schedule (WS-8/WS-9: "a pipeline that fails to construct a
				// processor must no longer be reported healthy").
				health = "error"
			}
		}
	} else {
		health = "error"
	}
	return health
}

// getPipelineStatusString is the backward-compatible single-string status
// used by the existing "status" field and the ?status= filter. It is a thin
// projection of getPipelineLifecycleStatus: "transitioning" unchanged,
// "healthy"/"error" unchanged for a running pipeline, and now "paused" /
// "stopped" instead of the old behaviour of reporting "error" for any
// pipeline with no worker -- callers filtering on status="error" will no
// longer match a deliberately paused/stopped pipeline. That is the one
// place this is not backward compatible; see plan section 4.1.
func (h *Handler) getPipelineStatusString(id string) string {
	return legacyStatusString(h.getPipelineLifecycleStatus(id))
}

// legacyStatusString projects a PipelineLifecycleStatus down to the single
// "status" string the API returned before this plan. See
// getPipelineStatusString's doc comment for the one behaviour change.
func legacyStatusString(st PipelineLifecycleStatus) string {
	switch st.Lifecycle {
	case "Transitioning":
		return "transitioning"
	case string(protocol.StateRunning):
		return st.Health
	case string(protocol.StatePaused), string(protocol.StatePausing):
		return "paused"
	case string(protocol.StateStopped), string(protocol.StateStopping):
		return "stopped"
	default:
		// NeedsResnapshot/Snapshotting/Resuming/Failed have no pre-WS-2
		// equivalent single-string status; "transitioning" is the closest
		// existing meaning ("the pipeline is neither settled-healthy nor
		// settled-paused/stopped right now") and keeps the legacy field
		// non-empty for status= filtering.
		return "transitioning"
	}
}

// RestartPipeline triggers a reload for a specific pipeline.
// @Summary      Restart pipeline
// @Description  Trigger a manual restart/reload for a pipeline
// @Tags         pipelines
// @Security     Bearer
// @Param        id   path      string  true  "Pipeline ID"
// @Success      202  "Accepted"
// @Failure      404  {object}  map[string]string "not found"
// @Router       /pipelines/{id}/restart [post]
func (h *Handler) RestartPipeline(c *gin.Context) {
	id := c.Param("id")
	key := protocol.PipelineConfigKey(id)
	if _, err := h.kv.Get(key); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pipeline not found"})
		return
	}

	// Trigger transition state to notify workers
	ts := protocol.PipelineTransitionState{
		ID:        id,
		Status:    "Transitioning",
		StartedAt: time.Now(),
	}
	data, _ := json.Marshal(ts)
	if _, err := h.kv.Put(protocol.TransitionStateKey(id), data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"status": "restart triggered"})
}

// getLifecycleRecord reads the persisted lifecycle record for a pipeline,
// defaulting to protocol.StateRunning when none exists yet -- every
// pipeline created before WS-2, or that has never been paused/stopped, has
// no record, and "no record" must mean the same thing PipelineConfig's
// EffectiveDesiredState zero value means: running.
func (h *Handler) getLifecycleRecord(id string) protocol.PipelineLifecycleRecord {
	entry, err := h.kv.Get(protocol.LifecycleStateKey(id))
	if err != nil {
		return protocol.PipelineLifecycleRecord{State: protocol.StateRunning}
	}
	var rec protocol.PipelineLifecycleRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil || rec.State == "" {
		return protocol.PipelineLifecycleRecord{State: protocol.StateRunning}
	}
	return rec
}

// putLifecycleRecord persists the lifecycle record. This, plus
// getLifecycleRecord above, is the only place the API package touches
// protocol.LifecycleStateKey -- every write is the result of a
// protocol.Transition call, never a direct state assignment (section 4.5).
func (h *Handler) putLifecycleRecord(id string, rec protocol.PipelineLifecycleRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = h.kv.Put(protocol.LifecycleStateKey(id), data)
	return err
}

// pausePipelineRequest is the optional body for POST /pipelines/{id}/pause.
// TTL, when set, is a Go duration string (e.g. "30m", "2h"). It is
// converted to an absolute paused_until timestamp at request time and
// stored that way (PipelineLifecycleRecord.PausedUntil) -- see plan
// section 8. The 4h ceiling and the WAL-budget projection that warns
// before confirming a pause (plan section 5) are WS-3/WS-4's guard, not
// this handler's: any positive TTL is accepted here.
type pausePipelineRequest struct {
	TTL string `json:"ttl,omitempty"`
}

// PausePipeline requests that a running pipeline stop consuming while
// retaining its replication slot (plan section 4.2), optionally for a
// bounded TTL. It only sets operator intent (desired_state) and the
// lifecycle record; it does not drain the worker itself. The actual drain
// reuses ConfigManager's existing stopWorker (manager.go,
// honourDesiredState), which the desired_state write below triggers
// asynchronously via ConfigManager's existing config-watch -- see plan
// section 9 (WS-2 must not add a second drain path).
//
// Because that drain is not observed here, the lifecycle record advances
// straight from Running through Pausing to Paused within this request,
// mirroring the same simplification StartPipeline makes for
// Resuming -> Running: WS-2 has no async drain-complete/worker-healthy
// watcher yet (that machinery arrives with WS-3's ticker), so the two
// legal hops are taken back-to-back rather than left half-finished.
//
// @Summary      Pause pipeline
// @Description  Stop consuming while retaining the replication slot, optionally for a bounded TTL
// @Tags         pipelines
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        id    path  string                true  "Pipeline ID"
// @Param        body  body  pausePipelineRequest  false "Optional TTL"
// @Success      200  {object}  protocol.PipelineLifecycleRecord
// @Failure      400  {object}  map[string]string "invalid ttl"
// @Failure      404  {object}  map[string]string "not found"
// @Failure      409  {object}  map[string]string "illegal transition"
// @Router       /pipelines/{id}/pause [post]
//nolint:gocyclo // lifecycle validation + state-machine handler; each branch is a distinct precondition/transition and extracting them would scatter the request flow
func (h *Handler) PausePipeline(c *gin.Context) {
	id := c.Param("id")
	entry, err := h.kv.Get(protocol.PipelineConfigKey(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pipeline not found"})
		return
	}
	var cfg protocol.PipelineConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Always attempt to read and bind the body rather than gating on
	// ContentLength: chunked requests, HTTP/2 streamed bodies, and any
	// client that omits Content-Length report ContentLength == -1 (and a
	// request with a nil Body, as in some test harnesses, isn't even
	// bindable via gin's own ShouldBindJSON, which returns a bare
	// "invalid request" for req.Body == nil rather than io.EOF). Skipping
	// the bind for any of those silently drops ttl -- turning a bounded
	// pause into an unbounded one, defeating the whole point of the TTL
	// backstop (plan section 2). Reading the raw body ourselves and only
	// unmarshalling non-empty bytes handles a nil body, a zero-length
	// body, and a chunked/streamed body identically: the only body that
	// legitimately binds to nothing is a truly empty one.
	var req pausePipelineRequest
	if c.Request != nil && c.Request.Body != nil {
		body, err := c.GetRawData()
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
		}
	}

	var ttl time.Duration
	if req.TTL != "" {
		ttl, err = time.ParseDuration(req.TTL)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid ttl: %s", err.Error())})
			return
		}
		if ttl <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "ttl must be positive"})
			return
		}
		// WS-3: enforce the 4h ceiling (plan section 2/OQ-3) here, at the
		// point the TTL is accepted, rather than silently clamping it --
		// silently truncating an operator's requested window would defeat
		// the point of asking for one.
		if ttl > protocol.MaxPauseTTL {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("ttl exceeds the maximum pause duration of %s", protocol.MaxPauseTTL)})
			return
		}
	}

	rec := h.getLifecycleRecord(id)

	// Running -> Pausing, or Paused -> Paused (extending an in-progress
	// pause, plan section 11: "show the resume time and make extending it
	// trivial"). Anything else (Stopping, Stopped, ...) is rejected with
	// the transition table's own error, so this handler carries no
	// separate "already paused" special case beyond the extend row itself.
	firstOutcome, err := protocol.Transition(rec.State, protocol.EventPause, protocol.Guards{})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	outcome := firstOutcome
	// Pausing -> Paused, taken immediately -- see doc comment above. Only
	// needed when the first hop actually landed in Pausing (Running ->
	// Pausing); the extend leg (Paused -> Paused) is already terminal and
	// re-running drain_complete against it would be a second, undocumented
	// transition.
	if firstOutcome.To == protocol.StatePausing {
		outcome, err = protocol.Transition(protocol.StatePausing, protocol.EventDrainComplete, protocol.Guards{})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	now := time.Now().UTC()
	newRec := protocol.PipelineLifecycleRecord{
		State:          outcome.To,
		Reconciliation: rec.Reconciliation,
		UpdatedAt:      now,
	}
	// A pause with no ttl in the body must still expire: an unbounded pause
	// is exactly the "forgotten pause filling the source disk" scenario the
	// timer exists to prevent (plan section 2). Falling back to the 4h
	// ceiling here -- rather than leaving PausedUntil nil -- closes that
	// hole; maybeResumeExpiredPause (internal/config/pause_expiry.go) only
	// acts on a non-nil PausedUntil, so a nil value here would retain the
	// slot forever regardless of the ceiling enforced above on an explicit
	// ttl.
	effectiveTTL := ttl
	if effectiveTTL <= 0 {
		effectiveTTL = protocol.MaxPauseTTL
	}
	pausedUntil := now.Add(effectiveTTL)
	newRec.PausedUntil = &pausedUntil

	// Marshal + write desired_state before the lifecycle record, and check
	// the marshal error instead of discarding it. Ordering matters: if the
	// config write fails, the lifecycle record must NOT have already been
	// committed to "Paused" -- otherwise the handler has reported 500 while
	// leaving a record that claims the pipeline is paused even though
	// desired_state (and therefore the running worker) never changed, and a
	// retried pause then gets rejected 409 by the transition table with no
	// way to reconcile except calling start. A dropped marshal error is
	// worse than cosmetic here too: a nil `data` would Put an empty value
	// into the config key, handing ConfigManager's watcher something it
	// cannot unmarshal (manager.go:351).
	cfg.DesiredState = protocol.DesiredStatePaused
	data, err := json.Marshal(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := h.kv.Put(protocol.PipelineConfigKey(id), data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if err := h.putLifecycleRecord(id, newRec); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, h.pauseResponse(c, id, cfg, ttl, newRec))
}

// pausePipelineResponse mirrors protocol.PipelineLifecycleRecord with one
// extra field: the plan section 5 time-to-breach warning, populated only
// when a SlotLagRateSampler is installed and its projection is shorter
// than the requested TTL. Embedding (rather than a new generated schema)
// keeps this additive and avoids the openapi.yaml regen this endpoint's
// core response shape doesn't otherwise need.
type pausePipelineResponse struct {
	protocol.PipelineLifecycleRecord
	Warning string `json:"warning,omitempty"`
}

// pauseResponse computes the optional WAL-budget warning for PausePipeline.
// effectiveTTL is protocol.MaxPauseTTL when the request left ttl
// unspecified -- newRec.PausedUntil is always set by the caller now (an
// unbounded request is capped to the 4h ceiling there, see PausePipeline),
// so MaxPauseTTL is genuinely the longest window the projection ever needs
// to warn about.
//
// The remaining budget fed into ProjectedTimeToBreach is the slot's actual
// headroom (protocol.WALBudgetBytes minus WAL already retained,
// remainingWALBudgetBytes), not the constant full budget: a slot already
// 20 GB into its 30 GB budget has far less runway than a fresh one, and
// passing the constant systematically under-warns (see WS-3 blocking
// finding). Fetching it costs a second short-lived query against the same
// connection the rate sampler already opened; on any failure it falls back
// to the full budget, which is the sampler's previous (optimistic, but not
// wrong-in-a-new-way) behaviour.
func (h *Handler) pauseResponse(c *gin.Context, id string, cfg protocol.PipelineConfig, ttl time.Duration, rec protocol.PipelineLifecycleRecord) pausePipelineResponse {
	resp := pausePipelineResponse{PipelineLifecycleRecord: rec}
	projected, ok := h.projectPauseBreach(c.Request.Context(), id, cfg)
	if !ok {
		return resp
	}
	effectiveTTL := ttl
	if effectiveTTL <= 0 {
		effectiveTTL = protocol.MaxPauseTTL
	}
	if projected < effectiveTTL {
		resp.Warning = breachWarning(projected)
	}
	return resp
}

// breachWarning renders the plan section 5 time-to-breach message shared by
// PausePipeline's post-commit warning and PausePauseProjection's pre-commit
// one -- the two endpoints must say the same thing about the same
// projection, just at different points in the operator's flow.
func breachWarning(projected time.Duration) string {
	return fmt.Sprintf(
		"at the current WAL growth rate this pause will hit the WAL budget in ~%s",
		projected.Round(time.Minute),
	)
}

// projectPauseBreach computes the plan section 5 projected time-to-breach
// for a pipeline: how long, at the WAL growth rate sampled from the
// existing cdc_source_slot_lag_bytes probe, until the source's slot exceeds
// its WAL budget. It is the single place both PausePipeline's post-commit
// warning (pauseResponse, above) and the pause-projection endpoint's
// pre-commit read (PausePauseProjection, below) get this number from, so
// the two can never drift apart on what "the projection" means.
//
// Returns false when no sampler is installed, the sampler cannot produce a
// rate (e.g. too little history yet), or the growth rate is non-positive
// (ProjectedTimeToBreach's own "never breaches" case) -- callers treat that
// as "no projection available" rather than a hard error, matching the
// sampler's existing best-effort contract.
func (h *Handler) projectPauseBreach(ctx context.Context, id string, cfg protocol.PipelineConfig) (time.Duration, bool) {
	if h.lagRateSampler == nil {
		return 0, false
	}
	rate, ok := h.lagRateSampler(ctx, id, cfg)
	if !ok {
		return 0, false
	}

	remaining := protocol.WALBudgetBytes
	if len(cfg.Sources) > 0 {
		if db, srcCfg, err := h.openSourceDBByID(cfg.Sources[0]); err == nil {
			if r, ok := remainingWALBudgetBytes(ctx, db, srcCfg.SlotName); ok {
				remaining = r
			}
			_ = db.Close()
		}
	}

	return protocol.ProjectedTimeToBreach(remaining, rate)
}

// pausePipelineProjectionResponse is GET /pipelines/{id}/pause-projection's
// body: the plan section 5 warning, read-only and computed against whatever
// ttl the operator is currently considering, without committing a pause.
// Deliberately the same shape as pausePipelineResponse's warning field so
// the frontend's post-commit and pre-commit code paths render identically.
type pausePipelineProjectionResponse struct {
	Warning string `json:"warning,omitempty"`
}

// PausePauseProjectionRoute adapts PausePauseProjection to gin.HandlerFunc.
//
// The generated oapi-codegen signature takes the path and query parameters as
// arguments, but this server registers its routes by hand (cmd/api/main.go)
// rather than through RegisterHandlers, so nothing would otherwise bind them.
// Without this adapter the route simply is not mounted and the endpoint 404s
// -- which the pause dialog reads as "no warning to show", silently defeating
// the whole point of a pre-commit projection.
func (h *Handler) PausePauseProjectionRoute(c *gin.Context) {
	var params PausePauseProjectionParams
	if ttl := c.Query("ttl"); ttl != "" {
		params.Ttl = &ttl
	}
	h.PausePauseProjection(c, c.Param("id"), params)
}

// PausePauseProjection answers "if I paused for this ttl right now, would I
// hit the WAL budget guard first?" without taking any action -- no
// desired_state write, no lifecycle transition. Plan section 5 is explicit
// that the projection must be shown "before the pause is confirmed"; this
// is that read. PauseDialog calls it as the operator adjusts the TTL slider
// and re-renders the same warning PausePipeline would otherwise only show
// after commit.
//
// @Summary      Project a pause's time-to-breach
// @Description  Read-only projection of when a pause of the given ttl would hit the WAL budget guard, shown before the pause is confirmed
// @Tags         pipelines
// @Produce      json
// @Security     Bearer
// @Param        id   path   string  true   "Pipeline ID"
// @Param        ttl  query  string  false  "Go duration string being considered, e.g. \"2h\" (defaults to the 4h ceiling)"
// @Success      200  {object}  pausePipelineProjectionResponse
// @Failure      400  {object}  map[string]string "invalid ttl"
// @Failure      404  {object}  map[string]string "not found"
// @Router       /pipelines/{id}/pause-projection [get]
func (h *Handler) PausePauseProjection(c *gin.Context, id PathID, params PausePauseProjectionParams) {
	entry, err := h.kv.Get(protocol.PipelineConfigKey(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pipeline not found"})
		return
	}
	var cfg protocol.PipelineConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ttl := protocol.MaxPauseTTL
	if params.Ttl != nil && *params.Ttl != "" {
		parsed, err := time.ParseDuration(*params.Ttl)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid ttl: %s", err.Error())})
			return
		}
		if parsed <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "ttl must be positive"})
			return
		}
		if parsed > protocol.MaxPauseTTL {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("ttl exceeds the maximum pause duration of %s", protocol.MaxPauseTTL)})
			return
		}
		ttl = parsed
	}

	resp := pausePipelineProjectionResponse{}
	if projected, ok := h.projectPauseBreach(c.Request.Context(), id, cfg); ok && projected < ttl {
		resp.Warning = breachWarning(projected)
	}
	c.JSON(http.StatusOK, resp)
}

// StartPipeline requests that a pipeline resume. For the common case
// (Paused -> Running) it closes the loop synchronously, the same way
// PausePipeline does for Running -> Paused, and flips desired_state back
// to running so ConfigManager's existing config-watch starts the worker
// (manager.go startNewWorker); it does not start the worker itself.
//
// For (NeedsResnapshot, start) -> Snapshotting, WS-6 drives it too: it sets
// desired_state=running so ConfigManager starts a worker that re-snapshots
// (see the branch below), but does not itself advance the lifecycle state
// any further -- that happens asynchronously once the re-snapshot completes
// (internal/config/resnapshot_watcher.go).
//
// For every other state outside this handler's scope -- Stopped, Failed --
// Transition is still consulted so the call is forward-compatible, but the
// resulting lifecycle state is only persisted, not driven further, and
// desired_state is deliberately left untouched: setting it to running for
// Stopped -> NeedsResnapshot would let ConfigManager start a plain worker
// for a pipeline that needs a re-snapshot first, which is exactly
// invariant 1.
//
// @Summary      Start/resume pipeline
// @Description  Resume a paused pipeline, or advance a stopped/failed pipeline's lifecycle state
// @Tags         pipelines
// @Produce      json
// @Security     Bearer
// @Param        id   path  string  true  "Pipeline ID"
// @Success      200  {object}  protocol.PipelineLifecycleRecord
// @Failure      404  {object}  map[string]string "not found"
// @Failure      409  {object}  map[string]string "illegal transition"
// @Router       /pipelines/{id}/start [post]
//nolint:gocyclo // lifecycle validation + state-machine handler; each branch is a distinct precondition/transition and extracting them would scatter the request flow
func (h *Handler) StartPipeline(c *gin.Context) {
	id := c.Param("id")
	entry, err := h.kv.Get(protocol.PipelineConfigKey(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pipeline not found"})
		return
	}
	var cfg protocol.PipelineConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	rec := h.getLifecycleRecord(id)

	guards := protocol.Guards{
		// Optimistic default: SlotAlive true, WALStatusLost false. Real
		// once WS-5 wires up SetSlotHealthChecker below -- until a checker
		// is installed this preserves the pre-WS-5 behaviour.
		SlotAlive: true,
	}
	if h.slotHealthChecker != nil {
		// WS-5: the slot can now genuinely be gone or invalidated by the
		// time an operator hits /start on a long-paused pipeline (plan
		// section 4.3) -- reuse the same probe the pause-expiry ticker
		// consults on timer expiry (config.SlotHealthChecker) rather than
		// trusting the constant above, so the operator-driven and
		// timer-driven resume paths can never disagree about slot health.
		// (Paused, start) only consults SlotAlive (see the transition
		// table, internal/protocol/lifecycle.go) -- WALStatusLost is
		// (Paused, timer_expiry)'s guard, not this one's.
		health := h.slotHealthChecker(c.Request.Context(), id, cfg)
		guards.SlotAlive = health.Alive
	}
	// RM-2: (Failed, start) is the only row that consults NeedsResnapshot
	// (protocol/lifecycle.go), and it was previously always left false --
	// unconditionally routing a recovered-from-Failed pipeline to Resuming
	// regardless of slot health. Derive it from the same SlotAlive probe
	// this handler already runs for (Paused, start): if the slot did not
	// survive whatever put the pipeline in Failed, a plain resume cannot
	// safely continue and must re-snapshot instead, mirroring
	// (Paused, timer_expiry)'s !SlotAlive -> NeedsResnapshot row.
	guards.NeedsResnapshot = !guards.SlotAlive
	outcome, err := protocol.Transition(rec.State, protocol.EventStart, guards)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	newRec := protocol.PipelineLifecycleRecord{
		State:          outcome.To,
		Reconciliation: rec.Reconciliation,
		UpdatedAt:      time.Now().UTC(),
	}

	if outcome.To == protocol.StateResuming {
		// Resuming -> Running, taken immediately -- see doc comment above.
		final, err := protocol.Transition(protocol.StateResuming, protocol.EventWorkerHealthy, protocol.Guards{})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		newRec.State = final.To
		newRec.PausedUntil = nil // invariant 3: cleared on exit from Paused/Pausing

		// WS-5: resume from Paused continues the existing snapshot
		// (Resnapshot: false -- internal/source/postgres/source.go never
		// sets Snapshot.Resnapshot, so LoadJob resumes cdc_snapshot_chunks
		// rather than wiping it). That is only safe for chunks recorded as
		// integer_range (plan section 10, OQ-5): ctid_block/offset chunks
		// key off physical row position, which can drift from concurrent
		// UPDATE/DELETE/VACUUM even while the slot stays alive through a
		// pause. The operator states every prioritised table has a single
		// integer PK (OQ-7), but that is an assumption to detect, not
		// trust -- read what each table's chunks actually recorded, and
		// degrade explicitly (log + reconciliation stale) rather than
		// resuming on a strategy that cannot guarantee coverage.
		if h.partitionStrategyChecker != nil {
			if degraded, tables, ok := h.partitionStrategyChecker(c.Request.Context(), id, cfg); ok && degraded {
				log.Warn().
					Str("pipeline_id", id).
					Strs("tables", tables).
					Msg("resume: some tables' snapshot chunks use a non-integer_range partition strategy; coverage cannot be guaranteed, marking reconciliation stale")
				newRec.Reconciliation = protocol.ReconciliationStale
				newRec.Reason = fmt.Sprintf(
					"resume detected non-integer_range snapshot chunks for table(s) %s; delete/update coverage during the pause cannot be guaranteed until reconciled",
					strings.Join(tables, ", "),
				)
			}
		}

		// See PausePipeline's matching comment: write desired_state (and
		// check the marshal error) before the lifecycle record, so a failed
		// config write never leaves a lifecycle record claiming "Running"
		// while desired_state -- and therefore ConfigManager's worker --
		// never actually resumed.
		cfg.DesiredState = protocol.DesiredStateRunning
		data, err := json.Marshal(cfg)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if _, err := h.kv.Put(protocol.PipelineConfigKey(id), data); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	} else if outcome.To == protocol.StateSnapshotting {
		// WS-6: (NeedsResnapshot, start) -> Snapshotting is the one other
		// outcome this handler must actually drive, not just persist. Unlike
		// the WS-5-era comment above (now superseded for this specific row),
		// invariant 1 does not forbid setting desired_state=running here --
		// it forbids reaching Running WITHOUT passing through Snapshotting,
		// and Snapshotting is exactly the state being entered. Flipping
		// desired_state lets ConfigManager's config-watch start a worker,
		// and PostgresSource.shouldResnapshot (internal/source/postgres/
		// source.go) reads this very record back to set
		// Snapshot.Resnapshot: true for that worker, wiping
		// cdc_snapshot_chunks and re-snapshotting from scratch. Completion
		// is detected asynchronously by the pause-expiry ticker's WS-6
		// sweep (maybeCompleteResnapshot, internal/config/
		// resnapshot_watcher.go), which fires (Snapshotting, complete) once
		// cdc_snapshot_job.completed flips -- that is what actually lands
		// the pipeline on Running with reconciliation marked stale
		// (invariant 5), not this handler.
		//
		// Write ordering here follows StopPipeline's rule, not
		// PausePipeline/StartPipeline's Resuming shortcut above: the second
		// hop (a worker actually starting and re-snapshotting) is
		// asynchronous, driven by ConfigManager's config-watch once
		// desired_state flips. PostgresSource.shouldResnapshot
		// (internal/source/postgres/source.go) reads this very lifecycle
		// record back to decide Snapshot.Resnapshot, so the record MUST be
		// durable as Snapshotting before desired_state is written -- writing
		// desired_state first would let a worker start, read the still-
		// NeedsResnapshot record, and boot without wiping
		// cdc_snapshot_chunks, silently skipping the re-snapshot.
		newRec.PausedUntil = nil
		if err := h.putLifecycleRecord(id, newRec); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		cfg.DesiredState = protocol.DesiredStateRunning
		data, err := json.Marshal(cfg)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if _, err := h.kv.Put(protocol.PipelineConfigKey(id), data); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, newRec)
		return
	} else {
		// Any other legal outcome (e.g. Stopped -> NeedsResnapshot):
		// paused_until is only meaningful in Pausing/Paused (invariant 3),
		// so it stays cleared for every state this branch can reach.
		// desired_state is deliberately left untouched here -- setting it to
		// running for Stopped -> NeedsResnapshot would let ConfigManager
		// start a plain (non-resnapshotting) worker before the operator's
		// second /start call actually reaches Snapshotting, which is
		// exactly invariant 1.
		newRec.PausedUntil = nil
	}

	if err := h.putLifecycleRecord(id, newRec); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, newRec)
}

// StopPipeline requests that a running or paused pipeline stop consuming
// and drop its replication slot, releasing WAL (plan section 4.2 -- the
// distinction from pause). Unlike PausePipeline/StartPipeline's
// Pausing->Paused and Resuming->Running shortcuts, the Stopping->Stopped
// leg is NOT taken synchronously here: dropping the slot only after the
// worker has genuinely finished draining is ConfigManager's job
// (honourDesiredState -> finalizeStop, internal/config/manager.go), and
// that drain is asynchronous. This handler performs only the
// Running/Paused -> Stopping half and hands off.
//
// Write ordering is deliberately the REVERSE of PausePipeline/StartPipeline:
// the lifecycle record is written before desired_state, not after. Those
// two endpoints write desired_state first because their second hop is taken
// synchronously in the same request, and the concern there is a failed
// config write leaving a record that claims a state change desired_state
// never made. Stop's second hop is asynchronous: the instant desired_state
// is written, ConfigManager's config-watch can fire and finalizeStop will
// look for State == Stopping. Writing desired_state first here would let
// that watcher race ahead of a lifecycle record that does not exist yet,
// so finalizeStop's guard would (harmlessly, but pointlessly) no-op on its
// first attempt. Writing the record first closes that race: by the time
// desired_state is written, Stopping is already durable.
//
// @Summary      Stop pipeline
// @Description  Stop consuming and drop the replication slot, releasing WAL
// @Tags         pipelines
// @Produce      json
// @Security     Bearer
// @Param        id   path  string  true  "Pipeline ID"
// @Success      200  {object}  protocol.PipelineLifecycleRecord
// @Failure      404  {object}  map[string]string "not found"
// @Failure      409  {object}  map[string]string "illegal transition"
// @Router       /pipelines/{id}/stop [post]
func (h *Handler) StopPipeline(c *gin.Context) {
	id := c.Param("id")
	entry, err := h.kv.Get(protocol.PipelineConfigKey(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pipeline not found"})
		return
	}
	var cfg protocol.PipelineConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	rec := h.getLifecycleRecord(id)

	// Running -> Stopping and Paused -> Stopping are both legal rows (plan
	// section 4.3); everything else (already Stopping/Stopped, etc.) is
	// rejected by the transition table's own error, so this handler carries
	// no separate "already stopped" special case.
	outcome, err := protocol.Transition(rec.State, protocol.EventStop, protocol.Guards{})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	newRec := protocol.PipelineLifecycleRecord{
		State: outcome.To,
		// invariant 3: paused_until is only meaningful in Pausing/Paused.
		// Stopping from Paused must clear it here, not carry it forward --
		// it stops meaning anything the moment the pipeline leaves Paused.
		Reconciliation: rec.Reconciliation,
		UpdatedAt:      time.Now().UTC(),
	}

	if err := h.putLifecycleRecord(id, newRec); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	cfg.DesiredState = protocol.DesiredStateStopped
	data, err := json.Marshal(cfg)
	if err != nil {
		// The lifecycle record already claims Stopping but desired_state
		// never changed, so the worker (if any) is still running and would
		// never be handed to finalizeStop. Best-effort restore the prior
		// record so a retried stop is not permanently rejected 409 by a
		// Stopping record nothing will ever advance.
		_ = h.putLifecycleRecord(id, rec)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if _, err := h.kv.Put(protocol.PipelineConfigKey(id), data); err != nil {
		_ = h.putLifecycleRecord(id, rec)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, newRec)
}

// CreatePipeline creates a new pipeline.
// @Summary      Create pipeline
// @Description  Create a new pipeline configuration
// @Tags         pipelines
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        pipeline  body      protocol.PipelineConfig  true  "Pipeline Config"
// @Success      201       {object}  protocol.PipelineConfig
// @Failure      429       {object}  map[string]string "too many requests"
// @Router       /pipelines [post]
func (h *Handler) CreatePipeline(c *gin.Context) {
	var cfg protocol.PipelineConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate sources/sinks exist
	for _, sid := range cfg.Sources {
		if _, err := h.kv.Get(protocol.SourceConfigKey(sid)); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("source %s not found", sid)})
			return
		}
	}
	for _, sid := range cfg.Sinks {
		if _, err := h.kv.Get(protocol.SinkConfigKey(sid)); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("sink %s not found", sid)})
			return
		}
	}

	// Dynamic Rate Limit: Check if pipeline is currently transitioning
	tsKey := protocol.TransitionStateKey(cfg.ID)
	if entry, err := h.kv.Get(tsKey); err == nil {
		var ts protocol.PipelineTransitionState
		if err := json.Unmarshal(entry.Value(), &ts); err == nil && ts.Status == "Transitioning" {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":      "pipeline is currently transitioning/restarting",
				"started_at": ts.StartedAt,
			})
			return
		}
	}

	// RM-1 (invariant 1, plan section 4.4): a plain desired_state=running
	// write must never be the thing that starts a worker for a pipeline
	// whose lifecycle record is Stopped or NeedsResnapshot -- that skips
	// Snapshotting entirely. CreatePipeline reuses cfg.ID as the KV key
	// (protocol.PipelineConfigKey below), so re-creating a pipeline that was
	// previously stopped (its lifecycle record, protocol.LifecycleStateKey,
	// outlives the config delete) hits exactly the same bypass UpdatePipeline
	// closes; see that handler's matching check for the full rationale.
	if err := h.rejectBypassOfInvariant1(cfg.ID, cfg); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	data, _ := json.Marshal(cfg)
	key := protocol.PipelineConfigKey(cfg.ID)
	if _, err := h.kv.Put(key, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, cfg)
}

// UpdatePipeline updates an existing pipeline.
// @Summary      Update pipeline
// @Description  Update an existing pipeline configuration
// @Tags         pipelines
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        id        path      string                   true  "Pipeline ID"
// @Param        pipeline  body      protocol.PipelineConfig  true  "Pipeline Config"
// @Success      200       {object}  protocol.PipelineConfig
// @Failure      429       {object}  map[string]string "too many requests"
// @Router       /pipelines/{id} [put]
func (h *Handler) UpdatePipeline(c *gin.Context) {
	id := c.Param("id")

	// Read the raw body so we can tell whether the request explicitly set
	// desired_state, as opposed to omitting it. This matters because
	// DesiredState's zero value ("") means "running" (EffectiveDesiredState),
	// so binding straight into a fresh protocol.PipelineConfig would make an
	// update body that omits desired_state indistinguishable from one that
	// explicitly asks for running -- silently clobbering a paused/stopped
	// pipeline back to running. The frontend never even sends the field
	// (web/src/api/pipelines.ts UpdatePipelineRequest has no desired_state),
	// so this is not a theoretical gap.
	raw, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var cfg protocol.PipelineConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var bodyFields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &bodyFields); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if _, sentDesiredState := bodyFields["desired_state"]; !sentDesiredState {
		// Carry the currently-persisted desired_state forward instead of
		// letting the zero value silently mean "running".
		if entry, err := h.kv.Get(protocol.PipelineConfigKey(id)); err == nil {
			var existing protocol.PipelineConfig
			if err := json.Unmarshal(entry.Value(), &existing); err == nil {
				cfg.DesiredState = existing.DesiredState
			}
		}
	}

	cfg.ID = id
	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate sources/sinks exist
	for _, sid := range cfg.Sources {
		if _, err := h.kv.Get(protocol.SourceConfigKey(sid)); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("source %s not found", sid)})
			return
		}
	}
	for _, sid := range cfg.Sinks {
		if _, err := h.kv.Get(protocol.SinkConfigKey(sid)); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("sink %s not found", sid)})
			return
		}
	}

	tsKey := protocol.TransitionStateKey(id)
	if entry, err := h.kv.Get(tsKey); err == nil {
		var ts protocol.PipelineTransitionState
		if err := json.Unmarshal(entry.Value(), &ts); err == nil && ts.Status == "Transitioning" {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":      "pipeline is currently transitioning/restarting",
				"started_at": ts.StartedAt,
			})
			return
		}
	}

	// RM-1 (invariant 1, plan section 4.4): "the invariant the whole plan
	// exists to protect". docs/openapi.yaml still publishes desired_state as
	// a writable property (it drives pause/stop-adjacent bookkeeping and is
	// the field the frontend reads back), but PUT is no longer allowed to be
	// a second, parallel path into Running: only protocol.Transition may
	// decide that (section 4.5). A caller setting desired_state=running
	// while the persisted lifecycle record is Stopped or NeedsResnapshot
	// gets rejected here rather than silently handed a plain worker with no
	// Snapshotting hop -- they must call POST /pipelines/{id}/start, which
	// drives Stopped -> NeedsResnapshot -> Snapshotting -> Running (plan
	// section 4.3) through the same Transition choke point StartPipeline
	// uses. This is intentionally a rejection, not a translation: silently
	// rewriting the caller's PUT into a /start call here would be a second
	// hand-rolled state-change path, which section 4.5 forbids just as much
	// as a bypass would be.
	if err := h.rejectBypassOfInvariant1(id, cfg); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}

	data, _ := json.Marshal(cfg)
	key := protocol.PipelineConfigKey(id)
	if _, err := h.kv.Put(key, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// rejectBypassOfInvariant1 enforces plan section 4.4 invariant 1 at the API
// layer: desired_state=running must never be the thing that starts a worker
// for a pipeline whose lifecycle record isn't already worker-bearing,
// because that skips the transitions (Snapshotting/Resuming, each gated by
// their own guards -- see the plan's section 4.3 transition table) that are
// the only sanctioned paths to Running. Rather than enumerate the bad
// states, this allow-lists the states that already carry (or are actively
// starting) a worker -- Running, Snapshotting, Resuming -- and rejects
// everything else, including Paused/Pausing/Stopping: a raw PUT must never
// be able to resume a Paused pipeline directly, because that skips the
// SlotAlive guard on Paused->Resuming (a dead slot must force
// NeedsResnapshot, not a silent resume from a stale/recreated slot) and the
// /start path's partition-strategy staleness check. getLifecycleRecord
// defaults to StateRunning when no record exists (a pipeline that predates
// WS-2, or was never paused/stopped), which is correct here too: "no
// record" is treated as already-running and never trips this check.
func (h *Handler) rejectBypassOfInvariant1(id string, cfg protocol.PipelineConfig) error {
	if cfg.EffectiveDesiredState() != protocol.DesiredStateRunning {
		return nil
	}
	rec := h.getLifecycleRecord(id)
	switch rec.State {
	case protocol.StateRunning, protocol.StateSnapshotting, protocol.StateResuming:
		return nil
	default:
		return fmt.Errorf(
			"cannot set desired_state=running directly: pipeline %s lifecycle state is %s -- call POST /pipelines/%s/start instead",
			id, rec.State, id,
		)
	}
}

// GetPipeline returns a single pipeline.
// @Summary      Get pipeline
// @Description  Retrieve a specific pipeline configuration
// @Tags         pipelines
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Pipeline ID"
// @Success      200  {object}  protocol.PipelineConfig
// @Failure      404  {object}  map[string]string "not found"
// @Router       /pipelines/{id} [get]
func (h *Handler) GetPipeline(c *gin.Context) {
	id := c.Param("id")
	key := protocol.PipelineConfigKey(id)
	entry, err := h.kv.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "pipeline not found"})
		return
	}

	var cfg protocol.PipelineConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Include the computed status, matching ListPipelines. Without it the
	// detail page had no way to show a pipeline's health and fell back to a
	// hardcoded "Configured" badge.
	item, err := h.pipelineWithStatus(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Data(http.StatusOK, "application/json; charset=utf-8", item)
}

// DeletePipeline deletes a pipeline.
// @Summary      Delete pipeline
// @Description  Delete a pipeline configuration and stop its worker
// @Tags         pipelines
// @Security     Bearer
// @Param        id   path      string  true  "Pipeline ID"
// @Success      204  "No Content"
// @Router       /pipelines/{id} [delete]
func (h *Handler) DeletePipeline(c *gin.Context) {
	id := c.Param("id")
	key := protocol.PipelineConfigKey(id)
	if err := h.kv.Delete(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusNoContent, nil)
}

// GetPipelineStatus returns current LSN status and stats.
// @Summary      Get pipeline status
// @Description  Retrieve aggregated multi-table status and stats for a pipeline
// @Tags         pipelines
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Pipeline ID"
// @Success      200  {object}  map[string]any
// @Router       /pipelines/{id}/status [get]
func (h *Handler) GetPipelineStatus(c *gin.Context) {
	id := c.Param("id")
	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 1. Identify debug sinks for this pipeline
	debugSinks := make(map[string]bool)
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSinkConfig) {
			if entry, err := h.kv.Get(key); err == nil {
				var sc protocol.SinkConfig
				if err := json.Unmarshal(entry.Value(), &sc); err == nil && sc.Type == "postgres_debug" {
					debugSinks[sc.ID] = true
				}
			}
		}
	}

	statusMap := make(map[string]any)
	tableStats := make(map[string]protocol.TableStats)           // aggregated by table
	sinkStats := make(map[string]map[string]protocol.TableStats) // sinkID -> tableName -> stats

	prefix := protocol.PipelineStatusPrefix(id)
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}

			if strings.HasSuffix(key, "_checkpoint") {
				var cp protocol.Checkpoint
				if err := protocol.UnmarshalState(entry.Value(), &cp); err != nil {
					log.Error().Err(err).Str("key", key).Msg("Failed to decode checkpoint; omitting from status")
				} else {
					statusMap[key] = cp
				}
			} else if strings.HasSuffix(key, ".stats") {
				info := protocol.ParseTableStatsKey(key)
				var st protocol.TableStats
				if err := protocol.UnmarshalState(entry.Value(), &st); err != nil {
					log.Error().Err(err).Str("key", key).Msg("Failed to decode table stats; omitting from status")
				} else {
					// Raw map for backward compatibility
					statusMap[key] = st

					if info != nil {
						// Per-sink stats
						if _, ok := sinkStats[info.SinkID]; !ok {
							sinkStats[info.SinkID] = make(map[string]protocol.TableStats)
						}
						sinkStats[info.SinkID][info.Table] = st

						// Aggregated table stats (exclude debug sinks)
						if !debugSinks[info.SinkID] {
							current := tableStats[info.Table]
							if st.TotalSynced > current.TotalSynced {
								current.TotalSynced = st.TotalSynced
							}
							if st.LagMS > current.LagMS {
								current.LagMS = st.LagMS
							}
							// Keep most recent update time
							if st.UpdatedAt.After(current.UpdatedAt) {
								current.UpdatedAt = st.UpdatedAt
								current.Status = st.Status
							}
							tableStats[info.Table] = current
						}
					}
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"pipeline_id": id,
		"status":      statusMap,  // Backward compatibility
		"tables":      tableStats, // Aggregated production stats
		"sinks":       sinkStats,  // Per-sink detailed stats
	})
}

// --- Sources ---

// ListSources returns all sources.
// @Summary      List sources
// @Description  Retrieve all source configurations
// @Tags         sources
// @Produce      json
// @Security     Bearer
// @Success      200  {object}  map[string][]protocol.SourceConfig
// @Router       /sources [get]
func (h *Handler) ListSources(c *gin.Context) {
	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var sources []protocol.SourceConfig
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSourceConfig) {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}
			var cfg protocol.SourceConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
				cfg.PassEncrypted = ""
				sources = append(sources, cfg)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"sources": sources})
}

// CreateSource creates a new source.
// @Summary      Create source
// @Description  Create a new source configuration
// @Tags         sources
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        source  body      protocol.SourceConfig  true  "Source Config"
// @Success      201     {object}  protocol.SourceConfig
// @Router       /sources [post]
func (h *Handler) CreateSource(c *gin.Context) {
	var cfg protocol.SourceConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Encrypt sensitive fields
	key, err := crypto.GetEncryptionKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if cfg.PassEncrypted != "" {
		encrypted, err := crypto.Encrypt(cfg.PassEncrypted, key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encrypt password"})
			return
		}
		cfg.PassEncrypted = encrypted
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal config"})
		return
	}
	storageKey := protocol.SourceConfigKey(cfg.ID)
	if _, err := h.kv.Put(storageKey, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, cfg)
}

// UpdateSource updates an existing source.
// @Summary      Update source
// @Description  Update an existing source configuration
// @Tags         sources
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        id      path      string                 true  "Source ID"
// @Param        source  body      protocol.SourceConfig  true  "Source Config"
// @Success      200     {object}  protocol.SourceConfig
// @Router       /sources/{id} [put]
func (h *Handler) UpdateSource(c *gin.Context) {
	id := c.Param("id")
	var cfg protocol.SourceConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	cfg.ID = id
	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Encrypt sensitive fields or preserve existing
	var oldCfg protocol.SourceConfig
	oldKey := protocol.SourceConfigKey(id)
	if entry, err := h.kv.Get(oldKey); err == nil {
		_ = json.Unmarshal(entry.Value(), &oldCfg)
	}

	if cfg.PassEncrypted == "" {
		cfg.PassEncrypted = oldCfg.PassEncrypted
	} else if cfg.PassEncrypted == "__CLEAR__" {
		cfg.PassEncrypted = ""
	} else {
		key, err := crypto.GetEncryptionKey()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		encrypted, err := crypto.Encrypt(cfg.PassEncrypted, key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encrypt password"})
			return
		}
		cfg.PassEncrypted = encrypted
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal config"})
		return
	}
	storageKey := protocol.SourceConfigKey(id)
	if _, err := h.kv.Put(storageKey, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// DeleteSource deletes a source.
// @Summary      Delete source
// @Description  Delete a source configuration
// @Tags         sources
// @Security     Bearer
// @Param        id   path      string  true  "Source ID"
// @Success      204  "No Content"
// @Router       /sources/{id} [delete]
func (h *Handler) DeleteSource(c *gin.Context) {
	id := c.Param("id")
	key := protocol.SourceConfigKey(id)
	if err := h.kv.Delete(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusNoContent, nil)
}

// GetSource returns a single source configuration.
// @Summary      Get source
// @Description  Retrieve a specific source configuration
// @Tags         sources
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Source ID"
// @Success      200  {object}  protocol.SourceConfig
// @Failure      404  {object}  map[string]string "not found"
// @Router       /sources/{id} [get]
func (h *Handler) GetSource(c *gin.Context) {
	id := c.Param("id")
	key := protocol.SourceConfigKey(id)
	entry, err := h.kv.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "source not found"})
		return
	}

	var cfg protocol.SourceConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Scrub sensitive fields to avoid shoulder-surfing and leakages.
	// The frontend will preserve existing passwords by omitting them from PUT payloads.
	cfg.PassEncrypted = ""

	c.JSON(http.StatusOK, SourceConfigFromProtocol(cfg))
}

// discoverySchemas returns the schemas ListSourceTables' fallback discovery
// path should query. Empty configured Schemas means "public" only, NOT all
// schemas -- see MULTI_SCHEMA_PLAN.md §2.4/§8 item 4. Every existing source
// config has Schemas empty (the field was dead until this stage), so
// defaulting to a wildcard here would silently start discovering tables from
// every schema on the database on upgrade.
func discoverySchemas(configured []string) []string {
	if len(configured) == 0 {
		return []string{"public"}
	}
	return configured
}

// openSourceDB loads the source config for id, decrypts its password, and
// opens a connection to the source database. Callers must Close() the
// returned *sql.DB. Returns (nil, nil, false) with the response already
// written if the source cannot be loaded or connected to -- callers should
// just return in that case.
func (h *Handler) openSourceDB(c *gin.Context, id string) (*sql.DB, bool) {
	key := protocol.SourceConfigKey(id)
	entry, err := h.kv.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "source not found"})
		return nil, false
	}

	var cfg protocol.SourceConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return nil, false
	}

	if cfg.PassEncrypted != "" {
		encKey, err := crypto.GetEncryptionKey()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return nil, false
		}
		if decrypted, err := crypto.Decrypt(cfg.PassEncrypted, encKey); err == nil {
			cfg.PassEncrypted = decrypted
		}
	}

	u := &url.URL{
		Scheme: "postgres", Host: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		User: url.UserPassword(cfg.User, cfg.PassEncrypted), Path: cfg.Database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	q.Set("connect_timeout", "3")
	u.RawQuery = q.Encode()

	db, err := sql.Open("pgx", u.String())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("failed to open connection: %v", err)})
		return nil, false
	}
	return db, true
}

// GetSourceSchema triggers or retrieves table schema discovery for a source.
// @Summary      Get source schema
// @Description  Discover available schemas directly from the source database
// @Tags         sources
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Source ID"
// @Success      200  {object}  map[string]any
// @Router       /sources/{id}/schema [get]
func (h *Handler) GetSourceSchema(c *gin.Context) {
	id := c.Param("id")

	db, ok := h.openSourceDB(c, id)
	if !ok {
		return
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// pg_namespace, not information_schema.schemata, so the query needs no
	// per-database grants beyond CONNECT -- excludes catalog/toast/temp
	// schemas, which are never valid replication targets.
	rows, err := db.QueryContext(ctx, `
		SELECT nspname FROM pg_namespace
		WHERE nspname NOT IN ('pg_catalog', 'information_schema')
		  AND nspname NOT LIKE 'pg_toast%'
		  AND nspname NOT LIKE 'pg_temp_%'
		ORDER BY nspname`)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("failed to discover schemas: %v", err)})
		return
	}
	defer rows.Close()

	schemas := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			schemas = append(schemas, name)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"source_id":         id,
		"available_schemas": schemas,
		"discovery_status":  "ready",
	})
}

// ListSourceTables returns all discovered tables for a source.
// @Summary      List source tables
// @Description  Retrieve all discovered tables and their metadata for a source
// @Tags         sources
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Source ID"
// @Success      200  {object}  map[string][]protocol.TableMetadata
// @Router       /sources/{id}/tables [get]
func (h *Handler) ListSourceTables(c *gin.Context) {
	sourceID := c.Param("id")
	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var tables []protocol.TableMetadata
	suffix := fmt.Sprintf(".sources.%s.tables.", sourceID)
	for _, key := range keys {
		if strings.Contains(key, suffix) && strings.HasSuffix(key, ".metadata") {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}
			var meta protocol.TableMetadata
			if err := json.Unmarshal(entry.Value(), &meta); err == nil {
				tables = append(tables, meta)
			}
		}
	}

	if len(tables) == 0 {
		// Attempt dynamic discovery from the source database.
		key := protocol.SourceConfigKey(sourceID)
		entry, err := h.kv.Get(key)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"source_id": sourceID, "tables": tables})
			return
		}
		var cfg protocol.SourceConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
			c.JSON(http.StatusOK, gin.H{"source_id": sourceID, "tables": tables})
			return
		}

		db, ok := h.openSourceDB(c, sourceID)
		if !ok {
			return
		}
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		schemas := discoverySchemas(cfg.Schemas)

		rows, err := db.QueryContext(ctx,
			"SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = ANY($1) AND table_type = 'BASE TABLE'",
			schemas)
		if err != nil {
			// Surface the failure instead of swallowing it. database/sql
			// connects lazily, so openSourceDB above succeeds even when the
			// source is unreachable and this query is where that first shows
			// up. Reporting 200 with an empty list made "cannot reach the
			// database" indistinguishable from "connected fine, no tables" --
			// the discovery button just showed nothing. GetSourceSchema
			// already answers 502 for the same condition.
			c.JSON(http.StatusBadGateway, gin.H{
				"error": fmt.Sprintf("table discovery failed: %v", err),
			})
			return
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var schema, tableName string
			if err := rows.Scan(&schema, &tableName); err == nil {
				// Filter out snapshot tables
				if strings.Contains(tableName, "cdc_snapshot") {
					continue
				}
				ref := protocol.TableRef{Schema: schema, Table: tableName}
				tables = append(tables, protocol.TableMetadata{
					ID:     ref.KeyToken(),
					Name:   tableName,
					Schema: ref.Schema,
				})
			}
		}

		// An error part-way through iteration would otherwise truncate the
		// list silently.
		if err := rows.Err(); err != nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"error": fmt.Sprintf("table discovery failed: %v", err),
			})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"source_id": sourceID, "tables": tables})
}

// --- Sinks ---

// ListSinks returns all sinks.
// @Summary      List sinks
// @Description  Retrieve all sink configurations
// @Tags         sinks
// @Produce      json
// @Security     Bearer
// @Success      200  {object}  map[string][]protocol.SinkConfig
// @Router       /sinks [get]
func (h *Handler) ListSinks(c *gin.Context) {
	keys, err := h.kv.Keys()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	encryptionKey, err := crypto.GetEncryptionKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var sinks []protocol.SinkConfig
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSinkConfig) {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}
			var cfg protocol.SinkConfig
			if err := json.Unmarshal(entry.Value(), &cfg); err == nil {
				if cfg.DSN != "" {
					decrypted, err := crypto.Decrypt(cfg.DSN, encryptionKey)
					if err != nil {
						log.Warn().Err(err).Str("sink_id", cfg.ID).Msg("Failed to decrypt DSN, returning encrypted value")
					} else {
						cfg.DSN = maskDSN(decrypted)
					}
				}
				sinks = append(sinks, cfg)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"sinks": sinks})
}

// GetSink returns a single sink configuration.
// @Summary      Get sink
// @Description  Retrieve a specific sink configuration
// @Tags         sinks
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Sink ID"
// @Success      200  {object}  protocol.SinkConfig
// @Failure      404  {object}  map[string]string "not found"
// @Router       /sinks/{id} [get]
func (h *Handler) GetSink(c *gin.Context) {
	id := c.Param("id")
	key := protocol.SinkConfigKey(id)
	entry, err := h.kv.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "sink not found"})
		return
	}

	var cfg protocol.SinkConfig
	if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Decrypt sensitive fields
	encryptionKey, err := crypto.GetEncryptionKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if cfg.DSN != "" {
		decrypted, err := crypto.Decrypt(cfg.DSN, encryptionKey)
		if err != nil {
			log.Warn().Err(err).Str("sink_id", cfg.ID).Msg("Failed to decrypt DSN, returning encrypted value")
		} else {
			cfg.DSN = maskDSN(decrypted)
		}
	}

	c.JSON(http.StatusOK, cfg)
}

// CreateSink creates a new sink.
// @Summary      Create sink
// @Description  Create a new sink configuration
// @Tags         sinks
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        sink  body      protocol.SinkConfig  true  "Sink Config"
// @Success      201   {object}  protocol.SinkConfig
// @Router       /sinks [post]
func (h *Handler) CreateSink(c *gin.Context) {
	var cfg protocol.SinkConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Encrypt sensitive fields
	key, err := crypto.GetEncryptionKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if cfg.DSN != "" {
		encrypted, err := crypto.Encrypt(cfg.DSN, key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encrypt DSN"})
			return
		}
		cfg.DSN = encrypted
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal config"})
		return
	}
	storageKey := protocol.SinkConfigKey(cfg.ID)
	if _, err := h.kv.Put(storageKey, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, cfg)
}

// UpdateSink updates an existing sink.
// @Summary      Update sink
// @Description  Update an existing sink configuration
// @Tags         sinks
// @Accept       json
// @Produce      json
// @Security     Bearer
// @Param        id    path      string               true  "Sink ID"
// @Param        sink  body      protocol.SinkConfig  true  "Sink Config"
// @Success      200   {object}  protocol.SinkConfig
// @Router       /sinks/{id} [put]
func (h *Handler) UpdateSink(c *gin.Context) {
	id := c.Param("id")
	var cfg protocol.SinkConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	cfg.ID = id
	if err := cfg.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Encrypt sensitive fields or reconstruct if masked
	key, err := crypto.GetEncryptionKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var oldCfg protocol.SinkConfig
	oldKey := protocol.SinkConfigKey(id)
	if entry, err := h.kv.Get(oldKey); err == nil {
		_ = json.Unmarshal(entry.Value(), &oldCfg)
	}

	var decryptedOldDSN string
	if oldCfg.DSN != "" {
		decrypted, err := crypto.Decrypt(oldCfg.DSN, key)
		if err == nil {
			decryptedOldDSN = decrypted
		}
	}

	reconstructed := reconstructDSN(cfg.DSN, decryptedOldDSN)
	cfg.DSN = reconstructed

	if cfg.DSN != "" {
		encrypted, err := crypto.Encrypt(cfg.DSN, key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encrypt DSN"})
			return
		}
		cfg.DSN = encrypted
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal config"})
		return
	}
	storageKey := protocol.SinkConfigKey(id)
	if _, err := h.kv.Put(storageKey, data); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, cfg)
}

// DeleteSink deletes a sink.
// @Summary      Delete sink
// @Description  Delete a sink configuration
// @Tags         sinks
// @Security     Bearer
// @Param        id   path      string  true  "Sink ID"
// @Success      204  "No Content"
// @Router       /sinks/{id} [delete]
func (h *Handler) DeleteSink(c *gin.Context) {
	id := c.Param("id")
	key := protocol.SinkConfigKey(id)
	if err := h.kv.Delete(key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusNoContent, nil)
}

// --- Workers ---

// GetWorkerHeartbeat returns worker status.
// @Summary      Get worker status
// @Description  Retrieve heartbeat and uptime for a worker
// @Tags         workers
// @Produce      json
// @Security     Bearer
// @Param        id   path      string  true  "Worker ID"
// @Success      200  {object}  protocol.WorkerHeartbeat
// @Router       /workers/{id}/heartbeat [get]
func (h *Handler) GetWorkerHeartbeat(c *gin.Context) {
	id := c.Param("id")
	key := protocol.WorkerHeartbeatKey(id)
	entry, err := h.kv.Get(key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
		return
	}

	var hb protocol.WorkerHeartbeat
	if err := json.Unmarshal(entry.Value(), &hb); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, hb)
}

func (h *Handler) cleanupStaleHeartbeats() {
	metrics.APICleanupRuns.Inc()

	keys, err := h.kv.Keys()
	if err != nil {
		return
	}

	for _, key := range keys {
		if strings.HasPrefix(key, "cdc.worker.") && strings.HasSuffix(key, ".heartbeat") {
			entry, err := h.kv.Get(key)
			if err != nil {
				continue
			}

			var hb protocol.WorkerHeartbeat
			if err := json.Unmarshal(entry.Value(), &hb); err == nil {
				if time.Since(hb.UpdatedAt) > 60*time.Second {
					log.Info().Str("worker_id", hb.WorkerID).Msg("Cleaning up stale heartbeat")
					h.kv.Delete(key)
				}
			}
		}
	}
}

// StreamMetrics provides real-time status updates via SSE using NATS Watch.
// @Summary      Stream pipeline metrics
// @Description  Server-Sent Events stream for pipeline status and stats
// @Tags         pipelines
// @Produce      text/event-stream
// @Security     Bearer
// @Param        id   path      string  true  "Pipeline ID"
// @Router       /pipelines/{id}/metrics [get]
func (h *Handler) StreamMetrics(c *gin.Context) {
	pipelineID := c.Param("id")

	// 1. Identify debug sinks for this pipeline to help frontend filter
	keys, _ := h.kv.Keys()
	debugSinks := make(map[string]bool)
	for _, key := range keys {
		if strings.HasPrefix(key, protocol.PrefixSinkConfig) {
			if entry, err := h.kv.Get(key); err == nil {
				var sc protocol.SinkConfig
				if err := json.Unmarshal(entry.Value(), &sc); err == nil && sc.Type == "postgres_debug" {
					debugSinks[sc.ID] = true
				}
			}
		}
	}

	pattern := protocol.PipelineStatusPrefix(pipelineID) + "*"
	watcher, err := h.kv.Watch(pattern)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer watcher.Stop()

	// Set SSE headers only after watcher creation succeeds
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Transfer-Encoding", "chunked")

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case entry, ok := <-watcher.Updates():
			if !ok {
				log.Warn().Msg("watcher closed, exiting metric stream")
				return
			}
			if entry == nil {
				continue
			}

			var data any
			key := entry.Key()
			sinkID := ""
			isDebug := false

			if strings.HasSuffix(key, "_checkpoint") {
				var cp protocol.Checkpoint
				if err := protocol.UnmarshalState(entry.Value(), &cp); err != nil {
					log.Error().Err(err).Str("key", key).Msg("Failed to decode checkpoint; omitting from state stream")
					continue
				}
				data = cp
			} else if strings.HasSuffix(key, ".stats") {
				var st protocol.TableStats
				if err := protocol.UnmarshalState(entry.Value(), &st); err != nil {
					log.Error().Err(err).Str("key", key).Msg("Failed to decode table stats; omitting from state stream")
					continue
				}
				data = st

				if info := protocol.ParseTableStatsKey(key); info != nil {
					sinkID = info.SinkID
					isDebug = debugSinks[sinkID]
				}
			} else if strings.HasSuffix(key, ".transition") {
				var ts protocol.PipelineTransitionState
				if err := json.Unmarshal(entry.Value(), &ts); err != nil {
					continue
				}
				data = ts
			} else {
				data = string(entry.Value())
			}

			c.SSEvent("message", map[string]any{
				"key":      key,
				"data":     data,
				"sink_id":  sinkID,
				"is_debug": isDebug,
			})
			c.Writer.Flush()
		}
	}
}

const (
	// dsnMask stands in for a sink password on every read path.
	dsnMask = "***"
	// dsnMaskEncoded is what net/url writes for dsnMask inside userinfo.
	dsnMaskEncoded = "%2A%2A%2A"
)

func maskDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	if u.User != nil {
		_, hasPassword := u.User.Password()
		if hasPassword {
			u.User = url.UserPassword(u.User.Username(), dsnMask)
			// url.String() percent-encodes "*" in userinfo, so the mask went
			// out as "%2A%2A%2A" and was shown to the operator that way in the
			// sink list and the edit form. reconstructDSN still worked (Parse
			// decodes it back), but a masked DSN nobody recognises invites
			// hand-editing around it, which is how the real password gets
			// clobbered. "*" is a sub-delim and legal unencoded in userinfo,
			// so emitting it literally is valid and round-trips.
			return strings.Replace(u.String(), dsnMaskEncoded, dsnMask, 1)
		}
	}
	return dsn
}

func reconstructDSN(newDSN, oldDSN string) string {
	uNew, err := url.Parse(newDSN)
	if err != nil {
		return newDSN
	}
	if uNew.User != nil {
		pass, hasPassword := uNew.User.Password()
		// Parse decodes userinfo, so this matches whether the client echoed
		// back the literal "***" or the percent-encoded form older responses
		// emitted.
		if hasPassword && pass == dsnMask {
			uOld, err := url.Parse(oldDSN)
			if err == nil && uOld.User != nil {
				oldPass, hasOldPassword := uOld.User.Password()
				if hasOldPassword {
					uNew.User = url.UserPassword(uNew.User.Username(), oldPass)
					return uNew.String()
				}
			}
		}
	}
	return newDSN
}

// TestSourceConnection tests connection to the source database.
func (h *Handler) TestSourceConnection(c *gin.Context) {
	var cfg protocol.SourceConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	// T2-1: Validate host resolves to a non-private IP before attempting connection.
	if errMsg := validateHost(cfg.Host); errMsg != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return
	}

	// If id is provided and pass is empty, inject old password from KV
	if cfg.PassEncrypted == "" {
		if cfg.ID != "" {
			key := protocol.SourceConfigKey(cfg.ID)
			if entry, err := h.kv.Get(key); err == nil {
				var oldCfg protocol.SourceConfig
				if err := json.Unmarshal(entry.Value(), &oldCfg); err == nil {
					encKey, err := crypto.GetEncryptionKey()
					if err != nil {
						c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
						return
					}
					if oldCfg.PassEncrypted != "" {
						decrypted, err := crypto.Decrypt(oldCfg.PassEncrypted, encKey)
						if err == nil {
							cfg.PassEncrypted = decrypted
						}
					}
				}
			}
		}
	} else if cfg.PassEncrypted == "__CLEAR__" {
		cfg.PassEncrypted = ""
	}

	u := &url.URL{
		Scheme: "postgres", Host: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		User: url.UserPassword(cfg.User, cfg.PassEncrypted), Path: cfg.Database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	q.Set("connect_timeout", "3")
	u.RawQuery = q.Encode()
	dsn := u.String()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to open connection: %v", err)})
		return
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Database connection failed: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "Connection successful"})
}

// TestSinkConnection tests connection to the sink database.
func (h *Handler) TestSinkConnection(c *gin.Context) {
	var cfg protocol.SinkConfig
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	// Parse DSN to check if password is masked. If ID is provided, reconstruct it.
	if cfg.ID != "" && cfg.DSN != "" {
		u, err := url.Parse(cfg.DSN)
		if err == nil && u.User != nil {
			pass, hasPass := u.User.Password()
			if hasPass && pass == "***" {
				key := protocol.SinkConfigKey(cfg.ID)
				if entry, err := h.kv.Get(key); err == nil {
					var oldCfg protocol.SinkConfig
					if err := json.Unmarshal(entry.Value(), &oldCfg); err == nil {
						encKey, err := crypto.GetEncryptionKey()
						if err != nil {
							c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
							return
						}
						decrypted, err := crypto.Decrypt(oldCfg.DSN, encKey)
						if err == nil {
							cfg.DSN = reconstructDSN(cfg.DSN, decrypted)
						}
					}
				}
			}
		}
	}

	if cfg.Type == "postgres_debug" || cfg.Type == "databend" {
		// T2-1: Validate host resolves to a non-private IP before attempting connection.
		// Extract host from DSN for validation.
		var host string
		if u, err := url.Parse(cfg.DSN); err == nil {
			host = u.Hostname()
		}
		if host != "" {
			if errMsg := validateHost(host); errMsg != "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
				return
			}
		}

		db, err := sql.Open(cfg.Type, cfg.DSN)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to open connection: %v", err)})
			return
		}
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := db.PingContext(ctx); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Connection failed: %v", err)})
			return
		}
	} else {
		_, err := url.Parse(cfg.DSN)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Invalid DSN: %v", err)})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "Connection successful"})
}
