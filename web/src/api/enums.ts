/**
 * The values the API actually emits and accepts.
 *
 * Source of truth is `getPipelineStatusString` in internal/api/handler.go,
 * which returns exactly "healthy", "transitioning" or "error". This list
 * previously read running/stopped/error/paused -- three of which the backend
 * never produces and none of which the `?status=` filter would have matched.
 */
export const PIPELINE_STATUS = ["healthy", "transitioning", "error"] as const;
export type PipelineStatus = (typeof PIPELINE_STATUS)[number];
export const PIPELINE_STATUS_LABELS: Record<PipelineStatus, string> = {
	healthy: "Healthy",
	transitioning: "Transitioning",
	error: "Error",
};

export const SOURCE_TYPE = ["postgres"] as const;
export type SourceType = (typeof SOURCE_TYPE)[number];
export const SOURCE_TYPE_LABELS: Record<SourceType, string> = {
	postgres: "PostgreSQL",
};

export const SINK_TYPE = ["databend", "postgres_debug"] as const;
export type SinkType = (typeof SINK_TYPE)[number];
export const SINK_TYPE_LABELS: Record<SinkType, string> = {
	databend: "Databend",
	postgres_debug: "Postgres (Debug)",
};

/**
 * Transformer types the worker can actually construct.
 *
 * Source of truth is the `RegisterTransformer(...)` call sites in
 * `internal/transformer/` — currently `builtin.go` (mask, uppercase) and
 * `nats/protobuf.go` (nats/protobuf). A pipeline naming anything else is
 * accepted by the API with 201 but refused by the worker at construction
 * ("references unregistered transformer type", engine/factory.go), which
 * leaves the pipeline reporting `error`.
 *
 * This list previously offered "custom", which is not registered anywhere, and
 * omitted "nats/protobuf", which is the transformer that routes records to
 * daya-core. Selecting the former produced a dead pipeline; the latter could
 * only be configured by hand-editing the raw JSON.
 *
 * There is no discovery endpoint, so this stays hand-maintained — see
 * docs/todos/frontend_control_plane_gaps.md.
 */
export const PROCESSOR_TYPE = ["mask", "uppercase", "nats/protobuf"] as const;
export type ProcessorType = (typeof PROCESSOR_TYPE)[number];
export const PROCESSOR_TYPE_LABELS: Record<ProcessorType, string> = {
	mask: "Mask",
	uppercase: "Uppercase",
	"nats/protobuf": "NATS / Protobuf (daya-core)",
};
export const PROCESSOR_TYPE_BUILTIN: ReadonlySet<ProcessorType> = new Set([
	"mask",
	"uppercase",
] as const);

export const OPERATION_TYPE = [
	"insert",
	"update",
	"delete",
	"snapshot",
	"schema_change",
	"schema_change_ack",
] as const;
export type OperationType = (typeof OPERATION_TYPE)[number];
export const OPERATION_TYPE_LABELS: Record<OperationType, string> = {
	insert: "Insert",
	update: "Update",
	delete: "Delete",
	snapshot: "Snapshot",
	schema_change: "Schema Change",
	schema_change_ack: "Schema Change Ack",
};
export const OPERATION_TYPE_GROUPS = {
	data: ["insert", "update", "delete", "snapshot"],
	schema: ["schema_change", "schema_change_ack"],
} as const satisfies Record<string, readonly OperationType[]>;

export const WORKER_STATUS = ["healthy", "unhealthy", "busy"] as const;
export type WorkerStatus = (typeof WORKER_STATUS)[number];
export const WORKER_STATUS_LABELS: Record<WorkerStatus, string> = {
	healthy: "Healthy",
	unhealthy: "Unhealthy",
	busy: "Busy",
};

export const SSE_MESSAGE_TYPE = [
	"metrics",
	"heartbeat",
	"error",
	"status_change",
] as const;
export type SSEMessageType = (typeof SSE_MESSAGE_TYPE)[number];
export const SSE_MESSAGE_TYPE_LABELS: Record<SSEMessageType, string> = {
	metrics: "Metrics",
	heartbeat: "Heartbeat",
	error: "Error",
	status_change: "Status Change",
};

export const PIPELINE_STATUS_FILTER = [
	"Healthy",
	"Transitioning",
	"Error",
] as const;
export type PipelineStatusFilter = (typeof PIPELINE_STATUS_FILTER)[number];

export const TABLE_STATE = [
	"Snapshotting",
	"Draining",
	"CDC",
	"Failed",
] as const;
export type TableState = (typeof TABLE_STATE)[number];

export const SCHEMA_STATUS = [
	"stable",
	"frozen",
	"draining",
	"type_conflict",
	"suspended",
] as const;
export type SchemaStatus = (typeof SCHEMA_STATUS)[number];

export const PER_TABLE_STATUS = ["ACTIVE", "CIRCUIT_OPEN"] as const;
export type PerTableStatus = (typeof PER_TABLE_STATUS)[number];
