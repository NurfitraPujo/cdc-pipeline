export const PIPELINE_STATUS = [
	"running",
	"stopped",
	"error",
	"paused",
] as const;
export type PipelineStatus = (typeof PIPELINE_STATUS)[number];
export const PIPELINE_STATUS_LABELS: Record<PipelineStatus, string> = {
	running: "Running",
	stopped: "Stopped",
	error: "Error",
	paused: "Paused",
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

export const PROCESSOR_TYPE = [
	"filter",
	"map",
	"enrich",
	"mask",
	"custom",
] as const;
export type ProcessorType = (typeof PROCESSOR_TYPE)[number];
export const PROCESSOR_TYPE_LABELS: Record<ProcessorType, string> = {
	filter: "Filter",
	map: "Map",
	enrich: "Enrich",
	mask: "Mask",
	custom: "Custom",
};

export const OPERATION_TYPE = ["insert", "update", "delete", "ddl"] as const;
export type OperationType = (typeof OPERATION_TYPE)[number];
export const OPERATION_TYPE_LABELS: Record<OperationType, string> = {
	insert: "Insert",
	update: "Update",
	delete: "Delete",
	ddl: "DDL",
};

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
