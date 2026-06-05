import type { components } from "./schema";

export type { components, operations, paths } from "./schema";
export type SchemaPipelineConfig = components["schemas"]["PipelineConfig"];
export type SchemaSourceConfig = components["schemas"]["SourceConfig"];
export type SchemaSinkConfig = components["schemas"]["SinkConfig"];
export type SchemaGlobalConfig = components["schemas"]["GlobalConfig"];
export type SchemaProcessorConfig = components["schemas"]["ProcessorConfig"];
export type SchemaRetryConfig = components["schemas"]["RetryConfig"];
export type SchemaErrorResponse = components["schemas"]["ErrorResponse"];
export type SchemaTableStats = components["schemas"]["TableStats"];

export interface LoginRequest {
	username: string;
	password: string;
}

export interface LoginResponse {
	token: string;
}

export type PipelineStatus = "running" | "stopped" | "error" | "paused";

export interface TableStats {
	tableName: string;
	status: string;
	rps: number;
	totalSynced: number;
	errorCount: number;
	lastSourceTs: string;
	lastProcessedTs: string;
	lagMs: number;
	updatedAt: string;
}

export interface PipelineStatusResponse {
	pipelineId: string;
	status: Record<string, unknown>;
	tables: Record<string, TableStats>;
	sinks: Record<string, Record<string, TableStats>>;
}

export interface SSEMessage {
	key: string;
	data: unknown;
	sinkId?: string;
	isDebug?: boolean;
}
