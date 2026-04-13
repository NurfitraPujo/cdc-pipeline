export interface User {
	id: string;
	email: string;
	name: string;
	role: "admin" | "user";
	createdAt: string;
	updatedAt: string;
}

export interface LoginRequest {
	username: string;
	password: string;
}

export interface LoginResponse {
	token: string;
}

export interface StatsSummary {
	total_pipelines: number;
	healthy_count: number;
	error_count: number;
	transitioning_count: number;
	total_rows_synchronized: number;
	avg_lag_ms: number;
}

export interface Pipeline {
	id: string;
	name: string;
	description?: string;
	status: PipelineStatus;
	source: Source;
	sink: Sink;
	processorConfig: ProcessorConfig;
	globalConfig: PipelineGlobalConfig;
	createdAt: string;
	updatedAt: string;
	lastRunAt?: string;
}

export type PipelineStatus = "running" | "stopped" | "error" | "paused";

export interface ProcessorConfig {
	batchSize: number;
	flushIntervalMs: number;
	maxRetries: number;
	retryDelayMs: number;
	deadLetterQueueEnabled: boolean;
	transformations?: Transformation[];
}

export interface Transformation {
	id: string;
	type: "filter" | "map" | "enrich";
	config: Record<string, unknown>;
	order: number;
}

export interface TableStats {
	tableName: string;
	status: string;
	rps: number;
	total_synced: number;
	error_count: number;
	last_source_ts: string;
	last_processed_ts: string;
	lag_ms: number;
	updated_at: string;
}

export interface PipelineStatusResponse {
	pipeline_id: string;
	status: Record<string, any>;
	tables: Record<string, TableStats>;
	sinks: Record<string, Record<string, TableStats>>;
}

export interface SSEMessage {
	key: string;
	data: any;
	sink_id?: string;
	is_debug?: boolean;
}

export interface Source {
	id: string;
	type: "postgres" | "mysql" | "mongodb";
	name: string;
	connection: {
		host: string;
		port: number;
		database: string;
		username: string;
		password?: string;
		ssl?: boolean;
	};
	tables: string[];
}

export interface Sink {
	id: string;
	type: "postgres" | "mysql" | "elasticsearch" | "kafka" | "webhook" | "databend";
	name: string;
	connection: {
		host: string;
		port?: number;
		index?: string;
		topic?: string;
		url?: string;
		headers?: Record<string, string>;
		database?: string;
		username?: string;
		password?: string;
		ssl?: boolean;
	};
}

export interface PipelineGlobalConfig {
	batchSize: number;
	batchWait: string;
	retry: {
		maxRetries: number;
		initialBackoff: string;
		maxBackoff: string;
	};
}

export interface GlobalConfig {
	maxConcurrentPipelines: number;
	defaultBatchSize: number;
	defaultFlushIntervalMs: number;
	healthCheckIntervalMs: number;
	metricsRetentionDays: number;
}

export interface WorkerHeartbeat {
	workerId: string;
	pipelineId: string;
	status: "healthy" | "unhealthy" | "busy";
	lastHeartbeatAt: string;
	eventsProcessed: number;
	currentLagMs: number;
	memoryUsageMb: number;
	cpuUsagePercent: number;
}

export interface SSEMetrics {
	type: "metrics" | "heartbeat" | "error" | "status_change";
	timestamp: string;
	data: {
		pipelineId?: string;
		eventsPerSecond?: number;
		lagMs?: number;
		errorCount?: number;
		message?: string;
		status?: PipelineStatus;
	};
}
