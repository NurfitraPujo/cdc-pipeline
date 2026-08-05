import { camelToSnake, snakeToCamel } from "./mappers";
import type { components } from "./schema";
import { apiClient, unwrap } from "./schema-client";

type WirePipeline = components["schemas"]["PipelineConfig"];
type WirePipelineList = components["schemas"]["PipelineListResponse"];
type WirePipelineStatus = components["schemas"]["PipelineStatusResponse"];
type WirePipelineListItem = components["schemas"]["PipelineListItem"];
type WirePausePipelineResponse = components["schemas"]["PausePipelineResponse"];
type WirePausePipelineProjectionResponse =
	components["schemas"]["PausePipelineProjectionResponse"];
type WirePipelineLifecycleRecord =
	components["schemas"]["PipelineLifecycleRecord"];

/**
 * Lifecycle state as reported by the server (plan section 4.2). Mirrors
 * `PipelineListItem.lifecycle_state` / `PipelineLifecycleRecord.state`.
 */
export type LifecycleState =
	| "Running"
	| "Pausing"
	| "Paused"
	| "Stopping"
	| "Stopped"
	| "NeedsResnapshot"
	| "Snapshotting"
	| "Resuming"
	| "Failed"
	| "Transitioning";

/** Best-effort delete-reconciliation sub-status (plan section 4.2/4.4 invariant 5). */
export type ReconciliationStatus = "" | "idle" | "running" | "stale";

/**
 * Retry settings, camelCase.
 *
 * These deliberately do NOT reuse `components["schemas"]["RetryConfig"]`.
 * Every response passes through `snakeToCamel`, so the runtime keys are
 * camelCase -- but the wire type declares them snake_case. Typing the domain
 * model with the wire type made `p.retry.max_retries` compile while always
 * evaluating to `undefined`, which is how the edit form came to silently wipe
 * retry config and processor operation types on save.
 */
export interface PipelineRetryConfig {
	maxRetries?: number;
	/** Go duration string, e.g. "1s". */
	initialInterval?: string;
	/** Go duration string, e.g. "30s". */
	maxInterval?: string;
	enableDlq?: boolean;
}

export interface PipelineProcessorConfig {
	name: string;
	type: string;
	/** Opaque, processor-specific. Keys are never case-converted. */
	options?: Record<string, unknown>;
	operationTypes?: string[];
}

export interface Pipeline {
	id: string;
	name: string;
	sources: string[];
	sinks: string[];
	processors?: PipelineProcessorConfig[];
	tables: string[];
	batchSize?: number;
	/** Go duration string, e.g. "10s". */
	batchWait?: string;
	retry?: PipelineRetryConfig;
	status?: string;
	/** What the pipeline is actually doing right now (plan section 4.1). */
	lifecycleState?: LifecycleState;
	/** Only meaningful while lifecycleState is "Running"; empty otherwise. */
	health?: "healthy" | "error" | "";
	/** Set only while Pausing/Paused with a TTL (plan invariant 3). */
	pausedUntil?: string | null;
	/** Operator-facing note, e.g. why a WAL guard escalated a pause to a stop. */
	reason?: string | null;
	/** Best-effort delete-reconciliation sub-status; MUST stay visible when "stale". */
	reconciliation?: ReconciliationStatus;
}

export interface ListPipelinesParams {
	search?: string;
	/** One of the PIPELINE_STATUS values: Healthy | Transitioning | Error. */
	status?: string;
	page?: number;
	limit?: number;
}

export interface PipelineListResponse {
	pipelines: Pipeline[];
	total: number;
	page: number;
	limit: number;
}

export interface PipelineStatus {
	pipelineId: string;
	status: Record<string, unknown>;
	tables: Record<string, unknown>;
	sinks: Record<string, Record<string, unknown>>;
}

export interface CreatePipelineRequest {
	id?: string;
	name: string;
	sources: string[];
	sinks: string[];
	processors?: PipelineProcessorConfig[];
	tables: string[];
	batchSize?: number;
	batchWait?: string;
	retry?: PipelineRetryConfig;
}

export type UpdatePipelineRequest = Partial<CreatePipelineRequest>;

/** The system-owned lifecycle record returned by pause/start/stop. */
export interface PipelineLifecycleRecord {
	state: LifecycleState;
	pausedUntil?: string | null;
	reconciliation?: ReconciliationStatus;
	reason?: string;
	updatedAt: string;
}

export interface PausePipelineResult extends PipelineLifecycleRecord {
	/**
	 * Present only when the projected time-to-breach (plan section 5) is
	 * shorter than this pause's effective TTL -- the pause is projected to
	 * hit the source's WAL budget and force an escalation to Stopping
	 * before it would otherwise expire.
	 */
	warning?: string;
}

/** GET /pipelines/{id}/pause-projection's body -- see PauseDialog. */
export interface PausePipelineProjectionResult {
	/**
	 * Present only when the projected time-to-breach (plan section 5) is
	 * shorter than the candidate `ttl` being considered. Read-only: unlike
	 * `PausePipelineResult.warning`, this is computed BEFORE any pause is
	 * committed, so the dialog can warn while the operator is still
	 * choosing a duration.
	 */
	warning?: string;
}

export const pipelinesApi = {
	async list(params: ListPipelinesParams = {}): Promise<PipelineListResponse> {
		// The server supports search/status/page/limit; previously none were
		// forwarded, so the search box, the status filter and the pager were
		// all inert.
		const query: Record<string, string | number> = {};
		if (params.search) query.search = params.search;
		if (params.status) query.status = params.status;
		if (params.page !== undefined) query.page = params.page;
		if (params.limit !== undefined) query.limit = params.limit;

		const result = await apiClient.GET("/pipelines", { params: { query } });
		return snakeToCamel<PipelineListResponse>(unwrap<WirePipelineList>(result));
	},

	async get(id: string): Promise<Pipeline> {
		const result = await apiClient.GET("/pipelines/{id}", {
			params: { path: { id } },
		});
		return snakeToCamel<Pipeline>(unwrap<WirePipelineListItem>(result));
	},

	async getStatus(id: string): Promise<PipelineStatus> {
		const result = await apiClient.GET("/pipelines/{id}/status", {
			params: { path: { id } },
		});
		return snakeToCamel<PipelineStatus>(unwrap<WirePipelineStatus>(result));
	},

	async create(data: CreatePipelineRequest): Promise<Pipeline> {
		const body = camelToSnake<WirePipeline>(data);
		const result = await apiClient.POST("/pipelines", { body });
		return snakeToCamel<Pipeline>(unwrap<WirePipeline>(result));
	},

	async update(id: string, data: UpdatePipelineRequest): Promise<Pipeline> {
		const body = camelToSnake<WirePipeline>(data);
		const result = await apiClient.PUT("/pipelines/{id}", {
			params: { path: { id } },
			body,
		});
		return snakeToCamel<Pipeline>(unwrap<WirePipeline>(result));
	},

	async delete(id: string): Promise<void> {
		const result = await apiClient.DELETE("/pipelines/{id}", {
			params: { path: { id } },
		});
		unwrap<undefined>(result);
	},

	async restart(id: string): Promise<void> {
		const result = await apiClient.POST("/pipelines/{id}/restart", {
			params: { path: { id } },
		});
		unwrap<undefined>(result);
	},

	/**
	 * Pause a running pipeline while retaining its replication slot (plan
	 * section 4.2). `ttl` is a Go duration string (e.g. "2h"), capped at a
	 * 4h ceiling; omitting it still bounds the pause at that same ceiling
	 * server-side -- it does not mean "pause indefinitely". Extending an
	 * existing pause is just calling this again with a new `ttl`.
	 */
	async pause(id: string, ttl?: string): Promise<PausePipelineResult> {
		const result = await apiClient.POST("/pipelines/{id}/pause", {
			params: { path: { id } },
			body: ttl ? { ttl } : undefined,
		});
		return snakeToCamel<PausePipelineResult>(
			unwrap<WirePausePipelineResponse>(result),
		);
	},

	/**
	 * Read-only projection of whether pausing for `ttl` right now would hit
	 * the WAL budget guard, WITHOUT committing a pause (plan section 5:
	 * "project the breach and show it before the pause is confirmed").
	 * `PauseDialog` calls this as the operator adjusts the TTL, then calls
	 * `pause()` itself only on "Confirm pause".
	 */
	async pauseProjection(
		id: string,
		ttl?: string,
	): Promise<PausePipelineProjectionResult> {
		const result = await apiClient.GET("/pipelines/{id}/pause-projection", {
			params: { path: { id }, query: ttl ? { ttl } : undefined },
		});
		return snakeToCamel<PausePipelineProjectionResult>(
			unwrap<WirePausePipelineProjectionResponse>(result),
		);
	},

	/** Resume a paused pipeline, or advance a stopped/failed one's lifecycle state. */
	async start(id: string): Promise<PipelineLifecycleRecord> {
		const result = await apiClient.POST("/pipelines/{id}/start", {
			params: { path: { id } },
		});
		return snakeToCamel<PipelineLifecycleRecord>(
			unwrap<WirePipelineLifecycleRecord>(result),
		);
	},

	/** Stop a running or paused pipeline, dropping its replication slot. */
	async stop(id: string): Promise<PipelineLifecycleRecord> {
		const result = await apiClient.POST("/pipelines/{id}/stop", {
			params: { path: { id } },
		});
		return snakeToCamel<PipelineLifecycleRecord>(
			unwrap<WirePipelineLifecycleRecord>(result),
		);
	},
};
