import { camelToSnake, snakeToCamel } from "./mappers";
import type { components } from "./schema";
import { apiClient, unwrap } from "./schema-client";

type WirePipeline = components["schemas"]["PipelineConfig"];
type WirePipelineList = components["schemas"]["PipelineListResponse"];
type WirePipelineStatus = components["schemas"]["PipelineStatusResponse"];

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
		return snakeToCamel<Pipeline>(unwrap<WirePipeline>(result));
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
};
