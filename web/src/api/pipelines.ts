import { camelToSnake, snakeToCamel } from "./mappers";
import type { components } from "./schema";
import { apiClient, unwrap } from "./schema-client";

type WirePipeline = components["schemas"]["PipelineConfig"];
type WirePipelineList = components["schemas"]["PipelineListResponse"];
type WirePipelineStatus = components["schemas"]["PipelineStatusResponse"];

export interface Pipeline {
	id: string;
	name: string;
	description?: string;
	sources: string[];
	sinks: string[];
	processors?: components["schemas"]["ProcessorConfig"][];
	tables: string[];
	batchSize?: number;
	batchWait?: string;
	retry?: components["schemas"]["RetryConfig"];
	status?: string;
}

export interface PipelineListResponse {
	pipelines: Pipeline[];
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
	processors?: components["schemas"]["ProcessorConfig"][];
	tables: string[];
	batchSize?: number;
	batchWait?: string;
	retry?: components["schemas"]["RetryConfig"];
}

export type UpdatePipelineRequest = Partial<CreatePipelineRequest>;

export const pipelinesApi = {
	async list(): Promise<PipelineListResponse> {
		const result = await apiClient.GET("/pipelines");
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
