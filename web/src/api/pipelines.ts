import { apiClient } from "./client";
import type { Pipeline } from "./types";

export interface PipelineListParams {
	search?: string;
	status?: string;
	page?: number;
	limit?: number;
}

export interface PipelineListResponse {
	pipelines: Pipeline[];
	pagination: {
		page: number;
		limit: number;
		total: number;
		total_pages: number;
	};
}

export interface CreatePipelineRequest {
	name: string;
	description?: string;
	source: {
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
	};
	sink: {
		type: "postgres" | "mysql" | "elasticsearch" | "kafka" | "webhook";
		name: string;
		connection: {
			host: string;
			port?: number;
			index?: string;
			topic?: string;
			url?: string;
			headers?: Record<string, string>;
		};
	};
	processorConfig?: {
		batchSize?: number;
		flushIntervalMs?: number;
		maxRetries?: number;
		retryDelayMs?: number;
		deadLetterQueueEnabled?: boolean;
	};
}

export interface UpdatePipelineRequest {
	name?: string;
	description?: string;
	source?: Partial<CreatePipelineRequest["source"]>;
	sink?: Partial<CreatePipelineRequest["sink"]>;
	processorConfig?: Partial<CreatePipelineRequest["processorConfig"]>;
}

function buildQueryString(params: PipelineListParams): string {
	const searchParams = new URLSearchParams();

	if (params.search) {
		searchParams.set("search", params.search);
	}
	if (params.status) {
		searchParams.set("status", params.status);
	}
	if (params.page !== undefined) {
		searchParams.set("page", params.page.toString());
	}
	if (params.limit !== undefined) {
		searchParams.set("limit", params.limit.toString());
	}

	const queryString = searchParams.toString();
	return queryString ? `?${queryString}` : "";
}

export const pipelinesApi = {
	async list(params: PipelineListParams = {}): Promise<PipelineListResponse> {
		const queryString = buildQueryString(params);
		return apiClient.get<PipelineListResponse>(`/pipelines${queryString}`);
	},

	async get(id: string): Promise<Pipeline> {
		return apiClient.get<Pipeline>(`/pipelines/${id}`);
	},

	async getStatus(id: string): Promise<any> {
		return apiClient.get<any>(`/pipelines/${id}/status`);
	},

	async create(data: CreatePipelineRequest): Promise<Pipeline> {
		return apiClient.post<Pipeline>("/pipelines", data);
	},

	async update(id: string, data: UpdatePipelineRequest): Promise<Pipeline> {
		return apiClient.put<Pipeline>(`/pipelines/${id}`, data);
	},

	async delete(id: string): Promise<void> {
		return apiClient.delete<void>(`/pipelines/${id}`);
	},

	async restart(id: string): Promise<void> {
		return apiClient.post<void>(`/pipelines/${id}/restart`);
	},
};
