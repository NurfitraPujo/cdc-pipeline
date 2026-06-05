import { snakeToCamel } from "./mappers";
import type { components } from "./schema";
import { apiClient, unwrap } from "./schema-client";

type WireGlobalConfig = components["schemas"]["GlobalConfig"];

export interface RetryConfig {
	maxRetries: number;
	initialBackoff: string;
	maxBackoff: string;
}

export interface GlobalConfig {
	batchSize: number;
	batchWait: string;
	retry: RetryConfig;
}

export const globalConfigApi = {
	async get(): Promise<GlobalConfig> {
		const result = await apiClient.GET("/global");
		return snakeToCamel<GlobalConfig>(unwrap<WireGlobalConfig>(result));
	},

	async update(input: GlobalConfig): Promise<GlobalConfig> {
		const body = input as unknown as WireGlobalConfig;
		const result = await apiClient.PUT("/global", { body });
		return snakeToCamel<GlobalConfig>(unwrap<WireGlobalConfig>(result));
	},
};
