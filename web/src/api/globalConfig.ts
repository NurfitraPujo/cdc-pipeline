import { camelToSnake, snakeToCamel } from "./mappers";
import type { components } from "./schema";
import { apiClient, unwrap } from "./schema-client";

type WireGlobalConfig = components["schemas"]["GlobalConfig"];

export interface RetryConfig {
	maxRetries: number;
	/** Go duration string, e.g. "1s". Wire name: initial_interval. */
	initialInterval: string;
	/** Go duration string, e.g. "30s". Wire name: max_interval. */
	maxInterval: string;
	enableDlq: boolean;
}

export interface GlobalConfig {
	batchSize: number;
	batchWait: string;
	retry: RetryConfig;
	drainTimeout: string;
	shutdownTimeout: string;
	stabilizationDelay: string;
	crashRecoveryDelay: string;
	globalReloadDelay: string;
}

export const globalConfigApi = {
	async get(): Promise<GlobalConfig> {
		const result = await apiClient.GET("/global");
		return snakeToCamel<GlobalConfig>(unwrap<WireGlobalConfig>(result));
	},

	async update(input: GlobalConfig): Promise<GlobalConfig> {
		// Must convert, not cast. A cast shipped camelCase keys, which
		// protocol.GlobalConfig's `json:"batch_size"` tags bound to nothing;
		// Validate() then rejected BatchSize < 1, so every save returned 400.
		const body = camelToSnake<WireGlobalConfig>(input);
		const result = await apiClient.PUT("/global", { body });
		return snakeToCamel<GlobalConfig>(unwrap<WireGlobalConfig>(result));
	},
};
