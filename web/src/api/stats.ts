import { snakeToCamel } from "./mappers";
import { apiClient, unwrap } from "./schema-client";

export interface StatsSummary {
	totalPipelines: number;
	healthyCount: number;
	errorCount: number;
	transitioningCount: number;
	totalRowsSynchronized: number;
	avgLagMs: number;
}

export interface HistoryPoint {
	timestamp: string;
	value: number;
}

export const statsApi = {
	async getSummary(): Promise<StatsSummary> {
		const result = await apiClient.GET("/stats/summary");
		return snakeToCamel<StatsSummary>(unwrap(result));
	},

	async getHistory(): Promise<HistoryPoint[]> {
		const result = await apiClient.GET("/stats/history");
		return snakeToCamel<HistoryPoint[]>(unwrap(result));
	},
};
