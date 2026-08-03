import { snakeToCamel } from "./mappers";
import { apiClient, unwrap } from "./schema-client";

export interface StatsSummary {
	totalPipelines: number;
	healthyCount: number;
	errorCount: number;
	transitioningCount: number;
	/**
	 * Wire name is `total_rows_synced` (protocol.StatsSummary in
	 * internal/protocol/state.go), which snakeToCamel renders as
	 * `totalRowsSynced`. This was previously declared
	 * `totalRowsSynchronized` -- matching the Go *field* name rather than
	 * its JSON tag -- so it was always undefined and the dashboard's
	 * "Rows Synchronized" card always read 0.
	 */
	totalRowsSynced: number;
	avgLagMs: number;
}

/** Mirrors protocol.HistoryPoint. */
export interface HistoryPoint {
	timestamp: string;
	rps: number;
	lagMs: number;
}

export const statsApi = {
	async getSummary(): Promise<StatsSummary> {
		const result = await apiClient.GET("/stats/summary");
		return snakeToCamel<StatsSummary>(unwrap(result));
	},

	/**
	 * Deprecated server-side: `GetStatsHistory` is hardcoded to return an
	 * empty array (internal/api/handler.go). Kept for contract completeness;
	 * the dashboard derives its throughput series by sampling `getSummary`.
	 */
	async getHistory(): Promise<HistoryPoint[]> {
		const result = await apiClient.GET("/stats/history");
		return snakeToCamel<HistoryPoint[]>(unwrap(result));
	},
};
