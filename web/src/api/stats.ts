import { apiClient } from "./client";
import type { StatsSummary } from "./types";

export const statsApi = {
	async getSummary(): Promise<StatsSummary> {
		return apiClient.get<StatsSummary>("/stats/summary");
	},
};
