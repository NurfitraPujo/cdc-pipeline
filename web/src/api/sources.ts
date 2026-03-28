import { apiClient } from "./client";
import type { Source } from "./types";

export interface TableResponse {
	tables: string[];
}

export const sourcesApi = {
	async list(): Promise<Source[]> {
		return apiClient.get<Source[]>("/sources");
	},

	async getTables(id: string): Promise<string[]> {
		const response = await apiClient.get<TableResponse>(
			`/sources/${id}/tables`,
		);
		return response.tables;
	},
};

export default sourcesApi;
