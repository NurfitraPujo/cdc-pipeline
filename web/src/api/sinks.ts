import { apiClient } from "./client";
import type { Sink } from "./types";

export const sinksApi = {
	async list(): Promise<Sink[]> {
		return apiClient.get<Sink[]>("/sinks");
	},
};

export default sinksApi;
