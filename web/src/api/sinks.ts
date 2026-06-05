import { camelToSnake, snakeToCamel } from "./mappers";
import type { components } from "./schema";
import { apiClient, unwrap } from "./schema-client";

type WireSink = components["schemas"]["SinkConfig"];
type WireSinkList = components["schemas"]["SinkListResponse"];

export type SinkType =
	| "postgres"
	| "mysql"
	| "elasticsearch"
	| "kafka"
	| "webhook"
	| "databend"
	| "postgres_debug"
	| "debug";

export interface Sink {
	id: string;
	type: SinkType;
	dsn?: string;
	host?: string;
	port?: number;
	database?: string;
	username?: string;
	password?: string;
	ssl?: boolean;
	index?: string;
	topic?: string;
	url?: string;
	headers?: Record<string, string>;
}

export type CreateSinkRequest = Omit<Sink, "id"> & { id?: string };
export type UpdateSinkRequest = Partial<Sink>;

export const sinksApi = {
	async list(): Promise<Sink[]> {
		const result = await apiClient.GET("/sinks");
		const data = unwrap<WireSinkList>(result);
		return snakeToCamel<Sink[]>(data.sinks ?? []);
	},

	async get(id: string): Promise<Sink> {
		const result = await apiClient.GET("/sinks/{id}", {
			params: { path: { id } },
		});
		return snakeToCamel<Sink>(unwrap<WireSink>(result));
	},

	async create(data: CreateSinkRequest): Promise<Sink> {
		const body = camelToSnake<WireSink>(data);
		const result = await apiClient.POST("/sinks", { body });
		return snakeToCamel<Sink>(unwrap<WireSink>(result));
	},

	async update(id: string, data: UpdateSinkRequest): Promise<Sink> {
		const body = camelToSnake<WireSink>(data);
		const result = await apiClient.PUT("/sinks/{id}", {
			params: { path: { id } },
			body,
		});
		return snakeToCamel<Sink>(unwrap<WireSink>(result));
	},

	async delete(id: string): Promise<void> {
		const result = await apiClient.DELETE("/sinks/{id}", {
			params: { path: { id } },
		});
		unwrap<undefined>(result);
	},
};
