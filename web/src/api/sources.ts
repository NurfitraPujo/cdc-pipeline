import { camelToSnake, snakeToCamel } from "./mappers";
import type { components } from "./schema";
import { apiClient, unwrap } from "./schema-client";

type WireSource = components["schemas"]["SourceConfig"];
type WireSourceList = components["schemas"]["SourceListResponse"];
type WireSourceSchema = components["schemas"]["SourceSchemaResponse"];
type WireSourceTables = components["schemas"]["SourceTablesResponse"];

export interface Source {
	id: string;
	type: "postgres";
	name?: string;
	host: string;
	port: number;
	database: string;
	user: string;
	pass?: string;
	tables: string[];
}

export interface SourceSchema {
	tables: Array<{
		name: string;
		columns: Array<{ name: string; type: string }>;
	}>;
}

export interface SourceTables {
	tables: Array<{ name: string; type: string; comment?: string }>;
}

export type CreateSourceRequest = Omit<Source, "id"> & { id?: string };
export type UpdateSourceRequest = Partial<Source>;

export const sourcesApi = {
	async list(): Promise<Source[]> {
		const result = await apiClient.GET("/sources");
		const data = unwrap<WireSourceList>(result);
		return snakeToCamel<Source[]>(data.sources ?? []);
	},

	async get(id: string): Promise<Source> {
		const result = await apiClient.GET("/sources/{id}", {
			params: { path: { id } },
		});
		return snakeToCamel<Source>(unwrap<WireSource>(result));
	},

	async create(data: CreateSourceRequest): Promise<Source> {
		const body = camelToSnake<WireSource>(data);
		const result = await apiClient.POST("/sources", { body });
		return snakeToCamel<Source>(unwrap<WireSource>(result));
	},

	async update(id: string, data: UpdateSourceRequest): Promise<Source> {
		const body = camelToSnake<WireSource>(data);
		const result = await apiClient.PUT("/sources/{id}", {
			params: { path: { id } },
			body,
		});
		return snakeToCamel<Source>(unwrap<WireSource>(result));
	},

	async delete(id: string): Promise<void> {
		const result = await apiClient.DELETE("/sources/{id}", {
			params: { path: { id } },
		});
		unwrap<undefined>(result);
	},

	async getSchema(id: string): Promise<SourceSchema> {
		const result = await apiClient.GET("/sources/{id}/schema", {
			params: { path: { id } },
		});
		return snakeToCamel<SourceSchema>(unwrap<WireSourceSchema>(result));
	},

	async getTables(id: string): Promise<string[]> {
		const result = await apiClient.GET("/sources/{id}/tables", {
			params: { path: { id } },
		});
		const data = unwrap<WireSourceTables>(result);
		return (data.tables ?? []).map((t) => t.name ?? "");
	},
};
