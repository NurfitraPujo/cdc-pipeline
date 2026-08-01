import { camelToSnake, snakeToCamel } from "./mappers";
import type { components } from "./schema";
import { apiClient, unwrap } from "./schema-client";

type WireSource = components["schemas"]["SourceConfig"];
type WireSourceList = components["schemas"]["SourceListResponse"];
type WireSourceSchema = components["schemas"]["SourceSchemaResponse"];
type WireSourceTables = components["schemas"]["SourceTablesResponse"];
type WireTestConnectionResponse =
	components["schemas"]["TestConnectionResponse"];

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
	slotName?: string;
	publicationName?: string;
	batchSize?: number;
	batchWait?: string;
	discoveryInterval?: string;
	snapshotChunkSize?: number;
	snapshotInterval?: string;
	schemas?: string[];
}

export interface SourceSchema {
	availableSchemas: string[];
	discoveryStatus: string;
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

	// getTables returns the config-shaped table identity: bare ("orders")
	// for tables in the "public" schema, "schema.table" (dot-qualified)
	// otherwise. This is deliberately NOT the backend's internal KeyToken()
	// id (which uses "=" as separator) -- callers feed this string straight
	// into PipelineConfig.Tables, and only the dot form is accepted by
	// protocol.ParseTableRef on the backend. See MULTI_SCHEMA_PLAN.md §2.1-2.3
	// and the Stage 3 caution about attempt 1's qualified-id regression.
	async getTables(id: string): Promise<string[]> {
		const result = await apiClient.GET("/sources/{id}/tables", {
			params: { path: { id } },
		});
		const data = unwrap<WireSourceTables>(result);
		return (data.tables ?? []).map((t) => {
			const name = t.name ?? "";
			const schema = t.schema ?? "public";
			return schema === "public" || schema === "" ? name : `${schema}.${name}`;
		});
	},

	async testConnection(
		data: CreateSourceRequest,
	): Promise<WireTestConnectionResponse> {
		const body = camelToSnake<WireSource>(data);
		const result = await apiClient.POST("/sources/test", { body });
		return snakeToCamel<WireTestConnectionResponse>(
			unwrap<WireTestConnectionResponse>(result),
		);
	},
};
