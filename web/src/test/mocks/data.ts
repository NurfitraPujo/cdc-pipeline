import type { Pipeline } from "@/api/pipelines";
import type { Sink } from "@/api/sinks";
import type { Source } from "@/api/sources";
import type { StatsSummary } from "@/api/stats";
import type { LoginResponse, TableStats } from "@/api/types";

export const createMockToken = (): string =>
	"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkFkbWluIiwiaWF0IjoxNTE2MjM5MDIyfQ.test";

export const mockLoginResponse: LoginResponse = {
	token: createMockToken(),
};

export const mockStatsSummary: StatsSummary = {
	totalPipelines: 5,
	healthyCount: 3,
	errorCount: 1,
	transitioningCount: 1,
	totalRowsSynchronized: 1500000,
	avgLagMs: 150,
};

export const mockSources: Source[] = [
	{
		id: "postgres-1",
		type: "postgres",
		host: "localhost",
		port: 5432,
		database: "prod",
		user: "cdc_user",
		pass: "secret",
		tables: ["users", "orders"],
	},
];

export const mockSinks: Sink[] = [
	{
		id: "databend-1",
		type: "databend",
		dsn: "databend://localhost:8000/analytics",
	},
];

export const mockTableStats: TableStats[] = [
	{
		tableName: "users",
		status: "active",
		rps: 12.5,
		totalSynced: 1600,
		errorCount: 0,
		lastSourceTs: new Date().toISOString(),
		lastProcessedTs: new Date().toISOString(),
		lagMs: 120,
		updatedAt: new Date().toISOString(),
	},
	{
		tableName: "orders",
		status: "active",
		rps: 4.2,
		totalSynced: 6200,
		errorCount: 0,
		lastSourceTs: new Date().toISOString(),
		lastProcessedTs: new Date().toISOString(),
		lagMs: 80,
		updatedAt: new Date().toISOString(),
	},
];

export const mockPipelines: Pipeline[] = [
	{
		id: "pipeline-1",
		name: "Users Sync",
		sources: ["postgres-1"],
		sinks: ["databend-1"],
		tables: ["users", "orders"],
	},
	{
		id: "pipeline-2",
		name: "Orders Sync",
		sources: ["postgres-1"],
		sinks: ["databend-1"],
		tables: ["orders"],
	},
];

export const createMockPipeline = (
	overrides?: Partial<Pipeline>,
): Pipeline => ({
	id: `pipeline-${Date.now()}`,
	name: "New Pipeline",
	sources: ["postgres-1"],
	sinks: ["databend-1"],
	tables: ["users"],
	...overrides,
});
