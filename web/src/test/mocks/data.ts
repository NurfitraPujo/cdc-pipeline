import type {
	LoginResponse,
	Pipeline,
	Source,
	Sink,
	StatsSummary,
	TableStats,
} from "@/api/types";

export const createMockToken = (): string =>
	"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkFkbWluIiwiaWF0IjoxNTE2MjM5MDIyfQ.test";

export const mockLoginResponse: LoginResponse = {
	token: createMockToken(),
};

export const mockStatsSummary: StatsSummary = {
	total_pipelines: 5,
	healthy_count: 3,
	error_count: 1,
	transitioning_count: 1,
	total_rows_synchronized: 1500000,
	avg_lag_ms: 150,
};

export const mockSources: Source[] = [
	{
		id: "postgres-1",
		type: "postgres",
		name: "Production Postgres",
		connection: {
			host: "localhost",
			port: 5432,
			database: "prod",
			username: "cdc_user",
			ssl: true,
		},
		tables: ["users", "orders"],
	},
	{
		id: "mysql-1",
		type: "mysql",
		name: "Legacy MySQL",
		connection: {
			host: "localhost",
			port: 3306,
			database: "legacy",
			username: "cdc_user",
			ssl: false,
		},
		tables: ["customers", "products"],
	},
];

export const mockSinks: Sink[] = [
	{
		id: "databend-1",
		type: "databend",
		name: "Analytics Warehouse",
		connection: {
			host: "localhost",
			port: 8000,
			database: "analytics",
			username: "admin",
			ssl: true,
		},
	},
];

export const mockTableStats: TableStats[] = [
	{
		tableName: "users",
		eventsInserted: 1000,
		eventsUpdated: 500,
		eventsDeleted: 100,
		eventsTotal: 1600,
		lagMs: 120,
		lastEventAt: new Date().toISOString(),
	},
	{
		tableName: "orders",
		eventsInserted: 5000,
		eventsUpdated: 1000,
		eventsDeleted: 200,
		eventsTotal: 6200,
		lagMs: 80,
		lastEventAt: new Date().toISOString(),
	},
];

export const mockPipelines: Pipeline[] = [
	{
		id: "pipeline-1",
		name: "Users Sync",
		description: "Sync users table to analytics",
		status: "running",
		source: mockSources[0],
		sink: mockSinks[0],
		processorConfig: {
			batchSize: 1000,
			flushIntervalMs: 5000,
			maxRetries: 3,
			retryDelayMs: 1000,
			deadLetterQueueEnabled: true,
		},
		globalConfig: {
			batchSize: 1000,
			batchWait: "5s",
			retry: {
				maxRetries: 3,
				initialBackoff: "1s",
				maxBackoff: "30s",
			},
		},
		createdAt: new Date().toISOString(),
		updatedAt: new Date().toISOString(),
		lastRunAt: new Date().toISOString(),
	},
	{
		id: "pipeline-2",
		name: "Orders Sync",
		description: "Sync orders table to analytics",
		status: "error",
		source: mockSources[0],
		sink: mockSinks[0],
		processorConfig: {
			batchSize: 2000,
			flushIntervalMs: 10000,
			maxRetries: 3,
			retryDelayMs: 1000,
			deadLetterQueueEnabled: true,
		},
		globalConfig: {
			batchSize: 2000,
			batchWait: "10s",
			retry: {
				maxRetries: 3,
				initialBackoff: "1s",
				maxBackoff: "30s",
			},
		},
		createdAt: new Date().toISOString(),
		updatedAt: new Date().toISOString(),
	},
];

export const createMockPipeline = (overrides?: Partial<Pipeline>): Pipeline => ({
	id: `pipeline-${Date.now()}`,
	name: "New Pipeline",
	status: "stopped",
	source: mockSources[0],
	sink: mockSinks[0],
	processorConfig: {
		batchSize: 1000,
		flushIntervalMs: 5000,
		maxRetries: 3,
		retryDelayMs: 1000,
		deadLetterQueueEnabled: true,
	},
	globalConfig: {
		batchSize: 1000,
		batchWait: "5s",
		retry: {
			maxRetries: 3,
			initialBackoff: "1s",
			maxBackoff: "30s",
		},
	},
	createdAt: new Date().toISOString(),
	updatedAt: new Date().toISOString(),
	...overrides,
});
