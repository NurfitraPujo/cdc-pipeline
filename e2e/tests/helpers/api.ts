import type { APIRequestContext } from "@playwright/test";
import { request } from "@playwright/test";

/**
 * The API ORIGIN, not the `/api/v1` base path.
 *
 * Playwright resolves request paths with `new URL(path, baseURL)`, so a
 * leading-slash path like "/pipelines" is resolved against the origin and any
 * path component of baseURL is discarded. Folding "/api/v1" into baseURL
 * therefore silently produced requests to "/pipelines" and a 404. Call sites
 * spell the prefix out instead.
 */
export const API_ORIGIN =
	process.env.E2E_API_ORIGIN ??
	`http://localhost:${process.env.E2E_API_PORT ?? 8090}`;

export const ADMIN_USER = "admin";
export const ADMIN_PASS = "admin";

/**
 * A direct API client, authenticated as admin.
 *
 * Specs use this for arrange/assert/cleanup so a UI test only has to drive the
 * behaviour it is actually about. Previously there was no such helper: tests
 * created pipelines through the UI and never removed them, so every run leaked
 * rows into the shared backend.
 */
export async function createApiContext(): Promise<APIRequestContext> {
	const anon = await request.newContext({ baseURL: API_ORIGIN });
	const res = await anon.post("/api/v1/login", {
		data: { username: ADMIN_USER, password: ADMIN_PASS },
	});

	if (!res.ok()) {
		throw new Error(
			`e2e: admin login failed (${res.status()}): ${await res.text()}`,
		);
	}

	const { token } = (await res.json()) as { token: string };
	await anon.dispose();

	return request.newContext({
		baseURL: API_ORIGIN,
		extraHTTPHeaders: { Authorization: `Bearer ${token}` },
	});
}

/** Unique-per-run identifier, safe for the API's `^[a-zA-Z0-9_-]+$` rule. */
export function uniqueId(prefix: string): string {
	return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

export interface SourceOptions {
	id: string;
	host?: string;
	port?: number;
	database?: string;
	user?: string;
	pass?: string;
}

/**
 * Creates a source. It does not need to point at a reachable database: the
 * control plane stores the config and only connects when explicitly asked to
 * (POST /sources/test) or when a worker starts.
 */
export async function createSource(
	api: APIRequestContext,
	opts: SourceOptions,
): Promise<Record<string, unknown>> {
	const res = await api.post("/api/v1/sources", {
		data: {
			id: opts.id,
			type: "postgres",
			host: opts.host ?? "localhost",
			port: opts.port ?? 5432,
			user: opts.user ?? "postgres",
			pass: opts.pass ?? "postgres",
			database: opts.database ?? "e2e",
			slot_name: `${opts.id}_slot`,
			publication_name: `${opts.id}_pub`,
			batch_wait: "5s",
			discovery_interval: "30s",
			snapshot_interval: "1s",
		},
	});

	if (!res.ok()) {
		throw new Error(
			`e2e: createSource(${opts.id}) failed (${res.status()}): ${await res.text()}`,
		);
	}
	return res.json();
}

export async function createSink(
	api: APIRequestContext,
	id: string,
	dsn = "databend://root:@localhost:8000/default?sslmode=disable",
): Promise<Record<string, unknown>> {
	const res = await api.post("/api/v1/sinks", {
		data: { id, type: "databend", dsn, max_ack_pending: 100 },
	});

	if (!res.ok()) {
		throw new Error(
			`e2e: createSink(${id}) failed (${res.status()}): ${await res.text()}`,
		);
	}
	return res.json();
}

export interface PipelineOptions {
	id: string;
	name?: string;
	sources: string[];
	sinks: string[];
	tables?: string[];
	batchSize?: number;
	batchWait?: string;
	processors?: unknown[];
	retry?: Record<string, unknown>;
}

export async function createPipeline(
	api: APIRequestContext,
	opts: PipelineOptions,
): Promise<Record<string, unknown>> {
	const res = await api.post("/api/v1/pipelines", {
		data: {
			id: opts.id,
			name: opts.name ?? opts.id,
			sources: opts.sources,
			sinks: opts.sinks,
			tables: opts.tables ?? ["public.orders"],
			batch_size: opts.batchSize ?? 100,
			batch_wait: opts.batchWait ?? "5s",
			...(opts.processors ? { processors: opts.processors } : {}),
			...(opts.retry ? { retry: opts.retry } : {}),
		},
	});

	if (!res.ok()) {
		throw new Error(
			`e2e: createPipeline(${opts.id}) failed (${res.status()}): ${await res.text()}`,
		);
	}
	return res.json();
}

/** Best-effort teardown; a already-deleted resource is not an error. */
export async function cleanup(
	api: APIRequestContext,
	resources: { pipelines?: string[]; sources?: string[]; sinks?: string[] },
): Promise<void> {
	// Pipelines first -- they reference sources and sinks.
	for (const id of resources.pipelines ?? []) {
		await api.delete(`/api/v1/pipelines/${id}`).catch(() => undefined);
	}
	for (const id of resources.sources ?? []) {
		await api.delete(`/api/v1/sources/${id}`).catch(() => undefined);
	}
	for (const id of resources.sinks ?? []) {
		await api.delete(`/api/v1/sinks/${id}`).catch(() => undefined);
	}
}
