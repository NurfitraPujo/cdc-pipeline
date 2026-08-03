import type { APIRequestContext } from "@playwright/test";
import { expect, test } from "@playwright/test";
import {
	cleanup,
	createApiContext,
	createPipeline,
	createSink,
	createSource,
	uniqueId,
} from "./helpers/api";

let api: APIRequestContext;
const created = {
	pipelines: [] as string[],
	sources: [] as string[],
	sinks: [] as string[],
};

test.beforeAll(async () => {
	api = await createApiContext();
});

test.afterAll(async () => {
	await cleanup(api, created);
	await api.dispose();
});

/** Seeds a pipeline with its source and sink, registered for teardown. */
async function seedPipeline(
	prefix: string,
	extra: Record<string, unknown> = {},
) {
	const sourceId = uniqueId(`${prefix}-src`);
	const sinkId = uniqueId(`${prefix}-snk`);
	const pipelineId = uniqueId(prefix);

	await createSource(api, { id: sourceId });
	created.sources.push(sourceId);
	await createSink(api, sinkId);
	created.sinks.push(sinkId);

	await createPipeline(api, {
		id: pipelineId,
		name: `E2E ${prefix}`,
		sources: [sourceId],
		sinks: [sinkId],
		...extra,
	});
	created.pipelines.push(pipelineId);

	return { pipelineId, sourceId, sinkId };
}

test.describe("Pipeline detail", () => {
	test("GET /pipelines/{id}/status returns the four-key envelope", async () => {
		const { pipelineId } = await seedPipeline("status");

		const res = await api.get(`/api/v1/pipelines/${pipelineId}/status`);
		expect(res.status()).toBe(200);

		const body = (await res.json()) as Record<string, unknown>;
		// Wire shape is snake_case (GetPipelineStatus in internal/api/handler.go);
		// the web client camelCases it, so the raw keys are asserted here.
		expect(Object.keys(body).sort()).toEqual([
			"pipeline_id",
			"sinks",
			"status",
			"tables",
		]);
		expect(body.pipeline_id).toBe(pipelineId);

		// No worker runs during e2e, so nothing has ever written a stats or
		// checkpoint key under this pipeline's prefix: every bucket is empty.
		expect(body.status).toEqual({});
		expect(body.tables).toEqual({});
		expect(body.sinks).toEqual({});
	});

	test("status of an unknown pipeline is an empty envelope, not a 404", async () => {
		const res = await api.get(`/api/v1/pipelines/${uniqueId("ghost")}/status`);
		expect(res.status()).toBe(200);
		const body = (await res.json()) as { tables: unknown; sinks: unknown };
		expect(body.tables).toEqual({});
		expect(body.sinks).toEqual({});
	});

	test("renders the configuration the API stores", async ({ page }) => {
		const { pipelineId, sourceId, sinkId } = await seedPipeline("detail");

		await page.goto(`/pipelines/${pipelineId}`);

		await expect(
			page.getByRole("heading", { name: "E2E detail" }),
		).toBeVisible();
		await expect(page.getByText(`Pipeline ID: ${pipelineId}`)).toBeVisible();

		// The source badge is enriched with the type looked up from GET /sources,
		// so this also proves the sources query resolved.
		await expect(page.getByText(`${sourceId} (postgres)`)).toBeVisible();
		await expect(page.getByText(sinkId, { exact: true })).toBeVisible();
		await expect(page.getByText("1 configured source", { exact: true })).toBeVisible();
		await expect(page.getByText("1 configured sinks")).toBeVisible();
		await expect(page.getByText("public.orders", { exact: true })).toBeVisible();

		// The raw config block must agree with what the API returns, including the
		// Go duration string form of batch_wait.
		const apiPipeline = (await (
			await api.get(`/api/v1/pipelines/${pipelineId}`)
		).json()) as { batch_wait: string; batch_size: number };
		expect(apiPipeline.batch_wait).toBe("5s");

		const raw = page.locator("pre code");
		await expect(raw).toContainText(`"batchWait": "5s"`);
		await expect(raw).toContainText(`"id": "${pipelineId}"`);
	});

	test("shows the server-computed status, which is error without a worker", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("badge");

		const listed = (await (await api.get(`/api/v1/pipelines/${pipelineId}`)).json()) as {
			status: string;
		};
		expect(listed.status).toBe("error");

		await page.goto(`/pipelines/${pipelineId}`);
		await expect(page.getByText("Error", { exact: true })).toBeVisible();
	});

	test("opens the SSE metrics stream and reports Offline until it delivers", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("sse");

		// EventSource is created on mount, so the wait must be armed before the
		// navigation. Only the request is awaited: StreamMetrics never flushes
		// its headers until the first KV update, and with no worker running no
		// update will ever arrive -- waiting on the response would hang forever.
		const sseRequest = page.waitForRequest((r) =>
			r.url().includes(`/pipelines/${pipelineId}/metrics`),
		);
		await page.goto(`/pipelines/${pipelineId}`);
		const req = await sseRequest;

		// The hook passes the JWT as a query param because EventSource cannot
		// send headers (useSSE.ts).
		expect(req.url()).toMatch(/[?&]token=/);
		expect(req.method()).toBe("GET");

		// onopen never fires, so the badge stays Offline.
		await expect(page.getByText("Offline")).toBeVisible();
		await expect(page.getByText("Live", { exact: true })).toHaveCount(0);

		// Both the checkpoints and per-table tables are conditional on streamed
		// data, so they must be absent rather than empty.
		await expect(page.getByText("Replication Checkpoints")).toHaveCount(0);
		await expect(page.getByText("Production Table Metrics")).toHaveCount(0);
		await expect(page.getByText("Per-Sink Details")).toHaveCount(0);
	});

	test("Last Update stays blank while nothing streams", async ({ page }) => {
		const { pipelineId } = await seedPipeline("lastupdate");

		await page.goto(`/pipelines/${pipelineId}`);
		await expect(page.getByText("Metrics timestamp")).toBeVisible();
		// isLoading is `!lastUpdate && isConnected`; the stream never connects,
		// so the placeholder renders rather than a skeleton.
		await expect(page.getByText("--", { exact: true })).toBeVisible();
	});

	test("Restart posts to /pipelines/{id}/restart", async ({ page }) => {
		const { pipelineId } = await seedPipeline("detail-restart");

		await page.goto(`/pipelines/${pipelineId}`);
		const response = page.waitForResponse(
			(r) =>
				r.url().includes(`/pipelines/${pipelineId}/restart`) &&
				r.request().method() === "POST",
		);
		await page.getByRole("button", { name: "Restart" }).click();
		expect((await response).status()).toBe(202);
	});

	test("Edit links to the edit route for this pipeline", async ({ page }) => {
		const { pipelineId } = await seedPipeline("detail-edit");

		await page.goto(`/pipelines/${pipelineId}`);
		await page.getByRole("link", { name: "Edit" }).click();
		await expect(page).toHaveURL(new RegExp(`/pipelines/${pipelineId}/edit$`));
		await expect(
			page.getByRole("heading", { name: "Edit Pipeline" }),
		).toBeVisible();
	});

	test("a deleted pipeline renders the failure state", async ({ page }) => {
		const { pipelineId } = await seedPipeline("gone");

		expect((await api.delete(`/api/v1/pipelines/${pipelineId}`)).ok()).toBe(true);
		created.pipelines = created.pipelines.filter((id) => id !== pipelineId);

		await page.goto(`/pipelines/${pipelineId}`);
		await expect(
			page.getByRole("heading", { name: "Failed to load pipeline" }),
		).toBeVisible();
		await expect(
			page.getByRole("link", { name: "Back to Pipelines" }),
		).toBeVisible();
	});
});
