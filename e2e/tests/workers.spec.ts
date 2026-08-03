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

test.describe("Workers", () => {
	test("GET /workers/{id}/heartbeat is 404 while no worker is running", async () => {
		const { pipelineId } = await seedPipeline("hb");

		// A worker ID *is* a pipeline ID (protocol.WorkerHeartbeatKey), so a
		// freshly created pipeline is addressable here immediately -- it simply
		// has no heartbeat record because the e2e stack runs only the API.
		const res = await api.get(`/api/v1/workers/${pipelineId}/heartbeat`);
		expect(res.status()).toBe(404);
		expect(await res.json()).toEqual({ error: "worker not found" });
	});

	test("heartbeat for an id that was never a pipeline is also 404", async () => {
		const res = await api.get(`/api/v1/workers/${uniqueId("nobody")}/heartbeat`);
		expect(res.status()).toBe(404);
	});

	test("lists every pipeline as a worker row", async ({ page }) => {
		const { pipelineId } = await seedPipeline("worker-row");

		await page.goto("/workers");
		await expect(page.getByRole("heading", { name: "Workers" })).toBeVisible();
		await expect(page.getByText("Worker Heartbeats")).toBeVisible();
		await expect(
			page.getByRole("columnheader", { name: "Worker Status" }),
		).toBeVisible();

		const row = page.getByRole("row").filter({ hasText: pipelineId });
		await expect(row).toBeVisible();
		await expect(row).toContainText("E2E worker-row");
	});

	test("a pipeline with no heartbeat shows the No heartbeat badge and no uptime", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("no-hb");

		// Bind to the row's own heartbeat call so the assertions below run after
		// the per-row query has settled, rather than against its loading skeleton.
		const heartbeat = page.waitForResponse((r) =>
			r.url().includes(`/workers/${pipelineId}/heartbeat`),
		);
		await page.goto("/workers");
		expect((await heartbeat).status()).toBe(404);

		const row = page.getByRole("row").filter({ hasText: pipelineId });
		await expect(row).toContainText("No heartbeat");
		// Uptime and Last Heartbeat both fall back to an em dash.
		await expect(row.getByText("—")).toHaveCount(2);
		await expect(row).not.toContainText("Stale");
		await expect(row).not.toContainText("Running");
	});

	test("the pipeline name links through to the detail page", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("worker-link");

		await page.goto("/workers");
		const row = page.getByRole("row").filter({ hasText: pipelineId });
		await row.getByRole("link", { name: "E2E worker-link" }).click();

		await expect(page).toHaveURL(new RegExp(`/pipelines/${pipelineId}$`));
		await expect(
			page.getByRole("heading", { name: "E2E worker-link" }),
		).toBeVisible();
	});

	test("a deleted pipeline drops out of the roster", async ({ page }) => {
		const { pipelineId } = await seedPipeline("worker-del");

		await page.goto("/workers");
		await expect(
			page.getByRole("row").filter({ hasText: pipelineId }),
		).toBeVisible();

		expect((await api.delete(`/api/v1/pipelines/${pipelineId}`)).ok()).toBe(true);
		created.pipelines = created.pipelines.filter((id) => id !== pipelineId);

		// The roster is derived from GET /pipelines, which the page polls; a
		// reload makes the removal deterministic instead of waiting on the timer.
		await page.reload();
		await expect(
			page.getByRole("row").filter({ hasText: pipelineId }),
		).toHaveCount(0);
	});

	test("the roster is capped at the list endpoint's page limit", async ({
		page,
	}) => {
		await seedPipeline("worker-cap");

		const listResponse = page.waitForResponse(
			(r) => r.url().includes("/pipelines?") && r.url().includes("limit=100"),
		);
		await page.goto("/workers");
		const res = await listResponse;
		expect(res.status()).toBe(200);

		const body = (await res.json()) as { pipelines: unknown[]; total: number };
		expect(body.pipelines.length).toBeLessThanOrEqual(100);
		expect(body.pipelines.length).toBe(Math.min(body.total, 100));
	});
});
