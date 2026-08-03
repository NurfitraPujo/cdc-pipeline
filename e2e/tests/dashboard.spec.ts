import type { APIRequestContext, Page, Response } from "@playwright/test";
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

interface StatsSummary {
	total_pipelines: number;
	healthy_count: number;
	error_count: number;
	transitioning_count: number;
	total_rows_synced: number;
	avg_lag_ms: number;
}

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

/**
 * Loads /dashboard and returns the summary the page itself rendered from.
 *
 * GET /stats/summary is served from a 30s cache, so a separately issued API
 * call can legitimately disagree with what the page received. Asserting against
 * the page's own response body removes that race entirely.
 */
async function gotoDashboard(page: Page): Promise<StatsSummary> {
	const summary: Promise<Response> = page.waitForResponse((r) =>
		r.url().includes("/stats/summary"),
	);
	await page.goto("/dashboard");
	const res = await summary;
	expect(res.status()).toBe(200);
	return (await res.json()) as StatsSummary;
}

test.describe("Dashboard", () => {
	test("GET /stats/summary returns the documented counters", async () => {
		const res = await api.get("/api/v1/stats/summary");
		expect(res.status()).toBe(200);

		const body = (await res.json()) as StatsSummary;
		expect(Object.keys(body).sort()).toEqual([
			"avg_lag_ms",
			"error_count",
			"healthy_count",
			"total_pipelines",
			"total_rows_synced",
			"transitioning_count",
		]);
		expect(
			body.healthy_count + body.transitioning_count + body.error_count,
		).toBe(body.total_pipelines);
	});

	test("the metric cards mirror the summary the page fetched", async ({
		page,
	}) => {
		await seedPipeline("dash");
		const s = await gotoDashboard(page);

		await expect(page.getByRole("heading", { name: "Dashboard" })).toBeVisible();

		// Every card's description embeds its numbers verbatim, so matching them
		// proves the values came from the response rather than a default.
		await expect(
			page.getByText(`${s.healthy_count} healthy, ${s.error_count} errors`),
		).toBeVisible();
		await expect(
			page.getByText(`${s.healthy_count} of ${s.total_pipelines} pipelines`),
		).toBeVisible();
		await expect(
			page.getByText("Total rows processed across all pipelines"),
		).toBeVisible();
		await expect(page.getByText("Average processing latency")).toBeVisible();
	});

	test("a seeded pipeline is counted as an error, since no worker runs", async ({
		page,
	}) => {
		const before = (await (
			await api.get("/api/v1/stats/summary")
		).json()) as StatsSummary;

		await seedPipeline("dash-count");

		// The summary is cached for 30s; poll until the recompute picks the new
		// pipeline up rather than sleeping.
		let after: StatsSummary = before;
		await expect(async () => {
			after = (await (await api.get("/api/v1/stats/summary")).json()) as StatsSummary;
			expect(after.total_pipelines).toBeGreaterThan(before.total_pipelines);
		}).toPass();

		// A pipeline whose worker never heartbeats is "error", never "healthy".
		expect(after.error_count).toBeGreaterThan(before.error_count);
		expect(after.healthy_count).toBe(0);

		const s = await gotoDashboard(page);
		expect(s.healthy_count).toBe(0);
		await expect(page.getByText(`0 of ${s.total_pipelines} pipelines`)).toBeVisible();
	});

	test("the throughput chart stays in its collecting state on first load", async ({
		page,
	}) => {
		await gotoDashboard(page);

		const throughput = page
			.locator("div.rounded-xl.border")
			.filter({ hasText: "Throughput" });
		await expect(
			throughput.getByRole("heading", { name: "Throughput" }),
		).toBeVisible();

		// The series is derived from successive polls 30s apart (the /stats/history
		// endpoint is deprecated and hardcoded to []), so a freshly opened page has
		// at most one sample and must render the placeholder, not an svg.
		await expect(throughput.getByText("Collecting samples…")).toBeVisible();
		await expect(throughput.locator("svg")).toHaveCount(0);
	});

	test("the status chart breaks the pipelines down by state", async ({
		page,
	}) => {
		await seedPipeline("dash-status");
		const s = await gotoDashboard(page);

		const status = page
			.locator("div.rounded-xl.border")
			.filter({ hasText: "Pipeline Status" });
		await expect(
			status.getByText(`Distribution across all ${s.total_pipelines} pipelines.`),
		).toBeVisible();

		// Each bar's row carries its label and its count side by side.
		const bar = (label: string) =>
			status.getByText(label, { exact: true }).locator("..");
		await expect(bar("Healthy")).toContainText(String(s.healthy_count));
		await expect(bar("Transitioning")).toContainText(
			String(s.transitioning_count),
		);
		await expect(bar("Error")).toContainText(String(s.error_count));

		// At least one pipeline exists, so the empty-state copy must be absent.
		expect(s.total_pipelines).toBeGreaterThan(0);
		await expect(
			status.getByText("No pipelines configured yet."),
		).toHaveCount(0);
	});

	test("a failing summary surfaces the error banner", async ({ page }) => {
		await page.route("**/stats/summary*", (route) =>
			route.fulfill({ status: 500, body: '{"error":"boom"}' }),
		);

		await page.goto("/dashboard");
		await expect(
			page.getByText("Failed to load dashboard data"),
		).toBeVisible();
	});
});
