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
	// The old suite created pipelines through the UI and never removed them,
	// so every run leaked rows into the shared backend.
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

test.describe("Pipelines", () => {
	test("lists a seeded pipeline with its status", async ({ page }) => {
		await seedPipeline("list");

		await page.goto("/pipelines");
		await expect(
			page.getByRole("heading", { name: /pipelines/i }).first(),
		).toBeVisible();

		// The row must actually appear -- the previous version asserted only
		// that a heading was visible, which a totally broken list satisfies.
		const row = page.getByRole("row").filter({ hasText: "E2E list" });
		await expect(row).toBeVisible();

		// GET /pipelines returns a computed status. With no worker running the
		// API reports "error"; what matters is that it is never blank.
		await expect(row).toContainText(/healthy|transitioning|error/i);
	});

	test("navigates from the list to the detail page", async ({ page }) => {
		const { pipelineId } = await seedPipeline("nav");

		await page.goto("/pipelines");

		// A real router link. The old selector was
		// `button[onclick*="/pipelines/"]`, which React never emits as an HTML
		// attribute -- count() was always 0, so the body silently never ran.
		await page.getByRole("link", { name: "E2E nav" }).click();

		await expect(page).toHaveURL(new RegExp(`/pipelines/${pipelineId}`));
		await expect(page.getByRole("heading", { name: "E2E nav" })).toBeVisible();
	});

	test("create pipeline page renders", async ({ page }) => {
		await page.goto("/pipelines/create");
		await expect(
			page.getByRole("heading", { name: /create pipeline/i }),
		).toBeVisible();
	});

	test("search filters the list server-side", async ({ page }) => {
		await seedPipeline("findme");
		await seedPipeline("otherone");

		await page.goto("/pipelines");
		await expect(
			page.getByRole("row").filter({ hasText: "E2E findme" }),
		).toBeVisible();
		await expect(
			page.getByRole("row").filter({ hasText: "E2E otherone" }),
		).toBeVisible();

		// Assert the query actually reaches the server, then that the UI
		// narrows. The search box used to be entirely inert.
		const request = page.waitForRequest(
			(r) =>
				r.url().includes("/pipelines?") && r.url().includes("search=findme"),
		);
		await page.getByLabel("Search pipelines").fill("findme");
		await request;

		await expect(
			page.getByRole("row").filter({ hasText: "E2E findme" }),
		).toBeVisible();
		await expect(
			page.getByRole("row").filter({ hasText: "E2E otherone" }),
		).toHaveCount(0);
	});

	test("deletes a pipeline and the API stops returning it", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("del");

		await page.goto("/pipelines");
		const row = page.getByRole("row").filter({ hasText: "E2E del" });
		await expect(row).toBeVisible();

		page.once("dialog", (d) => d.accept());
		await row.getByRole("button", { name: /open menu/i }).click();
		await page.getByRole("menuitem", { name: /delete/i }).click();

		await expect(row).toHaveCount(0);

		// Confirm server-side, not just visually.
		await expect(async () => {
			const res = await api.get(`/api/v1/pipelines/${pipelineId}`);
			expect(res.status()).toBe(404);
		}).toPass();

		created.pipelines = created.pipelines.filter((id) => id !== pipelineId);
	});

	test("restart triggers POST /pipelines/{id}/restart", async ({ page }) => {
		await seedPipeline("restart");

		await page.goto("/pipelines");
		const row = page.getByRole("row").filter({ hasText: "E2E restart" });
		await expect(row).toBeVisible();

		const response = page.waitForResponse(
			(r) => r.url().includes("/restart") && r.request().method() === "POST",
		);
		await row.getByRole("button", { name: /open menu/i }).click();
		await page.getByRole("menuitem", { name: /restart/i }).click();

		expect((await response).status()).toBe(202);
	});
});
