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
import { gotoHydrated } from "./helpers/ui";

let api: APIRequestContext;
const created = {
	pipelines: [] as string[],
	sources: [] as string[],
	sinks: [] as string[],
};

// A source and a sink the create form can select. These tests used to assume
// the backend already had some: with a per-run NATS there are none, so the
// radio list was empty and `input[name="source"]` never resolved.
let sourceId: string;
let sinkId: string;

test.beforeAll(async () => {
	api = await createApiContext();

	sourceId = uniqueId("adv-src");
	sinkId = uniqueId("adv-snk");
	await createSource(api, { id: sourceId });
	created.sources.push(sourceId);
	await createSink(api, sinkId);
	created.sinks.push(sinkId);
});

test.afterAll(async () => {
	await cleanup(api, created);
	await api.dispose();
});

/** Fill the ID and pick the seeded source and sink on the create form. */
async function startPipelineForm(
	page: import("@playwright/test").Page,
	id: string,
) {
	await gotoHydrated(page, "/pipelines/create", "#pipeline-id");
	await page.getByLabel("Pipeline ID").fill(id);
	await page.locator(`input[name="source"]`).first().check();
	await page.locator(`input[name="sink"]`).first().check();
}

test.describe("Pipelines - Advanced Configuration", () => {
	test("create pipeline with advanced overrides", async ({ page }) => {
		const id = uniqueId("adv");
		created.pipelines.push(id);

		await startPipelineForm(page, id);

		await page.getByRole("button", { name: /Batch & Performance/ }).click();
		await page.getByLabel("Batch Size (messages)").fill("50");
		await page.getByLabel("Batch Wait (duration)").fill("2s");

		await page.getByRole("button", { name: /Retry & Error Handling/ }).click();
		await page.getByLabel("Max Retries").fill("5");
		await page.getByLabel("Initial Backoff").fill("500ms");
		await page.getByLabel("Max Backoff").fill("5s");
		await page.getByLabel("Enable Dead Letter Queue").click();

		const postPromise = page.waitForRequest(
			(req) => req.url().endsWith("/pipelines") && req.method() === "POST",
		);
		await page.getByRole("button", { name: "Create Pipeline" }).click();

		const req = await postPromise;
		const body = JSON.parse(req.postData() ?? "{}");
		expect(body.batch_size).toBe(50);
		expect(body.batch_wait).toBe("2s");
		expect(body.retry).toBeTruthy();
		expect(body.retry.max_retries).toBe(5);
		expect(body.retry.initial_interval).toBe("500ms");
		expect(body.retry.max_interval).toBe("5s");
		expect(body.retry.enable_dlq).toBe(true);

		// Assert the server accepted it, not merely that the request was well
		// formed -- a 400 would otherwise still pass this test.
		await expect(page).toHaveURL(/\/pipelines$/);

		const stored = await (await api.get(`/api/v1/pipelines/${id}`)).json();
		expect(stored.batch_size).toBe(50);
		expect(stored.batch_wait).toBe("2s");
		expect(stored.retry.max_retries).toBe(5);
		expect(stored.retry.initial_interval).toBe("500ms");
		expect(stored.retry.enable_dlq).toBe(true);
	});

	test("invalid batch wait shows inline error and blocks submit", async ({
		page,
	}) => {
		await startPipelineForm(page, uniqueId("adv-invalid"));

		await page.getByRole("button", { name: /Batch & Performance/ }).click();
		await page.getByLabel("Batch Wait (duration)").fill("5x");

		await expect(page.getByText("Invalid duration format")).toBeVisible();

		let posted = false;
		page.on("request", (req) => {
			if (req.url().endsWith("/pipelines") && req.method() === "POST") {
				posted = true;
			}
		});

		await page.getByRole("button", { name: "Create Pipeline" }).click();

		// handleCreate validates advanced durations and returns early, so the
		// form must stay put rather than navigate to the list.
		await expect(page).toHaveURL(/\/pipelines\/create$/);
		expect(posted).toBe(false);
	});

	test("the JSON editor round-trips advanced fields through a save", async ({
		page,
	}) => {
		// Seeded explicitly. The previous version hunted for "the first
		// pipeline in the table" and called test.skip() when it found none,
		// which made this a silent no-op on an empty backend.
		const id = uniqueId("adv-edit");
		await createPipeline(api, {
			id,
			name: `E2E ${id}`,
			sources: [sourceId],
			sinks: [sinkId],
			retry: {
				max_retries: 7,
				initial_interval: "1s",
				max_interval: "20s",
				enable_dlq: true,
			},
		});
		created.pipelines.push(id);

		await page.goto(`/pipelines/${id}/edit`);

		const editorSurface = page.locator(".monaco-editor").first();
		await expect(editorSurface).toBeVisible();

		// Not asserting on the editor's rendered text: Monaco virtualises its
		// view lines, so innerText only covers what is currently scrolled into
		// view. The PUT body below is the reliable evidence of what the editor
		// actually holds.

		// Make a semantically neutral edit so Save enables. Restructuring the
		// JSON programmatically is not viable here: Monaco re-indents on every
		// newline and auto-closes brackets, so an inserted document comes back
		// malformed (which ConfigEditor then correctly refuses to save).
		// Appending a blank line leaves the JSON valid and untouched.
		//
		// The narrower guarantee -- that omitting a field in the editor falls
		// back to the stored value rather than wiping it -- is covered by
		// mergeWithCurrent's unit tests in web/src/lib/pipelineMerge.test.ts
		// ("falls back to current optionals when parsed omits them",
		// "retry: null falls back to current"). What this test adds is proof
		// of the wiring: ConfigEditor -> handleSaveFromJson -> mergeWithCurrent
		// -> PUT.
		await page.locator(".view-lines").first().click();
		await page.keyboard.press("ControlOrMeta+End");
		await page.keyboard.press("Enter");

		const save = page.getByRole("button", { name: "Save" });
		// Disabled until the editor reports a change, so this also confirms the
		// edit reached Monaco's model rather than just the DOM.
		await expect(save).toBeEnabled();

		const putPromise = page.waitForRequest(
			(req) => /\/pipelines\/[^/]+$/.test(req.url()) && req.method() === "PUT",
		);
		await save.click();

		const putBody = JSON.parse((await putPromise).postData() ?? "{}");
		expect(putBody.retry).toBeDefined();
		expect(putBody.retry.max_retries).toBe(7);
		expect(putBody.retry.initial_interval).toBe("1s");
		expect(putBody.retry.enable_dlq).toBe(true);

		// And it actually survived server-side.
		const stored = await (await api.get(`/api/v1/pipelines/${id}`)).json();
		expect(stored.retry.max_retries).toBe(7);
		expect(stored.retry.enable_dlq).toBe(true);
		expect(stored.batch_wait).toBe("5s");
	});
});
