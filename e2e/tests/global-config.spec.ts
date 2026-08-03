import type { APIRequestContext, Page } from "@playwright/test";
import { expect, test } from "@playwright/test";
import { createApiContext } from "./helpers/api";
import { waitForHydration } from "./helpers/ui";

/**
 * Open /config and block until React owns the form.
 *
 * The page is server-rendered, so its markup exists before React attaches.
 * Anything typed or clicked in that window never reaches React state: a fill
 * is discarded on hydration, and a click on the Form/Raw JSON toggle does
 * nothing at all. Waiting on the first field's hydration marker covers the
 * whole form, since it all mounts together.
 */
async function gotoConfig(page: Page) {
	await page.goto("/config");
	await waitForHydration(page, "#batchSize");
}

let api: APIRequestContext;

/** Restored after each test so ordering never matters. */
const BASELINE = {
	batch_size: 1000,
	batch_wait: "1s",
	retry: {
		max_retries: 3,
		initial_interval: "1s",
		max_interval: "30s",
		enable_dlq: false,
	},
	drain_timeout: "30s",
	shutdown_timeout: "30s",
	stabilization_delay: "2s",
	crash_recovery_delay: "5s",
	global_reload_delay: "2s",
};

test.beforeAll(async () => {
	api = await createApiContext();
});

test.beforeEach(async () => {
	const res = await api.put("/api/v1/global", { data: BASELINE });
	expect(res.ok(), `seeding global config failed: ${await res.text()}`).toBe(
		true,
	);
});

test.afterAll(async () => {
	await api.put("/api/v1/global", { data: BASELINE }).catch(() => undefined);
	await api.dispose();
});

test.describe("Global Config", () => {
	test("loads the config page with the current values bound to fields", async ({
		page,
	}) => {
		await gotoConfig(page);
		await expect(
			page.getByRole("heading", { name: /global configuration/i }),
		).toBeVisible();

		// The page used to show four static labels bound to nothing, naming
		// settings that do not exist on protocol.GlobalConfig.
		await expect(page.getByLabel("Batch Size")).toHaveValue("1000");
		await expect(page.getByLabel("Batch Wait")).toHaveValue("1s");
		await expect(page.getByLabel("Max Retries")).toHaveValue("3");
		await expect(page.getByLabel("Initial Interval")).toHaveValue("1s");
		await expect(page.getByLabel("Drain Timeout")).toHaveValue("30s");
	});

	test("saves an edit and the API persists it", async ({ page }) => {
		await gotoConfig(page);
		await expect(page.getByLabel("Batch Size")).toHaveValue("1000");

		await page.getByLabel("Batch Size").fill("2500");
		await page.getByLabel("Batch Wait").fill("7s");
		await page.getByLabel("Max Interval").fill("45s");

		const response = page.waitForResponse(
			(r) => r.url().endsWith("/global") && r.request().method() === "PUT",
		);
		await page.getByRole("button", { name: /save configuration/i }).click();

		// PUT /global previously always failed with 400: the client cast its
		// camelCase model to the wire type instead of converting it, so the
		// server bound no fields and rejected batch_size < 1.
		expect((await response).status()).toBe(200);
		await expect(page.getByText(/saved successfully/i)).toBeVisible();

		// Confirm it really landed, independently of the UI.
		const stored = await (await api.get("/api/v1/global")).json();
		expect(stored.batch_size).toBe(2500);
		expect(stored.batch_wait).toBe("7s");
		expect(stored.retry.max_interval).toBe("45s");
	});

	test("durations round-trip as strings, not nanosecond integers", async () => {
		await api.put("/api/v1/global", {
			data: { ...BASELINE, batch_wait: "2500ms" },
		});

		const cfg = await (await api.get("/api/v1/global")).json();

		// The contract has always claimed these are strings, but bare
		// time.Duration fields marshalled as int64 nanoseconds.
		expect(typeof cfg.batch_wait).toBe("string");
		expect(cfg.batch_wait).toBe("2.5s");
		expect(typeof cfg.retry.initial_interval).toBe("string");
		expect(typeof cfg.drain_timeout).toBe("string");
	});

	test("rejects an invalid duration before it reaches the server", async ({
		page,
	}) => {
		await gotoConfig(page);
		await expect(page.getByLabel("Batch Wait")).toHaveValue("1s");

		await page.getByLabel("Batch Wait").fill("not-a-duration");

		let putFired = false;
		page.on("request", (r) => {
			if (r.url().endsWith("/global") && r.method() === "PUT") putFired = true;
		});

		await page.getByRole("button", { name: /save configuration/i }).click();
		await expect(page.getByText(/invalid duration/i)).toBeVisible();
		expect(putFired).toBe(false);
	});

	test("the raw JSON editor is still available as an escape hatch", async ({
		page,
	}) => {
		await gotoConfig(page);
		await page.getByRole("button", { name: "Raw JSON" }).click();
		await expect(
			page.getByText("Configuration Editor", { exact: true }),
		).toBeVisible();
	});
});
