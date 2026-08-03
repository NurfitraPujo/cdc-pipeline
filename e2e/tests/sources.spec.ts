import type { APIRequestContext, Page } from "@playwright/test";
import { expect, test } from "@playwright/test";
import { gotoHydrated } from "./helpers/ui";
import {
	cleanup,
	createApiContext,
	createSource,
	uniqueId,
} from "./helpers/api";

let api: APIRequestContext;
const created = {
	sources: [] as string[],
};

test.beforeAll(async () => {
	api = await createApiContext();
});

test.afterAll(async () => {
	// Every id the spec touches -- seeded or created through the form -- is
	// registered below, so nothing survives the run in the shared backend.
	await cleanup(api, created);
	await api.dispose();
});

/** Seeds a source through the API, registered for teardown. */
async function seedSource(prefix: string) {
	const id = uniqueId(prefix);
	await createSource(api, { id });
	created.sources.push(id);
	return id;
}

/** Expands the "Advanced & Table Filters" card, which is collapsed by default. */
async function showAdvanced(page: Page) {
	await page.getByRole("button", { name: "Show", exact: true }).click();
	await expect(page.getByLabel("Batch Wait")).toBeVisible();
}

test.describe("Sources", () => {
	test("lists a seeded source with its connection details", async ({
		page,
	}) => {
		const id = await seedSource("list");

		await page.goto("/sources");
		await expect(
			page.getByRole("heading", { name: "Sources", exact: true }),
		).toBeVisible();

		// The row must actually appear -- the previous version asserted only
		// that a heading was visible, which a totally broken list satisfies.
		const row = page.getByRole("row").filter({ hasText: id });
		await expect(row).toBeVisible();
		await expect(row).toContainText("postgres");
		await expect(row).toContainText("localhost:5432 / e2e");
	});

	test("creates a source through the form and the API stores it", async ({
		page,
	}) => {
		const id = uniqueId("ui-create");

		await gotoHydrated(page, "/sources/create", "#source-id");
		await page.getByLabel("Source ID").fill(id);
		await page.getByLabel("Host").fill("localhost");
		await page.getByLabel("Port").fill("5432");
		await page.getByLabel("Database Name").fill("e2e");
		await page.getByLabel("User", { exact: true }).fill("postgres");
		await page.getByLabel("Password").fill("postgres");

		await showAdvanced(page);
		await page.getByLabel("Replication Slot Name").fill(`${id}_slot`);
		await page.getByLabel("Publication Name").fill(`${id}_pub`);
		await page.getByLabel("Batch Size").fill("250");
		await page.getByLabel("Batch Wait").fill("5s");
		await page.getByLabel("Discovery Interval").fill("30s");

		// Registered before the POST: a half-succeeded creation still has to be
		// cleaned up.
		created.sources.push(id);
		await page.getByRole("button", { name: "Create Source" }).click();
		await expect(page).toHaveURL(/\/sources$/);

		const res = await api.get(`/api/v1/sources/${id}`);
		expect(res.status()).toBe(200);
		const body = (await res.json()) as Record<string, unknown>;
		expect(body.type).toBe("postgres");
		expect(body.host).toBe("localhost");
		expect(body.port).toBe(5432);
		expect(body.database).toBe("e2e");
		expect(body.slot_name).toBe(`${id}_slot`);
		expect(body.batch_size).toBe(250);

		// Durations travel as Go duration strings now; they used to be
		// nanosecond integers, and a regression there is invisible in the UI.
		expect(typeof body.batch_wait).toBe("string");
		expect(body.batch_wait).toBe("5s");
		expect(typeof body.discovery_interval).toBe("string");
		expect(body.discovery_interval).toBe("30s");
	});

	test("edits a source through the form and the change persists", async ({
		page,
	}) => {
		const id = await seedSource("edit");

		await gotoHydrated(page, `/sources/${id}/edit`, "#source-id");
		await expect(
			page.getByRole("heading", { name: `Edit Source: ${id}` }),
		).toBeVisible();

		await page.getByLabel("Database Name").fill("e2e_edited");
		await showAdvanced(page);
		await page.getByLabel("Batch Wait").fill("7s");

		await page.getByRole("button", { name: "Save Changes" }).click();
		await expect(page).toHaveURL(/\/sources$/);

		const res = await api.get(`/api/v1/sources/${id}`);
		expect(res.status()).toBe(200);
		const body = (await res.json()) as Record<string, unknown>;
		expect(body.database).toBe("e2e_edited");
		expect(body.batch_wait).toBe("7s");
	});

	test("deletes a source and the API stops returning it", async ({ page }) => {
		const id = await seedSource("del");

		await page.goto("/sources");
		const row = page.getByRole("row").filter({ hasText: id });
		await expect(row).toBeVisible();

		// The row's only button is the icon-only dropdown trigger, which has no
		// accessible name of its own.
		page.once("dialog", (d) => d.accept());
		await row.getByRole("button").click();
		await page.getByRole("menuitem", { name: /delete source/i }).click();

		await expect(row).toHaveCount(0);

		// Confirm server-side, not just visually.
		await expect(async () => {
			const res = await api.get(`/api/v1/sources/${id}`);
			expect(res.status()).toBe(404);
		}).toPass();

		created.sources = created.sources.filter((s) => s !== id);
	});

	test("surfaces a failed connection test on the create form", async ({
		page,
	}) => {
		await gotoHydrated(page, "/sources/create", "#source-id");
		await page.getByLabel("Source ID").fill(uniqueId("testconn"));
		// Port 1 has nothing listening, so the control plane's dial is refused
		// promptly instead of hanging on a TCP timeout.
		await page.getByLabel("Host").fill("127.0.0.1");
		await page.getByLabel("Port").fill("1");
		await page.getByLabel("Database Name").fill("e2e");
		await page.getByLabel("User", { exact: true }).fill("postgres");
		await page.getByLabel("Password").fill("postgres");

		const response = page.waitForResponse(
			(r) =>
				r.url().includes("/sources/test") && r.request().method() === "POST",
		);
		await page.getByRole("button", { name: "Test Connection" }).click();

		// There is no reachable PostgreSQL in the e2e stack; the contract under
		// test is that the failure reaches the user, not that it connects.
		expect((await response).ok()).toBe(false);
		await expect(page.getByText("Connection Test Failed")).toBeVisible();
	});

	test("surfaces schema and table discovery failures on the edit form", async ({
		page,
	}) => {
		const id = await seedSource("discover");

		await gotoHydrated(page, `/sources/${id}/edit`, "#source-id");
		await showAdvanced(page);

		// Two "Discover" buttons: schemas first, tables second.
		const discover = page.getByRole("button", { name: "Discover" });
		await expect(discover).toHaveCount(2);

		const schemaResponse = page.waitForResponse(
			(r) =>
				r.url().includes(`/sources/${id}/schema`) &&
				r.request().method() === "GET",
		);
		await discover.first().click();
		expect((await schemaResponse).ok()).toBe(false);
		// Counting occurrences pins the message to the block that was clicked:
		// both blocks render the same "Discovery failed:" text.
		await expect(page.getByText(/Discovery failed:/)).toHaveCount(1);

		const tablesResponse = page.waitForResponse(
			(r) =>
				r.url().includes(`/sources/${id}/tables`) &&
				r.request().method() === "GET",
		);
		await discover.nth(1).click();
		expect((await tablesResponse).ok()).toBe(false);
		await expect(page.getByText(/Discovery failed:/)).toHaveCount(2);
	});
});
