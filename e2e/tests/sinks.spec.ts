import type { APIRequestContext } from "@playwright/test";
import { expect, test } from "@playwright/test";
import { gotoHydrated } from "./helpers/ui";
import { cleanup, createApiContext, createSink, uniqueId } from "./helpers/api";

let api: APIRequestContext;
const created = {
	sinks: [] as string[],
};

// A DSN with a real password, so the server-side masking round-trip is
// observable at all: an empty password has nothing to mask.
const SECRET = "s3cr3t";
const DSN = `databend://root:${SECRET}@localhost:8000/default?sslmode=disable`;

test.beforeAll(async () => {
	api = await createApiContext();
});

test.afterAll(async () => {
	// Every id the spec touches -- seeded or created through the form -- is
	// registered below, so nothing survives the run in the shared backend.
	await cleanup(api, created);
	await api.dispose();
});

/** Seeds a sink through the API, registered for teardown. */
async function seedSink(prefix: string, dsn = DSN) {
	const id = uniqueId(prefix);
	await createSink(api, id, dsn);
	created.sinks.push(id);
	return id;
}

test.describe("Sinks", () => {
	test("lists a seeded sink with its type and DSN", async ({ page }) => {
		const id = await seedSink("list");

		await page.goto("/sinks");
		await expect(
			page.getByRole("heading", { name: "Sinks", exact: true }),
		).toBeVisible();

		// The row must actually appear -- the previous version asserted only
		// that a heading was visible, which a totally broken list satisfies.
		const row = page.getByRole("row").filter({ hasText: id });
		await expect(row).toBeVisible();
		await expect(row).toContainText("databend");
		// The list renders whatever the API returned, which is the masked DSN.
		await expect(row).toContainText("***");
		await expect(row).not.toContainText(SECRET);
	});

	test("creates a sink through the form and the API stores it", async ({
		page,
	}) => {
		const id = uniqueId("ui-create");

		await gotoHydrated(page, "/sinks/create", "#sink-id");
		await page.getByLabel("Sink ID").fill(id);
		await page.getByLabel("Type").selectOption("databend");
		await page.getByLabel("DSN (Data Source Name)").fill(DSN);
		await page.getByLabel("Max ACK Pending").fill("250");
		await page
			.getByLabel("Options (JSON Object)")
			.fill('{"database": "e2e_target"}');

		// Registered before the POST: a half-succeeded creation still has to be
		// cleaned up.
		created.sinks.push(id);
		await page.getByRole("button", { name: "Create Sink" }).click();
		await expect(page).toHaveURL(/\/sinks$/);

		const res = await api.get(`/api/v1/sinks/${id}`);
		expect(res.status()).toBe(200);
		const body = (await res.json()) as Record<string, unknown>;
		expect(body.type).toBe("databend");
		expect(body.max_ack_pending).toBe(250);
		expect(body.options).toEqual({ database: "e2e_target" });
		// Reads mask the password rather than echoing it back.
		expect(body.dsn).toContain("***");
		expect(body.dsn).not.toContain(SECRET);
	});

	test("edits a sink through the form and the change persists", async ({
		page,
	}) => {
		const id = await seedSink("edit");

		await gotoHydrated(page, `/sinks/${id}/edit`, "#sink-id");
		await expect(
			page.getByRole("heading", { name: `Edit Sink: ${id}` }),
		).toBeVisible();

		await page.getByLabel("Max ACK Pending").fill("777");
		await page.getByLabel("Options (JSON Object)").fill('{"database": "edited"}');

		await page.getByRole("button", { name: "Save Changes" }).click();
		await expect(page).toHaveURL(/\/sinks$/);

		const res = await api.get(`/api/v1/sinks/${id}`);
		expect(res.status()).toBe(200);
		const body = (await res.json()) as Record<string, unknown>;
		expect(body.max_ack_pending).toBe(777);
		expect(body.options).toEqual({ database: "edited" });
	});

	test("keeps the real DSN password when the form saves the masked value", async ({
		page,
	}) => {
		const id = await seedSink("mask");

		await gotoHydrated(page, `/sinks/${id}/edit`, "#sink-id");
		// The form is populated from the masked read, so submitting without
		// touching the DSN sends "***" back; the server has to reconstruct the
		// stored password instead of persisting the mask literally.
		await expect(page.getByLabel("DSN (Data Source Name)")).toHaveValue(
			/\*\*\*/,
		);

		await page.getByLabel("Max ACK Pending").fill("42");
		await page.getByRole("button", { name: "Save Changes" }).click();
		await expect(page).toHaveURL(/\/sinks$/);

		const res = await api.get(`/api/v1/sinks/${id}`);
		expect(res.status()).toBe(200);
		const body = (await res.json()) as Record<string, unknown>;
		expect(body.max_ack_pending).toBe(42);

		// The DSN must still be a whole DSN. If the round-trip had written the
		// mask through, the stored value would collapse to a bare "***" and the
		// host/user/database would be gone.
		const dsn = String(body.dsn);
		expect(dsn).not.toBe("***");
		expect(dsn).toMatch(/^databend:\/\/root:\*+@localhost:8000\/default/);
		expect(dsn).not.toContain(SECRET);
	});

	test("deletes a sink and the API stops returning it", async ({ page }) => {
		const id = await seedSink("del");

		await page.goto("/sinks");
		const row = page.getByRole("row").filter({ hasText: id });
		await expect(row).toBeVisible();

		// The row's only button is the icon-only dropdown trigger, which has no
		// accessible name of its own.
		page.once("dialog", (d) => d.accept());
		await row.getByRole("button").click();
		await page.getByRole("menuitem", { name: /delete sink/i }).click();

		await expect(row).toHaveCount(0);

		// Confirm server-side, not just visually.
		await expect(async () => {
			const res = await api.get(`/api/v1/sinks/${id}`);
			expect(res.status()).toBe(404);
		}).toPass();

		created.sinks = created.sinks.filter((s) => s !== id);
	});

	test("surfaces a failed connection test on the create form", async ({
		page,
	}) => {
		await gotoHydrated(page, "/sinks/create", "#sink-id");
		await page.getByLabel("Sink ID").fill(uniqueId("testconn"));
		// Port 1 has nothing listening, so the control plane's dial is refused
		// promptly instead of hanging on a TCP timeout.
		await page
			.getByLabel("DSN (Data Source Name)")
			.fill("databend://root:pw@127.0.0.1:1/default?sslmode=disable");
		await page.getByLabel("Max ACK Pending").fill("100");

		const response = page.waitForResponse(
			(r) => r.url().includes("/sinks/test") && r.request().method() === "POST",
		);
		await page.getByRole("button", { name: "Test Connection" }).click();

		// There is no reachable Databend in the e2e stack; the contract under
		// test is that the failure reaches the user, not that it connects.
		expect((await response).ok()).toBe(false);
		await expect(page.getByText("Connection Test Failed")).toBeVisible();
	});
});
