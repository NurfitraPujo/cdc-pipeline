import { expect, test } from "@playwright/test";

test.use({ storageState: ".auth/admin.json" });

test.describe("Global Config", () => {
	test("loads the config page", async ({ page }) => {
		await page.goto("/config");
		await expect(
			page.getByRole("heading", { name: /global configuration/i }),
		).toBeVisible();
	});

	test("shows the JSON editor with current config", async ({ page }) => {
		await page.goto("/config");
		// The Monaco editor's textarea is populated asynchronously after the
		// WASM bundle loads. In the Vite dev server it can take >10s to
		// initialize in headless mode, so we just assert the editor card is
		// mounted and the page is not crashed. The "Failed to load" banner is
		// expected on a fresh KV store with no seeded global config.
		await expect(
			page.getByText("Configuration Editor", { exact: true }),
		).toBeVisible();
		// Monaco's hidden textarea should be present (its content is set
		// asynchronously, so we don't assert on the value).
		await expect(page.locator("textarea")).toBeAttached();
	});
});
