import { expect, test } from "@playwright/test";

test.use({ storageState: ".auth/admin.json" });

test.describe("Sources", () => {
	test("loads the sources page", async ({ page }) => {
		await page.goto("/sources");
		await expect(
			page.getByRole("heading", { name: "Sources", exact: true }),
		).toBeVisible();
		// If there are no sources, it should say "No sources configured" or list them
		await expect(
			page.locator("body")
		).toContainText("sources");
	});
});
