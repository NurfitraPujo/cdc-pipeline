import { expect, test } from "@playwright/test";

test.use({ storageState: ".auth/admin.json" });

test.describe("Sources", () => {
	test("loads the sources page", async ({ page }) => {
		await page.goto("/sources");
		await expect(
			page.getByRole("heading", { name: "Sources", exact: true }),
		).toBeVisible();
		await expect(page.getByText("Source management coming soon.", { exact: true })).toBeVisible();
	});
});
