import { expect, test } from "@playwright/test";

test.use({ storageState: ".auth/admin.json" });

test.describe("Sinks", () => {
	test("loads the sinks page", async ({ page }) => {
		await page.goto("/sinks");
		await expect(
			page.getByRole("heading", { name: "Sinks", exact: true }),
		).toBeVisible();
		await expect(page.getByText("Sink management coming soon.", { exact: true })).toBeVisible();
	});
});
