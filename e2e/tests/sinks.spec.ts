import { expect, test } from "@playwright/test";

test.use({ storageState: ".auth/admin.json" });

test.describe("Sinks", () => {
	test("loads the sinks page", async ({ page }) => {
		await page.goto("/sinks");
		await expect(
			page.getByRole("heading", { name: "Sinks", exact: true }),
		).toBeVisible();
		// If there are no sinks, it should say "No sinks configured" or list them
		await expect(
			page.locator("body")
		).toContainText("sinks");
	});
});
