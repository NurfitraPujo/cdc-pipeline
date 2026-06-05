import { expect, test } from "@playwright/test";

test.use({ storageState: ".auth/admin.json" });

test.describe("Dashboard (authenticated)", () => {
	test("renders metrics cards", async ({ page }) => {
		await page.goto("/dashboard");
		await expect(
			page.getByRole("heading", { name: /dashboard/i }),
		).toBeVisible();
		await expect(page.getByText(/total pipelines/i)).toBeVisible();
		await expect(page.getByText(/average lag/i)).toBeVisible();
	});
});
