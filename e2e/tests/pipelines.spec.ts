import { expect, test } from "@playwright/test";

test.use({ storageState: ".auth/admin.json" });

test.describe("Pipelines", () => {
	test("lists pipelines page", async ({ page }) => {
		await page.goto("/pipelines");
		await expect(
			page.getByRole("heading", { name: /pipelines/i }).first(),
		).toBeVisible();
	});

	test("navigates to a pipeline detail", async ({ page }) => {
		await page.goto("/pipelines");
		const firstLink = page
			.locator('button[onclick*="/pipelines/"]')
			.first();
		if ((await firstLink.count()) > 0) {
			await firstLink.click();
			await expect(page).toHaveURL(/\/pipelines\/.+/);
		}
	});

	test("create pipeline page renders", async ({ page }) => {
		await page.goto("/pipelines/create");
		await expect(
			page.getByRole("heading", { name: /create pipeline/i }),
		).toBeVisible();
	});
});
