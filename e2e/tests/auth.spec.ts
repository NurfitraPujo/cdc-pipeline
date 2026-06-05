import { expect, test } from "@playwright/test";

test.describe("Auth", () => {
	test("login page renders username, password and submit button", async ({
		page,
	}) => {
		await page.goto("/login");
		await page.waitForLoadState("domcontentloaded");
		await page.waitForTimeout(5000);
		await expect(page.getByLabel("Username")).toBeVisible();
		await expect(page.getByLabel("Password")).toBeVisible();
		await expect(
			page.getByRole("button", { name: /sign in/i }),
		).toBeVisible();
	});

	test("rejects invalid credentials", async ({ page }) => {
		await page.goto("/login");
		await page.waitForLoadState("domcontentloaded");
		await page.waitForTimeout(5000);
		await page.getByLabel("Username").click();
		await page.keyboard.type("admin");
		await page.getByLabel("Password").click();
		await page.keyboard.type("wrong-password");
		await page.getByRole("button", { name: /sign in/i }).click();
		await expect(page.getByText(/login failed|unauthorized|invalid/i)).toBeVisible();
		await expect(page).toHaveURL(/\/login/);
	});

	test("successful login lands on dashboard", async ({ page }) => {
		await page.goto("/login");
		await page.waitForLoadState("domcontentloaded");
		await page.waitForTimeout(5000);
		await page.getByLabel("Username").click();
		await page.keyboard.type("admin");
		await page.getByLabel("Password").click();
		await page.keyboard.type("admin");
		await page.getByRole("button", { name: /sign in/i }).click();
		await expect(page).toHaveURL(/\/dashboard/);
		await expect(page.getByRole("heading", { name: /dashboard/i })).toBeVisible();
	});
});
