import { expect, test } from "@playwright/test";
import { fillWhenHydrated, loginViaUI } from "./helpers/ui";

// These exercise the unauthenticated flow, so they must not inherit the
// project's saved admin session.
test.use({ storageState: { cookies: [], origins: [] } });

test.describe("Auth", () => {
	test("login page renders username, password and submit button", async ({
		page,
	}) => {
		await page.goto("/login");
		await expect(page.getByLabel("Username")).toBeVisible();
		await expect(page.getByLabel("Password")).toBeVisible();
		await expect(page.getByRole("button", { name: /sign in/i })).toBeVisible();
	});

	test("rejects invalid credentials", async ({ page }) => {
		await page.goto("/login");
		await fillWhenHydrated(page, "#username", "admin");
		await fillWhenHydrated(page, "#password", "wrong-password");

		const response = page.waitForResponse(
			(r) => r.url().endsWith("/login") && r.request().method() === "POST",
		);
		await page.getByRole("button", { name: /sign in/i }).click();

		// Assert on the server's answer, not only on the rendered text.
		expect((await response).status()).toBe(401);
		await expect(
			page.getByText(/login failed|unauthorized|invalid/i),
		).toBeVisible();
		await expect(page).toHaveURL(/\/login/);
	});

	test("successful login lands on dashboard", async ({ page }) => {
		await loginViaUI(page);
		await expect(page).toHaveURL(/\/dashboard/);
		await expect(
			page.getByRole("heading", { name: /dashboard/i }),
		).toBeVisible();
	});

	test("an unauthenticated visit to a protected route redirects to login", async ({
		page,
	}) => {
		await page.goto("/pipelines");
		await expect(page).toHaveURL(/\/login/);
	});
});
