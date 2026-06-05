import { expect, test } from "@playwright/test";

const ADMIN_USER = "admin";
const ADMIN_PASS = "admin";

test.describe("auth.setup", () => {
	test("logs in as admin and saves authenticated state", async ({
		page,
		context,
	}) => {
		await page.goto("/login");
		await page.waitForLoadState("domcontentloaded");
		// Wait for React hydration: with TanStack Start SSR, the form is
		// server-rendered before React attaches event handlers. Filling inputs
		// before hydration silently drops characters (controlled state stays "").
		// Typing via the keyboard dispatches native input events that React picks
		// up as it hydrates.
		await page.waitForTimeout(5000);
		await page.getByLabel("Username").click();
		await page.keyboard.type(ADMIN_USER);
		await page.getByLabel("Password").click();
		await page.keyboard.type(ADMIN_PASS);
		await page.getByRole("button", { name: /sign in/i }).click();
		await expect(page).toHaveURL(/\/dashboard/);

		await context.storageState({ path: ".auth/admin.json" });
	});
});
