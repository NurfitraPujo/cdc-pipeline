import { expect, test } from "@playwright/test";
import { loginViaUI } from "./helpers/ui";

test.describe("auth.setup", () => {
	test("logs in as admin and saves authenticated state", async ({
		page,
		context,
	}) => {
		await loginViaUI(page);
		await expect(page).toHaveURL(/\/dashboard/);

		// Every spec in the `chromium` project reuses this state. It is
		// rewritten on each run, so the stored JWT is always fresh -- the
		// previously committed file held a token that had already expired.
		await context.storageState({ path: ".auth/admin.json" });
	});
});
