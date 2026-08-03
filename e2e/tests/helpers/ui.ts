import { expect, type Page } from "@playwright/test";

/**
 * Block until React has hydrated the given element.
 *
 * With TanStack Start SSR the form markup is served before React attaches its
 * handlers. Anything typed in that window updates the DOM node but not React's
 * controlled state, and hydration then resets the field to its initial value.
 * The symptom is a login that submits empty credentials and comes back
 * "unauthorized" with both boxes blank.
 *
 * Waiting on a value "sticking" is not sufficient -- hydration can land between
 * the check and the click. React marks each hydrated host node with a
 * `__reactFiber$<hash>` property, so that is the actual signal to wait for.
 *
 * The suite previously used `waitForTimeout(5000)` here: a guess that cost
 * ~20s per run and still raced on a slow cold start.
 */
export async function waitForHydration(
	page: Page,
	selector: string,
): Promise<void> {
	await page.locator(selector).waitFor({ state: "visible" });
	await page.waitForFunction(
		(sel) => {
			const el = document.querySelector(sel);
			if (!el) return false;
			return Object.keys(el).some((k) => k.startsWith("__reactFiber$"));
		},
		selector,
		{ timeout: 30_000 },
	);
}

/**
 * Navigate and block until React owns the page.
 *
 * Prefer this over a bare `page.goto` for any test that then types or clicks.
 * `anchorSelector` should be an element that mounts with the interactive part
 * of the page -- usually its first form field.
 */
export async function gotoHydrated(
	page: Page,
	url: string,
	anchorSelector: string,
): Promise<void> {
	await page.goto(url);
	await page.waitForLoadState("domcontentloaded");
	await waitForHydration(page, anchorSelector);
}

/** Fill an input once React owns it, then confirm the value took. */
export async function fillWhenHydrated(
	page: Page,
	selector: string,
	value: string,
): Promise<void> {
	await waitForHydration(page, selector);
	const input = page.locator(selector);
	await input.fill(value);
	await expect(input).toHaveValue(value);
}

export const ADMIN_USER = "admin";
export const ADMIN_PASS = "admin";

/** Drives the real login form and waits for the post-login redirect. */
export async function loginViaUI(
	page: Page,
	user = ADMIN_USER,
	pass = ADMIN_PASS,
): Promise<void> {
	await page.goto("/login");
	await page.waitForLoadState("domcontentloaded");

	await fillWhenHydrated(page, "#username", user);
	await fillWhenHydrated(page, "#password", pass);

	// Re-assert immediately before submitting: this is the invariant that
	// actually matters, and it is cheap.
	await expect(page.locator("#username")).toHaveValue(user);
	await expect(page.locator("#password")).toHaveValue(pass);

	await page.getByRole("button", { name: /sign in/i }).click();
}
