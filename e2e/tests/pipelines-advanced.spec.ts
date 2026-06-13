import { expect, test } from "@playwright/test";

test.use({ storageState: ".auth/admin.json" });

test.describe("Pipelines - Advanced Configuration", () => {
	const uniqueId = () =>
		`adv-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

	test("create pipeline with advanced overrides", async ({ page }) => {
		const id = uniqueId();
		await page.goto("/pipelines/create");

		// Pipeline ID input has no associated <label>; use placeholder.
		await page
			.getByPlaceholder(/e\.g\., postgres-to-kafka/)
			.fill(id);
		// Pick the first source (radio) and first sink (checkbox).
		await page.locator('input[name="source"]').first().check();
		await page.locator('input[name="sink"]').first().check();

		// Expand Batch & Performance
		await page.getByRole("button", { name: /Batch & Performance/ }).click();
		await page.getByLabel("Batch Size (messages)").fill("50");
		await page.getByLabel("Batch Wait (duration)").fill("2s");

		// Expand Retry & Error Handling
		await page.getByRole("button", { name: /Retry & Error Handling/ }).click();
		await page.getByLabel("Max Retries").fill("5");
		await page.getByLabel("Initial Backoff").fill("500ms");
		await page.getByLabel("Max Backoff").fill("5s");
		await page.getByLabel("Enable Dead Letter Queue").click();

		// Intercept the POST /pipelines to assert the snake_case keys are sent.
		// The client base URL is http://localhost:8080/api/v1, so the final URL
		// is .../api/v1/pipelines (i.e. endsWith("/pipelines")).
		const postPromise = page.waitForRequest(
			(req) =>
				req.url().endsWith("/pipelines") && req.method() === "POST",
		);
		await page.getByRole("button", { name: "Create Pipeline" }).click();
		const req = await postPromise;
		const body = JSON.parse(req.postData() ?? "{}");
		expect(body.batch_size).toBe(50);
		expect(body.batch_wait).toBe("2s");
		expect(body.retry).toBeTruthy();
		expect(body.retry.max_retries).toBe(5);
		expect(body.retry.initial_interval).toBe("500ms");
		expect(body.retry.max_interval).toBe("5s");
		expect(body.retry.enable_dlq).toBe(true);
	});

	test("invalid batch wait shows inline error and blocks submit", async ({
		page,
	}) => {
		const id = uniqueId();
		await page.goto("/pipelines/create");
		await page
			.getByPlaceholder(/e\.g\., postgres-to-kafka/)
			.fill(id);
		await page.locator('input[name="source"]').first().check();
		await page.locator('input[name="sink"]').first().check();

		await page.getByRole("button", { name: /Batch & Performance/ }).click();
		await page.getByLabel("Batch Wait (duration)").fill("5x");

		const errorLocator = page.locator("text=Invalid duration format");
		await expect(errorLocator).toBeVisible();

		// Submit the form. The page should NOT navigate away and no POST /pipelines
		// should fire. NOTE: in the current create.tsx, handleCreate() does not
		// read advanced-panel errors, so the POST may still fire. This assertion
		// documents the desired behaviour; if it fails, the parent form needs to
		// be wired to surface AdvancedConfigPanel's `errors` prop.
		let posted = false;
		page.on("request", (req) => {
			if (req.url().endsWith("/pipelines") && req.method() === "POST") {
				posted = true;
			}
		});
		await page.getByRole("button", { name: "Create Pipeline" }).click();
		await page.waitForTimeout(500);
		expect(posted).toBe(false);
	});

	test("edit pipeline preserves advanced fields when JSON omits them", async ({
		page,
	}) => {
		// 1. List existing pipelines and open the first one (detail page).
		await page.goto("/pipelines");
		const firstPipelineLink = page
			.locator('table button:has-text(""), table a[href*="/pipelines/"]')
			.first();
		// The pipeline id/name is rendered as a <button> in the table cell, so
		// fall back to any link in the first row of the table.
		const rowLink = page
			.locator('table a[href*="/pipelines/"], table button[onclick*="/pipelines/"]')
			.first();
		const candidate = (await firstPipelineLink.count()) > 0
			? firstPipelineLink
			: rowLink;
		if ((await candidate.count()) === 0) {
			test.skip();
			return;
		}
		await candidate.click();
		await expect(page).toHaveURL(/\/pipelines\/[^/]+$/);

		// 2. From the detail page, click the "Edit" link to navigate to the edit page.
		const editLink = page.getByRole("link", { name: /^Edit$/ });
		if ((await editLink.count()) === 0) {
			test.skip();
			return;
		}
		await editLink.click();
		await expect(page).toHaveURL(/\/pipelines\/.+\/edit/);

		// 3. Capture the current JSON value from Monaco's hidden textarea.
		const editorContent = await page
			.locator("textarea.inputarea")
			.first()
			.inputValue();
		// The editor prefixes the JSON with `//` comment lines. Strip both
		// `//` and `#` line comments so JSON.parse succeeds.
		const jsonOnly = editorContent
			.split("\n")
			.filter((line) => {
				const trimmed = line.trim();
				return !trimmed.startsWith("//") && !trimmed.startsWith("#");
			})
			.join("\n");
		const parsed = JSON.parse(jsonOnly) as { retry?: unknown };
		expect(parsed.retry).toBeDefined();

		// 4. Remove `retry` from the JSON, set the value, save.
		const { retry: _omitted, ...rest } = parsed;
		const updatedJson = JSON.stringify(rest, null, 2);
		await page.locator("textarea.inputarea").first().fill(updatedJson);

		// PUT goes to .../api/v1/pipelines/{id} (no trailing path segment).
		const putPromise = page.waitForRequest(
			(req) =>
				/\/pipelines\/[^/]+$/.test(req.url()) && req.method() === "PUT",
		);
		await page.getByRole("button", { name: "Save" }).click();
		const putReq = await putPromise;
		const putBody = JSON.parse(putReq.postData() ?? "{}");
		expect(putBody.retry).toBeDefined();
	});
});
