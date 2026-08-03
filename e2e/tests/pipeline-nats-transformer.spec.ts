import type { APIRequestContext } from "@playwright/test";
import { expect, test } from "@playwright/test";
import {
	cleanup,
	createApiContext,
	createPipeline,
	createSink,
	createSource,
	uniqueId,
} from "./helpers/api";
import { gotoHydrated } from "./helpers/ui";

let api: APIRequestContext;
const created = {
	pipelines: [] as string[],
	sources: [] as string[],
	sinks: [] as string[],
};

let sourceId: string;
let sinkId: string;

test.beforeAll(async () => {
	api = await createApiContext();

	sourceId = uniqueId("nats-src");
	sinkId = uniqueId("nats-snk");
	await createSource(api, { id: sourceId });
	created.sources.push(sourceId);
	await createSink(api, sinkId);
	created.sinks.push(sinkId);
});

test.afterAll(async () => {
	await cleanup(api, created);
	await api.dispose();
});

/**
 * The e2e stack runs the API but no Postgres, so table discovery fails and the
 * create form has no discovered tables to offer. The transformer's
 * schemas/tables pickers therefore fall back to their free-text inputs, which
 * is the path these tests drive. The grouped-picker rendering is covered by
 * unit tests instead (web/src/lib/tableGrouping.test.ts and
 * web/src/test/components/NatsProtobufOptions.test.tsx).
 */
async function startFormWithProcessor(
	page: import("@playwright/test").Page,
	id: string,
) {
	await gotoHydrated(page, "/pipelines/create", "#pipeline-id");
	await page.getByLabel("Pipeline ID").fill(id);
	await page.locator('input[name="source"]').first().check();
	await page.locator('input[name="sink"]').first().check();

	await page.getByRole("button", { name: /Processors/ }).click();
	await page.getByRole("button", { name: /Add processor/ }).click();
}

test.describe("nats/protobuf transformer", () => {
	test("is offered as a processor type", async ({ page }) => {
		await startFormWithProcessor(page, uniqueId("nats-list"));

		await page.getByLabel("Type").click();

		// The registered transformer types, per RegisterTransformer call sites.
		await expect(
			page.getByRole("option", { name: "NATS / Protobuf (daya-core)" }),
		).toBeVisible();
		await expect(page.getByRole("option", { name: "Mask" })).toBeVisible();
		await expect(page.getByRole("option", { name: "Uppercase" })).toBeVisible();

		// "Custom" is not registered anywhere in internal/transformer. Offering
		// it produced pipelines the API accepted with 201 and the worker then
		// refused to construct.
		await expect(page.getByRole("option", { name: "Custom" })).toHaveCount(0);
	});

	test("configures a transformer through the form and the API stores it", async ({
		page,
	}) => {
		const id = uniqueId("nats-cfg");
		created.pipelines.push(id);

		await startFormWithProcessor(page, id);

		await page.getByLabel("Processor name").fill("daya-core");
		await page.getByLabel("Type").click();
		await page
			.getByRole("option", { name: "NATS / Protobuf (daya-core)" })
			.click();

		await page.getByLabel("NATS URL").fill("nats://core.internal:4222");
		await page.getByLabel("Subject").fill("daya.core.transform");
		await page.getByLabel(/Request timeout/).fill("20000");

		// No discovered tables here, so both filters are free-text.
		const schemaInput = page.getByPlaceholder(/Add a schema/);
		await schemaInput.fill("custom_objects");
		await schemaInput.press("Enter");

		const tableInput = page.getByPlaceholder(/Add a table name/);
		await tableInput.fill("visitations");
		await tableInput.press("Enter");

		for (const op of ["Insert", "Update", "Delete"]) {
			await page.getByRole("checkbox", { name: op }).check();
		}

		const postPromise = page.waitForRequest(
			(req) => req.url().endsWith("/pipelines") && req.method() === "POST",
		);
		await page.getByRole("button", { name: "Create Pipeline" }).click();

		const body = JSON.parse((await postPromise).postData() ?? "{}");
		expect(body.processors).toHaveLength(1);
		expect(body.processors[0].type).toBe("nats/protobuf");
		expect(body.processors[0].name).toBe("daya-core");

		await expect(page).toHaveURL(/\/pipelines$/);

		// Verify server-side, and that the opaque option keys survived verbatim
		// -- snake_case keys like nats_url must not be case-converted.
		const stored = await (await api.get(`/api/v1/pipelines/${id}`)).json();
		const proc = stored.processors[0];

		expect(proc.type).toBe("nats/protobuf");
		expect(proc.options.nats_url).toBe("nats://core.internal:4222");
		expect(proc.options.subject).toBe("daya.core.transform");
		expect(proc.options.timeout_ms).toBe(20000);
		expect(proc.options.schemas).toEqual(["custom_objects"]);
		// Stored bare: matchesTable compares against m.Table, which has no schema.
		expect(proc.options.tables).toEqual(["visitations"]);
		expect(proc.operation_types).toEqual(
			expect.arrayContaining(["insert", "update", "delete"]),
		);

		// pipeline_id and batch_size are injected by engine/factory.go and must
		// not be written by the form.
		expect(proc.options.pipeline_id).toBeUndefined();
		expect(proc.options.batch_size).toBeUndefined();
	});

	test("an unrecognised stored type survives a form-view save", async ({
		page,
	}) => {
		// A type written through the raw JSON editor, or left over from an
		// older config. The dropdown used to render blank for these, so the
		// next save silently rewrote the processor's type.
		const id = uniqueId("nats-unknown");
		await createPipeline(api, {
			id,
			name: `E2E ${id}`,
			sources: [sourceId],
			sinks: [sinkId],
			processors: [
				{
					name: "legacy",
					type: "ddl/add_column",
					options: { column: "x" },
					operation_types: ["schema_change"],
				},
			],
		});
		created.pipelines.push(id);

		await page.goto(`/pipelines/${id}/edit`);
		await page.getByLabel("Use form view").click();

		await expect(page.getByLabel("Type")).toContainText("ddl/add_column");

		// Change something unrelated, save, and confirm the type is intact.
		await page.getByLabel("Processor name").fill("legacy-renamed");

		const putPromise = page.waitForRequest(
			(req) => /\/pipelines\/[^/]+$/.test(req.url()) && req.method() === "PUT",
		);
		await page.getByRole("button", { name: "Save" }).click();

		const putBody = JSON.parse((await putPromise).postData() ?? "{}");
		expect(putBody.processors[0].type).toBe("ddl/add_column");

		const stored = await (await api.get(`/api/v1/pipelines/${id}`)).json();
		expect(stored.processors[0].type).toBe("ddl/add_column");
		expect(stored.processors[0].name).toBe("legacy-renamed");
	});
});
