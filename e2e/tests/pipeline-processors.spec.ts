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

let api: APIRequestContext;
const created = {
	pipelines: [] as string[],
	sources: [] as string[],
	sinks: [] as string[],
};

interface WireProcessor {
	name: string;
	type: string;
	options: Record<string, unknown>;
	operation_types: string[];
}

interface WirePipeline {
	id: string;
	name: string;
	batch_wait: string;
	processors?: WireProcessor[];
}

/**
 * An options map that fails loudly if anything in the stack "normalises" keys.
 *
 * `options` is declared `map[string]interface{}` in protocol.ProcessorConfig and
 * is opaque to the server, but the web client runs camelToSnake/snakeToCamel
 * over request and response bodies. A key like `maxLength` used to come back as
 * `max_length`, silently breaking every custom processor.
 */
const OPAQUE_OPTIONS = {
	maxLength: 32,
	Nested_Key: { innerCamelCase: true },
	"dotted.key": "kept",
	UPPER: ["a", "b"],
};

test.beforeAll(async () => {
	api = await createApiContext();
});

test.afterAll(async () => {
	await cleanup(api, created);
	await api.dispose();
});

/** Seeds a pipeline with its source and sink, registered for teardown. */
async function seedPipeline(
	prefix: string,
	extra: Record<string, unknown> = {},
) {
	const sourceId = uniqueId(`${prefix}-src`);
	const sinkId = uniqueId(`${prefix}-snk`);
	const pipelineId = uniqueId(prefix);

	await createSource(api, { id: sourceId });
	created.sources.push(sourceId);
	await createSink(api, sinkId);
	created.sinks.push(sinkId);

	await createPipeline(api, {
		id: pipelineId,
		name: `E2E ${prefix}`,
		sources: [sourceId],
		sinks: [sinkId],
		...extra,
	});
	created.pipelines.push(pipelineId);

	return { pipelineId, sourceId, sinkId };
}

async function getPipeline(id: string): Promise<WirePipeline> {
	const res = await api.get(`/api/v1/pipelines/${id}`);
	expect(res.status()).toBe(200);
	return (await res.json()) as WirePipeline;
}

test.describe("Pipeline processors", () => {
	test("round-trips name, type, options and operation_types verbatim", async () => {
		const { pipelineId } = await seedPipeline("proc-api", {
			processors: [
				{
					name: "redact-email",
					type: "nats/protobuf",
					options: OPAQUE_OPTIONS,
					operation_types: ["insert", "update", "schema_change"],
				},
			],
		});

		const stored = await getPipeline(pipelineId);
		expect(stored.processors).toHaveLength(1);

		const p = (stored.processors ?? [])[0];
		expect(p.name).toBe("redact-email");
		expect(p.type).toBe("nats/protobuf");
		expect(p.operation_types).toEqual(["insert", "update", "schema_change"]);
		// Deep-equal, not key-by-key: this fails on an added, dropped or renamed
		// key anywhere in the tree.
		expect(p.options).toEqual(OPAQUE_OPTIONS);
	});

	test("rejects a processor with no operation types", async () => {
		const sourceId = uniqueId("proc-bad-src");
		const sinkId = uniqueId("proc-bad-snk");
		const pipelineId = uniqueId("proc-bad");

		await createSource(api, { id: sourceId });
		created.sources.push(sourceId);
		await createSink(api, sinkId);
		created.sinks.push(sinkId);

		// A processor matching zero operations is skipped wholesale by the
		// consumer, so ProcessorConfig.Validate requires a non-empty list.
		const res = await api.post("/api/v1/pipelines", {
			data: {
				id: pipelineId,
				name: "E2E proc-bad",
				sources: [sourceId],
				sinks: [sinkId],
				tables: ["public.orders"],
				batch_size: 100,
				batch_wait: "5s",
				processors: [
					{ name: "noop", type: "mask", options: {}, operation_types: [] },
				],
			},
		});

		expect(res.status()).toBe(400);
		// Nothing was persisted, so there is nothing to register for teardown.
		expect((await api.get(`/api/v1/pipelines/${pipelineId}`)).status()).toBe(404);
	});

	test("rejects an unknown operation type", async () => {
		const sourceId = uniqueId("proc-op-src");
		const sinkId = uniqueId("proc-op-snk");
		const pipelineId = uniqueId("proc-op");

		await createSource(api, { id: sourceId });
		created.sources.push(sourceId);
		await createSink(api, sinkId);
		created.sinks.push(sinkId);

		const res = await api.post("/api/v1/pipelines", {
			data: {
				id: pipelineId,
				name: "E2E proc-op",
				sources: [sourceId],
				sinks: [sinkId],
				tables: ["public.orders"],
				batch_size: 100,
				batch_wait: "5s",
				processors: [
					{
						name: "bogus",
						type: "mask",
						options: {},
						operation_types: ["truncate"],
					},
				],
			},
		});

		expect(res.status()).toBe(400);
		expect((await api.get(`/api/v1/pipelines/${pipelineId}`)).status()).toBe(404);
	});

	test("the detail page renders the processor in the raw config", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("proc-detail", {
			processors: [
				{
					name: "redact-email",
					type: "nats/protobuf",
					options: OPAQUE_OPTIONS,
					operation_types: ["insert"],
				},
			],
		});

		await page.goto(`/pipelines/${pipelineId}`);
		const raw = page.locator("pre code");
		await expect(raw).toContainText(`"name": "redact-email"`);
		await expect(raw).toContainText(`"operationTypes"`);
		// The client camelCases envelope keys (operation_types -> operationTypes)
		// but must leave the opaque options map untouched.
		await expect(raw).toContainText(`"maxLength": 32`);
		await expect(raw).not.toContainText("max_length");
	});

	test("the edit form shows the stored processor", async ({ page }) => {
		const { pipelineId } = await seedPipeline("proc-form", {
			processors: [
				{
					name: "redact-email",
					type: "nats/protobuf",
					options: OPAQUE_OPTIONS,
					operation_types: ["insert", "update"],
				},
			],
		});

		await page.goto(`/pipelines/${pipelineId}/edit`);
		await page.getByLabel("Use form view").click();

		await expect(page.getByLabel("Processor name")).toHaveValue(
			"redact-email",
		);
		// These specs used to seed type "custom", which is not registered in
		// internal/transformer -- the worker refuses to construct it, so every
		// pipeline they created was dead on arrival.
		await expect(page.getByLabel("Type")).toContainText("NATS / Protobuf");
		await expect(page.getByRole("checkbox", { name: "Insert" })).toBeChecked();
		await expect(page.getByRole("checkbox", { name: "Update" })).toBeChecked();
		await expect(
			page.getByRole("checkbox", { name: "Delete" }),
		).not.toBeChecked();
		await expect(
			page.getByRole("checkbox", { name: "Schema Change Ack" }),
		).not.toBeChecked();
	});

	test("a UI edit preserves opaque option keys and updates operation types", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("proc-edit", {
			processors: [
				{
					name: "redact-email",
					type: "nats/protobuf",
					options: OPAQUE_OPTIONS,
					operation_types: ["insert"],
				},
			],
		});

		await page.goto(`/pipelines/${pipelineId}/edit`);
		await page.getByLabel("Use form view").click();

		await page.getByLabel("Processor name").fill("redact-email-v2");
		await page.getByRole("checkbox", { name: "Delete" }).click();

		const put = page.waitForRequest(
			(r) =>
				r.url().endsWith(`/pipelines/${pipelineId}`) &&
				r.method() === "PUT",
		);
		await page.getByRole("button", { name: "Save" }).click();
		const body = JSON.parse((await put).postData() ?? "{}") as {
			processors: WireProcessor[];
		};

		// The envelope is snake_cased on the way out...
		expect(body.processors[0].operation_types).toEqual(["insert", "delete"]);
		// ...but the opaque options map is not.
		expect(body.processors[0].options).toEqual(OPAQUE_OPTIONS);

		await expect(page).toHaveURL(new RegExp(`/pipelines/${pipelineId}$`));

		const stored = await getPipeline(pipelineId);
		const p = (stored.processors ?? [])[0];
		expect(p.name).toBe("redact-email-v2");
		expect(p.operation_types).toEqual(["insert", "delete"]);
		expect(p.options).toEqual(OPAQUE_OPTIONS);
	});

	test("removing the only processor clears the list server-side", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("proc-remove", {
			processors: [
				{
					name: "redact-email",
					type: "nats/protobuf",
					options: OPAQUE_OPTIONS,
					operation_types: ["insert"],
				},
			],
		});

		await page.goto(`/pipelines/${pipelineId}/edit`);
		await page.getByLabel("Use form view").click();
		await expect(page.getByLabel("Processor name")).toBeVisible();

		await page.getByRole("button", { name: "Remove processor" }).click();
		await expect(page.getByText("No processors configured.")).toBeVisible();

		const put = page.waitForRequest(
			(r) =>
				r.url().endsWith(`/pipelines/${pipelineId}`) &&
				r.method() === "PUT",
		);
		await page.getByRole("button", { name: "Save" }).click();
		const body = JSON.parse((await put).postData() ?? "{}") as {
			processors?: unknown[];
		};
		// advancedConfigToPayload omits the key entirely for an empty list.
		expect(body.processors).toBeUndefined();

		await expect(page).toHaveURL(new RegExp(`/pipelines/${pipelineId}$`));
		const stored = await getPipeline(pipelineId);
		expect(stored.processors ?? []).toEqual([]);
	});

	test("batch wait survives the form round-trip as a Go duration string", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("proc-batch", {
			batchWait: "5s",
			processors: [
				{
					name: "redact-email",
					type: "nats/protobuf",
					options: OPAQUE_OPTIONS,
					operation_types: ["insert"],
				},
			],
		});

		await page.goto(`/pipelines/${pipelineId}/edit`);
		await page.getByLabel("Use form view").click();

		await expect(page.getByLabel("Batch Wait (duration)")).toHaveValue("5s");
		await page.getByLabel("Batch Wait (duration)").fill("2500ms");

		const put = page.waitForRequest(
			(r) =>
				r.url().endsWith(`/pipelines/${pipelineId}`) &&
				r.method() === "PUT",
		);
		await page.getByRole("button", { name: "Save" }).click();
		const body = JSON.parse((await put).postData() ?? "{}") as {
			batch_wait: string;
		};
		expect(body.batch_wait).toBe("2500ms");

		const stored = await getPipeline(pipelineId);
		// Durations are strings on the wire, never nanosecond integers.
		expect(stored.batch_wait).toBe("2.5s");
		expect((stored.processors ?? [])[0].options).toEqual(OPAQUE_OPTIONS);
	});
});
