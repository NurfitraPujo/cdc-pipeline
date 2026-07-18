import { describe, expect, test } from "vitest";
import { advancedConfigToPayload } from "@/components/pipelines/advancedConfig";

describe("advancedConfigToPayload (T3-4)", () => {
	test("strips the local UI-only `id` from processors before dispatch", () => {
		const payload = advancedConfigToPayload({
			processors: [
				{
					id: "11111111-1111-4111-8111-111111111111",
					name: "row-1",
					type: "mask",
					operationTypes: ["insert"],
					options: { fields: ["email"] },
				},
			],
		});

		expect(payload.processors).toBeDefined();
		expect(payload.processors?.[0]).toEqual({
			name: "row-1",
			type: "mask",
			operationTypes: ["insert"],
			options: { fields: ["email"] },
		});
		expect(
			(payload.processors?.[0] as unknown as Record<string, unknown>).id,
		).toBeUndefined();
	});

	test("strips `id` from every processor, not just the first", () => {
		const payload = advancedConfigToPayload({
			processors: [
				{ id: "a", name: "a", type: "mask", operationTypes: [], options: {} },
				{ id: "b", name: "b", type: "uppercase", operationTypes: [], options: {} },
			],
		});

		expect(payload.processors).toHaveLength(2);
		for (const p of payload.processors ?? []) {
			expect((p as unknown as Record<string, unknown>).id).toBeUndefined();
		}
	});

	test("still includes processors that never had an id", () => {
		const payload = advancedConfigToPayload({
			processors: [
				{ name: "legacy", type: "mask", operationTypes: [], options: {} },
			],
		});

		expect(payload.processors?.[0]).toEqual({
			name: "legacy",
			type: "mask",
			operationTypes: [],
			options: {},
		});
	});
});
