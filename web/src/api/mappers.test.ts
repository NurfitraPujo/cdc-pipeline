import { describe, expect, it } from "vitest";
import { camelToSnake, snakeToCamel } from "./mappers";

describe("mappers", () => {
	describe("snakeToCamel", () => {
		it("converts top-level snake keys to camelCase", () => {
			expect(snakeToCamel({ batch_size: 10, total_pipelines: 5 })).toEqual({
				batchSize: 10,
				totalPipelines: 5,
			});
		});

		it("recurses into nested objects", () => {
			expect(
				snakeToCamel({
					outer_key: { inner_key: "value", deep: { last_source_ts: "x" } },
				}),
			).toEqual({
				outerKey: { innerKey: "value", deep: { lastSourceTs: "x" } },
			});
		});

		it("walks arrays of objects", () => {
			expect(
				snakeToCamel({
					pipelines: [{ total_synced: 1 }, { total_synced: 2 }],
				}),
			).toEqual({ pipelines: [{ totalSynced: 1 }, { totalSynced: 2 }] });
		});

		it("passes primitives, null, undefined through unchanged", () => {
			expect(snakeToCamel(null)).toBeNull();
			expect(snakeToCamel(undefined)).toBeUndefined();
			expect(snakeToCamel(0)).toBe(0);
			expect(snakeToCamel("hello")).toBe("hello");
			expect(snakeToCamel(false)).toBe(false);
		});

		it("leaves already-camelCase keys alone", () => {
			expect(snakeToCamel({ alreadyCamel: 1, snake_case: 2 })).toEqual({
				alreadyCamel: 1,
				snakeCase: 2,
			});
		});

		it("does not mutate the input object", () => {
			const input = { snake_case: { nested_key: 1 } };
			const snapshot = JSON.parse(JSON.stringify(input));
			snakeToCamel(input);
			expect(input).toEqual(snapshot);
		});

		it("handles free-form additionalProperties payloads", () => {
			expect(
				snakeToCamel({
					pipeline_id: "p1",
					status: {
						"cdc.pipeline.p1.sources.s1.tables.t1.stats": { lag_ms: 5 },
					},
				}),
			).toEqual({
				pipelineId: "p1",
				status: { "cdc.pipeline.p1.sources.s1.tables.t1.stats": { lagMs: 5 } },
			});
		});

		it("does not recurse into Date instances", () => {
			const d = new Date("2025-01-01T00:00:00Z");
			const out = snakeToCamel({ updated_at: d });
			expect(out).toEqual({ updatedAt: d });
		});
	});

	describe("camelToSnake", () => {
		it("converts camelCase keys to snake_case", () => {
			expect(camelToSnake({ batchSize: 10, totalPipelines: 5 })).toEqual({
				batch_size: 10,
				total_pipelines: 5,
			});
		});

		it("recurses into nested objects", () => {
			expect(
				camelToSnake({
					outerKey: { innerKey: "value", deep: { lastSourceTs: "x" } },
				}),
			).toEqual({
				outer_key: { inner_key: "value", deep: { last_source_ts: "x" } },
			});
		});

		it("walks arrays", () => {
			expect(
				camelToSnake({ items: [{ totalSynced: 1 }, { totalSynced: 2 }] }),
			).toEqual({ items: [{ total_synced: 1 }, { total_synced: 2 }] });
		});

		it("passes primitives and null through unchanged", () => {
			expect(camelToSnake(null)).toBeNull();
			expect(camelToSnake(0)).toBe(0);
			expect(camelToSnake("hello")).toBe("hello");
		});
	});

	describe("round-trip", () => {
		it("snake -> camel -> snake returns the original shape", () => {
			const original = {
				pipeline_id: "p1",
				total_pipelines: 5,
				tables: ["users", "orders"],
				tables_meta: { avg_lag_ms: 100, total_rows_synced: 1000 },
			};
			expect(camelToSnake(snakeToCamel(original))).toEqual(original);
		});

		it("camel -> snake -> camel returns the original shape", () => {
			const original = {
				pipelineId: "p1",
				totalPipelines: 5,
				tables: ["users", "orders"],
				tablesMeta: { avgLagMs: 100, totalRowsSynced: 1000 },
			};
			expect(snakeToCamel(camelToSnake(original))).toEqual(original);
		});
	});
});
