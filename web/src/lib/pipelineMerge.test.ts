import { describe, expect, test } from "vitest";
import type { Pipeline } from "@/api/pipelines";
import { mergeWithCurrent } from "./pipelineMerge";

function makeCurrent(overrides: Partial<Pipeline> = {}): Pipeline {
	return {
		id: "p1",
		name: "Pipeline 1",
		sources: ["s1"],
		sinks: ["k1"],
		tables: ["users"],
		batchSize: 100,
		batchWait: "1s",
		retry: {
			maxRetries: 5,
			enableDlq: false,
		} as unknown as Pipeline["retry"],
		processors: [
			{ name: "a", type: "filter" },
			{ name: "b", type: "mask" },
		],
		...overrides,
	};
}

function makeParsed(
	overrides: Record<string, unknown> = {},
): Record<string, unknown> {
	return {
		id: "p1",
		name: "Pipeline 1",
		sources: ["s1"],
		sinks: ["k1"],
		tables: ["users"],
		...overrides,
	};
}

describe("mergeWithCurrent", () => {
	test("returns all fields as-is when parsed has everything", () => {
		const parsed = makeParsed({
			batchSize: 50,
			batchWait: "2s",
			retry: { maxRetries: 3, enableDlq: true },
			processors: [{ name: "x", type: "filter" }],
		});
		const current = makeCurrent();
		expect(mergeWithCurrent(parsed, current)).toEqual({
			id: "p1",
			name: "Pipeline 1",
			sources: ["s1"],
			sinks: ["k1"],
			tables: ["users"],
			batchSize: 50,
			batchWait: "2s",
			retry: { maxRetries: 3, enableDlq: true },
			processors: [{ name: "x", type: "filter" }],
		});
	});

	test("falls back to current optionals when parsed omits them", () => {
		const parsed = makeParsed();
		const current = makeCurrent();
		const result = mergeWithCurrent(parsed, current);
		expect(result.batchSize).toBe(100);
		expect(result.batchWait).toBe("1s");
		expect(result.retry).toEqual({ maxRetries: 5, enableDlq: false });
		expect(result.processors).toHaveLength(2);
	});

	test("throws when id is missing", () => {
		const parsed = makeParsed();
		const { id: _id, ...rest } = parsed;
		void _id;
		expect(() => mergeWithCurrent(rest, makeCurrent())).toThrow(
			"Missing required field: id",
		);
	});

	test("throws when sources is missing", () => {
		const parsed = makeParsed();
		const { sources: _sources, ...rest } = parsed;
		void _sources;
		expect(() => mergeWithCurrent(rest, makeCurrent())).toThrow(
			"Missing required field: sources",
		);
	});

	test("throws when sources is empty", () => {
		const parsed = makeParsed({ sources: [] });
		expect(() => mergeWithCurrent(parsed, makeCurrent())).toThrow(
			"Missing required field: sources",
		);
	});

	test("processors: [] explicitly clears", () => {
		const parsed = makeParsed({ processors: [] });
		const current = makeCurrent();
		expect(mergeWithCurrent(parsed, current).processors).toEqual([]);
	});

	test("processors: undefined keeps current", () => {
		const parsed = makeParsed();
		const current = makeCurrent();
		expect(mergeWithCurrent(parsed, current).processors).toHaveLength(2);
	});

	test("batchSize 0 falls back to current", () => {
		const parsed = makeParsed({ batchSize: 0 });
		const current = makeCurrent();
		expect(mergeWithCurrent(parsed, current).batchSize).toBe(100);
	});

	test("batchSize 50 wins over current 100", () => {
		const parsed = makeParsed({ batchSize: 50 });
		const current = makeCurrent();
		expect(mergeWithCurrent(parsed, current).batchSize).toBe(50);
	});

	test("retry: null falls back to current", () => {
		const parsed = makeParsed({ retry: null });
		const current = makeCurrent();
		expect(mergeWithCurrent(parsed, current).retry).toEqual({
			maxRetries: 5,
			enableDlq: false,
		});
	});

	test("retry: object used as-is", () => {
		const parsed = makeParsed({ retry: { maxRetries: 99 } });
		const current = makeCurrent();
		expect(mergeWithCurrent(parsed, current).retry).toEqual({
			maxRetries: 99,
		});
	});

	test("snake_case keys are normalized", () => {
		const parsed = {
			id: "p1",
			name: "Pipeline 1",
			sources: ["s1"],
			sinks: ["k1"],
			tables: ["users"],
			batch_size: 50,
		};
		const current = makeCurrent();
		const result = mergeWithCurrent(parsed, current);
		expect(result.batchSize).toBe(50);
		expect((result as Record<string, unknown>).batch_size).toBeUndefined();
	});
});
