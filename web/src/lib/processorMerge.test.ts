import { describe, expect, test } from "vitest";
import { deepMergeOptions } from "./processorMerge";

describe("deepMergeOptions", () => {
	test("undefined existing -> template", () => {
		expect(deepMergeOptions(undefined, { a: 1 })).toEqual({ a: 1 });
	});
	test("null existing -> template", () => {
		expect(deepMergeOptions(null, { a: 1 })).toEqual({ a: 1 });
	});
	test("empty object existing -> template", () => {
		expect(deepMergeOptions({}, { a: 1 })).toEqual({ a: 1 });
	});
	test("preserves extra keys in existing", () => {
		expect(deepMergeOptions({ a: 1 }, { b: 2 })).toEqual({ a: 1, b: 2 });
	});
	test("template wins on conflict", () => {
		expect(deepMergeOptions({ a: 1 }, { a: 2 })).toEqual({ a: 2 });
	});
	test("deep merges nested objects", () => {
		expect(
			deepMergeOptions({ a: { x: 1, y: 2 } }, { a: { y: 99, z: 3 } }),
		).toEqual({ a: { x: 1, y: 99, z: 3 } });
	});
	test("arrays are appended", () => {
		expect(deepMergeOptions([1, 2], [3, 4])).toEqual([1, 2, 3, 4]);
	});
	test("type change: string replaced by object", () => {
		expect(deepMergeOptions({ a: "x" }, { a: { y: 1 } })).toEqual({
			a: { y: 1 },
		});
	});
	test("object replaced by string", () => {
		expect(deepMergeOptions({ a: 1 }, "string")).toBe("string");
	});
	test("strings: template wins", () => {
		expect(deepMergeOptions("a", "b")).toBe("b");
	});
});
