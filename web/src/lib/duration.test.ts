import { describe, expect, test } from "vitest";
import { isValidDuration, parseDuration } from "./duration";

describe("duration", () => {
	describe("single units", () => {
		test("1ns", () => {
			expect(isValidDuration("1ns")).toBe(true);
			expect(parseDuration("1ns")).toBe(0.000001);
		});
		test("1us", () => {
			expect(isValidDuration("1us")).toBe(true);
			expect(parseDuration("1us")).toBe(0.001);
		});
		test("1µs", () => {
			expect(isValidDuration("1µs")).toBe(true);
			expect(parseDuration("1µs")).toBe(0.001);
		});
		test("1μs", () => {
			expect(isValidDuration("1μs")).toBe(true);
			expect(parseDuration("1μs")).toBe(0.001);
		});
		test("1ms", () => {
			expect(isValidDuration("1ms")).toBe(true);
			expect(parseDuration("1ms")).toBe(1);
		});
		test("1s", () => {
			expect(isValidDuration("1s")).toBe(true);
			expect(parseDuration("1s")).toBe(1000);
		});
		test("1m", () => {
			expect(isValidDuration("1m")).toBe(true);
			expect(parseDuration("1m")).toBe(60_000);
		});
		test("1h", () => {
			expect(isValidDuration("1h")).toBe(true);
			expect(parseDuration("1h")).toBe(3_600_000);
		});
	});

	describe("decimals", () => {
		test("0.5s = 500ms", () => {
			expect(isValidDuration("0.5s")).toBe(true);
			expect(parseDuration("0.5s")).toBe(500);
		});
		test("1.5h = 5,400,000ms", () => {
			expect(isValidDuration("1.5h")).toBe(true);
			expect(parseDuration("1.5h")).toBe(5_400_000);
		});
		test("0.001ms = 0.001 (sub-millisecond)", () => {
			expect(isValidDuration("0.001ms")).toBe(true);
			expect(parseDuration("0.001ms")).toBe(0.001);
		});
	});

	describe("compounds", () => {
		test("1h30m = 5,400,000ms", () => {
			expect(isValidDuration("1h30m")).toBe(true);
			expect(parseDuration("1h30m")).toBe(5_400_000);
		});
		test("2h45m30s = 9,930,000ms", () => {
			expect(isValidDuration("2h45m30s")).toBe(true);
			expect(parseDuration("2h45m30s")).toBe(9_930_000);
		});
		test("1h1h = 7,200,000ms (summed)", () => {
			expect(isValidDuration("1h1h")).toBe(true);
			expect(parseDuration("1h1h")).toBe(7_200_000);
		});
		test("1s500ms = 1,500ms", () => {
			expect(isValidDuration("1s500ms")).toBe(true);
			expect(parseDuration("1s500ms")).toBe(1500);
		});
		test("2h45m30s500ms = 9,930,500ms", () => {
			expect(isValidDuration("2h45m30s500ms")).toBe(true);
			expect(parseDuration("2h45m30s500ms")).toBe(9_930_500);
		});
	});

	describe("case sensitivity", () => {
		test("1S is invalid", () => {
			expect(isValidDuration("1S")).toBe(false);
		});
		test("1MS is invalid", () => {
			expect(isValidDuration("1MS")).toBe(false);
		});
		test("1Ms is invalid", () => {
			expect(isValidDuration("1Ms")).toBe(false);
		});
	});

	describe("invalid inputs", () => {
		const invalids = [
			"",
			"5x",
			"1.2.3s",
			"1 m",
			"+1s",
			"-1s",
			"1",
			"s",
			"1.",
			"1s5",
			"abc",
			"1s ",
			" 1s",
			"1s2",
		];
		for (const s of invalids) {
			test(`isValidDuration(${JSON.stringify(s)}) is false`, () => {
				expect(isValidDuration(s)).toBe(false);
			});
			test(`parseDuration(${JSON.stringify(s)}) throws`, () => {
				expect(() => parseDuration(s)).toThrow();
			});
		}
	});
});
