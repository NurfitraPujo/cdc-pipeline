import { describe, expect, it } from "vitest";
import {
	bareTableName,
	filterGroupedTables,
	groupTablesBySchema,
	schemaOf,
} from "./tableGrouping";

describe("schemaOf", () => {
	it("reads the schema from a qualified name", () => {
		expect(schemaOf("custom_objects.visitations")).toBe("custom_objects");
	});

	// getTables emits a bare name for public, never "public.orders".
	it("treats a bare name as public", () => {
		expect(schemaOf("orders")).toBe("public");
	});
});

describe("bareTableName", () => {
	it("strips the schema prefix", () => {
		expect(bareTableName("custom_objects.visitations")).toBe("visitations");
	});

	it("leaves a bare name alone", () => {
		expect(bareTableName("orders")).toBe("orders");
	});
});

describe("groupTablesBySchema", () => {
	it("groups public and non-public tables", () => {
		const grouped = groupTablesBySchema([
			"orders",
			"custom_objects.visitations",
			"customers",
			"custom_objects.assets",
		]);

		expect([...grouped.keys()]).toEqual(["custom_objects", "public"]);
		expect(grouped.get("custom_objects")).toEqual([
			"custom_objects.assets",
			"custom_objects.visitations",
		]);
		expect(grouped.get("public")).toEqual(["customers", "orders"]);
	});

	// The grouped values stay fully qualified: they are written straight into
	// PipelineConfig.Tables, which needs the schema to disambiguate.
	it("keeps the qualified form as the stored value", () => {
		const grouped = groupTablesBySchema(["sales.orders"]);
		expect(grouped.get("sales")).toEqual(["sales.orders"]);
	});

	// A stable order means the chip list does not reshuffle between refetches.
	it("sorts schemas and tables", () => {
		const grouped = groupTablesBySchema([
			"zeta.b",
			"alpha.z",
			"zeta.a",
			"alpha.a",
		]);
		expect([...grouped.keys()]).toEqual(["alpha", "zeta"]);
		expect(grouped.get("zeta")).toEqual(["zeta.a", "zeta.b"]);
	});

	it("handles an empty list", () => {
		expect(groupTablesBySchema([]).size).toBe(0);
	});

	// A table name containing a dot would break the schema split; ParseTableRef
	// rejects those upstream, so only the first dot is treated as a separator.
	it("splits on the first dot only", () => {
		const grouped = groupTablesBySchema(["a.b.c"]);
		expect([...grouped.keys()]).toEqual(["a"]);
		expect(bareTableName("a.b.c")).toBe("b.c");
	});
});

describe("filterGroupedTables", () => {
	const grouped = groupTablesBySchema([
		"orders",
		"custom_objects.visitations",
		"custom_objects.assets",
	]);

	it("returns everything for an empty filter", () => {
		expect(filterGroupedTables(grouped, "")).toBe(grouped);
		expect(filterGroupedTables(grouped, "   ")).toBe(grouped);
	});

	it("matches case-insensitively on the qualified name", () => {
		const filtered = filterGroupedTables(grouped, "VISIT");
		expect([...filtered.keys()]).toEqual(["custom_objects"]);
		expect(filtered.get("custom_objects")).toEqual([
			"custom_objects.visitations",
		]);
	});

	// Matching the schema portion keeps a whole schema visible, which is how an
	// operator narrows to "just custom_objects".
	it("matches on the schema portion too", () => {
		const filtered = filterGroupedTables(grouped, "custom_objects");
		expect(filtered.get("custom_objects")).toHaveLength(2);
		expect(filtered.has("public")).toBe(false);
	});

	it("drops schemas with no matches entirely", () => {
		expect(filterGroupedTables(grouped, "nothing-matches").size).toBe(0);
	});
});
