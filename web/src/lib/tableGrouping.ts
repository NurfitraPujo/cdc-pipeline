/**
 * Helpers for presenting a source's discovered tables by schema.
 *
 * `sourcesApi.getTables` returns identifiers in the dot form that
 * `PipelineConfig.Tables` accepts: `schema.table`, or a bare `table` for the
 * public schema (see the comment on that function and MULTI_SCHEMA_PLAN.md
 * §2.1-2.3). These helpers only ever *read* that form -- the stored value
 * stays fully qualified so it round-trips to the backend unchanged.
 */

/** The schema a qualified table belongs to. A bare name means public. */
export function schemaOf(qualifiedTable: string): string {
	const dot = qualifiedTable.indexOf(".");
	return dot === -1 ? "public" : qualifiedTable.slice(0, dot);
}

/** The table name without its schema prefix. */
export function bareTableName(qualifiedTable: string): string {
	const dot = qualifiedTable.indexOf(".");
	return dot === -1 ? qualifiedTable : qualifiedTable.slice(dot + 1);
}

/**
 * Group qualified table identifiers by schema.
 *
 * Schemas are ordered alphabetically and tables within each schema are sorted,
 * so the rendered list is stable across refetches rather than following
 * whatever order discovery happened to return.
 */
export function groupTablesBySchema(
	tables: readonly string[],
): Map<string, string[]> {
	const groups = new Map<string, string[]>();

	for (const qualified of tables) {
		const schema = schemaOf(qualified);
		const group = groups.get(schema) ?? [];
		group.push(qualified);
		groups.set(schema, group);
	}

	for (const group of groups.values()) {
		group.sort();
	}

	return new Map([...groups.entries()].sort(([a], [b]) => a.localeCompare(b)));
}

/**
 * Narrow grouped tables to those matching a case-insensitive substring.
 * Schemas left with no matches are dropped entirely.
 */
export function filterGroupedTables(
	grouped: Map<string, string[]>,
	filter: string,
): Map<string, string[]> {
	const needle = filter.trim().toLowerCase();
	if (!needle) return grouped;

	const filtered = new Map<string, string[]>();
	for (const [schema, group] of grouped) {
		const matches = group.filter((t) => t.toLowerCase().includes(needle));
		if (matches.length > 0) filtered.set(schema, matches);
	}
	return filtered;
}
