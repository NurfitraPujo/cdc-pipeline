import { snakeToCamel } from "@/api/mappers";

export function jsonToUpdateRequest(
	jsonContent: string,
): Record<string, unknown> {
	// T2-8: the pipeline JSON scaffold is emitted with `//` comment headers
	// (see `pipelineToJson` in `routes/pipelines/$id/edit.tsx`). Strip both
	// `#` and `//` comment lines before handing the result to `JSON.parse`.
	const cleanedContent = jsonContent
		.split("\n")
		.filter((line) => {
			const trimmed = line.trim();
			return !trimmed.startsWith("#") && !trimmed.startsWith("//");
		})
		.join("\n");

	let parsed: unknown;
	try {
		parsed = JSON.parse(cleanedContent);
	} catch {
		throw new Error(
			"Invalid configuration format. Please ensure valid JSON syntax.",
		);
	}

	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
		throw new Error("Configuration must be a JSON object");
	}

	return snakeToCamel<Record<string, unknown>>(
		parsed as Record<string, unknown>,
	);
}
