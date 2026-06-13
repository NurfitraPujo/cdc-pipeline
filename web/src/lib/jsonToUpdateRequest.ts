import { snakeToCamel } from "@/api/mappers";

export function jsonToUpdateRequest(
	jsonContent: string,
): Record<string, unknown> {
	const cleanedContent = jsonContent
		.split("\n")
		.filter((line) => !line.trim().startsWith("#"))
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
