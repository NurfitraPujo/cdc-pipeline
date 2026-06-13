import { snakeToCamel } from "@/api/mappers";
import type { Pipeline, UpdatePipelineRequest } from "@/api/pipelines";

function isPlainObject(v: unknown): v is Record<string, unknown> {
	return typeof v === "object" && v !== null && !Array.isArray(v);
}

export function mergeWithCurrent(
	parsed: Record<string, unknown>,
	current: Pipeline,
): UpdatePipelineRequest {
	const normalized = snakeToCamel<Record<string, unknown>>(parsed);

	const id = normalized.id;
	if (typeof id !== "string" || id.length === 0) {
		throw new Error("Missing required field: id");
	}
	const name = normalized.name;
	if (typeof name !== "string" || name.length === 0) {
		throw new Error("Missing required field: name");
	}
	if (!Array.isArray(normalized.sources) || normalized.sources.length === 0) {
		throw new Error("Missing required field: sources");
	}
	if (!Array.isArray(normalized.sinks) || normalized.sinks.length === 0) {
		throw new Error("Missing required field: sinks");
	}
	if (!Array.isArray(normalized.tables) || normalized.tables.length === 0) {
		throw new Error("Missing required field: tables");
	}

	const result: Record<string, unknown> = {
		id,
		name,
		sources: normalized.sources,
		sinks: normalized.sinks,
		tables: normalized.tables,
	};

	const batchSize = normalized.batchSize;
	if (
		batchSize !== undefined &&
		batchSize !== null &&
		batchSize !== "" &&
		batchSize !== 0
	) {
		result.batchSize = Number(batchSize);
	} else if (current.batchSize !== undefined) {
		result.batchSize = current.batchSize;
	}

	const batchWait = normalized.batchWait;
	if (batchWait !== undefined && batchWait !== null && batchWait !== "") {
		result.batchWait = batchWait;
	} else if (current.batchWait !== undefined) {
		result.batchWait = current.batchWait;
	}

	const retry = normalized.retry;
	if (retry === undefined || retry === null) {
		if (current.retry !== undefined) {
			result.retry = current.retry;
		}
	} else if (isPlainObject(retry)) {
		result.retry = retry;
	}

	const processors = normalized.processors;
	if (processors === undefined) {
		if (current.processors !== undefined) {
			result.processors = current.processors;
		}
	} else if (Array.isArray(processors)) {
		result.processors = processors;
	}

	return result as UpdatePipelineRequest;
}
