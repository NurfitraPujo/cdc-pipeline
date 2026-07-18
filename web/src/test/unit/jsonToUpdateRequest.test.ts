import { describe, expect, test } from "vitest";
import { jsonToUpdateRequest } from "@/lib/jsonToUpdateRequest";

describe("jsonToUpdateRequest", () => {
	test("converts pipelineToJson-shaped input with snake_case keys to camelCase", () => {
		const json = `{
  "id": "p1",
  "name": "Pipeline 1",
  "sources": ["s1"],
  "sinks": ["k1"],
  "tables": ["users"],
  "batch_size": 100,
  "batch_wait": "1s",
  "retry": { "max_retries": 5 }
}`;
		const result = jsonToUpdateRequest(json);
		expect(result).toEqual({
			id: "p1",
			name: "Pipeline 1",
			sources: ["s1"],
			sinks: ["k1"],
			tables: ["users"],
			batchSize: 100,
			batchWait: "1s",
			retry: { maxRetries: 5 },
		});
		expect((result as Record<string, unknown>).batch_size).toBeUndefined();
	});

	test("strips lines starting with # before parsing", () => {
		const json = `# top comment
{
  # inside comment
  "id": "p1",
  "name": "Pipeline 1"
}`;
		const result = jsonToUpdateRequest(json);
		expect(result).toEqual({ id: "p1", name: "Pipeline 1" });
	});

	test("strips lines starting with // before parsing (T2-8)", () => {
		const json = `// top comment
{
  // inside comment
  "id": "p1",
  "name": "Pipeline 1"
}`;
		const result = jsonToUpdateRequest(json);
		expect(result).toEqual({ id: "p1", name: "Pipeline 1" });
	});

	test("strips mixed # and // comment headers as emitted by pipelineToJson", () => {
		const json = `// Pipeline Configuration
// Edit the configuration below and click Save to apply changes
{
  "id": "p1",
  "name": "Pipeline 1",
  "sources": ["s1"],
  "sinks": ["k1"],
  "tables": ["users"]
}`;
		const result = jsonToUpdateRequest(json);
		expect(result).toEqual({
			id: "p1",
			name: "Pipeline 1",
			sources: ["s1"],
			sinks: ["k1"],
			tables: ["users"],
		});
	});

	test("does not strip // inside string values", () => {
		const json = `{
  "name": "http://example.com/path"
}`;
		const result = jsonToUpdateRequest(json);
		expect(result).toEqual({ name: "http://example.com/path" });
	});

	test("does not strip inline trailing # or // that are not at line start", () => {
		const json = `{
  "id": "p1#frag",
  "name": "Pipeline // 1"
}`;
		const result = jsonToUpdateRequest(json);
		expect(result).toEqual({ id: "p1#frag", name: "Pipeline // 1" });
	});

	test("returns empty object for {}", () => {
		const result = jsonToUpdateRequest("{}");
		expect(result).toEqual({});
	});

	test("throws on invalid JSON", () => {
		expect(() => jsonToUpdateRequest("{ not json")).toThrow(
			/Invalid configuration format/,
		);
	});

	test("throws on array input", () => {
		expect(() => jsonToUpdateRequest("[1, 2, 3]")).toThrow(
			"Configuration must be a JSON object",
		);
	});

	test("throws on primitive input", () => {
		expect(() => jsonToUpdateRequest("42")).toThrow(
			"Configuration must be a JSON object",
		);
		expect(() => jsonToUpdateRequest('"hello"')).toThrow(
			"Configuration must be a JSON object",
		);
		expect(() => jsonToUpdateRequest("null")).toThrow(
			"Configuration must be a JSON object",
		);
	});

	test("recurses into nested objects", () => {
		const result = jsonToUpdateRequest(
			'{"retry": { "max_retries": 5, "enable_dlq": true }}',
		);
		expect(result).toEqual({
			retry: { maxRetries: 5, enableDlq: true },
		});
	});

	test("recurses into array elements", () => {
		const result = jsonToUpdateRequest(
			'{"processors": [{ "operation_types": ["insert"], "max_retries": 3 }]}',
		);
		expect(result).toEqual({
			processors: [{ operationTypes: ["insert"], maxRetries: 3 }],
		});
	});
});
