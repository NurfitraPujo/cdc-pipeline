const SNAKE_RE = /_([a-z0-9])/g;
const CAMEL_RE = /[A-Z]/g;

/**
 * Keys whose *values* are opaque user data, not part of the API schema.
 *
 * The backend types these as `map[string]interface{}` and never interprets
 * their keys (`protocol.ProcessorConfig.Options`, `protocol.SinkConfig.Options`).
 * Rewriting them corrupts user input: a processor option `maxLength` was being
 * sent as `max_length`, and on the way back `field_1` decoded to `field1` --
 * which does not round-trip, because `snakeToCamelKey` consumes the underscore
 * before a digit.
 *
 * Values under these keys are deep-cloned verbatim, in both directions.
 */
const OPAQUE_VALUE_KEYS = new Set(["options"]);

function snakeToCamelKey(key: string): string {
	if (!key.includes("_")) return key;
	return key.replace(SNAKE_RE, (_, c: string) => c.toUpperCase());
}

function camelToSnakeKey(key: string): string {
	if (!/[A-Z]/.test(key)) return key;
	return key.replace(CAMEL_RE, (m) => `_${m.toLowerCase()}`);
}

/** Deep clone that preserves every key exactly as written. */
function cloneVerbatim(
	input: unknown,
	seen: WeakMap<object, unknown>,
): unknown {
	if (input === null || typeof input !== "object") return input;
	if (input instanceof Date || input instanceof RegExp) return input;
	if (Array.isArray(input))
		return input.map((item) => cloneVerbatim(item, seen));

	const obj = input as Record<string, unknown>;
	const cached = seen.get(obj);
	if (cached) return cached;

	const out: Record<string, unknown> = {};
	seen.set(obj, out);
	for (const key of Object.keys(obj)) {
		out[key] = cloneVerbatim(obj[key], seen);
	}
	return out;
}

function transform(
	input: unknown,
	keyFn: (k: string) => string,
	seen: WeakMap<object, unknown>,
): unknown {
	if (input === null || input === undefined) return input;
	if (Array.isArray(input)) {
		return input.map((item) => transform(item, keyFn, seen));
	}
	if (typeof input !== "object") return input;
	if (input instanceof Date || input instanceof RegExp) return input;

	const obj = input as Record<string, unknown>;
	const cached = seen.get(obj);
	if (cached) return cached;

	const out: Record<string, unknown> = {};
	seen.set(obj, out);
	for (const key of Object.keys(obj)) {
		const value = obj[key];
		// The key itself is schema and still gets converted; only the value
		// beneath it is off limits.
		out[keyFn(key)] = OPAQUE_VALUE_KEYS.has(key)
			? cloneVerbatim(value, seen)
			: transform(value, keyFn, seen);
	}
	return out;
}

export function snakeToCamel<T = unknown>(obj: unknown): T {
	return transform(obj, snakeToCamelKey, new WeakMap()) as T;
}

export function camelToSnake<T = unknown>(obj: unknown): T {
	return transform(obj, camelToSnakeKey, new WeakMap()) as T;
}
