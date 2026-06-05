const SNAKE_RE = /_([a-z0-9])/g;
const CAMEL_RE = /[A-Z]/g;

function snakeToCamelKey(key: string): string {
	if (!key.includes("_")) return key;
	return key.replace(SNAKE_RE, (_, c: string) => c.toUpperCase());
}

function camelToSnakeKey(key: string): string {
	if (!/[A-Z]/.test(key)) return key;
	return key.replace(CAMEL_RE, (m) => `_${m.toLowerCase()}`);
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
		if (key === value) {
			out[key] = value;
			continue;
		}
		if (Object.hasOwn(obj, key)) {
			out[keyFn(key)] = transform(value, keyFn, seen);
		}
	}
	return out;
}

export function snakeToCamel<T = unknown>(obj: unknown): T {
	return transform(obj, snakeToCamelKey, new WeakMap()) as T;
}

export function camelToSnake<T = unknown>(obj: unknown): T {
	return transform(obj, camelToSnakeKey, new WeakMap()) as T;
}
