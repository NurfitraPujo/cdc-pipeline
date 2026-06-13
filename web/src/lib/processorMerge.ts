function isPlainObject(v: unknown): v is Record<string, unknown> {
	return typeof v === "object" && v !== null && !Array.isArray(v);
}

export function deepMergeOptions(
	existing: unknown,
	template: unknown,
): unknown {
	if (existing === undefined || existing === null) {
		return template;
	}
	if (isPlainObject(existing) && isPlainObject(template)) {
		const out: Record<string, unknown> = { ...existing };
		for (const key of Object.keys(template)) {
			const tVal = template[key];
			const eVal = out[key];
			if (isPlainObject(eVal) && isPlainObject(tVal)) {
				out[key] = deepMergeOptions(eVal, tVal);
			} else if (Array.isArray(eVal) && Array.isArray(tVal)) {
				out[key] = [...eVal, ...tVal];
			} else {
				out[key] = tVal;
			}
		}
		return out;
	}
	if (Array.isArray(existing) && Array.isArray(template)) {
		return [...existing, ...template];
	}
	return template;
}
