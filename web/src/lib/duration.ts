const TOKEN_RE = /^(\d+(?:\.\d+)?(?:ns|us|µs|μs|ms|s|m|h))+$/;
const NUMBER_UNIT_RE = /^(\d+(?:\.\d+)?)(ns|us|µs|μs|ms|s|m|h)$/;

const UNIT_NS: Record<string, number> = {
	ns: 1,
	us: 1000,
	µs: 1000,
	μs: 1000,
	ms: 1_000_000,
	s: 1_000_000_000,
	m: 60_000_000_000,
	h: 3_600_000_000_000,
};

function validate(s: string): boolean {
	if (typeof s !== "string" || s.length === 0) return false;
	if (s.startsWith("+") || s.startsWith("-")) return false;
	if (!TOKEN_RE.test(s)) return false;

	const tokens = s.match(/\d+(?:\.\d+)?(?:ns|us|µs|μs|ms|s|m|h)/g);
	if (!tokens || tokens.length === 0) return false;

	let totalNs = 0;
	for (const tok of tokens) {
		const m = NUMBER_UNIT_RE.exec(tok);
		if (!m) return false;
		const num = Number(m[1]);
		if (!Number.isFinite(num)) return false;
		const unit = m[2];
		const factor = UNIT_NS[unit];
		if (factor === undefined) return false;
		totalNs += num * factor;
	}

	if (!Number.isFinite(totalNs)) return false;
	return true;
}

export function isValidDuration(s: string): boolean {
	return validate(s);
}

export function parseDuration(s: string): number {
	if (!validate(s)) {
		throw new Error(`Invalid duration: ${JSON.stringify(s)}`);
	}

	const tokens = s.match(/\d+(?:\.\d+)?(?:ns|us|µs|μs|ms|s|m|h)/g) as string[];
	let totalNs = 0;
	for (const tok of tokens) {
		const m = NUMBER_UNIT_RE.exec(tok) as RegExpExecArray;
		const num = Number(m[1]);
		const unit = m[2];
		totalNs += num * UNIT_NS[unit];
	}

	const ms = totalNs / 1_000_000;
	if (!Number.isFinite(ms)) {
		throw new Error(`Invalid duration: ${JSON.stringify(s)}`);
	}
	return ms;
}
