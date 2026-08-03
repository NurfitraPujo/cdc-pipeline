import { useEffect, useState } from "react";
import type { GlobalConfig } from "@/api/globalConfig";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { isValidDuration } from "@/lib/duration";

interface GlobalConfigFormProps {
	value: GlobalConfig;
	onSave: (config: GlobalConfig) => void;
	isSaving?: boolean;
}

/** Every duration field on GlobalConfig, with its server-side constraint. */
const DURATION_FIELDS = [
	{
		key: "batchWait",
		label: "Batch Wait",
		help: "How long to accumulate a batch before flushing. Minimum 100ms.",
		minMs: 100,
	},
	{
		key: "drainTimeout",
		label: "Drain Timeout",
		help: "How long to wait for in-flight work to drain on reload. Minimum 1s.",
		minMs: 1000,
	},
	{
		key: "shutdownTimeout",
		label: "Shutdown Timeout",
		help: "How long to wait for a clean shutdown. Minimum 1s.",
		minMs: 1000,
	},
	{
		key: "stabilizationDelay",
		label: "Stabilization Delay",
		help: "Settle time after a worker starts before it is judged healthy.",
		minMs: 0,
	},
	{
		key: "crashRecoveryDelay",
		label: "Crash Recovery Delay",
		help: "Backoff before restarting a crashed worker.",
		minMs: 0,
	},
	{
		key: "globalReloadDelay",
		label: "Global Reload Delay",
		help: "Delay between pipeline reloads when global config changes.",
		minMs: 0,
	},
] as const satisfies readonly {
	key: keyof GlobalConfig;
	label: string;
	help: string;
	minMs: number;
}[];

const RETRY_DURATION_FIELDS = [
	{
		key: "initialInterval",
		label: "Initial Interval",
		help: "First retry backoff. Minimum 100ms.",
	},
	{
		key: "maxInterval",
		label: "Max Interval",
		help: "Ceiling for the retry backoff. Minimum 100ms.",
	},
] as const;

const MIN_RETRY_INTERVAL_MS = 100;

/**
 * Field-level editor for the global config.
 *
 * Replaces four static labels that were bound to nothing and named settings
 * ("Max Concurrent Pipelines", "Health Check Interval") that do not exist on
 * protocol.GlobalConfig. Validation mirrors GlobalConfig.Validate() so the
 * user sees the constraint before the server returns a 400.
 */
export function GlobalConfigForm({
	value,
	onSave,
	isSaving,
}: GlobalConfigFormProps) {
	const [draft, setDraft] = useState<GlobalConfig>(value);
	const [errors, setErrors] = useState<Record<string, string>>({});

	// Re-seed when the query resolves or refetches.
	useEffect(() => {
		setDraft(value);
	}, [value]);

	const setField = <K extends keyof GlobalConfig>(
		key: K,
		v: GlobalConfig[K],
	) => {
		setDraft((prev) => ({ ...prev, [key]: v }));
	};

	const setRetryField = <K extends keyof GlobalConfig["retry"]>(
		key: K,
		v: GlobalConfig["retry"][K],
	) => {
		setDraft((prev) => ({ ...prev, retry: { ...prev.retry, [key]: v } }));
	};

	const validate = (): Record<string, string> => {
		const next: Record<string, string> = {};

		if (!Number.isFinite(draft.batchSize) || draft.batchSize < 1) {
			next.batchSize = "Must be at least 1.";
		}

		for (const f of DURATION_FIELDS) {
			const raw = draft[f.key] as string;
			if (!raw || !isValidDuration(raw)) {
				next[f.key] =
					"Invalid duration. Use a Go duration such as 5s or 100ms.";
			} else if (f.minMs > 0 && parseDurationMs(raw) < f.minMs) {
				next[f.key] = `Must be at least ${f.minMs}ms.`;
			}
		}

		if (
			!Number.isFinite(draft.retry.maxRetries) ||
			draft.retry.maxRetries < 0
		) {
			next["retry.maxRetries"] = "Must be 0 or greater.";
		}

		for (const f of RETRY_DURATION_FIELDS) {
			const raw = draft.retry[f.key];
			if (!raw || !isValidDuration(raw)) {
				next[`retry.${f.key}`] =
					"Invalid duration. Use a Go duration such as 1s or 30s.";
			} else if (parseDurationMs(raw) < MIN_RETRY_INTERVAL_MS) {
				next[`retry.${f.key}`] = `Must be at least ${MIN_RETRY_INTERVAL_MS}ms.`;
			}
		}

		return next;
	};

	const handleSubmit = (e: React.FormEvent) => {
		e.preventDefault();
		const found = validate();
		setErrors(found);
		if (Object.keys(found).length === 0) {
			onSave(draft);
		}
	};

	return (
		<form onSubmit={handleSubmit}>
			<Card className="mb-6">
				<CardHeader>
					<CardTitle>Batching</CardTitle>
					<CardDescription>
						Defaults applied to every pipeline that does not override them.
					</CardDescription>
				</CardHeader>
				<CardContent className="grid gap-4 md:grid-cols-2">
					<Field
						id="batchSize"
						label="Batch Size"
						help="Events to accumulate before flushing. Minimum 1."
						error={errors.batchSize}
					>
						<Input
							id="batchSize"
							type="number"
							min={1}
							value={draft.batchSize}
							onChange={(e) => setField("batchSize", Number(e.target.value))}
						/>
					</Field>

					{DURATION_FIELDS.filter((f) => f.key === "batchWait").map((f) => (
						<DurationField
							key={f.key}
							id={f.key}
							label={f.label}
							help={f.help}
							error={errors[f.key]}
							value={draft[f.key] as string}
							onChange={(v) => setField(f.key, v as never)}
						/>
					))}
				</CardContent>
			</Card>

			<Card className="mb-6">
				<CardHeader>
					<CardTitle>Retry &amp; Dead Letter Queue</CardTitle>
					<CardDescription>
						How failed batches are retried before being parked.
					</CardDescription>
				</CardHeader>
				<CardContent className="grid gap-4 md:grid-cols-2">
					<Field
						id="maxRetries"
						label="Max Retries"
						help="Attempts before the batch is considered failed. 0 or greater."
						error={errors["retry.maxRetries"]}
					>
						<Input
							id="maxRetries"
							type="number"
							min={0}
							value={draft.retry.maxRetries}
							onChange={(e) =>
								setRetryField("maxRetries", Number(e.target.value))
							}
						/>
					</Field>

					{RETRY_DURATION_FIELDS.map((f) => (
						<DurationField
							key={f.key}
							id={`retry-${f.key}`}
							label={f.label}
							help={f.help}
							error={errors[`retry.${f.key}`]}
							value={draft.retry[f.key]}
							onChange={(v) => setRetryField(f.key, v)}
						/>
					))}

					<div className="flex items-start gap-3 pt-6">
						<Checkbox
							id="enableDlq"
							checked={draft.retry.enableDlq}
							onCheckedChange={(checked) =>
								setRetryField("enableDlq", checked === true)
							}
						/>
						<div>
							<Label htmlFor="enableDlq">Enable Dead Letter Queue</Label>
							<p className="text-xs text-muted-foreground">
								Park permanently failed batches instead of dropping them.
							</p>
						</div>
					</div>
				</CardContent>
			</Card>

			<Card className="mb-6">
				<CardHeader>
					<CardTitle>Lifecycle Timeouts</CardTitle>
					<CardDescription>
						Timing for worker startup, reload and shutdown.
					</CardDescription>
				</CardHeader>
				<CardContent className="grid gap-4 md:grid-cols-2">
					{DURATION_FIELDS.filter((f) => f.key !== "batchWait").map((f) => (
						<DurationField
							key={f.key}
							id={f.key}
							label={f.label}
							help={f.help}
							error={errors[f.key]}
							value={draft[f.key] as string}
							onChange={(v) => setField(f.key, v as never)}
						/>
					))}
				</CardContent>
			</Card>

			<div className="flex items-center gap-3">
				<Button type="submit" disabled={isSaving}>
					{isSaving ? "Saving..." : "Save Configuration"}
				</Button>
				<Button
					type="button"
					variant="outline"
					onClick={() => {
						setDraft(value);
						setErrors({});
					}}
					disabled={isSaving}
				>
					Reset
				</Button>
			</div>
		</form>
	);
}

function Field({
	id,
	label,
	help,
	error,
	children,
}: {
	id: string;
	label: string;
	help: string;
	error?: string;
	children: React.ReactNode;
}) {
	return (
		<div className="space-y-2">
			<Label htmlFor={id}>{label}</Label>
			{children}
			{error ? (
				<p className="text-xs text-destructive">{error}</p>
			) : (
				<p className="text-xs text-muted-foreground">{help}</p>
			)}
		</div>
	);
}

function DurationField({
	id,
	label,
	help,
	error,
	value,
	onChange,
}: {
	id: string;
	label: string;
	help: string;
	error?: string;
	value: string;
	onChange: (v: string) => void;
}) {
	return (
		<Field id={id} label={label} help={help} error={error}>
			<Input
				id={id}
				value={value ?? ""}
				placeholder="e.g. 5s, 100ms, 1m"
				onChange={(e) => onChange(e.target.value)}
			/>
		</Field>
	);
}

/** Milliseconds for a Go duration string, or NaN if unparseable. */
function parseDurationMs(s: string): number {
	const re = /(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)/g;
	const unitMs: Record<string, number> = {
		ns: 1e-6,
		us: 1e-3,
		µs: 1e-3,
		ms: 1,
		s: 1000,
		m: 60_000,
		h: 3_600_000,
	};

	let total = 0;
	let matched = false;
	for (const m of s.matchAll(re)) {
		matched = true;
		total += Number(m[1]) * unitMs[m[2]];
	}
	return matched ? total : Number.NaN;
}
