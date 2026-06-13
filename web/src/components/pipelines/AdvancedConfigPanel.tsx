import { AlertCircle } from "lucide-react";
import { useState } from "react";
import {
	Accordion,
	AccordionContent,
	AccordionItem,
	AccordionTrigger,
} from "@/components/ui/accordion";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { isValidDuration } from "@/lib/duration";
import type { AdvancedConfig } from "./advancedConfig";
import { ProcessorList } from "./ProcessorList";

interface AdvancedConfigPanelProps {
	value: AdvancedConfig;
	onChange: (next: AdvancedConfig) => void;
	defaultOpen?: string[];
	errors?: Record<string, string>;
}

function parseIntOrUndefined(s: string): number | undefined {
	if (s === "") return undefined;
	const n = Number.parseInt(s, 10);
	return Number.isNaN(n) ? undefined : n;
}

function durationMs(s: string | undefined): number | null {
	if (!s) return null;
	if (!isValidDuration(s)) return null;
	const tokens = s.match(/\d+(?:\.\d+)?(?:ns|us|µs|μs|ms|s|m|h)/g) ?? [];
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
	let total = 0;
	for (const tok of tokens) {
		const m = /^(\d+(?:\.\d+)?)(ns|us|µs|μs|ms|s|m|h)$/.exec(tok);
		if (!m) return null;
		total += Number(m[1]) * UNIT_NS[m[2]];
	}
	return total;
}

export function AdvancedConfigPanel({
	value,
	onChange,
	defaultOpen,
	errors,
}: AdvancedConfigPanelProps) {
	const [localErrors, setLocalErrors] = useState<Record<string, string>>({});

	const allErrors: Record<string, string> = {
		...localErrors,
		...(errors ?? {}),
	};

	const setBatchSize = (s: string) => {
		const n = parseIntOrUndefined(s);
		onChange({ ...value, batchSize: n });
	};

	const setBatchWait = (s: string) => {
		onChange({ ...value, batchWait: s === "" ? undefined : s });
		if (s !== "" && !isValidDuration(s)) {
			setLocalErrors((e) => ({
				...e,
				batchWait: "Invalid duration format (e.g. 5s, 100ms, 1m)",
			}));
		} else {
			setLocalErrors((e) => {
				const { batchWait: _, ...rest } = e;
				return rest;
			});
		}
	};

	const retry = value.retry ?? {};
	const setRetry = (next: Partial<typeof retry>) => {
		onChange({ ...value, retry: { ...retry, ...next } });
	};

	const validateBackoffs = (next: Partial<typeof retry>) => {
		const init = next.initialInterval ?? retry.initialInterval;
		const max = next.maxInterval ?? retry.maxInterval;
		if (init && max) {
			const i = durationMs(init);
			const m = durationMs(max);
			if (i !== null && m !== null && m < i) {
				setLocalErrors((e) => ({ ...e, maxInterval: "Max must be ≥ initial" }));
				return;
			}
		}
		setLocalErrors((e) => {
			const { maxInterval: _, ...rest } = e;
			return rest;
		});
	};

	const setMaxRetries = (s: string) => {
		const n = parseIntOrUndefined(s);
		setRetry({ maxRetries: n });
	};

	const setEnableDlq = (checked: boolean) => {
		setRetry({ enableDlq: checked });
	};

	const setInitialInterval = (s: string) => {
		setRetry({ initialInterval: s === "" ? undefined : s });
		if (s !== "" && !isValidDuration(s)) {
			setLocalErrors((e) => ({
				...e,
				initialInterval: "Invalid duration format (e.g. 5s, 100ms, 1m)",
			}));
		} else {
			setLocalErrors((e) => {
				const { initialInterval: _, ...rest } = e;
				return rest;
			});
			validateBackoffs({ initialInterval: s === "" ? undefined : s });
		}
	};

	const setMaxInterval = (s: string) => {
		setRetry({ maxInterval: s === "" ? undefined : s });
		if (s !== "" && !isValidDuration(s)) {
			setLocalErrors((e) => ({
				...e,
				maxInterval: "Invalid duration format (e.g. 5s, 100ms, 1m)",
			}));
		} else {
			setLocalErrors((e) => {
				const { maxInterval: _, ...rest } = e;
				return rest;
			});
			validateBackoffs({ maxInterval: s === "" ? undefined : s });
		}
	};

	return (
		<Accordion type="multiple" defaultValue={defaultOpen} className="w-full">
			<AccordionItem value="batch">
				<AccordionTrigger>
					<span className="flex items-center gap-2">
						Batch & Performance
						<span className="text-xs text-muted-foreground">Optional</span>
					</span>
				</AccordionTrigger>
				<AccordionContent>
					<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
						<div className="space-y-1.5">
							<Label htmlFor="batch-size">Batch Size (messages)</Label>
							<Input
								id="batch-size"
								type="number"
								min={1}
								value={
									value.batchSize === undefined ? "" : String(value.batchSize)
								}
								onChange={(e) => setBatchSize(e.target.value)}
								aria-invalid={Boolean(allErrors.batchSize)}
							/>
							<div className="text-xs text-muted-foreground">
								Number of messages per batch.
							</div>
							{allErrors.batchSize && (
								<div className="flex items-center gap-1 text-xs text-destructive">
									<AlertCircle className="h-3 w-3" />
									<span>{allErrors.batchSize}</span>
								</div>
							)}
						</div>
						<div className="space-y-1.5">
							<Label htmlFor="batch-wait">Batch Wait (duration)</Label>
							<Input
								id="batch-wait"
								type="text"
								value={value.batchWait ?? ""}
								onChange={(e) => setBatchWait(e.target.value)}
								aria-invalid={Boolean(allErrors.batchWait)}
							/>
							<div className="text-xs text-muted-foreground">
								e.g. 5s, 100ms, 1m
							</div>
							{allErrors.batchWait && (
								<div className="flex items-center gap-1 text-xs text-destructive">
									<AlertCircle className="h-3 w-3" />
									<span>{allErrors.batchWait}</span>
								</div>
							)}
						</div>
					</div>
				</AccordionContent>
			</AccordionItem>

			<AccordionItem value="retry">
				<AccordionTrigger>
					<span className="flex items-center gap-2">
						Retry & Error Handling
						<span className="text-xs text-muted-foreground">Optional</span>
					</span>
				</AccordionTrigger>
				<AccordionContent>
					<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
						<div className="space-y-1.5">
							<Label htmlFor="max-retries">Max Retries</Label>
							<Input
								id="max-retries"
								type="number"
								min={0}
								value={
									retry.maxRetries === undefined ? "" : String(retry.maxRetries)
								}
								onChange={(e) => setMaxRetries(e.target.value)}
							/>
							<div className="text-xs text-muted-foreground">
								Maximum retry attempts before failing.
							</div>
						</div>
						<div className="space-y-1.5">
							<Label htmlFor="enable-dlq">Enable Dead Letter Queue</Label>
							<div className="h-9 flex items-center">
								<Switch
									id="enable-dlq"
									checked={retry.enableDlq === true}
									onCheckedChange={setEnableDlq}
								/>
							</div>
							<div className="text-xs text-muted-foreground">
								Route failed messages to DLQ after exhausting retries.
							</div>
						</div>
					</div>
					<div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
						<div className="space-y-1.5">
							<Label htmlFor="initial-interval">Initial Backoff</Label>
							<Input
								id="initial-interval"
								type="text"
								value={retry.initialInterval ?? ""}
								onChange={(e) => setInitialInterval(e.target.value)}
								aria-invalid={Boolean(allErrors.initialInterval)}
							/>
							<div className="text-xs text-muted-foreground">e.g. 1s</div>
							{allErrors.initialInterval && (
								<div className="flex items-center gap-1 text-xs text-destructive">
									<AlertCircle className="h-3 w-3" />
									<span>{allErrors.initialInterval}</span>
								</div>
							)}
						</div>
						<div className="space-y-1.5">
							<Label htmlFor="max-interval">Max Backoff</Label>
							<Input
								id="max-interval"
								type="text"
								value={retry.maxInterval ?? ""}
								onChange={(e) => setMaxInterval(e.target.value)}
								aria-invalid={Boolean(allErrors.maxInterval)}
							/>
							<div className="text-xs text-muted-foreground">e.g. 30s</div>
							{allErrors.maxInterval && (
								<div className="flex items-center gap-1 text-xs text-destructive">
									<AlertCircle className="h-3 w-3" />
									<span>{allErrors.maxInterval}</span>
								</div>
							)}
						</div>
					</div>
					<div className="mt-3">
						<Button
							type="button"
							variant="ghost"
							size="sm"
							onClick={() => onChange({ ...value, retry: undefined })}
						>
							Clear retry settings
						</Button>
					</div>
				</AccordionContent>
			</AccordionItem>

			<AccordionItem value="processors">
				<AccordionTrigger>
					<span className="flex items-center gap-2">
						Processors
						<span className="text-xs text-muted-foreground">Optional</span>
					</span>
				</AccordionTrigger>
				<AccordionContent>
					<ProcessorList
						value={value.processors}
						onChange={(p) => onChange({ ...value, processors: p })}
					/>
				</AccordionContent>
			</AccordionItem>
		</Accordion>
	);
}

export default AdvancedConfigPanel;
