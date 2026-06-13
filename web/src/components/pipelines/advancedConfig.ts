import type { ProcessorConfigShape } from "./optionsTemplates";

export type AdvancedProcessor = ProcessorConfigShape;

export interface AdvancedRetry {
	maxRetries?: number;
	initialInterval?: string;
	maxInterval?: string;
	enableDlq?: boolean;
}

export interface AdvancedConfig {
	batchSize?: number;
	batchWait?: string;
	retry?: AdvancedRetry;
	processors: AdvancedProcessor[];
}

export function defaultAdvancedConfig(): AdvancedConfig {
	return { processors: [] };
}

export function isAdvancedConfigEmpty(cfg: AdvancedConfig): boolean {
	return (
		cfg.batchSize === undefined &&
		cfg.batchWait === undefined &&
		(cfg.retry === undefined ||
			(cfg.retry.maxRetries === undefined &&
				cfg.retry.initialInterval === undefined &&
				cfg.retry.maxInterval === undefined &&
				cfg.retry.enableDlq === undefined)) &&
		cfg.processors.length === 0
	);
}

export function advancedConfigToPayload(cfg: AdvancedConfig): Partial<{
	batchSize: number;
	batchWait: string;
	retry: {
		maxRetries?: number;
		initialInterval?: string;
		maxInterval?: string;
		enableDlq?: boolean;
	};
	processors: AdvancedProcessor[];
}> {
	const out: Partial<{
		batchSize: number;
		batchWait: string;
		retry: AdvancedRetry;
		processors: AdvancedProcessor[];
	}> = {};
	if (cfg.batchSize !== undefined) out.batchSize = cfg.batchSize;
	if (cfg.batchWait !== undefined && cfg.batchWait !== "")
		out.batchWait = cfg.batchWait;
	if (cfg.retry !== undefined) {
		const r: AdvancedRetry = {};
		if (cfg.retry.maxRetries !== undefined) r.maxRetries = cfg.retry.maxRetries;
		if (cfg.retry.initialInterval)
			r.initialInterval = cfg.retry.initialInterval;
		if (cfg.retry.maxInterval) r.maxInterval = cfg.retry.maxInterval;
		if (cfg.retry.enableDlq !== undefined) r.enableDlq = cfg.retry.enableDlq;
		if (Object.keys(r).length > 0) out.retry = r;
	}
	if (cfg.processors.length > 0) out.processors = cfg.processors;
	return out;
}
