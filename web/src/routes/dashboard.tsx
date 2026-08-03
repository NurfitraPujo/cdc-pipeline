import { useQuery } from "@tanstack/react-query";
import { createFileRoute } from "@tanstack/react-router";
import { Activity, Clock, Database, Rows3 } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { statsApi } from "@/api/stats";
import { MetricCard } from "@/components/MetricCard";

export const Route = createFileRoute("/dashboard")({
	component: DashboardPage,
});

const REFRESH_INTERVAL = 30000; // 30 seconds
/** Roughly 30 minutes of history at the current refresh interval. */
const MAX_SAMPLES = 60;

function formatLag(lagMs: number): string {
	if (lagMs < 1000) {
		return `${lagMs.toFixed(0)}ms`;
	}
	if (lagMs < 60000) {
		return `${(lagMs / 1000).toFixed(1)}s`;
	}
	return `${(lagMs / 60000).toFixed(1)}m`;
}

function DashboardPage() {
	const { data, isLoading, isError, error } = useQuery({
		queryKey: ["stats", "summary"],
		queryFn: () => statsApi.getSummary(),
		refetchInterval: REFRESH_INTERVAL,
	});

	const healthyPercent = data?.totalPipelines
		? `${Math.round((data.healthyCount / data.totalPipelines) * 100)}%`
		: null;

	// Derive a throughput series from successive total_rows_synced readings.
	const [samples, setSamples] = useState<number[]>([]);
	const previousTotal = useRef<number | null>(null);

	useEffect(() => {
		const total = data?.totalRowsSynced;
		if (total === undefined) return;

		const prev = previousTotal.current;
		previousTotal.current = total;
		if (prev === null) return;

		// Counters reset when the API restarts; a negative delta is noise.
		const delta = Math.max(0, total - prev);
		setSamples((s) => [...s, delta].slice(-MAX_SAMPLES));
	}, [data?.totalRowsSynced]);

	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			<div className="mb-8">
				<h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
				<p className="mt-2 text-muted-foreground">
					Overview of your CDC pipeline health and performance metrics.
				</p>
			</div>

			{isError && (
				<div className="mb-4 rounded-lg border border-destructive/50 bg-destructive/10 p-4 text-destructive">
					<p className="font-medium">Failed to load dashboard data</p>
					<p className="text-sm">
						{error instanceof Error ? error.message : "Please try again later."}
					</p>
				</div>
			)}

			<div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
				<MetricCard
					title="Total Pipelines"
					value={data?.totalPipelines ?? 0}
					description={`${data?.healthyCount ?? 0} healthy, ${data?.errorCount ?? 0} errors`}
					icon={Database}
					isLoading={isLoading}
				/>

				<MetricCard
					title="Rows Synchronized"
					value={data?.totalRowsSynced ?? 0}
					description="Total rows processed across all pipelines"
					icon={Rows3}
					isLoading={isLoading}
				/>

				<MetricCard
					title="Healthy"
					value={healthyPercent ?? "0%"}
					description={`${data?.healthyCount ?? 0} of ${data?.totalPipelines ?? 0} pipelines`}
					icon={Activity}
					isLoading={isLoading}
				/>

				<MetricCard
					title="Average Lag"
					value={data ? formatLag(data.avgLagMs) : "0ms"}
					description="Average processing latency"
					icon={Clock}
					isLoading={isLoading}
				/>
			</div>

			<div className="mt-8 grid gap-4 lg:grid-cols-2">
				<div className="rounded-xl border bg-card p-6">
					<h3 className="text-lg font-semibold">Throughput</h3>
					<p className="text-sm text-muted-foreground mt-2">
						Rows synchronized per interval, sampled every{" "}
						{REFRESH_INTERVAL / 1000}s since this page was opened.
					</p>
					<ThroughputChart samples={samples} />
				</div>

				<div className="rounded-xl border bg-card p-6">
					<h3 className="text-lg font-semibold">Pipeline Status</h3>
					<p className="text-sm text-muted-foreground mt-2">
						Distribution across all {data?.totalPipelines ?? 0} pipelines.
					</p>
					<StatusChart
						healthy={data?.healthyCount ?? 0}
						transitioning={data?.transitioningCount ?? 0}
						error={data?.errorCount ?? 0}
					/>
				</div>
			</div>
		</div>
	);
}

/**
 * Rows-per-interval, derived from successive `total_rows_synced` readings.
 *
 * The server has a `/stats/history` endpoint, but it is deprecated and
 * hardcoded to return an empty array (`GetStatsHistory` in
 * internal/api/handler.go), so there is no server-side time series to plot.
 * Sampling the summary poll gives a real, if session-scoped, throughput trace.
 */
function ThroughputChart({ samples }: { samples: number[] }) {
	if (samples.length < 2) {
		return (
			<div className="mt-4 h-64 flex items-center justify-center rounded-lg bg-muted/50">
				<span className="text-muted-foreground text-sm">
					Collecting samples…
				</span>
			</div>
		);
	}

	const max = Math.max(...samples, 1);
	const width = 400;
	const height = 240;
	const step = width / (samples.length - 1);
	const points = samples
		.map((v, i) => `${i * step},${height - (v / max) * (height - 20)}`)
		.join(" ");

	return (
		<div className="mt-4 h-64 rounded-lg bg-muted/30 p-2">
			<svg
				viewBox={`0 0 ${width} ${height}`}
				preserveAspectRatio="none"
				className="h-full w-full"
				role="img"
				aria-label={`Throughput over the last ${samples.length} samples, peaking at ${max} rows`}
			>
				<polyline
					points={points}
					fill="none"
					stroke="currentColor"
					strokeWidth={2}
					className="text-primary"
				/>
			</svg>
			<div className="flex justify-between text-xs text-muted-foreground">
				<span>peak {max.toLocaleString()} rows/interval</span>
				<span>{samples.length} samples</span>
			</div>
		</div>
	);
}

function StatusChart({
	healthy,
	transitioning,
	error,
}: {
	healthy: number;
	transitioning: number;
	error: number;
}) {
	const rows = [
		{ label: "Healthy", value: healthy, className: "bg-green-500" },
		{
			label: "Transitioning",
			value: transitioning,
			className: "bg-yellow-500",
		},
		{ label: "Error", value: error, className: "bg-destructive" },
	];
	const total = healthy + transitioning + error;

	return (
		<div className="mt-4 h-64 flex flex-col justify-center gap-4">
			{rows.map((r) => (
				<div key={r.label}>
					<div className="flex justify-between text-sm">
						<span>{r.label}</span>
						<span className="text-muted-foreground">{r.value}</span>
					</div>
					<div className="mt-1 h-3 w-full rounded-full bg-muted">
						<div
							className={`h-3 rounded-full ${r.className}`}
							style={{
								width: total > 0 ? `${(r.value / total) * 100}%` : "0%",
							}}
						/>
					</div>
				</div>
			))}
			{total === 0 && (
				<p className="text-center text-sm text-muted-foreground">
					No pipelines configured yet.
				</p>
			)}
		</div>
	);
}
