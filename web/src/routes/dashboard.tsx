import { useQuery } from "@tanstack/react-query";
import { createFileRoute } from "@tanstack/react-router";
import { Activity, Clock, Database, Rows3 } from "lucide-react";
import { statsApi } from "@/api/stats";
import { MetricCard } from "@/components/MetricCard";

export const Route = createFileRoute("/dashboard")({
	component: DashboardPage,
});

const REFRESH_INTERVAL = 30000; // 30 seconds

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

	const healthyPercent = data?.total_pipelines
		? `${Math.round((data.healthy_count / data.total_pipelines) * 100)}%`
		: null;

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
					value={data?.total_pipelines ?? 0}
					description={`${data?.healthy_count ?? 0} healthy, ${data?.error_count ?? 0} errors`}
					icon={Database}
					isLoading={isLoading}
				/>

				<MetricCard
					title="Rows Synchronized"
					value={data?.total_rows_synchronized ?? 0}
					description="Total rows processed across all pipelines"
					icon={Rows3}
					isLoading={isLoading}
				/>

				<MetricCard
					title="Healthy"
					value={healthyPercent ?? "0%"}
					description={`${data?.healthy_count ?? 0} of ${data?.total_pipelines ?? 0} pipelines`}
					icon={Activity}
					isLoading={isLoading}
				/>

				<MetricCard
					title="Average Lag"
					value={data ? formatLag(data.avg_lag_ms) : "0ms"}
					description="Average processing latency"
					icon={Clock}
					isLoading={isLoading}
				/>
			</div>

			<div className="mt-8 grid gap-4 lg:grid-cols-2">
				<div className="rounded-xl border bg-card p-6">
					<h3 className="text-lg font-semibold">Throughput Chart</h3>
					<p className="text-sm text-muted-foreground mt-2">
						Chart placeholder - rows processed over time will be displayed here.
					</p>
					<div className="mt-4 h-64 flex items-center justify-center rounded-lg bg-muted/50">
						<span className="text-muted-foreground">
							Throughput visualization coming soon
						</span>
					</div>
				</div>

				<div className="rounded-xl border bg-card p-6">
					<h3 className="text-lg font-semibold">Pipeline Status Chart</h3>
					<p className="text-sm text-muted-foreground mt-2">
						Chart placeholder - pipeline status distribution will be displayed
						here.
					</p>
					<div className="mt-4 h-64 flex items-center justify-center rounded-lg bg-muted/50">
						<span className="text-muted-foreground">
							Status visualization coming soon
						</span>
					</div>
				</div>
			</div>
		</div>
	);
}
