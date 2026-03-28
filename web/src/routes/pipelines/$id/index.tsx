import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import {
	ArrowLeft,
	Edit,
	RefreshCw,
	Database,
	Table,
	Activity,
	Wifi,
	WifiOff,
	AlertCircle,
} from "lucide-react";
import { pipelinesApi } from "@/api/pipelines";
import { useSSE } from "@/hooks/useSSE";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { MetricCard } from "@/components/MetricCard";
import type { Pipeline, PipelineStatus, TableStats } from "@/api/types";

interface PipelineMetrics {
	pipeline_id: string;
	timestamp: string;
	tables: TableStats[];
}

export const Route = createFileRoute("/pipelines/$id/")({
	component: PipelineDetailPage,
});

const statusVariantMap: Record<
	PipelineStatus,
	"success" | "destructive" | "warning" | "secondary"
> = {
	running: "success",
	stopped: "secondary",
	error: "destructive",
	paused: "warning",
};

function PipelineDetailPage() {
	const { id } = Route.useParams();
	const queryClient = useQueryClient();

	const {
		data: pipeline,
		isLoading: isLoadingPipeline,
		error: pipelineError,
	} = useQuery({
		queryKey: ["pipeline", id],
		queryFn: () => pipelinesApi.get(id),
	});

	const { data: metrics, isConnected, error: sseError } = useSSE<PipelineMetrics>(
		`/pipelines/${id}/metrics`
	);

	const restartMutation = useMutation({
		mutationFn: () => pipelinesApi.restart(id),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["pipeline", id] });
		},
	});

	if (isLoadingPipeline) {
		return (
			<div className="page-wrap px-4 pb-8 pt-14">
				<div className="animate-pulse space-y-4">
					<div className="h-8 w-64 bg-muted rounded" />
					<div className="h-4 w-48 bg-muted rounded" />
					<div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mt-8">
					{[1, 2, 3, 4].map((num) => (
						<div key={`loading-${num}`} className="h-32 bg-muted rounded" />
						))}
					</div>
				</div>
			</div>
		);
	}

	if (pipelineError || !pipeline) {
		return (
			<div className="page-wrap px-4 pb-8 pt-14">
				<div className="flex flex-col items-center justify-center py-12">
					<AlertCircle className="h-12 w-12 text-destructive mb-4" />
					<h2 className="text-xl font-semibold">Failed to load pipeline</h2>
					<p className="text-muted-foreground mt-2">
						{pipelineError?.message || "Pipeline not found"}
					</p>
					<Button asChild className="mt-4" variant="outline">
						<Link to="/pipelines">
							<ArrowLeft className="mr-2 h-4 w-4" />
							Back to Pipelines
						</Link>
					</Button>
				</div>
			</div>
		);
	}

	const totalEvents =
		metrics?.tables?.reduce((sum, t) => sum + (t.eventsTotal || 0), 0) || 0;
	const avgLag =
		metrics?.tables?.length
			? Math.round(
					metrics.tables.reduce((sum, t) => sum + (t.lagMs || 0), 0) /
						metrics.tables.length
				)
			: 0;

	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			{/* Header */}
			<div className="mb-8">
				<div className="flex items-center gap-4 mb-4">
					<Button asChild variant="outline" size="sm">
						<Link to="/pipelines">
							<ArrowLeft className="mr-2 h-4 w-4" />
							Back
						</Link>
					</Button>
					<Badge variant={statusVariantMap[pipeline.status]}>
						{pipeline.status}
					</Badge>
					{isConnected ? (
						<Badge variant="success" className="gap-1">
							<Wifi className="h-3 w-3" />
							Live
						</Badge>
					) : (
						<Badge variant="secondary" className="gap-1">
							<WifiOff className="h-3 w-3" />
							Offline
						</Badge>
					)}
				</div>
				<div className="flex items-center justify-between">
					<div>
						<h1 className="text-3xl font-bold tracking-tight">
							{pipeline.name}
						</h1>
						<p className="text-muted-foreground mt-1">
							Pipeline ID: {pipeline.id}
						</p>
					</div>
					<div className="flex gap-2">
						<Button
							variant="outline"
							onClick={() => restartMutation.mutate()}
							disabled={restartMutation.isPending}
						>
							<RefreshCw
								className={`mr-2 h-4 w-4 ${restartMutation.isPending ? "animate-spin" : ""}`}
							/>
							Restart
						</Button>
						<Button asChild>
							<Link to={`/pipelines/${id}/edit`}>
								<Edit className="mr-2 h-4 w-4" />
								Edit
							</Link>
						</Button>
					</div>
				</div>
				{pipeline.description && (
					<p className="text-muted-foreground mt-4">{pipeline.description}</p>
				)}
			</div>

			{/* Real-time Metrics */}
			<div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-8">
				<MetricCard
					title="Total Events"
					value={totalEvents.toLocaleString()}
					description="Events processed"
					icon={Activity}
					isLoading={!metrics && isConnected}
				/>
				<MetricCard
					title="Average Lag"
					value={`${avgLag}ms`}
					description="Replication lag"
					icon={Activity}
					isLoading={!metrics && isConnected}
				/>
				<MetricCard
					title="Tables"
					value={pipeline.source.tables.length}
					description="Monitored tables"
					icon={Table}
					isLoading={isLoadingPipeline}
				/>
				<MetricCard
					title="Last Update"
					value={
						metrics?.timestamp
							? new Date(metrics.timestamp).toLocaleTimeString()
							: "--"
					}
					description="Metrics timestamp"
					icon={Activity}
					isLoading={!metrics && isConnected}
				/>
			</div>

			{/* Configuration Cards */}
			<div className="grid gap-6 md:grid-cols-2 mb-8">
				{/* Source Configuration */}
				<Card>
					<CardHeader>
						<div className="flex items-center gap-2">
							<Database className="h-5 w-5 text-muted-foreground" />
							<CardTitle>Source</CardTitle>
						</div>
						<CardDescription>
							{pipeline.source.type.toUpperCase()} - {pipeline.source.name}
						</CardDescription>
					</CardHeader>
					<CardContent className="space-y-4">
						<div className="grid grid-cols-2 gap-4">
							<div>
								<p className="text-sm text-muted-foreground">Host</p>
								<p className="font-medium">{pipeline.source.connection.host}</p>
							</div>
							<div>
								<p className="text-sm text-muted-foreground">Port</p>
								<p className="font-medium">{pipeline.source.connection.port}</p>
							</div>
							<div>
								<p className="text-sm text-muted-foreground">Database</p>
								<p className="font-medium">
									{pipeline.source.connection.database}
								</p>
							</div>
							<div>
								<p className="text-sm text-muted-foreground">Username</p>
								<p className="font-medium">
									{pipeline.source.connection.username}
								</p>
							</div>
						</div>
						<div>
							<p className="text-sm text-muted-foreground mb-2">Tables</p>
							<div className="flex flex-wrap gap-2">
								{pipeline.source.tables.map((table) => (
									<Badge key={table} variant="secondary">
										{table}
									</Badge>
									))}
								</div>
							</div>
						</CardContent>
					</Card>

					{/* Sink Configuration */}
					<Card>
						<CardHeader>
							<div className="flex items-center gap-2">
								<Database className="h-5 w-5 text-muted-foreground" />
								<CardTitle>Sink</CardTitle>
							</div>
							<CardDescription>
								{pipeline.sink.type.toUpperCase()} - {pipeline.sink.name}
							</CardDescription>
						</CardHeader>
						<CardContent className="space-y-4">
							<div className="grid grid-cols-2 gap-4">
								<div>
									<p className="text-sm text-muted-foreground">Host</p>
									<p className="font-medium">{pipeline.sink.connection.host}</p>
								</div>
								{pipeline.sink.connection.port && (
									<div>
										<p className="text-sm text-muted-foreground">Port</p>
										<p className="font-medium">{pipeline.sink.connection.port}</p>
									</div>
								)}
								{pipeline.sink.connection.index && (
									<div>
										<p className="text-sm text-muted-foreground">Index</p>
										<p className="font-medium">{pipeline.sink.connection.index}</p>
									</div>
								)}
								{pipeline.sink.connection.topic && (
									<div>
										<p className="text-sm text-muted-foreground">Topic</p>
										<p className="font-medium">{pipeline.sink.connection.topic}</p>
									</div>
								)}
							</div>
						</CardContent>
					</Card>
				</div>

				{/* Table Metrics */}
				{metrics?.tables && metrics.tables.length > 0 && (
					<Card className="mb-8">
						<CardHeader>
							<CardTitle>Table Metrics</CardTitle>
							<CardDescription>Real-time metrics per table</CardDescription>
						</CardHeader>
						<CardContent>
							<div className="overflow-x-auto">
								<table className="w-full">
									<thead>
										<tr className="border-b">
											<th className="text-left py-3 px-4 font-medium">
												Table Name
											</th>
											<th className="text-right py-3 px-4 font-medium">Inserted</th>
											<th className="text-right py-3 px-4 font-medium">Updated</th>
											<th className="text-right py-3 px-4 font-medium">Deleted</th>
											<th className="text-right py-3 px-4 font-medium">Total</th>
											<th className="text-right py-3 px-4 font-medium">Lag</th>
										</tr>
										</thead>
										<tbody>
											{metrics.tables.map((table) => (
												<tr key={table.tableName} className="border-b last:border-0">
													<td className="py-3 px-4 font-medium">{table.tableName}</td>
													<td className="text-right py-3 px-4">
														{table.eventsInserted.toLocaleString()}
													</td>
													<td className="text-right py-3 px-4">
														{table.eventsUpdated.toLocaleString()}
													</td>
													<td className="text-right py-3 px-4">
														{table.eventsDeleted.toLocaleString()}
													</td>
													<td className="text-right py-3 px-4 font-semibold">
														{table.eventsTotal.toLocaleString()}
													</td>
													<td className="text-right py-3 px-4">
														{table.lagMs}ms
													</td>
												</tr>
											))}
											</tbody>
										</table>
									</div>
								</CardContent>
							</Card>
						)}

						{/* Raw Config */}
						<Card>
							<CardHeader>
								<CardTitle>Configuration</CardTitle>
								<CardDescription>Raw pipeline configuration</CardDescription>
							</CardHeader>
							<CardContent>
								<pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
									<code>{JSON.stringify(pipeline, null, 2)}</code>
								</pre>
							</CardContent>
						</Card>
					</div>
	);
}
