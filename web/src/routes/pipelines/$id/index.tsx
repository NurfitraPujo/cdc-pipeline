import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import {
	Activity,
	AlertCircle,
	ArrowLeft,
	Database,
	Edit,
	RefreshCw,
	Table,
	Wifi,
	WifiOff,
	Monitor,
} from "lucide-react";
import { useEffect, useState } from "react";
import { pipelinesApi } from "@/api/pipelines";
import type { PipelineStatus, TableStats, SSEMessage, PipelineStatusResponse } from "@/api/types";
import { MetricCard } from "@/components/MetricCard";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { useSSE } from "@/hooks/useSSE";

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

	const [tables, setTables] = useState<Record<string, TableStats>>({});
	const [sinks, setSinks] = useState<Record<string, Record<string, TableStats>>>({});
	const [lastUpdate, setLastUpdate] = useState<string | null>(null);

	const {
		data: pipeline,
		isLoading: isLoadingPipeline,
		error: pipelineError,
	} = useQuery({
		queryKey: ["pipeline", id],
		queryFn: () => pipelinesApi.get(id),
	});

	// Fetch initial status
	const { data: initialStatus } = useQuery<PipelineStatusResponse>({
		queryKey: ["pipeline-status", id],
		queryFn: () => pipelinesApi.getStatus(id),
		enabled: !!id,
	});

	// Initialize state from initial status
	useEffect(() => {
		if (initialStatus) {
			setTables(initialStatus.tables || {});
			setSinks(initialStatus.sinks || {});
		}
	}, [initialStatus]);

	// Handle real-time updates
	const { isConnected } = useSSE<SSEMessage>(
		`/pipelines/${id}/metrics`,
		{
			onMessage: (msg) => {
				if (!msg || !msg.key) return;
				
				const key = msg.key;
				const data = msg.data as TableStats;
				
				if (key.endsWith(".stats")) {
					setLastUpdate(new Date().toISOString());
					
					// Extract table name from key or data if available
					// Format: cdc.pipeline.{pid}.sources.{sid}.sinks.{sinkID}.tables.{table}.stats
					const parts = key.split(".");
					const tableName = parts[8] || data.tableName;
					const sinkID = msg.sink_id;

					if (sinkID) {
						// Update per-sink stats
						setSinks(prev => ({
							...prev,
							[sinkID]: {
								...(prev[sinkID] || {}),
								[tableName]: data
							}
						}));

						// Update aggregated table stats (exclude debug)
						if (!msg.is_debug) {
							setTables(prev => {
								const current = prev[tableName] || { total_synced: 0, lag_ms: 0 };
								const next = { ...data, tableName };
								
								// Simple aggregation: max for count and lag
								if (data.total_synced > current.total_synced) {
									next.total_synced = data.total_synced;
								} else {
									next.total_synced = current.total_synced;
								}
								
								if (data.lag_ms > current.lag_ms) {
									next.lag_ms = data.lag_ms;
								} else {
									next.lag_ms = current.lag_ms;
								}
								
								return { ...prev, [tableName]: next };
							});
						}
					}
				}
			}
		}
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

	const tableList = Object.values(tables);
	const totalEvents = tableList.reduce((sum, t) => sum + (t.total_synced || 0), 0);
	const avgLag = tableList.length
		? Math.round(
				tableList.reduce((sum, t) => sum + (t.lag_ms || 0), 0) /
					tableList.length,
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
							<Link to="/pipelines/$id/edit" params={{ id }}>
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
					isLoading={tableList.length === 0 && isConnected}
				/>
				<MetricCard
					title="Average Lag"
					value={`${avgLag}ms`}
					description="Replication lag"
					icon={Activity}
					isLoading={tableList.length === 0 && isConnected}
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
						lastUpdate
							? new Date(lastUpdate).toLocaleTimeString()
							: "--"
					}
					description="Metrics timestamp"
					icon={Activity}
					isLoading={!lastUpdate && isConnected}
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
							<CardTitle>Sinks</CardTitle>
						</div>
						<CardDescription>
							{pipeline.sinks.length} configured sinks
						</CardDescription>
					</CardHeader>
					<CardContent className="space-y-4">
						<div className="flex flex-wrap gap-2">
							{pipeline.sinks.map((sinkId) => (
								<Badge key={sinkId} variant="outline" className="px-3 py-1">
									<Monitor className="mr-2 h-3 w-3" />
									{sinkId}
								</Badge>
							))}
						</div>
						<p className="text-xs text-muted-foreground">
							Each sink has a dedicated consumer for failure isolation.
						</p>
					</CardContent>
				</Card>
			</div>

			{/* Table Metrics (Aggregated) */}
			{tableList.length > 0 && (
				<Card className="mb-8">
					<CardHeader>
						<CardTitle>Production Table Metrics</CardTitle>
						<CardDescription>Aggregated real-time metrics (excluding debug sinks)</CardDescription>
					</CardHeader>
					<CardContent>
						<div className="overflow-x-auto">
							<table className="w-full">
								<thead>
									<tr className="border-b">
										<th className="text-left py-3 px-4 font-medium">
											Table Name
										</th>
										<th className="text-right py-3 px-4 font-medium">
											Status
										</th>
										<th className="text-right py-3 px-4 font-medium">
											Total Synced
										</th>
										<th className="text-right py-3 px-4 font-medium">
											RPS
										</th>
										<th className="text-right py-3 px-4 font-medium">
											Lag
										</th>
										<th className="text-right py-3 px-4 font-medium">
											Last Update
										</th>
									</tr>
								</thead>
								<tbody>
									{tableList.map((table) => (
										<tr
											key={table.tableName}
											className="border-b last:border-0 hover:bg-muted/50 transition-colors"
										>
											<td className="py-3 px-4 font-medium">
												{table.tableName}
											</td>
											<td className="text-right py-3 px-4">
												<Badge variant={table.status === "ACTIVE" ? "success" : "destructive"}>
													{table.status}
												</Badge>
											</td>
											<td className="text-right py-3 px-4 font-semibold">
												{table.total_synced?.toLocaleString()}
											</td>
											<td className="text-right py-3 px-4">
												{table.rps?.toFixed(1)}
											</td>
											<td className="text-right py-3 px-4 font-mono">
												{table.lag_ms}ms
											</td>
											<td className="text-right py-3 px-4 text-xs text-muted-foreground">
												{table.updated_at ? new Date(table.updated_at).toLocaleTimeString() : "--"}
											</td>
										</tr>
									))}
								</tbody>
							</table>
						</div>
					</CardContent>
				</Card>
			)}

			{/* Per-Sink Details */}
			{Object.keys(sinks).length > 0 && (
				<div className="space-y-6 mb-8">
					<h2 className="text-xl font-bold flex items-center gap-2">
						<Monitor className="h-5 w-5" />
						Per-Sink Details
					</h2>
					<div className="grid gap-6">
						{Object.entries(sinks).map(([sinkId, sinkTables]) => (
							<Card key={sinkId}>
								<CardHeader className="pb-3">
									<CardTitle className="text-lg flex items-center justify-between">
										<span>Sink: <span className="text-primary">{sinkId}</span></span>
										{sinkId.includes("debug") && (
											<Badge variant="secondary">Debug Sink</Badge>
										)}
									</CardTitle>
								</CardHeader>
								<CardContent>
									<div className="overflow-x-auto">
										<table className="w-full text-sm">
											<thead>
												<tr className="border-b text-muted-foreground">
													<th className="text-left py-2 px-4 font-medium">Table</th>
													<th className="text-right py-2 px-4 font-medium">Synced</th>
													<th className="text-right py-2 px-4 font-medium">Lag</th>
													<th className="text-right py-2 px-4 font-medium">Errors</th>
												</tr>
											</thead>
											<tbody>
												{Object.entries(sinkTables).map(([tableName, stats]) => (
													<tr key={tableName} className="border-b last:border-0">
														<td className="py-2 px-4">{tableName}</td>
														<td className="text-right py-2 px-4 font-medium">
															{stats.total_synced?.toLocaleString()}
														</td>
														<td className="text-right py-2 px-4 font-mono">
															{stats.lag_ms}ms
														</td>
														<td className="text-right py-2 px-4 text-destructive">
															{stats.error_count || 0}
														</td>
													</tr>
												))}
											</tbody>
										</table>
									</div>
								</CardContent>
							</Card>
						))}
					</div>
				</div>
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
