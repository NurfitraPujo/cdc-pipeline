import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import {
	Activity,
	AlertCircle,
	ArrowLeft,
	Database,
	Edit,
	Monitor,
	RefreshCw,
	Table,
	Wifi,
	WifiOff,
} from "lucide-react";
import { useEffect, useState } from "react";
import { pipelinesApi } from "@/api/pipelines";
import { sourcesApi } from "@/api/sources";
import type {
	Checkpoint,
	PipelineTransitionState,
	SSEMessage,
	TableStats,
} from "@/api/types";
import { MetricCard } from "@/components/MetricCard";
import { StatusBadge, type StatusBadgeStatus } from "@/components/StatusBadge";
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

/**
 * Reduce a checkpoint KV key to the table token it refers to.
 *
 * Keys look like
 *   cdc.pipeline.{pid}.sources.{sid}.tables.{token}.ingress_checkpoint
 *   cdc.pipeline.{pid}.sources.{sid}.sinks.{sink}.tables.{token}.egress_checkpoint
 * so the token is whatever sits between the "tables" marker and the trailing
 * checkpoint segment. Falls back to the whole key if the shape is unfamiliar.
 */
function checkpointLabel(key: string): string {
	const parts = key.split(".");
	const tablesAt = parts.lastIndexOf("tables");
	if (tablesAt === -1 || tablesAt + 1 >= parts.length - 1) return key;
	return parts.slice(tablesAt + 1, parts.length - 1).join(".");
}

function PipelineDetailPage() {
	const { id } = Route.useParams();
	const queryClient = useQueryClient();

	const [tables, setTables] = useState<Record<string, TableStats>>({});
	const [sinks, setSinks] = useState<
		Record<string, Record<string, TableStats>>
	>({});
	const [lastUpdate, setLastUpdate] = useState<string | null>(null);
	const [checkpoints, setCheckpoints] = useState<Record<string, Checkpoint>>(
		{},
	);
	const [transition, setTransition] = useState<PipelineTransitionState | null>(
		null,
	);

	const {
		data: pipeline,
		isLoading: isLoadingPipeline,
		error: pipelineError,
	} = useQuery({
		queryKey: ["pipeline", id],
		queryFn: () => pipelinesApi.get(id),
	});

	const { data: sources = [] } = useQuery({
		queryKey: ["sources"],
		queryFn: () => sourcesApi.list(),
	});

	const sourceById = new Map(sources.map((s) => [s.id, s]));

	// GET /pipelines/{id} now carries the server-computed status.
	const livePipelineStatus = pipeline?.status ?? "unknown";

	// Fetch initial status
	const { data: initialStatus } = useQuery({
		queryKey: ["pipeline-status", id],
		queryFn: () => pipelinesApi.getStatus(id),
		enabled: !!id,
	});

	// Initialize state from initial status
	useEffect(() => {
		if (initialStatus) {
			setTables((initialStatus.tables as Record<string, TableStats>) || {});
			setSinks(
				(initialStatus.sinks as Record<string, Record<string, TableStats>>) ||
					{},
			);
		}
	}, [initialStatus]);

	// Handle real-time updates
	const { isConnected } = useSSE<SSEMessage>(`/pipelines/${id}/metrics`, {
		onMessage: (raw) => {
			const msg = raw as SSEMessage;
			if (!msg || !msg.key) return;

			const key = msg.key;

			// The server streams three payload variants over one event name.
			// Only `.stats` used to be handled; checkpoints and transitions
			// were parsed and thrown away, so the page never reflected a
			// transition and never showed replication progress.
			if (key.endsWith("_checkpoint")) {
				setLastUpdate(new Date().toISOString());
				setCheckpoints((prev) => ({
					...prev,
					[key]: msg.data as Checkpoint,
				}));
				return;
			}

			if (key.endsWith(".transition")) {
				setLastUpdate(new Date().toISOString());
				setTransition(msg.data as PipelineTransitionState);
				// A transition changes the pipeline's reported status, so the
				// cached config/status queries are now stale.
				queryClient.invalidateQueries({ queryKey: ["pipeline-status", id] });
				return;
			}

			const data = msg.data as TableStats;

			if (key.endsWith(".stats")) {
				setLastUpdate(new Date().toISOString());

				// Extract table name from key or data if available.
				// Format: cdc.pipeline.{pid}.sources.{sid}.sinks.{sinkID}.tables.{token}.stats
				// Parsed from BOTH ENDS (fixed prefix tokens 0..7, terminal
				// "stats") rather than a fixed positional index -- a schema-
				// qualified KeyToken (e.g. "sales=orders") is still one
				// token, but this stays correct even if the token itself
				// ever contains a "." (see ParseTableStatsKey in
				// internal/protocol/config.go and MULTI_SCHEMA_PLAN.md §2.3).
				const parts = key.split(".");
				const tableName =
					(parts.length >= 10 && parts[parts.length - 1] === "stats"
						? parts.slice(8, parts.length - 1).join(".")
						: undefined) || data.tableName;
				const sinkID = msg.sinkId;

				// Per-sink breakdown, when the event carries a sink.
				if (sinkID) {
					setSinks((prev) => ({
						...prev,
						[sinkID]: {
							...(prev[sinkID] || {}),
							[tableName]: data,
						},
					}));
				}

				// Aggregate into the table totals. Producer-level stats keys
				// (ProducerTableStatsKey) carry no sink, so this used to sit
				// inside `if (sinkID)` and drop them entirely -- while
				// setLastUpdate still fired, making "Last Update" tick as the
				// numbers stayed frozen. Debug sinks stay excluded so a
				// postgres_debug mirror does not double-count.
				if (!msg.isDebug) {
					setTables((prev) => {
						const current = prev[tableName] || { totalSynced: 0, lagMs: 0 };
						const next = { ...data, tableName };

						// Simple aggregation: max for count and lag
						if (data.totalSynced > current.totalSynced) {
							next.totalSynced = data.totalSynced;
						} else {
							next.totalSynced = current.totalSynced;
						}

						if (data.lagMs > current.lagMs) {
							next.lagMs = data.lagMs;
						} else {
							next.lagMs = current.lagMs;
						}

						return { ...prev, [tableName]: next };
					});
				}
			}
		},
	});

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
	const totalEvents = tableList.reduce(
		(sum, t) => sum + (t.totalSynced || 0),
		0,
	);
	const avgLag = tableList.length
		? Math.round(
				tableList.reduce((sum, t) => sum + (t.lagMs || 0), 0) /
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
					{/* Live status, not a hardcoded "Configured" literal. A
					    transition event from the stream wins over the value the
					    status query returned when the page loaded. */}
					<StatusBadge
						status={
							(transition?.status?.toLowerCase() === "transitioning"
								? "transitioning"
								: livePipelineStatus) as StatusBadgeStatus
						}
					/>
					{transition?.startedAt && (
						<span className="text-xs text-muted-foreground">
							since {new Date(transition.startedAt).toLocaleTimeString()}
						</span>
					)}
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
					value={pipeline.tables.length}
					description="Monitored tables"
					icon={Table}
					isLoading={isLoadingPipeline}
				/>
				<MetricCard
					title="Last Update"
					value={lastUpdate ? new Date(lastUpdate).toLocaleTimeString() : "--"}
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
							<CardTitle>Sources</CardTitle>
						</div>
						<CardDescription>
							{pipeline.sources.length} configured source
							{pipeline.sources.length === 1 ? "" : "s"}
						</CardDescription>
					</CardHeader>
					<CardContent className="space-y-4">
						<div className="flex flex-wrap gap-2">
							{pipeline.sources.map((sourceId) => {
								const src = sourceById.get(sourceId);
								return (
									<Badge
										key={sourceId}
										variant="secondary"
										className="px-3 py-1"
									>
										<Database className="mr-2 h-3 w-3" />
										{src ? `${sourceId} (${src.type})` : sourceId}
									</Badge>
								);
							})}
						</div>
						<div>
							<p className="text-sm text-muted-foreground mb-2">Tables</p>
							<div className="flex flex-wrap gap-2">
								{pipeline.tables.map((table) => (
									<Badge key={table} variant="outline">
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
						<CardDescription>
							Aggregated real-time metrics (excluding debug sinks)
						</CardDescription>
					</CardHeader>
					<CardContent>
						<div className="overflow-x-auto">
							<table className="w-full">
								<thead>
									<tr className="border-b">
										<th className="text-left py-3 px-4 font-medium">
											Table Name
										</th>
										<th className="text-right py-3 px-4 font-medium">Status</th>
										<th className="text-right py-3 px-4 font-medium">
											Total Synced
										</th>
										<th className="text-right py-3 px-4 font-medium">RPS</th>
										<th className="text-right py-3 px-4 font-medium">Lag</th>
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
												<Badge
													variant={
														table.status === "ACTIVE"
															? "success"
															: "destructive"
													}
												>
													{table.status}
												</Badge>
											</td>
											<td className="text-right py-3 px-4 font-semibold">
												{table.totalSynced?.toLocaleString()}
											</td>
											<td className="text-right py-3 px-4">
												{table.rps?.toFixed(1)}
											</td>
											<td className="text-right py-3 px-4 font-mono">
												{table.lagMs}ms
											</td>
											<td className="text-right py-3 px-4 text-xs text-muted-foreground">
												{table.updatedAt
													? new Date(table.updatedAt).toLocaleTimeString()
													: "--"}
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
										<span>
											Sink: <span className="text-primary">{sinkId}</span>
										</span>
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
													<th className="text-left py-2 px-4 font-medium">
														Table
													</th>
													<th className="text-right py-2 px-4 font-medium">
														Synced
													</th>
													<th className="text-right py-2 px-4 font-medium">
														Lag
													</th>
													<th className="text-right py-2 px-4 font-medium">
														Errors
													</th>
												</tr>
											</thead>
											<tbody>
												{Object.entries(sinkTables).map(
													([tableName, stats]) => (
														<tr
															key={tableName}
															className="border-b last:border-0"
														>
															<td className="py-2 px-4">{tableName}</td>
															<td className="text-right py-2 px-4 font-medium">
																{stats.totalSynced?.toLocaleString()}
															</td>
															<td className="text-right py-2 px-4 font-mono">
																{stats.lagMs}ms
															</td>
															<td className="text-right py-2 px-4 text-destructive">
																{stats.errorCount || 0}
															</td>
														</tr>
													),
												)}
											</tbody>
										</table>
									</div>
								</CardContent>
							</Card>
						))}
					</div>
				</div>
			)}

			{/* Replication checkpoints, streamed on `_checkpoint` keys. */}
			{Object.keys(checkpoints).length > 0 && (
				<Card className="mb-8">
					<CardHeader>
						<CardTitle>Replication Checkpoints</CardTitle>
						<CardDescription>
							Ingress and egress LSN positions per table
						</CardDescription>
					</CardHeader>
					<CardContent>
						<div className="overflow-x-auto">
							<table className="w-full text-sm">
								<thead>
									<tr className="border-b text-left text-muted-foreground">
										<th className="pb-2 pr-4 font-medium">Table</th>
										<th className="pb-2 pr-4 font-medium">Ingress LSN</th>
										<th className="pb-2 pr-4 font-medium">Egress LSN</th>
										<th className="pb-2 pr-4 font-medium">Last PK</th>
										<th className="pb-2 font-medium">Status</th>
									</tr>
								</thead>
								<tbody>
									{Object.entries(checkpoints).map(([key, cp]) => (
										<tr key={key} className="border-b last:border-0">
											<td className="py-2 pr-4 font-mono text-xs" title={key}>
												{checkpointLabel(key)}
											</td>
											<td className="py-2 pr-4 font-mono text-xs">
												{cp.ingressLsn || "—"}
											</td>
											<td className="py-2 pr-4 font-mono text-xs">
												{cp.egressLsn || "—"}
											</td>
											<td className="py-2 pr-4 font-mono text-xs">
												{cp.lastPk || "—"}
											</td>
											<td className="py-2">
												<Badge variant="secondary">{cp.status || "—"}</Badge>
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
