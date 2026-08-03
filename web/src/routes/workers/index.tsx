import { useQueries, useQuery } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Cpu } from "lucide-react";
import { pipelinesApi } from "@/api/pipelines";
import { isHeartbeatStale, workersApi } from "@/api/workers";
import { Badge } from "@/components/ui/badge";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";

export const Route = createFileRoute("/workers/")({
	component: WorkersPage,
});

const REFETCH_MS = 10_000;
/** The list endpoint caps `limit` at 100 (see ListPipelines in handler.go). */
const MAX_WORKERS = 100;

function WorkersPage() {
	// There is no "list workers" endpoint. Worker IDs are pipeline IDs, so the
	// roster is derived from the pipeline list and each heartbeat is fetched
	// individually.
	const {
		data: pipelineList,
		isLoading: isLoadingPipelines,
		isError,
		error,
	} = useQuery({
		queryKey: ["pipelines", "list", { limit: MAX_WORKERS }],
		queryFn: () => pipelinesApi.list({ limit: MAX_WORKERS, page: 1 }),
		refetchInterval: REFETCH_MS,
	});

	const pipelines = pipelineList?.pipelines ?? [];
	const total = pipelineList?.total ?? 0;

	const heartbeats = useQueries({
		queries: pipelines.map((p) => ({
			queryKey: ["worker-heartbeat", p.id],
			queryFn: () => workersApi.getHeartbeat(p.id),
			refetchInterval: REFETCH_MS,
			// A missing heartbeat is a normal state, not a transient failure.
			retry: false,
		})),
	});

	if (isError) {
		return (
			<div className="page-wrap px-4 pb-8 pt-14">
				<div className="rounded-lg border border-destructive/50 bg-destructive/10 p-4 text-destructive">
					<p className="font-medium">Failed to load workers</p>
					<p className="text-sm">
						{error instanceof Error ? error.message : "Please try again later."}
					</p>
				</div>
			</div>
		);
	}

	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			<div className="mb-8">
				<h1 className="text-3xl font-bold tracking-tight">Workers</h1>
				<p className="mt-2 text-muted-foreground">
					Liveness of the pipeline workers, refreshed every {REFETCH_MS / 1000}
					s.
				</p>
			</div>

			{total > MAX_WORKERS && (
				<div className="mb-4 rounded-lg border border-warning/50 bg-warning/10 p-3 text-sm">
					Showing the first {MAX_WORKERS} of {total} pipelines — the list
					endpoint caps a page at {MAX_WORKERS}.
				</div>
			)}

			<Card>
				<CardHeader>
					<CardTitle className="flex items-center gap-2">
						<Cpu className="h-5 w-5" />
						Worker Heartbeats
					</CardTitle>
					<CardDescription>
						A worker is considered dead once its heartbeat is more than 60s old.
					</CardDescription>
				</CardHeader>
				<CardContent>
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead>Pipeline</TableHead>
								<TableHead>Worker Status</TableHead>
								<TableHead>Uptime</TableHead>
								<TableHead>Last Heartbeat</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{isLoadingPipelines ? (
								<TableRow>
									<TableCell colSpan={4}>
										<Skeleton className="h-4 w-full" />
									</TableCell>
								</TableRow>
							) : pipelines.length === 0 ? (
								<TableRow>
									<TableCell
										colSpan={4}
										className="h-24 text-center text-muted-foreground"
									>
										No pipelines, so no workers.
									</TableCell>
								</TableRow>
							) : (
								pipelines.map((p, i) => {
									const q = heartbeats[i];
									const hb = q?.data ?? null;
									const stale = hb ? isHeartbeatStale(hb) : false;

									return (
										<TableRow key={p.id}>
											<TableCell>
												<Link
													to="/pipelines/$id"
													params={{ id: p.id }}
													className="font-medium text-primary hover:underline"
												>
													{p.name}
												</Link>
												<div className="text-xs text-muted-foreground">
													{p.id}
												</div>
											</TableCell>
											<TableCell>
												{q?.isLoading ? (
													<Skeleton className="h-6 w-20" />
												) : !hb ? (
													<Badge variant="secondary">No heartbeat</Badge>
												) : stale ? (
													<Badge variant="destructive">Stale</Badge>
												) : (
													<Badge
														variant={
															hb.status === "Running" ? "success" : "warning"
														}
													>
														{hb.status || "Unknown"}
													</Badge>
												)}
											</TableCell>
											<TableCell>
												{hb ? formatUptime(hb.uptimeSec) : "—"}
											</TableCell>
											<TableCell className="text-sm text-muted-foreground">
												{hb?.updatedAt
													? new Date(hb.updatedAt).toLocaleString()
													: "—"}
											</TableCell>
										</TableRow>
									);
								})
							)}
						</TableBody>
					</Table>
				</CardContent>
			</Card>
		</div>
	);
}

function formatUptime(seconds: number): string {
	if (!Number.isFinite(seconds) || seconds < 0) return "—";

	const d = Math.floor(seconds / 86400);
	const h = Math.floor((seconds % 86400) / 3600);
	const m = Math.floor((seconds % 3600) / 60);
	const s = Math.floor(seconds % 60);

	if (d > 0) return `${d}d ${h}h`;
	if (h > 0) return `${h}h ${m}m`;
	if (m > 0) return `${m}m ${s}s`;
	return `${s}s`;
}
