import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
	ChevronLeft,
	ChevronRight,
	MoreHorizontal,
	Play,
	RotateCcw,
	Trash2,
} from "lucide-react";
import { useState } from "react";
import { type Pipeline, pipelinesApi } from "@/api/pipelines";
import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import {
	DropdownMenu,
	DropdownMenuContent,
	DropdownMenuItem,
	DropdownMenuSeparator,
	DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Skeleton } from "@/components/ui/skeleton";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";

interface PipelineTableProps {
	search?: string;
	status?: string;
}

const DEFAULT_PAGE_SIZE = 10;

function PipelineTableRow({
	pipeline,
	onRestart,
	onDelete,
	isRestarting,
	isDeleting,
}: {
	pipeline: Pipeline;
	onRestart: (id: string) => void;
	onDelete: (id: string) => void;
	isRestarting: boolean;
	isDeleting: boolean;
}) {
	const isProcessing = isRestarting || isDeleting;

	return (
		<TableRow>
			<TableCell>
				<button
					type="button"
					onClick={() => (window.location.href = `/pipelines/${pipeline.id}`)}
					className="font-medium text-primary hover:underline bg-transparent border-none p-0 cursor-pointer"
				>
					{pipeline.name}
				</button>
			</TableCell>
			<TableCell>
				<StatusBadge status={(pipeline.status as any) || "unknown"} />
			</TableCell>
			<TableCell>{pipeline.sources.length} source(s)</TableCell>
			<TableCell>{pipeline.sinks.length} sink(s)</TableCell>
			<TableCell>{pipeline.tables.length}</TableCell>
			<TableCell>
				<DropdownMenu>
					<DropdownMenuTrigger asChild>
						<Button variant="ghost" size="icon" disabled={isProcessing}>
							<MoreHorizontal className="h-4 w-4" />
							<span className="sr-only">Open menu</span>
						</Button>
					</DropdownMenuTrigger>
					<DropdownMenuContent align="end">
						<DropdownMenuItem
							onClick={() =>
								(window.location.href = `/pipelines/${pipeline.id}`)
							}
						>
							<Play className="mr-2 h-4 w-4" />
							View Details
						</DropdownMenuItem>
						<DropdownMenuItem
							onClick={() => onRestart(pipeline.id)}
							disabled={isRestarting}
						>
							<RotateCcw className="mr-2 h-4 w-4" />
							Restart
						</DropdownMenuItem>
						<DropdownMenuSeparator />
						<DropdownMenuItem
							onClick={() => onDelete(pipeline.id)}
							disabled={isDeleting}
							className="text-destructive focus:text-destructive"
						>
							<Trash2 className="mr-2 h-4 w-4" />
							Delete
						</DropdownMenuItem>
					</DropdownMenuContent>
				</DropdownMenu>
			</TableCell>
		</TableRow>
	);
}

function TableSkeleton() {
	const rows = [
		{ id: "skeleton-1" },
		{ id: "skeleton-2" },
		{ id: "skeleton-3" },
		{ id: "skeleton-4" },
		{ id: "skeleton-5" },
	];

	return (
		<>
			{rows.map((row) => (
				<TableRow key={row.id}>
					<TableCell>
						<Skeleton className="h-4 w-32" />
					</TableCell>
					<TableCell>
						<Skeleton className="h-6 w-20" />
					</TableCell>
					<TableCell>
						<Skeleton className="h-4 w-16" />
					</TableCell>
					<TableCell>
						<Skeleton className="h-4 w-20" />
					</TableCell>
					<TableCell>
						<Skeleton className="h-4 w-8" />
					</TableCell>
					<TableCell>
						<Skeleton className="h-8 w-8" />
					</TableCell>
				</TableRow>
			))}
		</>
	);
}

export function PipelineTable({ search, status }: PipelineTableProps) {
	const [page, setPage] = useState(1);
	const queryClient = useQueryClient();

	const _params = { search, status, page, limit: DEFAULT_PAGE_SIZE };
	void _params;

	const { data, isLoading, isError, error } = useQuery({
		queryKey: ["pipelines", "list", { search, status, page }],
		queryFn: () => pipelinesApi.list(),
	});

	const restartMutation = useMutation({
		mutationFn: pipelinesApi.restart,
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["pipelines", "list"] });
		},
	});

	const deleteMutation = useMutation({
		mutationFn: pipelinesApi.delete,
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["pipelines", "list"] });
		},
	});

	const handleRestart = (id: string) => {
		restartMutation.mutate(id);
	};

	const handleDelete = (id: string) => {
		if (confirm("Are you sure you want to delete this pipeline?")) {
			deleteMutation.mutate(id);
		}
	};

	const pipelines = data?.pipelines ?? [];
	const pagination = {
		page: 1,
		total_pages: 1,
		total: pipelines.length,
		limit: DEFAULT_PAGE_SIZE,
	};

	const handlePreviousPage = () => {
		if (page > 1) {
			setPage(page - 1);
		}
	};

	const handleNextPage = () => {
		if (pagination && page < pagination.total_pages) {
			setPage(page + 1);
		}
	};

	if (isError) {
		return (
			<div className="rounded-lg border border-destructive/50 bg-destructive/10 p-4 text-destructive">
				<p className="font-medium">Failed to load pipelines</p>
				<p className="text-sm">
					{error instanceof Error ? error.message : "Please try again later."}
				</p>
			</div>
		);
	}

	return (
		<div className="space-y-4">
			<div className="rounded-md border">
				<Table>
					<TableHeader>
						<TableRow>
							<TableHead>ID</TableHead>
							<TableHead>Status</TableHead>
							<TableHead>Source</TableHead>
							<TableHead>Sink</TableHead>
							<TableHead>Tables</TableHead>
							<TableHead className="w-12">Actions</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{isLoading ? (
							<TableSkeleton />
						) : pipelines.length === 0 ? (
							<TableRow>
								<TableCell
									colSpan={6}
									className="h-24 text-center text-muted-foreground"
								>
									No pipelines found.
								</TableCell>
							</TableRow>
						) : (
							pipelines.map((pipeline) => (
								<PipelineTableRow
									key={pipeline.id}
									pipeline={pipeline}
									onRestart={handleRestart}
									onDelete={handleDelete}
									isRestarting={
										restartMutation.variables === pipeline.id &&
										restartMutation.isPending
									}
									isDeleting={
										deleteMutation.variables === pipeline.id &&
										deleteMutation.isPending
									}
								/>
							))
						)}
					</TableBody>
				</Table>
			</div>

			{pagination && pagination.total_pages > 1 && (
				<div className="flex items-center justify-between">
					<div className="text-sm text-muted-foreground">
						Showing {(page - 1) * DEFAULT_PAGE_SIZE + 1} to{" "}
						{Math.min(page * DEFAULT_PAGE_SIZE, pagination.total)} of{" "}
						{pagination.total} pipelines
					</div>
					<div className="flex items-center gap-2">
						<Button
							variant="outline"
							size="sm"
							onClick={handlePreviousPage}
							disabled={page <= 1 || isLoading}
						>
							<ChevronLeft className="h-4 w-4" />
							Previous
						</Button>
						<div className="text-sm">
							Page {page} of {pagination.total_pages}
						</div>
						<Button
							variant="outline"
							size="sm"
							onClick={handleNextPage}
							disabled={page >= pagination.total_pages || isLoading}
						>
							Next
							<ChevronRight className="h-4 w-4" />
						</Button>
					</div>
				</div>
			)}
		</div>
	);
}
