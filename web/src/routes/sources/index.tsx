import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import {
	Database,
	Edit,
	Loader2,
	MoreHorizontal,
	Plus,
	Trash2,
} from "lucide-react";
import { sourcesApi } from "@/api/sources";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
	DropdownMenu,
	DropdownMenuContent,
	DropdownMenuItem,
	DropdownMenuSeparator,
	DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";

export const Route = createFileRoute("/sources/")({
	component: SourcesPage,
});

export function SourcesPage() {
	const queryClient = useQueryClient();

	const {
		data: sources = [],
		isLoading,
		isError,
		error,
	} = useQuery({
		queryKey: ["sources"],
		queryFn: () => sourcesApi.list(),
	});

	const deleteMutation = useMutation({
		mutationFn: sourcesApi.delete,
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["sources"] });
		},
	});

	const handleDelete = (id: string) => {
		if (confirm(`Are you sure you want to delete source "${id}"?`)) {
			deleteMutation.mutate(id);
		}
	};

	if (isLoading) {
		return (
			<div className="page-wrap px-4 pb-8 pt-14 flex items-center justify-center">
				<Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		);
	}

	if (isError) {
		return (
			<div className="page-wrap px-4 pb-8 pt-14 max-w-4xl mx-auto">
				<div className="rounded-lg border border-destructive/50 bg-destructive/10 p-4 text-destructive">
					<p className="font-semibold">Failed to load sources</p>
					<p className="text-sm">
						{error instanceof Error ? error.message : "Unexpected error"}
					</p>
				</div>
			</div>
		);
	}

	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			<div className="mb-8 flex items-center justify-between">
				<div>
					<div className="flex items-center gap-3">
						<Database className="h-8 w-8 text-[#56c6be]" />
						<h1 className="text-3xl font-bold tracking-tight">Sources</h1>
					</div>
					<p className="mt-2 text-muted-foreground">
						Manage your database sources for CDC pipelines.
					</p>
				</div>
				<Button asChild>
					<Link to="/sources/create">
						<Plus className="mr-2 h-4 w-4" />
						Create Source
					</Link>
				</Button>
			</div>

			{sources.length === 0 ? (
				<Card className="border bg-card p-8">
					<CardContent className="flex flex-col items-center justify-center py-12 text-center">
						<div className="mb-4 rounded-full bg-muted p-4">
							<Database className="h-8 w-8 text-muted-foreground" />
						</div>
						<h3 className="text-lg font-semibold">No sources configured</h3>
						<p className="mt-2 max-w-sm text-sm text-muted-foreground">
							Sources represent your database connections (PostgreSQL, MySQL,
							MongoDB) that you want to capture change events from.
						</p>
						<Button asChild className="mt-6">
							<Link to="/sources/create">
								<Plus className="mr-2 h-4 w-4" />
								Configure a Source
							</Link>
						</Button>
					</CardContent>
				</Card>
			) : (
				<div className="rounded-md border bg-card">
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead>Source ID</TableHead>
								<TableHead>Type</TableHead>
								<TableHead>Connection Details</TableHead>
								<TableHead>Tables</TableHead>
								<TableHead className="w-12">Actions</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{sources.map((source) => (
								<TableRow key={source.id}>
									<TableCell className="font-semibold">{source.id}</TableCell>
									<TableCell>
										<Badge variant="secondary" className="capitalize">
											{source.type}
										</Badge>
									</TableCell>
									<TableCell className="text-sm text-muted-foreground">
										{source.host}:{source.port} / {source.database}
									</TableCell>
									<TableCell>
										{source.tables && source.tables.length > 0 ? (
											<div className="flex flex-wrap gap-1 max-w-[300px]">
												{source.tables.slice(0, 3).map((t, _idx) => (
													<Badge key={t} variant="outline" className="text-xs">
														{t}
													</Badge>
												))}
												{source.tables.length > 3 && (
													<Badge variant="outline" className="text-xs">
														+{source.tables.length - 3} more
													</Badge>
												)}
											</div>
										) : (
											<span className="text-xs text-muted-foreground">
												All tables
											</span>
										)}
									</TableCell>
									<TableCell>
										<DropdownMenu>
											<DropdownMenuTrigger asChild>
												<Button
													variant="ghost"
													size="icon"
													disabled={
														deleteMutation.isPending &&
														deleteMutation.variables === source.id
													}
												>
													<MoreHorizontal className="h-4 w-4" />
												</Button>
											</DropdownMenuTrigger>
											<DropdownMenuContent align="end">
												<DropdownMenuItem asChild>
													<Link
														to="/sources/$id/edit"
														params={{ id: source.id }}
													>
														<Edit className="mr-2 h-4 w-4" />
														Edit Details
													</Link>
												</DropdownMenuItem>
												<DropdownMenuSeparator />
												<DropdownMenuItem
													onClick={() => handleDelete(source.id)}
													className="text-destructive focus:text-destructive"
												>
													<Trash2 className="mr-2 h-4 w-4" />
													Delete Source
												</DropdownMenuItem>
											</DropdownMenuContent>
										</DropdownMenu>
									</TableCell>
								</TableRow>
							))}
						</TableBody>
					</Table>
				</div>
			)}
		</div>
	);
}
