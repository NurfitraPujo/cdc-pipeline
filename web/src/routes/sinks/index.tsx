import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import {
	Edit,
	HardDrive,
	Loader2,
	MoreHorizontal,
	Plus,
	Trash2,
} from "lucide-react";
import { sinksApi } from "@/api/sinks";
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

export const Route = createFileRoute("/sinks/")({
	component: SinksPage,
});

export function SinksPage() {
	const queryClient = useQueryClient();

	const {
		data: sinks = [],
		isLoading,
		isError,
		error,
	} = useQuery({
		queryKey: ["sinks"],
		queryFn: () => sinksApi.list(),
	});

	const deleteMutation = useMutation({
		mutationFn: sinksApi.delete,
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["sinks"] });
		},
	});

	const handleDelete = (id: string) => {
		if (confirm(`Are you sure you want to delete sink "${id}"?`)) {
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
					<p className="font-semibold">Failed to load sinks</p>
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
						<HardDrive className="h-8 w-8 text-[#56c6be]" />
						<h1 className="text-3xl font-bold tracking-tight">Sinks</h1>
					</div>
					<p className="mt-2 text-muted-foreground">
						Manage your destination sinks for CDC pipeline data.
					</p>
				</div>
				<Button asChild>
					<Link to="/sinks/create">
						<Plus className="mr-2 h-4 w-4" />
						Create Sink
					</Link>
				</Button>
			</div>

			{sinks.length === 0 ? (
				<Card className="border bg-card p-8">
					<CardContent className="flex flex-col items-center justify-center py-12 text-center">
						<div className="mb-4 rounded-full bg-muted p-4">
							<HardDrive className="h-8 w-8 text-muted-foreground" />
						</div>
						<h3 className="text-lg font-semibold">No sinks configured</h3>
						<p className="mt-2 max-w-sm text-sm text-muted-foreground">
							Sinks represent your destination systems (PostgreSQL, MySQL,
							Elasticsearch, Kafka, Webhook) where change events will be
							delivered to.
						</p>
						<Button asChild className="mt-6">
							<Link to="/sinks/create">
								<Plus className="mr-2 h-4 w-4" />
								Configure a Sink
							</Link>
						</Button>
					</CardContent>
				</Card>
			) : (
				<div className="rounded-md border bg-card">
					<Table>
						<TableHeader>
							<TableRow>
								<TableHead>Sink ID</TableHead>
								<TableHead>Type</TableHead>
								<TableHead>DSN / Connection</TableHead>
								<TableHead className="w-12">Actions</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{sinks.map((sink) => (
								<TableRow key={sink.id}>
									<TableCell className="font-semibold">{sink.id}</TableCell>
									<TableCell>
										<Badge variant="secondary" className="capitalize">
											{sink.type}
										</Badge>
									</TableCell>
									<TableCell className="text-sm text-muted-foreground max-w-[400px] truncate">
										{sink.dsn || "N/A"}
									</TableCell>
									<TableCell>
										<DropdownMenu>
											<DropdownMenuTrigger asChild>
												<Button
													variant="ghost"
													size="icon"
													disabled={
														deleteMutation.isPending &&
														deleteMutation.variables === sink.id
													}
												>
													<MoreHorizontal className="h-4 w-4" />
												</Button>
											</DropdownMenuTrigger>
											<DropdownMenuContent align="end">
												<DropdownMenuItem asChild>
													<Link to="/sinks/$id/edit" params={{ id: sink.id }}>
														<Edit className="mr-2 h-4 w-4" />
														Edit Details
													</Link>
												</DropdownMenuItem>
												<DropdownMenuSeparator />
												<DropdownMenuItem
													onClick={() => handleDelete(sink.id)}
													className="text-destructive focus:text-destructive"
												>
													<Trash2 className="mr-2 h-4 w-4" />
													Delete Sink
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
