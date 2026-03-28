import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import {
	ArrowLeft,
	Database,
	Server,
	Plus,
	X,
	Loader2,
	AlertCircle,
} from "lucide-react";
import { pipelinesApi } from "@/api/pipelines";
import { sourcesApi } from "@/api/sources";
import { sinksApi } from "@/api/sinks";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import type { Source, Sink } from "@/api/types";

export const Route = createFileRoute("/pipelines/create")({
	component: CreatePipelinePage,
});

function CreatePipelinePage() {
	const navigate = useNavigate();
	const queryClient = useQueryClient();

	const [pipelineId, setPipelineId] = useState("");
	const [selectedSource, setSelectedSource] = useState<string | null>(null);
	const [selectedSink, setSelectedSink] = useState<string | null>(null);
	const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());
	const [validationErrors, setValidationErrors] = useState<Record<string, string>>({});

	const { data: sources = [], isLoading: isLoadingSources } = useQuery({
		queryKey: ["sources"],
		queryFn: () => sourcesApi.list(),
	});

	const { data: sinks = [], isLoading: isLoadingSinks } = useQuery({
		queryKey: ["sinks"],
		queryFn: () => sinksApi.list(),
	});

	// Fetch tables when a source is selected
	const { data: tables = [], isLoading: isLoadingTables } = useQuery({
		queryKey: ["source-tables", selectedSource],
		queryFn: () => {
			if (!selectedSource) return Promise.resolve([]);
			return sourcesApi.getTables(selectedSource);
		},
		enabled: !!selectedSource,
	});

	// Reset selected tables when source changes
	useEffect(() => {
		setSelectedTables(new Set());
	}, [selectedSource]);

	const createMutation = useMutation({
		mutationFn: pipelinesApi.create,
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["pipelines"] });
			navigate({ to: "/pipelines" });
		},
	});

	const handleSelectSource = (sourceId: string) => {
		setSelectedSource(sourceId);
		if (validationErrors.source) {
			setValidationErrors((prev) => ({ ...prev, source: "" }));
		}
	};

	const handleSelectSink = (sinkId: string) => {
		setSelectedSink(sinkId);
		if (validationErrors.sink) {
			setValidationErrors((prev) => ({ ...prev, sink: "" }));
		}
	};

	const handleToggleTable = (table: string) => {
		setSelectedTables((prev) => {
			const next = new Set(prev);
			if (next.has(table)) {
				next.delete(table);
			} else {
				next.add(table);
			}
			return next;
		});
	};

	const handleCreate = () => {
		const errors: Record<string, string> = {};

		if (!pipelineId.trim()) {
			errors.pipelineId = "Pipeline ID is required";
		}
		if (!selectedSource) {
			errors.source = "Please select a source";
		}
		if (!selectedSink) {
			errors.sink = "Please select a sink";
		}
		if (selectedTables.size === 0) {
			errors.tables = "Please select at least one table";
		}

		if (Object.keys(errors).length > 0) {
			setValidationErrors(errors);
			return;
		}

		const source = sources.find((s) => s.id === selectedSource);
		const sink = sinks.find((s) => s.id === selectedSink);

		if (!source || !sink) return;

		createMutation.mutate({
			name: pipelineId,
			source: {
				type: source.type,
				name: source.name,
				connection: source.connection,
				tables: Array.from(selectedTables),
			},
			sink: {
				type: sink.type,
				name: sink.name,
				connection: sink.connection,
			},
		});
	};

	const canCreate =
		pipelineId.trim() !== "" &&
		selectedSource !== null &&
		selectedSink !== null &&
		selectedTables.size > 0;

	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			{/* Header */}
			<div className="mb-8">
				<Button asChild variant="outline" size="sm">
					<Link to="/pipelines">
						<ArrowLeft className="mr-2 h-4 w-4" />
						Back to Pipelines
					</Link>
				</Button>
				<h1 className="mt-4 text-3xl font-bold tracking-tight">Create Pipeline</h1>
				<p className="mt-2 text-muted-foreground">
					Configure a new CDC pipeline to sync data from source to sink.
				</p>
			</div>

			<div className="grid gap-6 lg:grid-cols-2">
				{/* Pipeline ID */}
				<Card className="lg:col-span-2">
					<CardHeader>
						<CardTitle>Pipeline ID</CardTitle>
						<CardDescription>
							Enter a unique identifier for this pipeline.
						</CardDescription>
					</CardHeader>
					<CardContent>
					<Input
						placeholder="e.g., postgres-to-kafka"
						value={pipelineId}
						onChange={(e) => {
							setPipelineId(e.target.value);
							if (validationErrors.pipelineId) {
								setValidationErrors((prev) => ({ ...prev, pipelineId: "" }));
							}
						}}
						className="max-w-md"
					/>
					{validationErrors.pipelineId && (
						<p className="text-sm text-destructive mt-2 flex items-center gap-1">
							<AlertCircle className="h-3 w-3" />
							{validationErrors.pipelineId}
						</p>
					)}
					</CardContent>
				</Card>

				{/* Sources */}
				<Card>
					<CardHeader>
						<div className="flex items-center gap-2">
							<Database className="h-5 w-5 text-muted-foreground" />
							<CardTitle>Select Source</CardTitle>
						</div>
						<CardDescription>
							Choose a data source for this pipeline.
						</CardDescription>
					</CardHeader>
					<CardContent>
						{isLoadingSources ? (
							<div className="flex items-center justify-center py-8">
								<Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
							</div>
						) : sources.length === 0 ? (
							<p className="text-center text-muted-foreground py-8">
								No sources available. Create a source first.
							</p>
						) : (
							<div className="space-y-3">
								{sources.map((source) => (
									<label
										key={source.id}
										className="flex items-start gap-3 rounded-lg border p-4 cursor-pointer hover:bg-muted/50 transition-colors"
									>
										<input
											type="radio"
											name="source"
											checked={selectedSource === source.id}
											onChange={() => handleSelectSource(source.id)}
											className="mt-1 h-4 w-4"
										/>
										<div className="flex-1">
											<p className="font-medium">{source.name}</p>
											<p className="text-sm text-muted-foreground">
												{source.type.toUpperCase()} - {source.connection.host}:
												{source.connection.port}
											</p>
										</div>
									</label>
								))}
							</div>
						)}
						{validationErrors.source && (
							<p className="text-sm text-destructive mt-2 flex items-center gap-1">
								<AlertCircle className="h-3 w-3" />
								{validationErrors.source}
							</p>
						)}
					</CardContent>
				</Card>

				{/* Sinks */}
				<Card>
					<CardHeader>
						<div className="flex items-center gap-2">
							<Server className="h-5 w-5 text-muted-foreground" />
							<CardTitle>Select Sink</CardTitle>
						</div>
						<CardDescription>
							Choose a destination for this pipeline.
						</CardDescription>
					</CardHeader>
					<CardContent>
						{isLoadingSinks ? (
							<div className="flex items-center justify-center py-8">
								<Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
							</div>
						) : sinks.length === 0 ? (
							<p className="text-center text-muted-foreground py-8">
								No sinks available. Create a sink first.
							</p>
						) : (
							<div className="space-y-3">
								{sinks.map((sink) => (
									<label
										key={sink.id}
										className="flex items-start gap-3 rounded-lg border p-4 cursor-pointer hover:bg-muted/50 transition-colors"
									>
										<input
											type="radio"
											name="sink"
											checked={selectedSink === sink.id}
											onChange={() => handleSelectSink(sink.id)}
											className="mt-1 h-4 w-4"
										/>
										<div className="flex-1">
											<p className="font-medium">{sink.name}</p>
											<p className="text-sm text-muted-foreground">
												{sink.type.toUpperCase()} - {sink.connection.host}
												{sink.connection.port && `:${sink.connection.port}`}
											</p>
										</div>
									</label>
								))}
							</div>
						)}
						{validationErrors.sink && (
							<p className="text-sm text-destructive mt-2 flex items-center gap-1">
								<AlertCircle className="h-3 w-3" />
								{validationErrors.sink}
							</p>
						)}
					</CardContent>
				</Card>

				{/* Tables */}
				<Card className="lg:col-span-2">
					<CardHeader>
						<CardTitle>Select Tables</CardTitle>
						<CardDescription>
							{selectedSource
								? "Choose which tables to sync from the selected source."
								: "Select a source first to see available tables."}
						</CardDescription>
					</CardHeader>
					<CardContent>
						{!selectedSource ? (
							<p className="text-center text-muted-foreground py-8">
								Select a source to view available tables.
							</p>
						) : isLoadingTables ? (
							<div className="flex items-center justify-center py-8">
								<Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
							</div>
						) : tables.length === 0 ? (
							<p className="text-center text-muted-foreground py-8">
								No tables found in the selected source.
							</p>
						) : (
							<div className="space-y-4">
								<div className="flex flex-wrap gap-2">
									{tables.map((table) => (
										<button
											key={table}
											type="button"
											onClick={() => handleToggleTable(table)}
											className={`inline-flex items-center gap-1 rounded-full border px-3 py-1 text-sm transition-colors ${
												selectedTables.has(table)
													? "bg-primary text-primary-foreground border-primary"
													: "bg-background hover:bg-muted"
											}`}
										>
											{table}
											{selectedTables.has(table) && (
												<X className="h-3 w-3" />
											)}
										</button>
									))}
								</div>
								{selectedTables.size > 0 && (
									<div className="flex items-center gap-2 text-sm text-muted-foreground">
										<span>Selected: {selectedTables.size} table(s)</span>
										<Button
											variant="ghost"
											size="sm"
											onClick={() => setSelectedTables(new Set())}
										>
											Clear all
										</Button>
									</div>
							)}
							{validationErrors.tables && (
								<p className="text-sm text-destructive mt-2 flex items-center gap-1">
									<AlertCircle className="h-3 w-3" />
									{validationErrors.tables}
								</p>
							)}
						</div>
					)}
				</CardContent>
			</Card>
		</div>

			{/* Mutation Error */}
			{createMutation.error && (
				<div className="mt-6 rounded-lg border border-destructive bg-destructive/10 p-4">
					<div className="flex items-center gap-2 text-destructive">
						<AlertCircle className="h-5 w-5" />
						<p className="font-medium">Failed to create pipeline</p>
					</div>
					<p className="text-sm text-destructive/80 mt-1">
						{createMutation.error instanceof Error
							? createMutation.error.message
							: "An unexpected error occurred"}
					</p>
				</div>
			)}

			{/* Actions */}
			<div className="mt-8 flex items-center justify-end gap-4">
				<Button asChild variant="outline">
					<Link to="/pipelines">
						Cancel
					</Link>
				</Button>
				<Button
					onClick={handleCreate}
					disabled={!canCreate || createMutation.isPending}
				>
					{createMutation.isPending ? (
						<>
							<Loader2 className="mr-2 h-4 w-4 animate-spin" />
							Creating...
						</>
					) : (
						<>
							<Plus className="mr-2 h-4 w-4" />
							Create Pipeline
						</>
					)}
				</Button>
			</div>
		</div>
	);
}

export default CreatePipelinePage;
