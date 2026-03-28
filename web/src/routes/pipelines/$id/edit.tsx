import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { ArrowLeft, AlertCircle, Loader2 } from "lucide-react";
import { pipelinesApi } from "@/api/pipelines";
import { ConfigEditor } from "@/components/ConfigEditor";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import type { Pipeline } from "@/api/types";

export const Route = createFileRoute("/pipelines/$id/edit")({
	component: EditPipelinePage,
});

// Convert pipeline config to JSON format
function pipelineToJson(pipeline: Pipeline): string {
	const config = {
		id: pipeline.id,
		name: pipeline.name,
		description: pipeline.description || "",
		status: pipeline.status,
		source: {
			id: pipeline.source.id,
			type: pipeline.source.type,
			name: pipeline.source.name,
			connection: {
				host: pipeline.source.connection.host,
				port: pipeline.source.connection.port,
				database: pipeline.source.connection.database,
				username: pipeline.source.connection.username,
				ssl: pipeline.source.connection.ssl || false,
			},
			tables: pipeline.source.tables,
		},
		sink: {
			id: pipeline.sink.id,
			type: pipeline.sink.type,
			name: pipeline.sink.name,
			connection: {
				host: pipeline.sink.connection.host,
				port: pipeline.sink.connection.port,
				index: pipeline.sink.connection.index,
				topic: pipeline.sink.connection.topic,
				url: pipeline.sink.connection.url,
				headers: pipeline.sink.connection.headers,
			},
		},
		processorConfig: {
			batchSize: pipeline.processorConfig.batchSize,
			flushIntervalMs: pipeline.processorConfig.flushIntervalMs,
			maxRetries: pipeline.processorConfig.maxRetries,
			retryDelayMs: pipeline.processorConfig.retryDelayMs,
			deadLetterQueueEnabled: pipeline.processorConfig.deadLetterQueueEnabled,
		},
	};

	return `// Pipeline Configuration
// Edit the configuration below and click Save to apply changes

${JSON.stringify(config, null, 2)}
`;
}

// Parse JSON content back to update request
function jsonToUpdateRequest(jsonContent: string): Record<string, unknown> {
	try {
		// Remove comments and parse JSON
		const cleanedContent = jsonContent
			.split("\n")
			.filter((line) => !line.trim().startsWith("#"))
			.join("\n");

		const parsed = JSON.parse(cleanedContent);

		// Extract only the updatable fields
		return {
			name: parsed.name,
			description: parsed.description,
			source: parsed.source,
			sink: parsed.sink,
			processorConfig: parsed.processorConfig,
		};
	} catch {
		throw new Error("Invalid configuration format. Please ensure valid JSON syntax.");
	}
}

function EditPipelinePage() {
	const { id } = Route.useParams();
	const navigate = useNavigate();
	const queryClient = useQueryClient();

	const {
		data: pipeline,
		isLoading,
		error,
	} = useQuery({
		queryKey: ["pipeline", id],
		queryFn: () => pipelinesApi.get(id),
	});

	const updateMutation = useMutation({
		mutationFn: (config: string) => {
			const updateData = jsonToUpdateRequest(config);
			return pipelinesApi.update(id, updateData);
		},
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["pipeline", id] });
			queryClient.invalidateQueries({ queryKey: ["pipelines"] });
			navigate({ to: `/pipelines/${id}` });
		},
	});

	const handleSave = (value: string) => {
		updateMutation.mutate(value);
	};

	if (isLoading) {
		return (
			<div className="page-wrap px-4 pb-8 pt-14">
				<div className="flex items-center justify-center py-12">
					<Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
				</div>
			</div>
		);
	}

	if (error || !pipeline) {
		return (
			<div className="page-wrap px-4 pb-8 pt-14">
				<div className="flex flex-col items-center justify-center py-12">
					<AlertCircle className="h-12 w-12 text-destructive mb-4" />
					<h2 className="text-xl font-semibold">Failed to load pipeline</h2>
					<p className="text-muted-foreground mt-2">
						{error?.message || "Pipeline not found"}
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

	const initialValue = pipelineToJson(pipeline);

	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			{/* Header */}
			<div className="mb-8">
				<Button asChild variant="outline" size="sm">
					<Link to={`/pipelines/${id}`}>
						<ArrowLeft className="mr-2 h-4 w-4" />
						Back to Pipeline
					</Link>
				</Button>
				<h1 className="mt-4 text-3xl font-bold tracking-tight">Edit Pipeline</h1>
				<p className="mt-2 text-muted-foreground">
					Edit the configuration for <strong>{pipeline.name}</strong>.
				</p>
			</div>

			{/* Error display */}
			{updateMutation.error && (
				<Card className="mb-6 border-destructive">
					<CardHeader>
						<CardTitle className="text-destructive flex items-center gap-2">
							<AlertCircle className="h-5 w-5" />
							Save Failed
						</CardTitle>
					</CardHeader>
					<CardContent>
						<p className="text-sm text-destructive">
							{updateMutation.error instanceof Error
								? updateMutation.error.message
								: "Failed to save configuration"}
						</p>
					</CardContent>
				</Card>
			)}

			{/* Config Editor */}
			<ConfigEditor
				initialValue={initialValue}
				onSave={handleSave}
				isLoading={updateMutation.isPending}
			/>

			{/* Info card */}
			<Card className="mt-6">
				<CardHeader>
					<CardTitle>Editing Tips</CardTitle>
					<CardDescription>
						Guidelines for editing pipeline configuration
					</CardDescription>
				</CardHeader>
				<CardContent className="space-y-2 text-sm text-muted-foreground">
					<ul className="list-disc list-inside space-y-1">
					<li>Ensure JSON syntax is valid before saving</li>
						<li>Connection passwords are not shown for security reasons</li>
						<li>Changes to the pipeline ID are not allowed</li>
						<li>Use the Reset button to discard changes</li>
					</ul>
				</CardContent>
			</Card>
		</div>
	);
}

export default EditPipelinePage;
