import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { AlertCircle, ArrowLeft, Loader2, Save } from "lucide-react";
import { useEffect, useState } from "react";
import { camelToSnake } from "@/api/mappers";
import { type Pipeline, pipelinesApi } from "@/api/pipelines";
import { ConfigEditor } from "@/components/ConfigEditor";
import { AdvancedConfigPanel } from "@/components/pipelines/AdvancedConfigPanel";
import {
	type AdvancedConfig,
	advancedConfigToPayload,
	defaultAdvancedConfig,
} from "@/components/pipelines/advancedConfig";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { jsonToUpdateRequest } from "@/lib/jsonToUpdateRequest";
import { mergeWithCurrent } from "@/lib/pipelineMerge";

export const Route = createFileRoute("/pipelines/$id/edit")({
	component: EditPipelinePage,
});

// Convert pipeline config to JSON format
function pipelineToJson(pipeline: Pipeline): string {
	const config = {
		id: pipeline.id,
		name: pipeline.name,
		sources: pipeline.sources,
		sinks: pipeline.sinks,
		processors: pipeline.processors,
		tables: pipeline.tables,
		batch_size: pipeline.batchSize,
		batch_wait: pipeline.batchWait,
		retry: pipeline.retry,
	};

	return `// Pipeline Configuration
// Edit the configuration below and click Save to apply changes

${JSON.stringify(config, null, 2)}
`;
}

function pipelineToAdvancedConfig(p: Pipeline): AdvancedConfig {
	return {
		batchSize: p.batchSize,
		batchWait: p.batchWait,
		retry: p.retry
			? {
					maxRetries: p.retry.max_retries,
					initialInterval: p.retry.initial_interval,
					maxInterval: p.retry.max_interval,
					enableDlq: p.retry.enable_dlq,
				}
			: undefined,
		processors: (p.processors ?? []).map((pp) => ({
			name: pp.name,
			type: pp.type,
			options: pp.options ?? undefined,
			operationTypes: pp.operation_types ?? undefined,
		})),
	};
}

const REQUIRED_FIELDS = ["id", "name", "sources", "sinks", "tables"] as const;

function missingRequiredFields(jsonContent: string): string[] {
	try {
		const parsed = jsonToUpdateRequest(jsonContent);
		return REQUIRED_FIELDS.filter((f) => {
			const v = parsed[f];
			if (Array.isArray(v)) return v.length === 0;
			return v === undefined || v === null || v === "";
		});
	} catch {
		return REQUIRED_FIELDS.slice();
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

	const [useFormView, setUseFormView] = useState(false);
	const [formConfig, setFormConfig] = useState<AdvancedConfig>(
		defaultAdvancedConfig,
	);
	const [jsonWarning, setJsonWarning] = useState<string | null>(null);
	const [validationError, setValidationError] = useState<string | null>(null);

	useEffect(() => {
		if (pipeline) {
			setFormConfig(pipelineToAdvancedConfig(pipeline));
		}
	}, [pipeline]);

	const updateMutation = useMutation({
		mutationFn: (data: Parameters<typeof pipelinesApi.update>[1]) =>
			pipelinesApi.update(id, data),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["pipeline", id] });
			queryClient.invalidateQueries({ queryKey: ["pipelines"] });
			navigate({ to: "/pipelines/$id" as const, params: { id } });
		},
	});

	const handleSaveFromForm = () => {
		if (!pipeline) return;
		setValidationError(null);
		const required = {
			id: pipeline.id,
			name: pipeline.name,
			sources: pipeline.sources,
			sinks: pipeline.sinks,
			tables: pipeline.tables,
		};
		const advanced = camelToSnake<Record<string, unknown>>(
			advancedConfigToPayload(formConfig),
		);
		updateMutation.mutate({
			...required,
			...advanced,
		} as Parameters<typeof pipelinesApi.update>[1]);
	};

	const handleSaveFromJson = (jsonValue: string) => {
		if (!pipeline) return;
		setValidationError(null);
		const missing = missingRequiredFields(jsonValue);
		if (missing.length > 0) {
			setJsonWarning(
				`Missing required field(s): ${missing.join(", ")}. The pipeline will fall back to current values where possible.`,
			);
		} else {
			setJsonWarning(null);
		}
		try {
			const parsed = jsonToUpdateRequest(jsonValue);
			const merged = mergeWithCurrent(parsed, pipeline);
			updateMutation.mutate(merged);
		} catch (err) {
			setValidationError(err instanceof Error ? err.message : String(err));
		}
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
					<Link to="/pipelines/$id" params={{ id }}>
						<ArrowLeft className="mr-2 h-4 w-4" />
						Back to Pipeline
					</Link>
				</Button>
				<h1 className="mt-4 text-3xl font-bold tracking-tight">
					Edit Pipeline
				</h1>
				<p className="mt-2 text-muted-foreground">
					Edit the configuration for <strong>{pipeline.name}</strong>.
				</p>
			</div>

			{/* Error display */}
			{(updateMutation.error || validationError) && (
				<Card className="mb-6 border-destructive">
					<CardHeader>
						<CardTitle className="text-destructive flex items-center gap-2">
							<AlertCircle className="h-5 w-5" />
							Save Failed
						</CardTitle>
					</CardHeader>
					<CardContent>
						<p className="text-sm text-destructive">
							{validationError ||
								(updateMutation.error instanceof Error
									? updateMutation.error.message
									: "Failed to save configuration")}
						</p>
					</CardContent>
				</Card>
			)}

			{/* View toggle */}
			<div className="mb-4 flex items-center justify-end gap-2">
				<Switch
					id="use-form-view"
					checked={useFormView}
					onCheckedChange={setUseFormView}
				/>
				<Label htmlFor="use-form-view">Use form view</Label>
			</div>

			{/* Non-blocking warning for JSON view */}
			{!useFormView && jsonWarning && (
				<Card className="mb-6 border-yellow-500">
					<CardHeader>
						<CardTitle className="text-yellow-700 flex items-center gap-2">
							<AlertCircle className="h-5 w-5" />
							Heads up
						</CardTitle>
					</CardHeader>
					<CardContent>
						<p className="text-sm text-yellow-700">{jsonWarning}</p>
					</CardContent>
				</Card>
			)}

			{useFormView ? (
				<Card>
					<CardHeader>
						<CardTitle>Configuration</CardTitle>
						<CardDescription>
							Edit batch, retry, and processors for this pipeline.
						</CardDescription>
					</CardHeader>
					<CardContent className="space-y-4">
						<AdvancedConfigPanel
							value={formConfig}
							onChange={setFormConfig}
							defaultOpen={["batch", "retry", "processors"]}
						/>
						<div className="flex justify-end">
							<Button
								onClick={handleSaveFromForm}
								disabled={updateMutation.isPending}
							>
								{updateMutation.isPending ? (
									<>
										<Loader2 className="mr-2 h-4 w-4 animate-spin" />
										Saving...
									</>
								) : (
									<>
										<Save className="mr-2 h-4 w-4" />
										Save
									</>
								)}
							</Button>
						</div>
					</CardContent>
				</Card>
			) : (
				<ConfigEditor
					initialValue={initialValue}
					onSave={handleSaveFromJson}
					isLoading={updateMutation.isPending}
				/>
			)}

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
						<li>Toggle "Use form view" for a guided editor</li>
					</ul>
				</CardContent>
			</Card>
		</div>
	);
}

export default EditPipelinePage;
