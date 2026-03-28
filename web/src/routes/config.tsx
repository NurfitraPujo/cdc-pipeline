import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { createFileRoute } from "@tanstack/react-router";
import { Settings, Save, AlertCircle, CheckCircle } from "lucide-react";
import { apiClient } from "@/api/client";
import { ConfigEditor } from "@/components/ConfigEditor";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { useState } from "react";

interface GlobalConfig {
	maxConcurrentPipelines: number;
	defaultBatchSize: number;
	defaultFlushIntervalMs: number;
	healthCheckIntervalMs: number;
	metricsRetentionDays: number;
}

const fetchGlobalConfig = async (): Promise<GlobalConfig> => {
	return apiClient.get<GlobalConfig>("/global");
};

const saveGlobalConfig = async (config: GlobalConfig): Promise<GlobalConfig> => {
	return apiClient.put<GlobalConfig>("/global", config);
};

export const Route = createFileRoute("/config")({
	component: ConfigPage,
});

const defaultConfig: GlobalConfig = {
	maxConcurrentPipelines: 10,
	defaultBatchSize: 1000,
	defaultFlushIntervalMs: 5000,
	healthCheckIntervalMs: 30000,
	metricsRetentionDays: 30,
};

function ConfigPage() {
	const queryClient = useQueryClient();
	const [saveError, setSaveError] = useState<string | null>(null);
	const [saveSuccess, setSaveSuccess] = useState(false);

	const { data: config, isLoading, error } = useQuery({
		queryKey: ["global-config"],
		queryFn: fetchGlobalConfig,
	});

	const mutation = useMutation({
		mutationFn: saveGlobalConfig,
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["global-config"] });
			setSaveSuccess(true);
			setTimeout(() => setSaveSuccess(false), 3000);
			setSaveError(null);
		},
		onError: (err: Error) => {
			setSaveError(err.message || "Failed to save configuration");
			setSaveSuccess(false);
		},
	});

	const handleSave = (jsonContent: string) => {
		try {
			const parsedConfig = JSON.parse(jsonContent) as GlobalConfig;
			mutation.mutate(parsedConfig);
		} catch (err) {
			setSaveError("Invalid JSON format. Please check your configuration.");
		}
	};

	const currentConfig = config ?? defaultConfig;
	const initialValue = JSON.stringify(currentConfig, null, 2);

	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			<div className="mb-8">
				<div className="flex items-center gap-3">
					<Settings className="h-8 w-8 text-[#56c6be]" />
					<h1 className="text-3xl font-bold tracking-tight">Global Configuration</h1>
				</div>
				<p className="mt-2 text-muted-foreground">
					Manage global settings for your CDC pipeline system.
				</p>
			</div>

			{error && (
				<div className="mb-4 flex items-center gap-2 rounded-lg border border-destructive/50 bg-destructive/10 p-4 text-destructive">
					<AlertCircle className="h-5 w-5" />
					<span>Failed to load configuration: {error.message}</span>
				</div>
			)}

			{saveError && (
				<div className="mb-4 flex items-center gap-2 rounded-lg border border-destructive/50 bg-destructive/10 p-4 text-destructive">
					<AlertCircle className="h-5 w-5" />
					<span>{saveError}</span>
				</div>
			)}

			{saveSuccess && (
				<div className="mb-4 flex items-center gap-2 rounded-lg border border-green-500/50 bg-green-500/10 p-4 text-green-600">
					<CheckCircle className="h-5 w-5" />
					<span>Configuration saved successfully!</span>
				</div>
			)}

			<Card className="mb-6">
				<CardHeader>
					<CardTitle>Configuration Settings</CardTitle>
					<CardDescription>
						Edit the global configuration below. Changes will take effect immediately after saving.
					</CardDescription>
				</CardHeader>
				<CardContent className="space-y-4">
					<div className="grid gap-4 md:grid-cols-2">
						<div className="space-y-2">
							<span className="text-sm font-medium">Max Concurrent Pipelines</span>
							<p className="text-xs text-muted-foreground">
								Maximum number of pipelines that can run simultaneously
							</p>
						</div>
						<div className="space-y-2">
							<span className="text-sm font-medium">Default Batch Size</span>
							<p className="text-xs text-muted-foreground">
								Number of events to batch before flushing
							</p>
						</div>
						<div className="space-y-2">
							<span className="text-sm font-medium">Default Flush Interval</span>
							<p className="text-xs text-muted-foreground">
								Milliseconds between automatic flushes
							</p>
						</div>
						<div className="space-y-2">
							<span className="text-sm font-medium">Health Check Interval</span>
							<p className="text-xs text-muted-foreground">
								Milliseconds between health checks
							</p>
						</div>
					</div>
				</CardContent>
			</Card>

			<ConfigEditor
				initialValue={initialValue}
				onSave={handleSave}
				isLoading={mutation.isPending}
			/>
		</div>
	);
}
