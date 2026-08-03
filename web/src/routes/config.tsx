import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute } from "@tanstack/react-router";
import { AlertCircle, CheckCircle, Settings } from "lucide-react";
import { useState } from "react";
import { type GlobalConfig, globalConfigApi } from "@/api/globalConfig";
import { ConfigEditor } from "@/components/ConfigEditor";
import { GlobalConfigForm } from "@/components/GlobalConfigForm";
import { Button } from "@/components/ui/button";

// Mirrors protocol.GlobalConfig.SetDefaults() in internal/protocol/config.go.
const defaultConfig: GlobalConfig = {
	batchSize: 1000,
	batchWait: "1s",
	retry: {
		maxRetries: 3,
		initialInterval: "1s",
		maxInterval: "30s",
		enableDlq: false,
	},
	drainTimeout: "30s",
	shutdownTimeout: "30s",
	stabilizationDelay: "2s",
	crashRecoveryDelay: "5s",
	globalReloadDelay: "2s",
};

const fetchGlobalConfig = async (): Promise<GlobalConfig> => {
	return globalConfigApi.get();
};

const saveGlobalConfig = async (
	config: GlobalConfig,
): Promise<GlobalConfig> => {
	return globalConfigApi.update(config);
};

export const Route = createFileRoute("/config")({
	component: ConfigPage,
});

function ConfigPage() {
	const queryClient = useQueryClient();
	const [saveError, setSaveError] = useState<string | null>(null);
	const [saveSuccess, setSaveSuccess] = useState(false);
	const [mode, setMode] = useState<"form" | "json">("form");

	const { data: config, error } = useQuery({
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
		} catch (_err) {
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
					<h1 className="text-3xl font-bold tracking-tight">
						Global Configuration
					</h1>
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

			<div className="mb-6 flex items-center gap-3">
				<Button
					variant={mode === "form" ? "default" : "outline"}
					size="sm"
					onClick={() => setMode("form")}
				>
					Form
				</Button>
				<Button
					variant={mode === "json" ? "default" : "outline"}
					size="sm"
					onClick={() => setMode("json")}
				>
					Raw JSON
				</Button>
			</div>

			{mode === "form" ? (
				<GlobalConfigForm
					value={currentConfig}
					onSave={(cfg) => mutation.mutate(cfg)}
					isSaving={mutation.isPending}
				/>
			) : (
				<ConfigEditor
					initialValue={initialValue}
					onSave={handleSave}
					isLoading={mutation.isPending}
				/>
			)}
		</div>
	);
}
