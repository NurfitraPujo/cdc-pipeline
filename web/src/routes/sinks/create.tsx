import { useMutation, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { AlertCircle, ArrowLeft, Loader2, Plus } from "lucide-react";
import { useState } from "react";
import { type CreateSinkRequest, sinksApi } from "@/api/sinks";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";

export const Route = createFileRoute("/sinks/create")({
	component: CreateSinkPage,
});

function isValidJson(v: string): boolean {
	if (!v.trim()) return true;
	try {
		const parsed = JSON.parse(v);
		return (
			parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
		);
	} catch {
		return false;
	}
}

export function CreateSinkPage() {
	const navigate = useNavigate();
	const queryClient = useQueryClient();

	const [id, setId] = useState("");
	const [type, setType] = useState<"databend" | "postgres_debug">("databend");
	const [dsn, setDsn] = useState("");
	const [maxAckPending, setMaxAckPending] = useState("1000");
	const [optionsStr, setOptionsStr] = useState("{\n  \n}");
	const [testResult, setTestResult] = useState<{
		success: boolean;
		message: string;
	} | null>(null);

	const [validationErrors, setValidationErrors] = useState<
		Record<string, string>
	>({});

	const createMutation = useMutation({
		mutationFn: sinksApi.create,
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["sinks"] });
			navigate({ to: "/sinks" });
		},
	});

	const testMutation = useMutation({
		mutationFn: sinksApi.testConnection,
		onSuccess: () => {
			setTestResult({ success: true, message: "Connection test succeeded!" });
		},
		onError: (err: any) => {
			setTestResult({
				success: false,
				message: err.message || "Connection test failed.",
			});
		},
	});

	const handleSubmit = (e: React.FormEvent) => {
		e.preventDefault();
		const errors: Record<string, string> = {};

		if (!id) {
			errors.id = "Sink ID is required";
		} else if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
			errors.id = "Sink ID must be alphanumeric (dashes/underscores allowed)";
		}

		if (!dsn) {
			errors.dsn = "DSN (connection string) is required";
		}

		const maxAck = Number(maxAckPending);
		if (!maxAckPending) {
			errors.maxAckPending = "Max ACK Pending is required";
		} else if (Number.isNaN(maxAck) || maxAck < 0) {
			errors.maxAckPending = "Max ACK Pending must be a non-negative number";
		}

		if (optionsStr && !isValidJson(optionsStr)) {
			errors.options = "Invalid JSON. Must be a valid JSON object.";
		}

		if (Object.keys(errors).length > 0) {
			setValidationErrors(errors);
			return;
		}

		setValidationErrors({});

		let options: Record<string, any> = {};
		if (optionsStr.trim()) {
			try {
				options = JSON.parse(optionsStr);
			} catch (_) {}
		}

		const payload: CreateSinkRequest = {
			id,
			type,
			dsn,
			maxAckPending: maxAck,
			options,
		};

		createMutation.mutate(payload);
	};

	const handleTestConnection = (e: React.MouseEvent) => {
		e.preventDefault();
		const errors: Record<string, string> = {};

		if (!id) {
			errors.id = "Sink ID is required";
		} else if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
			errors.id = "Sink ID must be alphanumeric (dashes/underscores allowed)";
		}

		if (!dsn) {
			errors.dsn = "DSN (connection string) is required";
		}

		const maxAck = Number(maxAckPending);
		if (!maxAckPending) {
			errors.maxAckPending = "Max ACK Pending is required";
		} else if (Number.isNaN(maxAck) || maxAck < 0) {
			errors.maxAckPending = "Max ACK Pending must be a non-negative number";
		}

		if (optionsStr && !isValidJson(optionsStr)) {
			errors.options = "Invalid JSON. Must be a valid JSON object.";
		}

		if (Object.keys(errors).length > 0) {
			setValidationErrors(errors);
			return;
		}

		setValidationErrors({});
		setTestResult(null);

		let options: Record<string, any> = {};
		if (optionsStr.trim()) {
			try {
				options = JSON.parse(optionsStr);
			} catch (_) {}
		}

		const payload: CreateSinkRequest = {
			id,
			type,
			dsn,
			maxAckPending: maxAck,
			options,
		};

		testMutation.mutate(payload);
	};

	return (
		<div className="page-wrap px-4 pb-8 pt-14 max-w-4xl mx-auto">
			<div className="mb-6">
				<Button asChild variant="ghost" className="mb-4">
					<Link to="/sinks">
						<ArrowLeft className="mr-2 h-4 w-4" />
						Back to Sinks
					</Link>
				</Button>
				<h1 className="text-3xl font-bold tracking-tight">Create Sink</h1>
				<p className="mt-2 text-muted-foreground">
					Configure a new destination sink for CDC data delivery.
				</p>
			</div>

			{createMutation.isError && (
				<Card className="mb-6 border-destructive bg-destructive/10">
					<CardContent className="pt-6">
						<div className="flex items-center gap-2 text-destructive-foreground">
							<AlertCircle className="h-5 w-5" />
							<p className="font-semibold">Creation Failed</p>
						</div>
						<p className="mt-2 text-sm text-destructive-foreground">
							{createMutation.error instanceof Error
								? createMutation.error.message
								: "An unexpected error occurred. Please try again."}
						</p>
					</CardContent>
				</Card>
			)}

			{testResult && (
				<Card
					className={`mb-6 border-${testResult.success ? "emerald-500" : "destructive"} bg-${testResult.success ? "emerald-500/10" : "destructive/10"}`}
				>
					<CardContent className="pt-6">
						<div
							className={`flex items-center gap-2 ${testResult.success ? "text-emerald-500" : "text-destructive-foreground"}`}
						>
							<AlertCircle className="h-5 w-5" />
							<p className="font-semibold">
								{testResult.success
									? "Connection Test Passed"
									: "Connection Test Failed"}
							</p>
						</div>
						<p
							className={`mt-2 text-sm ${testResult.success ? "text-emerald-600" : "text-destructive-foreground"}`}
						>
							{testResult.message}
						</p>
					</CardContent>
				</Card>
			)}

			<form onSubmit={handleSubmit} className="space-y-6">
				<Card>
					<CardHeader>
						<CardTitle>Destination Settings</CardTitle>
						<CardDescription>
							Primary connection details and backend type for the sink.
						</CardDescription>
					</CardHeader>
					<CardContent className="space-y-4">
						<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
							<div className="space-y-2">
								<label htmlFor="sink-id" className="text-sm font-medium">
									Sink ID
								</label>
								<Input
									id="sink-id"
									placeholder="e.g. analytics-databend"
									value={id}
									onChange={(e) => setId(e.target.value)}
								/>
								{validationErrors.id && (
									<p className="text-xs text-destructive">
										{validationErrors.id}
									</p>
								)}
							</div>
							<div className="space-y-2">
								<label htmlFor="sink-type" className="text-sm font-medium">
									Type
								</label>
								<select
									id="sink-type"
									className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-xs transition-colors focus-visible:outline-hidden focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
									value={type}
									onChange={(e) => setType(e.target.value as any)}
								>
									<option value="databend">Databend</option>
									<option value="postgres_debug">PostgreSQL Debug</option>
								</select>
							</div>
						</div>

						<div className="space-y-2">
							<label htmlFor="sink-dsn" className="text-sm font-medium">
								DSN (Data Source Name)
							</label>
							<Input
								id="sink-dsn"
								placeholder="e.g. databend://localhost:8000/analytics"
								value={dsn}
								onChange={(e) => setDsn(e.target.value)}
							/>
							{validationErrors.dsn && (
								<p className="text-xs text-destructive">
									{validationErrors.dsn}
								</p>
							)}
						</div>

						<div className="space-y-2">
							<label htmlFor="sink-max-ack" className="text-sm font-medium">
								Max ACK Pending
							</label>
							<Input
								id="sink-max-ack"
								placeholder="1000"
								value={maxAckPending}
								onChange={(e) => setMaxAckPending(e.target.value)}
							/>
							{validationErrors.maxAckPending && (
								<p className="text-xs text-destructive">
									{validationErrors.maxAckPending}
								</p>
							)}
						</div>

						<div className="space-y-2">
							<label htmlFor="sink-options" className="text-sm font-medium">
								Options (JSON Object)
							</label>
							<textarea
								id="sink-options"
								rows={5}
								className="flex min-h-[120px] w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm font-mono shadow-xs placeholder:text-muted-foreground focus-visible:outline-hidden focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
								value={optionsStr}
								onChange={(e) => setOptionsStr(e.target.value)}
							/>
							{validationErrors.options && (
								<p className="text-xs text-destructive">
									{validationErrors.options}
								</p>
							)}
						</div>
					</CardContent>
				</Card>

				<div className="flex justify-end gap-3">
					<Button asChild variant="outline">
						<Link to="/sinks">Cancel</Link>
					</Button>
					<Button
						type="button"
						variant="secondary"
						disabled={testMutation.isPending || createMutation.isPending}
						onClick={handleTestConnection}
					>
						{testMutation.isPending ? (
							<>
								<Loader2 className="mr-2 h-4 w-4 animate-spin" />
								Testing...
							</>
						) : (
							"Test Connection"
						)}
					</Button>
					<Button
						type="submit"
						disabled={createMutation.isPending || testMutation.isPending}
					>
						{createMutation.isPending ? (
							<>
								<Loader2 className="mr-2 h-4 w-4 animate-spin" />
								Creating...
							</>
						) : (
							<>
								<Plus className="mr-2 h-4 w-4" />
								Create Sink
							</>
						)}
					</Button>
				</div>
			</form>
		</div>
	);
}
