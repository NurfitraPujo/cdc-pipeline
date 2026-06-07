import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { AlertCircle, ArrowLeft, Loader2, Save } from "lucide-react";
import { useEffect, useState } from "react";
import { sinksApi, type UpdateSinkRequest } from "@/api/sinks";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";

export const Route = createFileRoute("/sinks/$id/edit")({
	component: EditSinkPage,
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

export function EditSinkPage() {
	const { id } = Route.useParams();
	const navigate = useNavigate();
	const queryClient = useQueryClient();

	const {
		data: sink,
		isLoading,
		error,
	} = useQuery({
		queryKey: ["sink", id],
		queryFn: () => sinksApi.get(id),
	});

	const [type, setType] = useState<"databend" | "postgres_debug">("databend");
	const [dsn, setDsn] = useState("");
	const [maxAckPending, setMaxAckPending] = useState("1000");
	const [optionsStr, setOptionsStr] = useState("{\n  \n}");

	const [validationErrors, setValidationErrors] = useState<
		Record<string, string>
	>({});
	const [testResult, setTestResult] = useState<{
		success: boolean;
		message: string;
	} | null>(null);

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

	// Initialize form when data is loaded
	useEffect(() => {
		if (sink) {
			setType((sink.type as any) || "databend");
			setDsn(sink.dsn || "");
			const s = sink as any;
			setMaxAckPending(
				s.maxAckPending !== undefined
					? String(s.maxAckPending)
					: s.max_ack_pending !== undefined
						? String(s.max_ack_pending)
						: "1000",
			);
			const optionsObj = s.options || {};
			setOptionsStr(JSON.stringify(optionsObj, null, 2));
		}
	}, [sink]);

	const updateMutation = useMutation({
		mutationFn: (payload: UpdateSinkRequest) => sinksApi.update(id, payload),
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["sink", id] });
			queryClient.invalidateQueries({ queryKey: ["sinks"] });
			navigate({ to: "/sinks" });
		},
	});

	const handleSubmit = (e: React.FormEvent) => {
		e.preventDefault();
		const errors: Record<string, string> = {};

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

		const payload: UpdateSinkRequest = {
			type,
			dsn,
			maxAckPending: maxAck,
			options,
		};

		updateMutation.mutate(payload);
	};

	const handleTestConnection = (e: React.MouseEvent) => {
		e.preventDefault();
		const errors: Record<string, string> = {};

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

		const payload: any = {
			id,
			type,
			dsn,
			maxAckPending: maxAck,
			options,
		};

		testMutation.mutate(payload);
	};

	if (isLoading) {
		return (
			<div className="page-wrap px-4 pb-8 pt-14 flex items-center justify-center">
				<Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		);
	}

	if (error || !sink) {
		return (
			<div className="page-wrap px-4 pb-8 pt-14 max-w-4xl mx-auto text-center">
				<AlertCircle className="h-12 w-12 text-destructive mx-auto mb-4" />
				<h2 className="text-xl font-bold">Failed to Load Sink</h2>
				<p className="mt-2 text-muted-foreground">
					{error?.message || "Sink not found"}
				</p>
				<Button asChild className="mt-4" variant="outline">
					<Link to="/sinks">
						<ArrowLeft className="mr-2 h-4 w-4" />
						Back to Sinks
					</Link>
				</Button>
			</div>
		);
	}

	return (
		<div className="page-wrap px-4 pb-8 pt-14 max-w-4xl mx-auto">
			<div className="mb-6">
				<Button asChild variant="ghost" className="mb-4">
					<Link to="/sinks">
						<ArrowLeft className="mr-2 h-4 w-4" />
						Back to Sinks
					</Link>
				</Button>
				<h1 className="text-3xl font-bold tracking-tight">Edit Sink: {id}</h1>
				<p className="mt-2 text-muted-foreground">
					Modify destination sink details and options.
				</p>
			</div>

			{updateMutation.isError && (
				<Card className="mb-6 border-destructive bg-destructive/10">
					<CardContent className="pt-6">
						<div className="flex items-center gap-2 text-destructive-foreground">
							<AlertCircle className="h-5 w-5" />
							<p className="font-semibold">Update Failed</p>
						</div>
						<p className="mt-2 text-sm text-destructive-foreground">
							{updateMutation.error instanceof Error
								? updateMutation.error.message
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
								<Input id="sink-id" value={id} disabled />
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
						disabled={testMutation.isPending || updateMutation.isPending}
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
						disabled={updateMutation.isPending || testMutation.isPending}
					>
						{updateMutation.isPending ? (
							<>
								<Loader2 className="mr-2 h-4 w-4 animate-spin" />
								Saving...
							</>
						) : (
							<>
								<Save className="mr-2 h-4 w-4" />
								Save Changes
							</>
						)}
					</Button>
				</div>
			</form>
		</div>
	);
}
