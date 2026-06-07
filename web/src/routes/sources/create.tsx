import { useMutation, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { AlertCircle, ArrowLeft, Loader2, Plus, X } from "lucide-react";
import { useState } from "react";
import { type CreateSourceRequest, sourcesApi } from "@/api/sources";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";

export const Route = createFileRoute("/sources/create")({
	component: CreateSourcePage,
});

function isValidDuration(v: string): boolean {
	if (!v) return true;
	return /^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$/.test(v);
}

export function CreateSourcePage() {
	const navigate = useNavigate();
	const queryClient = useQueryClient();

	const [id, setId] = useState("");
	const [type] = useState<"postgres">("postgres");
	const [host, setHost] = useState("");
	const [port, setPort] = useState("5432");
	const [database, setDatabase] = useState("");
	const [user, setUser] = useState("");
	const [password, setPassword] = useState("");

	// Advanced Settings
	const [showAdvanced, setShowAdvanced] = useState(false);
	const [slotName, setSlotName] = useState("");
	const [publicationName, setPublicationName] = useState("");
	const [batchSize, setBatchSize] = useState("");
	const [batchWait, setBatchWait] = useState("");
	const [discoveryInterval, setDiscoveryInterval] = useState("");
	const [snapshotChunkSize, setSnapshotChunkSize] = useState("");
	const [snapshotInterval, setSnapshotInterval] = useState("");

	// Schema & Tables Tag Inputs
	const [schemas, setSchemas] = useState<string[]>([]);
	const [schemaInput, setSchemaInput] = useState("");
	const [tables, setTables] = useState<string[]>([]);
	const [tableInput, setTableInput] = useState("");

	const [validationErrors, setValidationErrors] = useState<
		Record<string, string>
	>({});
	const [testResult, setTestResult] = useState<{
		success: boolean;
		message: string;
	} | null>(null);

	const createMutation = useMutation({
		mutationFn: sourcesApi.create,
		onSuccess: () => {
			queryClient.invalidateQueries({ queryKey: ["sources"] });
			navigate({ to: "/sources" });
		},
	});

	const testMutation = useMutation({
		mutationFn: sourcesApi.testConnection,
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

	const handleAddSchema = (e: React.KeyboardEvent<HTMLInputElement>) => {
		if (e.key === "Enter" || e.key === ",") {
			e.preventDefault();
			const trimmed = schemaInput.trim();
			if (trimmed && !schemas.includes(trimmed)) {
				setSchemas([...schemas, trimmed]);
			}
			setSchemaInput("");
		}
	};

	const handleRemoveSchema = (index: number) => {
		setSchemas(schemas.filter((_, i) => i !== index));
	};

	const handleAddTable = (e: React.KeyboardEvent<HTMLInputElement>) => {
		if (e.key === "Enter" || e.key === ",") {
			e.preventDefault();
			const trimmed = tableInput.trim();
			if (trimmed && !tables.includes(trimmed)) {
				setTables([...tables, trimmed]);
			}
			setTableInput("");
		}
	};

	const handleRemoveTable = (index: number) => {
		setTables(tables.filter((_, i) => i !== index));
	};

	const handleSubmit = (e: React.FormEvent) => {
		e.preventDefault();
		const errors: Record<string, string> = {};

		if (!id) {
			errors.id = "Source ID is required";
		} else if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
			errors.id = "Source ID must be alphanumeric (dashes/underscores allowed)";
		}

		if (!host) errors.host = "Host is required";
		if (!database) errors.database = "Database name is required";
		if (!user) errors.user = "User is required";
		if (!password) errors.password = "Password is required";

		const portNum = Number(port);
		if (!port) {
			errors.port = "Port is required";
		} else if (Number.isNaN(portNum) || portNum < 1 || portNum > 65535) {
			errors.port = "Port must be a valid number between 1 and 65535";
		}

		if (batchWait && !isValidDuration(batchWait)) {
			errors.batchWait = "Invalid duration. Format: '30s', '1m', '500ms'";
		}
		if (discoveryInterval && !isValidDuration(discoveryInterval)) {
			errors.discoveryInterval =
				"Invalid duration. Format: '30s', '1m', '500ms'";
		}
		if (snapshotInterval && !isValidDuration(snapshotInterval)) {
			errors.snapshotInterval =
				"Invalid duration. Format: '30s', '1m', '500ms'";
		}

		if (Object.keys(errors).length > 0) {
			setValidationErrors(errors);
			return;
		}

		setValidationErrors({});

		const payload: CreateSourceRequest = {
			id,
			type,
			host,
			port: portNum,
			database,
			user,
			pass: password,
			tables,
			// Additional fields if filled
			...(slotName && { slotName }),
			...(publicationName && { publicationName }),
			...(batchSize && { batchSize: Number(batchSize) }),
			...(batchWait && { batchWait }),
			...(discoveryInterval && { discoveryInterval }),
			...(snapshotChunkSize && {
				snapshotChunkSize: Number(snapshotChunkSize),
			}),
			...(snapshotInterval && { snapshotInterval }),
			schemas,
		};

		createMutation.mutate(payload);
	};

	const handleTestConnection = (e: React.MouseEvent) => {
		e.preventDefault();
		const errors: Record<string, string> = {};

		if (!id) {
			errors.id = "Source ID is required";
		} else if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
			errors.id = "Source ID must be alphanumeric (dashes/underscores allowed)";
		}

		if (!host) errors.host = "Host is required";
		if (!database) errors.database = "Database name is required";
		if (!user) errors.user = "User is required";
		if (!password) errors.password = "Password is required";

		const portNum = Number(port);
		if (!port) {
			errors.port = "Port is required";
		} else if (Number.isNaN(portNum) || portNum < 1 || portNum > 65535) {
			errors.port = "Port must be a valid number between 1 and 65535";
		}

		if (batchWait && !isValidDuration(batchWait)) {
			errors.batchWait = "Invalid duration. Format: '30s', '1m', '500ms'";
		}
		if (discoveryInterval && !isValidDuration(discoveryInterval)) {
			errors.discoveryInterval =
				"Invalid duration. Format: '30s', '1m', '500ms'";
		}
		if (snapshotInterval && !isValidDuration(snapshotInterval)) {
			errors.snapshotInterval =
				"Invalid duration. Format: '30s', '1m', '500ms'";
		}

		if (Object.keys(errors).length > 0) {
			setValidationErrors(errors);
			return;
		}

		setValidationErrors({});
		setTestResult(null);

		const payload: CreateSourceRequest = {
			id,
			type,
			host,
			port: portNum,
			database,
			user,
			pass: password,
			tables,
			...(slotName && { slotName }),
			...(publicationName && { publicationName }),
			...(batchSize && { batchSize: Number(batchSize) }),
			...(batchWait && { batchWait }),
			...(discoveryInterval && { discoveryInterval }),
			...(snapshotChunkSize && {
				snapshotChunkSize: Number(snapshotChunkSize),
			}),
			...(snapshotInterval && { snapshotInterval }),
			schemas,
		};

		testMutation.mutate(payload);
	};

	return (
		<div className="page-wrap px-4 pb-8 pt-14 max-w-4xl mx-auto">
			<div className="mb-6">
				<Button asChild variant="ghost" className="mb-4">
					<Link to="/sources">
						<ArrowLeft className="mr-2 h-4 w-4" />
						Back to Sources
					</Link>
				</Button>
				<h1 className="text-3xl font-bold tracking-tight">Create Source</h1>
				<p className="mt-2 text-muted-foreground">
					Configure a new PostgreSQL database source to start capturing change
					events.
				</p>
			</div>

			{createMutation.isError && (
				<Card className="mb-6 border-destructive bg-destructive/10">
					<CardContent className="pt-6">
						<div className="flex items-center gap-2 text-destructive">
							<AlertCircle className="h-5 w-5" />
							<p className="font-semibold">Creation Failed</p>
						</div>
						<p className="mt-2 text-sm text-destructive">
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
							className={`flex items-center gap-2 ${testResult.success ? "text-emerald-500" : "text-destructive"}`}
						>
							<AlertCircle className="h-5 w-5" />
							<p className="font-semibold">
								{testResult.success
									? "Connection Test Passed"
									: "Connection Test Failed"}
							</p>
						</div>
						<p
							className={`mt-2 text-sm ${testResult.success ? "text-emerald-600" : "text-destructive"}`}
						>
							{testResult.message}
						</p>
					</CardContent>
				</Card>
			)}

			<form onSubmit={handleSubmit} className="space-y-6">
				<Card>
					<CardHeader>
						<CardTitle>Connection Settings</CardTitle>
						<CardDescription>
							Primary credentials and host info for the database.
						</CardDescription>
					</CardHeader>
					<CardContent className="space-y-4">
						<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
							<div className="space-y-2">
								<label htmlFor="source-id" className="text-sm font-medium">
									Source ID
								</label>
								<Input
									id="source-id"
									placeholder="e.g. main-postgres"
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
								<label htmlFor="source-type" className="text-sm font-medium">
									Type
								</label>
								<select
									id="source-type"
									className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-xs transition-colors focus-visible:outline-hidden focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
									value={type}
									disabled
								>
									<option value="postgres">PostgreSQL</option>
								</select>
							</div>
						</div>

						<div className="grid grid-cols-1 md:grid-cols-3 gap-4">
							<div className="space-y-2 md:col-span-2">
								<label htmlFor="source-host" className="text-sm font-medium">
									Host
								</label>
								<Input
									id="source-host"
									placeholder="e.g. localhost or 10.0.0.5"
									value={host}
									onChange={(e) => setHost(e.target.value)}
								/>
								{validationErrors.host && (
									<p className="text-xs text-destructive">
										{validationErrors.host}
									</p>
								)}
							</div>
							<div className="space-y-2">
								<label htmlFor="source-port" className="text-sm font-medium">
									Port
								</label>
								<Input
									id="source-port"
									placeholder="5432"
									value={port}
									onChange={(e) => setPort(e.target.value)}
								/>
								{validationErrors.port && (
									<p className="text-xs text-destructive">
										{validationErrors.port}
									</p>
								)}
							</div>
						</div>

						<div className="space-y-2">
							<label htmlFor="source-database" className="text-sm font-medium">
								Database Name
							</label>
							<Input
								id="source-database"
								placeholder="e.g. production"
								value={database}
								onChange={(e) => setDatabase(e.target.value)}
							/>
							{validationErrors.database && (
								<p className="text-xs text-destructive">
									{validationErrors.database}
								</p>
							)}
						</div>

						<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
							<div className="space-y-2">
								<label htmlFor="source-user" className="text-sm font-medium">
									User
								</label>
								<Input
									id="source-user"
									placeholder="e.g. postgres"
									value={user}
									onChange={(e) => setUser(e.target.value)}
								/>
								{validationErrors.user && (
									<p className="text-xs text-destructive">
										{validationErrors.user}
									</p>
								)}
							</div>
							<div className="space-y-2">
								<label
									htmlFor="source-password"
									className="text-sm font-medium"
								>
									Password
								</label>
								<Input
									id="source-password"
									type="password"
									placeholder="••••••••"
									value={password}
									onChange={(e) => setPassword(e.target.value)}
								/>
								{validationErrors.password && (
									<p className="text-xs text-destructive">
										{validationErrors.password}
									</p>
								)}
							</div>
						</div>
					</CardContent>
				</Card>

				<Card>
					<CardHeader
						className="cursor-pointer select-none"
						onClick={() => setShowAdvanced(!showAdvanced)}
					>
						<div className="flex items-center justify-between">
							<div>
								<CardTitle>Advanced & Table Filters</CardTitle>
								<CardDescription>
									Configure replication parameters and filtering tags.
								</CardDescription>
							</div>
							<Button type="button" variant="ghost" size="sm">
								{showAdvanced ? "Hide" : "Show"}
							</Button>
						</div>
					</CardHeader>
					{showAdvanced && (
						<CardContent className="space-y-6 pt-4 border-t">
							<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
								<div className="space-y-2">
									<label htmlFor="source-slot" className="text-sm font-medium">
										Replication Slot Name
									</label>
									<Input
										id="source-slot"
										placeholder="e.g. cdc_replication_slot"
										value={slotName}
										onChange={(e) => setSlotName(e.target.value)}
									/>
								</div>
								<div className="space-y-2">
									<label htmlFor="source-pub" className="text-sm font-medium">
										Publication Name
									</label>
									<Input
										id="source-pub"
										placeholder="e.g. cdc_publication"
										value={publicationName}
										onChange={(e) => setPublicationName(e.target.value)}
									/>
								</div>
							</div>

							<div className="grid grid-cols-1 md:grid-cols-3 gap-4">
								<div className="space-y-2">
									<label
										htmlFor="source-batch-size"
										className="text-sm font-medium"
									>
										Batch Size
									</label>
									<Input
										id="source-batch-size"
										type="number"
										placeholder="e.g. 1000"
										value={batchSize}
										onChange={(e) => setBatchSize(e.target.value)}
									/>
								</div>
								<div className="space-y-2">
									<label
										htmlFor="source-batch-wait"
										className="text-sm font-medium"
									>
										Batch Wait
									</label>
									<Input
										id="source-batch-wait"
										placeholder="e.g. 5s"
										value={batchWait}
										onChange={(e) => setBatchWait(e.target.value)}
									/>
									{validationErrors.batchWait && (
										<p className="text-xs text-destructive">
											{validationErrors.batchWait}
										</p>
									)}
								</div>
								<div className="space-y-2">
									<label
										htmlFor="source-discovery-int"
										className="text-sm font-medium"
									>
										Discovery Interval
									</label>
									<Input
										id="source-discovery-int"
										placeholder="e.g. 30s"
										value={discoveryInterval}
										onChange={(e) => setDiscoveryInterval(e.target.value)}
									/>
									{validationErrors.discoveryInterval && (
										<p className="text-xs text-destructive">
											{validationErrors.discoveryInterval}
										</p>
									)}
								</div>
							</div>

							<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
								<div className="space-y-2">
									<label
										htmlFor="source-snapshot-chunk"
										className="text-sm font-medium"
									>
										Snapshot Chunk Size
									</label>
									<Input
										id="source-snapshot-chunk"
										type="number"
										placeholder="e.g. 1000"
										value={snapshotChunkSize}
										onChange={(e) => setSnapshotChunkSize(e.target.value)}
									/>
								</div>
								<div className="space-y-2">
									<label
										htmlFor="source-snapshot-int"
										className="text-sm font-medium"
									>
										Snapshot Interval
									</label>
									<Input
										id="source-snapshot-int"
										placeholder="e.g. 1s"
										value={snapshotInterval}
										onChange={(e) => setSnapshotInterval(e.target.value)}
									/>
									{validationErrors.snapshotInterval && (
										<p className="text-xs text-destructive">
											{validationErrors.snapshotInterval}
										</p>
									)}
								</div>
							</div>

							<div className="space-y-4">
								<div className="space-y-2">
									<label
										htmlFor="source-schemas-input"
										className="text-sm font-medium"
									>
										Whitelisted Schemas
									</label>
									<div className="flex flex-wrap gap-2 mb-2">
										{schemas.map((s, index) => (
											<Badge
												key={s}
												variant="secondary"
												className="flex items-center gap-1"
											>
												{s}
												<button
													type="button"
													onClick={() => handleRemoveSchema(index)}
													className="hover:text-destructive cursor-pointer"
												>
													<X className="h-3 w-3" />
												</button>
											</Badge>
										))}
									</div>
									<Input
										id="source-schemas-input"
										placeholder="Type a schema and press Enter or comma"
										value={schemaInput}
										onChange={(e) => setSchemaInput(e.target.value)}
										onKeyDown={handleAddSchema}
									/>
								</div>

								<div className="space-y-2">
									<label
										htmlFor="source-tables-input"
										className="text-sm font-medium"
									>
										Whitelisted Tables
									</label>
									<div className="flex flex-wrap gap-2 mb-2">
										{tables.map((t, index) => (
											<Badge
												key={t}
												variant="secondary"
												className="flex items-center gap-1"
											>
												{t}
												<button
													type="button"
													onClick={() => handleRemoveTable(index)}
													className="hover:text-destructive cursor-pointer"
												>
													<X className="h-3 w-3" />
												</button>
											</Badge>
										))}
									</div>
									<Input
										id="source-tables-input"
										placeholder="Type a table and press Enter or comma"
										value={tableInput}
										onChange={(e) => setTableInput(e.target.value)}
										onKeyDown={handleAddTable}
									/>
								</div>
							</div>
						</CardContent>
					)}
				</Card>

				<div className="flex justify-end gap-3">
					<Button asChild variant="outline">
						<Link to="/sources">Cancel</Link>
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
								Create Source
							</>
						)}
					</Button>
				</div>
			</form>
		</div>
	);
}
