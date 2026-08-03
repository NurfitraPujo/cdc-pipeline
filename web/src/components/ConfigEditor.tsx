import Editor from "@monaco-editor/react";
import { AlertCircle, RotateCcw, Save } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardFooter,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";

interface ConfigEditorProps {
	initialValue: string;
	onSave: (value: string) => void;
	isLoading?: boolean;
}

export function ConfigEditor({
	initialValue,
	onSave,
	isLoading = false,
}: ConfigEditorProps) {
	const [value, setValue] = useState(initialValue);
	const [error, setError] = useState<string | null>(null);

	// T2-9: `useState(initialValue)` only seeds the editor once on mount. When
	// the parent re-renders with a fresh server value (e.g. after an unrelated
	// query refetch, or navigating between pipelines) the editor keeps showing
	// the stale text. Mirror `initialValue` into local state so external
	// updates are reflected without dropping in-flight edits the user has not
	// saved — `hasChanges` already gates the Reset/Save buttons.
	useEffect(() => {
		setValue(initialValue);
		setError(null);
	}, [initialValue]);

	const handleChange = useCallback((newValue: string | undefined) => {
		if (newValue !== undefined) {
			setValue(newValue);
			setError(null);
		}
	}, []);

	const handleReset = useCallback(() => {
		setValue(initialValue);
		setError(null);
	}, [initialValue]);

	/**
	 * Validate by actually parsing.
	 *
	 * This previously counted braces and policed indentation but never called
	 * JSON.parse, so it rejected valid JSON that used tabs while happily
	 * forwarding syntactically invalid JSON (balanced braces are not grammar)
	 * to the parent, which then failed on its own parse.
	 */
	const validateJson = useCallback((jsonContent: string): boolean => {
		// The editor's own header comment lines are stripped before parsing by
		// callers that use them; tolerate a leading `//`-comment block here so
		// the pipeline editor's template still validates.
		const stripped = jsonContent
			.split("\n")
			.filter((line) => !line.trim().startsWith("//"))
			.join("\n");

		if (!stripped.trim()) {
			setError("Configuration cannot be empty.");
			return false;
		}

		try {
			const parsed: unknown = JSON.parse(stripped);
			if (parsed === null || typeof parsed !== "object") {
				setError("Configuration must be a JSON object.");
				return false;
			}
			setError(null);
			return true;
		} catch (err) {
			setError(
				err instanceof Error ? `Invalid JSON: ${err.message}` : "Invalid JSON.",
			);
			return false;
		}
	}, []);

	const handleSave = useCallback(() => {
		if (validateJson(value)) {
			onSave(value);
		}
	}, [value, onSave, validateJson]);

	const hasChanges = value !== initialValue;

	return (
		<Card className="w-full">
			<CardHeader>
				<CardTitle>Configuration Editor</CardTitle>
			</CardHeader>
			<CardContent className="space-y-4">
				<div
					className="border rounded-md overflow-hidden"
					style={{ height: "500px" }}
				>
					<Editor
						height="100%"
						defaultLanguage="json"
						value={value}
						onChange={handleChange}
						options={{
							minimap: { enabled: false },
							scrollBeyondLastLine: false,
							fontSize: 14,
							lineNumbers: "on",
							roundedSelection: false,
							padding: { top: 16, bottom: 16 },
							automaticLayout: true,
							formatOnPaste: true,
							formatOnType: true,
						}}
						loading={
							<div className="flex items-center justify-center h-full">
								<div className="animate-pulse text-muted-foreground">
									Loading editor...
								</div>
							</div>
						}
					/>
				</div>
				{error && (
					<div className="flex items-center gap-2 text-destructive text-sm">
						<AlertCircle className="h-4 w-4" />
						<span>{error}</span>
					</div>
				)}
			</CardContent>
			<CardFooter className="flex justify-end gap-2">
				<Button
					variant="outline"
					onClick={handleReset}
					disabled={!hasChanges || isLoading}
				>
					<RotateCcw className="mr-2 h-4 w-4" />
					Reset
				</Button>
				<Button onClick={handleSave} disabled={!hasChanges || isLoading}>
					{isLoading ? (
						<>
							<div className="mr-2 h-4 w-4 animate-spin rounded-full border-2 border-current border-t-transparent" />
							Saving...
						</>
					) : (
						<>
							<Save className="mr-2 h-4 w-4" />
							Save
						</>
					)}
				</Button>
			</CardFooter>
		</Card>
	);
}

export default ConfigEditor;
