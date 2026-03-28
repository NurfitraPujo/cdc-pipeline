import Editor from "@monaco-editor/react";
import { AlertCircle, RotateCcw, Save } from "lucide-react";
import { useCallback, useState } from "react";
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

	const validateJson = useCallback((jsonContent: string): boolean => {
		// Basic JSON validation - check for common errors
		const lines = jsonContent.split("\n");

		for (let i = 0; i < lines.length; i++) {
			const line = lines[i];
			const trimmedLine = line.trim();

			// Skip empty lines and comments
			if (!trimmedLine || trimmedLine.startsWith("#")) {
				continue;
			}

			// Check for tabs (JSON should use spaces)
			if (line.includes("\t")) {
				setError(
					`Line ${i + 1}: Tabs are not allowed. Use spaces for indentation.`,
				);
				return false;
			}

			// Check indentation consistency
			const leadingSpaces = line.length - line.trimStart().length;
			if (leadingSpaces % 2 !== 0 && leadingSpaces > 0) {
				setError(
					`Line ${i + 1}: Indentation should be a multiple of 2 spaces.`,
				);
				return false;
			}
		}

		// Try to check for basic structure
		try {
			// Check for unmatched braces/brackets (simple check)
			const openBraces = (jsonContent.match(/\{/g) || []).length;
			const closeBraces = (jsonContent.match(/\}/g) || []).length;
			const openBrackets = (jsonContent.match(/\[/g) || []).length;
			const closeBrackets = (jsonContent.match(/\]/g) || []).length;

			if (openBraces !== closeBraces) {
				setError("Unmatched curly braces in JSON content.");
				return false;
			}

			if (openBrackets !== closeBrackets) {
				setError("Unmatched square brackets in JSON content.");
				return false;
			}

			return true;
		} catch {
			setError("Invalid JSON format.");
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
