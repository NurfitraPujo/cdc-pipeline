import Editor from "@monaco-editor/react";
import { Loader2, Trash2 } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import {
	OPERATION_TYPE,
	OPERATION_TYPE_GROUPS,
	OPERATION_TYPE_LABELS,
	PROCESSOR_TYPE,
	PROCESSOR_TYPE_LABELS,
} from "@/api/enums";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
	Select,
	SelectContent,
	SelectItem,
	SelectTrigger,
	SelectValue,
} from "@/components/ui/select";
import { deepMergeOptions } from "@/lib/processorMerge";
import { cn } from "@/lib/utils";
import type { ProcessorConfigShape } from "./optionsTemplates";
import { TEMPLATES_BY_TYPE } from "./optionsTemplates";

type ProcessorConfig = ProcessorConfigShape;

interface ProcessorEditorProps {
	value: ProcessorConfig;
	onChange: (next: ProcessorConfig) => void;
	onRemove: () => void;
	index: number;
}

const OPERATION_ORDER = OPERATION_TYPE;

function orderOps(ops: string[]): string[] {
	return [...ops].sort(
		(a, b) =>
			OPERATION_ORDER.indexOf(a as (typeof OPERATION_ORDER)[number]) -
			OPERATION_ORDER.indexOf(b as (typeof OPERATION_ORDER)[number]),
	);
}

export function ProcessorEditor({
	value,
	onChange,
	onRemove,
	index,
}: ProcessorEditorProps) {
	const [optionsText, setOptionsText] = useState<string>(() =>
		JSON.stringify(value.options ?? {}, null, 2),
	);
	const [optionsError, setOptionsError] = useState<string | null>(null);
	const [pendingTemplate, setPendingTemplate] = useState<
		ProcessorConfig["options"] | null
	>(null);

	const valueOptionsKey = JSON.stringify(value.options ?? {});

	useEffect(() => {
		const current = valueOptionsKey;
		if (optionsText === current) return;
		try {
			const parsed = JSON.parse(optionsText);
			if (JSON.stringify(parsed ?? {}) === current) return;
		} catch {
			// invalid: fall through to resync
		}
		setOptionsText(current);
		setOptionsError(null);
	}, [valueOptionsKey, optionsText]);

	const selectedOps = useMemo(
		() => new Set<string>(value.operationTypes ?? []),
		[value.operationTypes],
	);

	const templates = TEMPLATES_BY_TYPE[value.type] ?? [];

	const updateName = (name: string) => {
		onChange({ ...value, name });
	};

	const updateType = (type: string) => {
		onChange({ ...value, type });
	};

	const toggleOp = (op: string, checked: boolean) => {
		const next = new Set(selectedOps);
		if (checked) next.add(op);
		else next.delete(op);
		const arr = orderOps(Array.from(next));
		onChange({ ...value, operationTypes: arr });
	};

	const handleOptionsTextChange = (next: string | undefined) => {
		const text = next ?? "";
		setOptionsText(text);
		try {
			const parsed = text.trim() === "" ? {} : JSON.parse(text);
			setOptionsError(null);
			onChange({ ...value, options: parsed });
		} catch (e) {
			setOptionsError(e instanceof Error ? e.message : "Invalid JSON");
		}
	};

	const applyTemplate = (tplOptions: ProcessorConfig["options"]) => {
		const merged = deepMergeOptions(
			value.options ?? {},
			tplOptions ?? {},
		) as ProcessorConfig["options"];
		onChange({ ...value, options: merged });
		setOptionsText(JSON.stringify(merged ?? {}, null, 2));
		setOptionsError(null);
	};

	const loadTemplate = (tplOptions: ProcessorConfig["options"]) => {
		if (optionsError) {
			setPendingTemplate(tplOptions ?? null);
			return;
		}
		applyTemplate(tplOptions);
	};

	const confirmTemplateOverwrite = () => {
		if (pendingTemplate === null) return;
		const next = (pendingTemplate ?? {}) as ProcessorConfig["options"];
		onChange({ ...value, options: next });
		setOptionsText(JSON.stringify(next, null, 2));
		setOptionsError(null);
		setPendingTemplate(null);
	};

	const resetOptions = () => {
		onChange({ ...value, options: {} });
		setOptionsText("{}");
		setOptionsError(null);
	};

	return (
		<div className="space-y-4">
			<div className="flex flex-wrap items-center gap-2">
				<div className="flex-1 min-w-[180px]">
					<Label htmlFor={`processor-name-${index}`} className="sr-only">
						Processor name
					</Label>
					<Input
						id={`processor-name-${index}`}
						value={value.name}
						placeholder="Processor name"
						onChange={(e) => updateName(e.target.value)}
					/>
				</div>
				<div className="w-[180px]">
					<Label htmlFor={`processor-type-${index}`} className="sr-only">
						Type
					</Label>
					<Select value={value.type} onValueChange={updateType}>
						<SelectTrigger id={`processor-type-${index}`}>
							<SelectValue placeholder="Type" />
						</SelectTrigger>
						<SelectContent>
							{PROCESSOR_TYPE.map((t) => (
								<SelectItem key={t} value={t}>
									{PROCESSOR_TYPE_LABELS[t] ?? t}
								</SelectItem>
							))}
						</SelectContent>
					</Select>
				</div>
				<Button
					type="button"
					variant="outline"
					size="icon"
					onClick={onRemove}
					aria-label="Remove processor"
				>
					<Trash2 className="h-4 w-4" />
				</Button>
			</div>

			<div className="space-y-2">
				<div className="text-sm font-medium">Data operations</div>
				<div className="flex flex-wrap gap-4">
					{OPERATION_TYPE_GROUPS.data.map((op) => {
						const checked = selectedOps.has(op);
						const id = `${index}-op-${op}`;
						return (
							<label
								key={op}
								htmlFor={id}
								className="flex items-center gap-2 text-sm cursor-pointer"
							>
								<Checkbox
									id={id}
									checked={checked}
									onCheckedChange={(c) => toggleOp(op, c === true)}
								/>
								<span>{OPERATION_TYPE_LABELS[op]}</span>
							</label>
						);
					})}
				</div>
			</div>

			<div className="space-y-2">
				<div className="text-sm font-medium">Schema & lifecycle</div>
				<div className="flex flex-wrap gap-4">
					{OPERATION_TYPE_GROUPS.schema.map((op) => {
						const checked = selectedOps.has(op);
						const id = `${index}-op-${op}`;
						return (
							<label
								key={op}
								htmlFor={id}
								className="flex items-center gap-2 text-sm cursor-pointer"
							>
								<Checkbox
									id={id}
									checked={checked}
									onCheckedChange={(c) => toggleOp(op, c === true)}
								/>
								<span>{OPERATION_TYPE_LABELS[op]}</span>
							</label>
						);
					})}
				</div>
			</div>

			<div className="space-y-2">
				<div className="text-sm font-medium">Options</div>
				<div className="grid grid-cols-1 md:grid-cols-5 gap-3">
					<div className="md:col-span-3">
						<div
							className="border rounded-md overflow-hidden"
							style={{ height: "200px" }}
						>
							<Editor
								height="200px"
								defaultLanguage="json"
								value={optionsText}
								onChange={handleOptionsTextChange}
								options={{
									minimap: { enabled: false },
									scrollBeyondLastLine: false,
									fontSize: 13,
									automaticLayout: true,
									wordWrap: "on",
								}}
								loading={
									<div className="flex items-center justify-center h-full">
										<Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
									</div>
								}
							/>
						</div>
						{optionsError && (
							<div className="text-xs text-destructive mt-1">
								{optionsError}
							</div>
						)}
					</div>
					<div className="md:col-span-2 space-y-2">
						{templates.length === 0 && (
							<div className="text-xs text-muted-foreground">
								No templates available
							</div>
						)}
						{templates.map((tpl) => (
							<div
								key={`${tpl.type}-${tpl.name}`}
								className={cn(
									"border rounded-md p-2 space-y-2 bg-card text-card-foreground",
								)}
							>
								<div className="text-xs font-medium">Default</div>
								<pre className="text-[11px] bg-muted text-foreground rounded p-2 overflow-auto max-h-32 whitespace-pre-wrap break-all">
									{JSON.stringify(tpl.options ?? {}, null, 2)}
								</pre>
								<Button
									type="button"
									size="sm"
									variant="outline"
									className="w-full"
									onClick={() => loadTemplate(tpl.options ?? undefined)}
								>
									Load Template
								</Button>
							</div>
						))}
						<Button
							type="button"
							size="sm"
							variant="ghost"
							className="w-full"
							onClick={resetOptions}
						>
							Reset to empty
						</Button>
					</div>
				</div>
			</div>

			<Dialog
				open={pendingTemplate !== null}
				onOpenChange={(o) => {
					if (!o) setPendingTemplate(null);
				}}
			>
				<DialogContent>
					<DialogHeader>
						<DialogTitle>Overwrite invalid JSON?</DialogTitle>
						<DialogDescription>
							Template will overwrite invalid JSON. Continue?
						</DialogDescription>
					</DialogHeader>
					<DialogFooter>
						<Button variant="outline" onClick={() => setPendingTemplate(null)}>
							Cancel
						</Button>
						<Button onClick={confirmTemplateOverwrite}>Continue</Button>
					</DialogFooter>
				</DialogContent>
			</Dialog>
		</div>
	);
}

export default ProcessorEditor;
