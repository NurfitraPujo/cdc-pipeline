import { Plus } from "lucide-react";
import { useEffect, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import type { ProcessorConfigShape } from "./optionsTemplates";
import { ProcessorEditor } from "./ProcessorEditor";

type ProcessorConfig = ProcessorConfigShape;

interface ProcessorListProps {
	value: ProcessorConfig[];
	onChange: (next: ProcessorConfig[]) => void;
}

function makeDefault(): ProcessorConfig {
	return {
		// T3-4: a freshly-minted UUID per row so React keys stay stable across
		// reorders and `onChange` callbacks that replace the entire array.
		id: crypto.randomUUID(),
		name: "",
		type: "mask",
		operationTypes: [],
		options: {},
	};
}

export function ProcessorList({ value, onChange }: ProcessorListProps) {
	// T3-4: rows arriving from the API (or produced by older code paths) do
	// not carry an `id`. Backfill them once on mount and whenever the array
	// length grows so the `key={p.id}` below never falls back to an index.
	const backfilledRef = useRef(false);
	useEffect(() => {
		const needsBackfill = value.some((p) => typeof p.id !== "string");
		if (!needsBackfill || backfilledRef.current) return;
		backfilledRef.current = true;
		const next = value.map((p) =>
			typeof p.id === "string" ? p : { ...p, id: crypto.randomUUID() },
		);
		onChange(next);
	}, [value, onChange]);

	const update = (i: number, p: ProcessorConfig) => {
		const next = value.slice();
		next[i] = p;
		onChange(next);
	};

	const remove = (i: number) => {
		const next = value.slice();
		next.splice(i, 1);
		onChange(next);
	};

	const add = () => {
		onChange([...value, makeDefault()]);
	};

	return (
		<div className="space-y-3">
			<div>
				<Button type="button" size="sm" variant="outline" onClick={add}>
					<Plus className="h-4 w-4 mr-1" /> Add processor
				</Button>
			</div>
			{value.length === 0 && (
				<div className="text-sm text-muted-foreground">
					No processors configured.
				</div>
			)}
			{value.map((p, i) => (
				<Card key={p.id ?? `__missing_${i}`}>
					<CardContent className="pt-6">
						<ProcessorEditor
							value={p}
							onChange={(next) => update(i, next)}
							onRemove={() => remove(i)}
							index={i}
						/>
					</CardContent>
				</Card>
			))}
		</div>
	);
}

export default ProcessorList;
