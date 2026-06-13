import { Plus } from "lucide-react";
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
		name: "",
		type: "mask",
		operationTypes: [],
		options: {},
	};
}

export function ProcessorList({ value, onChange }: ProcessorListProps) {
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
				<Card
					// biome-ignore lint/suspicious/noArrayIndexKey: list is owned by parent; stable id is not available
					key={i}
				>
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
