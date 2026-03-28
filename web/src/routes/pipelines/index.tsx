import { createFileRoute } from "@tanstack/react-router";
import { Plus } from "lucide-react";
import { PipelineTable } from "@/components/PipelineTable";
import { Button } from "@/components/ui/button";

export const Route = createFileRoute("/pipelines/")({
	component: PipelinesPage,
});

function PipelinesPage() {
	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			<div className="mb-8 flex items-center justify-between">
				<div>
					<h1 className="text-3xl font-bold tracking-tight">Pipelines</h1>
					<p className="mt-2 text-muted-foreground">
						Manage and monitor your CDC pipelines.
					</p>
				</div>
				<Button onClick={() => (window.location.href = "/pipelines/create")}>
					<Plus className="mr-2 h-4 w-4" />
					Create Pipeline
				</Button>
			</div>

			<PipelineTable />
		</div>
	);
}
