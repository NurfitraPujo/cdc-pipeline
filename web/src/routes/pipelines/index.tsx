import { createFileRoute, Link } from "@tanstack/react-router";
import { Plus, Search, X } from "lucide-react";
import { useEffect, useState } from "react";
import { PIPELINE_STATUS, PIPELINE_STATUS_LABELS } from "@/api/enums";
import { PipelineTable } from "@/components/PipelineTable";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
	Select,
	SelectContent,
	SelectItem,
	SelectTrigger,
	SelectValue,
} from "@/components/ui/select";

export const Route = createFileRoute("/pipelines/")({
	component: PipelinesPage,
});

const ALL_STATUSES = "__all__";

function PipelinesPage() {
	const [searchInput, setSearchInput] = useState("");
	const [search, setSearch] = useState("");
	const [status, setStatus] = useState<string>(ALL_STATUSES);

	// Debounce so typing does not fire a request per keystroke. The table
	// refetches whenever `search` changes.
	useEffect(() => {
		const timer = setTimeout(() => setSearch(searchInput.trim()), 300);
		return () => clearTimeout(timer);
	}, [searchInput]);

	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			<div className="mb-8 flex items-center justify-between">
				<div>
					<h1 className="text-3xl font-bold tracking-tight">Pipelines</h1>
					<p className="mt-2 text-muted-foreground">
						Manage and monitor your CDC pipelines.
					</p>
				</div>
				<Button asChild>
					<Link to="/pipelines/create">
						<Plus className="mr-2 h-4 w-4" />
						Create Pipeline
					</Link>
				</Button>
			</div>

			<div className="mb-4 flex flex-wrap items-center gap-3">
				<div className="relative flex-1 min-w-[16rem]">
					<Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
					<Input
						aria-label="Search pipelines"
						placeholder="Search by name or ID..."
						value={searchInput}
						onChange={(e) => setSearchInput(e.target.value)}
						className="pl-9"
					/>
				</div>

				<Select value={status} onValueChange={setStatus}>
					<SelectTrigger className="w-[12rem]" aria-label="Filter by status">
						<SelectValue placeholder="All statuses" />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value={ALL_STATUSES}>All statuses</SelectItem>
						{PIPELINE_STATUS.map((s) => (
							<SelectItem key={s} value={s}>
								{PIPELINE_STATUS_LABELS[s]}
							</SelectItem>
						))}
					</SelectContent>
				</Select>

				{(search || status !== ALL_STATUSES) && (
					<Button
						variant="ghost"
						size="sm"
						onClick={() => {
							setSearchInput("");
							setSearch("");
							setStatus(ALL_STATUSES);
						}}
					>
						<X className="mr-1 h-4 w-4" />
						Clear
					</Button>
				)}
			</div>

			<PipelineTable
				search={search || undefined}
				status={status === ALL_STATUSES ? undefined : status}
			/>
		</div>
	);
}
