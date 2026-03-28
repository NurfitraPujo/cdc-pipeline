import { createFileRoute } from "@tanstack/react-router";
import { Database } from "lucide-react";

export const Route = createFileRoute("/sources/")({
	component: SourcesPage,
});

function SourcesPage() {
	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			<div className="mb-8">
				<div className="flex items-center gap-3">
					<Database className="h-8 w-8 text-[#56c6be]" />
					<h1 className="text-3xl font-bold tracking-tight">Sources</h1>
				</div>
				<p className="mt-2 text-muted-foreground">
					Manage your database sources for CDC pipelines.
				</p>
			</div>

			<div className="rounded-xl border bg-card p-8">
				<div className="flex flex-col items-center justify-center py-12 text-center">
					<div className="mb-4 rounded-full bg-muted p-4">
						<Database className="h-8 w-8 text-muted-foreground" />
					</div>
					<h3 className="text-lg font-semibold">No sources configured</h3>
					<p className="mt-2 max-w-sm text-sm text-muted-foreground">
						Sources represent your database connections (PostgreSQL, MySQL,
						MongoDB) that you want to capture change events from.
					</p>
					<p className="mt-4 text-sm text-muted-foreground">
						Source management coming soon.
					</p>
				</div>
			</div>
		</div>
	);
}
