import { createFileRoute } from "@tanstack/react-router";
import { HardDrive } from "lucide-react";

export const Route = createFileRoute("/sinks/")({
	component: SinksPage,
});

function SinksPage() {
	return (
		<div className="page-wrap px-4 pb-8 pt-14">
			<div className="mb-8">
				<div className="flex items-center gap-3">
					<HardDrive className="h-8 w-8 text-[#56c6be]" />
					<h1 className="text-3xl font-bold tracking-tight">Sinks</h1>
				</div>
				<p className="mt-2 text-muted-foreground">
					Manage your destination sinks for CDC pipeline data.
				</p>
			</div>

			<div className="rounded-xl border bg-card p-8">
				<div className="flex flex-col items-center justify-center py-12 text-center">
					<div className="mb-4 rounded-full bg-muted p-4">
						<HardDrive className="h-8 w-8 text-muted-foreground" />
					</div>
					<h3 className="text-lg font-semibold">No sinks configured</h3>
					<p className="mt-2 max-w-sm text-sm text-muted-foreground">
						Sinks represent your destination systems (PostgreSQL, MySQL, Elasticsearch,
						Kafka, Webhook) where change events will be delivered to.
					</p>
					<p className="mt-4 text-sm text-muted-foreground">
						Sink management coming soon.
					</p>
				</div>
			</div>
		</div>
	);
}
