import type { ReconciliationStatus } from "@/api/pipelines";
import { Badge } from "@/components/ui/badge";

/**
 * Renders the best-effort delete-reconciliation sub-status (plan section
 * 4.2/4.4 invariant 5). "" (no record, or never went through a stop window)
 * renders nothing -- there is nothing to report. "stale" MUST render:
 * hiding it recreates the "reports healthy while diverging" failure this
 * plan exists to prevent, so this component has no way to suppress it.
 */
export function ReconciliationBadge({
	status,
	className,
}: {
	status: ReconciliationStatus | undefined;
	className?: string;
}) {
	if (!status) return null;

	if (status === "stale") {
		return (
			<Badge
				variant="warning"
				className={className}
				data-testid="reconciliation-badge"
				title="The sink may still hold rows deleted at the source since the last stop window. A background sweep will catch up; this pipeline serves reads and writes normally in the meantime."
			>
				Deletes stale
			</Badge>
		);
	}

	if (status === "running") {
		return (
			<Badge
				variant="secondary"
				className={className}
				data-testid="reconciliation-badge"
			>
				Reconciling deletes…
			</Badge>
		);
	}

	// "idle" -- reconciliation has run and has nothing pending. Worth a quiet
	// badge rather than silence, so "idle" and "no record at all" stay
	// visually distinct.
	return (
		<Badge
			variant="outline"
			className={className}
			data-testid="reconciliation-badge"
		>
			Deletes reconciled
		</Badge>
	);
}
