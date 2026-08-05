import type { LifecycleState } from "@/api/pipelines";
import { Badge, type BadgeProps } from "@/components/ui/badge";

/**
 * Renders `PipelineListItem.lifecycle_state` -- "what is this pipeline
 * actually doing right now" (plan section 4.1) -- as its own badge, distinct
 * from `StatusBadge`'s health ("is it doing it well"). A `Paused` pipeline is
 * neither healthy nor unhealthy; collapsing the two into one badge is exactly
 * the ambiguity the plan's three-concept split (desired state / lifecycle
 * state / health) exists to remove.
 */

const VARIANT_MAP: Record<LifecycleState, BadgeProps["variant"]> = {
	Running: "success",
	Pausing: "warning",
	Paused: "secondary",
	Stopping: "warning",
	Stopped: "secondary",
	NeedsResnapshot: "warning",
	Snapshotting: "warning",
	Resuming: "warning",
	Failed: "destructive",
	Transitioning: "warning",
};

const LABEL_MAP: Record<LifecycleState, string> = {
	Running: "Running",
	Pausing: "Pausing…",
	Paused: "Paused",
	Stopping: "Stopping…",
	Stopped: "Stopped",
	NeedsResnapshot: "Needs Re-snapshot",
	Snapshotting: "Snapshotting…",
	Resuming: "Resuming…",
	Failed: "Failed",
	Transitioning: "Transitioning",
};

interface LifecycleBadgeProps {
	state: LifecycleState;
	className?: string;
}

export function LifecycleBadge({ state, className }: LifecycleBadgeProps) {
	const variant = VARIANT_MAP[state] ?? "secondary";
	const label = LABEL_MAP[state] ?? state;

	return (
		<Badge
			variant={variant}
			className={className}
			data-testid="lifecycle-badge"
		>
			{label}
		</Badge>
	);
}
