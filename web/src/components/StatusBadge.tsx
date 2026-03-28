import { Badge } from "@/components/ui/badge";

export type StatusBadgeStatus =
	| "healthy"
	| "error"
	| "transitioning"
	| "unknown";

interface StatusBadgeProps {
	status: StatusBadgeStatus;
	className?: string;
}

const statusVariantMap: Record<
	StatusBadgeStatus,
	"success" | "destructive" | "warning" | "secondary"
> = {
	healthy: "success",
	error: "destructive",
	transitioning: "warning",
	unknown: "secondary",
};

const statusLabelMap: Record<StatusBadgeStatus, string> = {
	healthy: "Healthy",
	error: "Error",
	transitioning: "Transitioning",
	unknown: "Unknown",
};

export function StatusBadge({ status, className }: StatusBadgeProps) {
	const variant = statusVariantMap[status] ?? "secondary";
	const label = statusLabelMap[status] ?? "Unknown";

	return (
		<Badge variant={variant} className={className}>
			{label}
		</Badge>
	);
}
