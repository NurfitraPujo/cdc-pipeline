import type { LucideIcon } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

interface MetricCardProps {
	title: string;
	value: string | number;
	description?: string;
	icon: LucideIcon;
	isLoading?: boolean;
	trend?: {
		value: number;
		label: string;
		isPositive: boolean;
	};
}

function formatNumber(num: number): string {
	if (num >= 1000000) {
		return `${(num / 1000000).toFixed(1)}M`;
	}
	if (num >= 1000) {
		return `${(num / 1000).toFixed(1)}K`;
	}
	return num.toString();
}

export function MetricCard({
	title,
	value,
	description,
	icon: Icon,
	isLoading,
	trend,
}: MetricCardProps) {
	const formattedValue =
		typeof value === "number" ? formatNumber(value) : value;

	return (
		<Card>
			<CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
				<CardTitle className="text-sm font-medium text-muted-foreground">
					{title}
				</CardTitle>
				<Icon className="h-4 w-4 text-muted-foreground" />
			</CardHeader>
			<CardContent>
				{isLoading ? (
					<Skeleton className="h-8 w-24" />
				) : (
					<div className="text-2xl font-bold">{formattedValue}</div>
				)}
				{description && !isLoading && (
					<p className="text-xs text-muted-foreground mt-1">{description}</p>
				)}
				{trend && !isLoading && (
					<div className="flex items-center gap-1 mt-2">
						<span
							className={`text-xs font-medium ${
								trend.isPositive ? "text-green-600" : "text-red-600"
							}`}
						>
							{trend.isPositive ? "+" : ""}
							{trend.value}%
						</span>
						<span className="text-xs text-muted-foreground">{trend.label}</span>
					</div>
				)}
			</CardContent>
		</Card>
	);
}
