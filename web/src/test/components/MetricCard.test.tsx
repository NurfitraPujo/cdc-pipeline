import { describe, it, expect } from "vitest";
import { screen } from "@testing-library/react";
import { renderWithProviders } from "../utils";
import { MetricCard } from "@/components/MetricCard";
import { Activity } from "lucide-react";

describe("MetricCard Component", () => {
	it("should render with title and value", () => {
		renderWithProviders(
			<MetricCard
				title="Total Pipelines"
				value={5}
				icon={Activity}
			/>
		);

		expect(screen.getByText("Total Pipelines")).toBeInTheDocument();
		expect(screen.getByText("5")).toBeInTheDocument();
	});

	it("should show loading state", () => {
		renderWithProviders(
			<MetricCard
				title="Total Pipelines"
				value={0}
				icon={Activity}
				isLoading={true}
			/>
		);

		// Check for skeleton element
		const skeleton = document.querySelector("[class*=*skeleton*]");
		expect(skeleton).toBeInTheDocument();
	});

	it("should show description when provided", () => {
		renderWithProviders(
			<MetricCard
				title="Total Pipelines"
				value={5}
				icon={Activity}
				description="Active pipeline configurations"
			/>
		);

		expect(screen.getByText("Active pipeline configurations")).toBeInTheDocument();
	});

	it("should show trend indicator", () => {
		renderWithProviders(
			<MetricCard
				title="Total Pipelines"
				value={5}
				icon={Activity}
				trend={{ value: 10, isPositive: true }}
			/>
		);

		expect(screen.getByText(/↑ 10%/)).toBeInTheDocument();
	});

	it("should format large numbers", () => {
		const { rerender } = renderWithProviders(
			<MetricCard
				title="Rows"
				value={1500000}
				icon={Activity}
			/>
		);

		expect(screen.getByText("1.5M")).toBeInTheDocument();

		rerender(
			<MetricCard
				title="Rows"
				value={2500}
				icon={Activity}
			/>
		);

		expect(screen.getByText("2.5K")).toBeInTheDocument();
	});
});
