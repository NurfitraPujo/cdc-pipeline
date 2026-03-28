import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { renderWithProviders } from "../utils";
import DashboardPage from "@/routes/dashboard";
import { mockStatsSummary } from "../mocks/data";
import { server } from "../mocks/server";
import { errorHandlers } from "../mocks/handlers";

describe("Dashboard Integration", () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	it("should render dashboard with metrics", async () => {
		renderWithProviders(<DashboardPage />);

		// Wait for page title
		expect(await screen.findByText("Dashboard")).toBeInTheDocument();

		// Check metric cards - they should appear once data loads
		await waitFor(() => {
			expect(screen.getByText("Total Pipelines")).toBeInTheDocument();
		});

		expect(screen.getByText("Rows Synchronized")).toBeInTheDocument();
		expect(screen.getByText("Healthy")).toBeInTheDocument();
		expect(screen.getByText("Average Lag")).toBeInTheDocument();
	});

	it("should handle API errors gracefully", async () => {
		server.use(errorHandlers.serverError);

		renderWithProviders(<DashboardPage />);

		// Should still render the dashboard title
		expect(await screen.findByText("Dashboard")).toBeInTheDocument();
	});
});
