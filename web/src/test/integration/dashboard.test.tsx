import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { renderWithRouter } from "../utils";
import { server } from "../mocks/server";
import { errorHandlers } from "../mocks/handlers";

describe("Dashboard Integration", () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	it("should render dashboard with metrics when authenticated", async () => {
		renderWithRouter("/dashboard", { authenticated: true });

		// Wait for page title (specifically the h1 heading)
		expect(await screen.findByRole("heading", { name: "Dashboard", level: 1 })).toBeInTheDocument();

		// Check metric cards - they should appear once data loads
		await waitFor(() => {
			expect(screen.getByText("Total Pipelines")).toBeInTheDocument();
		});

		expect(screen.getByText("Rows Synchronized")).toBeInTheDocument();
		expect(screen.getByText("Healthy")).toBeInTheDocument();
		expect(screen.getByText("Average Lag")).toBeInTheDocument();
	});

	it("should redirect to login when not authenticated", async () => {
		renderWithRouter("/dashboard", { authenticated: false });

		// Should redirect to login - look for username/password labels
		await waitFor(() => {
			expect(screen.getByLabelText(/username/i)).toBeInTheDocument();
		});
		expect(screen.getByLabelText(/password/i)).toBeInTheDocument();
	});

	it("should handle API errors gracefully", async () => {
		server.use(errorHandlers.serverError);

		renderWithRouter("/dashboard", { authenticated: true });

		// Should still render the dashboard title even on error
		expect(await screen.findByRole("heading", { name: "Dashboard", level: 1 })).toBeInTheDocument();
	});
});
