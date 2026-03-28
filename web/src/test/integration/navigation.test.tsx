import { describe, it, expect } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { renderWithRouter } from "../utils";

describe("Navigation Integration", () => {
	it("should render sidebar navigation", async () => {
		renderWithRouter("/dashboard", { authenticated: true });

		// Wait for dashboard page heading (h1) to be loaded first
		expect(await screen.findByRole("heading", { name: "Dashboard", level: 1 })).toBeInTheDocument();

		// Check sidebar navigation links are present
		expect(screen.getAllByText("Dashboard").length).toBeGreaterThanOrEqual(1);
		expect(screen.getByText("Pipelines")).toBeInTheDocument();
		expect(screen.getByText("Sources")).toBeInTheDocument();
		expect(screen.getByText("Sinks")).toBeInTheDocument();
		expect(screen.getByText("Configuration")).toBeInTheDocument();
	});

	it("should redirect to login when not authenticated", async () => {
		renderWithRouter("/dashboard", { authenticated: false });

		// Should redirect to login - look for username/password labels which are unique to login page
		await waitFor(() => {
			expect(screen.getByLabelText(/username/i)).toBeInTheDocument();
		});
		expect(screen.getByLabelText(/password/i)).toBeInTheDocument();
	});
});
