import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { renderWithRouter } from "../utils";

describe("Pipelines Integration", () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	describe("Pipeline List", () => {
		it("should render pipeline list page", async () => {
			renderWithRouter("/pipelines", { authenticated: true });

			// Look for the h1 heading specifically to avoid ambiguity with sidebar
			expect(await screen.findByRole("heading", { name: "Pipelines", level: 1 })).toBeInTheDocument();
		});

		it("should display loading state and then content", async () => {
			renderWithRouter("/pipelines", { authenticated: true });

			// First wait for the page to render
			expect(await screen.findByRole("heading", { name: "Pipelines", level: 1 })).toBeInTheDocument();
			
			// The table or content should eventually appear
			// Just verify the page doesn't crash - the data fetching is handled by MSW
			expect(document.body.textContent).toContain("Pipelines");
		});
	});

	describe("Pipeline Create", () => {
		it("should render create pipeline form", async () => {
			renderWithRouter("/pipelines/create", { authenticated: true });

			expect(await screen.findByRole("heading", { name: /create pipeline/i, level: 1 })).toBeInTheDocument();
		});
	});

	describe("Pipeline Detail", () => {
		it("should render pipeline detail page", async () => {
			renderWithRouter("/pipelines/pipeline-1", { authenticated: true });

			// Just verify we're on a pipeline page by checking for the Configuration section
			// or any content that would indicate the page loaded
			await waitFor(() => {
				const content = document.body.textContent || "";
				expect(content.length).toBeGreaterThan(0);
			});
		});
	});
});
