import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderWithRouter } from "../utils";
import { mockPipelines } from "../mocks/data";

describe("Pipelines Integration", () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	describe("Pipeline List", () => {
		it("should render pipeline list page", async () => {
			renderWithRouter("/pipelines");

			expect(await screen.findByText("Pipelines")).toBeInTheDocument();
			expect(screen.getByText("Manage your data replication pipelines")).toBeInTheDocument();
		});

		it("should display pipelines in table", async () => {
			renderWithRouter("/pipelines");

			await waitFor(() => {
				expect(screen.getByText(mockPipelines[0].id)).toBeInTheDocument();
			});

			expect(screen.getByText(mockPipelines[1].id)).toBeInTheDocument();
		});
	});

	describe("Pipeline Create", () => {
		it("should render create pipeline form", async () => {
			renderWithRouter("/pipelines/create");

			expect(await screen.findByText(/create pipeline/i)).toBeInTheDocument();
		});
	});

	describe("Pipeline Detail", () => {
		it("should render pipeline detail page", async () => {
			renderWithRouter(`/pipelines/${mockPipelines[0].id}`);

			await waitFor(() => {
				expect(screen.getByText(mockPipelines[0].id)).toBeInTheDocument();
			});

			expect(screen.getByText("Configuration")).toBeInTheDocument();
		});
	});
});
