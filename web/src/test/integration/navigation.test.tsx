import { describe, it, expect, vi } from "vitest";
import { screen } from "@testing-library/react";
import { renderWithRouter } from "../utils";

describe("Navigation Integration", () => {
	it("should render sidebar navigation", async () => {
		renderWithRouter("/dashboard");

		// Check sidebar items
		expect(await screen.findByText("Dashboard")).toBeInTheDocument();
		expect(screen.getByText("Pipelines")).toBeInTheDocument();
		expect(screen.getByText("Sources")).toBeInTheDocument();
		expect(screen.getByText("Sinks")).toBeInTheDocument();
		expect(screen.getByText("Configuration")).toBeInTheDocument();
	});
});
