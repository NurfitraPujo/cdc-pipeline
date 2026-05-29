import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { StatusBadge } from "@/components/StatusBadge";
import { renderWithProviders } from "../utils";

describe("StatusBadge Component", () => {
	it.each([
		["healthy", "Healthy"],
		["error", "Error"],
		["transitioning", "Transitioning"],
		["unknown", "Unknown"],
	])("should render %s status correctly", (status, expectedLabel) => {
		renderWithProviders(<StatusBadge status={status as any} />);

		expect(screen.getByText(expectedLabel)).toBeInTheDocument();
	});

	it("should render badge element", () => {
		renderWithProviders(<StatusBadge status="healthy" />);

		const badge = screen.getByText("Healthy");
		expect(badge).toBeInTheDocument();
		// Check it's a div (or span) element
		expect(badge.tagName).toMatch(/DIV|SPAN/);
	});

	it("should fallback to unknown for invalid status", () => {
		renderWithProviders(<StatusBadge status={"invalid" as any} />);

		expect(screen.getByText("Unknown")).toBeInTheDocument();
	});
});
