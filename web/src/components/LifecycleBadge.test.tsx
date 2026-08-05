import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { LifecycleBadge } from "@/components/LifecycleBadge";

describe("LifecycleBadge", () => {
	it.each([
		["Running", "Running"],
		["Paused", "Paused"],
		["Stopping", "Stopping…"],
		["NeedsResnapshot", "Needs Re-snapshot"],
		["Failed", "Failed"],
	] as const)("renders %s as %s", (state, label) => {
		render(<LifecycleBadge state={state} />);
		expect(screen.getByTestId("lifecycle-badge")).toHaveTextContent(label);
	});

	it("is a distinct element from a health badge -- both can render side by side", () => {
		render(
			<div>
				<LifecycleBadge state="Paused" />
				<span data-testid="health-badge">Healthy</span>
			</div>,
		);
		expect(screen.getByTestId("lifecycle-badge")).toHaveTextContent("Paused");
		expect(screen.getByTestId("health-badge")).toHaveTextContent("Healthy");
	});
});
