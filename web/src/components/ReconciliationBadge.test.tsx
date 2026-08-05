import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ReconciliationBadge } from "@/components/ReconciliationBadge";

describe("ReconciliationBadge", () => {
	it("renders nothing for an empty status", () => {
		const { container } = render(<ReconciliationBadge status="" />);
		expect(container).toBeEmptyDOMElement();
	});

	it("renders nothing for undefined (no lifecycle record yet)", () => {
		const { container } = render(<ReconciliationBadge status={undefined} />);
		expect(container).toBeEmptyDOMElement();
	});

	// The plan's invariant 5 is explicit: "stale" must stay visible. This is
	// the one case this component is not allowed to ever suppress.
	it("renders 'stale' -- the one status that must never be hidden", () => {
		render(<ReconciliationBadge status="stale" />);
		const badge = screen.getByTestId("reconciliation-badge");
		expect(badge).toHaveTextContent("Deletes stale");
		expect(badge).toBeVisible();
	});

	it("renders 'running' distinctly from 'stale'", () => {
		render(<ReconciliationBadge status="running" />);
		expect(screen.getByTestId("reconciliation-badge")).toHaveTextContent(
			"Reconciling deletes",
		);
	});

	it("renders 'idle' distinctly from empty/no-record", () => {
		render(<ReconciliationBadge status="idle" />);
		expect(screen.getByTestId("reconciliation-badge")).toHaveTextContent(
			"Deletes reconciled",
		);
	});
});
