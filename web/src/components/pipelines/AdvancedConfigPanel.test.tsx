import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

vi.mock("@monaco-editor/react", () => ({
	default: (props: { value?: string; onChange?: (v?: string) => void }) => (
		<textarea
			data-testid="monaco"
			value={props.value ?? ""}
			onChange={(e) => props.onChange?.(e.target.value)}
		/>
	),
}));

import { AdvancedConfigPanel } from "@/components/pipelines/AdvancedConfigPanel";
import { defaultAdvancedConfig } from "@/components/pipelines/advancedConfig";
import { renderWithProviders } from "@/test/utils";

describe("AdvancedConfigPanel", () => {
	it("renders all three accordion sections", () => {
		renderWithProviders(
			<AdvancedConfigPanel
				value={defaultAdvancedConfig()}
				onChange={() => undefined}
				errors={{}}
			/>,
		);
		expect(screen.getByText("Batch & Performance")).toBeInTheDocument();
		expect(screen.getByText("Retry & Error Handling")).toBeInTheDocument();
		expect(screen.getByText("Processors")).toBeInTheDocument();
	});

	it("entering invalid batch wait shows error", async () => {
		const user = userEvent.setup();
		renderWithProviders(
			<AdvancedConfigPanel
				value={defaultAdvancedConfig()}
				onChange={() => undefined}
				errors={{}}
				defaultOpen={["batch"]}
			/>,
		);
		const input = screen.getByLabelText("Batch Wait (duration)");
		await user.click(input);
		await user.type(input, "not-a-duration");
		await user.tab();
		await waitFor(() => {
			expect(
				screen.getByText("Invalid duration format (e.g. 5s, 100ms, 1m)"),
			).toBeInTheDocument();
		});
	});

	it("entering valid batch wait clears error", async () => {
		const user = userEvent.setup();
		renderWithProviders(
			<AdvancedConfigPanel
				value={defaultAdvancedConfig()}
				onChange={() => undefined}
				errors={{}}
				defaultOpen={["batch"]}
			/>,
		);
		const input = screen.getByLabelText(
			"Batch Wait (duration)",
		) as HTMLInputElement;
		fireEvent.change(input, { target: { value: "bad" } });
		fireEvent.blur(input);
		await waitFor(() => {
			expect(
				screen.getByText("Invalid duration format (e.g. 5s, 100ms, 1m)"),
			).toBeInTheDocument();
		});
		fireEvent.change(input, { target: { value: "5s" } });
		fireEvent.blur(input);
		await waitFor(() => {
			expect(
				screen.queryByText("Invalid duration format (e.g. 5s, 100ms, 1m)"),
			).not.toBeInTheDocument();
		});
		await user.tab();
	});

	it("toggling enableDlq updates retry state", async () => {
		const user = userEvent.setup();
		const onChange = vi.fn();
		renderWithProviders(
			<AdvancedConfigPanel
				value={defaultAdvancedConfig()}
				onChange={onChange}
				errors={{}}
				defaultOpen={["retry"]}
			/>,
		);
		const sw = screen.getByLabelText("Enable Dead Letter Queue");
		await user.click(sw);
		await waitFor(() => {
			const calls = onChange.mock.calls.map((c) => c[0]);
			expect(
				calls.some((c) => c.retry !== undefined && c.retry.enableDlq === true),
			).toBe(true);
		});
	});

	it("add processor appends a default mask processor", async () => {
		const user = userEvent.setup();
		const onChange = vi.fn();
		renderWithProviders(
			<AdvancedConfigPanel
				value={defaultAdvancedConfig()}
				onChange={onChange}
				errors={{}}
				defaultOpen={["processors"]}
			/>,
		);
		const addBtn = screen.getByRole("button", { name: /add processor/i });
		await user.click(addBtn);
		await waitFor(() => {
			const calls = onChange.mock.calls.map((c) => c[0]);
			expect(
				calls.some(
					(c) =>
						Array.isArray(c.processors) &&
						c.processors.length === 1 &&
						c.processors[0].type === "mask",
				),
			).toBe(true);
		});
	});

	it("remove processor deletes it", async () => {
		const user = userEvent.setup();
		const onChange = vi.fn();
		renderWithProviders(
			<AdvancedConfigPanel
				value={{
					processors: [
						{
							name: "p1",
							type: "mask",
							operationTypes: [],
							options: {},
						},
					],
				}}
				onChange={onChange}
				errors={{}}
				defaultOpen={["processors"]}
			/>,
		);
		const removeBtn = screen.getByRole("button", { name: /remove processor/i });
		await user.click(removeBtn);
		await waitFor(() => {
			const calls = onChange.mock.calls.map((c) => c[0]);
			expect(
				calls.some(
					(c) => Array.isArray(c.processors) && c.processors.length === 0,
				),
			).toBe(true);
		});
	});
});
