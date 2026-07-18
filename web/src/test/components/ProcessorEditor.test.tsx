import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

// Radix UI primitives that ProcessorEditor pulls in are stubbed via the
// `resolve.alias` block in `vitest.config.ts`. We only mock Monaco here.
vi.mock("@monaco-editor/react", () => ({
	default: (props: {
		value?: string;
		onChange?: (v?: string) => void;
	}) => (
		<textarea
			data-testid="monaco"
			value={props.value ?? ""}
			onChange={(e) => props.onChange?.(e.target.value)}
		/>
	),
}));

import { ProcessorEditor } from "@/components/pipelines/ProcessorEditor";

afterEach(() => {
	vi.restoreAllMocks();
});

describe("ProcessorEditor (T2-10)", () => {
	it("does not reset editor text while the user is mid-typing invalid JSON", () => {
		const onChange = vi.fn();
		render(
			<ProcessorEditor
				value={{
					name: "",
					type: "mask",
					operationTypes: [],
					options: { fields: ["email"] },
				}}
				onChange={onChange}
				onRemove={() => undefined}
				index={0}
			/>,
		);

		const textarea = screen.getByTestId("monaco") as HTMLTextAreaElement;
		// Sanity: editor seeded with the initial formatted options.
		expect(textarea.value).toBe(
			JSON.stringify({ fields: ["email"] }, null, 2),
		);

		// User types something that breaks JSON mid-edit.
		fireEvent.change(textarea, { target: { value: '{ "fields": ["ema' } });

		// The editor must retain the in-flight text — it must NOT snap back
		// to the formatted upstream value (which was the bug).
		expect(textarea.value).toBe('{ "fields": ["ema');

		// The inline error surface must report the JSON problem. Use a
		// specific class selector to avoid clashing with the dialog copy
		// ("Overwrite invalid JSON?", "Template will overwrite invalid
		// JSON. Continue?") that the component renders as a fallback when
		// loading a template onto broken input.
		const errorNodes = document.getElementsByClassName(
			"text-xs text-destructive",
		);
		const errorTexts = Array.from(errorNodes).map((n) => n.textContent ?? "");
		expect(
			errorTexts.some((t) => /JSON/i.test(t)),
		).toBe(true);
	});

	it("resyncs the editor text when an external prop change arrives", () => {
		const onChange = vi.fn();
		const { rerender } = render(
			<ProcessorEditor
				value={{
					name: "",
					type: "mask",
					operationTypes: [],
					options: { fields: ["email"] },
				}}
				onChange={onChange}
				onRemove={() => undefined}
				index={0}
			/>,
		);

		// External change (e.g. parent re-fetched pipeline) — the editor
		// should pick up the new options, not the stale local text.
		rerender(
			<ProcessorEditor
				value={{
					name: "",
					type: "mask",
					operationTypes: [],
					options: { fields: ["phone"], salt: "abc" },
				}}
				onChange={onChange}
				onRemove={() => undefined}
				index={0}
			/>,
		);

		const textarea = screen.getByTestId("monaco") as HTMLTextAreaElement;
		// Resync writes the compact form via JSON.stringify(value.options).
		expect(textarea.value).toBe(
			JSON.stringify({ fields: ["phone"], salt: "abc" }),
		);
	});

	it("propagates valid JSON edits to onChange and does not clobber them on the echo", () => {
		const onChange = vi.fn();
		render(
			<ProcessorEditor
				value={{
					name: "",
					type: "mask",
					operationTypes: [],
					options: {},
				}}
				onChange={onChange}
				onRemove={() => undefined}
				index={0}
			/>,
		);

		const textarea = screen.getByTestId("monaco") as HTMLTextAreaElement;
		fireEvent.change(textarea, {
			target: { value: '{ "fields": ["phone"] }' },
		});

		// The new options were pushed upstream via onChange.
		expect(onChange).toHaveBeenCalled();
		const lastCall = onChange.mock.calls.at(-1)?.[0] as
			| { options?: Record<string, unknown> }
			| undefined;
		expect(lastCall?.options).toEqual({ fields: ["phone"] });

		// And the editor text is preserved (not snapped back to the original {}).
		expect(textarea.value).toBe('{ "fields": ["phone"] }');
	});
});
