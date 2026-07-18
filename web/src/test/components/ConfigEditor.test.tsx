import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

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

import { ConfigEditor } from "@/components/ConfigEditor";

afterEach(() => {
	vi.restoreAllMocks();
});

describe("ConfigEditor (T2-9)", () => {
	it("updates the editor text when initialValue changes after mount", () => {
		const onSave = vi.fn();
		const { rerender } = render(
			<ConfigEditor initialValue='{"a":1}' onSave={onSave} />,
		);

		const textarea = screen.getByTestId("monaco") as HTMLTextAreaElement;
		expect(textarea.value).toBe('{"a":1}');

		// Server pushes a new value (e.g. parent re-fetched the config).
		rerender(<ConfigEditor initialValue='{"a":2}' onSave={onSave} />);

		expect(textarea.value).toBe('{"a":2}');
	});

	it("clears any pending error when initialValue changes", () => {
		const onSave = vi.fn();
		const { rerender } = render(
			<ConfigEditor initialValue='{"a":1}' onSave={onSave} />,
		);

		const textarea = screen.getByTestId("monaco") as HTMLTextAreaElement;
		// Introduce an inline validation error: tabs are not allowed. The
		// validator only runs when the user clicks Save, so we exercise that
		// path explicitly.
		fireEvent.change(textarea, { target: { value: '{\t"a": 1}' } });
		fireEvent.click(screen.getByRole("button", { name: /^Save$/i }));
		expect(screen.getByText(/Tabs are not allowed/i)).toBeInTheDocument();

		// A fresh initialValue from the parent should drop the stale
		// validation message AND refresh the editor text.
		rerender(<ConfigEditor initialValue='{"a":2}' onSave={onSave} />);
		expect(screen.queryByText(/Tabs are not allowed/i)).not.toBeInTheDocument();
		expect((screen.getByTestId("monaco") as HTMLTextAreaElement).value).toBe(
			'{"a":2}',
		);
	});
});
