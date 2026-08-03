import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { NatsProtobufOptionsValue } from "@/components/pipelines/NatsProtobufOptions";
import { NatsProtobufOptions } from "@/components/pipelines/NatsProtobufOptions";

function renderOptions(
	value: NatsProtobufOptionsValue = {},
	props: Partial<React.ComponentProps<typeof NatsProtobufOptions>> = {},
) {
	const onChange = vi.fn();
	render(
		<NatsProtobufOptions
			index={0}
			value={value}
			onChange={onChange}
			{...props}
		/>,
	);
	return { onChange };
}

describe("NatsProtobufOptions", () => {
	it("flags the two options the transformer cannot start without", () => {
		renderOptions({});
		// NewNatsProtoTransformer returns an error when either is absent.
		expect(screen.getAllByText(/Required/i).length).toBeGreaterThanOrEqual(2);
	});

	it("warns when neither a schema nor a table is set", () => {
		renderOptions({ nats_url: "nats://x:4222", subject: "s" });
		// WS-8 item 4: an unfiltered instance forwards every table to the
		// responder, where they all fail metadata lookup.
		expect(
			screen.getByText(/at least one schema or table/i),
		).toBeInTheDocument();
	});

	it("drops the warning once a filter is present", () => {
		renderOptions({
			nats_url: "nats://x:4222",
			subject: "s",
			schemas: ["custom_objects"],
		});
		expect(screen.queryByText(/at least one schema or table/i)).toBeNull();
	});

	// The OR semantics are a genuine footgun -- schemas:[custom_objects] plus
	// tables:[visitations] admits public.visitations too.
	it("spells out that schemas and tables OR together", () => {
		renderOptions({});
		expect(screen.getByText(/combine with OR/i)).toBeInTheDocument();
	});

	it("writes nats_url and subject onto the options map", () => {
		const { onChange } = renderOptions({});

		fireEvent.change(screen.getByLabelText("NATS URL"), {
			target: { value: "nats://core:4222" },
		});
		expect(onChange).toHaveBeenCalledWith(
			expect.objectContaining({ nats_url: "nats://core:4222" }),
		);

		fireEvent.change(screen.getByLabelText("Subject"), {
			target: { value: "daya.core.transform" },
		});
		expect(onChange).toHaveBeenCalledWith(
			expect.objectContaining({ subject: "daya.core.transform" }),
		);
	});

	it("removes a key rather than storing an empty string", () => {
		const { onChange } = renderOptions({ nats_url: "nats://core:4222" });

		fireEvent.change(screen.getByLabelText("NATS URL"), {
			target: { value: "" },
		});
		expect(onChange).toHaveBeenCalledWith({});
	});

	it("offers the source's schemas and toggles them", () => {
		const { onChange } = renderOptions(
			{},
			{ availableSchemas: ["public", "custom_objects"] },
		);

		fireEvent.click(screen.getByRole("button", { name: "custom_objects" }));
		expect(onChange).toHaveBeenCalledWith(
			expect.objectContaining({ schemas: ["custom_objects"] }),
		);
	});

	it("derives schema suggestions from the discovered tables too", () => {
		renderOptions(
			{},
			{ availableTables: ["orders", "custom_objects.visitations"] },
		);

		expect(
			screen.getByRole("button", { name: "custom_objects" }),
		).toBeInTheDocument();
		expect(screen.getByRole("button", { name: "public" })).toBeInTheDocument();
	});

	// matchesTable compares against m.Table, which carries no schema, so the
	// stored value must be the bare name even though the chip lives under a
	// schema heading.
	it("stores a table by its bare name, not qualified", () => {
		const { onChange } = renderOptions(
			{},
			{ availableTables: ["custom_objects.visitations"] },
		);

		fireEvent.click(screen.getByRole("button", { name: "visitations" }));
		expect(onChange).toHaveBeenCalledWith(
			expect.objectContaining({ tables: ["visitations"] }),
		);
	});

	it("deselects an already-selected entry", () => {
		const { onChange } = renderOptions(
			{ schemas: ["custom_objects"] },
			{ availableSchemas: ["custom_objects"] },
		);

		fireEvent.click(screen.getByRole("button", { name: /custom_objects/ }));
		// Emptied lists are removed entirely rather than sent as [].
		expect(onChange).toHaveBeenCalledWith(
			expect.not.objectContaining({ schemas: expect.anything() }),
		);
	});

	it("falls back to free-text entry when nothing was discovered", () => {
		const { onChange } = renderOptions({}, {});

		const input = screen.getByPlaceholderText(/Add a schema/i);
		fireEvent.change(input, { target: { value: "custom_objects" } });
		fireEvent.keyDown(input, { key: "Enter" });

		expect(onChange).toHaveBeenCalledWith(
			expect.objectContaining({ schemas: ["custom_objects"] }),
		);
	});

	it("keeps timeout_ms numeric and clears it when blanked", () => {
		const { onChange } = renderOptions({ timeout_ms: 5000 });

		const input = screen.getByLabelText(/Request timeout/i);
		fireEvent.change(input, { target: { value: "20000" } });
		expect(onChange).toHaveBeenCalledWith(
			expect.objectContaining({ timeout_ms: 20000 }),
		);

		fireEvent.change(input, { target: { value: "" } });
		expect(onChange).toHaveBeenCalledWith(
			expect.not.objectContaining({ timeout_ms: expect.anything() }),
		);
	});

	// batch_size and pipeline_id are injected by engine/factory.go; offering
	// them here would only let an operator override values the worker owns.
	it("does not expose batch_size or pipeline_id", () => {
		renderOptions({});
		expect(screen.queryByLabelText(/batch size/i)).toBeNull();
		expect(screen.queryByLabelText(/pipeline id/i)).toBeNull();
	});
});
