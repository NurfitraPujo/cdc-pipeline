import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useSSE } from "@/hooks/useSSE";
import { useAuthStore } from "@/stores/authStore";

interface MockEventSourceInstance {
	url: string;
	openSpy: () => void;
	messageSpy: (data: string) => void;
	errorSpy: () => void;
	close: () => void;
	readyState: number;
	listeners: Record<string, Array<(event: unknown) => void>>;
}

const constructed: MockEventSourceInstance[] = [];

class MockEventSource {
	static CONNECTING = 0;
	static OPEN = 1;
	static CLOSED = 2;

	url: string;
	readyState = MockEventSource.OPEN;
	close = vi.fn(() => {
		this.readyState = MockEventSource.CLOSED;
	});
	onopen: ((event: Event) => void) | null = null;
	onmessage: ((event: MessageEvent) => void) | null = null;
	onerror: ((event: Event) => void) | null = null;

	constructor(url: string) {
		this.url = url;
		const instance: MockEventSourceInstance = {
			url,
			openSpy: () => this.onopen?.(new Event("open")),
			messageSpy: (data: string) =>
				this.onmessage?.({ data } as unknown as MessageEvent),
			errorSpy: () => this.onerror?.(new Event("error")),
			close: this.close,
			readyState: this.readyState,
			listeners: {},
		};
		constructed.push(instance);
	}
}

beforeEach(() => {
	constructed.length = 0;
	(globalThis as unknown as { EventSource: typeof MockEventSource }).EventSource =
		MockEventSource;
	useAuthStore.setState({ token: "test-token", isAuthenticated: true });
});

afterEach(() => {
	vi.restoreAllMocks();
});

describe("useSSE", () => {
	it("does not reconnect when the caller re-renders with a fresh options object (T1-16)", async () => {
		const { result, rerender } = renderHook(
			({ cb }: { cb: (data: unknown) => void }) =>
				useSSE<{ hello: string }>("/pipelines/p1/metrics", {
					onMessage: cb,
				}),
			{ initialProps: { cb: (_d: unknown) => undefined } },
		);

		expect(constructed).toHaveLength(1);
		const original = constructed[0];

		// Simulate a parent re-render that supplies a brand-new inline options
		// object literal — the exact pattern that previously caused EventSource
		// to be torn down and reconnected on every render.
		const newCb = (_d: unknown) => undefined;
		rerender({ cb: newCb });

		// No new EventSource should have been created; the original connection
		// must remain intact.
		expect(constructed).toHaveLength(1);
		expect(constructed[0]).toBe(original);
		expect(original.close).not.toHaveBeenCalled();

		// The new callback should still be invoked when the server pushes a
		// message, proving the optionsRef is read at fire time, not memoized.
		const spy = vi.fn();
		rerender({ cb: spy });
		expect(constructed).toHaveLength(1);
		act(() => {
			constructed[0].messageSpy(JSON.stringify({ hello: "world" }));
		});
		expect(spy).toHaveBeenCalledWith({ hello: "world" });

		// Sanity check: hook still exposes the reconnect control.
		expect(typeof result.current.reconnect).toBe("function");
	});

	it("reconnects when the endpoint prop changes", async () => {
		const { rerender } = renderHook(
			({ endpoint }: { endpoint: string }) => useSSE(endpoint),
			{ initialProps: { endpoint: "/pipelines/p1/metrics" } },
		);

		expect(constructed).toHaveLength(1);
		const first = constructed[0];

		rerender({ endpoint: "/pipelines/p2/metrics" });

		// A different endpoint must trigger a fresh connection.
		expect(constructed).toHaveLength(2);
		expect(constructed[1]).not.toBe(first);
		expect(first.close).toHaveBeenCalled();
	});

	it("closes the EventSource on unmount", async () => {
		const { unmount } = renderHook(() => useSSE("/pipelines/p1/metrics"));
		expect(constructed).toHaveLength(1);
		const instance = constructed[0];
		unmount();
		expect(instance.close).toHaveBeenCalled();
	});
});
