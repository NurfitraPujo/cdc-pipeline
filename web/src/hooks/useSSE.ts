import { useCallback, useEffect, useRef, useState } from "react";
import { API_BASE_URL } from "@/lib/constants";
import { useAuthStore } from "@/stores/authStore";

interface SSEOptions {
	onMessage?: (data: unknown) => void;
	onError?: (error: Event) => void;
	onOpen?: () => void;
}

interface SSEState<T> {
	data: T | null;
	isConnected: boolean;
	error: Error | null;
}

interface SSEReturn<T> extends SSEState<T> {
	reconnect: () => void;
}

export function useSSE<T = unknown>(
	endpoint: string,
	options: SSEOptions = {},
): SSEReturn<T> {
	const { token } = useAuthStore();
	const [state, setState] = useState<SSEState<T>>({
		data: null,
		isConnected: false,
		error: null,
	});

	const eventSourceRef = useRef<EventSource | null>(null);
	const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(
		null,
	);

	// T1-16: callers typically pass an inline object literal for `options`,
	// e.g. `useSSE("/x", { onMessage: fn })`. A plain `[endpoint, token, options]`
	// dependency array would therefore re-create the EventSource on every render,
	// leaking the previous connection. Hold the latest callbacks in a ref so the
	// connect closure reads the freshest handlers without invalidating itself.
	const optionsRef = useRef<SSEOptions>(options);
	useEffect(() => {
		optionsRef.current = options;
	}, [options]);

	const connect = useCallback(() => {
		// Close existing connection
		if (eventSourceRef.current) {
			eventSourceRef.current.close();
		}

		// Clear any pending reconnect
		if (reconnectTimeoutRef.current) {
			clearTimeout(reconnectTimeoutRef.current);
			reconnectTimeoutRef.current = null;
		}

		if (!token) {
			setState((prev) => ({
				...prev,
				isConnected: false,
				error: new Error("No authentication token available"),
			}));
			return;
		}

		// Build URL with token as query param since EventSource doesn't support headers
		const baseUrl = API_BASE_URL.replace("/api/v1", "");
		const cleanEndpoint = endpoint.startsWith("/") ? endpoint : `/${endpoint}`;
		const url = `${baseUrl}/api/v1${cleanEndpoint}?token=${encodeURIComponent(token)}`;

		try {
			const es = new EventSource(url);
			eventSourceRef.current = es;

			es.onopen = () => {
				setState((prev) => ({
					...prev,
					isConnected: true,
					error: null,
				}));
				optionsRef.current.onOpen?.();
			};

			es.onmessage = (event) => {
				try {
					const parsedData = JSON.parse(event.data) as T;
					setState((prev) => ({
						...prev,
						data: parsedData,
					}));
					optionsRef.current.onMessage?.(parsedData);
				} catch (_err) {
					// If not JSON, use raw data
					const rawData = event.data as unknown as T;
					setState((prev) => ({
						...prev,
						data: rawData,
					}));
					optionsRef.current.onMessage?.(rawData);
				}
			};

			es.onerror = (error) => {
				setState((prev) => ({
					...prev,
					isConnected: false,
					error: new Error("SSE connection error"),
				}));
				optionsRef.current.onError?.(error);

				// Auto-reconnect after 3 seconds
				reconnectTimeoutRef.current = setTimeout(() => {
					connect();
				}, 3000);
			};
		} catch (err) {
			setState((prev) => ({
				...prev,
				isConnected: false,
				error:
					err instanceof Error ? err : new Error("Failed to connect to SSE"),
			}));
		}
	}, [endpoint, token]);

	const reconnect = useCallback(() => {
		setState({
			data: null,
			isConnected: false,
			error: null,
		});
		connect();
	}, [connect]);

	useEffect(() => {
		connect();

		return () => {
			if (eventSourceRef.current) {
				eventSourceRef.current.close();
			}
			if (reconnectTimeoutRef.current) {
				clearTimeout(reconnectTimeoutRef.current);
			}
		};
	}, [connect]);

	return {
		...state,
		reconnect,
	};
}
