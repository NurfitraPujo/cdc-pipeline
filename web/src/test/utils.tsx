import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { RenderOptions } from "@testing-library/react";
import { render } from "@testing-library/react";
import type { ReactElement, ReactNode } from "react";

// Create a test query client
export function createTestQueryClient() {
	return new QueryClient({
		defaultOptions: {
			queries: {
				retry: false,
				gcTime: 0,
				staleTime: 0,
			},
		},
	});
}

interface ProvidersProps {
	children: ReactNode;
	queryClient?: QueryClient;
}

export function TestProviders({ children, queryClient }: ProvidersProps) {
	const client = queryClient ?? createTestQueryClient();

	return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

interface RenderWithProvidersOptions extends Omit<RenderOptions, "wrapper"> {
	queryClient?: QueryClient;
}

export function renderWithProviders(
	ui: ReactElement,
	options: RenderWithProvidersOptions = {},
) {
	const { queryClient, ...renderOptions } = options;
	const testQueryClient = queryClient ?? createTestQueryClient();

	return render(ui, {
		wrapper: ({ children }) => (
			<TestProviders queryClient={testQueryClient}>{children}</TestProviders>
		),
		...renderOptions,
	});
}

// Re-export testing library utilities
export * from "@testing-library/react";
export { default as userEvent } from "@testing-library/user-event";
