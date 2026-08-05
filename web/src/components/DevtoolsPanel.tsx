import { TanStackDevtools } from "@tanstack/react-devtools";
import { TanStackRouterDevtoolsPanel } from "@tanstack/react-router-devtools";
import TanStackQueryDevtools from "../integrations/tanstack-query/devtools";
import StoreDevtools from "../lib/demo-store-devtools";

// Isolated so it can be lazy-loaded: this keeps @tanstack/react-devtools,
// @tanstack/react-router-devtools, and @tanstack/devtools-event-client out of
// the eagerly-loaded server/client bundles when devtools are disabled
// (VITE_ENABLE_DEVTOOLS !== "true", e.g. in production).
export default function DevtoolsPanel() {
	return (
		<TanStackDevtools
			config={{
				position: "bottom-right",
			}}
			plugins={[
				{
					name: "Tanstack Router",
					render: <TanStackRouterDevtoolsPanel />,
				},
				TanStackQueryDevtools,
				StoreDevtools,
			]}
		/>
	);
}
