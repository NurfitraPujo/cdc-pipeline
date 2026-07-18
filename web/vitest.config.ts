import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import tsconfigPaths from "vite-tsconfig-paths";
import { defineConfig } from "vitest/config";

// Some `@radix-ui/react-*` primitives used by the dashboard UI primitives
// (`src/components/ui/*`) are not installed in this project's
// `node_modules`. Alias the packages so component-level tests can render.
// `react-accordion` is part of the production `dependencies` but pnpm has
// not synced it into `node_modules` in this environment, so the stub is used
// in tests as a transitive fallback (the production bundle ships the real
// package). See `web/src/test/stubs/radix-stub.ts` for the stub.
const RADIX_STUB = fileURLToPath(
	new URL("./src/test/stubs/radix-stub.ts", import.meta.url),
);
const radixAliases = Object.fromEntries(
	[
		"react-accordion",
		"react-checkbox",
		"react-dialog",
		"react-dropdown-menu",
		"react-label",
		"react-radio-group",
		"react-select",
		"react-separator",
		"react-slot",
		"react-switch",
		"react-tooltip",
	].map((pkg) => [`@radix-ui/${pkg}`, RADIX_STUB]),
);

export default defineConfig({
	plugins: [react(), tsconfigPaths()],
	resolve: {
		alias: radixAliases,
	},
	test: {
		environment: "happy-dom",
		globals: true,
		setupFiles: ["./src/test/setup.ts"],
		include: ["./src/**/*.test.{ts,tsx}"],
		exclude: ["./node_modules/**", "./dist/**"],
		coverage: {
			provider: "v8",
			reporter: ["text", "html"],
			include: ["src/**/*.{ts,tsx}"],
			exclude: [
				"src/**/*.d.ts",
				"src/test/**",
				"src/**/*.test.{ts,tsx}",
				"src/routeTree.gen.ts",
			],
		},
	},
});
