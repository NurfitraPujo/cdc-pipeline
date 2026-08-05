import { cloudflare } from "@cloudflare/vite-plugin";
import tailwindcss from "@tailwindcss/vite";
import { devtools } from "@tanstack/devtools-vite";

import { tanstackStart } from "@tanstack/react-start/plugin/vite";

import viteReact from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import tsconfigPaths from "vite-tsconfig-paths";

const config = defineConfig({
	server: {
		proxy: {
			"/api": {
				target: "http://localhost:8080",
				changeOrigin: true,
			},
		},
	},
	plugins: [
		// Devtools visibility is handled ourselves in src/routes/__root.tsx
		// (VITE_ENABLE_DEVTOOLS + lazy import), which also covers the SSR
		// bundle. This plugin's own removeDevtoolsOnBuild transform only
		// touches the client build and doesn't rewrite the `return (...)`
		// wrapper cleanly when devtools JSX is a component's sole child, so
		// disable it here to avoid the conflict.
		devtools({ removeDevtoolsOnBuild: false }),
		...(process.env.BUILD_TARGET === "cloudflare"
			? [cloudflare({ viteEnvironment: { name: "ssr" } })]
			: []),
		tsconfigPaths({ projects: ["./tsconfig.json"] }),
		tailwindcss(),
		// SPA mode: this is an internal, auth-gated dashboard whose auth state
		// lives in localStorage (client-only, invisible to the server -- see
		// src/stores/authStore.ts). SSR therefore cannot evaluate the auth
		// guard and was rendering the protected /dashboard shell for
		// unauthenticated requests, with the client expected to redirect to
		// /login afterwards. If client JS was slow or failed to load, users saw
		// the dashboard shell instead of the login form. Prerendering a static
		// shell and rendering routes only on the client makes the beforeLoad
		// auth guard authoritative before any protected content is shown.
		tanstackStart({ spa: { enabled: true } }),
		viteReact({
			babel: {
				plugins: ["babel-plugin-react-compiler"],
			},
		}),
	],
});

export default config;
