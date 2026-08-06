// Runtime config injected by the Node entrypoint (server-node.mjs) into the
// SPA shell's <head> as `window.__APP_CONFIG__`, BEFORE this bundle loads.
// Vite inlines `import.meta.env.VITE_*` at BUILD time, so the container's
// runtime env (Helm configMap) can never reach the already-built browser
// bundle -- that is why the prod dashboard used to POST to localhost:8080.
// Reading the injected config first lets one image serve any environment.
declare global {
	interface Window {
		__APP_CONFIG__?: { apiBaseUrl?: string };
	}
}

const isServer = typeof window === "undefined";
export const API_BASE_URL = isServer
	? import.meta.env.VITE_INTERNAL_API_BASE_URL ||
		import.meta.env.VITE_API_BASE_URL ||
		"http://api:8080/api/v1"
	: window.__APP_CONFIG__?.apiBaseUrl ||
		import.meta.env.VITE_API_BASE_URL ||
		"http://localhost:8080/api/v1";

// The JWT lives in the Zustand persist key "cdc-auth-storage" — see
// `web/src/stores/authStore.ts`. A previous, separate localStorage key was
// removed when the auth store became the single source of truth (T0-7).
export const DEFAULT_PAGE_SIZE = 20;
