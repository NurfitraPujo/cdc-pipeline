const isServer = typeof window === "undefined";
export const API_BASE_URL = isServer
	? import.meta.env.VITE_INTERNAL_API_BASE_URL ||
		import.meta.env.VITE_API_BASE_URL ||
		"http://api:8080/api/v1"
	: import.meta.env.VITE_API_BASE_URL || "http://localhost:8080/api/v1";

// The JWT lives in the Zustand persist key "cdc-auth-storage" — see
// `web/src/stores/authStore.ts`. A previous, separate localStorage key was
// removed when the auth store became the single source of truth (T0-7).
export const DEFAULT_PAGE_SIZE = 20;
