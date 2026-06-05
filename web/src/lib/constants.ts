const isServer = typeof window === "undefined";
export const API_BASE_URL = isServer
	? import.meta.env.VITE_INTERNAL_API_BASE_URL ||
		import.meta.env.VITE_API_BASE_URL ||
		"http://api:8080/api/v1"
	: import.meta.env.VITE_API_BASE_URL || "/api/v1";

export const TOKEN_KEY = "cdc_token";

export const DEFAULT_PAGE_SIZE = 20;
