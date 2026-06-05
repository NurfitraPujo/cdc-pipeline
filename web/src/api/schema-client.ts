import createClient from "openapi-fetch";
import { API_BASE_URL, TOKEN_KEY } from "@/lib/constants";
import type { paths } from "./schema";

export const apiClient = createClient<paths>({
	baseUrl: API_BASE_URL,
});

export function getAuthToken(): string | null {
	if (typeof window === "undefined") return null;
	return localStorage.getItem(TOKEN_KEY);
}

export function setAuthToken(token: string | null): void {
	if (typeof window === "undefined") return;
	if (token) {
		localStorage.setItem(TOKEN_KEY, token);
	} else {
		localStorage.removeItem(TOKEN_KEY);
	}
}

export function handleUnauthorized(): void {
	if (typeof window === "undefined") return;
	localStorage.removeItem(TOKEN_KEY);
	window.location.href = "/login";
}

apiClient.use({
	async onRequest({ request }) {
		const token = getAuthToken();
		if (token) {
			request.headers.set("Authorization", `Bearer ${token}`);
		}
		return request;
	},
	async onResponse({ response, request }) {
		// Only treat 401 as a session expiry when the request was authenticated.
		// The `/login` endpoint is public and returns 401 for bad credentials —
		// navigating away on that response would wipe the in-flight error message.
		if (response.status === 401 && !request.url.endsWith("/login")) {
			handleUnauthorized();
		}
		return response;
	},
});

export function unwrap<T = unknown>(result: {
	error?: unknown;
	data?: unknown;
	response: Response;
}): T {
	if (result.error) {
		const errObj = result.error as { error?: string } | undefined;
		throw new Error(
			errObj?.error ?? `Request failed (${result.response.status})`,
		);
	}
	if (!result.data) {
		throw new Error(`Request failed (${result.response.status})`);
	}
	return result.data as T;
}
