import createClient from "openapi-fetch";
import { API_BASE_URL } from "@/lib/constants";
import { useAuthStore } from "@/stores/authStore";
import type { paths } from "./schema";

export const apiClient = createClient<paths>({
	baseUrl: API_BASE_URL,
});

/**
 * In-flight 401 redirect guard.
 *
 * The `onResponse` interceptor may fire concurrently for several in-flight
 * requests after the access token is revoked. Without coordination, each one
 * would invoke `logout()` and schedule `window.location.assign("/login")`,
 * racing against the navigation and corrupting the Zustand store. We use a
 * module-scoped flag so only the first 401 performs the redirect; later ones
 * see the flag and no-op until the page reload completes.
 */
let isRedirectingForAuth = false;

export function getAuthToken(): string | null {
	if (typeof window === "undefined") return null;
	return useAuthStore.getState().token;
}

export function setAuthToken(token: string | null): void {
	if (typeof window === "undefined") return;
	if (token !== null && token !== "") {
		useAuthStore.getState().setToken(token);
	} else {
		useAuthStore.getState().setToken(null);
	}
}

export function handleUnauthorized(): void {
	if (typeof window === "undefined") return;
	if (isRedirectingForAuth) return;
	isRedirectingForAuth = true;
	useAuthStore.getState().logout();
	window.location.assign("/login");
}

/**
 * Test-only helper. Resets the in-flight 401 redirect guard so unit tests
 * can exercise the 401 handling logic more than once per module instance.
 * Not intended for production use.
 */
export function __resetAuthRedirectFlag(): void {
	isRedirectingForAuth = false;
}

apiClient.use({
	async onRequest({ request }) {
		const token = useAuthStore.getState().token;
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
	if (result.response.status === 204) {
		return undefined as T;
	}
	if (
		result.response.status >= 200 &&
		result.response.status < 300 &&
		!result.data
	) {
		return undefined as T;
	}
	if (!result.data) {
		throw new Error(`Request failed (${result.response.status})`);
	}
	return result.data as T;
}
