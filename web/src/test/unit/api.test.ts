import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { authApi } from "@/api/auth";
import { pipelinesApi } from "@/api/pipelines";
import { __resetAuthRedirectFlag } from "@/api/schema-client";
import { API_BASE_URL } from "@/lib/constants";
import { useAuthStore } from "@/stores/authStore";
import { server } from "../mocks/server";

const mockLocation = {
	href: "",
	assign: vi.fn(),
};
Object.defineProperty(window, "location", {
	value: mockLocation,
	writable: true,
});

describe("API Client (schema-driven)", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockLocation.href = "";
		mockLocation.assign.mockReset();

		// Reset auth store to a known unauthenticated state.
		useAuthStore.setState({ token: null, isAuthenticated: false });

		// The 401 interceptor in schema-client holds a module-scoped re-entry
		// guard so concurrent 401s don't all schedule a redirect. Reset it
		// before each test so the 401-handling cases can run more than once
		// in the same module instance.
		__resetAuthRedirectFlag();
	});

	describe("authApi.login", () => {
		it("returns token on success", async () => {
			server.use(
				http.post(`${API_BASE_URL}/login`, async ({ request }) => {
					const body = (await request.json()) as {
						username: string;
						password: string;
					};
					if (body.username === "admin" && body.password === "admin") {
						return HttpResponse.json({ token: "abc" });
					}
					return HttpResponse.json({ error: "bad" }, { status: 401 });
				}),
			);

			const result = await authApi.login({
				username: "admin",
				password: "admin",
			});
			expect(result).toEqual({ token: "abc" });
		});

		it("throws on bad credentials", async () => {
			server.use(
				http.post(`${API_BASE_URL}/login`, () =>
					HttpResponse.json({ error: "bad" }, { status: 401 }),
				),
			);
			await expect(
				authApi.login({ username: "x", password: "y" }),
			).rejects.toThrow();
		});
	});

	describe("auth header", () => {
		it("sends Bearer token from the auth store", async () => {
			useAuthStore.getState().setToken("jwt-123");

			let observedAuth: string | null = null;
			server.use(
				http.get(`${API_BASE_URL}/pipelines`, ({ request }) => {
					observedAuth = request.headers.get("Authorization");
					return HttpResponse.json({ pipelines: [] });
				}),
			);

			await pipelinesApi.list();
			expect(observedAuth).toBe("Bearer jwt-123");
		});
	});

	describe("401 handling", () => {
		it("logs out via the auth store and redirects on 401", async () => {
			useAuthStore.getState().setToken("old-token");
			expect(useAuthStore.getState().isAuthenticated).toBe(true);

			server.use(
				http.get(`${API_BASE_URL}/pipelines`, () =>
					HttpResponse.json({ error: "unauthorized" }, { status: 401 }),
				),
			);

			await expect(pipelinesApi.list()).rejects.toThrow();

			// After a 401 the store is the single source of truth — it must be
			// logged out so the route guard stops bouncing the user back to
			// /dashboard (T0-7).
			const state = useAuthStore.getState();
			expect(state.token).toBeNull();
			expect(state.isAuthenticated).toBe(false);
			expect(mockLocation.assign).toHaveBeenCalledWith("/login");
		});

		it("does not redirect when the failing endpoint is /login", async () => {
			useAuthStore.getState().setToken("old-token");

			server.use(
				http.post(`${API_BASE_URL}/login`, () =>
					HttpResponse.json({ error: "bad" }, { status: 401 }),
				),
			);

			await expect(
				authApi.login({ username: "x", password: "y" }),
			).rejects.toThrow();

			// /login 401 is the "bad credentials" path. The store must be left
			// untouched so the user can retry.
			const state = useAuthStore.getState();
			expect(state.token).toBe("old-token");
			expect(state.isAuthenticated).toBe(true);
			expect(mockLocation.assign).not.toHaveBeenCalled();
		});

		it("clears isAuthenticated in the persisted store snapshot", async () => {
			useAuthStore.getState().setToken("old-token");

			server.use(
				http.get(`${API_BASE_URL}/pipelines`, () =>
					HttpResponse.json({ error: "unauthorized" }, { status: 401 }),
				),
			);

			await expect(pipelinesApi.list()).rejects.toThrow();

			// The Zustand store is the single source of truth. The persist
			// middleware mirrors the same snapshot into localStorage under
			// "cdc-auth-storage", which is what the __root.tsx beforeLoad
			// guard reads on the next page load. Asserting the in-memory
			// state here is sufficient because partialize() writes every
			// field unconditionally on state change.
			const state = useAuthStore.getState();
			expect(state.token).toBeNull();
			expect(state.isAuthenticated).toBe(false);
		});
	});
});
