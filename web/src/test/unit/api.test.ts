import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { authApi } from "@/api/auth";
import { pipelinesApi } from "@/api/pipelines";
import { API_BASE_URL, TOKEN_KEY } from "@/lib/constants";
import { server } from "../mocks/server";

const localStorageMock = {
	getItem: vi.fn(),
	setItem: vi.fn(),
	removeItem: vi.fn(),
};
Object.defineProperty(window, "localStorage", {
	value: localStorageMock,
});

const mockLocation = { href: "" };
Object.defineProperty(window, "location", {
	value: mockLocation,
	writable: true,
});

describe("API Client (schema-driven)", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockLocation.href = "";
		localStorageMock.getItem.mockReturnValue(null);
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
		it("sends Bearer token from localStorage", async () => {
			localStorageMock.getItem.mockReturnValue("jwt-123");

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
		it("clears token and redirects on 401", async () => {
			localStorageMock.getItem.mockReturnValue("old-token");
			server.use(
				http.get(`${API_BASE_URL}/pipelines`, () =>
					HttpResponse.json({ error: "unauthorized" }, { status: 401 }),
				),
			);

			await expect(pipelinesApi.list()).rejects.toThrow();
			expect(localStorageMock.removeItem).toHaveBeenCalledWith(TOKEN_KEY);
			expect(mockLocation.href).toBe("/login");
		});
	});
});
