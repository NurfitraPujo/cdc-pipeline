import { describe, it, expect, vi, beforeEach } from "vitest";
import { apiClient } from "@/api/client";
import { API_BASE_URL, TOKEN_KEY } from "@/lib/constants";
import { server } from "../mocks/server";
import { http, HttpResponse } from "msw";

// Mock localStorage
const localStorageMock = {
	getItem: vi.fn(),
	setItem: vi.fn(),
	removeItem: vi.fn(),
};
Object.defineProperty(window, "localStorage", {
	value: localStorageMock,
});

// Mock window.location
const mockLocation = { href: "" };
Object.defineProperty(window, "location", {
	value: mockLocation,
	writable: true,
});

describe("API Client", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockLocation.href = "";
	});

	describe("HTTP Methods", () => {
		it("should make GET request", async () => {
			server.use(
				http.get(`${API_BASE_URL}/test`, () => {
					return HttpResponse.json({ data: "test" });
				})
			);

			const result = await apiClient.get("/test");
			expect(result).toEqual({ data: "test" });
		});

		it("should make POST request with body", async () => {
			server.use(
				http.post(`${API_BASE_URL}/test`, async ({ request }) => {
					const body = await request.json();
					return HttpResponse.json({ received: body });
				})
			);

			const body = { name: "test" };
			const result = await apiClient.post("/test", body);
			expect(result).toEqual({ received: body });
		});

		it("should make PUT request", async () => {
			server.use(
				http.put(`${API_BASE_URL}/test`, async ({ request }) => {
					const body = await request.json();
					return HttpResponse.json({ updated: body });
				})
			);

			const body = { id: 1, name: "updated" };
			const result = await apiClient.put("/test", body);
			expect(result).toEqual({ updated: body });
		});

		it("should make DELETE request", async () => {
			server.use(
				http.delete(`${API_BASE_URL}/test`, () => {
					return HttpResponse.json({ deleted: true });
				})
			);

			const result = await apiClient.delete("/test");
			expect(result).toEqual({ deleted: true });
		});
	});

	describe("Authentication", () => {
		it("should include auth token when available", async () => {
			const token = "test-token";
			localStorageMock.getItem.mockReturnValue(token);

			let requestHeaders: Headers | null = null;
			server.use(
				http.get(`${API_BASE_URL}/protected`, ({ request }) => {
					requestHeaders = request.headers;
					return HttpResponse.json({ data: "protected" });
				})
			);

			await apiClient.get("/protected");

			expect(requestHeaders?.get("Authorization")).toBe(`Bearer ${token}`);
		});

		it("should redirect to login on 401", async () => {
			localStorageMock.getItem.mockReturnValue("expired-token");

			server.use(
				http.get(`${API_BASE_URL}/protected`, () => {
					return HttpResponse.json(
						{ error: "Unauthorized" },
						{ status: 401 }
					);
				})
			);

			await expect(apiClient.get("/protected")).rejects.toThrow();

			expect(localStorageMock.removeItem).toHaveBeenCalledWith(TOKEN_KEY);
			expect(mockLocation.href).toBe("/login");
		});
	});

	describe("Error Handling", () => {
		it("should throw error with status code for API errors", async () => {
			server.use(
				http.get(`${API_BASE_URL}/error`, () => {
					return HttpResponse.json(
						{ error: "Something went wrong" },
						{ status: 500 }
					);
				})
			);

			await expect(apiClient.get("/error")).rejects.toThrow("HTTP 500");
		});

		it("should throw generic error for network failures", async () => {
			server.use(
				http.get(`${API_BASE_URL}/network-error`, () => {
					return HttpResponse.error();
				})
			);

			await expect(apiClient.get("/network-error")).rejects.toThrow();
		});
	});
});
