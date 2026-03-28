import { describe, it, expect, beforeEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { useAuthStore } from "@/stores/authStore";

describe("Auth Store", () => {
	beforeEach(() => {
		// Clear the store state before each test
		const { result } = renderHook(() => useAuthStore());
		act(() => {
			result.current.logout();
		});
	});

	it("should have initial state", () => {
		const { result } = renderHook(() => useAuthStore());

		expect(result.current.token).toBeNull();
		expect(result.current.isAuthenticated).toBe(false);
	});

	it("should set token and update auth state", () => {
		const { result } = renderHook(() => useAuthStore());

		act(() => {
			result.current.setToken("test-token");
		});

		expect(result.current.token).toBe("test-token");
		expect(result.current.isAuthenticated).toBe(true);
	});

	it("should handle logout", () => {
		const { result } = renderHook(() => useAuthStore());

		// First set token
		act(() => {
			result.current.setToken("test-token");
		});

		// Then logout
		act(() => {
			result.current.logout();
		});

		expect(result.current.token).toBeNull();
		expect(result.current.isAuthenticated).toBe(false);
	});

	it("should not authenticate with empty string", () => {
		const { result } = renderHook(() => useAuthStore());

		act(() => {
			result.current.setToken("");
		});

		expect(result.current.token).toBe("");
		expect(result.current.isAuthenticated).toBe(false);
	});

	it("should update auth state when token changes", () => {
		const { result } = renderHook(() => useAuthStore());

		// Start unauthenticated
		expect(result.current.isAuthenticated).toBe(false);

		// Set valid token
		act(() => {
			result.current.setToken("valid-token");
		});
		expect(result.current.isAuthenticated).toBe(true);

		// Clear token
		act(() => {
			result.current.setToken(null);
		});
		expect(result.current.isAuthenticated).toBe(false);
	});
});
