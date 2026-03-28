import { describe, it, expect, vi, beforeEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { useAuthStore } from "@/stores/authStore";

// Mock localStorage
const localStorageMock = {\tgetItem: vi.fn(),
	setItem: vi.fn(),
	removeItem: vi.fn(),
};
Object.defineProperty(window, "localStorage", {
	value: localStorageMock,
});

describe("Auth Store", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		localStorageMock.getItem.mockReturnValue(null);
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

	it("should persist to localStorage", () => {
		const { result } = renderHook(() => useAuthStore());

		act(() => {
			result.current.setToken("test-token");
		});

		expect(localStorageMock.setItem).toHaveBeenCalledWith(
			"cdc-auth-storage",
			expect.stringContaining("test-token")
		);
	});

	it("should restore from localStorage on init", () => {
		const persistedState = JSON.stringify({
			state: {
				token: "persisted-token",
				isAuthenticated: true,
			},
			version: 0,
		});
		localStorageMock.getItem.mockReturnValue(persistedState);

		const { result } = renderHook(() => useAuthStore());

		expect(result.current.token).toBe("persisted-token");
		expect(result.current.isAuthenticated).toBe(true);
	});
});
