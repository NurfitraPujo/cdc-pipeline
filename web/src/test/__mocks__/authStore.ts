import { vi } from "vitest";

// Create a mock store that can be used in tests
export const mockAuthStore = {
	token: null as string | null,
	isAuthenticated: false,
	setToken: vi.fn((token: string | null) => {
		mockAuthStore.token = token;
		mockAuthStore.isAuthenticated = token !== null && token !== "";
	}),
	logout: vi.fn(() => {
		mockAuthStore.token = null;
		mockAuthStore.isAuthenticated = false;
	}),
	getState: vi.fn(() => ({
		token: mockAuthStore.token,
		isAuthenticated: mockAuthStore.isAuthenticated,
		setToken: mockAuthStore.setToken,
		logout: mockAuthStore.logout,
	})),
};

// Reset mock store to initial state
export function resetMockAuthStore() {
	mockAuthStore.token = null;
	mockAuthStore.isAuthenticated = false;
	mockAuthStore.setToken.mockClear();
	mockAuthStore.logout.mockClear();
	mockAuthStore.getState.mockClear();
}
