import "@testing-library/jest-dom";
import { cleanup } from "@testing-library/react";
import { afterAll, afterEach, beforeAll, vi } from "vitest";
import { server } from "./mocks/server";

// Mock matchMedia
Object.defineProperty(window, "matchMedia", {
	writable: true,
	value: vi.fn().mockImplementation((query: string) => ({
		matches: false,
		media: query,
		onchange: null,
		addListener: vi.fn(),
		removeListener: vi.fn(),
		addEventListener: vi.fn(),
		removeEventListener: vi.fn(),
		dispatchEvent: vi.fn(),
	})),
});

// Mock localStorage
const localStorageMock = {
	getItem: vi.fn(),
	setItem: vi.fn(),
	removeItem: vi.fn(),
	clear: vi.fn(),
};
Object.defineProperty(window, "localStorage", {
	value: localStorageMock,
});

// Mock IntersectionObserver
const intersectionObserverMock = vi.fn().mockImplementation(() => ({
	observe: vi.fn(),
	disconnect: vi.fn(),
	unobserve: vi.fn(),
}));
Object.defineProperty(window, "IntersectionObserver", {
	value: intersectionObserverMock,
});

// Start MSW server before all tests
beforeAll(() => {
	server.listen({ onUnhandledRequest: "error" });
});

// Reset handlers after each test
afterEach(() => {
	server.resetHandlers();
	cleanup();
	vi.clearAllMocks();
});

// Close server after all tests
afterAll(() => {
	server.close();
});
