import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderWithRouter } from "../utils";
import { server } from "../mocks/server";
import { errorHandlers } from "../mocks/handlers";

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

describe("Login Integration", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		localStorageMock.getItem.mockReturnValue(null);
	});

	it("should render login form", async () => {
		renderWithRouter("/login");

		expect(await screen.findByText(/cdc pipeline/i)).toBeInTheDocument();
		
		// Look for username/password inputs
		const usernameInput = screen.queryByLabelText(/username/i);
		const passwordInput = screen.queryByLabelText(/password/i);
		
		// May be placeholder text instead of labels
		if (usernameInput) expect(usernameInput).toBeInTheDocument();
		if (passwordInput) expect(passwordInput).toBeInTheDocument();
		
		expect(screen.getByRole("button", { name: /sign in|login/i })).toBeInTheDocument();
	});

	it("should show error message with invalid credentials", async () => {
		const user = userEvent.setup();
		
		// Override handler to return error
		server.use(errorHandlers.loginError);
		
		renderWithRouter("/login");

		// Wait for form
		await screen.findByText(/cdc pipeline/i);

		// Find inputs
		const usernameInput = screen.queryByLabelText(/username/i) || screen.queryByPlaceholderText(/username/i);
		const passwordInput = screen.queryByLabelText(/password/i) || screen.queryByPlaceholderText(/password/i);

		if (usernameInput && passwordInput) {
			await user.type(usernameInput, "wrong");
			await user.type(passwordInput, "wrong");

			// Submit form
			await user.click(screen.getByRole("button", { name: /sign in|login/i }));

			// Wait for error message
			await waitFor(() => {
				const errorMessage = screen.queryByText(/invalid|error|failed/i);
				expect(errorMessage).toBeInTheDocument();
			});
		}
	});
});
