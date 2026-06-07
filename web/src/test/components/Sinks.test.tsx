import { fireEvent, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { EditSinkPage } from "@/routes/sinks/$id/edit";
import { CreateSinkPage } from "@/routes/sinks/create";
import { SinksPage } from "@/routes/sinks/index";
import { renderWithProviders } from "../utils";

// Mock @tanstack/react-router
vi.mock("@tanstack/react-router", () => {
	return {
		Link: ({ children, to }: any) => <a href={to}>{children}</a>,
		useNavigate: () => vi.fn(),
		createFileRoute: () => {
			return () => ({
				useParams: () => ({ id: "databend-1" }),
			});
		},
	};
});

describe("Sinks Index Page", () => {
	it("renders sinks list successfully", async () => {
		renderWithProviders(<SinksPage />);
		expect(await screen.findByText("Sinks")).toBeInTheDocument();
		await waitFor(() => {
			expect(screen.getByText("databend-1")).toBeInTheDocument();
			expect(
				screen.getByText("databend://localhost:8000/analytics"),
			).toBeInTheDocument();
		});
	});
});

describe("Create Sink Page", () => {
	it("displays validation errors for required fields", async () => {
		renderWithProviders(<CreateSinkPage />);
		const submitBtn = screen.getByRole("button", { name: /Create Sink/i });
		fireEvent.click(submitBtn);

		await waitFor(() => {
			expect(screen.getByText("Sink ID is required")).toBeInTheDocument();
			expect(
				screen.getByText("DSN (connection string) is required"),
			).toBeInTheDocument();
		});
	});

	it("triggers connection test and shows success message", async () => {
		renderWithProviders(<CreateSinkPage />);
		fireEvent.change(screen.getByLabelText(/Sink ID/i), {
			target: { value: "test-sink" },
		});
		fireEvent.change(screen.getByLabelText(/DSN/i), {
			target: { value: "postgres://localhost/test" },
		});

		const testBtn = screen.getByRole("button", { name: /Test Connection/i });
		fireEvent.click(testBtn);

		await waitFor(() => {
			expect(screen.getByText("Connection Test Passed")).toBeInTheDocument();
			expect(
				screen.getByText("Connection test succeeded!"),
			).toBeInTheDocument();
		});
	});
});

describe("Edit Sink Page", () => {
	it("prefills sink details", async () => {
		renderWithProviders(<EditSinkPage />);
		await waitFor(() => {
			expect(screen.getByLabelText(/Sink ID/i)).toBeDisabled();
			expect(screen.getByLabelText(/DSN/i)).toHaveValue(
				"databend://localhost:8000/analytics",
			);
		});
	});

	it("shows connection test success on edit page", async () => {
		renderWithProviders(<EditSinkPage />);
		await waitFor(() => {
			expect(screen.getByLabelText(/DSN/i)).toHaveValue(
				"databend://localhost:8000/analytics",
			);
		});

		const testBtn = screen.getByRole("button", { name: /Test Connection/i });
		fireEvent.click(testBtn);

		await waitFor(() => {
			expect(screen.getByText("Connection Test Passed")).toBeInTheDocument();
		});
	});
});
