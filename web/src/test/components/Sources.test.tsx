import { fireEvent, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { EditSourcePage } from "@/routes/sources/$id/edit";
import { CreateSourcePage } from "@/routes/sources/create";
import { SourcesPage } from "@/routes/sources/index";
import { renderWithProviders } from "../utils";

// Mock @tanstack/react-router
vi.mock("@tanstack/react-router", () => {
	return {
		Link: ({ children, to }: any) => <a href={to}>{children}</a>,
		useNavigate: () => vi.fn(),
		createFileRoute: () => {
			return () => ({
				useParams: () => ({ id: "postgres-1" }),
			});
		},
	};
});

describe("Sources Index Page", () => {
	it("renders sources list successfully", async () => {
		renderWithProviders(<SourcesPage />);
		expect(await screen.findByText("Sources")).toBeInTheDocument();
		await waitFor(() => {
			expect(screen.getByText("postgres-1")).toBeInTheDocument();
			expect(screen.getByText("localhost:5432 / prod")).toBeInTheDocument();
		});
	});
});

describe("Create Source Page", () => {
	it("displays validation errors for required fields", async () => {
		renderWithProviders(<CreateSourcePage />);
		const submitBtn = screen.getByRole("button", { name: /Create Source/i });
		fireEvent.click(submitBtn);

		await waitFor(() => {
			expect(screen.getByText("Source ID is required")).toBeInTheDocument();
			expect(screen.getByText("Host is required")).toBeInTheDocument();
			expect(screen.getByText("Database name is required")).toBeInTheDocument();
			expect(screen.getByText("User is required")).toBeInTheDocument();
			expect(screen.getByText("Password is required")).toBeInTheDocument();
		});
	});

	it("triggers connection test and shows success message", async () => {
		renderWithProviders(<CreateSourcePage />);
		fireEvent.change(screen.getByLabelText(/Source ID/i), {
			target: { value: "test-src" },
		});
		fireEvent.change(screen.getByLabelText(/Host/i), {
			target: { value: "localhost" },
		});
		fireEvent.change(screen.getByLabelText(/Database Name/i), {
			target: { value: "testdb" },
		});
		fireEvent.change(screen.getByLabelText(/User/i), {
			target: { value: "postgres" },
		});
		fireEvent.change(screen.getByLabelText(/Password/i), {
			target: { value: "secret" },
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

describe("Edit Source Page", () => {
	it("prefills source details and submits edit", async () => {
		renderWithProviders(<EditSourcePage />);
		await waitFor(() => {
			expect(screen.getByLabelText(/Source ID/i)).toBeDisabled();
			expect(screen.getByLabelText(/Host/i)).toHaveValue("localhost");
			expect(screen.getByLabelText(/Database Name/i)).toHaveValue("prod");
		});
	});

	it("shows connection test success on edit page", async () => {
		renderWithProviders(<EditSourcePage />);
		await waitFor(() => {
			expect(screen.getByLabelText(/Host/i)).toHaveValue("localhost");
		});

		const testBtn = screen.getByRole("button", { name: /Test Connection/i });
		fireEvent.click(testBtn);

		await waitFor(() => {
			expect(screen.getByText("Connection Test Passed")).toBeInTheDocument();
		});
	});
});
