import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { describe, expect, it } from "vitest";
import { PauseDialog } from "@/components/pipelines/PauseDialog";
import { API_BASE_URL } from "@/lib/constants";
import { server } from "@/test/mocks/server";
import { renderWithProviders } from "@/test/utils";

describe("PauseDialog", () => {
	it("shows the resume time for the default TTL before submitting", async () => {
		const user = userEvent.setup();
		renderWithProviders(<PauseDialog pipelineId="p1" />);

		await user.click(screen.getByRole("button", { name: /^pause$/i }));
		expect(screen.getByText(/resumes automatically at/i)).toBeInTheDocument();
	});

	it("caps the TTL slider at the 4h ceiling", async () => {
		const user = userEvent.setup();
		renderWithProviders(<PauseDialog pipelineId="p1" />);
		await user.click(screen.getByRole("button", { name: /^pause$/i }));

		const slider = screen.getByLabelText(/pause for/i) as HTMLInputElement;
		expect(slider.max).toBe(String(4 * 60));
	});

	it("submitting shows the resume time and the WAL-budget warning from the server", async () => {
		server.use(
			http.post(`${API_BASE_URL}/pipelines/:id/pause`, async () => {
				return HttpResponse.json({
					state: "Paused",
					paused_until: "2026-08-04T12:00:00Z",
					reconciliation: "",
					updated_at: "2026-08-04T08:00:00Z",
					warning:
						"at the current WAL growth rate this pause will hit the WAL budget in ~1h45m0s",
				});
			}),
		);

		const user = userEvent.setup();
		renderWithProviders(<PauseDialog pipelineId="p1" />);
		await user.click(screen.getByRole("button", { name: /^pause$/i }));
		await user.click(screen.getByRole("button", { name: /confirm pause/i }));

		await waitFor(() => {
			expect(screen.getByTestId("pause-result")).toBeInTheDocument();
		});
		expect(screen.getByTestId("pause-breach-warning")).toHaveTextContent(
			"WAL budget in ~1h45m0s",
		);
	});

	it("submitting with no warning shows the resume time only", async () => {
		server.use(
			http.post(`${API_BASE_URL}/pipelines/:id/pause`, async () => {
				return HttpResponse.json({
					state: "Paused",
					paused_until: "2026-08-04T12:00:00Z",
					reconciliation: "",
					updated_at: "2026-08-04T08:00:00Z",
				});
			}),
		);

		const user = userEvent.setup();
		renderWithProviders(<PauseDialog pipelineId="p1" />);
		await user.click(screen.getByRole("button", { name: /^pause$/i }));
		await user.click(screen.getByRole("button", { name: /confirm pause/i }));

		await waitFor(() => {
			expect(screen.getByTestId("pause-result")).toBeInTheDocument();
		});
		expect(
			screen.queryByTestId("pause-breach-warning"),
		).not.toBeInTheDocument();
	});

	it("surfaces a 409 error and does not show the pause-result panel", async () => {
		server.use(
			http.post(`${API_BASE_URL}/pipelines/:id/pause`, async () => {
				return HttpResponse.json(
					{
						error:
							'lifecycle: illegal transition: Stopping does not accept event "pause"',
					},
					{ status: 409 },
				);
			}),
		);

		const user = userEvent.setup();
		renderWithProviders(<PauseDialog pipelineId="p1" />);
		await user.click(screen.getByRole("button", { name: /^pause$/i }));
		await user.click(screen.getByRole("button", { name: /confirm pause/i }));

		await waitFor(() => {
			expect(screen.getByText(/illegal transition/i)).toBeInTheDocument();
		});
		expect(screen.queryByTestId("pause-result")).not.toBeInTheDocument();
	});

	it("renders as 'Extend pause' when isExtend is set -- extending must be trivial", async () => {
		renderWithProviders(<PauseDialog pipelineId="p1" isExtend />);
		expect(
			screen.getByRole("button", { name: /extend pause/i }),
		).toBeInTheDocument();
	});

	// Plan section 5's whole mitigation for "a 4h pause on a busy source
	// silently becomes a stop + re-snapshot" is this pre-commit projection --
	// it must actually render before the operator confirms, not just on the
	// post-commit response.
	it("shows the pre-commit WAL-budget warning when the projected breach is shorter than the TTL", async () => {
		server.use(
			http.get(`${API_BASE_URL}/pipelines/:id/pause-projection`, () => {
				return HttpResponse.json({
					warning:
						"at the current WAL growth rate this pause will hit the WAL budget in ~45m0s",
				});
			}),
		);

		const user = userEvent.setup();
		renderWithProviders(<PauseDialog pipelineId="p1" />);
		await user.click(screen.getByRole("button", { name: /^pause$/i }));

		// Default TTL (60m preset) is long enough that a ~45m projected
		// breach must surface as a warning before "Confirm pause" is clicked.
		await waitFor(() => {
			expect(
				screen.getByTestId("pause-projection-warning"),
			).toBeInTheDocument();
		});
		expect(screen.getByTestId("pause-projection-warning")).toHaveTextContent(
			"WAL budget in ~45m0s",
		);
	});

	// The operator must not be left thinking a long pause is safe when the
	// projection is simply unavailable -- a failed projection must fail
	// visibly, not render as "no warning".
	it("fails visibly, not silently, when the projection query errors", async () => {
		server.use(
			http.get(`${API_BASE_URL}/pipelines/:id/pause-projection`, () => {
				return HttpResponse.json({ error: "internal error" }, { status: 500 });
			}),
		);

		const user = userEvent.setup();
		renderWithProviders(<PauseDialog pipelineId="p1" />);
		await user.click(screen.getByRole("button", { name: /^pause$/i }));

		await waitFor(() => {
			expect(
				screen.getByTestId("pause-projection-unavailable"),
			).toBeInTheDocument();
		});
		expect(
			screen.queryByTestId("pause-projection-warning"),
		).not.toBeInTheDocument();
	});
});
