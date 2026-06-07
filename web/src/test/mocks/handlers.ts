import { HttpResponse, http } from "msw";
import type { Pipeline } from "@/api/pipelines";
import type { LoginRequest } from "@/api/types";
import {
	createMockPipeline,
	mockLoginResponse,
	mockPipelines,
	mockSinks,
	mockSources,
	mockStatsSummary,
} from "./data";

const API_BASE = "http://localhost:8080/api/v1";

export const handlers = [
	// Auth
	http.post(`${API_BASE}/login`, async ({ request }) => {
		const body = (await request.json()) as LoginRequest;

		if (body.username === "admin" && body.password === "admin") {
			return HttpResponse.json(mockLoginResponse);
		}

		return HttpResponse.json({ error: "Invalid credentials" }, { status: 401 });
	}),

	// Stats
	http.get(`${API_BASE}/stats/summary`, () => {
		return HttpResponse.json(mockStatsSummary);
	}),

	http.get(`${API_BASE}/stats/history`, () => {
		return HttpResponse.json([]);
	}),

	// Pipelines
	http.get(`${API_BASE}/pipelines`, ({ request }) => {
		const url = new URL(request.url);
		const page = Number(url.searchParams.get("page")) || 1;
		const limit = Number(url.searchParams.get("limit")) || 10;
		const search = url.searchParams.get("search") || "";
		const status = url.searchParams.get("status");

		let filtered = [...mockPipelines];

		if (search) {
			filtered = filtered.filter(
				(p) =>
					p.name.toLowerCase().includes(search.toLowerCase()) ||
					p.id.toLowerCase().includes(search.toLowerCase()),
			);
		}

		if (status) {
			filtered = filtered.filter(
				(p) => (p as { status?: string }).status === status,
			);
		}

		const total = filtered.length;
		const totalPages = Math.ceil(total / limit);
		const offset = (page - 1) * limit;
		const paginated = filtered.slice(offset, offset + limit);

		return HttpResponse.json({
			pipelines: paginated,
			pagination: {
				page,
				limit,
				total,
				total_pages: totalPages,
			},
		});
	}),

	http.get<{ id: string }>(`${API_BASE}/pipelines/:id`, ({ params }) => {
		const pipeline = mockPipelines.find((p) => p.id === params.id);

		if (!pipeline) {
			return HttpResponse.json(
				{ error: "Pipeline not found" },
				{ status: 404 },
			);
		}

		return HttpResponse.json(pipeline);
	}),

	http.post<never, Pipeline>(`${API_BASE}/pipelines`, async ({ request }) => {
		const body = (await request.json()) as Omit<Pipeline, "id">;
		const newPipeline = createMockPipeline(body);
		mockPipelines.push(newPipeline);
		return HttpResponse.json(newPipeline, { status: 201 });
	}),

	http.put<{ id: string }, Partial<Pipeline>>(
		`${API_BASE}/pipelines/:id`,
		async ({ params, request }) => {
			const body = await request.json();
			const index = mockPipelines.findIndex((p) => p.id === params.id);

			if (index === -1) {
				return HttpResponse.json(
					{ error: "Pipeline not found" },
					{ status: 404 },
				);
			}

			mockPipelines[index] = { ...mockPipelines[index], ...body };
			return HttpResponse.json(mockPipelines[index]);
		},
	),

	http.delete<{ id: string }>(`${API_BASE}/pipelines/:id`, ({ params }) => {
		const index = mockPipelines.findIndex((p) => p.id === params.id);

		if (index === -1) {
			return HttpResponse.json(
				{ error: "Pipeline not found" },
				{ status: 404 },
			);
		}

		mockPipelines.splice(index, 1);
		return HttpResponse.json({ success: true });
	}),

	http.post<{ id: string }>(
		`${API_BASE}/pipelines/:id/restart`,
		({ params }) => {
			const pipeline = mockPipelines.find((p) => p.id === params.id);

			if (!pipeline) {
				return HttpResponse.json(
					{ error: "Pipeline not found" },
					{ status: 404 },
				);
			}

			(pipeline as { status?: string }).status = "running";
			return HttpResponse.json({ success: true });
		},
	),

	// Sources
	http.get(`${API_BASE}/sources`, () => {
		return HttpResponse.json({ sources: mockSources });
	}),

	http.get<{ id: string }>(`${API_BASE}/sources/:id`, ({ params }) => {
		const source = mockSources.find((s) => s.id === params.id);
		if (!source) {
			return HttpResponse.json({ error: "Source not found" }, { status: 404 });
		}
		return HttpResponse.json(source);
	}),

	http.post<never, any>(`${API_BASE}/sources`, async ({ request }) => {
		const body = await request.json();
		if (mockSources.some((s) => s.id === body.id)) {
			return HttpResponse.json(
				{ error: "Source already exists" },
				{ status: 400 },
			);
		}
		const newSource = { ...body };
		mockSources.push(newSource);
		return HttpResponse.json(newSource, { status: 201 });
	}),

	http.put<{ id: string }, any>(
		`${API_BASE}/sources/:id`,
		async ({ params, request }) => {
			const body = await request.json();
			const index = mockSources.findIndex((s) => s.id === params.id);
			if (index === -1) {
				return HttpResponse.json(
					{ error: "Source not found" },
					{ status: 404 },
				);
			}
			mockSources[index] = { ...mockSources[index], ...body };
			return HttpResponse.json(mockSources[index]);
		},
	),

	http.delete<{ id: string }>(`${API_BASE}/sources/:id`, ({ params }) => {
		const index = mockSources.findIndex((s) => s.id === params.id);
		if (index === -1) {
			return HttpResponse.json({ error: "Source not found" }, { status: 404 });
		}
		mockSources.splice(index, 1);
		return new HttpResponse(null, { status: 204 });
	}),

	http.get<{ id: string }>(`${API_BASE}/sources/:id/tables`, () => {
		return HttpResponse.json(["users", "orders", "products", "categories"]);
	}),

	http.post<never, any>(`${API_BASE}/sources/test`, async ({ request }) => {
		const body = await request.json();
		if (body.host === "invalid") {
			return HttpResponse.json({ error: "Host not found" }, { status: 400 });
		}
		return HttpResponse.json({
			status: "ok",
			message: "Connection successful",
		});
	}),

	// Sinks
	http.get(`${API_BASE}/sinks`, () => {
		return HttpResponse.json({ sinks: mockSinks });
	}),

	http.get<{ id: string }>(`${API_BASE}/sinks/:id`, ({ params }) => {
		const sink = mockSinks.find((s) => s.id === params.id);
		if (!sink) {
			return HttpResponse.json({ error: "Sink not found" }, { status: 404 });
		}
		return HttpResponse.json(sink);
	}),

	http.post<never, any>(`${API_BASE}/sinks`, async ({ request }) => {
		const body = await request.json();
		if (mockSinks.some((s) => s.id === body.id)) {
			return HttpResponse.json(
				{ error: "Sink already exists" },
				{ status: 400 },
			);
		}
		const newSink = { ...body };
		mockSinks.push(newSink);
		return HttpResponse.json(newSink, { status: 201 });
	}),

	http.put<{ id: string }, any>(
		`${API_BASE}/sinks/:id`,
		async ({ params, request }) => {
			const body = await request.json();
			const index = mockSinks.findIndex((s) => s.id === params.id);
			if (index === -1) {
				return HttpResponse.json({ error: "Sink not found" }, { status: 404 });
			}
			mockSinks[index] = { ...mockSinks[index], ...body };
			return HttpResponse.json(mockSinks[index]);
		},
	),

	http.delete<{ id: string }>(`${API_BASE}/sinks/:id`, ({ params }) => {
		const index = mockSinks.findIndex((s) => s.id === params.id);
		if (index === -1) {
			return HttpResponse.json({ error: "Sink not found" }, { status: 404 });
		}
		mockSinks.splice(index, 1);
		return new HttpResponse(null, { status: 204 });
	}),

	http.post<never, any>(`${API_BASE}/sinks/test`, async ({ request }) => {
		const body = await request.json();
		if (body.dsn === "invalid") {
			return HttpResponse.json(
				{ error: "Failed to connect to sink" },
				{ status: 400 },
			);
		}
		return HttpResponse.json({
			status: "ok",
			message: "Connection successful",
		});
	}),

	// Global Config
	http.get(`${API_BASE}/global`, () => {
		return HttpResponse.json({
			batchSize: 1000,
			batchWait: "5s",
			retry: {
				maxRetries: 3,
				initialBackoff: "1s",
				maxBackoff: "30s",
			},
		});
	}),

	http.put(`${API_BASE}/global`, async ({ request }) => {
		const body = await request.json();
		return HttpResponse.json(body);
	}),
];

// Error handlers for testing error states
export const errorHandlers = {
	loginError: http.post(`${API_BASE}/login`, () => {
		return HttpResponse.json({ error: "Invalid credentials" }, { status: 401 });
	}),

	serverError: http.get(`${API_BASE}/stats/summary`, () => {
		return HttpResponse.json(
			{ error: "Internal server error" },
			{ status: 500 },
		);
	}),

	networkError: http.get(`${API_BASE}/pipelines`, () => {
		return HttpResponse.error();
	}),
};
