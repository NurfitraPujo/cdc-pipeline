import { http, HttpResponse } from "msw";
import type {
	LoginRequest,
	LoginResponse,
	Pipeline,
	Source,
	Sink,
	StatsSummary,
} from "@/api/types";
import {
	mockLoginResponse,
	mockStatsSummary,
	mockPipelines,
	mockSources,
	mockSinks,
	createMockPipeline,
} from "./data";

const API_BASE = "http://localhost:8080/api/v1";

export const handlers = [
	// Auth
	http.post<LoginRequest, LoginResponse>(
		`${API_BASE}/login`,
		async ({ request }) => {
			const body = await request.json();
			
			if (body.username === "admin" && body.password === "admin") {
				return HttpResponse.json(mockLoginResponse);
			}
			
			return HttpResponse.json(
				{ error: "Invalid credentials" },
				{ status: 401 }
			);
		}
	),

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
					p.id.toLowerCase().includes(search.toLowerCase())
			);
		}

		if (status) {
			filtered = filtered.filter((p) => p.status === status);
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
				{ status: 404 }
			);
		}
		
		return HttpResponse.json(pipeline);
	}),

	http.post<Omit<Pipeline, "id">, Pipeline>(
		`${API_BASE}/pipelines`,
		async ({ request }) => {
			const body = await request.json();
			const newPipeline = createMockPipeline(body);
			mockPipelines.push(newPipeline);
			return HttpResponse.json(newPipeline, { status: 201 });
		}
	),

	http.put<{ id: string }, Partial<Pipeline>>(
		`${API_BASE}/pipelines/:id`,
		async ({ params, request }) => {
			const body = await request.json();
			const index = mockPipelines.findIndex((p) => p.id === params.id);
			
			if (index === -1) {
				return HttpResponse.json(
					{ error: "Pipeline not found" },
					{ status: 404 }
				);
			}
			
			mockPipelines[index] = { ...mockPipelines[index], ...body };
			return HttpResponse.json(mockPipelines[index]);
		}
	),

	http.delete<{ id: string }>(`${API_BASE}/pipelines/:id`, ({ params }) => {
		const index = mockPipelines.findIndex((p) => p.id === params.id);
		
		if (index === -1) {
			return HttpResponse.json(
				{ error: "Pipeline not found" },
				{ status: 404 }
			);
		}
		
		mockPipelines.splice(index, 1);
		return HttpResponse.json({ success: true });
	}),

	http.post<{ id: string }>(`${API_BASE}/pipelines/:id/restart`, ({ params }) => {
		const pipeline = mockPipelines.find((p) => p.id === params.id);
		
		if (!pipeline) {
			return HttpResponse.json(
				{ error: "Pipeline not found" },
				{ status: 404 }
			);
		}
		
		pipeline.status = "running";
		pipeline.lastRunAt = new Date().toISOString();
		return HttpResponse.json({ success: true });
	}),

	// Sources
	http.get(`${API_BASE}/sources`, () => {
		return HttpResponse.json(mockSources);
	}),

	http.get<{ id: string }>(`${API_BASE}/sources/:id/tables`, ({ params }) => {
		return HttpResponse.json(["users", "orders", "products", "categories"]);
	}),

	// Sinks
	http.get(`${API_BASE}/sinks`, () => {
		return HttpResponse.json(mockSinks);
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
		return HttpResponse.json(
			{ error: "Invalid credentials" },
			{ status: 401 }
		);
	}),

	serverError: http.get(`${API_BASE}/stats/summary`, () => {
		return HttpResponse.json(
			{ error: "Internal server error" },
			{ status: 500 }
		);
	}),

	networkError: http.get(`${API_BASE}/pipelines`, () => {
		return HttpResponse.error();
	}),
};
