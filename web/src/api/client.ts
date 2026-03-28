import { API_BASE_URL, TOKEN_KEY } from "@/lib/constants";

type HttpMethod = "GET" | "POST" | "PUT" | "DELETE" | "PATCH";

interface RequestConfig extends RequestInit {
	skipAuth?: boolean;
}

interface ApiError {
	message: string;
	code?: string;
	status?: number;
}

class ApiClient {
	private baseUrl: string;

	constructor(baseUrl: string) {
		this.baseUrl = baseUrl;
	}

	private getToken(): string | null {
		return localStorage.getItem(TOKEN_KEY);
	}

	private clearAuth(): void {
		localStorage.removeItem(TOKEN_KEY);
	}

	private buildUrl(path: string): string {
		const cleanPath = path.startsWith("/") ? path : `/${path}`;
		return `${this.baseUrl}${cleanPath}`;
	}

	private async request<T>(
		method: HttpMethod,
		path: string,
		config: RequestConfig = {},
	): Promise<T> {
		const { skipAuth, ...fetchConfig } = config;
		const url = this.buildUrl(path);

		const headers = new Headers(fetchConfig.headers);

		headers.set("Accept", "application/json");

		if (!(fetchConfig.body instanceof FormData)) {
			headers.set("Content-Type", "application/json");
		}

		if (!skipAuth) {
			const token = this.getToken();
			if (token) {
				headers.set("Authorization", `Bearer ${token}`);
			}
		}

		try {
			const response = await fetch(url, {
				...fetchConfig,
				method,
				headers,
			});

			if (response.status === 401) {
				this.clearAuth();
				window.location.href = "/login";
				throw new Error("Unauthorized");
			}

			if (!response.ok) {
				let errorData: ApiError;
				try {
					errorData = await response.json();
				} catch {
					errorData = {
						message: response.statusText || "Request failed",
						status: response.status,
					};
				}
				throw new Error(errorData.message || `HTTP ${response.status}`);
			}

			const contentType = response.headers.get("content-type");
			if (contentType?.includes("application/json")) {
				return response.json() as Promise<T>;
			}

			return response.text() as Promise<T>;
		} catch (error) {
			if (error instanceof Error) {
				throw error;
			}
			throw new Error("Network error");
		}
	}

	async get<T>(path: string, config?: RequestConfig): Promise<T> {
		return this.request<T>("GET", path, config);
	}

	async post<T>(
		path: string,
		body?: unknown,
		config?: RequestConfig,
	): Promise<T> {
		return this.request<T>("POST", path, {
			...config,
			body: body ? JSON.stringify(body) : undefined,
		});
	}

	async put<T>(
		path: string,
		body?: unknown,
		config?: RequestConfig,
	): Promise<T> {
		return this.request<T>("PUT", path, {
			...config,
			body: body ? JSON.stringify(body) : undefined,
		});
	}

	async delete<T>(path: string, config?: RequestConfig): Promise<T> {
		return this.request<T>("DELETE", path, config);
	}

	async patch<T>(
		path: string,
		body?: unknown,
		config?: RequestConfig,
	): Promise<T> {
		return this.request<T>("PATCH", path, {
			...config,
			body: body ? JSON.stringify(body) : undefined,
		});
	}
}

export const apiClient = new ApiClient(API_BASE_URL);

export default apiClient;
