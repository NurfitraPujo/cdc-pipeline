import { apiClient } from "./client";
import type { LoginRequest, LoginResponse } from "./types";

export const authApi = {
	async login(credentials: LoginRequest): Promise<LoginResponse> {
		return apiClient.post<LoginResponse>("/login", credentials, {
			skipAuth: true,
		});
	},
};
