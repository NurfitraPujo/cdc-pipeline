import { camelToSnake, snakeToCamel } from "./mappers";
import type { components } from "./schema";
import { apiClient, unwrap } from "./schema-client";
import type { LoginRequest, LoginResponse } from "./types";

type WireLoginRequest = components["schemas"]["LoginRequest"];
type WireLoginResponse = components["schemas"]["LoginResponse"];

export const authApi = {
	async login(credentials: LoginRequest): Promise<LoginResponse> {
		const wire = camelToSnake<WireLoginRequest>(credentials);
		const result = await apiClient.POST("/login", { body: wire });
		return snakeToCamel<LoginResponse>(unwrap<WireLoginResponse>(result));
	},
};
