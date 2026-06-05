import { setAuthToken } from "./schema-client";

const USERNAME_KEY = "cdc_username";

export interface User {
	username: string;
}

export function getStoredUsername(): string | null {
	if (typeof window === "undefined") return null;
	return localStorage.getItem(USERNAME_KEY);
}

export function setStoredUsername(username: string | null): void {
	if (typeof window === "undefined") return;
	if (username) {
		localStorage.setItem(USERNAME_KEY, username);
	} else {
		localStorage.removeItem(USERNAME_KEY);
	}
}

export function logout(): void {
	setAuthToken(null);
	setStoredUsername(null);
	if (typeof window !== "undefined") {
		window.location.href = "/login";
	}
}
