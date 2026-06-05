import { defineConfig, devices } from "@playwright/test";

const API_URL = "http://localhost:8080";
const WEB_URL = "http://localhost:3000";

export default defineConfig({
	testDir: "./tests",
	timeout: 30_000,
	expect: { timeout: 5_000 },
	fullyParallel: false,
	retries: 0,
	workers: 1,
	reporter: [["list"]],
	use: {
		baseURL: WEB_URL,
		trace: "on-first-retry",
		screenshot: "only-on-failure",
		video: "retain-on-failure",
	},
	projects: [
		{ name: "setup", testMatch: /.*\.setup\.ts/ },
		{
			name: "chromium",
			use: { ...devices["Desktop Chrome"] },
			dependencies: ["setup"],
		},
	],
	webServer: {
		command: "cd ../web && npm run dev",
		url: WEB_URL,
		timeout: 60_000,
		reuseExistingServer: true,
		stdout: "pipe",
		stderr: "pipe",
	},
});
