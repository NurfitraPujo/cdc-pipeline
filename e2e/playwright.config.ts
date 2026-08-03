import { defineConfig, devices } from "@playwright/test";

/**
 * Dedicated e2e ports.
 *
 * These deliberately avoid the defaults (3000 / 8080 / 4222). The previous
 * config pinned the web app to :3000 with `reuseExistingServer: true`, so
 * Playwright would attach to whatever happened to be listening there. A run
 * recorded in test-results/ did exactly that against an unrelated app, the
 * auth setup timed out on `#username`, and -- because `setup` is a dependency
 * of the `chromium` project -- all 12 tests were skipped rather than run.
 */
const WEB_PORT = Number(process.env.E2E_WEB_PORT ?? 3100);
const API_PORT = Number(process.env.E2E_API_PORT ?? 8090);

const WEB_URL = `http://localhost:${WEB_PORT}`;
export const API_URL = `http://localhost:${API_PORT}`;
export const API_BASE_URL = `${API_URL}/api/v1`;

export default defineConfig({
	testDir: "./tests",
	timeout: 60_000,
	expect: { timeout: 10_000 },
	fullyParallel: false,
	// The suite mutates shared server state, so it must stay serial.
	workers: 1,
	// One retry absorbs genuine startup jitter without masking a real failure,
	// which `retries: 0` turned into a hard red.
	retries: process.env.CI ? 2 : 1,
	forbidOnly: !!process.env.CI,
	reporter: process.env.CI
		? [["list"], ["html", { open: "never" }], ["junit", { outputFile: "test-results/junit.xml" }]]
		: [["list"], ["html", { open: "never" }]],
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
			use: {
				...devices["Desktop Chrome"],
				// Applied here rather than re-declared by hand in every spec.
				storageState: ".auth/admin.json",
			},
			dependencies: ["setup"],
		},
	],
	webServer: [
		{
			// Brings up an isolated NATS + the Go API. Without this the suite
			// silently required a manually-started backend.
			command: "bash ./scripts/start-api.sh",
			url: `${API_URL}/readyz`,
			timeout: 180_000,
			// Never adopt a foreign server: a stale or unrelated process on
			// this port must fail loudly instead of corrupting the run.
			reuseExistingServer: false,
			stdout: "pipe",
			stderr: "pipe",
			env: {
				E2E_API_PORT: String(API_PORT),
				E2E_WEB_PORT: String(WEB_PORT),
			},
		},
		{
			command: `npm run dev -- --port ${WEB_PORT}`,
			cwd: "../web",
			url: WEB_URL,
			timeout: 120_000,
			reuseExistingServer: false,
			stdout: "pipe",
			stderr: "pipe",
			env: {
				VITE_API_BASE_URL: API_BASE_URL,
				VITE_INTERNAL_API_BASE_URL: API_BASE_URL,
			},
		},
	],
});
