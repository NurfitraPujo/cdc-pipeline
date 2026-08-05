import type { APIRequestContext } from "@playwright/test";
import { expect, test } from "@playwright/test";
import {
	cleanup,
	createApiContext,
	createPipeline,
	createSink,
	createSource,
	uniqueId,
} from "./helpers/api";

let api: APIRequestContext;
const created = {
	pipelines: [] as string[],
	sources: [] as string[],
	sinks: [] as string[],
};

test.beforeAll(async () => {
	api = await createApiContext();
});

test.afterAll(async () => {
	await cleanup(api, created);
	await api.dispose();
});

/** Seeds a pipeline with its source and sink, registered for teardown. */
async function seedPipeline(prefix: string) {
	const sourceId = uniqueId(`${prefix}-src`);
	const sinkId = uniqueId(`${prefix}-snk`);
	const pipelineId = uniqueId(prefix);

	await createSource(api, { id: sourceId });
	created.sources.push(sourceId);
	await createSink(api, sinkId);
	created.sinks.push(sinkId);

	await createPipeline(api, {
		id: pipelineId,
		name: `E2E ${prefix}`,
		sources: [sourceId],
		sinks: [sinkId],
	});
	created.pipelines.push(pipelineId);

	return { pipelineId, sourceId, sinkId };
}

/**
 * This suite's harness (scripts/start-api.sh) boots only the Go API against
 * an isolated NATS instance -- there is no live worker and no reachable
 * Postgres source (see e2e/scripts/start-api.sh, docker-only NATS). That is
 * enough to exercise the full pause/stop lifecycle (both endpoints commit
 * their state transition synchronously and only hand the *drain* off
 * asynchronously -- see PausePipeline/StopPipeline in internal/api/handler.go),
 * but NOT enough to exercise a resume that depends on a real replication
 * slot: StartPipeline's (Paused, start) -> Resuming guard consults a real
 * `config.NewPostgresSlotHealthChecker` in this binary (cmd/api/main.go),
 * which will fail to reach a slot that was never created and correctly
 * reject the resume with 409 ("do not resume" is the documented behaviour
 * on any probe failure). The re-snapshot/reconciliation legs of the
 * lifecycle (NeedsResnapshot -> Snapshotting -> Running with a stale
 * reconciliation sub-status) need a genuinely running worker driving a real
 * Postgres source through a stop window and are not exercised here; that
 * requires the live-worker harness named in the plan (section 9) that does
 * not exist yet, and no test below claims otherwise.
 */
test.describe("Pipeline lifecycle", () => {
	test("pausing shows a distinct lifecycle badge and the resume time", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("lc-pause");

		await page.goto(`/pipelines/${pipelineId}`);
		await expect(page.getByRole("button", { name: /^pause$/i })).toBeVisible();

		await page.getByRole("button", { name: /^pause$/i }).click();
		await expect(
			page.getByRole("heading", { name: "Pause pipeline" }),
		).toBeVisible();
		await expect(page.getByText(/resumes automatically at/i)).toBeVisible();

		const response = page.waitForResponse(
			(r) =>
				r.url().includes(`/pipelines/${pipelineId}/pause`) &&
				r.request().method() === "POST",
		);
		await page.getByRole("button", { name: /confirm pause/i }).click();
		expect((await response).status()).toBe(200);

		await page.getByRole("button", { name: /^done$/i }).click();

		// Lifecycle badge, distinct from the health badge -- a paused
		// pipeline is neither healthy nor unhealthy (plan section 4.1).
		await expect(page.getByTestId("lifecycle-badge")).toHaveText("Paused");
		await expect(page.getByTestId("paused-until")).toBeVisible();

		// The API record agrees.
		const rec = (await (await api.get(`/api/v1/pipelines/${pipelineId}`)).json()) as {
			lifecycle_state: string;
			paused_until: string | null;
		};
		expect(rec.lifecycle_state).toBe("Paused");
		expect(rec.paused_until).not.toBeNull();
	});

	test("extending a pause reuses the same dialog and posts a fresh ttl", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("lc-extend");

		expect((await api.post(`/api/v1/pipelines/${pipelineId}/pause`, {
			data: { ttl: "15m" },
		})).status()).toBe(200);

		const before = (await (
			await api.get(`/api/v1/pipelines/${pipelineId}`)
		).json()) as { paused_until: string };
		expect(before.paused_until).toBeTruthy();

		await page.goto(`/pipelines/${pipelineId}`);
		await expect(
			page.getByRole("button", { name: /extend pause/i }),
		).toBeVisible();

		const response = page.waitForResponse(
			(r) =>
				r.url().includes(`/pipelines/${pipelineId}/pause`) &&
				r.request().method() === "POST",
		);
		await page.getByRole("button", { name: /extend pause/i }).click();
		// Pick a preset well past the original 15m so the recomputed
		// paused_until is unambiguously later, not just later-by-clock-skew.
		await page.getByRole("button", { name: /^4h$/i }).click();
		await page.getByRole("button", { name: /confirm pause/i }).click();
		const body = (await response).request().postDataJSON() as { ttl: string };
		expect(body.ttl).toBeTruthy();

		// This is the entire point of "extend": the request must actually
		// succeed (Paused -> Paused is now a legal transition, not the 409
		// the dialog used to always get back) and paused_until must have
		// moved later, not stayed pinned to the original pause.
		expect((await response).status()).toBe(200);
		await expect(page.getByTestId("pause-result")).toBeVisible();
		await expect(page.getByTestId("pause-breach-warning")).not.toBeVisible();

		const after = (await (
			await api.get(`/api/v1/pipelines/${pipelineId}`)
		).json()) as { lifecycle_state: string; paused_until: string };
		expect(after.lifecycle_state).toBe("Paused");
		expect(new Date(after.paused_until).getTime()).toBeGreaterThan(
			new Date(before.paused_until).getTime(),
		);
	});

	test("a rejected resume (no live slot in this harness) surfaces the 409 rather than silently no-oping", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("lc-resume-reject");

		expect((await api.post(`/api/v1/pipelines/${pipelineId}/pause`, {
			data: { ttl: "15m" },
		})).status()).toBe(200);

		await page.goto(`/pipelines/${pipelineId}`);
		await expect(page.getByRole("button", { name: /^start$/i })).toBeVisible();

		const response = page.waitForResponse(
			(r) =>
				r.url().includes(`/pipelines/${pipelineId}/start`) &&
				r.request().method() === "POST",
		);
		await page.getByRole("button", { name: /^start$/i }).click();
		expect((await response).status()).toBe(409);

		// Lifecycle stays Paused -- the UI must not claim success.
		await expect(page.getByTestId("lifecycle-badge")).toHaveText("Paused");
	});

	test("stopping transitions to Stopping and the badge reflects it", async ({
		page,
	}) => {
		const { pipelineId } = await seedPipeline("lc-stop");

		await page.goto(`/pipelines/${pipelineId}`);
		const response = page.waitForResponse(
			(r) =>
				r.url().includes(`/pipelines/${pipelineId}/stop`) &&
				r.request().method() === "POST",
		);
		await page.getByRole("button", { name: /^stop$/i }).click();
		expect((await response).status()).toBe(200);

		await expect(page.getByTestId("lifecycle-badge")).toHaveText(
			/Stopping|Stopped/,
		);
	});

	test("the reconciliation sub-status renders when the server reports it stale", async ({
		page,
	}) => {
		// Drives this through the API directly rather than a real stop-window
		// resnapshot (which needs the live-worker harness noted above): the
		// point under test is that the UI surfaces `reconciliation: stale`
		// when present, not how the server comes to set it. WS-7's stepper
		// already has its own coverage for the latter.
		const { pipelineId } = await seedPipeline("lc-stale");

		await page.route(`**/api/v1/pipelines/${pipelineId}`, async (route) => {
			const response = await route.fetch();
			const json = await response.json();
			json.reconciliation = "stale";
			await route.fulfill({ response, json });
		});

		await page.goto(`/pipelines/${pipelineId}`);
		await expect(page.getByTestId("reconciliation-badge")).toHaveText(
			"Deletes stale",
		);
	});
});
