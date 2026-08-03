import { snakeToCamel } from "./mappers";
import { apiClient, unwrap } from "./schema-client";

/**
 * A worker's liveness record, from `GET /workers/{id}/heartbeat`.
 *
 * Worker IDs are pipeline IDs: the API writes heartbeats under
 * `protocol.WorkerHeartbeatKey(pipelineID)` and reads them back the same way
 * when computing a pipeline's status (see `getPipelineStatusString` in
 * internal/api/handler.go).
 *
 * The endpoint is `additionalProperties: true` in the spec, so this interface
 * is hand-written rather than derived from schema.d.ts.
 */
export interface WorkerHeartbeat {
	workerId: string;
	status: string;
	uptimeSec: number;
	updatedAt: string;
}

/**
 * How long a heartbeat may go unrefreshed before the worker is considered
 * dead. Must stay in step with the 60s threshold in `getPipelineStatusString`.
 */
export const HEARTBEAT_STALE_AFTER_MS = 60_000;

export function isHeartbeatStale(
	hb: WorkerHeartbeat,
	now: number = Date.now(),
): boolean {
	const updated = Date.parse(hb.updatedAt);
	if (Number.isNaN(updated)) return true;
	return now - updated > HEARTBEAT_STALE_AFTER_MS;
}

export const workersApi = {
	/** Returns null when the worker has no heartbeat record (HTTP 404). */
	async getHeartbeat(id: string): Promise<WorkerHeartbeat | null> {
		const result = await apiClient.GET("/workers/{id}/heartbeat", {
			params: { path: { id } },
		});
		if (result.response.status === 404) return null;
		return snakeToCamel<WorkerHeartbeat>(unwrap<unknown>(result));
	},
};
