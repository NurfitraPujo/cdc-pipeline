// Node/container entrypoint. The Cloudflare target does not use this file --
// `wrangler deploy` uses dist/server/wrangler.json instead (see BUILD_TARGET
// in vite.config.ts). Both targets build from the same source; the app itself
// has no Cloudflare-specific runtime dependencies.
//
// `vite build` emits dist/server/server.js as a fetch-style handler that does
// NOT listen on its own, so this adapter bridges it to node:http.
import { createServer } from "node:http";
import handler from "./dist/server/server.js";

const port = Number(process.env.PORT) || 3000;

const server = createServer(async (req, res) => {
	try {
		const url = `http://${req.headers.host ?? "localhost"}${req.url}`;
		const hasBody = req.method !== "GET" && req.method !== "HEAD";
		const request = new Request(url, {
			method: req.method,
			headers: req.headers,
			body: hasBody ? req : undefined,
			duplex: "half",
		});

		const response = await handler.fetch(request);
		res.writeHead(response.status, Object.fromEntries(response.headers));

		if (response.body) {
			const reader = response.body.getReader();
			for (;;) {
				const { done, value } = await reader.read();
				if (done) break;
				res.write(value);
			}
		}
		res.end();
	} catch (err) {
		console.error("request failed", err);
		if (!res.headersSent) res.writeHead(500);
		res.end("Internal Server Error");
	}
});

server.listen(port, "0.0.0.0", () => console.log(`listening on ${port}`));

// Let Kubernetes terminate the pod cleanly instead of waiting out the grace period.
for (const sig of ["SIGTERM", "SIGINT"]) {
	process.on(sig, () => server.close(() => process.exit(0)));
}
