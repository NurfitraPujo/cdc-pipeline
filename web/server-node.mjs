// Node/container entrypoint. The Cloudflare target does not use this file --
// `wrangler deploy` uses dist/server/wrangler.json instead (see BUILD_TARGET
// in vite.config.ts). Both targets build from the same source; the app itself
// has no Cloudflare-specific runtime dependencies.
//
// The app is built in TanStack Start SPA mode (see vite.config.ts): `vite
// build` prerenders a route-agnostic shell at dist/client/_shell.html and the
// app renders entirely on the client. There is no server-side rendering of
// route content and no server functions, so this entrypoint is a plain static
// file server: it serves dist/client/** and falls back to the SPA shell for
// every navigation request. Rendering only on the client is what lets the
// localStorage-based auth guard (src/routes/__root.tsx) run before any
// protected content is shown -- SSR could not see localStorage and used to
// leak the dashboard shell to unauthenticated users.
import { createServer } from "node:http";
import { createReadStream } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const clientDir = path.join(__dirname, "dist", "client");
const shellPath = path.join(clientDir, "_shell.html");

// Read the SPA shell once at startup; it never changes for the life of the
// process (a new deploy ships a fresh image).
const shellHtml = await readFile(shellPath);

const MIME_TYPES = {
	".js": "text/javascript; charset=utf-8",
	".mjs": "text/javascript; charset=utf-8",
	".css": "text/css; charset=utf-8",
	".html": "text/html; charset=utf-8",
	".json": "application/json; charset=utf-8",
	".ico": "image/x-icon",
	".png": "image/png",
	".jpg": "image/jpeg",
	".jpeg": "image/jpeg",
	".svg": "image/svg+xml",
	".webp": "image/webp",
	".txt": "text/plain; charset=utf-8",
	".map": "application/json; charset=utf-8",
	".woff": "font/woff",
	".woff2": "font/woff2",
};

/**
 * Serve a file from dist/client if it exists, resolving it safely so
 * request paths can never escape clientDir via `..` traversal.
 * Returns true if the request was handled (response sent), false to let
 * the caller fall through to the SPA shell.
 */
async function serveStaticAsset(req, res, pathname) {
	if (req.method !== "GET" && req.method !== "HEAD") return false;

	const decoded = decodeURIComponent(pathname);
	const resolved = path.normalize(path.join(clientDir, decoded));
	if (!resolved.startsWith(clientDir + path.sep) && resolved !== clientDir) {
		return false;
	}

	let stats;
	try {
		stats = await stat(resolved);
	} catch {
		return false;
	}
	if (!stats.isFile()) return false;

	const ext = path.extname(resolved);
	const contentType = MIME_TYPES[ext] ?? "application/octet-stream";
	// Hashed filenames under /assets/ are content-addressed and safe to cache
	// forever; everything else (favicon, manifest, robots.txt) can change
	// across deploys without a hash bump, so keep it revalidating.
	const cacheControl = decoded.startsWith("/assets/")
		? "public, max-age=31536000, immutable"
		: "public, max-age=3600";

	res.writeHead(200, {
		"Content-Type": contentType,
		"Content-Length": stats.size,
		"Cache-Control": cacheControl,
	});

	if (req.method === "HEAD") {
		res.end();
		return true;
	}

	await new Promise((resolve, reject) => {
		const stream = createReadStream(resolved);
		stream.on("error", reject);
		stream.on("end", resolve);
		stream.pipe(res);
	});
	return true;
}

/**
 * Serve the SPA shell for a navigation request. The shell references
 * content-hashed asset URLs, so it must never be cached -- a stale shell
 * would point at asset filenames that no longer exist after a deploy.
 */
function serveShell(req, res) {
	res.writeHead(200, {
		"Content-Type": "text/html; charset=utf-8",
		"Cache-Control": "no-cache",
	});
	res.end(req.method === "HEAD" ? undefined : shellHtml);
}

const port = Number(process.env.PORT) || 3000;

const server = createServer(async (req, res) => {
	try {
		const url = `http://${req.headers.host ?? "localhost"}${req.url}`;
		const pathname = new URL(url).pathname;

		if (await serveStaticAsset(req, res, pathname)) return;

		// SPA fallback: any GET/HEAD that isn't a real file is a client route.
		if (req.method === "GET" || req.method === "HEAD") {
			serveShell(req, res);
			return;
		}

		// Nothing on this server handles non-GET requests (the app talks to a
		// separate API service directly from the browser).
		res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
		res.end("Not Found");
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
