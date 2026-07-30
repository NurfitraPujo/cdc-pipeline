# Fix Sequence 4 — SSRF Hardening + Config-Drift Encryption Key

**Scope:** Critical 14 (SSRF via DNS-rebinding TOCTOU + fail-open + unguarded discovery), the reclassified MEDIUM encryption-key config drift, and the confirmed same-surface items (isPrivateHost gaps, hardcoded `sslmode=disable`, test-endpoint error leakage + no rate limit, X-Forwarded-For rate-limit bypass, encrypted-secret echo on writes, `maskDSN` fail-open, unauthenticated `/metrics` + `/swagger`, insecure default DSNs).

**Verified against code at these exact locations** (re-read for this plan):
- `internal/api/handler.go`: `isPrivateHost` :30-56, `validateHost` :60-72, `CreateSource` :790-828, `UpdateSource` :842-893, `ListSourceTables` discovery :975-1052 (connect block :1015-1047), `ListSinks` decrypt/mask :1072-1099, `CreateSink` :1155-1194, `UpdateSink` :1207-1265, `maskDSN` :1435-1448, `reconstructDSN` :1450-1469, `TestSourceConnection` :1471-1536, `TestSinkConnection` :1538-1608.
- `internal/api/ratelimit.go`: `c.ClientIP()` :79, no trusted-proxy config anywhere.
- `internal/api/cors.go`: fixed allowlist (good, untouched).
- `internal/crypto/aes.go`: `GetEncryptionKey` :19-39 (16/24/32 raw or base64), `Encrypt` :41-67, `Decrypt` :69-101.
- `cmd/api/main.go`: `/metrics` :99, `/swagger` :101 mounted **outside** the `authorized` group; `WriteTimeout: 30s` :169.
- `cmd/pipeline/main.go`: insecure default DSN construction :218 (databend `http://root:@host:port`), :231-246 (`postgres://…?sslmode=disable`).
- `internal/source/postgres/source.go`: `sslmode=disable` hardcoded :356.
- `internal/vendor/go-pq-cdc/config/config.go`: `DSN()` :56-58 (no sslmode), `ReplicationDSN()` :62-64, `DSNWithoutSSL()` :66-68; call sites in `connector.go` :97,122,124,127,137,211,715 and `replication/stream.go` :490.
- `internal/sink/databend/sink.go`: `sql.Open("databend", dsn)` :70 (HTTP transport under the hood).
- `.env.example`, `cmd/api/.env.example`, `cmd/pipeline/.env.example`: `ENCRYPTION_KEY=my-super-secret-key-32-chars-long!` (34 bytes → rejected by crypto).
- `docker-compose.yaml` :101,125: `ENCRYPTION_KEY=${ENCRYPTION_KEY:-my-super-secret-key-32-chars-lon}` (32 bytes → accepted). **This is the drift.**
- `deploy/helm-chart/values.{staging,production}.yml` :19-20: `ENCRYPTION_KEY`/`JWT_SECRET` are real kubeseal SealedSecret blobs injected via `templates/shared-secrets.yaml`. Prod is fine.

---

## 1. Objective & threat model

### Who is authenticated
Every route that reaches the SSRF-capable code lives inside the `authorized` group behind `AuthMiddleware()` (`cmd/api/main.go` :107-157). So the attacker is **any holder of a valid JWT** — a logged-in dashboard user, or anyone who obtained a token. The system seeds a default admin (`EnsureDevAuth`, `cmd/api/main.go` :79) and JWT is HMAC with a ≥32-byte secret (good). This is **authenticated SSRF**, not pre-auth. That lowers likelihood but not impact: the CDC control plane is a high-value pivot box sitting next to Postgres, Databend, NATS, and (in cloud) the instance metadata endpoint.

### What an authenticated caller can reach today
An attacker supplies an arbitrary `host`/`port` (source) or `dsn` (sink) and the API server **dials it from inside the cluster**:

| Path | Entry | Validation | Re-resolve gap | TLS | Error leak |
|---|---|---|---|---|---|
| Source test | `TestSourceConnection` :1480 | `validateHost(cfg.Host)` :1480 | **YES** — validate resolves once via `net.LookupIP` (:61), then `sql.Open`+`PingContext` (:1520,1530) re-resolve the same hostname independently → DNS rebinding | `sslmode=disable` :1515 | **YES** raw `%v` :1522,1531 |
| Sink test | `TestSinkConnection` :1579 | `validateHost(host)` :1579 | **YES** same TOCTOU; databend/postgres_debug both dial :1585 | DSN-controlled | **YES** raw `%v` :1587,1596 |
| **Table discovery** | `ListSourceTables` :1015-1047 | **NONE** — no `validateHost` call at all | N/A (unguarded outright) | `sslmode=disable` :1020 | No (silently swallowed) but timing/behavior oracle |
| Pipeline source connect | `source.go` :351-362 | none (config already trusted) | n/a | `sslmode=disable` :356 | n/a |

**Concrete attack, DNS rebinding (the TOCTOU):**
1. Attacker controls `evil.attacker.com`, a DNS record with a very low TTL.
2. Calls `POST /api/v1/sources/test` with `host=evil.attacker.com`. First resolution (in `validateHost` :61) returns a public IP → passes `isPrivateHost`.
3. Between that lookup and `sql.Open`/`PingContext` (:1520/1530), the attacker flips the DNS record to `169.254.169.254` (cloud metadata) or `10.0.0.5` (internal Postgres).
4. `pgx` re-resolves the hostname at dial time and connects to the **internal** address. Validation is bypassed.

**Attack, no rebinding needed (`ListSourceTables`):** create a source with `host=169.254.169.254` (or any internal host), then `GET /api/v1/sources/{id}/tables`. The discovery block at :1015 builds `postgres://…@169.254.169.254:port/…?sslmode=disable` and dials it with **zero** host validation. Even simpler than rebinding.

**Fail-open amplifier:** `validateHost` returns `""` (allowed) on **any** DNS error (:62-64). An attacker can force a lookup error (e.g. a name that NXDOMASHes on the resolver used by `net.LookupIP` but resolves differently inside `pgx`'s resolver, or transient SERVFAIL) and be waved through.

**Oracle amplifier:** the test endpoints return the raw backend error (`Database connection failed: %v`, :1531; `Connection failed: %v`, :1596). Distinct errors for "connection refused" vs "no route" vs "TLS handshake" vs "auth failed" turn the endpoint into an **internal port/host scanner** — the caller learns what is listening on which internal IP:port. No rate limit compounds this into a fast scanner.

**IP blocklist gaps (widen the target set even after adding validation to all paths):** `isPrivateHost` :30-56 misses:
- `fc00::/7` (IPv6 unique-local) — confirmed API-3.
- `::` (IPv6 unspecified) and `::1` handled only partially (`IsLoopback` covers `::1`; `::` is not).
- IPv4-mapped IPv6 `::ffff:10.0.0.1` — `cidr.Contains` on the v4 blocks won't match the 16-byte mapped form unless normalized (N17).
- `fe80::/10` link-local IPv6 — `IsLinkLocalUnicast` covers unicast, but `IsLinkLocalMulticast`/`ff02::` not covered.
- `0.0.0.0/8` "this network" / unspecified v4.
- `192.0.2.0/24`, `198.18.0.0/15`, `198.51.100.0/24`, `203.0.113.0/24`, `240.0.0.0/4` reserved/benchmark ranges (defense-in-depth).

### Encryption-key drift (MEDIUM, not a breach)
`GetEncryptionKey` (`aes.go` :19-39) accepts only raw 16/24/32 bytes or base64 decoding to those. The example value `my-super-secret-key-32-chars-long!` is **34 bytes** and contains `!` (base64 decode fails) → **rejected**. So `cp .env.example .env` (documented flow) produces a key that **fails API+worker startup** (`GetEncryptionKey` errors bubble up: `CreateSource` :803-806, bootstrap `cmd/pipeline/main.go` :261-267). Meanwhile `docker-compose.yaml` uses a **different** inline default `my-super-secret-key-32-chars-lon` (32 bytes, no `!`) that **works**. Two example sources disagree — a config/dev-experience trap, not exposure. Prod helm injects real sealed secrets, so prod is unaffected.

### Out of scope for this sequence (owned elsewhere)
CAS on config writes (sequence for data-integrity), CORS (already good), JWT/AES-GCM correctness (already good), SSE WriteTimeout N2 (operational sequence). We touch `/metrics` + `/swagger` gating here because it is a pure auth-surface item on the same file.

---

## 2. Target design per item

### 2.1 SSRF defense that closes the TOCTOU (resolve-once, validate-all, pin-the-IP)

The only correct fix is to **remove the second, independent DNS resolution**. Do NOT resolve-then-connect-by-hostname. Instead: resolve once, validate every returned IP, and **connect to a validated IP directly**, re-checking the IP at connect time so a rebinding cannot slip an unvalidated address through. Use a custom `net.Dialer.Control` hook — it runs **after DNS resolution, immediately before `connect(2)`**, receives the concrete resolved IP, and can reject it. This is the standard anti-SSRF pattern and it works uniformly across `database/sql`/`pgx` and the databend HTTP driver because both ultimately dial through a `net.Dialer`.

Create a new package `internal/netsafe` (single source of truth; both `internal/api` and `internal/source/postgres` and the databend sink import it):

```go
// internal/netsafe/guard.go
package netsafe

import (
	"context"
	"fmt"
	"net"
	"syscall"
)

// blockedNets is the complete denylist. Parsed once at init.
var blockedNets []*net.IPNet

func init() {
	for _, c := range []string{
		// IPv4
		"0.0.0.0/8",        // "this" network / unspecified
		"10.0.0.0/8",       // RFC1918
		"127.0.0.0/8",      // loopback
		"169.254.0.0/16",   // link-local incl. 169.254.169.254 cloud metadata
		"172.16.0.0/12",    // RFC1918
		"192.0.0.0/24",     // IETF protocol assignments
		"192.0.2.0/24",     // TEST-NET-1
		"192.168.0.0/16",   // RFC1918
		"198.18.0.0/15",    // benchmarking
		"198.51.100.0/24",  // TEST-NET-2
		"203.0.113.0/24",   // TEST-NET-3
		"100.64.0.0/10",    // CGNAT
		"240.0.0.0/4",      // reserved (incl. 255.255.255.255 broadcast)
		// IPv6
		"::/128",           // unspecified ::
		"::1/128",          // loopback
		"fc00::/7",         // unique-local
		"fe80::/10",        // link-local unicast
		"ff00::/8",         // multicast
		"::ffff:0:0/96",    // IPv4-mapped (matched after normalization below too)
		"64:ff9b::/96",     // NAT64 (maps to public but can wrap private; block conservatively)
		"2001:db8::/32",    // documentation
	} {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic("netsafe: bad CIDR " + c) // compile-time-constant list; never fails
		}
		blockedNets = append(blockedNets, n)
	}
}

// IsBlockedIP reports whether ip is in a private/reserved/loopback/link-local range.
// Normalizes IPv4-mapped IPv6 to the v4 form so ::ffff:10.0.0.1 is caught by 10.0.0.0/8.
func IsBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true // fail closed
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4 // normalize mapped/compat forms
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast() || ip.IsInterfaceLocalMulticast() {
		return true
	}
	for _, n := range blockedNets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// Control is a net.Dialer.Control hook. It runs AFTER DNS resolution and BEFORE
// connect(2), receiving the concrete address the socket is about to dial. This is
// what closes the TOCTOU: the IP checked here is exactly the IP connected to.
func Control(network, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("netsafe: cannot parse dial address %q: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// address should already be an IP at Control time; if not, fail closed.
		return fmt.Errorf("netsafe: non-IP dial target %q blocked", host)
	}
	if IsBlockedIP(ip) {
		return fmt.Errorf("netsafe: connection to private/reserved address %s blocked", ip)
	}
	return nil
}

// Dialer returns a *net.Dialer whose every connect is guarded by Control.
func Dialer(timeout time.Duration) *net.Dialer {
	return &net.Dialer{Timeout: timeout, Control: Control}
}
```

**Why `Control` and not a pre-resolve check:** `Control` is invoked by the Go net stack on the *actual* resolved address for *every* connection the driver opens (including reconnects and connection-pool refills), so there is no window between validation and connect. Even if the driver re-resolves a rebinding hostname, the fresh IP is re-checked here. This is strictly stronger than "resolve once, pass the IP as the connect target," and it needs no driver-specific IP-pinning plumbing.

#### Wiring per path

**(a) Postgres source test (`TestSourceConnection`) and pipeline source connect (`source.go` :351-362) and discovery (`ListSourceTables` :1015-1047):** these use `sql.Open("pgx", dsn)`. `pgx/stdlib` supports a custom dialer via a registered connector config. Replace `sql.Open` with a pgx connector that injects the guarded dialer:

```go
import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func openGuardedPgx(dsn string, connectTimeout time.Duration) (*sql.DB, error) {
	pgxCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	d := netsafe.Dialer(connectTimeout)
	pgxCfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return d.DialContext(ctx, network, addr)
	}
	return sql.OpenDB(stdlib.GetConnector(pgxCfg)), nil
}
```

`pgx.ConnConfig.DialFunc` receives the already-resolved `addr`? No — pgx resolves inside its own lookup and calls `DialFunc` with the host:port; the `net.Dialer` we hand it then resolves and invokes `Control` on the concrete IP. Because we route pgx's dial through **our** guarded `net.Dialer`, the resolution that actually feeds `connect(2)` is the guarded one. (pgx's `BuildFrontend` uses `ConnConfig.DialFunc` for the socket; the `LookupFunc`/`DialFunc` split means the final dial goes through our dialer's Control.) Set `pgxCfg.LookupFunc = nil` to let the dialer own resolution, ensuring a single resolve+connect+check pass.

Apply `openGuardedPgx` at **all three** pgx call sites:
- `handler.go` :1520 (`TestSourceConnection`)
- `handler.go` :1024 (`ListSourceTables` discovery) — **and add the missing `validateHost`/pre-check too** (defense in depth; the Control hook is the real gate).
- `source.go` :362 (pipeline runtime connect).
- Vendored `go-pq-cdc` also dials via `pq.NewConnection(cfg.DSN())` (`connector.go` :97,122,...). The runtime replication path is *inside the vendored lib*. Guarding it requires either (i) a vendored patch to route `pq.NewConnection`/`pgconn` through a guarded dialer, or (ii) accepting that the source host is operator-configured (KV, admin-only) and therefore lower-risk than the user-supplied test/discovery paths. **Decision:** the API-exposed, user-driven paths (test + discovery) are the priority and get the dialer. For the vendored replication dial, add the guard by setting the `pgconn` dialer in the vendored `pq.NewConnection` (see work item 3.4) OR document it as accepted residual risk since source host comes from admin-only KV writes. Recommend the vendored patch for completeness.

**(b) Sink test (`TestSinkConnection` :1585) — postgres_debug:** same `openGuardedPgx` (driver is `postgres`/`pgx`).

**(c) Sink test — databend (`sql.Open("databend", cfg.DSN)` :1585) and runtime databend sink (`sink.go` :70):** `databend-go` speaks HTTP. It exposes a DSN and uses `net/http` internally. Two options:
- **Preferred:** databend-go's driver builds its `http.Client` from the DSN; if the driver version supports a custom transport/dialer hook, inject `&http.Transport{DialContext: netsafe.Dialer(timeout).DialContext}`. Check the vendored databend-go API for a `RegisterTransport`/`SetTransport` or `sql.OpenDB` connector. If it does **not** expose one, fall back to:
- **Fallback (pre-flight + host allow):** before `sql.Open("databend", …)`, parse the DSN host, resolve it **once** with a guarded resolver, and if it resolves to a blocked IP reject. This re-introduces a small TOCTOU for databend only, but the impact is bounded because the databend driver is HTTP (no replication, no metadata-endpoint credential theft via psql protocol). Prefer the transport injection; document the residual if the driver can't take one.

Because the databend guard depends on the driver's extensibility, **spike the databend-go transport API first** (open question in §6).

### 2.2 `validateHost` FAIL CLOSED + consolidated blocklist

Replace `validateHost` (`handler.go` :60-72) and `isPrivateHost` (:30-56) with thin wrappers over `netsafe`:

```go
func validateHost(host string) string {
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Sprintf("host %s could not be resolved: %v", host, err) // FAIL CLOSED
	}
	if len(ips) == 0 {
		return fmt.Sprintf("host %s resolved to no addresses", host)
	}
	for _, ip := range ips {
		if netsafe.IsBlockedIP(ip) {
			return fmt.Sprintf("host %s resolves to a private/reserved address", host) // no IP leak
		}
	}
	return ""
}
```

Key changes: (1) DNS error is now **rejected** not allowed; (2) empty result rejected; (3) the returned message no longer echoes the resolved private IP (avoids confirming internal topology). `validateHost` remains a cheap pre-check for a fast, clear error message — the authoritative gate is the `Control` dialer hook, which is what actually prevents the connection. Delete the old `isPrivateHost` and `privateBlocks` slice entirely; `netsafe.IsBlockedIP` is the single implementation.

### 2.3 TLS: stop hardcoding `sslmode=disable`

Make sslmode configurable, default to a secure mode, keep a documented escape hatch.

- Add `SSLMode string` to `protocol.SourceConfig` (`config.go` :219-236, tag `msg:"sslmode" yaml:"ssl_mode" json:"ssl_mode"`), validated against pgx's set (`disable|allow|prefer|require|verify-ca|verify-full`), **defaulting to `require`** when empty. Optionally `SSLRootCert string` for `verify-full`.
- `handler.go` :1515 (`TestSourceConnection`) and :1020 (`ListSourceTables`): replace the hardcoded `q.Set("sslmode", "disable")` with `q.Set("sslmode", effectiveSSLMode(cfg.SSLMode))` where `effectiveSSLMode` returns `require` on empty.
- `source.go` :356: same substitution using `srcConfig.SSLMode`.
- Vendored `go-pq-cdc/config/config.go`: the CDC path calls `DSN()` (:56, **no sslmode param at all** → libpq default `prefer`) and `DSNWithoutSSL()` (:66). The active runtime uses `DSN()` (connector.go), which is already not `disable`. Do **not** switch the vendored default silently; instead thread the chosen sslmode from `srcConfig.SSLMode` into the vendored `config.Config` (add an `SSLMode` field to the vendored `Config` and have `DSN()` append `?sslmode=<mode>`). If patching vendor is undesirable, at minimum ensure the API/test/discovery/source-connect paths we own default to `require`.
- Sink DSNs (databend/postgres_debug) carry their own sslmode in the DSN string — document that operators should use `sslmode=require` and TLS endpoints; add a validation warning (not hard reject) when a sink DSN contains `sslmode=disable`.
- **Databend:** `http://` is plaintext. Recommend `https://` in examples and a startup warning when a databend DSN is `http://` to a non-loopback host.

### 2.4 Error hygiene + rate limiting + trusted proxies on test endpoints

**Error hygiene:** replace the four raw-error returns with a generic message and log the detail server-side:
- `handler.go` :1522, :1531 (`TestSourceConnection`), :1587, :1596 (`TestSinkConnection`).

```go
if err := db.PingContext(ctx); err != nil {
	log.Warn().Err(err).Str("host", cfg.Host).Msg("source connection test failed")
	c.JSON(http.StatusBadGateway, gin.H{"error": "connection test failed"})
	return
}
```

Return a single opaque message for all failure modes (open/ping/resolve) so the endpoint stops being a scan oracle. Use `502` for backend-reach failures, `400` only for malformed input.

**Rate limiting on test/discovery endpoints:** the connection-test and discovery endpoints are the SSRF surface and currently have **no** limiter (only `/login` :105 does). Add `RateLimitMiddleware()` (or a stricter dedicated limiter, e.g. 5 req/s burst 5) to:
- `POST /sources/test` (:140)
- `POST /sinks/test` (:150)
- `GET /sources/:id/tables` (:139)

Register a second, tighter `rateLimiters` instance for these (they are expensive + security-sensitive), separate from the 10/20 default.

**Trusted proxies (fixes both the limiter key and `ClientIP`):** Gin trusts all proxies by default, so `c.ClientIP()` (`ratelimit.go` :79) honors a spoofed `X-Forwarded-For`, letting an attacker mint unlimited distinct keys and defeat the `/login` limiter. In `cmd/api/main.go` after `r := gin.New()` (:84), call:

```go
trusted := strings.Split(os.Getenv("TRUSTED_PROXY_CIDRS"), ",") // e.g. the ingress/pod CIDR
if len(trusted) == 1 && trusted[0] == "" {
	// Default: trust nobody → ClientIP returns the direct RemoteAddr (safe).
	_ = r.SetTrustedProxies(nil)
} else {
	if err := r.SetTrustedProxies(trusted); err != nil {
		log.Fatal().Err(err).Msg("invalid TRUSTED_PROXY_CIDRS")
	}
}
```

With `SetTrustedProxies(nil)`, `c.ClientIP()` uses `RemoteAddr` and ignores `X-Forwarded-For` — the spoof is dead. In prod, set `TRUSTED_PROXY_CIDRS` to the real ingress/pod CIDR so real client IPs are honored for logging/limiting. Document this as a required prod env var.

### 2.5 Secret hygiene

**Don't echo encrypted blobs on write paths.** After encrypting, the handlers return `cfg` with the ciphertext in `PassEncrypted`/`DSN`:
- `CreateSource` :828 returns `cfg` with encrypted `PassEncrypted`.
- `UpdateSource` :892 same.
- `CreateSink` :1193 returns `cfg` with encrypted `DSN`.
- `UpdateSink` :1264 same.

Fix: before the final `c.JSON`, blank the secret field (or set to `maskDSN`/`"***"`). Never return ciphertext or plaintext to the client. E.g. `cfg.PassEncrypted = ""` / `cfg.DSN = maskDSN(decryptedForDisplay)` on the response copy (don't mutate what was persisted). Exposing the ciphertext is low-severity (AES-GCM), but it leaks nonce+tag structure and is needless.

**`maskDSN` fail-closed.** `maskDSN` (:1435-1448) returns the raw DSN unchanged when `url.Parse` fails (:1437-1438) or when there is no userinfo — meaning a malformed-but-secret-bearing DSN leaks verbatim through `ListSinks` (:1091). Fix:

```go
func maskDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "***" // FAIL CLOSED: never echo an unparseable DSN
	}
	if u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(u.User.Username(), "***")
		}
	}
	// also strip password-bearing query params if any driver uses them
	return u.Redacted() // Go's url.URL.Redacted() masks userinfo password
}
```

Prefer `u.Redacted()` which is purpose-built. Keep `reconstructDSN` (:1450) working with the `***` sentinel (it already keys off `pass == "***"`).

**Gate `/metrics` and `/swagger`.** Both are mounted outside `authorized` (`main.go` :99,101).
- `/metrics`: keep it reachable for Prometheus but restrict — either bind it to a **separate internal listener/port** not exposed by the ingress, or require a bearer/basic token via a lightweight middleware. Simplest infra-aligned fix: serve `/metrics` on the health port (8081-style) that is cluster-internal only, not on the public API port. If it must stay on `r`, wrap with an auth check (a scrape token env var).
- `/swagger`: gate behind `ENV != production` (only register the route when `isDev`), or behind `AuthMiddleware`. It exposes the full API surface + example payloads; no reason to serve it publicly in prod.

### 2.6 Encryption-key config drift + insecure defaults

**Fix the three `.env.example` files** to a value that (a) is a valid 32-byte base64 key so `cp .env.example .env` actually boots, and (b) is unmistakably not-for-production:

```
# Generate your own with: openssl rand -base64 32
# This example value is for LOCAL DEV ONLY — DO NOT USE IN PRODUCTION.
ENCRYPTION_KEY=ZXhhbXBsZS1kZXYta2V5LW5vdC1mb3ItcHJvZDEyMzQ1Ng==
JWT_SECRET=example-dev-jwt-secret-not-for-prod-change-me!!
```

(The `ENCRYPTION_KEY` above is base64 that decodes to 32 bytes → accepted by `GetEncryptionKey` :26-28. `JWT_SECRET` ≥32 chars → passes `validateJWTSecret` :37.) Files: `.env.example`, `cmd/api/.env.example`, `cmd/pipeline/.env.example` — all must use the **same** value so API and worker can decrypt each other's secrets.

**Remove the docker-compose drift.** `docker-compose.yaml` :101,125 inline default `my-super-secret-key-32-chars-lon` must become the **same** base64 example (or better, drop the inline default so compose reads from `.env`, forcing consistency):

```yaml
- ENCRYPTION_KEY=${ENCRYPTION_KEY:?set ENCRYPTION_KEY (openssl rand -base64 32)}
- JWT_SECRET=${JWT_SECRET:?set JWT_SECRET (openssl rand -base64 48)}
```

The `:?` form fails fast with a helpful message instead of silently using a weak shared default — best for a security-sensitive value. If keeping a dev default is desired for zero-config `docker compose up`, make it byte-identical to the `.env.example` base64 value.

**Document key generation** in README / a `docs/security.md`: `openssl rand -base64 32` for `ENCRYPTION_KEY`, `openssl rand -base64 48` for `JWT_SECRET`; both must be identical across API and every worker.

**Verify helm injects a real key (it does).** `deploy/helm-chart/values.{staging,production}.yml` :19-20 carry real kubeseal SealedSecret blobs, rendered by `templates/shared-secrets.yaml`. No change needed beyond confirming the decrypted value is a valid 32-byte key and matches across api/worker components (they share `shared-secrets`). Add a note to the rollout checklist to rotate these if the weak dev key ever touched prod.

**Remove insecure default DSNs in `cmd/pipeline/main.go`** :218 and :231-246. These synthesize `http://root:@host:port` (databend, no auth) and `postgres://postgres:postgres@…?sslmode=disable` from partial env vars with hardcoded fallback creds (`postgres`/`postgres`, user `postgres`, db `debug_db`). Change to: require an explicit `DATABEND_DSN` / `POSTGRES_DEBUG_DSN` and **fail (or skip seeding that sink) if absent**, rather than fabricating a plaintext, weak-cred, sslmode-disabled DSN. At minimum, when synthesizing, default sslmode to `require` and do not default the password.

---

## 3. Ordered work items

> Dependency spine: **W1 (netsafe pkg)** → W2 (validateHost) → W3 (dialer wiring per path) → W4 (TLS config) run in parallel-ish after W1; W5/W6 (rate-limit/proxies/errors) independent; W7 (secret hygiene) independent; W8 (key/env/compose) independent; W9 (routes gating) independent; W10 (default DSNs) independent. Tests (W11) last.

**W1 — Create `internal/netsafe/guard.go`** (new file). The complete blocklist, `IsBlockedIP`, `Control`, `Dialer` from §2.1. No other file depends on API. *Foundation for everything else.*

**W2 — Rewrite `validateHost` + delete `isPrivateHost`** (`handler.go` :30-72). Fail closed on DNS error/empty; delegate IP check to `netsafe.IsBlockedIP`; stop echoing the resolved IP. *Depends on W1.*

**W3 — Route every outbound dial through the guarded dialer.**
- W3.1 `TestSourceConnection` (`handler.go` :1520): `openGuardedPgx` helper; also switch sslmode (W4). *Depends W1.*
- W3.2 `ListSourceTables` discovery (`handler.go` :1024): `openGuardedPgx` **and** add `validateHost(cfg.Host)` pre-check (currently absent). *Depends W1,W2.*
- W3.3 `TestSinkConnection` (`handler.go` :1585): `openGuardedPgx` for postgres_debug; databend transport injection or documented pre-flight (see §2.1c). *Depends W1; databend spike (§6).*
- W3.4 Pipeline source connect (`source.go` :362): `openGuardedPgx`. For the vendored replication dial (`go-pq-cdc/connector.go` `pq.NewConnection`), either patch the vendored `pq` connection to accept a guarded dialer, or record accepted residual risk (host is admin-only KV). *Depends W1.*
- W3.5 Databend runtime sink (`sink.go` :70): apply the same transport guard chosen in W3.3. *Depends W3.3 decision.*

**W4 — TLS configurable, default secure** (§2.3). Add `SSLMode`(+`SSLRootCert`) to `protocol.SourceConfig`; `effectiveSSLMode` helper; replace hardcoded `sslmode=disable` at `handler.go` :1020,1515 and `source.go` :356; thread sslmode into vendored `config.Config.DSN()` (or accept vendored default `prefer`). Sink DSN sslmode=disable warning. *Depends on protocol change; coordinate with any migration (§5).*

**W5 — Trusted proxies + limiter key** (`cmd/api/main.go` after :84): `SetTrustedProxies` from `TRUSTED_PROXY_CIDRS`, default nil. *Independent.*

**W6 — Rate-limit + error hygiene on test/discovery** (`main.go` :139,140,150 add middleware; `handler.go` :1522,1531,1587,1596 opaque errors + server-side log). Add a tighter limiter instance in `ratelimit.go`. *Depends W5 for trustworthy keys.*

**W7 — Secret hygiene** (`handler.go`): blank/mask secret on responses at :828,892,1193,1264; `maskDSN` fail-closed via `url.Redacted()` at :1435. *Independent.*

**W8 — Key/env/compose drift** (§2.6): fix `.env.example` × 3; `docker-compose.yaml` :101,102,125 use `:?` or identical base64; document `openssl rand`. *Independent, low-risk.*

**W9 — Gate `/metrics` + `/swagger`** (`main.go` :99,101): move `/metrics` to internal port or token-gate; register `/swagger` only when `isDev`. *Independent.*

**W10 — Remove insecure default DSNs** (`cmd/pipeline/main.go` :218,231-246): require explicit DSNs or fail/skip; never fabricate weak-cred plaintext sslmode-disable DSNs; default sslmode `require` if synthesizing. *Independent.*

**W11 — Tests** (§4) for netsafe, validateHost, rate-limit/XFF, secret masking. *After W1–W10.*

---

## 4. Test plan

### 4.1 `internal/netsafe` unit tests (`guard_test.go`)
- **Private-IP matrix for `IsBlockedIP`** — table test, each must return `true`:
  - IPv4: `127.0.0.1`, `10.1.2.3`, `172.16.0.1`, `172.31.255.255`, `192.168.1.1`, `169.254.169.254` (metadata), `100.64.0.1` (CGNAT), `0.0.0.0`, `192.0.2.5`, `198.18.0.1`, `203.0.113.9`, `255.255.255.255`, `240.0.0.1`.
  - IPv6: `::1`, `::`, `fc00::1`, `fd12:3456::1` (fc00::/7), `fe80::1`, `ff02::1`, `2001:db8::1`.
  - IPv4-mapped IPv6: `::ffff:10.0.0.1`, `::ffff:169.254.169.254`, `::ffff:127.0.0.1` — **critical regression guard for N17**; must be `true` (normalization path).
  - `nil` IP → `true` (fail closed).
- **Public IPs must return `false`:** `8.8.8.8`, `1.1.1.1`, `93.184.216.34`, `2606:2800:220:1::` (a public v6). Guards against over-blocking `240.0.0.0/4` false positives etc.
- **`Control` hook:** call `Control("tcp", "169.254.169.254:80", nil)` → error; `Control("tcp", "8.8.8.8:443", nil)` → nil; `Control("tcp", "garbage", nil)` → error (fail closed on unparseable); `Control("tcp", "example.com:80", nil)` → error (non-IP target rejected).

### 4.2 SSRF / rebinding simulation (`handler_ssrf_test.go`)
- **DNS rebinding via a swappable resolver.** Inject a fake resolver (or use `httptest`+a stub `net.Resolver` via `netsafe` accepting a `Resolver` seam) that returns a **public** IP on the first lookup and a **private** IP (`127.0.0.1` pointing at a local `httptest`/listener) on the second. Drive `TestSourceConnection`. Assert: even though `validateHost`'s pre-check passes on the public first answer, the **dial is blocked by `Control`** (the connection never reaches the private listener). Verify by asserting the private listener recorded **zero** accepted connections and the response is the opaque `502 connection test failed`.
- **`ListSourceTables` unguarded-path regression.** Seed a source with `host=127.0.0.1` (or a test listener bound to loopback). `GET /sources/:id/tables`. Assert the discovery dial is blocked (no connection to the loopback listener), response contains no table leak. This is the "no rebinding needed" case.
- **Fail-closed on DNS error.** Resolver returns `err`. Assert `validateHost` returns a non-empty rejection and the handler responds 4xx **without** dialing (contrast: old code returned `""` = allowed).
- **Error-oracle removal.** Two requests, one to a refused port and one to an unroutable host, must return **byte-identical** opaque error bodies (no distinguishing `connection refused` vs `no route`).

### 4.3 Rate-limit / XFF (`ratelimit_test.go`)
- With `SetTrustedProxies(nil)`: send N+burst requests to `/login` all with varying `X-Forwarded-For` headers but the **same** `RemoteAddr`. Assert the limiter counts them as **one** client and returns 429 after the burst — proving the XFF spoof no longer mints fresh buckets.
- With `SetTrustedProxies([ingressCIDR])` and `RemoteAddr` inside that CIDR: assert `XFF` **is** honored (distinct client IPs get distinct buckets) — proving real proxying still works.
- Test-endpoint limiter: hammer `/sources/test` past its (tighter) burst → 429.

### 4.4 Secret hygiene (`handler_secret_test.go`)
- `CreateSource`/`UpdateSource`: POST a plaintext password, assert the **JSON response** contains no ciphertext and no plaintext in `pass` (field blanked/masked), while the KV entry **does** contain ciphertext (encrypted at rest).
- `CreateSink`/`UpdateSink`: same for `DSN`; response DSN is masked (`***`), KV encrypted.
- `maskDSN`: table test — valid DSN with password → password `***`; **unparseable** DSN → returns `"***"` (fail closed, regression for :1437); DSN with no userinfo → unchanged but no secret present; DSN with password in query param (if any) → redacted via `Redacted()`.
- `ListSinks`: seed an encrypted sink, GET, assert response DSN is masked and decrypt-failure path (bad key) returns a masked/omitted value, never the ciphertext.

### 4.5 Config / boot tests
- `GetEncryptionKey` with the new base64 `.env.example` value → returns 32 bytes, no error (guards the drift fix; a CI check that `cp .env.example .env && start` boots).
- A test asserting all three `.env.example` files and the docker-compose default (if any) contain the **same** `ENCRYPTION_KEY` string (drift regression).
- `TestSourceConnection`/source connect default to `sslmode=require` when `SSLMode` empty (assert the constructed DSN query).

---

## 5. Rollout / migration

- **`sslmode` default change is the one breaking change.** Existing deployments connecting to Postgres with no TLS will **break** when the default flips from `disable` to `require`. Mitigation:
  1. Ship `SSLMode` as an explicit config field first; **default to `require`** but honor an explicit `disable`.
  2. For existing stored sources (KV entries lacking `ssl_mode`), a one-time migration/back-compat: treat empty as `require` **only** behind a env flag `PG_SSLMODE_DEFAULT` (default `require`, operators can set `disable` for a grace period). Document that operators must either enable TLS on their Postgres or set the source's `ssl_mode: disable` explicitly (making the insecurity a conscious, auditable choice rather than a silent default).
  3. Communicate in release notes: "connection tests and pipelines now require TLS by default; set `ssl_mode: disable` per source to opt out."
- **Encryption-key `.env.example` change is safe** — it only affects fresh local setups (makes `cp .env.example .env` work). No running system reads `.env.example`. Prod (helm sealed secrets) untouched. The docker-compose `:?` change **will** break `docker compose up` for anyone relying on the old inline default until they set the var — call this out; if that friction is unacceptable, keep an inline default but make it the byte-identical valid base64 value.
- **Trusted-proxy config needs the real CIDR.** Default `SetTrustedProxies(nil)` is safe (ignores XFF) but in prod behind an ingress the logged/limited client IP becomes the ingress IP unless `TRUSTED_PROXY_CIDRS` is set to the ingress/pod CIDR. Add `TRUSTED_PROXY_CIDRS` to helm values and `.env.example` with guidance; wrong CIDR degrades limiter accuracy but is not a security regression.
- **`/metrics` relocation** to an internal port must be coordinated with the Prometheus scrape config / ServiceMonitor (helm). If moved to the health port, update the scrape target. **Do not** land the gating without updating the scrape config or metrics silently disappear.
- **Insecure-default-DSN removal** may break dev/CI seeding that relied on the fabricated defaults. Provide the explicit `DATABEND_DSN`/`POSTGRES_DEBUG_DSN` in the dev `.env.example` (already partly present) so local flows keep working.
- **Vendored patches** (`go-pq-cdc`): if we patch the vendored dialer/sslmode, note it in a `VENDOR_PATCHES.md` so a future `go mod vendor` refresh doesn't silently revert the SSRF/TLS hardening on the replication path.

---

## 6. Risks, open questions, sequencing

**Open questions (resolve before/inside implementation):**
1. **databend-go transport hook.** Does the vendored `datafuselabs/databend-go` expose a custom `http.Transport`/`DialContext` (via `sql.OpenDB` connector or a `RegisterTransport`)? If yes → clean `Control`-guarded dial (§2.1c preferred). If no → accept the bounded pre-flight-resolve fallback for databend only and document the residual TOCTOU. **Spike this first**; it gates W3.3/W3.5.
2. **pgx DialFunc vs LookupFunc semantics.** Confirm that setting `ConnConfig.DialFunc` to a `net.Dialer.DialContext` (with `Control`) and leaving `LookupFunc` nil routes the *actual* connect through our guarded dialer's resolution (single resolve+check). Verify against the vendored pgx/v5 version (note pgx v5.6.0 also has CVE GO-2026-5004, N5 — bump while here).
3. **Vendored replication dial.** Decide: patch `go-pq-cdc pq.NewConnection` to accept a guarded dialer (full coverage) vs accept residual risk (admin-only source host). Recommend the patch for defense-in-depth but it is lower priority than the user-facing test/discovery paths.
4. **`/metrics` gating approach** — internal port vs scrape-token — depends on the Prometheus deployment model (ServiceMonitor? annotation scrape?). Coordinate with infra sequence.

**Risks:**
- **Over-blocking** legitimate targets: `240.0.0.0/4` and NAT64 `64:ff9b::/96` are conservative; if a real deployment must reach an address in a benchmark/reserved range (rare), make the blocklist overridable via an allow-CIDR env for advanced operators (default empty). Tests in §4.1 pin the intended set.
- **`Control` runs on every pool dial** — negligible overhead (a few CIDR `Contains` checks), but ensure it's allocation-light (blocklist parsed once at `init`).
- **sslmode default flip** is the highest-friction change; the env-flag grace period (§5) contains it.
- **Vendor drift** silently reverting patches — mitigated by `VENDOR_PATCHES.md` and a CI check.

**Suggested internal sequencing:** W1 → W2 → (W3.1, W3.2, W3.4 in parallel) → databend spike → W3.3/W3.5 → W4 → W5 → W6 → W7 → W8 → W9 → W10 → W11 tests. W7/W8/W9/W10 can land independently at any point (no dependency on the dialer work) and are the lowest-risk quick wins — W8 (key drift) especially can go first since it unblocks anyone whose local setup is currently broken by `cp .env.example .env`.
