# Maintaining Patched Dependencies

This project contains a locally patched version of `github.com/Trendyol/go-pq-cdc` located in `internal/vendor/go-pq-cdc/`. This was done to address a specific `panic` issue during connection shutdown.

## **Why a Local Vendor?**
The `vendor/` directory was removed from Git to reduce the repository size from ~13MB to <1MB. However, since we need to keep our custom fix for `go-pq-cdc`, we've isolated only that dependency into the `internal/vendor/` directory and used a Go `replace` directive in `go.mod`.

---

## **How to Update `go-pq-cdc` (Upstream)**

When you need to sync with the latest upstream patches from `Trendyol/go-pq-cdc`, follow this workflow:

### **1. Save the Current Patch**
Before overwriting the local code, generate a diff of your existing changes so you can re-apply them:
```bash
git diff HEAD -- internal/vendor/go-pq-cdc > internal/vendor/go-pq-cdc/my_patch.diff
```

### **2. Refresh the Code**
Download the latest version from the upstream repository:
```bash
# 1. Clear the old source code (keeping the go.mod we created)
# (Optionally backup the go.mod first)
rm -rf /tmp/go-pq-cdc-tmp && git clone --depth 1 https://github.com/Trendyol/go-pq-cdc /tmp/go-pq-cdc-tmp

# 2. Sync the new files into the internal vendor directory
rsync -av --exclude='.git/' /tmp/go-pq-cdc-tmp/ internal/vendor/go-pq-cdc/
```

### **3. Re-apply the Patch**
Try to apply your saved diff back onto the new code:
```bash
patch -p3 < internal/vendor/go-pq-cdc/my_patch.diff
```
*Note: If the upstream code has changed significantly, you may need to resolve merge conflicts manually.*

### **4. Verify and Tidy**
Ensure the project still builds and the modules are consistent:
```bash
go mod tidy
go test ./internal/source/postgres/...
```

---

## **Current Patches**

See `internal/vendor/go-pq-cdc/PATCHES.md` for the full catalogue of local divergences
(searchable via `// vendored-patch:` markers). Notably **T0-1** adds an opt-in
`Config.ManualCommit` mode plus a `Config.KeepaliveFunc` callback so an embedding source can
own replication-slot advancement exclusively (gating it on downstream sink acks) instead of
the library advancing the slot the instant an event is handed off, and it also always-on-fixes
a monotonic-position bug in `stream.UpdateXLogPos` (the reported LSN could regress on
keepalive/per-message interleaving). Because T0-1 is flag-guarded (default `false` = upstream
byte-for-byte) except for two small always-on bug-fix guards, it should re-apply cleanly via
the diff/rsync/patch workflow below — but re-verify all six sites in the PATCHES.md T0-1 entry
by hand after any upstream re-sync, since `stream.go` is exactly the file most likely to shift
underneath a line-based patch.

**T0-2** is a different and more invasive class of patch: it is **API-breaking**. It widens
`UpdateXLogPos` to `(ctx context.Context, lsn pq.LSN) error` across three exported interfaces
(`Connector`, `replication.Streamer`, `slot.XLogUpdater`), and bounds the standby status write
by running it on its own goroutine behind a capacity-1 semaphore. Both are required for a
correct at-least-once contract: under T0-1's `ManualCommit`, `UpdateXLogPos` is the *only* thing
that advances the replication slot, so the caller must be able to bound the write and learn
whether it succeeded — otherwise a stalled slot silently retains WAL on the source primary until
its disk fills.

Note that `SendStandbyStatusUpdate` itself is left in its upstream form with the context still
ignored. Bounding it via a socket write deadline was tried and rejected as unsafe: pgx's own
context watcher concurrently clears deadlines on the same socket (defeating it), and a deadline
firing mid-frame would leave a truncated protocol frame on a connection nothing marks as broken.
The PATCHES.md T0-2 entry documents this in full — **do not reintroduce the deadline approach.**

Consequences for re-sync: T0-2 cannot "mostly apply". Because the signatures change,
a partial re-apply **fails to compile**, which is intentional — it is loud rather than silent.
Re-apply the interface changes first, then the implementations, then the call sites listed in
the PATCHES.md T0-2 entry. This patch is a strong argument for the fork approach below.

## **Recommended Alternative: Use a Fork**

If you find yourself frequently updating this dependency, the most sustainable approach is to:
1.  **Fork** `github.com/Trendyol/go-pq-cdc` to your own GitHub account.
2.  **Commit** your fix to a branch in your fork (e.g., `fix-shutdown-panic`).
3.  **Update `go.mod`** to point to your fork:
    ```go
    replace github.com/Trendyol/go-pq-cdc => github.com/YourUsername/go-pq-cdc v1.6.8-patch
    ```
4.  **Delete** the `internal/vendor/go-pq-cdc/` directory.

Using a fork allows you to use standard Git tools (`git merge upstream/main`) to keep your patch in sync with upstream improvements without manual file copying.
