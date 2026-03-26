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
