#!/usr/bin/env bash
#
# Boots an isolated API stack for the Playwright suite.
#
# The suite used to assume a CDC API was already listening on :8080 and that
# the web app owned :3000. Neither was enforced, and the failure mode was
# silent: Playwright's `reuseExistingServer` health check saw *some* server on
# :3000, skipped launching the real one, and every test then ran against an
# unrelated app. See summaries/frontend_control_plane_audit.md §5.
#
# Everything here runs on dedicated ports so a developer's own stack on the
# default ports is never touched or overwritten.
set -euo pipefail

# Must NOT be `cdc-e2e-nats`: that name belongs to the Makefile's `e2e-up` /
# `e2e-down` targets, which the pre-push hook uses to provision NATS on 4222
# and Postgres on 5432. Sharing the name meant this script's cleanup did
# `rm -f cdc-e2e-nats` and destroyed the hook's container out from under its
# own API server.
NATS_CONTAINER="cdc-playwright-nats"
NATS_PORT="${E2E_NATS_PORT:-4322}"
API_PORT="${E2E_API_PORT:-8090}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

API_PID=""

cleanup() {
  if [ -n "$API_PID" ]; then
    kill "$API_PID" 2>/dev/null || true
    wait "$API_PID" 2>/dev/null || true
  fi
  docker rm -f "$NATS_CONTAINER" >/dev/null 2>&1 || true
}

# Best-effort only. Playwright tears the webServer process group down hard
# enough that this trap frequently does not get to run, so the container is
# also removed by scripts/free-ports.sh, which package.json wires to both
# `pretest` and `posttest`. That pair is what actually guarantees a clean slate.
trap cleanup EXIT INT TERM

if ! command -v docker >/dev/null 2>&1; then
  echo "e2e: docker is required to run the API's NATS backend" >&2
  exit 1
fi

# A fresh JetStream node per run, so tests never inherit state from a previous
# run and never see a developer's real pipelines.
cleanup
echo "e2e: starting NATS on :${NATS_PORT}"
docker run -d --rm \
  --name "$NATS_CONTAINER" \
  -p "${NATS_PORT}:4222" \
  nats:2.10-alpine -js >/dev/null

# Wait for the client port to accept connections.
for _ in $(seq 1 60); do
  if (echo > "/dev/tcp/127.0.0.1/${NATS_PORT}") >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

if ! (echo > "/dev/tcp/127.0.0.1/${NATS_PORT}") >/dev/null 2>&1; then
  echo "e2e: NATS did not become ready on :${NATS_PORT}" >&2
  docker logs "$NATS_CONTAINER" >&2 || true
  exit 1
fi

echo "e2e: starting API on :${API_PORT}"
cd "$repo_root"

# ENV=development makes the API seed the admin/admin account that
# tests/auth.setup.ts logs in with (EnsureDevAuth in internal/api/auth.go).
export ENV=development
export DEV_ADMIN_USERNAME=admin
export DEV_ADMIN_PASSWORD=admin
export JWT_SECRET="e2e-jwt-secret-not-for-production"
# Base64 of exactly 32 bytes. crypto.GetEncryptionKey rejects anything that is
# not 16/24/32 raw bytes or base64 thereof, so a "looks about right" passphrase
# fails at runtime with a 500 on the first source or sink write. Base64 is
# unambiguous about its decoded length; a bare ASCII string is easy to get
# off by one.
export ENCRYPTION_KEY="Y2RjLWUyZS10ZXN0LWtleS0wMDAwMDAwMDAwMDAwMDA="
export NATS_URL="nats://127.0.0.1:${NATS_PORT}"
# The suite serves the dashboard on a non-default port, which is not in the
# API's built-in CORS allowlist; without this the browser reports a bare
# "Failed to fetch" on every request.
export CORS_ALLOWED_ORIGINS="http://localhost:${E2E_WEB_PORT:-3100}"
export PORT="${API_PORT}"
export LOG_LEVEL="${E2E_API_LOG_LEVEL:-info}"

# Build a binary rather than using `go run`: `go run` stays alive as a parent
# of the compiled binary, so when the harness kills the process group the child
# can survive and keep holding the API port, making the *next* run fail with
# "address already in use".
API_BIN="$(mktemp -d)/cdc-e2e-api"
go build -o "$API_BIN" ./cmd/api

# Run it as a child and wait, rather than exec'ing it. `exec` replaces this
# shell, which discards the EXIT/TERM trap above -- so the NATS container was
# never torn down and every run leaked one.
"$API_BIN" &
API_PID=$!
wait "$API_PID"
