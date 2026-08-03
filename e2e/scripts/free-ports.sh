#!/usr/bin/env bash
#
# Frees the suite's dedicated ports before a run.
#
# `reuseExistingServer` is deliberately false, so Playwright refuses to start
# if anything is already listening -- which is the right behaviour (adopting a
# foreign server is how an entire run once executed against an unrelated app),
# but it means an interrupted run leaves orphans that block the next one.
#
# This only ever touches the ports in E2E_WEB_PORT / E2E_API_PORT /
# E2E_NATS_PORT, which default to 3100 / 8090 / 4322 precisely so they do not
# collide with the conventional 3000 / 8080 / 4222 a developer runs by hand.
set -uo pipefail

WEB_PORT="${E2E_WEB_PORT:-3100}"
API_PORT="${E2E_API_PORT:-8090}"
NATS_PORT="${E2E_NATS_PORT:-4322}"

for port in "$WEB_PORT" "$API_PORT" "$NATS_PORT"; do
  pid="$(ss -ltnp 2>/dev/null | grep ":${port} " | grep -oP 'pid=\K[0-9]+' | head -1)"
  if [ -n "$pid" ]; then
    echo "e2e: freeing port ${port} (pid ${pid}, $(ps -p "$pid" -o comm= 2>/dev/null))"
    kill -9 "$pid" 2>/dev/null || true
  fi
done

docker rm -f cdc-e2e-nats >/dev/null 2>&1 || true

exit 0
