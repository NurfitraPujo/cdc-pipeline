#!/usr/bin/env bash
#
# Docs hygiene sensor.
#
# Blocks on staleness that can be detected MECHANICALLY and unambiguously. It
# deliberately does not attempt judgement calls ("is this claim still true?",
# "does this deserve an ADR?") -- those belong to the /docs-hygiene skill,
# because a blocking check that produces false positives gets --no-verify'd and
# then nobody trusts the harness at all.
#
# Checks:
#   1. Every `path/to/file.ext:NNN` cited in Markdown points at a file that
#      exists and actually has that many lines.
#   2. Every backtick-quoted repo path cited in Markdown exists.
#   3. Every ADR cross-link resolves.
#   4. Every ADR file appears in the ADR index, and vice versa.
#   5. Generated swagger matches the source annotations (skipped if `swag` is
#      absent, since a missing tool is not evidence of staleness).
#
# Usage: scripts/check-docs-hygiene.sh [--changed-only]
set -uo pipefail

cd "$(git rev-parse --show-toplevel)" || exit 1

FAILED=0
note() { printf '  \033[31m✗\033[0m %s\n' "$1"; FAILED=1; }
ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; }
skip() { printf '  \033[33m-\033[0m %s\n' "$1"; }

# LIVING docs only -- documents that describe how the system works *now* and
# are expected to be kept true.
#
# Deliberately excluded: summaries/, docs/todos/, docs/requirements/ and
# docs/GRACEFUL_SHUTDOWN_INVESTIGATION.md. Those are point-in-time records
# (review output, planning notes, investigations). They legitimately cite files
# that were proposed but never built, or that have since moved -- rewriting them
# would falsify history, so failing a commit over them is wrong. The
# /docs-hygiene skill is where staleness in those is judged, not here.
mapfile -t DOCS < <(git ls-files '*.md' \
  | grep -v '^web/node_modules/' \
  | grep -v '^\.kilo/' \
  | grep -v '^summaries/' \
  | grep -v '^docs/todos/' \
  | grep -v '^docs/requirements/' \
  | grep -v '^docs/GRACEFUL_SHUTDOWN_INVESTIGATION\.md$' \
  | grep -v '^internal/vendor/go-pq-cdc/\(README\|CONTRIBUTING\|CHANGELOG\)' )

echo "🔍 Docs hygiene sensor (${#DOCS[@]} markdown files)"

# ---------------------------------------------------------------- 1 & 2
MAP_OUT=$(python3 - "${DOCS[@]}" <<'PY'
import os, re, sys
docs = sys.argv[1:]
# NOTE: alternation is ordered, so longer suffixes MUST come first -- with
# "ts|tsx", ".ts" matches the prefix of "__root.tsx" and reports a false stale
# reference. The trailing lookahead enforces the same boundary rule.
PATH_RE = re.compile(r'`([A-Za-z0-9_][A-Za-z0-9_./-]*/[A-Za-z0-9_.-]+\.(?:go|md|yaml|yml|json|tsx|ts|sh|sql))(?![A-Za-z0-9])(:(\d+))?')
def lc(p):
    try:
        return sum(1 for _ in open(p, 'rb'))
    except OSError:
        return None
bad = []
for doc in docs:
    base = os.path.dirname(doc)
    try:
        text = open(doc, encoding='utf-8', errors='replace').read()
    except OSError:
        continue
    for m in PATH_RE.finditer(text):
        raw, ln = m.group(1), m.group(3)
        # A citation may be written repo-root-relative, relative to the doc's own
        # directory (PATCHES.md uses vendored-module-relative paths), or in the
        # common shorthand that drops the leading "internal/".
        cands = (
            raw,
            os.path.normpath(os.path.join(base, raw)),
            os.path.join("internal", raw),
            os.path.join("internal/vendor/go-pq-cdc", raw),
        )
        hit = next((c for c in cands if os.path.isfile(c)), None)
        if hit is None:
            bad.append(f"{doc}: cites `{raw}` which does not exist")
        elif ln:
            n = lc(hit)
            if n is not None and int(ln) > n:
                bad.append(f"{doc}: cites `{raw}` but that file has only {n} lines")
for b in sorted(set(bad)):
    print(b)
PY
)
if [[ -n "$MAP_OUT" ]]; then
  while IFS= read -r l; do note "$l"; done <<< "$MAP_OUT"
else
  ok "doc file references resolve"
fi

# ---------------------------------------------------------------- 3 & 4
ADR_DIR="docs/decisions"
if [[ -d "$ADR_DIR" ]]; then
  ADR_OUT=$(python3 - "$ADR_DIR" <<'PY'
import os, re, sys
d = sys.argv[1]
idx = os.path.join(d, "README.md")
adrs = sorted(f for f in os.listdir(d) if re.match(r'^\d{4}-.*\.md$', f))
bad = []
for f in adrs + (["README.md"] if os.path.exists(idx) else []):
    text = open(os.path.join(d, f), encoding='utf-8', errors='replace').read()
    for t in re.findall(r'\]\((\d{4}-[A-Za-z0-9_-]+\.md)\)', text):
        if not os.path.exists(os.path.join(d, t)):
            bad.append(f"{d}/{f}: links {t} which does not exist")
if os.path.exists(idx):
    itext = open(idx, encoding='utf-8', errors='replace').read()
    for a in adrs:
        if a not in itext:
            bad.append(f"{idx}: does not index {a}")
    for t in set(re.findall(r'\]\((\d{4}-[A-Za-z0-9_-]+\.md)\)', itext)):
        if t not in adrs:
            bad.append(f"{idx}: indexes {t} which does not exist")
else:
    bad.append(f"{idx}: ADR index is missing")
for b in sorted(set(bad)):
    print(b)
PY
)
  if [[ -n "$ADR_OUT" ]]; then
    while IFS= read -r l; do note "$l"; done <<< "$ADR_OUT"
  else
    ok "ADR links and index are consistent"
  fi
fi

# ---------------------------------------------------------------- 5
if command -v swag >/dev/null 2>&1; then
  TMP=$(mktemp -d)
  if swag init -g cmd/api/main.go -o "$TMP" --parseDependency --parseInternal >/dev/null 2>&1; then
    if ! diff -q "$TMP/swagger.json" docs/swagger.json >/dev/null 2>&1; then
      note "docs/swagger.json is stale — run: swag init -g cmd/api/main.go -o docs --parseDependency --parseInternal"
    else
      ok "generated swagger matches source annotations"
    fi
  else
    skip "swag init failed; skipping swagger drift check"
  fi
  rm -rf "$TMP"
else
  skip "swag not installed; skipping swagger drift check"
fi

if [[ $FAILED -ne 0 ]]; then
  echo
  echo "Docs hygiene FAILED. These are mechanical staleness checks — a citation"
  echo "that no longer resolves means the docs describe code that has moved."
  echo "For judgement calls (is a claim still true? does this need an ADR?),"
  echo "run the /docs-hygiene skill."
  exit 1
fi
echo "✅ Docs hygiene passed."
