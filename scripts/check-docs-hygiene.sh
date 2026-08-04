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
#   2. Every backtick-quoted repo path cited in Markdown exists. Citations into
#      gitignored paths (plans/, summaries/) are reported as unverifiable rather
#      than failed -- they are real on the author's machine and absent in a clean
#      checkout, so failing would demand a hygiene: marker on every one forever.
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

# LIVING docs -- documents expected to be kept true. Includes:
#   - README / AGENTS / **/AGENT.md / LOCAL_DEVELOPMENT / VENDOR / PATCHES
#   - docs/decisions/  (ADRs; immutable, but their citations must resolve)
#   - rfc/             (design records for significant changes; kept current)
#   - docs/todos/      (the live backlog of deferred work -- a TODO that cites
#                       a file which no longer exists is a TODO nobody can act
#                       on, which is exactly the rot this sensor exists to catch)
#
# Deliberately excluded, as point-in-time records that must NOT be rewritten:
#   - summaries/                     (archived review output)
#   - docs/requirements/             (as-written specs; superseded by ADRs)
#   - docs/*INVESTIGATION*.md        (findings as they stood on the day)
# These legitimately cite files that were proposed but never built. Failing a
# commit over them would falsify history. Staleness there is a judgement call
# for the /docs-hygiene skill.
# `git ls-files` lists tracked files only, so a brand-new doc would go
# unchecked until it was staged -- exactly when a mistake is most likely. Include
# untracked-but-not-ignored Markdown too.
mapfile -t DOCS < <( { git ls-files '*.md'; git ls-files --others --exclude-standard '*.md'; } \
  | grep -v '^web/node_modules/' \
  | grep -v '^\.kilo/' \
  | grep -v '^summaries/' \
  | grep -v '^docs/requirements/' \
  | grep -v 'INVESTIGATION\.md$' \
  | grep -v '^internal/vendor/go-pq-cdc/\(README\|CONTRIBUTING\|CHANGELOG\)' )

echo "🔍 Docs hygiene sensor (${#DOCS[@]} markdown files)"

# ---------------------------------------------------------------- 1 & 2
MAP_OUT=$(python3 - "${DOCS[@]}" <<'PY'
import os, re, subprocess, sys
docs = sys.argv[1:]

# NOTE: alternation is ordered, so longer suffixes MUST come first -- with
# "ts|tsx", ".ts" matches the prefix of "__root.tsx" and reports a false stale
# reference. The trailing lookahead enforces the same boundary rule.
PATH_RE = re.compile(r'`([A-Za-z0-9_][A-Za-z0-9_./-]*/[A-Za-z0-9_.-]+\.(?:go|md|yaml|yml|json|tsx|ts|sh|sql))(?![A-Za-z0-9])(:(\d+))?')

# Escape hatch, per line. A backlog entry may legitimately cite a file that does
# not exist yet ("create docs/runbooks/foo.md"), and an ADR may cite a path that
# was deliberately removed. Marking the line is a deliberate, greppable act --
# unlike a blanket directory exclusion, which silently stops checking everything.
#
# It is NOT the mechanism for citing plans/ or summaries/. Those are gitignored,
# so every such citation would need a marker forever -- a tax with no signal.
# See gitignored() below: git decides, and the count is always reported.
IGNORE_RE = re.compile(r'hygiene:(planned|ignore)')

def lc(p):
    try:
        return sum(1 for _ in open(p, 'rb'))
    except OSError:
        return None

def gitignored(paths):
    """Subset of paths that .gitignore excludes.

    A citation into an ignored path cannot be checked in a clean checkout --
    the target is real on the author's machine and absent in CI. That is
    UNVERIFIABLE, not stale, and failing on it would force a hygiene: marker
    onto every line that cites plans/ or summaries/ forever.

    This is deliberately narrower than a hardcoded directory list: it asks git,
    so it tracks .gitignore instead of drifting from it, and a citation that
    is merely wrong still fails.
    """
    if not paths:
        return set()
    try:
        p = subprocess.run(["git", "check-ignore", "--stdin"],
                           input="\n".join(paths), capture_output=True,
                           text=True, check=False)
    except OSError:
        return set()
    return {l.strip() for l in p.stdout.splitlines() if l.strip()}

bad, missing = [], []
for doc in docs:
    base = os.path.dirname(doc)
    try:
        lines = open(doc, encoding='utf-8', errors='replace').read().splitlines()
    except OSError:
        continue
    for i, line in enumerate(lines, 1):
        if IGNORE_RE.search(line):
            continue
        for m in PATH_RE.finditer(line):
            raw, ln = m.group(1), m.group(3)
            # A citation may be repo-root-relative, relative to the doc's own
            # directory (PATCHES.md uses vendored-module-relative paths), or in
            # the common shorthand that drops the leading "internal/".
            cands = (
                raw,
                os.path.normpath(os.path.join(base, raw)),
                os.path.join("internal", raw),
                os.path.join("internal/vendor/go-pq-cdc", raw),
            )
            hit = next((c for c in cands if os.path.isfile(c)), None)
            if hit is None:
                missing.append((doc, i, raw))
            elif ln:
                n = lc(hit)
                if n is not None and int(ln) > n:
                    bad.append(f"{doc}:{i}: cites `{raw}` but that file has only {n} lines")

ignored = gitignored(sorted({raw for _, _, raw in missing}))
unverifiable = 0
for doc, i, raw in missing:
    if raw in ignored:
        unverifiable += 1
        continue
    bad.append(f"{doc}:{i}: cites `{raw}` which does not exist "
               f"(if intentional, mark the line hygiene:planned)")

for b in sorted(set(bad)):
    print("F\t" + b)
if unverifiable:
    print(f"U\t{unverifiable} citation(s) point into gitignored paths "
          f"(real locally, absent in a clean checkout) — not checked")
PY
)
UNVERIFIABLE=""
if [[ -n "$MAP_OUT" ]]; then
  while IFS= read -r l; do
    case "$l" in
      F$'\t'*) note "${l#F$'\t'}" ;;
      U$'\t'*) UNVERIFIABLE="${l#U$'\t'}" ;;
    esac
  done <<< "$MAP_OUT"
fi
if [[ $FAILED -eq 0 ]]; then
  ok "doc file references resolve"
fi
# Reported, never silent: the point of the sensor is that nothing stops being
# checked without someone being told.
[[ -n "$UNVERIFIABLE" ]] && skip "$UNVERIFIABLE"

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
