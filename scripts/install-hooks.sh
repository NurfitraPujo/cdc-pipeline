#!/usr/bin/env bash
set -eo pipefail

HOOK_DIR="$(git rev-parse --git-path hooks)"
mkdir -p "$HOOK_DIR"

echo "🔧 Installing Git pre-commit hook..."
cp .git-hooks/pre-commit "$HOOK_DIR/pre-commit"
chmod +x "$HOOK_DIR/pre-commit"

echo "✅ Git hooks installed successfully!"
