#!/usr/bin/env bash
# Install git hooks for the Vireo project.
# Run once after cloning: bash scripts/install-hooks.sh

set -e

# In worktrees, git-dir points to .git/worktrees/<name> which has no hooks dir.
# Use git-common-dir to always find the shared hooks location.
HOOK_DIR="$(git rev-parse --git-common-dir)/hooks"
mkdir -p "$HOOK_DIR"

cat > "$HOOK_DIR/pre-commit" << 'HOOK'
#!/usr/bin/env bash
# Pre-commit hook: run ruff on staged Python files.
# Auto-fixes trivial issues (import sorting, unused imports) and re-stages.
# Blocks commit if unfixable errors remain.

STAGED=$(git diff --cached --name-only --diff-filter=ACM -- '*.py')
[ -z "$STAGED" ] && exit 0

if ! command -v ruff &>/dev/null; then
    echo "ruff not found — install with: pip install ruff"
    exit 1
fi

# Auto-fix and re-stage
ruff check --fix $STAGED 2>/dev/null
git add $STAGED

# Final check — block on remaining errors
if ! ruff check $STAGED; then
    echo ""
    echo "ruff check failed. Fix the errors above, then retry your commit."
    exit 1
fi
HOOK

chmod +x "$HOOK_DIR/pre-commit"
echo "Installed pre-commit hook (ruff check + auto-fix)"
