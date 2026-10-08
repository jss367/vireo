#!/bin/bash
# Release Vireo: bump version, tag, and let CI build all platforms.
#
# Usage:
#   ./scripts/release.sh patch                # bump + local build (for testing)
#   ./scripts/release.sh minor --publish      # bump + tag + push (CI builds all platforms)
#   ./scripts/release.sh 0.5.0 --publish      # explicit version
#
# With --publish, the script bumps the version, commits, tags, and pushes.
# CI (build-release.yml) then builds macOS ARM64, macOS Intel, Windows, and
# Linux, and creates a draft GitHub Release with all artifacts.
#
# Without --publish, a local build is done for testing on the current machine.
#
# Optional environment variables (for local signed builds):
#   APPLE_SIGNING_IDENTITY  - e.g. "Developer ID Application: Name (TEAM_ID)"
#   APPLE_ID                - Your Apple ID email
#   APPLE_PASSWORD          - App-specific password for notarization
#   APPLE_TEAM_ID           - 10-character Team ID

set -euo pipefail
cd "$(dirname "$0")/.."

# Keep the Mac from idle-sleeping until this script exits. The E2E suite takes
# ~30 minutes, and a sleep mid-run fails a page load with
# net::ERR_NETWORK_IO_SUSPENDED, which aborts the release.
if command -v caffeinate >/dev/null 2>&1; then
    caffeinate -i -w $$ >/dev/null 2>&1 &
fi

# Browser tests can exhaust macOS's default 256-descriptor soft limit. Raise
# only this process's soft limit; child tests/builds inherit it, and the hard
# limit and caller's shell remain unchanged. Check before modifying manifests.
RELEASE_MIN_OPEN_FILES=4096
RELEASE_OPEN_FILES=$(ulimit -S -n)
if [[ "$RELEASE_OPEN_FILES" != "unlimited" && "$RELEASE_OPEN_FILES" -lt "$RELEASE_MIN_OPEN_FILES" ]]; then
    if ! ulimit -S -n "$RELEASE_MIN_OPEN_FILES"; then
        echo "ERROR: Release requires at least $RELEASE_MIN_OPEN_FILES open files; current soft limit is $RELEASE_OPEN_FILES, hard limit is $(ulimit -H -n)." >&2
        echo "Raise the system or parent shell limit, then rerun the release command." >&2
        exit 1
    fi
    echo "==> Raised open-file limit from $RELEASE_OPEN_FILES to $RELEASE_MIN_OPEN_FILES."
fi

# --- Parse args ---
BUMP="${1:-patch}"
PUBLISH=false
if [[ "${2:-}" == "--publish" ]]; then
    PUBLISH=true
fi

# Publishing targets main explicitly. Synchronize before changing manifests or
# running the long test gate; never merge untested remote changes after it.
if $PUBLISH; then
    if [[ "$(git branch --show-current)" != "main" ]]; then
        echo "ERROR: Publish releases from main." >&2
        exit 1
    fi
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "ERROR: Commit or stash changes before publishing a release." >&2
        exit 1
    fi
    echo "==> Syncing main before release checks..."
    git fetch origin main
    git merge --ff-only origin/main
fi

# --- Read current version from pyproject.toml ---
CURRENT=$(grep -m1 '^version' pyproject.toml | sed 's/version = "\(.*\)"/\1/')
echo "Current version: $CURRENT"

# --- Calculate new version ---
IFS='.' read -r MAJOR MINOR PATCH_NUM <<< "$CURRENT"
case "$BUMP" in
    patch) NEW_VERSION="$MAJOR.$MINOR.$((PATCH_NUM + 1))" ;;
    minor) NEW_VERSION="$MAJOR.$((MINOR + 1)).0" ;;
    major) NEW_VERSION="$((MAJOR + 1)).0.0" ;;
    *)     NEW_VERSION="$BUMP" ;;
esac
echo "New version:     $NEW_VERSION"
echo ""

if $PUBLISH; then
    if git show-ref --verify --quiet "refs/tags/v$NEW_VERSION"; then
        echo "ERROR: Local tag v$NEW_VERSION already exists; recover the existing release instead of rerunning the version bump." >&2
        exit 1
    fi
    REMOTE_TAG=$(git ls-remote origin "refs/tags/v$NEW_VERSION")
    if [[ -n "$REMOTE_TAG" ]]; then
        echo "ERROR: Remote tag v$NEW_VERSION already exists." >&2
        exit 1
    fi
fi

# --- Sync version across all manifests ---
echo "==> Syncing version..."
python scripts/sync_version.py "$NEW_VERSION"
# `--workspace` rewrites only the `vireo` entry in Cargo.lock so it matches the
# version sync_version.py just wrote to Cargo.toml. Do NOT use
# `cargo generate-lockfile` here: it re-resolves every third-party crate to the
# newest compatible version, so a release silently ships dependency bumps that
# no CI run has ever compiled. That is how v0.32.3 picked up zune-core 0.5.2 —
# published three hours earlier, broken, yanked shortly after — and failed the
# macOS build. Dependency updates belong in reviewed Dependabot PRs; see
# .github/dependabot.yml.
echo "==> Updating Cargo.lock (workspace version only)..."
(cd src-tauri && cargo update --workspace)
echo ""

# --- Verify the Rust dependency graph compiles (publish path only) ---
# The publish path never builds locally — CI does that, but only *after* the tag
# is pushed, so a dependency that does not compile is discovered 20 minutes into
# a tagged build with the tag already public. `cargo check --locked` compiles
# every dependency for this host in a couple of minutes and also asserts that
# Cargo.lock is complete and in sync with Cargo.toml. The non-publish path skips
# this because the full local build below already covers it.
if $PUBLISH; then
    echo "==> Checking Rust dependency graph..."
    (cd src-tauri && cargo check --locked)
    echo ""
fi

# --- Run E2E tests ---
echo "==> Running E2E tests..."
# Match the release gate in e2e-full.yml: retry a failed test up to twice so a
# one-off timing flake doesn't block the release. A broken test still fails all
# three attempts.
python -m pytest tests/e2e/ -v --reruns 2 --reruns-delay 1
echo ""

# --- Local build (only when NOT publishing — CI handles publish builds) ---
if ! $PUBLISH; then
    # Check if full signing credentials are available
    FULL_SIGNING=true
    for var in APPLE_SIGNING_IDENTITY APPLE_ID APPLE_PASSWORD APPLE_TEAM_ID; do
        if [ -z "${!var:-}" ]; then
            FULL_SIGNING=false
            break
        fi
    done

    if $FULL_SIGNING; then
        echo "==> Building with full signing and notarization..."
        ./scripts/build_signed.sh
    else
        echo "==> Building (ad-hoc signing — no Apple Developer credentials)..."
        python scripts/build_sidecar.py

        BUILD_LOG=$(mktemp)
        if ! cargo tauri build 2>&1 | tee "$BUILD_LOG"; then
            if grep -q "TAURI_SIGNING_PRIVATE_KEY" "$BUILD_LOG"; then
                echo ""
                echo "WARNING: Updater artifact signing skipped (TAURI_SIGNING_PRIVATE_KEY not set)."
            else
                echo "ERROR: cargo tauri build failed (see output above)"
                rm -f "$BUILD_LOG"
                exit 1
            fi
        fi
        rm -f "$BUILD_LOG"

        APP_PATH="src-tauri/target/release/bundle/macos/Vireo.app"
        if [ ! -d "$APP_PATH" ]; then
            echo "ERROR: $APP_PATH not found"
            exit 1
        fi
        echo "==> Ad-hoc signing app bundle..."
        codesign --sign - --force --deep "$APP_PATH"
        codesign --verify --deep --verbose=2 "$APP_PATH"
    fi
    echo ""

    # Find the DMG
    DMG=$(find src-tauri/target/release/bundle/dmg -name "*.dmg" 2>/dev/null | head -1)
    if [[ -z "$DMG" ]]; then
        echo "ERROR: No .dmg found"
        exit 1
    fi

    # Rebuild DMG after ad-hoc signing
    if ! $FULL_SIGNING; then
        echo "==> Rebuilding DMG with signed app..."
        hdiutil create -volname "Vireo" -srcfolder "$APP_PATH" -ov -format UDZO "$DMG"
    fi
    echo "==> Built: $DMG"
    echo ""
fi

# --- Commit version bump ---
echo "==> Committing version bump..."
git add pyproject.toml package.json src-tauri/tauri.conf.json src-tauri/Cargo.toml src-tauri/Cargo.lock
git commit -m "release: v$NEW_VERSION"
echo ""

# --- Tag and publish ---
if $PUBLISH; then
    echo "==> Tagging v$NEW_VERSION..."
    git tag "v$NEW_VERSION"
    # Publish both refs or neither. Main can advance during the test gate; a
    # rejection must not publish a tag or change the source that passed tests.
    if ! git push --atomic origin HEAD:refs/heads/main "refs/tags/v$NEW_VERSION"; then
        echo "ERROR: Release push failed. The local release commit and tag v$NEW_VERSION are retained." >&2
        echo "Do not rerun the version bump or force-push. Inspect origin/main and the remote tag first." >&2
        echo "If main advanced, merge origin/main into main without rebasing or moving the tag to preserve the tested release source." >&2
        echo "Then retry: git push --atomic origin HEAD:refs/heads/main refs/tags/v$NEW_VERSION" >&2
        exit 1
    fi
    echo ""
    echo "Tag pushed. CI will build all platforms and create a draft release."
    echo "Monitor: https://github.com/jss367/vireo/actions"
    echo "Release: https://github.com/jss367/vireo/releases/tag/v$NEW_VERSION"
else
    echo "Build complete. To publish:"
    echo "  git push"
    echo "  git tag v$NEW_VERSION && git push origin v$NEW_VERSION"
    echo ""
    echo "CI will build all platforms and create a draft release."
fi
