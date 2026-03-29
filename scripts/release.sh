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

# --- Parse args ---
BUMP="${1:-patch}"
PUBLISH=false
if [[ "${2:-}" == "--publish" ]]; then
    PUBLISH=true
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

# --- Sync version across all manifests ---
echo "==> Syncing version..."
python scripts/sync_version.py "$NEW_VERSION"
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
git commit -m "release: v$NEW_VERSION" || true
echo ""

# --- Check CI health before publishing ---
if $PUBLISH && command -v gh &> /dev/null; then
    echo "==> Checking recent Build & Release CI runs..."
    RECENT_RUNS=$(gh run list --workflow="Build & Release" --limit 3 --json conclusion --jq '.[].conclusion' 2>/dev/null || true)
    if [[ -n "$RECENT_RUNS" ]]; then
        ALL_FAILED=true
        for conclusion in $RECENT_RUNS; do
            if [[ "$conclusion" == "success" ]]; then
                ALL_FAILED=false
                break
            fi
        done
        if $ALL_FAILED; then
            echo ""
            echo "WARNING: The last 3 Build & Release runs all failed/cancelled."
            echo "         Pushing a tag will trigger another build that will likely fail."
            echo ""
            echo "  Recent runs:"
            gh run list --workflow="Build & Release" --limit 3 --json conclusion,createdAt --jq '.[] | "    \(.createdAt)  \(.conclusion)"' 2>/dev/null || true
            echo ""
            read -r -p "Publish anyway? [y/N] " CONFIRM
            if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
                echo "Aborted. Version bump commit is still on your local branch."
                exit 1
            fi
        fi
    fi
fi

# --- Tag and publish ---
if $PUBLISH; then
    echo "==> Tagging v$NEW_VERSION..."
    git tag "v$NEW_VERSION"
    git push && git push origin "v$NEW_VERSION"
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
