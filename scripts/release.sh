#!/bin/bash
# Build a release and optionally publish to GitHub.
#
# The app is always code-signed so macOS won't reject it as "damaged".
# If Apple Developer credentials are set, uses full signing + notarization
# (no Gatekeeper warning at all). Otherwise, uses ad-hoc signing (users
# see "from an unidentified developer" and can right-click → Open).
#
# Usage:
#   ./scripts/release.sh patch          # 0.2.1 -> 0.2.2
#   ./scripts/release.sh minor          # 0.2.1 -> 0.3.0
#   ./scripts/release.sh major          # 0.2.1 -> 1.0.0
#   ./scripts/release.sh 0.5.0          # explicit version
#   ./scripts/release.sh patch --publish # also upload to GitHub Release
#
# Optional environment variables (for full notarization):
#   APPLE_SIGNING_IDENTITY  - e.g. "Developer ID Application: Name (TEAM_ID)"
#   APPLE_ID                - Your Apple ID email
#   APPLE_PASSWORD          - App-specific password for notarization
#   APPLE_TEAM_ID           - 10-character Team ID

set -euo pipefail
cd "$(dirname "$0")/.."

# --- Check if full signing credentials are available ---
FULL_SIGNING=true
for var in APPLE_SIGNING_IDENTITY APPLE_ID APPLE_PASSWORD APPLE_TEAM_ID; do
    if [ -z "${!var:-}" ]; then
        FULL_SIGNING=false
        break
    fi
done

# --- Parse args ---
BUMP="${1:?Usage: release.sh <patch|minor|major|X.Y.Z> [--publish]}"
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

# --- Build ---
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

# --- Find the DMG ---
DMG=$(find src-tauri/target/release/bundle/dmg -name "*.dmg" 2>/dev/null | head -1)
if [[ -z "$DMG" ]]; then
    echo "ERROR: No .dmg found"
    exit 1
fi

# --- Rebuild DMG after ad-hoc signing ---
# cargo tauri build creates .app and .dmg in one step, so the original
# DMG contains the unsigned app. Mount the Tauri DMG, swap in the signed
# .app, and repackage — preserving the layout (background, icon positions,
# Applications symlink).
if ! $FULL_SIGNING; then
    echo "==> Rebuilding DMG with signed app..."
    DMG_MOUNT=$(mktemp -d)
    DMG_STAGING=$(mktemp -d)
    hdiutil attach "$DMG" -mountpoint "$DMG_MOUNT" -nobrowse -readonly -noverify
    cp -a "$DMG_MOUNT"/ "$DMG_STAGING"/
    hdiutil detach "$DMG_MOUNT"
    rm -rf "$DMG_STAGING/Vireo.app"
    cp -a "$APP_PATH" "$DMG_STAGING/"
    hdiutil create -volname "Vireo" -srcfolder "$DMG_STAGING" -ov -format UDZO "$DMG"
    rm -rf "$DMG_MOUNT" "$DMG_STAGING"
fi
echo "==> Built: $DMG"
echo ""

# --- Commit version bump ---
echo "==> Committing version bump..."
git add pyproject.toml package.json src-tauri/tauri.conf.json src-tauri/Cargo.toml src-tauri/Cargo.lock website/src/pages/download.astro
git commit -m "release: v$NEW_VERSION" || true
echo ""

# --- Tag and publish ---
if $PUBLISH; then
    echo "==> Tagging v$NEW_VERSION..."
    git tag "v$NEW_VERSION"
    git push && git push origin "v$NEW_VERSION"

    echo "==> Creating GitHub Release..."
    gh release create "v$NEW_VERSION" \
        "$DMG" \
        --title "Vireo $NEW_VERSION" \
        --generate-notes
    echo ""
    echo "Release published: https://github.com/jss367/vireo/releases/tag/v$NEW_VERSION"
else
    echo "Build complete. To publish:"
    echo "  git push"
    echo "  git tag v$NEW_VERSION && git push origin v$NEW_VERSION"
    echo "  gh release create v$NEW_VERSION $DMG --title 'Vireo $NEW_VERSION' --generate-notes"
fi
