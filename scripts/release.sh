#!/bin/bash
# Build a signed and notarized release, optionally publish to GitHub.
#
# Usage:
#   ./scripts/release.sh patch          # 0.2.1 -> 0.2.2
#   ./scripts/release.sh minor          # 0.2.1 -> 0.3.0
#   ./scripts/release.sh major          # 0.2.1 -> 1.0.0
#   ./scripts/release.sh 0.5.0          # explicit version
#   ./scripts/release.sh patch --publish # also upload to GitHub Release
#
# Required environment variables (for macOS code signing & notarization):
#   APPLE_SIGNING_IDENTITY  - e.g. "Developer ID Application: Name (TEAM_ID)"
#   APPLE_ID                - Your Apple ID email
#   APPLE_PASSWORD          - App-specific password for notarization
#   APPLE_TEAM_ID           - 10-character Team ID

set -euo pipefail
cd "$(dirname "$0")/.."

# --- Validate signing env vars ---
missing=()
for var in APPLE_SIGNING_IDENTITY APPLE_ID APPLE_PASSWORD APPLE_TEAM_ID; do
    if [ -z "${!var:-}" ]; then
        missing+=("$var")
    fi
done

if [ ${#missing[@]} -gt 0 ]; then
    echo "ERROR: Missing required environment variables for code signing:"
    for var in "${missing[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "All releases must be signed and notarized. Set these variables and retry."
    exit 1
fi

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

# --- Build, sign, and notarize ---
echo "==> Building signed and notarized app..."
./scripts/build_signed.sh
echo ""

# --- Find the DMG ---
DMG=$(find src-tauri/target/release/bundle/dmg -name "*.dmg" 2>/dev/null | head -1)
if [[ -z "$DMG" ]]; then
    echo "ERROR: No .dmg found"
    exit 1
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
