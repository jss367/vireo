#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Notarize, staple, and publish an already-built DMG.
# Use this when build_signed.sh or release.sh fails during
# the notarization upload step.
#
# Tries submitting the DMG directly first. If the upload fails
# (common with large files), falls back to zipping the .app
# bundle and submitting that instead (smaller upload).
# Retries up to 3 times per method before giving up.
#
# Usage:
#   ./scripts/notarize_and_publish.sh                  # notarize + staple + tag + GitHub release
#   ./scripts/notarize_and_publish.sh --no-publish      # notarize + staple only
#   ./scripts/notarize_and_publish.sh --skip-notarize   # skip notarization, just publish
# ============================================================

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MAX_RETRIES=5

# --- Validate required env vars ---
for var in APPLE_ID APPLE_PASSWORD APPLE_TEAM_ID; do
    if [ -z "${!var:-}" ]; then
        echo "ERROR: $var is not set"
        exit 1
    fi
done

# --- Parse args ---
PUBLISH=true
NOTARIZE=true
for arg in "$@"; do
    case "$arg" in
        --no-publish)     PUBLISH=false ;;
        --skip-notarize)  NOTARIZE=false ;;
    esac
done

# --- Find the DMG ---
DMG_DIR="$REPO_ROOT/src-tauri/target/release/bundle/dmg"
DMG_PATH=$(find "$DMG_DIR" -name "*.dmg" 2>/dev/null | head -1)
if [ -z "$DMG_PATH" ]; then
    echo "ERROR: No .dmg found in $DMG_DIR"
    exit 1
fi
echo "DMG: $DMG_PATH"

# --- Find the .app bundle ---
APP_PATH="$REPO_ROOT/src-tauri/target/release/bundle/macos/Vireo.app"

# --- Read version from pyproject.toml ---
VERSION=$(grep -m1 '^version' "$REPO_ROOT/pyproject.toml" | sed 's/version = "\(.*\)"/\1/')
echo "Version: $VERSION"
echo ""


if $NOTARIZE; then
    # --- Notarize: alternate between DMG and zip attempts ---
    NOTARIZED=false
    ZIP_PATH=""

    if [ -d "$APP_PATH" ]; then
        echo "==> Zipping .app bundle for notarization (max compression)..."
        ZIP_PATH="$REPO_ROOT/src-tauri/target/release/bundle/Vireo.app.zip"
        BUNDLE_DIR="$(dirname "$APP_PATH")"
        (cd "$BUNDLE_DIR" && zip -9 -r -q "$ZIP_PATH" "$(basename "$APP_PATH")")
        ZIP_SIZE=$(du -h "$ZIP_PATH" | cut -f1)
        echo "    Zip created: $ZIP_PATH ($ZIP_SIZE)"
        echo ""
    fi

    backoff=15
    for attempt in $(seq 1 "$MAX_RETRIES"); do
        # Odd attempts: try DMG; even attempts: try zip (if available)
        if (( attempt % 2 == 1 )); then
            echo "==> Attempt $attempt/$MAX_RETRIES: Submitting DMG for notarization..."
            file="$DMG_PATH"
        elif [ -n "$ZIP_PATH" ]; then
            echo "==> Attempt $attempt/$MAX_RETRIES: Submitting zipped .app for notarization..."
            file="$ZIP_PATH"
        else
            echo "==> Attempt $attempt/$MAX_RETRIES: Submitting DMG for notarization..."
            file="$DMG_PATH"
        fi

        if xcrun notarytool submit "$file" \
            --apple-id "$APPLE_ID" \
            --password "$APPLE_PASSWORD" \
            --team-id "$APPLE_TEAM_ID" \
            --wait; then
            NOTARIZED=true
            break
        fi

        echo ""
        echo "Upload failed (attempt $attempt/$MAX_RETRIES)"
        if [ "$attempt" -lt "$MAX_RETRIES" ]; then
            echo "Retrying in ${backoff} seconds..."
            sleep "$backoff"
            backoff=$((backoff * 2))
        fi
    done

    [ -n "$ZIP_PATH" ] && rm -f "$ZIP_PATH"

    if ! $NOTARIZED; then
        echo ""
        echo "ERROR: Notarization failed after all attempts."
        echo "Check your network connection and try again."
        exit 1
    fi

    echo ""
    echo "==> Stapling..."
    xcrun stapler staple "$DMG_PATH"

    echo ""
    echo "==> Verification:"
    spctl --assess --type open --context context:primary-signature --verbose=2 "$DMG_PATH"
    echo ""
else
    echo "==> Skipping notarization (--skip-notarize)"
    echo "    Users will see a Gatekeeper warning but can right-click → Open"
    echo ""
fi

# --- Publish ---
if $PUBLISH; then
    echo "==> Tagging v$VERSION..."
    git tag "v$VERSION" 2>/dev/null || echo "Tag v$VERSION already exists, skipping"
    git push
    git push origin "v$VERSION"

    echo "==> Creating GitHub Release..."
    gh release create "v$VERSION" \
        "$DMG_PATH" \
        --title "Vireo $VERSION" \
        --generate-notes
    echo ""
    echo "Release published: https://github.com/jss367/vireo/releases/tag/v$VERSION"
else
    echo "Notarization complete. To publish:"
    echo "  git tag v$VERSION && git push && git push origin v$VERSION"
    echo "  gh release create v$VERSION $DMG_PATH --title 'Vireo $VERSION' --generate-notes"
fi
