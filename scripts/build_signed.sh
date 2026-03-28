#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Build, sign, and notarize Vireo for macOS
# ============================================================
#
# Required environment variables:
#   APPLE_SIGNING_IDENTITY  - e.g. "Developer ID Application: Name (TEAM_ID)"
#   APPLE_ID                - Your Apple ID email
#   APPLE_PASSWORD          - App-specific password for notarization
#   APPLE_TEAM_ID           - 10-character Team ID
#
# Usage:
#   export APPLE_SIGNING_IDENTITY="Developer ID Application: ..."
#   export APPLE_ID="your@email.com"
#   export APPLE_PASSWORD="abcd-efgh-ijkl-mnop"
#   export APPLE_TEAM_ID="ABC123DEF4"
#   ./scripts/build_signed.sh
# ============================================================

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# --- Validate required env vars ---
missing=()
for var in APPLE_SIGNING_IDENTITY APPLE_ID APPLE_PASSWORD APPLE_TEAM_ID; do
    if [ -z "${!var:-}" ]; then
        missing+=("$var")
    fi
done

if [ ${#missing[@]} -gt 0 ]; then
    echo "ERROR: Missing required environment variables:"
    for var in "${missing[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "See docs/plans/2025-03-26-code-signing.md Task 1 for setup instructions."
    exit 1
fi

echo "=== Step 1/4: Build and sign the Python sidecar ==="
cd "$REPO_ROOT"
python scripts/build_sidecar.py

echo ""
echo "=== Step 2/4: Build the Tauri app (with code signing) ==="
cd "$REPO_ROOT"
cargo tauri build 2>&1 || true  # updater signing error is non-fatal until key is configured

echo ""
echo "=== Step 3/4: Verify code signature on .app bundle ==="
APP_PATH="$REPO_ROOT/src-tauri/target/release/bundle/macos/Vireo.app"
if [ ! -d "$APP_PATH" ]; then
    echo "ERROR: $APP_PATH not found"
    exit 1
fi

echo "Verifying main app bundle..."
codesign --verify --deep --strict --verbose=2 "$APP_PATH"

echo ""
echo "Verifying sidecar inside bundle..."
SIDECAR_PATH="$APP_PATH/Contents/MacOS/vireo-server"
if [ -f "$SIDECAR_PATH" ]; then
    codesign --verify --verbose=2 "$SIDECAR_PATH"
else
    echo "WARNING: Sidecar not found at $SIDECAR_PATH"
    echo "Checking alternative location..."
    find "$APP_PATH" -name "vireo-server*" -type f 2>/dev/null
fi

echo ""
echo "=== Step 4/4: Notarize and staple ==="
DMG_DIR="$REPO_ROOT/src-tauri/target/release/bundle/dmg"
if [ -d "$DMG_DIR" ]; then
    DMG_PATH=$(find "$DMG_DIR" -name "*.dmg" | head -1)
else
    DMG_PATH=""
fi
if [ -z "$DMG_PATH" ]; then
    echo "ERROR: No .dmg found in target/release/bundle/dmg/"
    exit 1
fi

echo "Submitting $DMG_PATH for notarization..."
xcrun notarytool submit "$DMG_PATH" \
    --apple-id "$APPLE_ID" \
    --password "$APPLE_PASSWORD" \
    --team-id "$APPLE_TEAM_ID" \
    --wait

echo ""
echo "Stapling notarization ticket to DMG..."
xcrun stapler staple "$DMG_PATH"

echo ""
echo "=== Done ==="
echo "Signed and notarized DMG: $DMG_PATH"
echo ""
echo "Verification:"
spctl --assess --type open --context context:primary-signature --verbose=2 "$DMG_PATH"
