# Building Vireo

## Prerequisites

- Python 3.11+ with project dependencies installed
- Rust toolchain (`rustup`)
- Node.js 20+
- Tauri CLI: `cargo install tauri-cli@2`
- PyInstaller: `pip install pyinstaller`

## Development

Run Flask and Tauri separately:

```bash
# Terminal 1: Flask backend
python vireo/app.py --db ~/.vireo/vireo.db --port 8080

# Terminal 2: Tauri desktop shell
cargo tauri dev
```

The Tauri webview connects to the Flask server at `localhost:8080`. Changes to Python code require restarting Flask. Changes to templates/JS are picked up on page reload.

## Building a Release

### Local Mac build (.dmg)

```bash
python scripts/build_sidecar.py    # Bundle Python into sidecar binary
cargo tauri build                   # Build Vireo.app and .dmg
```

Output:
- `src-tauri/target/release/bundle/macos/Vireo.app`
- `src-tauri/target/release/bundle/dmg/Vireo_X.Y.Z_aarch64.dmg`

### With a version bump

```bash
python scripts/sync_version.py 0.2.0   # Patches tauri.conf.json, package.json, Cargo.toml, pyproject.toml
python scripts/build_sidecar.py
cargo tauri build
```

### CI/CD (all platforms)

Tag a release to trigger GitHub Actions:

```bash
git tag v0.2.0
git push origin v0.2.0
```

This builds for macOS (ARM64 + x86_64), Windows, and Linux, then creates a draft GitHub Release with all installers attached. Review the draft and publish when ready.

To trigger manually without a tag, use the "Run workflow" button on the Actions tab.

### Signed Mac build (notarized)

Requires an Apple Developer account. Set these environment variables:

```bash
export APPLE_SIGNING_IDENTITY="Developer ID Application: Your Name (TEAMID)"
export APPLE_ID="your@email.com"
export APPLE_PASSWORD="app-specific-password"
export APPLE_TEAM_ID="XXXXXXXXXX"
```

Then run:

```bash
bash scripts/build_signed.sh
```

This builds the sidecar, signs it, builds the Tauri app, signs the .app bundle, notarizes the .dmg with Apple, and staples the notarization ticket.

## Build outputs

| Platform | Installer | Location |
|----------|-----------|----------|
| macOS | `.dmg` | `src-tauri/target/release/bundle/dmg/` |
| macOS | `.app` | `src-tauri/target/release/bundle/macos/` |
| Windows | `.msi` | `src-tauri/target/release/bundle/msi/` |
| Linux | `.AppImage` | `src-tauri/target/release/bundle/appimage/` |
| Linux | `.deb` | `src-tauri/target/release/bundle/deb/` |

## Troubleshooting

**Sidecar binary not found:** Make sure you ran `python scripts/build_sidecar.py` before `cargo tauri build`. The sidecar must exist at `src-tauri/binaries/vireo-server-{target-triple}`.

**Template not found errors in production:** The PyInstaller build script bundles `vireo/templates/` and `vireo/static/` via `--add-data`. If you add new template directories, update `scripts/build_sidecar.py`.

**First `cargo tauri build` is slow:** The initial Rust release compilation takes 2-5 minutes. Subsequent builds are incremental and much faster.

**macOS Gatekeeper blocks unsigned app:** Either sign the build (see above) or right-click the app and select "Open" to bypass Gatekeeper for local testing.
