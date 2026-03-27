# Vireo Developer Notes

## Architecture Overview

Vireo is a desktop app with two layers:

- **Python backend** — Flask server handling all routes, SQLite database, ML inference (BioCLIP, PyTorch Wildlife, DINOv2), background job runner
- **Tauri shell** — Rust-based native window that wraps the Flask app, providing OS-native chrome, file dialogs, system tray, menu bar, and auto-updates

In development, these run as separate processes. In production, Tauri spawns the Python backend as a sidecar process automatically.

## Development Workflow

### Running locally

```bash
# Terminal 1: Flask backend
python vireo/app.py --db ~/.vireo/vireo.db --port 8080

# Terminal 2: Tauri desktop shell (optional — browser works fine too)
cargo tauri dev
```

You can also just use the Flask server with a regular browser at `http://localhost:8080`.

### Making changes

All feature work and bug fixes go through worktrees and PRs:

```bash
git worktree add .claude/worktrees/my-feature origin/main -b feature/my-feature
# ... work in the worktree ...
gh pr create
```

### Running tests

```bash
# All tests
python -m pytest tests/ vireo/tests/ -q

# Specific test files
python -m pytest vireo/tests/test_app.py -v
python -m pytest vireo/tests/test_db.py -v

# Rust compilation check
cd src-tauri && cargo check
```

## Releasing

### One-command release

```bash
./scripts/release.sh patch              # 0.2.1 → 0.2.2, build locally
./scripts/release.sh minor              # 0.2.1 → 0.3.0, build locally
./scripts/release.sh major              # 0.2.1 → 1.0.0, build locally
./scripts/release.sh 1.0.0              # explicit version, build locally
./scripts/release.sh patch --publish    # bump, build, tag, upload to GitHub
```

This script:
1. Reads the current version from `pyproject.toml`
2. Bumps it (patch/minor/major or explicit)
3. Syncs the version across all manifests (`sync_version.py`)
4. Builds the Python sidecar via PyInstaller
5. Builds the Tauri app (`.dmg` on Mac)
6. Commits the version bump
7. With `--publish`: tags, pushes, creates a GitHub Release with the `.dmg` attached

### Manual steps (if you prefer)

```bash
python scripts/sync_version.py 0.3.0    # Update version in all manifests
python scripts/build_sidecar.py          # Bundle Python → sidecar binary
cargo tauri build                        # Build .app and .dmg
```

### CI/CD

Pushing a version tag triggers GitHub Actions to build for all platforms:

```bash
git tag v0.3.0
git push origin v0.3.0
```

This produces `.dmg` (macOS ARM + Intel), `.msi` (Windows), `.AppImage` and `.deb` (Linux) as a draft GitHub Release.

## Version Management

The version lives in 4 files, kept in sync by `scripts/sync_version.py`:

| File | Field |
|------|-------|
| `pyproject.toml` | `version = "X.Y.Z"` |
| `src-tauri/tauri.conf.json` | `"version": "X.Y.Z"` |
| `src-tauri/Cargo.toml` | `version = "X.Y.Z"` |
| `package.json` | `"version": "X.Y.Z"` |

Users see the version on the Settings page (About section) via `GET /api/version`.

## Project Structure

```
vireo/
├── vireo/                  # Python backend
│   ├── app.py              # Flask app with all routes
│   ├── db.py               # SQLite database layer
│   ├── jobs.py             # Background job runner
│   ├── config.py           # Global config (~/.vireo/config.json)
│   ├── templates/          # HTML pages (Jinja2 includes only)
│   ├── static/             # CSS, JS, images
│   └── tests/              # Python tests
├── src-tauri/              # Tauri desktop shell (Rust)
│   ├── src/
│   │   ├── main.rs         # Entry point
│   │   ├── lib.rs          # App setup, plugin registration
│   │   ├── sidecar.rs      # Python process lifecycle
│   │   ├── menu.rs         # Native menu bar
│   │   ├── tray.rs         # System tray with job status
│   │   └── updater.rs      # Auto-update commands
│   ├── tauri.conf.json     # Tauri configuration
│   ├── Cargo.toml          # Rust dependencies
│   ├── capabilities/       # Permission declarations
│   └── icons/              # App icons (all sizes)
├── scripts/
│   ├── release.sh          # One-command release builds
│   ├── build_sidecar.py    # PyInstaller bundling
│   ├── build_signed.sh     # macOS signed + notarized build
│   ├── build_static.py     # Resolve Jinja2 includes → static HTML
│   └── sync_version.py     # Version sync across manifests
├── build/
│   └── index.html          # Loading screen (shown while sidecar starts)
├── docs/
│   ├── BUILDING.md         # Build instructions
│   ├── DEV_NOTES.md        # This file
│   └── plans/              # Implementation plans
└── tests/                  # Additional Python tests
```

## Key Data Paths

| Path | Purpose |
|------|---------|
| `~/.vireo/vireo.db` | SQLite database (WAL mode) |
| `~/.vireo/vireo.log` | App logs (5MB rotating, 3 backups) |
| `~/.vireo/config.json` | Global settings |
| `~/.vireo/thumbnails/` | Thumbnail cache |
| `~/.vireo/labels/` | Species label files |
| `~/.vireo/previews/` | Photo preview cache |

## Tauri Sidecar Lifecycle

In production (packaged app):
1. Tauri starts, shows loading screen (`build/index.html`)
2. Finds a free port, spawns `vireo-server` sidecar with `--port N --no-browser`
3. Polls `GET /api/health` every 200ms until the sidecar responds (30s timeout)
4. Navigates the webview to `http://127.0.0.1:N`
5. On window close: sends `POST /api/shutdown` (with `X-Vireo-Shutdown` header), waits 500ms, force-kills if needed

In development (`cargo tauri dev`):
- No sidecar is spawned — the webview connects to `http://localhost:8080` (your manually-started Flask server)
- Closing the Tauri window does NOT kill your Flask server

## Code Signing (macOS)

Requires an Apple Developer account. Set these env vars:

```bash
export APPLE_SIGNING_IDENTITY="Developer ID Application: Name (TEAMID)"
export APPLE_ID="your@email.com"
export APPLE_PASSWORD="app-specific-password"
export APPLE_TEAM_ID="XXXXXXXXXX"
```

Then: `bash scripts/build_signed.sh`

Without signing, macOS Gatekeeper blocks the app. Users can bypass by right-clicking and selecting "Open".

## Auto-Updates

The updater checks GitHub Releases for new versions. To enable:

1. Generate signing keys: `cargo tauri signer generate -w ~/.tauri/vireo.key`
2. Put the public key in `src-tauri/tauri.conf.json` → `plugins.updater.pubkey`
3. Set `TAURI_SIGNING_PRIVATE_KEY` when building releases
4. Publish releases to GitHub — the updater checks automatically (24h cooldown)
