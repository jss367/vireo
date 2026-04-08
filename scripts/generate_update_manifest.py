#!/usr/bin/env python3
"""Generate latest.json for Tauri auto-updater from release artifacts.

Usage: generate_update_manifest.py <tag> <release-dir>

Scans <release-dir> for signed updater artifacts and writes a latest.json
manifest that the Tauri updater plugin can consume.

Tauri v2 with ``createUpdaterArtifacts: true`` produces:
  - macOS:   <App>_<ver>_<arch>.app.tar.gz  + .app.tar.gz.sig
  - Windows: <App>_<ver>_<arch>-setup.exe   + .exe.sig   (NSIS installer)
             <App>_<ver>_<arch>.msi          + .msi.sig   (MSI installer)
  - Linux:   <App>_<ver>_<arch>.AppImage    + .AppImage.sig

The manifest ``url`` points to the installer; ``signature`` is the content
of the matching ``.sig`` sidecar.
"""

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO = "jss367/vireo"

# Each entry: (installer suffix, sig suffix, Tauri platform key)
# The sig suffix is the extension appended to the installer filename.
PLATFORM_MAP = [
    # macOS — .app.tar.gz bundle + .app.tar.gz.sig
    ("_aarch64.app.tar.gz", ".sig", "darwin-aarch64"),
    # Windows — NSIS .exe installer + .exe.sig  (preferred over .msi)
    ("-setup.exe", ".sig", "windows-x86_64"),
    # Linux — .AppImage + .AppImage.sig
    (".AppImage", ".sig", "linux-x86_64"),
]


def generate(tag: str, release_dir: Path) -> dict | None:
    version = tag.lstrip("v")
    base_url = f"https://github.com/{REPO}/releases/download/{tag}"

    platforms = {}
    for suffix, sig_ext, key in PLATFORM_MAP:
        matches = [
            f
            for f in release_dir.iterdir()
            if f.name.endswith(suffix) and not f.name.endswith(".sig")
        ]
        if not matches:
            print(f"  skip {key}: no *{suffix}")
            continue
        artifact = matches[0]
        sig_path = Path(str(artifact) + sig_ext)
        if not sig_path.exists():
            print(f"  skip {key}: missing {sig_path.name}")
            continue
        platforms[key] = {
            "url": f"{base_url}/{artifact.name}",
            "signature": sig_path.read_text().strip(),
        }

    if not platforms:
        return None

    return {
        "version": version,
        "notes": f"https://github.com/{REPO}/releases/tag/{tag}",
        "pub_date": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "platforms": platforms,
    }


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <tag> <release-dir>")
        sys.exit(1)

    tag = sys.argv[1]
    release_dir = Path(sys.argv[2])
    manifest = generate(tag, release_dir)

    if manifest is None:
        print("No updater artifacts found — skipping latest.json")
        return

    out = release_dir / "latest.json"
    out.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
