#!/usr/bin/env python3
"""Synchronize version across all project manifests.

Usage:
    python scripts/sync_version.py 0.1.0
"""
import json
import re
import sys


def update_json_file(path, version):
    """Update the 'version' field in a JSON file."""
    with open(path) as f:
        data = json.load(f)
    old = data.get("version", "unknown")
    data["version"] = version
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    print(f"  {path}: {old} -> {version}")


def update_astro_version(path, version):
    """Update the version constant in an Astro file."""
    with open(path) as f:
        content = f.read()
    new_content, count = re.subn(
        r"const version = '[^']*'",
        f"const version = '{version}'",
        content,
        count=1,
    )
    if count == 0:
        print(f"  {path}: WARNING - no version constant found")
        return
    with open(path, "w") as f:
        f.write(new_content)
    print(f"  {path}: updated to {version}")


def update_toml_file(path, version):
    """Update the version in a TOML file (simple regex replacement)."""
    with open(path) as f:
        content = f.read()
    new_content, count = re.subn(
        r'^version\s*=\s*"[^"]*"',
        f'version = "{version}"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
    if count == 0:
        print(f"  {path}: WARNING - no version field found")
        return
    with open(path, "w") as f:
        f.write(new_content)
    print(f"  {path}: updated to {version}")


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) != 1:
        print(f"Usage: {sys.argv[0]} <version> [--include-website]")
        sys.exit(1)

    version = args[0].lstrip("v")
    print(f"Syncing version to {version}")

    update_json_file("src-tauri/tauri.conf.json", version)
    update_json_file("package.json", version)
    update_toml_file("src-tauri/Cargo.toml", version)
    update_toml_file("pyproject.toml", version)

    # download.astro is updated by the build-release workflow AFTER
    # artifacts are uploaded, so the website never points to a 404.
    if "--include-website" in sys.argv:
        update_astro_version("website/src/pages/download.astro", version)

    print("Done.")


if __name__ == "__main__":
    main()
