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
    """Update all per-platform version constants in an Astro file."""
    with open(path) as f:
        content = f.read()
    # Update all per-platform version constants
    platforms = ["macosArm64Version", "macosX86Version", "windowsVersion", "linuxVersion"]
    count = 0
    for plat in platforms:
        content, n = re.subn(
            rf"const {plat} = '[^']*'",
            f"const {plat} = '{version}'",
            content,
            count=1,
        )
        count += n
    if count == 0:
        print(f"  {path}: WARNING - no version constants found")
        return
    with open(path, "w") as f:
        f.write(content)
    print(f"  {path}: updated {count} platform versions to {version}")


def update_astro_platform_version(path, platform, version):
    """Update a single platform version constant in an Astro file."""
    var_map = {
        "macos-arm64": "macosArm64Version",
        "macos-x86_64": "macosX86Version",
        "windows-x86_64": "windowsVersion",
        "linux-x86_64": "linuxVersion",
    }
    var_name = var_map.get(platform)
    if not var_name:
        print(f"  {path}: WARNING - unknown platform '{platform}'")
        return
    with open(path) as f:
        content = f.read()
    new_content, count = re.subn(
        rf"const {var_name} = '[^']*'",
        f"const {var_name} = '{version}'",
        content,
        count=1,
    )
    if count == 0:
        print(f"  {path}: WARNING - {var_name} not found")
        return
    with open(path, "w") as f:
        f.write(new_content)
    print(f"  {path}: {var_name} -> {version}")


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
    # Parse --platform values before stripping flags
    platform_flags = []
    skip_next = False
    positional = []
    for i, a in enumerate(sys.argv[1:], 1):
        if skip_next:
            skip_next = False
            continue
        if a == "--platform":
            if i + 1 < len(sys.argv):
                platform_flags.append(sys.argv[i + 1])
                skip_next = True
            continue
        if a.startswith("--"):
            continue
        positional.append(a)

    if len(positional) != 1:
        print(f"Usage: {sys.argv[0]} <version> [--include-website] [--platform LABEL]")
        sys.exit(1)

    version = positional[0].lstrip("v")
    if platform_flags:
        path = "website/src/pages/download.astro"
        for plat in platform_flags:
            update_astro_platform_version(path, plat, version)
        print("Done.")
        return

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
