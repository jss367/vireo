#!/usr/bin/env python3
"""Fetch the pinned ExifTool distribution for desktop packaging.

The archive is downloaded only by release builders.  It is verified against a
pinned checksum before extraction; an upstream replacement at the same URL
therefore fails the build instead of silently entering a Vireo installer.
"""

from __future__ import annotations

import hashlib
import shutil
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path

VERSION = "13.59"
WINDOWS_SHA256 = "44b512b25af500724ba579d0a53c8fc5851628b692dd5e5d94ae4a15c2cba9ec"
WINDOWS_URL = (
    "https://sourceforge.net/projects/exiftool/files/"
    f"exiftool-{VERSION}_64.zip/download"
)
UNIX_SHA256 = "668ea3acececb7235fbd0f4900e72d5f12c9b07e5c778fd36cb1e9b5828fd65a"
UNIX_URL = (
    "https://sourceforge.net/projects/exiftool/files/"
    f"Image-ExifTool-{VERSION}.tar.gz/download"
)


def _download(url: str, archive: Path, expected_sha256: str) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "Vireo build"})
    with urllib.request.urlopen(request, timeout=120) as response, archive.open("wb") as out:
        shutil.copyfileobj(response, out)

    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    if digest != expected_sha256:
        raise RuntimeError(
            f"ExifTool {VERSION} checksum mismatch: expected {expected_sha256}, got {digest}"
        )


def _fetch_windows(destination: Path) -> Path:
    executable = destination / "exiftool.exe"
    support_dir = destination / "exiftool_files"
    if executable.is_file() and support_dir.is_dir():
        return destination

    with tempfile.TemporaryDirectory(prefix="vireo-exiftool-") as tmp:
        archive = Path(tmp) / "exiftool.zip"
        _download(WINDOWS_URL, archive, WINDOWS_SHA256)

        extracted = Path(tmp) / "extracted"
        with zipfile.ZipFile(archive) as bundle:
            bundle.extractall(extracted)

        roots = [path for path in extracted.iterdir() if path.is_dir()]
        if len(roots) != 1:
            raise RuntimeError("Unexpected ExifTool archive layout")
        source = roots[0]
        source_exe = source / "exiftool(-k).exe"
        source_support = source / "exiftool_files"
        if not source_exe.is_file() or not source_support.is_dir():
            raise RuntimeError("ExifTool archive is missing its executable or support directory")

        if destination.exists():
            shutil.rmtree(destination)
        destination.mkdir(parents=True)
        shutil.copy2(source_exe, executable)
        shutil.copytree(source_support, support_dir)
        (destination / "VERSION.txt").write_text(
            f"ExifTool {VERSION}\nSource: {WINDOWS_URL}\nSHA-256: {WINDOWS_SHA256}\n",
            encoding="utf-8",
        )

    return destination


def _fetch_unix(destination: Path) -> Path:
    """Fetch the portable Perl application used by macOS builds.

    ExifTool's documented portable layout is the ``exiftool`` script beside
    its ``lib`` directory.  Copy only that runtime plus the upstream README
    (which contains its copyright and dual Artistic/GPL license statement),
    avoiding the large HTML/test portions of the source distribution.
    """
    executable = destination / "exiftool"
    support_dir = destination / "lib"
    if executable.is_file() and support_dir.is_dir():
        return destination

    with tempfile.TemporaryDirectory(prefix="vireo-exiftool-") as tmp:
        archive = Path(tmp) / "exiftool.tar.gz"
        _download(UNIX_URL, archive, UNIX_SHA256)
        root = f"Image-ExifTool-{VERSION}/"

        if destination.exists():
            shutil.rmtree(destination)
        destination.mkdir(parents=True)

        with tarfile.open(archive, "r:gz") as bundle:
            copied_runtime = False
            copied_readme = False
            for member in bundle.getmembers():
                if not member.isfile() or not member.name.startswith(root):
                    continue
                relative = member.name[len(root):]
                if relative == "exiftool":
                    target_relative = Path("exiftool")
                    copied_runtime = True
                elif relative == "README":
                    target_relative = Path("README")
                    copied_readme = True
                elif relative.startswith("lib/"):
                    target_relative = Path(relative)
                else:
                    continue
                source = bundle.extractfile(member)
                if source is None:
                    raise RuntimeError(f"Unable to extract ExifTool member: {member.name}")
                target = destination / target_relative
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open("wb") as out:
                    shutil.copyfileobj(source, out)

        if not copied_runtime or not copied_readme or not support_dir.is_dir():
            raise RuntimeError("ExifTool archive is missing its script, libraries, or README")
        executable.chmod(0o755)
        (destination / "VERSION.txt").write_text(
            f"ExifTool {VERSION}\nSource: {UNIX_URL}\nSHA-256: {UNIX_SHA256}\n",
            encoding="utf-8",
        )

    return destination


def fetch(destination: Path, platform_name: str | None = None) -> Path:
    platform_name = platform_name or sys.platform
    if platform_name.startswith("win"):
        return _fetch_windows(destination)
    if platform_name == "darwin":
        return _fetch_unix(destination)
    raise RuntimeError(f"Bundled ExifTool is not configured for {platform_name}")


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    result = fetch(root / "build" / "vendor" / "exiftool")
    print(result)
