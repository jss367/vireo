#!/usr/bin/env python3
"""Fetch the pinned 64-bit Windows ExifTool distribution for packaging.

The archive is intentionally downloaded only on Windows release builders.  It
is verified against the checksum published by exiftool.org before extraction;
an upstream replacement at the same URL therefore fails the build instead of
silently entering a Vireo installer.
"""

from __future__ import annotations

import hashlib
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path

VERSION = "13.59"
SHA256 = "44b512b25af500724ba579d0a53c8fc5851628b692dd5e5d94ae4a15c2cba9ec"
URL = (
    "https://sourceforge.net/projects/exiftool/files/"
    f"exiftool-{VERSION}_64.zip/download"
)


def fetch(destination: Path) -> Path:
    executable = destination / "exiftool.exe"
    support_dir = destination / "exiftool_files"
    if executable.is_file() and support_dir.is_dir():
        return destination

    with tempfile.TemporaryDirectory(prefix="vireo-exiftool-") as tmp:
        archive = Path(tmp) / "exiftool.zip"
        request = urllib.request.Request(URL, headers={"User-Agent": "Vireo build"})
        with urllib.request.urlopen(request, timeout=120) as response, archive.open("wb") as out:
            shutil.copyfileobj(response, out)

        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        if digest != SHA256:
            raise RuntimeError(
                f"ExifTool {VERSION} checksum mismatch: expected {SHA256}, got {digest}"
            )

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
            f"ExifTool {VERSION}\nSource: {URL}\nSHA-256: {SHA256}\n",
            encoding="utf-8",
        )

    return destination


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    result = fetch(root / "build" / "vendor" / "exiftool")
    print(result)
