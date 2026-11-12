#!/usr/bin/env python3
"""Freeze the decoder with the desktop hook and decode real HE/HE* sensor data."""

import argparse
import os
import subprocess
import sys
from pathlib import Path

from build_rawpy_snapshot import check_installation
from check_nikon_raw import SAMPLES, check_raw


def check_licenses():
    from importlib.metadata import distribution

    package = distribution("rawpy")
    licenses = {item.name: item for item in package.files or () if "/licenses/" in str(item)}
    for name in ("LICENSE", "LibRaw-COPYRIGHT", "LibRaw-LICENSE.LGPL", "jasper-LICENSE.txt",
                 "lcms-LICENSE.txt", "libjpeg-turbo-LICENSE.txt", "zlib-LICENSE.txt"):
        if name not in licenses or not package.locate_file(licenses[name]).read_text().strip():
            raise RuntimeError(f"Frozen RAW decoder is missing its license: {name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=Path, default=Path("build/nikon-raw-samples"))
    parser.add_argument("--output", type=Path, default=Path("build/raw-decoder-smoke"))
    args = parser.parse_args()
    if getattr(sys, "frozen", False):
        check_installation()
        check_licenses()
        for name, _, _ in SAMPLES:
            print(check_raw(args.samples / f"{name}.NEF"), flush=True)
        return
    root = Path(__file__).resolve().parents[1]
    destination = args.output.resolve()
    subprocess.run([
        sys.executable, "-m", "PyInstaller", "--noconfirm", "--clean", "--onefile",
        "--name", "raw-decoder-smoke", "--paths", str(root / "scripts"),
        "--additional-hooks-dir", str(root / "scripts/pyinstaller_hooks"),
        "--exclude-module", "matplotlib", "--exclude-module", "scipy",
        "--exclude-module", "pandas", "--exclude-module", "IPython",
        "--exclude-module", "pytest", "--exclude-module", "tkinter",
        "--distpath", str(destination / "dist"),
        "--workpath", str(destination / "build"), "--specpath", str(destination),
        str(Path(__file__).resolve()),
    ], check=True, cwd=root)
    binary = destination / "dist" / ("raw-decoder-smoke.exe" if sys.platform == "win32" else "raw-decoder-smoke")
    # Do not let a local library search path conceal missing bundled libraries.
    env = {key: value for key, value in os.environ.items()
           if key not in ("DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH", "LD_LIBRARY_PATH", "PYTHONPATH")}
    subprocess.run([str(binary), "--samples", str(args.samples.resolve())],
                   cwd=destination, env=env, check=True, timeout=180)


if __name__ == "__main__":
    main()
