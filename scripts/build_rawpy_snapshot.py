#!/usr/bin/env python3
"""Temporary rawpy wheel for Nikon HE/HE*: build, install, or --check.

Remove this builder once a published rawpy wheel includes LibRaw 202611.
See docs/nikon-high-efficiency.md for prerequisites and the removal checklist.
"""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

RAWPY_COMMIT = "2705f5b223c162c3cbc48474fe1a01c3cf09fa4f"  # v0.28.0
LIBRAW_COMMIT = "a9a14b1970d188c25dade81f8326ff00a6e33649"  # snapshot 202611 + upstream fixes
CMAKE_COMMIT = "6e26c9e73677dc04f9eb236a97c6a4dc225ba7e8"  # rawpy's CMake submodule
VCPKG_COMMIT = "9e593bb18ea69cc5095e012465dcd675a822ed0d"  # 2026.07.29
VERSION = "0.28.0+vireo.libraw202611"
LIBRAW_VERSION = (0, 23, 0)
REQUIRED_FLAGS = ("LCMS", "DNGLOSSYCODEC", "DNGDEFLATECODEC", "REDCINECODEC", "X3FTOOLS", "6BY9RPI")


def run(args, *, cwd=None, env=None):
    subprocess.run([str(arg) for arg in args], cwd=cwd, env=env, check=True)


def checkout(repository, commit, destination):
    """Fetch only an immutable revision; do not follow branches or submodules."""
    run(["git", "init", "--quiet", destination])
    run(["git", "fetch", "--quiet", "--depth=1", f"https://github.com/{repository}.git", commit], cwd=destination)
    run(["git", "checkout", "--quiet", "--detach", "FETCH_HEAD"], cwd=destination)
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=destination, text=True).strip()
    if actual != commit:
        raise RuntimeError(f"Wrong source revision for {repository}: {actual}")


def triplet():
    machine = platform.machine().lower()
    arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "x64", "amd64": "x64"}.get(machine)
    system = {"darwin": "osx", "linux": "linux", "win32": "windows-static"}.get(sys.platform)
    if not arch or not system:
        raise RuntimeError(f"Unsupported native build: {sys.platform}/{machine}")
    return f"{arch}-{system}"


def build_dependencies(root, env):
    """Static codec dependencies keep the wheel independent of system installs."""
    vcpkg = root / "vcpkg"
    checkout("microsoft/vcpkg", VCPKG_COMMIT, vcpkg)
    env["VCPKG_ROOT"] = str(vcpkg)
    if sys.platform == "win32":
        run(["cmd", "/c", str(vcpkg / "bootstrap-vcpkg.bat"), "-disableMetrics"], env=env)
    else:
        run([vcpkg / "bootstrap-vcpkg.sh", "-disableMetrics"], env=env)
    target = triplet()
    if sys.platform == "darwin":
        # Match Vireo's minimum OS, including the static libraries. An overlay
        # makes this part of vcpkg's ABI/cache key, unlike an ambient compiler flag.
        overlay = root / "triplets"
        overlay.mkdir()
        arch = target.split("-", 1)[0]
        (overlay / f"{target}.cmake").write_text(
            f"set(VCPKG_TARGET_ARCHITECTURE {arch})\n"
            "set(VCPKG_CRT_LINKAGE dynamic)\nset(VCPKG_LIBRARY_LINKAGE static)\n"
            "set(VCPKG_CMAKE_SYSTEM_NAME Darwin)\nset(VCPKG_OSX_DEPLOYMENT_TARGET 11.0)\n")
        env["VCPKG_OVERLAY_TRIPLETS"] = str(overlay)
    run([vcpkg / ("vcpkg.exe" if sys.platform == "win32" else "vcpkg"), "install",
         "zlib", "libjpeg-turbo[jpeg8]", "jasper", "lcms", f"--triplet={target}", "--recurse"], env=env)
    prefix = vcpkg / "installed" / target
    env["CMAKE_PREFIX_PATH"] = str(prefix)
    env["CMAKE_TOOLCHAIN_FILE"] = str(vcpkg / "scripts/buildsystems/vcpkg.cmake")
    env["VCPKG_TARGET_TRIPLET"] = target
    return prefix


def prepare_sources(root, prefix):
    source = root / "rawpy"
    checkout("letmaik/rawpy", RAWPY_COMMIT, source)
    checkout("LibRaw/LibRaw", LIBRAW_COMMIT, source / "external/LibRaw")
    checkout("LibRaw/LibRaw-cmake", CMAKE_COMMIT, source / "external/LibRaw-cmake")
    (source / "rawpy/_version.py").write_text(f'__version__ = "{VERSION}"\n')
    provenance = {"rawpy": RAWPY_COMMIT, "libraw": LIBRAW_COMMIT, "libraw_cmake": CMAKE_COMMIT,
                  "vcpkg": VCPKG_COMMIT, "version": VERSION}
    (source / "rawpy/vireo-build.json").write_text(json.dumps(provenance, indent=2) + "\n")
    # The licenses travel with both the wheel and the frozen app's metadata.
    licenses = source / "vireo-licenses"
    licenses.mkdir()
    for name in ("COPYRIGHT", "LICENSE.LGPL", "LICENSE.CDDL"):
        shutil.copyfile(source / "external/LibRaw" / name, licenses / f"LibRaw-{name}")
    for copyright_file in (prefix / "share").glob("*/copyright"):
        shutil.copyfile(copyright_file, licenses / f"{copyright_file.parent.name}-LICENSE.txt")
    project = source / "pyproject.toml"
    project.write_text(project.read_text().replace('license = "MIT"',
                       'license = "MIT AND LGPL-2.1-only"\nlicense-files = ["LICENSE", "vireo-licenses/*"]'))
    setup = source / "setup.py"
    setup.write_text(setup.read_text().replace('["py.typed", "*.pyi"]',
                     '["py.typed", "*.pyi", "vireo-build.json"]'))
    return source


def check_installation():
    """Fail closed before freezing an older or incompatible decoder."""
    import rawpy

    provenance_path = Path(rawpy.__file__).with_name("vireo-build.json")
    if rawpy.__version__ != VERSION or tuple(rawpy.libraw_version) != LIBRAW_VERSION:
        raise RuntimeError(f"Expected rawpy {VERSION} / LibRaw {LIBRAW_VERSION}; got "
                           f"{rawpy.__version__} / {rawpy.libraw_version}. "
                           "Run python scripts/build_rawpy_snapshot.py first.")
    if tuple(rawpy.libraw_version_compiled) != LIBRAW_VERSION:
        raise RuntimeError("rawpy was compiled against different LibRaw headers")
    provenance = json.loads(provenance_path.read_text())
    if provenance != {"rawpy": RAWPY_COMMIT, "libraw": LIBRAW_COMMIT, "libraw_cmake": CMAKE_COMMIT,
                      "vcpkg": VCPKG_COMMIT, "version": VERSION}:
        raise RuntimeError("rawpy snapshot source provenance does not match")
    missing = [name for name in REQUIRED_FLAGS if not rawpy.flags.get(name)]
    if missing:
        raise RuntimeError(f"Custom LibRaw lost existing codec features: {missing}")
    print(f"RAW decoder: rawpy {rawpy.__version__}, LibRaw {rawpy.libraw_version}; codecs verified")


def build(output):
    output.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    # Do not inherit a machine-specific system LibRaw or enable GPL demosaic packs.
    env["RAWPY_USE_SYSTEM_LIBRAW"] = "0"
    env["RAWPY_BUILD_GPL_CODE"] = "0"
    env["CMAKE_BUILD_PARALLEL_LEVEL"] = "4"
    env["VCPKG_MAX_CONCURRENCY"] = "4"
    if sys.platform == "darwin":
        env["MACOSX_DEPLOYMENT_TARGET"] = "11.0"
    with tempfile.TemporaryDirectory(prefix="vireo-rawpy-") as directory:
        root = Path(directory)
        prefix = build_dependencies(root, env)
        source = prepare_sources(root, prefix)
        # Publish the complete modified library/wrapper sources alongside releases.
        archive = output / f"vireo-rawpy-sources-{sys.platform}-{platform.machine().lower()}.tar.gz"
        with tarfile.open(archive, "w:gz") as bundle:
            bundle.add(source, arcname="rawpy", filter=source_filter)
            bundle.add(__file__, arcname="build_rawpy_snapshot.py")
        wheel_dir = root / "wheels"
        wheel_dir.mkdir()
        run([sys.executable, "-m", "pip", "wheel", source, "--no-deps", "--wheel-dir", wheel_dir], env=env)
        wheels = list(wheel_dir.glob(f"rawpy-{VERSION}-*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"Expected one newly built rawpy wheel, found {wheels}")
        wheel = output / wheels[0].name
        shutil.copyfile(wheels[0], wheel)
    run([sys.executable, "-m", "pip", "install", "--force-reinstall", "--no-deps", wheel])
    # A new interpreter verifies the installed wheel, not cached imports.
    run([sys.executable, __file__, "--check"])


def source_filter(item):
    # The upstream test folder holds large camera photographs, not library sources.
    if ".git" in Path(item.name).parts or item.name.startswith("rawpy/test"):
        return None
    return item


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Verify the installed decoder without building")
    parser.add_argument("--output", type=Path, default=Path("build/rawpy-snapshot"))
    args = parser.parse_args()
    if args.check:
        check_installation()
    else:
        build(args.output.resolve())


if __name__ == "__main__":
    main()
