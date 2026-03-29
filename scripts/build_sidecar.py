#!/usr/bin/env python3
"""Build the Vireo sidecar binary for Tauri.

Usage:
    python scripts/build_sidecar.py [--ci]

Produces: src-tauri/binaries/vireo-server-<target-triple>
"""
import argparse
import os
import platform
import shutil
import subprocess
import sys


def sign_binary(binary_path, entitlements_path=None):
    """Sign the binary with the hardened runtime for macOS notarization."""
    if platform.system() != "Darwin":
        return

    identity = os.environ.get("APPLE_SIGNING_IDENTITY")
    if not identity:
        print("WARNING: APPLE_SIGNING_IDENTITY not set, skipping code signing")
        return

    cmd = [
        "codesign",
        "--sign", identity,
        "--options", "runtime",       # Enable hardened runtime
        "--timestamp",                # Use Apple's timestamp server
        "--force",                    # Replace any existing signature
    ]
    if entitlements_path and os.path.exists(entitlements_path):
        cmd.extend(["--entitlements", entitlements_path])
    cmd.append(binary_path)

    print(f"Signing: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print(f"Signed: {binary_path}")

    # Verify the signature
    subprocess.run(
        ["codesign", "--verify", "--verbose", binary_path],
        check=True,
    )
    print(f"Signature verified: {binary_path}")


def get_target_triple():
    """Get the Rust target triple for the current platform."""
    result = subprocess.run(
        ["rustc", "--print", "host-tuple"],
        capture_output=True, text=True,
    )
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description="Build sidecar binary")
    parser.add_argument(
        "--ci", action="store_true",
        help="Apply CI optimizations (strip debug symbols, exclude test packages)",
    )
    args = parser.parse_args()

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    target = get_target_triple()
    print(f"Building sidecar for target: {target}")

    # Platform-specific path separator for --add-data
    sep = ";" if platform.system() == "Windows" else ":"
    vireo_dir = os.path.join(repo_root, "vireo")

    pyinstaller_args = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",
        "--name", "vireo-server",
        "--paths", vireo_dir,
        # Bundle Flask templates and static assets — destinations are
        # relative to _MEIPASS, and Flask resolves them relative to
        # os.path.dirname(__file__) which is _MEIPASS for the entry script.
        "--add-data", f"{os.path.join(vireo_dir, 'templates')}{sep}templates",
        "--add-data", f"{os.path.join(vireo_dir, 'static')}{sep}static",
        "--hidden-import", "config",
        "--hidden-import", "db",
        "--hidden-import", "jobs",
        "--hidden-import", "scanner",
        "--hidden-import", "classifier",
        "--hidden-import", "classify_job",
        "--hidden-import", "thumbnails",
        "--hidden-import", "pipeline",
        "--hidden-import", "audit",
        "--hidden-import", "sync",
        "--hidden-import", "importer",
        "--hidden-import", "labels",
        "--hidden-import", "taxonomy",
        "--hidden-import", "models",
        "--hidden-import", "quality",
        "--hidden-import", "sharpness",
        "--hidden-import", "culling",
        "--hidden-import", "selection",
        "--hidden-import", "encounters",
        "--hidden-import", "grouping",
        "--hidden-import", "masking",
        "--hidden-import", "scoring",
        "--hidden-import", "compare",
        "--hidden-import", "develop",
        "--hidden-import", "dino_embed",
        "--hidden-import", "image_loader",
        "--hidden-import", "label_photos",
        "--hidden-import", "analyze",
        "--hidden-import", "bursts",
        "--hidden-import", "detector",
        "--hidden-import", "timm_classifier",
        "--hidden-import", "text_encoder",
        "--hidden-import", "xmp",
        "--hidden-import", "catalog",
        # ONNX Runtime and related libraries
        "--hidden-import", "onnx_runtime",
        "--hidden-import", "onnxruntime",
        "--hidden-import", "tokenizers",
    ]

    if args.ci:
        # Exclude packages that bloat the binary but aren't needed at runtime
        pyinstaller_args += [
            "--exclude-module", "tkinter",
            "--exclude-module", "matplotlib",
            "--exclude-module", "notebook",
            "--exclude-module", "jupyter",
            "--exclude-module", "IPython",
            # Safety: prevent accidental bundling of PyTorch via transitive deps
            "--exclude-module", "torch",
            "--exclude-module", "torchvision",
            "--exclude-module", "lightning",
            "--strip",
        ]

    pyinstaller_args.append(os.path.join(repo_root, "vireo", "app.py"))

    # Run PyInstaller
    subprocess.run(pyinstaller_args, cwd=repo_root, check=True)

    # Copy to Tauri binaries directory with target triple suffix
    src = os.path.join(repo_root, "dist", "vireo-server")
    if platform.system() == "Windows":
        src += ".exe"

    dest_dir = os.path.join(repo_root, "src-tauri", "binaries")
    os.makedirs(dest_dir, exist_ok=True)

    dest_name = f"vireo-server-{target}"
    if platform.system() == "Windows":
        dest_name += ".exe"
    dest = os.path.join(dest_dir, dest_name)

    shutil.copy2(src, dest)
    os.chmod(dest, 0o755)

    # Report size
    size_mb = os.path.getsize(dest) / (1024 * 1024)
    print(f"Sidecar binary: {dest} ({size_mb:.1f} MB)")

    # Sign the sidecar with hardened runtime
    entitlements = os.path.join(repo_root, "src-tauri", "Entitlements.plist")
    sign_binary(dest, entitlements)


if __name__ == "__main__":
    main()
