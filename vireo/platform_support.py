"""Platform capability discovery used by Settings and support diagnostics."""

from __future__ import annotations

import contextlib
import ctypes
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

from develop import find_darktable, find_dng_converter
from metadata import exiftool_status
from move import is_gnu_rsync, resolve_rsync_bin
from proc import no_window_kwargs


def _program_files_candidates(*relative_parts: str) -> list[str]:
    candidates = []
    for env_var in ("PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432"):
        base = os.environ.get(env_var)
        if base:
            candidate = os.path.join(base, *relative_parts)
            if candidate not in candidates:
                candidates.append(candidate)
    return candidates


def find_lightroom() -> str | None:
    if os.name != "nt":
        return None
    candidates = []
    for base in _program_files_candidates("Adobe"):
        root = Path(base)
        with contextlib.suppress(OSError):
            for child in root.glob("Adobe Lightroom Classic*"):
                candidates.extend(child.glob("lightroom.exe"))
                candidates.extend(child.glob("Lightroom.exe"))
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate.resolve())
    return None


def find_ssh(configured: str = "") -> str | None:
    if configured and os.path.isfile(configured):
        return os.path.abspath(configured)
    found = shutil.which("ssh")
    if found:
        return os.path.abspath(found)
    if os.name == "nt":
        system_root = os.environ.get("SYSTEMROOT", r"C:\Windows")
        candidate = os.path.join(system_root, "System32", "OpenSSH", "ssh.exe")
        if os.path.isfile(candidate):
            return candidate
    return None


def _probe(binary: str | None, args: list[str]) -> tuple[bool, str | None]:
    if not binary:
        return False, None
    try:
        result = subprocess.run(
            [binary, *args],
            capture_output=True,
            text=True,
            timeout=10,
            **no_window_kwargs(),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return False, str(exc)
    detail = (result.stdout or result.stderr or "").strip().splitlines()
    return result.returncode == 0, (detail[0] if detail else None)


def windows_long_path_status() -> dict:
    if os.name != "nt":
        return {"state": "not_applicable", "enabled": True, "detail": "Not Windows"}
    try:
        import winreg

        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SYSTEM\CurrentControlSet\Control\FileSystem",
        ) as key:
            value, _kind = winreg.QueryValueEx(key, "LongPathsEnabled")
        enabled = bool(value)
        return {
            "state": "ready" if enabled else "warning",
            "enabled": enabled,
            "detail": (
                "Windows long paths are enabled"
                if enabled
                else "Enable Win32 long paths in Windows policy before importing deeply nested libraries"
            ),
        }
    except OSError as exc:
        return {"state": "warning", "enabled": False, "detail": str(exc)}


def webview2_version() -> str | None:
    if os.name != "nt":
        return None
    try:
        import winreg

        roots = (
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Microsoft\EdgeUpdate\Clients"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\EdgeUpdate\Clients"),
            (winreg.HKEY_CURRENT_USER, r"Software\Microsoft\EdgeUpdate\Clients"),
        )
        for hive, root_name in roots:
            with contextlib.suppress(OSError), winreg.OpenKey(hive, root_name) as root:
                index = 0
                while True:
                    try:
                        child_name = winreg.EnumKey(root, index)
                    except OSError:
                        break
                    index += 1
                    with contextlib.suppress(OSError), winreg.OpenKey(root, child_name) as child:
                        product_name, _kind = winreg.QueryValueEx(child, "name")
                        if "webview2" not in str(product_name).lower():
                            continue
                        version, _kind = winreg.QueryValueEx(child, "pv")
                        if version:
                            return str(version)
    except ImportError:
        pass
    return None


def filesystem_type(path: str) -> str | None:
    """Return the Windows volume filesystem without exposing the input path."""
    if os.name != "nt" or not path:
        return None
    root = os.path.splitdrive(os.path.abspath(path))[0] + "\\"
    if path.startswith("\\\\"):
        parts = path.strip("\\").split("\\")
        if len(parts) >= 2:
            root = f"\\\\{parts[0]}\\{parts[1]}\\"
    fs_name = ctypes.create_unicode_buffer(64)
    try:
        ok = ctypes.windll.kernel32.GetVolumeInformationW(
            root, None, 0, None, None, None, fs_name, len(fs_name)
        )
    except (AttributeError, OSError):
        return None
    return fs_name.value if ok else None


def dependency_readiness(config: dict | None = None) -> dict:
    config = config or {}
    exif = exiftool_status()
    darktable = find_darktable(config.get("darktable_bin", ""))
    dng = find_dng_converter(config.get("dng_converter_bin", ""))
    lightroom = find_lightroom()
    ssh = find_ssh(config.get("ssh_bin", ""))
    ssh_ok, ssh_detail = _probe(ssh, ["-V"])
    rsync = resolve_rsync_bin(config.get("rsync_bin", ""))
    rsync_ok = bool(rsync and is_gnu_rsync(rsync))
    remote_ready = ssh_ok and rsync_ok

    return {
        "exiftool": {
            "required": True,
            "state": "ready" if exif["available"] else ("misconfigured" if exif["path"] else "missing"),
            **exif,
        },
        "darktable": {
            "required": False,
            "state": "ready" if darktable else "missing",
            "path": darktable,
            "hint": "Install Darktable or configure darktable-cli under Settings → Paths.",
        },
        "dng_converter": {
            "required": False,
            "state": "ready" if dng else "missing",
            "path": dng,
            "hint": "Install Adobe DNG Converter or configure its executable under Settings → Paths.",
        },
        "lightroom": {
            "required": False,
            "state": "ready" if lightroom else "missing",
            "path": lightroom,
            "hint": "Lightroom Classic is optional; catalog import remains available when installed.",
        },
        "openssh": {
            "required": False,
            "state": "ready" if ssh_ok else ("misconfigured" if ssh else "missing"),
            "path": ssh,
            "version": ssh_detail,
            "hint": "Install the Windows OpenSSH Client optional feature or configure ssh.exe.",
        },
        "rsync": {
            "required": False,
            "state": "ready" if rsync_ok else ("misconfigured" if rsync else "missing"),
            "path": rsync,
            "hint": "Install GNU rsync and configure its executable under Settings → Paths.",
        },
        "remote_transfer": {
            "required": False,
            "state": "ready" if remote_ready else "unavailable",
            "hint": "Remote import, archive, and move require both OpenSSH Client and GNU rsync.",
        },
    }


def platform_support_info(config: dict | None = None) -> dict:
    windows_release = platform.release() if os.name == "nt" else None
    windows_build = platform.version() if os.name == "nt" else None
    windows_11 = False
    if os.name == "nt":
        with contextlib.suppress(ValueError, IndexError):
            windows_11 = int((windows_build or "").split(".")[-1]) >= 22000
        windows_11 = windows_11 or windows_release == "11"
    return {
        "support_tier": (
            "public_beta" if windows_11 else "unsupported"
        ) if os.name == "nt" else "supported",
        "platform": sys.platform,
        "windows_release": windows_release,
        "windows_build": windows_build,
        "architecture": platform.machine(),
        "guaranteed_inference": "CPUExecutionProvider" if os.name == "nt" else None,
        "webview2_version": webview2_version(),
        "long_paths": windows_long_path_status(),
        "dependencies": dependency_readiness(config),
    }
