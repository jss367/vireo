"""Test-profile path resolution and safety guard.

The guard refuses to start the harness against real data. Violations
are hard errors (`UnsafeProfileError`), never warnings.
"""
import os
import sqlite3
from pathlib import Path


class UnsafeProfileError(RuntimeError):
    """The configured profile would touch real data."""


def _reject_if_unsafe(path, env_name):
    home = Path.home().resolve()
    if path == home:
        raise UnsafeProfileError(f"{env_name} cannot be $HOME: {path}")
    real_vireo = (home / ".vireo").resolve()
    if path == real_vireo:
        raise UnsafeProfileError(f"{env_name} cannot be ~/.vireo/: {path}")
    try:
        path.relative_to(real_vireo)
    except ValueError:
        return
    raise UnsafeProfileError(f"{env_name} cannot be under ~/.vireo/: {path}")


def resolve_profile():
    """Resolve and validate the test profile path from VIREO_PROFILE.

    Raises UnsafeProfileError if the env var is unset or points at real data.
    """
    raw = os.environ.get("VIREO_PROFILE")
    if not raw:
        raise UnsafeProfileError(
            "VIREO_PROFILE is not set — the harness refuses to start without "
            "an explicit test profile directory"
        )
    path = Path(raw).expanduser().resolve()
    _reject_if_unsafe(path, "VIREO_PROFILE")
    return path


def resolve_photos_root():
    """Resolve and validate the test photos root from VIREO_TEST_PHOTOS.

    Returns None if unset (photo-folder validation is skipped).
    Raises UnsafeProfileError if set but unsafe.
    """
    raw = os.environ.get("VIREO_TEST_PHOTOS")
    if not raw:
        return None
    path = Path(raw).expanduser().resolve()
    _reject_if_unsafe(path, "VIREO_TEST_PHOTOS")
    return path


def profile_paths(profile_dir):
    """Standard paths under a profile directory."""
    p = Path(profile_dir)
    return {
        "db": p / "vireo.db",
        "thumbnails": p / "thumbnails",
        "labels": p / "labels",
        "config": p / "config.json",
        "runs": p / "runs",
    }


def validate_profile_tree(profile_dir):
    """Reject any entry under `profile_dir` that symlinks outside it.

    `resolve_profile()` only checks the profile directory path itself. Without
    this walk, a child like `<profile>/vireo.db -> ~/.vireo/vireo.db` would
    still pass the outer guard and let the harness read/write real data via
    the symlink. Every symlink in the tree must resolve to a target inside
    the profile, or the harness refuses to start.
    """
    profile = Path(profile_dir).resolve()
    if not profile.exists():
        return
    for root, dirs, files in os.walk(profile, followlinks=False):
        root_path = Path(root)
        for name in list(dirs) + list(files):
            entry = root_path / name
            if not entry.is_symlink():
                continue
            target = Path(os.path.realpath(entry))
            try:
                target.relative_to(profile)
            except ValueError as err:
                raise UnsafeProfileError(
                    f"symlink inside profile escapes it: {entry} -> {target}"
                ) from err


def validate_db_folders(db_path, photos_root):
    """Ensure every folder in the DB lives under `photos_root`.

    No-op if `photos_root` is None. Raises UnsafeProfileError on violation.
    """
    if photos_root is None:
        return
    photos_root = Path(photos_root).resolve()
    conn = sqlite3.connect(str(db_path))
    try:
        try:
            cur = conn.execute("SELECT path FROM folders")
        except sqlite3.OperationalError:
            return
        for (path_str,) in cur:
            if not path_str:
                continue
            folder = Path(path_str).expanduser().resolve()
            try:
                folder.relative_to(photos_root)
            except ValueError as err:
                raise UnsafeProfileError(
                    f"DB references folder outside photos root: {folder} "
                    f"(allowed root: {photos_root})"
                ) from err
    finally:
        conn.close()
