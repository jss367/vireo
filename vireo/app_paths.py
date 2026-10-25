"""Storage defaults shared with the desktop launcher.

An existing ~/.vireo (even an empty directory or broken symlink) keeps the
entire legacy layout. A native-install marker keeps that choice stable if an
older Vireo version later creates ~/.vireo. Fresh installations use data/cache
folders. Resolution does not create, move, or delete files. Explicit caller
paths take precedence; this module supplies defaults only.
"""

import json
import os
import sys
from pathlib import Path

_LAYOUT = json.loads(
    (Path(__file__).parent / "data" / "app_paths.json").read_text(encoding="utf-8")
)


def _entry_exists(path: str) -> bool:
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError:
        # An inaccessible legacy directory must never look like a fresh install.
        return True
    return True


def _native_root(kind: str, home: str) -> str:
    platform = sys.platform if sys.platform in _LAYOUT["roots"] else "linux"
    spec = _LAYOUT["roots"][platform][kind]
    override = os.environ.get(spec.get("env", ""), "")
    base = override if os.path.isabs(override) else os.path.join(home, *spec["fallback"].split("/"))
    return os.path.join(base, *spec["suffix"].split("/"))


def _uses_legacy(home: str) -> bool:
    marker = os.path.join(_native_root("data", home), _LAYOUT["native_marker"])
    return not _entry_exists(marker) and _entry_exists(os.path.join(home, ".vireo"))


def initialize_layout() -> None:
    """Pin a fresh install's choice before startup writes any user-selected paths.

    The empty versioned marker is created exclusively, never overwriting a file.
    Existing legacy installs are untouched. Both launcher and server do this;
    resolving defaults alone remains read-only.
    """
    if os.path.lexists(migration_journal()):
        raise RuntimeError(
            "Storage migration is unfinished. Quit Vireo and run "
            "--migrate-storage apply to resume or --migrate-storage rollback to restore."
        )
    home = os.path.expanduser("~")
    if _uses_legacy(home):
        return
    root = _native_root("data", home)
    os.makedirs(root, exist_ok=True)
    try:
        with open(os.path.join(root, _LAYOUT["native_marker"]), "x", encoding="utf-8"):
            pass
    except FileExistsError:
        pass


def migration_journal() -> str:
    """Recovery record outside both storage layouts, shared with the launcher."""
    return os.path.join(os.path.expanduser("~"), _LAYOUT["migration_journal"])


def native_roots() -> tuple[str, str]:
    """Platform destinations even when an existing install selects legacy paths."""
    home = os.path.expanduser("~")
    return _native_root("data", home), _native_root("cache", home)


def default_path(name: str, *, expand_user: bool = True) -> str:
    """Resolve a default using the current home, preserving existing installs.

    ``expand_user=False`` defers home expansion where the default is home-relative;
    absolute XDG/Windows environment overrides remain absolute.
    """
    legacy = _LAYOUT["legacy"][name]  # Reject unknown names in either layout.
    home = os.path.expanduser("~")
    base_home = home if expand_user else "~"
    if name == "desktop_logs_macos" or _uses_legacy(home):
        return os.path.join(base_home, *legacy.split("/"))
    spec = _LAYOUT["native"][name]
    root = _native_root(spec["root"], base_home)
    return os.path.join(root, *spec["path"].split("/")) if spec["path"] else root


def media_root(thumb_dir: str) -> str:
    """Persistent media root; custom thumbnail folders keep their sibling layout.

    Native default thumbnails live in a purgeable cache. Originals, offline
    copies, working files and external editor output must never follow them.
    Compare lexical paths so a custom symlink keeps its existing semantics.
    """
    if os.path.normcase(os.path.abspath(thumb_dir)) == os.path.normcase(os.path.abspath(default_path("thumbnails"))):
        return default_path("data")
    return os.path.dirname(thumb_dir)


def protected_roots() -> list[str]:
    """All default storage roots, including inactive ones, for test safety guards."""
    home = os.path.expanduser("~")
    platform = sys.platform if sys.platform in _LAYOUT["roots"] else "linux"
    roots = [os.path.join(home, ".vireo"), _native_root("data", home), _native_root("cache", home)]
    for spec in _LAYOUT["roots"][platform].values():
        roots.append(os.path.join(home, *spec["fallback"].split("/"), *spec["suffix"].split("/")))
    return list(dict.fromkeys(roots))
