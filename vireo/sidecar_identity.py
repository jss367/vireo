"""Filesystem identities shared by keyword cancellation and XMP sync retries."""

import os


def directory_folds_case(directory: str) -> bool | None:
    """Probe names *inside* a directory, including Windows per-directory flags.

    The directory's own spelling tests its parent instead. Photos normally
    provide an existing entry even before their sidecars are written. An
    unreadable/empty directory provides no evidence; callers must distinguish
    that from a directory proven to keep case variants separate.
    """
    try:
        with os.scandir(directory) as entries:
            names = {entry.name for entry in entries}
        for name in names:
            swapped = name.swapcase()
            if swapped == name:
                continue
            # Two listed spellings prove sensitivity even if they are hard
            # links or symlinks to the same file.
            if swapped in names:
                return False
            original = os.path.join(directory, name)
            try:
                os.stat(original)
            except FileNotFoundError:
                # A dangling symlink (or an entry removed during the scan)
                # says nothing about lookups of a differently cased name.
                continue
            try:
                return os.path.samefile(original, os.path.join(directory, swapped))
            except FileNotFoundError:
                return False
    except OSError:
        return None
    return None


def missing_sidecar_identity(path: str) -> tuple[int, int, str] | None:
    """Identify a future sidecar using its existing containing directory.

    Parent inode equality handles different parent spellings without assuming
    that the names *inside* that parent also fold case. Never normcase the
    path: Windows can keep case variants distinct within a single directory.
    """
    parent, name = os.path.split(path)
    parent = parent or os.curdir
    try:
        stat = os.stat(parent)
    except OSError:
        return None
    folds = directory_folds_case(parent)
    if not stat.st_ino or folds is None:
        return None
    return stat.st_dev, stat.st_ino, name.casefold() if folds else name


def sidecar_paths_alias(own: str, other: str) -> bool:
    """Compare resolved sidecar paths, including files that do not exist yet."""
    if own == other:
        return True
    if own.casefold() != other.casefold():
        return False
    try:
        return os.path.samefile(own, other)
    except FileNotFoundError:
        own_identity = missing_sidecar_identity(own)
        return own_identity is not None and own_identity == missing_sidecar_identity(other)
    except OSError:
        # Cancellation must not manufacture a destructive inverse without
        # evidence that another queued edit writes this same target.
        return False
