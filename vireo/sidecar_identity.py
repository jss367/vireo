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
            # Toggle one ASCII letter: Unicode swapcase can expand a
            # character (ß -> SS), which tests more than case sensitivity.
            index = next((i for i, c in enumerate(name) if c.isascii() and c.isalpha()), None)
            if index is None:
                continue
            swapped = name[:index] + name[index].swapcase() + name[index + 1:]
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


def _missing_name_key(directory: str, name: str) -> str | None:
    """Use original-photo lookups to prove Unicode stem equivalence.

    Unicode casefold is only a candidate filter. For example, NTFS can keep
    straße and STRASSE distinct, while other filesystems may join them. Keep
    an existing entry's extension and substitute the missing sidecar's stem;
    samefile then asks this directory how those exact characters compare.
    """
    stem, extension = os.path.splitext(name)
    try:
        with os.scandir(directory) as entries:
            names = {entry.name for entry in entries}
        equivalents = []
        for entry in names:
            entry_stem, entry_extension = os.path.splitext(entry)
            if entry_stem.casefold() != stem.casefold():
                continue
            if entry_stem == stem:
                equivalents.append(entry_stem)
                continue
            alternate = stem + entry_extension
            # Separate listed entries are separate names, even if a user
            # made them hard links or symlinks to the same inode.
            if alternate in names:
                continue
            try:
                if os.path.samefile(os.path.join(directory, entry), os.path.join(directory, alternate)):
                    equivalents.append(entry_stem)
            except FileNotFoundError:
                continue
    except OSError:
        return None
    canonical = min(equivalents) + extension if equivalents else name
    # ASCII case equivalence was already proven by directory_folds_case.
    # Preserve every other character instead of expanding Unicode names.
    if not equivalents and not name.isascii():
        return None
    return canonical.translate(str.maketrans('ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'))


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
    key = _missing_name_key(parent, name) if folds else name
    return (stat.st_dev, stat.st_ino, key) if key is not None else None


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
