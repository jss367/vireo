"""Where a photo's XMP sidecars are, and when two sidecar paths are one file.

Every caller that looks for, moves, copies or deletes an existing sidecar asks
this module: which spellings to probe, which naming styles count, and which
files still own a sidecar. Keyword cancellation and XMP sync retries use the
identity helpers at the bottom to compare sidecars that may not exist yet.
"""

import logging
import os
from collections.abc import Iterable

log = logging.getLogger(__name__)

SIDECAR_EXTENSION = ".xmp"

# The extension spellings probed beside a photo. Each probe is an ordinary
# lookup, so the containing directory applies its own case rules: a
# case-folding directory answers both for one file (deduplicated below), a
# case-sensitive one keeps ``IMG.xmp`` and ``IMG.XMP`` as two sidecars.
_SIDECAR_SUFFIXES = (".xmp", ".XMP")


def is_sidecar_name(name: str) -> bool:
    """True for a file name with an XMP extension in any case."""
    return os.path.splitext(name)[1].lower() == SIDECAR_EXTENSION


def sidecar_owner_key(sidecar_name: str) -> str:
    """The key a sidecar's owner must answer to: ``bird`` for ``bird.xmp``."""
    return os.path.splitext(sidecar_name)[0].lower()


def owner_keys(name: str) -> tuple[str, str]:
    """Keys a non-sidecar file answers to in both naming styles.

    ``bird.jpg`` owns ``bird.xmp`` (Vireo/Lightroom) through its stem and
    ``bird.jpg.xmp`` (darktable) through its full name. Matching ignores case
    so ``BIRD.JPG`` owns ``bird.xmp``; when unsure, a sidecar is kept.
    """
    return name.lower(), os.path.splitext(name)[0].lower()


def _same_existing_file(path: str, other: str) -> bool:
    try:
        return os.path.samefile(path, other)
    except OSError:
        return False


def _probe_sidecars(base: str) -> list[str]:
    found: list[str] = []
    for suffix in _SIDECAR_SUFFIXES:
        candidate = base + suffix
        if os.path.isfile(candidate) and not any(
            _same_existing_file(candidate, seen) for seen in found
        ):
            found.append(candidate)
    return found


def stem_sidecar(photo_path: str) -> str | None:
    """The existing ``<stem>.xmp`` sidecar beside ``photo_path``, or None.

    This is the sidecar Vireo and Lightroom write, and the one that moves
    and the offline cache carry with a photo; same-stem photos (RAW+JPEG)
    share it. ``.xmp`` wins over ``.XMP`` when a case-sensitive directory
    holds both.
    """
    found = _probe_sidecars(os.path.splitext(photo_path)[0])
    return found[0] if found else None


def photo_sidecars(photo_path: str) -> list[str]:
    """Every existing sidecar of ``photo_path``, each file once.

    Both naming styles: ``bird.xmp`` beside ``bird.jpg`` first, then
    ``bird.jpg.xmp``. Spellings that resolve to one file (a case-folding
    directory) are listed once, under the first spelling probed.
    """
    found: list[str] = []
    for base in (os.path.splitext(photo_path)[0], photo_path):
        for candidate in _probe_sidecars(base):
            if not any(_same_existing_file(candidate, seen) for seen in found):
                found.append(candidate)
    return found


def directory_owner_keys(directory: str) -> frozenset[str] | None:
    """Owner keys of every non-sidecar regular file in ``directory``.

    Hidden files count: a sidecar beside a hidden owner is still data.
    Entries that are directories do not, matching ``check_stray_sidecars``.
    Returns None when the directory cannot be listed, which callers must
    treat as "every sidecar is owned".
    """
    keys: set[str] = set()
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if is_sidecar_name(entry.name):
                    continue
                try:
                    if not entry.is_file():
                        continue
                except OSError:
                    continue
                keys.update(owner_keys(entry.name))
    except OSError:
        return None
    return frozenset(keys)


def sidecar_has_owner(sidecar_path: str, keys: frozenset[str] | None = None) -> bool:
    """True if any non-sidecar file beside ``sidecar_path`` owns it.

    ``keys`` is ``directory_owner_keys`` of the sidecar's directory, for
    callers checking many sidecars in one folder; it is read here when not
    given. An unlistable directory counts as owned, so an unreadable folder
    never makes a sidecar look deletable.
    """
    if keys is None:
        keys = directory_owner_keys(os.path.dirname(sidecar_path) or os.curdir)
    if keys is None:
        return True
    return sidecar_owner_key(os.path.basename(sidecar_path)) in keys


def delete_orphaned_sidecars(photo_paths: Iterable[str]) -> tuple[int, int]:
    """Delete the sidecars of photos whose originals are confirmed gone.

    Callers verify each original is missing first. A sidecar that another
    file still owns is kept: ``IMG_1.xmp`` beside a surviving ``IMG_1.JPG``
    holds that photo's metadata (same-stem photos share one sidecar), and
    a sidecar whose own original reappeared is owned again. Each directory
    is listed once per call.

    Returns ``(photos_cleaned, photos_skipped)``: photos at least one of
    whose sidecars was removed, and photos with nothing removed.
    """
    cleaned = 0
    skipped = 0
    keys_by_directory: dict[str, frozenset[str] | None] = {}
    for photo_path in photo_paths:
        directory = os.path.dirname(photo_path) or os.curdir
        if directory not in keys_by_directory:
            keys_by_directory[directory] = directory_owner_keys(directory)
        keys = keys_by_directory[directory]
        removed_one = False
        for sidecar in photo_sidecars(photo_path):
            if sidecar_has_owner(sidecar, keys):
                continue
            try:
                os.remove(sidecar)
                removed_one = True
            except OSError as e:
                log.warning("Failed to delete sidecar %s: %s", sidecar, e)
        if removed_one:
            cleaned += 1
        else:
            skipped += 1
    return cleaned, skipped


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
