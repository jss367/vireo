"""Filesystem-aware path containment.

Extracted from the import endpoint's destination-inside-source guard
(PR #1107) so the card-cleanup overlap guard makes identical decisions.
``realpath`` alone is not enough: macOS/Windows default filesystems are
case-insensitive but realpath does not case-normalize, and FAT/exFAT
removable media on Linux are case-insensitive under a case-sensitive
parent. Inconclusive probes fall back to case-folding — the strict
direction for a containment guard.

macOS's default APFS/HFS+ volumes and Windows NTFS are case-INSENSITIVE,
so ``/Volumes/Card`` and ``/volumes/card`` are the same directory but
``realpath`` doesn't case-normalize on POSIX (macOS reports as POSIX). On
Linux, a platform-wide case-sensitivity assumption misses
FAT/exFAT/NTFS-mounted removable media (typical SD-card setup) that sit
under a case-sensitive ext4 parent — the user's real card mount point can
be case-insensitive even when the root filesystem isn't. Compare
case-folded on darwin/win32 unconditionally, and on Linux probe each
source's actual filesystem: if the resolved path with an alpha character
case-swapped still stat's to the same inode, the mount is case-insensitive
and both sides of the containment comparison need case-folding for that
source.
"""
import os
import sys


def is_case_insensitive_platform():
    return sys.platform in ("darwin", "win32")


# Probe results keyed by (realpath, st_dev). The per-file guards call
# contains_resolved in tight loops (one call per catalog row per card
# file), and each uncached call would listdir-probe the same root on
# Linux. st_dev in the key invalidates the entry when a different
# filesystem is mounted at the same path (card swapped at the same
# mount point) — a plain path key could serve a stale, non-strict
# answer. Inconclusive probes are cached too: they are stable for a
# given mount and the cached value (True) is the strict direction.
_probe_cache = {}
_PROBE_CACHE_MAX = 256


def fs_is_case_insensitive(path):
    """Probe whether the filesystem at ``path`` treats case as insensitive.

    List an entry inside ``path`` and check whether accessing it
    under a case-swapped name resolves to the same inode. Probing
    *inside* the directory (rather than swapping characters in
    ``path`` itself) is essential when a case-insensitive mount
    sits under a case-sensitive parent — a FAT/exFAT SD card
    mounted at ``/mnt/Card`` on Linux under an ext4 root: the
    ext4 ``/mnt`` cannot resolve ``/Mnt`` or a differently-cased
    ``Card`` entry (mount-point dentries are stored in the
    parent FS), so swapping characters in the ``path`` string
    always reports case-sensitive regardless of the card's own
    semantics.

    Any inconclusive result (unlistable, empty, no
    alpha-containing entry, or a stat error while comparing)
    returns True so the containment check falls back to
    case-fold — the stricter direction of this safety guard.
    A false positive on a case-sensitive filesystem can only
    reject a legitimate destination (recoverable UX error the
    user immediately sees and fixes by picking a different
    path), whereas a false negative on a case-insensitive
    filesystem accepts a case-collision destination inside the
    card and lets ``safe_to_format`` later green-light
    formatting while the archive lives on it. The reviewer's
    example: an SD card whose root contains only numeric
    top-level directories (Nikon-style ``100``/``101``/``102``)
    has no alpha-containing entry to probe with. See PR #1107
    review.
    """
    try:
        cache_key = (os.path.realpath(path), os.stat(path).st_dev)
    except (OSError, ValueError):
        cache_key = None
    if cache_key is not None and cache_key in _probe_cache:
        return _probe_cache[cache_key]
    result = _probe_uncached(path)
    if cache_key is not None:
        if len(_probe_cache) >= _PROBE_CACHE_MAX:
            _probe_cache.clear()
        _probe_cache[cache_key] = result
    return result


def _probe_uncached(path):
    try:
        entries = os.listdir(path)
    except OSError:
        return True
    for name in entries:
        for i, c in enumerate(name):
            if c.isalpha():
                swapped = name[:i] + c.swapcase() + name[i + 1:]
                if swapped == name:
                    continue
                original_full = os.path.join(path, name)
                probe_full = os.path.join(path, swapped)
                # os.stat, not os.path.exists: exists() collapses
                # permission errors into False, which would classify an
                # unreadable probe as case-SENSITIVE — the non-strict
                # direction, contradicting the fallback rule above.
                # Only a definitive ENOENT proves case sensitivity.
                try:
                    os.stat(probe_full)
                except FileNotFoundError:
                    # Definitive: case-swap resolves to nothing,
                    # so the filesystem distinguishes case.
                    return False
                except OSError:
                    return True
                try:
                    return os.path.samefile(original_full, probe_full)
                except OSError:
                    return True
    return True


def contains_resolved(root_real, child_real):
    """True if ``child_real`` equals or lies inside ``root_real``.

    Both arguments must already be realpath'd. Case sensitivity is
    decided per root: unconditional case-fold on darwin/win32; on Linux,
    probe the root's actual filesystem.
    """
    if is_case_insensitive_platform() or fs_is_case_insensitive(root_real):
        root_cmp = root_real.casefold().rstrip(os.sep)
        child_cmp = child_real.casefold()
    else:
        root_cmp = root_real.rstrip(os.sep)
        child_cmp = child_real
    return child_cmp == root_cmp or child_cmp.startswith(root_cmp + os.sep)


def path_contains(root, child):
    """realpath both sides, then ``contains_resolved``.

    Unresolvable paths return True — for a guard that *disqualifies*
    when contained, "can't tell" must be the strict direction.
    """
    try:
        root_real = os.path.realpath(root)
        child_real = os.path.realpath(child)
    except (OSError, ValueError):
        # ValueError: e.g. an embedded null byte — as unresolvable as
        # any OSError, and the same strict fallback applies.
        return True
    return contains_resolved(root_real, child_real)
