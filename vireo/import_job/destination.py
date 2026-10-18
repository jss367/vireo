"""The destination context: path normalization, case-insensitivity
probes, and the source-root guard that keeps writes off the card.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from import_job.state import _fail

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

    from db import Database
    from import_job.state import ImportParams, _ImportRunState

# Case-folded matching is unconditional on darwin/win32 (the OS enforces
# case-insensitive filesystems). On Linux we probe each source's actual
# filesystem: a FAT/exFAT/NTFS-mounted SD card is case-insensitive even
# under a case-sensitive ext4 parent, so a platform-wide check would miss
# a differently-cased twin path there. See PR #1107 review.
_CASE_INSENSITIVE_PLATFORM = sys.platform in ("darwin", "win32")


def _fs_is_case_insensitive(path: str) -> bool:
    """Probe whether the filesystem at ``path`` treats case as insensitive.

    List an entry inside ``path`` and check whether accessing it under a
    case-swapped name resolves to the same inode. Probing *inside* the
    directory (rather than swapping characters in ``path`` itself) is
    essential when a case-insensitive mount sits under a case-sensitive
    parent — a FAT/exFAT SD card mounted at ``/mnt/Card`` on Linux under
    an ext4 root: the ext4 ``/mnt`` cannot resolve ``/Mnt`` or a
    differently-cased ``Card`` entry (mount-point dentries live in the
    parent FS), so swapping characters in the ``path`` string always
    reports case-sensitive regardless of the card's own semantics.

    Any inconclusive result (unlistable, empty, no alpha-containing entry
    — Nikon-style ``100``/``101``/``102`` roots — or a stat error while
    comparing) returns True so the caller falls back to case-fold,
    mirroring the ``/api/jobs/import-photos`` route guard. False on
    inconclusive would let a differently-cased catalog twin under a
    source pass duplicate acceptance (or a differently-cased twin folder
    under the destination skip workspace linking), and
    ``safe_to_format`` could then go green without a visible off-card
    copy. See PR #1107 review.
    """
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
                if not os.path.exists(probe_full):
                    return False
                try:
                    return os.path.samefile(original_full, probe_full)
                except OSError:
                    return True
    return True


def _build_source_root_guard(sources: Iterable[str]) -> Callable[[str | Path], bool]:
    """Return ``path_under_any_source(path) -> bool`` for the given roots.

    Shared by both the local and remote duplicate gates to reject
    cataloged twins that live under the card being imported. A stale scan
    of a mounted card can leave a photos row whose ``folder_path`` IS the
    card; re-hashing that twin just re-reads the very card file being
    imported, so accepting it as duplicate proof would flip
    ``safe_to_format`` green while the card holds the only bytes. Only an
    off-card twin can back a duplicate skip. See PR #1107 review.
    """
    def _norm(s: str) -> tuple[str, bool]:
        try:
            real = os.path.realpath(s)
        except OSError:
            real = str(s)
        ci = _CASE_INSENSITIVE_PLATFORM or _fs_is_case_insensitive(real)
        return (real.casefold() if ci else real).rstrip(os.sep), ci

    roots = [_norm(s) for s in sources]

    def path_under_any_source(path: str | Path) -> bool:
        try:
            real = os.path.realpath(path)
        except OSError:
            real = str(path)
        real_folded = real.casefold()
        for root, ci in roots:
            if not root:
                continue
            cmp = real_folded if ci else real
            if cmp == root or cmp.startswith(root + os.sep):
                return True
        return False

    return path_under_any_source


def _is_source_backed_dest(ctx: _DestContext, source_file: Path, dest_path: str) -> bool:
    """True when ``dest_path`` IS the source file or resolves under any
    source root — i.e. writing to it, or adopting it, would attest bytes
    the card still owns.

    ONE pair of probes, shared by the orchestrator's per-file guard (spec
    decision 12, which fails the file) and the collision walk's candidate
    check (decision 13, which advances past it). They disagreed on nothing
    before; keeping them one function is what stops them from ever
    starting to. Note that both probes stat/realpath the destination, so
    the walk pays them only for candidates that actually exist.

    KNOWN-REMAINING: a candidate hardlinked to a DIFFERENT card file has
    no path- or inode-visible tie to *this* source — ``samefile`` is False
    and its realpath is its own path — so neither probe sees it. Catching
    that needs ``st_dev`` comparison against the source roots; out of
    scope for PR 7b and recorded in spec row 13.
    """
    # Reject the source-under-destination overlap where the folder
    # template maps the source right back to its own directory
    # (e.g. source ``/archive/2026/2026-07-05``, destination
    # ``/archive``, template ``%Y/%Y-%m-%d`` → dest_path IS the
    # source file). The API rejects destinations INSIDE any source;
    # this catches the opposite direction, where the destination is
    # a legal ancestor but the template resolves back to the source
    # directory. Without this the collision walk's adopt branch hashes
    # the source against itself, records it as ``skipped_duplicate``,
    # and safe_to_format goes green — deleting/formatting the
    # source then erases the only copy. See PR #1107 review.
    try:
        same_file = (
            os.path.exists(dest_path)
            and os.path.samefile(str(source_file), dest_path)
        )
    except OSError:
        # Fall back to normalized-path equality when samefile can't
        # stat (e.g. the destination is a stale entry). Prefer a
        # false positive here over a false negative that lets the
        # adopt branch loop back onto the source itself.
        same_file = (
            os.path.normpath(str(source_file))
            == os.path.normpath(dest_path)
        )
    if same_file:
        return True
    # Also reject any dest_path (not just an exact self-copy) that
    # resolves under any source root. Example: source
    # ``/Volumes/Card/DCIM``, destination ``/Volumes/Card``,
    # template ``DCIM/Archive/%Y`` — dest_path lands at
    # ``/Volumes/Card/DCIM/Archive/2026/<name>``, which is NOT the
    # source file (samefile is False) but is still inside the card.
    # A copy there is counted as ``copied``, safe_to_format can go
    # green, and formatting the card erases the "archive" copy too.
    # ``path_under_any_source`` realpaths its argument, so this is
    # also what catches a candidate that is a SYMLINK into the card.
    # See PR #1107 review.
    return ctx.path_under_any_source(dest_path)


def _reject_source_backed_dest(
    state: _ImportRunState, ctx: _DestContext, *, rel: str, source_file: Path, dest_file: str,
) -> bool:
    """Per-file dest-safety guard (spec decision 12): fail a ``dest_file``
    that IS the source file or resolves under any source root. Returns
    True when the file was rejected (booked failed; the caller skips it).

    Complements ``_batch_preflight``'s folder-level guard, which realpaths
    only ``dest_folder`` and so cannot see ``os.path.samefile`` identity
    (hard-links/symlinks to the same inode) or a ``dest_file`` that is
    itself a symlink resolving back into the card. Runs on BOTH
    transports: on the remote path this geometry is exactly the
    safe_to_format-over-unbacked-bytes hazard the batch guard's own
    comment describes — before the port the collision walk hashed the
    source-backed dest, byte-matched it, and adopted it.

    SUFFIX candidates are handled elsewhere, and differently. This guard
    only sees the PRIMARY dest name; the collision walk probes each
    candidate it visits with the same ``_is_source_backed_dest`` and
    ADVANCES past a source-backed one rather than failing the file (spec
    decision 13, closed in PR 7b — before that it hashed such a candidate,
    byte-matched the card against itself, and adopted it). The two
    dispositions differ deliberately: this guard's geometry is a folder
    template that resolves back into the card, which poisons every suffix
    too, so there is no safe slot to advance to; a single symlinked entry
    at ``name_1.ext`` poisons only itself. Do not "unify" them.
    """
    if _is_source_backed_dest(ctx, source_file, dest_file):
        _fail(
            state, rel, source_file,
            "destination file resolves inside a source directory "
            "(dest_file would live under the card being imported); "
            "formatting the card would erase the archive copy",
        )
        return True
    return False


@dataclass(frozen=True)
class _DestContext:
    """Normalized destination plus the run-scoped guards derived from it.

    Built once per run by ``_build_destination_context``; both import
    paths consume the same bundle. ``fold_basename`` backs the remote
    path's intra-batch collision map and is unused — free and harmless —
    on the local path until PR 7 merges the loops.
    """
    destination: str
    mount_baseline: dict[str, bool]  # mount-root candidate -> was-live-at-run-start
    path_under_any_source: Callable[[str | Path], bool]
    path_under_destination: Callable[[str], bool]
    fold_basename: Callable[[str], str]


def _build_destination_context(db: Database, params: ImportParams) -> _DestContext:
    """Normalize the destination and derive the run-scoped guards.

    ORDERING IS LOAD-BEARING: normalize first, capture the live-mount
    baseline immediately after (before the caller runs discovery,
    catalog-index construction or timestamp extraction), then build the
    source/destination predicates against the normalized path. This
    function preserves that order by construction — see the block
    comments below for why each step sits where it does.
    """
    # Function-level import mirrors the two import-path bodies: pipeline_job
    # imports from this module, so a module-level import would be a cycle.
    from pipeline_job import (
        _archive_mount_baseline,
        _load_known_mount_roots,
        _record_known_mount_roots,
    )

    destination = _normalize_destination(params)

    # Which of the destination's mount-root candidates are live mounts.
    # Captured HERE — immediately after normalization, before discovery,
    # catalog-index construction or timestamp extraction — because all of
    # those are slow against a network archive (the 2026-07-30 incident
    # spent eight minutes just enumerating the destination). A share that
    # detached during that window would be recorded as already-unmounted,
    # the mounted → unmounted transition would never fire, and the guard
    # would be silently disarmed for the rest of the run. See PR #1396
    # review (Codex P1 r3687336684).
    #
    # Keying on the transition (rather than "is it mounted?") is what lets
    # an ordinary local directory at a mount-shaped path stay usable: it
    # is False here and stays False, so it never trips the check.
    #
    # ``known_mount_roots`` seeds True from a persistent record of
    # mount roots ever observed live. Without it, a share that detached
    # BEFORE the run started (baseline is False from the outset) escapes
    # the guard: no True → False transition can fire against a False
    # baseline, so the persistent ``/mnt/<name>`` stub still passes the
    # per-batch check and copies land on the local disk (on the remote
    # path, rsync would still push to the NAS while the per-batch
    # mount-side scan reads a fresh local shadow). Cross-run history
    # closes that hole; a hand-made local dir is never observed as a
    # live mount and stays out of the known-set. See PR #1396 review
    # (Codex P1 r3687401636).
    known_mount_roots = _load_known_mount_roots(db)
    mount_baseline = _archive_mount_baseline(destination, known_mount_roots)
    _record_known_mount_roots(db, mount_baseline)

    # Reject cataloged twins that live under the card being imported. The
    # /api/jobs/import-photos route already refuses destinations that sit
    # inside a source (formatting the card would erase the archive copy),
    # but the duplicate acceptance loop separately trusts any cataloged
    # twin whose bytes hash to ``src_hash`` — including a stale row for a
    # previously scanned mounted card. That twin's re-hash just re-reads
    # the very card file being imported, so accepting it as duplicate
    # proof would flip ``safe_to_format`` green over a card whose bytes
    # never made it to the archive (remotely: whose bytes never crossed
    # the network). Shared by both import paths. See PR #1107 review.
    _path_under_any_source = _build_source_root_guard(params.sources)

    _path_under_destination, _fold_basename = _destination_case_guards(
        destination)

    return _DestContext(
        destination=destination,
        mount_baseline=mount_baseline,
        path_under_any_source=_path_under_any_source,
        path_under_destination=_path_under_destination,
        fold_basename=_fold_basename,
    )


def _normalize_destination(params: ImportParams) -> str:
    """The run's destination: normalized, then ``realpath``-resolved."""
    # Normalize once — the raw destination string is passed as ``root`` to
    # both the copy layout (``os.path.normpath(os.path.join(destination,
    # rel))``) and to ``scan(root, …, restrict_dirs=[dest_folder])``.
    # ``scanner._ensure_folder`` stops walking the folder chain when the
    # parent equals the scan root string; a destination like
    # ``/photos/tmp/../archive`` copies into the normalized
    # ``/photos/archive/…`` but the restricted scan root would remain the
    # dot-segment form, so the recursion never reaches root and the scan
    # loses those files (copied bytes then bucket as catalog failures).
    #
    # Also resolve symlinks (``realpath``) so a destination like ``/photos``
    # symlinked at ``/Volumes/Photos`` matches cataloged twin folders whose
    # ``folders.path`` was scanned under the real archive root — otherwise
    # duplicate matching compares paths from prior catalog scans against
    # this destination. Sources are already ``realpath``-resolved (see
    # ``_norm_source``); doing the same to destination keeps the two sides
    # symmetric. See PR #1107 review.
    #
    # On the remote (SSH) path ``params.destination`` is the catalog/mount
    # base — the route sets it to mount_path/subpath — and the same
    # normalization applies.
    try:
        destination = os.path.realpath(os.path.normpath(str(params.destination)))
    except OSError:
        destination = os.path.normpath(str(params.destination))
    return destination


def _destination_case_guards(destination: str) -> tuple[Callable[[str], bool], Callable[[str], str]]:
    """``(path_under_destination, fold_basename)`` for the normalized
    ``destination``, case-folded when its filesystem is case-insensitive.
    """
    # Destination containment for cataloged twin folders. Used to scope
    # ``_linkable_twin_dirs`` to twins under the destination/mount base —
    # an off-destination twin in some other library root is none of this
    # import's business. ``destination`` is already ``realpath``-resolved
    # (``_normalize_destination``) so a symlinked destination like ``/photos`` ->
    # ``/Volumes/Photos`` matches twin folders cataloged under
    # ``/Volumes/Photos/…``. Case-different spellings on case-insensitive
    # mounts (SMB, FAT, HFS+/APFS/exFAT) still need explicit
    # case-folding: ``realpath`` on APFS preserves the case the user
    # gave. Probe the destination's own filesystem (walking up to the
    # closest existing ancestor when the destination itself hasn't been
    # created yet); default to case-insensitive on inconclusive results
    # so a differently-cased twin folder under the destination is still
    # linked to the workspace — otherwise ``safe_to_format`` can go
    # green while the imported photo stays invisible in the active
    # workspace. See PR #1107 review.
    def _probe_dir_case_insensitive(path: str) -> bool:
        p = os.path.normpath(path)
        while True:
            if os.path.isdir(p):
                return _fs_is_case_insensitive(p)
            parent = os.path.dirname(p)
            if parent == p:
                return True
            p = parent

    _dest_ci = _CASE_INSENSITIVE_PLATFORM or _probe_dir_case_insensitive(destination)
    _dest_root_norm = (
        destination.casefold() if _dest_ci else destination
    ).rstrip(os.sep)

    def _path_under_destination(path: str) -> bool:
        if not _dest_root_norm:
            return False
        try:
            real = os.path.realpath(path)
        except OSError:
            real = str(path)
        cmp = (real.casefold() if _dest_ci else real).rstrip(os.sep)
        return cmp == _dest_root_norm or cmp.startswith(_dest_root_norm + os.sep)

    # Case-insensitive destinations (macOS APFS/HFS+, SMB, FAT/exFAT)
    # collapse basenames that differ only by case onto the same on-disk
    # file. The intra-batch collision map ``claimed_basenames`` keys by
    # basename, so keying it case-foldedly there makes a second file whose
    # basename differs from an earlier queued file's only by case (e.g.
    # ``IMG_0001.JPG`` then ``img_0001.jpg``) collide and advance through
    # numeric suffixes, instead of being sent to the same effective
    # receiver path where ``--ignore-existing`` would silently drop it and
    # the later catalog/hash validation would fail. See PR #1113 review.
    def _fold_basename(name: str) -> str:
        return name.casefold() if _dest_ci else name

    return _path_under_destination, _fold_basename
