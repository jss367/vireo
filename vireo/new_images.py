"""Detect image files present on disk but not yet ingested into a workspace."""
import errno
import logging
import os
import stat
import threading
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dir_listing_cache
import volume_reachability
from image_loader import (
    SUPPORTED_EXTENSIONS,
    is_excluded_scan_dir,
    is_excluded_scan_path,
    safe_scan_walk,
)

log = logging.getLogger(__name__)

def _known_paths_for_workspace(db, workspace_id):
    """Return absolute primary and companion paths already ingested.

    A newly-created same-stem JPEG is intentionally *not* known until a scan
    attaches it to the RAW record. It therefore appears once in New Images,
    giving the user a visible path to ingest the developed file. After pairing,
    ``companion_path`` makes the same walk converge to zero instead of leaving
    a stuck banner.
    """
    rows = db.photo_visibility.active_path_rows(workspace_id)
    known = set()
    for row in rows:
        known.add(os.path.join(row["folder_path"], row["filename"]))
        if row["companion_path"]:
            companion = row["companion_path"]
            known.add(
                companion if os.path.isabs(companion)
                else os.path.join(row["folder_path"], companion)
            )
    return known


def _path_key(path):
    """Comparison key for a directory path: normalized, case-folded where the
    platform folds case."""
    return os.path.normcase(os.path.normpath(path))


def _is_within_key(path_key, root_key):
    try:
        return os.path.commonpath([path_key, root_key]) == root_key
    except ValueError:
        return False


def staged_source_paths(db):
    """Original locations of every folder-level local copy (Work Locally).

    Staging rebases the catalog rows of ``source_path`` onto the local copy,
    so the originals there are known to the catalog only by their local
    path. A walk that compared them against catalog paths would report every
    one as new, and the in-place import refuses any path inside a staged
    source anyway, so the walk leaves these trees out and reports them.
    """
    return [
        row["source_path"]
        for row in db.local_folders.root_source_paths()
        if row["source_path"]
    ]


def _staged_exclusions_for_root(root_path, staged_sources, source_resolutions=None):
    """Where staged sources meet one root, in the walk's own spelling.

    Returns ``(covering_source, excluded_dir_keys)``: ``covering_source`` is a
    staged source containing the whole root (the root is not walked at all),
    and ``excluded_dir_keys`` maps the :func:`_path_key` of each directory to
    prune to the path reported to the user. Both the literal path and its
    symlink-resolved form are compared on each side
    with bounded component probes, including aliases below a mount root.
    Inconclusive probes fail closed as an unchecked root rather than count
    staged originals as new; this mirrors import admission's overlap check.
    """
    if not staged_sources:
        return None, {}
    root_forms = {_path_key(root_path)}
    root_forms.add(_path_key(volume_reachability.resolve_alias_lexically(root_path)))
    resolved_root = volume_reachability.resolve_alias_bounded(root_path)
    if resolved_root is None:
        raise _RootOffline(OSError(errno.ETIMEDOUT, "root alias resolution timed out"))
    root_forms.add(_path_key(resolved_root))
    excluded = {}
    for source, source_forms in staged_sources:
        if source_resolutions is None:
            resolved_source = volume_reachability.resolve_alias_bounded(source)
            source_mounts = None
        else:
            resolved_source, source_mounts = source_resolutions[source]
        if resolved_source is None:
            lexical_overlap = any(
                _is_within_key(root_key, source_key)
                or _is_within_key(source_key, root_key)
                for root_key in root_forms for source_key in source_forms
            )
            if source_mounts is None:
                source_mounts = set(volume_reachability.mount_root_candidates(source))
            root_mounts = set(volume_reachability.mount_root_candidates(resolved_root))
            if not lexical_overlap and source_mounts and source_mounts.isdisjoint(root_mounts):
                # A disconnected share cannot overlap a conclusively
                # resolved local root or another share. Aliases on the
                # same share remain ambiguous and must fail closed.
                continue
            raise _RootOffline(OSError(errno.ETIMEDOUT, "staged alias resolution timed out"))
        source_forms = {*source_forms, _path_key(resolved_source)}
        for source_key in source_forms:
            for root_key in root_forms:
                if _is_within_key(root_key, source_key):
                    return source, {}
                if _is_within_key(source_key, root_key):
                    rel = os.path.relpath(source_key, root_key)
                    walk_path = os.path.normpath(os.path.join(root_path, rel))
                    excluded[_path_key(walk_path)] = walk_path
    return None, excluded


def mapped_roots(db, workspace_id, *, include_missing=False):
    """Return the workspace's user-facing roots — folders linked to the
    workspace with ``is_root = 1`` and no ``is_root = 1`` ancestor also linked
    here. Skips folders marked 'missing' unless ``include_missing=True``;
    folders flagged ``'partial'`` from an interrupted scan are kept so a
    rescan can pick up where it stopped. Snapshot consumers opt into missing
    roots so files that vanish after discovery retain their provenance.

    Scoping by ``is_root`` (the canonical "user-facing root" flag, same as
    ``get_workspace_folder_roots``) rather than by mere linkage is what keeps
    the new-images walk aligned with what the user actually imported. A
    templated copy-import records its leaf destination subfolders as roots and
    links the destination *base* as a non-root parent (is_root=0) so photo
    queries and the folder tree still work. Walking by linkage would treat that
    base as a root and surface every un-imported sibling under it — a whole
    archive of past shoots — as "new". The is_root migration backfills
    is_root=1 for exactly the topmost-linked folder of each chain, so this is
    behaviour-identical to the old topology walk for every pre-existing
    workspace; it only diverges for the import-container case above.

    The ancestor check still guards against double-counting when two folders in
    one chain are both marked roots (e.g. /A and /A/B/C with /A/B unlinked):
    os.walk'ing both would count files under /A/B/C twice.
    """
    rows = db.workspace_folders.linked_folder_rows(
        workspace_id, include_missing=include_missing,
    )
    root_ids = {r["id"] for r in rows if r["is_root"]}
    if not root_ids:
        return []

    # Load parent_id for every folder — needed to walk arbitrary-depth ancestor
    # chains where intermediate folders may not themselves be roots.
    parent_of = {
        r["id"]: r["parent_id"]
        for r in db.workspace_folders.folder_parent_rows()
    }

    def has_root_ancestor(folder_id):
        parent = parent_of.get(folder_id)
        while parent is not None:
            if parent in root_ids:
                return True
            parent = parent_of.get(parent)
        return False

    return [
        {"id": r["id"], "path": r["path"]}
        for r in rows
        if r["is_root"] and not has_root_ancestor(r["id"])
    ]


def snapshot_folder_counts(paths):
    """Captured file count per containing folder of a new-images snapshot.

    The folders are the snapshot paths' exact ``os.path.dirname`` strings —
    the same spelling the Import page lists and sends back in
    ``exclude_folders``.
    """
    counts = {}
    for path in paths:
        folder = os.path.dirname(path)
        counts[folder] = counts.get(folder, 0) + 1
    return counts


def exclude_snapshot_folders(paths, exclude_folders):
    """Drop the snapshot files that sit directly in ``exclude_folders``.

    Returns the kept paths. Exclusion is per folder, not per subtree: a
    captured parent folder and its captured subfolders are separate rows,
    so leaving out ``2020`` keeps ``2020/2020-10-08``. Raises ValueError
    for a folder the snapshot never captured (rather than a silent no-op)
    and for leaving out every folder.
    """
    if exclude_folders is None:
        return list(paths)
    if not isinstance(exclude_folders, list) or not all(
        isinstance(folder, str) and folder for folder in exclude_folders
    ):
        raise ValueError("exclude_folders must be a list of folder paths")
    excluded = set(exclude_folders)
    captured = {os.path.dirname(path) for path in paths}
    unknown = sorted(excluded - captured)
    if unknown:
        raise ValueError(
            "exclude_folders names a folder this snapshot did not capture: "
            + unknown[0]
        )
    kept = [path for path in paths if os.path.dirname(path) not in excluded]
    if paths and not kept:
        raise ValueError("every captured folder is left out; nothing to import")
    return kept


class _RootOffline(Exception):
    """Raised from the walk's ``onerror`` to abandon one root's traversal."""

    def __init__(self, exc):
        super().__init__(str(exc))
        self.exc = exc


class DiscoveryDeferred(Exception):
    """Close this root's directory iterators before yielding to a transfer."""


class _NewImagesTally:
    """Running totals of one :func:`count_new_images_for_workspace` pass."""

    def __init__(self):
        self.per_root = []
        self.sample = []
        self.unreachable_roots = []
        self.total = 0
        self.files_checked = 0
        self.last_emitted = 0
        self.seen_new_paths = set()
        self.live_mount_roots = set()
        self.folders_read = 0
        self.folders_unchanged = 0
        self.local_copy_excluded = []

    def unreachable(self, root, mount_root):
        log.warning(
            "new-images: skipping %s — volume %s is offline",
            root["path"], mount_root or root["path"],
        )
        self.unreachable_roots.append(root["path"])
        self.per_root.append({
            "folder_id": root["id"], "path": root["path"],
            "new_count": 0, "unreachable": True,
        })

    def result(self, sample_limit):
        return {
            "new_count": self.total,
            "per_root": self.per_root,
            "sample": self.sample,
            "sample_complete": sample_limit is None or len(self.sample) >= self.total,
            "unreachable_roots": self.unreachable_roots,
            # Folders left out because their originals are staged as a local
            # copy. ``new_count`` and ``sample`` exclude them, so an import of
            # this answer never names a path the import would refuse.
            "local_copy_excluded": sorted(set(self.local_copy_excluded)),
            "folders_read": self.folders_read,
            "folders_unchanged": self.folders_unchanged,
            # Wall-clock stamp of when this answer was produced. The banner shows
            # it whenever a root was skipped: a user who clicks "Check again" on a
            # volume that is *still* offline gets the same sentence back, and the
            # time is the only thing that tells them the recheck really ran.
            "checked_at": time.time(),
        }


def _staged_source_resolutions(db):
    """Staged local-copy sources with their path keys, and each one's
    alias resolution (probed once per snapshot)."""
    staged_sources = [
        (source, {
            _path_key(source),
            _path_key(volume_reachability.resolve_alias_lexically(source)),
        })
        for source in staged_source_paths(db)
    ]
    # Source identity is invariant during this snapshot. Probe each source
    # once, rather than repeating NAS metadata lookups for every root.
    source_resolutions = {}
    for source, _forms in staged_sources:
        resolved = volume_reachability.resolve_alias_bounded(source)
        mounts = (set(volume_reachability.mount_root_candidates(source))
                  if resolved is None else None)
        source_resolutions[source] = (resolved, mounts)
    return staged_sources, source_resolutions


def _root_excluded_before_walk(root_path, mount_root):
    """Whether a reachable root is reported with no new images unwalked:
    it is, or sits inside, an excluded bundle, or a local root is not a
    directory."""
    if mount_root is not None:
        # Mount-shaped root: the gate's verdict may be up to 30s old and
        # the share can have dropped since, so no unbounded lookup may
        # run here. The bundle exclusion is decided lexically from the
        # path's component names (the same name test the walker applies
        # to children), and there is no ``isdir`` — a root that vanished
        # surfaces as an ``OSError`` from the walk's first ``scandir``
        # and is handled by ``_on_walk_error`` below.
        # The literal path *and* its alias-resolved form are both
        # checked, so ``~/PhotoLib -> /Volumes/NAS/Photos Library.photoslibrary``
        # is still excluded; resolution follows local symlinks only and
        # never looks below the mount root.
        lexical_forms = {
            root_path,
            volume_reachability.resolve_alias_lexically(root_path),
        }
        return any(
            is_excluded_scan_dir(part)
            for form in lexical_forms
            for part in form.replace("\\", "/").split("/")
        )
    if is_excluded_scan_path(root_path):
        return True
    return not os.path.isdir(root_path)


def _root_checkpoint(progress):
    """Restart a stale root snapshot instead of waiting and then using it."""
    if progress.should_yield is not None:
        if progress.should_yield():
            raise DiscoveryDeferred()
    elif progress.checkpoint is not None:
        progress.checkpoint()


def _count_root_new_images(tally, root, known, reachability, staged_sources,
                           source_resolutions, reachability_generation,
                           sample_limit, progress,
                           stall_timeout, listing_cache):
    """Walk one mapped root and add what it holds to ``tally``."""
    root_path = root["path"]
    _root_checkpoint(progress)
    # prune_scan_dirs filters only children; if the root is, or sits
    # inside, an excluded bundle (e.g. user added
    # ``~/Pictures/Photos Library.photoslibrary`` directly, or a stale
    # folder row points at ``.../Photos Library.photoslibrary/originals``),
    # os.walk would still open it and inflate the banner with managed
    # images the scanner never ingests. This must run BEFORE
    # ``os.path.isdir`` — isdir follows symlinks and stat's the target,
    # so for a directly selected bundle (or a symlink to one) the
    # existence test alone is enough to trip the macOS TCC prompt.
    #
    # The reachability gate runs *first*, before even that exclusion
    # check: ``is_excluded_scan_path`` walks the path's components with
    # ``os.path.islink``, which on a dead SMB mount is an unbounded
    # lookup. The gate itself never touches a mount-shaped path — its
    # bounded probe is the only filesystem access — so ordering it ahead
    # keeps every lookup on this root behind the timeout. The exclusion
    # check still precedes ``isdir`` (the TCC concern above) because a
    # bundle root is local and the gate passes it through untouched.
    mount_root, reachable = reachability.check(root_path)
    if not reachable:
        tally.unreachable(root, mount_root)
        return
    if (
        mount_root is not None
        and volume_reachability.was_observed_mounted(mount_root)
    ):
        tally.live_mount_roots.add(mount_root)
    if _root_excluded_before_walk(root_path, mount_root):
        tally.per_root.append({"folder_id": root["id"], "path": root_path, "new_count": 0})
        return

    try:
        covering_source, excluded_dirs = _staged_exclusions_for_root(
            root_path, staged_sources, source_resolutions,
        )
    except _RootOffline:
        tally.unreachable(root, mount_root)
        return
    if covering_source is not None:
        tally.local_copy_excluded.append(root_path)
        tally.per_root.append({
            "folder_id": root["id"], "path": root_path, "new_count": 0,
            "local_copy_excluded": [root_path],
        })
        return

    listing_pass = dir_listing_cache.ListingPass(listing_cache)
    excluded_hits = []
    outcome = _walk_root_bounded(
        _RootWalk(root_path, mount_root, reachability, reachability_generation),
        known, tally.seen_new_paths,
        progress,
        stall_timeout,
        listing_pass=listing_pass,
        excluded_dirs=excluded_dirs,
        excluded_hits=excluded_hits,
    )
    if outcome is None:
        # Offline (error or stall): nothing from this root is kept.
        tally.unreachable(root, mount_root)
        return
    root_new_paths, checked, tally.last_emitted = outcome
    tally.files_checked += checked
    tally.folders_read += listing_pass.read
    tally.folders_unchanged += listing_pass.unchanged
    tally.total += len(root_new_paths)
    tally.seen_new_paths.update(root_new_paths)
    for path in root_new_paths:
        if sample_limit is None or len(tally.sample) < sample_limit:
            tally.sample.append(path)

    entry = {
        "folder_id": root["id"], "path": root_path,
        "new_count": len(root_new_paths),
    }
    if excluded_hits:
        entry["local_copy_excluded"] = list(excluded_hits)
        tally.local_copy_excluded.extend(excluded_hits)
    tally.per_root.append(entry)


def count_new_images_for_workspace(db, workspace_id, sample_limit: int | None = 5,
                                   progress_callback=None,
                                   progress_every=250,
                                   reachability=None,
                                   stall_timeout=None,
                                   listing_cache=None,
                                   checkpoint=None, should_yield=None):
    """Return {'new_count': int, 'per_root': [...], 'sample': [abs_path, ...],
    'unreachable_roots': [abs_path, ...], 'folders_read': int,
    'folders_unchanged': int}.

    Walks each mapped root recursively, collects image files, and diffs against
    the set of photo paths already ingested into the workspace.

    A root whose volume is offline is *reported*, never raised: it appears in
    ``per_root`` with ``unreachable: True`` and in ``unreachable_roots``, and
    the remaining roots are still walked. Two signals feed that verdict —
    ``reachability`` (a :class:`volume_reachability.VolumeReachability`,
    defaulting to the shared gate) is consulted before touching each root so
    a known-dead share is skipped without any filesystem call, and an
    offline-class ``OSError`` (``ENOTCONN``/``EIO``/…) raised mid-walk marks
    the root offline in that gate for everyone else. ``new_count`` therefore
    covers reachable roots only, and callers must say so when they show it.

    Each root is walked on its own worker thread with a stall watchdog
    (``stall_timeout`` seconds without a single directory entry or file
    being processed; default :data:`WALK_STALL_TIMEOUT_SECONDS`). A share
    that *blocks* instead of raising — the SMB failure mode Python cannot
    interrupt — therefore ends as "root offline" rather than a compute that
    never finishes and a banner stuck on "checking". The wedged thread is
    left to die on its own; its root is reported offline immediately on
    later walks while it is still alive.

    ``progress_callback``, if given, is invoked as
    ``progress_callback(files_checked, new_found)`` once every
    ``progress_every`` files traversed (counting all candidate filenames,
    including ones we skip), and once at the end with the final totals.
    Callers use this to surface live progress for transparency without
    needing to refactor the walk.

    ``listing_cache`` (a :class:`dir_listing_cache.DirListingCache`) lets the
    walk reuse the listing of every directory whose modification time has not
    changed since it was last read, so a periodic check re-reads only the
    folders that changed. ``folders_read`` / ``folders_unchanged`` say how
    many directories were read from disk versus reused.

    Folders whose originals are staged as a local copy (Work Locally, in any
    workspace) are not walked: their catalog rows point at the local copy,
    so every original would read as new, and an import of them is refused
    until the copy is synced or discarded. They are listed in
    ``local_copy_excluded`` so the banner can say what was left out.
    """
    if checkpoint is not None:
        checkpoint()
    if reachability is None:
        reachability = volume_reachability.get_shared()
    if stall_timeout is None:
        stall_timeout = WALK_STALL_TIMEOUT_SECONDS
    known = _known_paths_for_workspace(db, workspace_id)
    roots = mapped_roots(db, workspace_id)
    staged_sources, source_resolutions = _staged_source_resolutions(db)
    # Snapshot now, before any root is touched: an outage this walk observes
    # later belongs to the world as it is here. If a manual recheck clears
    # the gate mid-walk, the stale report is dropped rather than undoing it.
    reachability_generation = _reachability_generation(reachability)
    volume_reachability.seed_known_mount_roots(
        volume_reachability.load_known_mount_roots(db)
    )

    tally = _NewImagesTally()
    for root in roots:
        _count_root_new_images(
            tally, root, known, reachability, staged_sources,
            source_resolutions, reachability_generation, sample_limit,
            _WalkProgress(progress_callback, progress_every, tally.files_checked,
                          tally.total, tally.last_emitted, checkpoint, should_yield),
            stall_timeout, listing_cache,
        )

    if progress_callback is not None:
        progress_callback(tally.files_checked, tally.total)
    volume_reachability.record_known_mount_roots(
        db, {root: True for root in tally.live_mount_roots}
    )

    return tally.result(sample_limit)


# A root walk that makes no progress for this long — no directory entry
# enumerated, no file checked — is treated as a wedged share. SMB stalls
# freeze a syscall for minutes; a healthy listing heartbeats on every
# directory entry received via ``safe_scan_walk(on_entry=...)``.
WALK_STALL_TIMEOUT_SECONDS = 60.0
_WALK_POLL_SECONDS = 0.25
# Bound the registry before starting a worker, not only after it stalls. That
# reservation is what keeps several distinct shares that wedge concurrently
# from all becoming permanently blocked daemon threads.
_MAX_STALLED_WALKS = 8
_WALK_RESERVED = object()
# root_path -> reservation or worker thread. The companion set distinguishes
# genuinely stalled workers from healthy overlapping walks: another workspace
# waits for the latter instead of poisoning the shared reachability cache.
_STALLED_WALKS = {}
_STALLED_WALK_PATHS = set()
_STALLED_WALKS_LOCK = threading.Lock()
# Workers dropped from the registry by :func:`forget_stalled_walks` while
# still running. They no longer report their root offline, but they still
# occupy a slot until they return, so repeated rechecks against a wedged
# share cannot stack walk threads without bound.
_FORGOTTEN_STALLED_WALKS = set()
# Bumped by :func:`forget_stalled_walks`. Each walk records the epoch it
# started under, so a walk that was already hung at recheck time but only
# crosses its stall timeout *after* the sweep cannot register itself as the
# reason to skip the root. Registrations without an epoch (test doubles) read
# as current, i.e. they keep the historical fail-fast behaviour.
_WALK_INVALIDATION_EPOCH = 0
_STALLED_WALK_EPOCHS = {}


def _current_walk_epoch():
    with _STALLED_WALKS_LOCK:
        return _WALK_INVALIDATION_EPOCH


def _live_forgotten_stalled_walks():
    """Prune finished forgotten workers; return how many are still running.

    Callers must hold ``_STALLED_WALKS_LOCK``.
    """
    for worker in list(_FORGOTTEN_STALLED_WALKS):
        if not worker.is_alive():
            _FORGOTTEN_STALLED_WALKS.discard(worker)
    return len(_FORGOTTEN_STALLED_WALKS)


def forget_stalled_walks():
    """Let one fresh walk run for each root with a wedged walker.

    A walk abandoned by the stall watchdog leaves its worker registered, and
    every later check of that root reports it offline without walking — right
    for automatic polls, wrong for an explicit recheck, which would otherwise
    answer "offline" (with a fresh check time) for a volume nobody has looked
    at since the user remounted it. This is the walk-side counterpart of
    ``volume_reachability._forget_wedged_probes``: drop the registration so
    exactly one new walk starts, and if the share is still wedged that walk
    stalls and re-arms the fast path. The workers keep counting against
    ``_MAX_STALLED_WALKS`` while they live, and the registry writes in
    ``_walk_root_bounded`` are all identity-checked, so a worker that wakes
    later cannot free a slot twice.
    """
    global _WALK_INVALIDATION_EPOCH
    with _STALLED_WALKS_LOCK:
        # Bump first: a walk that is hung right now and trips its watchdog a
        # moment after this sweep registers itself with the *old* epoch, so
        # the check in ``_walk_root_bounded`` retires it instead of reporting
        # the root offline on its behalf.
        _WALK_INVALIDATION_EPOCH += 1
        for root_path in list(_STALLED_WALK_PATHS):
            _forget_stalled_walk_locked(root_path)


def _forget_stalled_walk_locked(root_path):
    """Drop ``root_path``'s stalled registration, keeping a live worker
    counted. Callers must hold ``_STALLED_WALKS_LOCK``."""
    worker = _STALLED_WALKS.get(root_path)
    if worker is not None and worker is not _WALK_RESERVED:
        del _STALLED_WALKS[root_path]
        if worker.is_alive():
            _FORGOTTEN_STALLED_WALKS.add(worker)
    _STALLED_WALK_PATHS.discard(root_path)
    _STALLED_WALK_EPOCHS.pop(root_path, None)


class _WalkAbandoned(Exception):
    """Raised inside an abandoned worker so it exits as soon as it wakes."""


def _mark_offline(reachability, mount_root, generation):
    """Publish an outage, tied to the reachability generation this walk began
    under when the gate supports one (test doubles need not)."""
    if generation is None:
        reachability.mark_offline(mount_root)
    else:
        reachability.mark_offline(mount_root, generation=generation)


def _reachability_generation(reachability):
    """Generation to quote when reporting an outage, or None for a gate that
    does not track one."""
    getter = getattr(reachability, "current_generation", None)
    return getter() if callable(getter) else None


@dataclass(frozen=True)
class _RootWalk:
    """The root one bounded walk covers and the gate its outage goes to."""

    root_path: str
    mount_root: str | None
    """The volume ``root_path`` lives on, as the gate reported it, or None
    for a local root."""
    reachability: Any
    """The reachability gate: ``check(path)`` and ``mark_offline(mount)``."""
    reachability_generation: int | None = None
    """The gate generation the walk began under, quoted when it reports an
    outage; None for a gate that does not track one."""

    def mark_offline(self):
        _mark_offline(self.reachability, self.mount_root, self.reachability_generation)


@dataclass(frozen=True)
class _WalkProgress:
    """How one root's walk reports progress, and the counts it continues.

    The counts are the caller's totals before this root; the walk adds its
    own on top when it reports, and returns the new ``last_emitted``.
    """

    callback: Callable[[int, int], Any] | None
    """``callback(files_checked, new_found)``, or None for no reports."""
    every: int
    """Report after at least this many more files were checked."""
    files_checked: int
    total: int
    """New images found in earlier roots."""
    last_emitted: int
    """``files_checked`` at the last report."""
    checkpoint: Callable[[], object] | None = None
    """Yield background I/O without reporting the intentional wait as a stall."""
    should_yield: Callable[[], bool] | None = None
    """Nonblocking transfer/cancellation probe safe inside directory reads."""


def _reserve_root_walk(walk):
    """Reserve ``walk.root_path``'s slot in the stalled-walk registry.

    Waits out a healthy overlapping walk of the same root. Returns False,
    without reserving, when the root must be reported offline instead: a
    wedged walk from this epoch still holds it, the worker limit is reached,
    or the predecessor found the volume offline.
    """
    root_path = walk.root_path
    while True:
        wait_for = None
        with _STALLED_WALKS_LOCK:
            existing = _STALLED_WALKS.get(root_path)
            if existing is not None:
                if existing is _WALK_RESERVED:
                    wait_for = existing
                elif existing.is_alive():
                    if root_path in _STALLED_WALK_PATHS:
                        if _STALLED_WALK_EPOCHS.get(
                            root_path, _WALK_INVALIDATION_EPOCH,
                        ) < _WALK_INVALIDATION_EPOCH:
                            # Wedged by a walk that predates the last
                            # recheck: what it proved is about the volume as
                            # it was then. Retire the registration and walk.
                            _forget_stalled_walk_locked(root_path)
                            continue
                        log.warning(
                            "new-images: %s still has a wedged walk from an "
                            "earlier check; reporting offline without walking "
                            "again", root_path,
                        )
                        walk.mark_offline()
                        return False
                    # Another workspace is walking the same root. Wait outside
                    # the lock, then retry; an active healthy walk is not
                    # evidence that the shared volume is offline.
                    wait_for = existing
                else:
                    del _STALLED_WALKS[root_path]
                    _STALLED_WALK_PATHS.discard(root_path)
                    _STALLED_WALK_EPOCHS.pop(root_path, None)
            if wait_for is None:
                # Finished workers for other roots do not consume capacity.
                # Live workers do: any may become an uninterruptible SMB stall.
                for other_path, other in list(_STALLED_WALKS.items()):
                    if other is not _WALK_RESERVED and not other.is_alive():
                        del _STALLED_WALKS[other_path]
                        _STALLED_WALK_PATHS.discard(other_path)
                        _STALLED_WALK_EPOCHS.pop(other_path, None)
                if (
                    len(_STALLED_WALKS) + _live_forgotten_stalled_walks()
                    >= _MAX_STALLED_WALKS
                ):
                    log.warning(
                        "new-images: root-walk limit reached while checking %s; "
                        "not starting another worker", root_path,
                    )
                    return False
                _STALLED_WALKS[root_path] = _WALK_RESERVED
                return True
        if wait_for is _WALK_RESERVED:
            time.sleep(_WALK_POLL_SECONDS)
        else:
            wait_for.join(_WALK_POLL_SECONDS)
            if not wait_for.is_alive():
                # The predecessor may have published an offline verdict after
                # our initial gate check. Re-read it before reserving a fresh
                # worker so overlapping workspace polls do not immediately
                # walk into the same disconnected share again.
                _checked_root, still_reachable = walk.reachability.check(root_path)
                if not still_reachable:
                    return False


def _root_walk_worker(state, walk, known, local_seen, progress, listing_pass,
                      excluded_dirs):
    """Build the worker that walks one root, recording into ``state``."""
    root_path = walk.root_path

    def _beat():
        if state["abandoned"]:
            raise _WalkAbandoned()
        if progress.should_yield is not None and progress.should_yield():
            raise DiscoveryDeferred()
        state["last"] = time.monotonic()

    def _on_walk_error(exc):
        # Offline-class errors abort this root and publish the outage;
        # anything else (a permission-denied subfolder) is skipped the way
        # the scanner skips it, so the count stays a lower bound for the
        # same files a scan would ingest.
        error_path = getattr(exc, "filename", None)
        root_disappeared = (
            isinstance(exc, OSError)
            and exc.errno == errno.ENOENT
            and error_path is not None
            and os.path.normcase(os.path.normpath(os.fspath(error_path)))
            == os.path.normcase(os.path.normpath(root_path))
        )
        if volume_reachability.is_offline_error(exc) or root_disappeared:
            walk.mark_offline()
            raise _RootOffline(exc)
        log.debug("new-images: skipping unreadable path: %s", exc)

    def _on_file(is_new):
        _beat()
        state["checked"] += 1
        if progress.callback is None:
            return
        checked_now = progress.files_checked + state["checked"]
        if checked_now - state["emitted"] >= progress.every:
            progress.callback(checked_now, progress.total + len(state["paths"]))
            state["emitted"] = checked_now

    def worker():
        try:
            # safe_scan_walk skips other-app data bundles (e.g. "Photos
            # Library.photoslibrary") without stat-following any symlinked
            # child that points at one — the os.walk classification stat
            # alone is enough to trip the macOS TCC prompt. Mirror what
            # scanner.scan() will eventually pick up, so the banner can't be
            # inflated by files the scanner will never ingest.
            # ``on_entry`` heartbeats on every directory entry received, so a
            # slow-but-streaming network listing (or a directory with fewer
            # than 256 entries taking a while) is not mistaken for a stall.
            with closing(safe_scan_walk(
                root_path, onerror=_on_walk_error, on_entry=_beat,
                listing_pass=listing_pass,
            )) as entries:
                _walk_root_for_new_images(
                    entries, known, local_seen, state["paths"],
                    on_file=_on_file, on_error=_on_walk_error,
                    excluded_dirs=excluded_dirs, excluded_hits=state["excluded"],
                )
        except _RootOffline as exc:
            state["offline"] = exc
        except _WalkAbandoned:
            pass
        except Exception as exc:  # broad: surfaced to the caller below as the root's error
            state["offline"] = exc

    return worker


def _walk_root_bounded(walk, known, seen_new_paths, progress, stall_timeout, *,
                       listing_pass=None, excluded_dirs=None, excluded_hits=None):
    """Walk ``walk.root_path`` on a worker thread under a stall watchdog.

    Returns ``(root_new_paths, files_checked_in_root, last_emitted)`` on
    success, or ``None`` when the root is offline — either an offline-class
    ``OSError`` surfaced through ``_RootOffline`` or the walk stalled for
    ``stall_timeout`` seconds. The worker never mutates the caller's
    counters or ``seen_new_paths``: it works on a snapshot and the caller
    merges only on success, so an abandoned thread that wakes up later can
    not corrupt a result that has already been published. ``listing_pass``
    is this root's own, and the caller reads its counts only on success.

    ``excluded_dirs`` maps the :func:`_path_key` of each directory to leave
    out (staged local-copy sources) to its reported path; the ones the walk
    actually met are appended to ``excluded_hits`` on success only.
    """
    # Recorded on this root if the watchdog fires: it dates the outage to the
    # world this walk started in, so a later recheck can tell it apart from
    # one observed after the user asked us to look again.
    _root_checkpoint(progress)
    walk_epoch = _current_walk_epoch()
    if not _reserve_root_walk(walk):
        return None
    root_path = walk.root_path
    state = {
        "checked": 0, "paths": [], "last": time.monotonic(),
        "emitted": progress.last_emitted, "abandoned": False, "offline": None,
        "excluded": [],
    }
    local_seen = set(seen_new_paths)
    worker = _root_walk_worker(
        state, walk, known, local_seen, progress, listing_pass, excluded_dirs,
    )
    thread = threading.Thread(
        target=worker, daemon=True, name="new-images-walk-root",
    )
    with _STALLED_WALKS_LOCK:
        _STALLED_WALKS[root_path] = thread
        try:
            thread.start()
        except Exception:  # broad: unregister the stalled walk on any start failure, then re-raise
            if _STALLED_WALKS.get(root_path) is thread:
                del _STALLED_WALKS[root_path]
                _STALLED_WALK_PATHS.discard(root_path)
                _STALLED_WALK_EPOCHS.pop(root_path, None)
            raise
    while True:
        thread.join(_WALK_POLL_SECONDS)
        if not thread.is_alive():
            break
        if time.monotonic() - state["last"] > stall_timeout:
            state["abandoned"] = True
            with _STALLED_WALKS_LOCK:
                if _STALLED_WALKS.get(root_path) is thread:
                    _STALLED_WALK_PATHS.add(root_path)
                    _STALLED_WALK_EPOCHS[root_path] = walk_epoch
            log.warning(
                "new-images: walk of %s made no progress for %.0fs; "
                "treating volume %s as offline and abandoning the walk",
                root_path, stall_timeout, walk.mount_root or root_path,
            )
            walk.mark_offline()
            return None

    failure = state["offline"]
    if failure is not None:
        if isinstance(failure, _RootOffline):
            return None
        raise failure
    if excluded_hits is not None:
        excluded_hits.extend(state["excluded"])
    return state["paths"], state["checked"], state["emitted"]


def _is_regular_file(path, on_error):
    """``os.path.isfile`` that does not swallow volume-loss errors.

    ``os.path.isfile`` returns False for *any* ``OSError``, which would let a
    share that drops between ``scandir`` and the per-file ``stat`` look like
    "no new files here" instead of "this root went offline". Route the error
    through ``on_error`` (which raises :class:`_RootOffline` for the offline
    class and merely logs the rest) and treat every failure as "not a file"
    — a broken symlink's ``ENOENT`` still lands on the skip path.
    """
    try:
        st = os.stat(path)
    except OSError as exc:
        on_error(exc)
        return False
    return stat.S_ISREG(st.st_mode)


def _walk_root_for_new_images(walk, known, seen_new_paths, root_new_paths,
                              on_file, on_error, excluded_dirs=None,
                              excluded_hits=None):
    """Consume one root's ``safe_scan_walk`` and collect its new image paths.

    Appends each newly discovered path to ``root_new_paths`` (and to the
    cross-root ``seen_new_paths`` set) and calls ``on_file(is_new)`` once per
    filename encountered so the caller can keep its progress counters. The
    caller owns error handling: an offline-class ``OSError`` — from the walk's
    ``onerror`` or from the per-file ``stat`` via ``on_error`` — surfaces as
    :class:`_RootOffline` and unwinds this loop.

    Subdirectories named in ``excluded_dirs`` (staged local-copy sources,
    keyed by :func:`_path_key`) are pruned from the top-down walk before it
    descends, and their reported paths are appended to ``excluded_hits``.
    """
    root_new = 0
    for dirpath, dirnames, filenames in walk:
        if excluded_dirs:
            kept = []
            for name in dirnames:
                reported = excluded_dirs.get(_path_key(os.path.join(dirpath, name)))
                if reported is None:
                    kept.append(name)
                elif excluded_hits is not None:
                    excluded_hits.append(reported)
            dirnames[:] = kept
        for name in filenames:
            is_new = False
            # Mirror ``vireo/scanner/discovery.py``: skip dotfiles (e.g. macOS
            # AppleDouble sidecars ``._IMG_0001.JPG``) so we don't count
            # files the scanner will never ingest, which would otherwise
            # produce a stuck "new images" banner.
            if not name.startswith("."):
                ext = Path(name).suffix.lower()
                if ext in SUPPORTED_EXTENSIONS:
                    full = os.path.join(dirpath, name)
                    # Mirror ``vireo/scanner/discovery.py``: os.walk lists broken
                    # symlinks in `filenames`, but scanner refuses to ingest
                    # them (os.path.isfile == False). Counting them as "new"
                    # would leave the banner stuck on files no scan can clear.
                    if (
                        full not in known
                        and full not in seen_new_paths
                        and _is_regular_file(full, on_error)
                    ):
                        seen_new_paths.add(full)
                        root_new_paths.append(full)
                        root_new += 1
                        is_new = True
            on_file(is_new)
    return root_new


def get_new_images_for_workspace(db, workspace_id):
    """Return new-images result for workspace, using cache when fresh.

    Race-safe: we snapshot the cache generation before the (potentially
    slow) walk and pass it to ``set``. If an invalidation fires during
    the walk, the generation advances and the stale result is dropped
    on write — so the next reader recomputes instead of seeing the
    pre-invalidation value. The current caller still returns its own
    best-effort result.
    """
    cache = db._new_images_cache
    cached = cache.get(db._db_path, workspace_id)
    if cached is not None:
        return cached
    generation = cache.get_generation(db._db_path, workspace_id)
    result = count_new_images_for_workspace(db, workspace_id)
    cache.set(db._db_path, workspace_id, result, generation=generation)
    return result


def invalidate_new_images_after_scan(db, root):
    """Invalidate the new-images cache for every workspace linked to any folder
    touched by a scan of ``root``.

    Uses a LIKE query because ``scanner.scan`` auto-registers subfolders as
    their own ``folders`` rows (see ``vireo/db.py`` ``add_folder``), so we
    need to invalidate caches for all workspaces that reference any of those
    descendant folders, not just the explicit scan root.

    Lives in this module (not ``app.py``) so non-Flask code paths such as
    ``pipeline_job.py`` can import it without pulling in the app module.
    """
    from file_identity import catalog_scan_root

    # Match both the caller's spelling and the one the scan cataloged.
    # ``str(Path(...))`` strips trailing slashes but keeps ``..`` segments, so
    # it still meets a folder row stored with them. ``catalog_scan_root`` is
    # the spelling ``scanner.scan`` files a root under: a root reached through
    # a symlink (``/tmp`` -> ``/private/tmp`` on macOS) or a case alias of a
    # cataloged folder is stored resolved, which the supplied spelling misses.
    roots = dict.fromkeys((str(Path(root)), catalog_scan_root(db, root)))
    # LIKE wildcards (%, _) in a root are not escaped. Worst case is a harmless
    # over-invalidation that triggers a re-walk. The descendant pattern uses
    # os.sep so it matches what the scanner stores via str(Path(...)) on both
    # POSIX and Windows.
    touched_ids = list(dict.fromkeys(
        r["id"]
        for spelling in roots
        for r in db.workspace_folders.folder_ids_at_or_like(
            spelling, spelling.rstrip("/\\") + os.sep + "%",
        )
    ))
    db.invalidate_new_images_cache_for_folders(touched_ids)
