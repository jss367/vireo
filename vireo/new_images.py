"""Detect image files present on disk but not yet ingested into a workspace."""
import errno
import logging
import os
import stat
import threading
import time
from pathlib import Path

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
    rows = db.conn.execute(
        """SELECT f.path AS folder_path, p.filename, p.companion_path
           FROM photos p
           JOIN folders f ON f.id = p.folder_id
           JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
           WHERE wf.workspace_id = ?""",
        (workspace_id,),
    ).fetchall()
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
        for row in db.conn.execute(
            "SELECT source_path FROM local_folder_mappings "
            "WHERE is_root = 1 ORDER BY source_path"
        ).fetchall()
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
    status_clause = "" if include_missing else (
        " AND f.status IN ('ok', 'partial')"
    )
    rows = db.conn.execute(
        """SELECT f.id, f.path, f.parent_id, wf.is_root
           FROM folders f
           JOIN workspace_folders wf ON wf.folder_id = f.id
           WHERE wf.workspace_id = ?""" + status_clause,
        (workspace_id,),
    ).fetchall()
    root_ids = {r["id"] for r in rows if r["is_root"]}
    if not root_ids:
        return []

    # Load parent_id for every folder — needed to walk arbitrary-depth ancestor
    # chains where intermediate folders may not themselves be roots.
    parent_of = {
        r["id"]: r["parent_id"]
        for r in db.conn.execute("SELECT id, parent_id FROM folders").fetchall()
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


class _RootOffline(Exception):
    """Raised from the walk's ``onerror`` to abandon one root's traversal."""

    def __init__(self, exc):
        super().__init__(str(exc))
        self.exc = exc


def count_new_images_for_workspace(db, workspace_id, sample_limit=5,
                                   progress_callback=None,
                                   progress_every=250,
                                   reachability=None,
                                   stall_timeout=None,
                                   listing_cache=None):
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
    if reachability is None:
        reachability = volume_reachability.get_shared()
    if stall_timeout is None:
        stall_timeout = WALK_STALL_TIMEOUT_SECONDS
    known = _known_paths_for_workspace(db, workspace_id)
    roots = mapped_roots(db, workspace_id)
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
    # Snapshot now, before any root is touched: an outage this walk observes
    # later belongs to the world as it is here. If a manual recheck clears
    # the gate mid-walk, the stale report is dropped rather than undoing it.
    reachability_generation = _reachability_generation(reachability)
    volume_reachability.seed_known_mount_roots(
        volume_reachability.load_known_mount_roots(db)
    )

    per_root = []
    sample = []
    unreachable_roots = []
    total = 0
    files_checked = 0
    last_emitted = 0
    seen_new_paths = set()
    live_mount_roots = set()
    folders_read = 0
    folders_unchanged = 0
    local_copy_excluded = []

    def _unreachable(root, mount_root):
        log.warning(
            "new-images: skipping %s — volume %s is offline",
            root["path"], mount_root or root["path"],
        )
        unreachable_roots.append(root["path"])
        per_root.append({
            "folder_id": root["id"], "path": root["path"],
            "new_count": 0, "unreachable": True,
        })

    for root in roots:
        root_path = root["path"]
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
            _unreachable(root, mount_root)
            continue
        if (
            mount_root is not None
            and volume_reachability.was_observed_mounted(mount_root)
        ):
            live_mount_roots.add(mount_root)
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
            if any(
                is_excluded_scan_dir(part)
                for form in lexical_forms
                for part in form.replace("\\", "/").split("/")
            ):
                per_root.append({"folder_id": root["id"], "path": root_path, "new_count": 0})
                continue
        else:
            if is_excluded_scan_path(root_path):
                per_root.append({"folder_id": root["id"], "path": root_path, "new_count": 0})
                continue
            if not os.path.isdir(root_path):
                per_root.append({"folder_id": root["id"], "path": root_path, "new_count": 0})
                continue

        try:
            covering_source, excluded_dirs = _staged_exclusions_for_root(
                root_path, staged_sources, source_resolutions,
            )
        except _RootOffline:
            _unreachable(root, mount_root)
            continue
        if covering_source is not None:
            local_copy_excluded.append(root_path)
            per_root.append({
                "folder_id": root["id"], "path": root_path, "new_count": 0,
                "local_copy_excluded": [root_path],
            })
            continue

        listing_pass = dir_listing_cache.ListingPass(listing_cache)
        excluded_hits = []
        outcome = _walk_root_bounded(
            root, root_path, mount_root, known, seen_new_paths, reachability,
            files_checked, total, progress_callback, progress_every,
            last_emitted, stall_timeout,
            reachability_generation=reachability_generation,
            listing_pass=listing_pass,
            excluded_dirs=excluded_dirs,
            excluded_hits=excluded_hits,
        )
        if outcome is None:
            # Offline (error or stall): nothing from this root is kept.
            _unreachable(root, mount_root)
            continue
        root_new_paths, checked, last_emitted = outcome
        files_checked += checked
        folders_read += listing_pass.read
        folders_unchanged += listing_pass.unchanged
        total += len(root_new_paths)
        seen_new_paths.update(root_new_paths)
        for path in root_new_paths:
            if sample_limit is None or len(sample) < sample_limit:
                sample.append(path)

        entry = {
            "folder_id": root["id"], "path": root_path,
            "new_count": len(root_new_paths),
        }
        if excluded_hits:
            entry["local_copy_excluded"] = list(excluded_hits)
            local_copy_excluded.extend(excluded_hits)
        per_root.append(entry)

    if progress_callback is not None:
        progress_callback(files_checked, total)
    volume_reachability.record_known_mount_roots(
        db, {root: True for root in live_mount_roots}
    )

    return {
        "new_count": total,
        "per_root": per_root,
        "sample": sample,
        "sample_complete": sample_limit is None or len(sample) >= total,
        "unreachable_roots": unreachable_roots,
        # Folders left out because their originals are staged as a local
        # copy. ``new_count`` and ``sample`` exclude them, so an import of
        # this answer never names a path the import would refuse.
        "local_copy_excluded": sorted(set(local_copy_excluded)),
        "folders_read": folders_read,
        "folders_unchanged": folders_unchanged,
        # Wall-clock stamp of when this answer was produced. The banner shows
        # it whenever a root was skipped: a user who clicks "Check again" on a
        # volume that is *still* offline gets the same sentence back, and the
        # time is the only thing that tells them the recheck really ran.
        "checked_at": time.time(),
    }


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


def _walk_root_bounded(root, root_path, mount_root, known, seen_new_paths,
                       reachability, files_checked, total, progress_callback,
                       progress_every, last_emitted, stall_timeout,
                       reachability_generation=None, listing_pass=None,
                       excluded_dirs=None, excluded_hits=None):
    """Walk one root on a worker thread under a stall watchdog.

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
    walk_epoch = _current_walk_epoch()
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
                        _mark_offline(
                            reachability, mount_root, reachability_generation,
                        )
                        return None
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
                    return None
                _STALLED_WALKS[root_path] = _WALK_RESERVED
                break
        if wait_for is _WALK_RESERVED:
            time.sleep(_WALK_POLL_SECONDS)
        else:
            wait_for.join(_WALK_POLL_SECONDS)
            if not wait_for.is_alive():
                # The predecessor may have published an offline verdict after
                # our initial gate check. Re-read it before reserving a fresh
                # worker so overlapping workspace polls do not immediately
                # walk into the same disconnected share again.
                _checked_root, still_reachable = reachability.check(root_path)
                if not still_reachable:
                    return None

    state = {
        "checked": 0, "paths": [], "last": time.monotonic(),
        "emitted": last_emitted, "abandoned": False, "offline": None,
        "excluded": [],
    }
    local_seen = set(seen_new_paths)

    def _beat():
        if state["abandoned"]:
            raise _WalkAbandoned()
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
            _mark_offline(reachability, mount_root, reachability_generation)
            raise _RootOffline(exc)
        log.debug("new-images: skipping unreadable path: %s", exc)

    def _on_file(is_new):
        _beat()
        state["checked"] += 1
        if progress_callback is None:
            return
        checked_now = files_checked + state["checked"]
        if checked_now - state["emitted"] >= progress_every:
            progress_callback(checked_now, total + len(state["paths"]))
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
            walk = safe_scan_walk(
                root_path, onerror=_on_walk_error, on_entry=_beat,
                listing_pass=listing_pass,
            )
            _walk_root_for_new_images(
                walk, known, local_seen, state["paths"],
                on_file=_on_file, on_error=_on_walk_error,
                excluded_dirs=excluded_dirs, excluded_hits=state["excluded"],
            )
        except _RootOffline as exc:
            state["offline"] = exc
        except _WalkAbandoned:
            pass
        except Exception as exc:  # surfaced to the caller below
            state["offline"] = exc

    thread = threading.Thread(
        target=worker, daemon=True, name="new-images-walk-root",
    )
    with _STALLED_WALKS_LOCK:
        _STALLED_WALKS[root_path] = thread
        try:
            thread.start()
        except Exception:
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
                root_path, stall_timeout, mount_root or root_path,
            )
            _mark_offline(reachability, mount_root, reachability_generation)
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
            # Mirror ``vireo/scanner.py``: skip dotfiles (e.g. macOS
            # AppleDouble sidecars ``._IMG_0001.JPG``) so we don't count
            # files the scanner will never ingest, which would otherwise
            # produce a stuck "new images" banner.
            if not name.startswith("."):
                ext = Path(name).suffix.lower()
                if ext in SUPPORTED_EXTENSIONS:
                    full = os.path.join(dirpath, name)
                    # Mirror ``vireo/scanner.py``: os.walk lists broken
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


class NewImagesCache:
    """In-memory per-``(db_path, workspace_id)`` cache with a TTL ceiling.

    Thread-safe. Keyed by the compound ``(db_path, workspace_id)`` so that two
    :class:`Database` instances pointed at different SQLite files cannot read
    each other's cached results — ``workspace_id=1`` (the default workspace)
    is reused across databases, and without the db_path scope tests or
    multi-database embeddings would cross-contaminate.

    Invalidation takes a list of workspace_ids (computed by the caller from
    the set of folder_ids touched by a scan) plus the originating ``db_path``;
    only entries and generations for that db are affected.

    A per-key generation counter protects against a race between an
    in-flight compute and a concurrent invalidation: a caller snapshots the
    generation before starting the walk and passes it to :meth:`set`; if
    invalidation bumps the generation during the walk, the stale result is
    silently dropped instead of repopulating the cache.
    """

    # Persistent compute failures (unreachable volume, DB error) suppress
    # retries for this long so we don't hammer the failing resource on every
    # navbar poll. Short enough that natural recovery is observable; long
    # enough that ten open tabs can't hot-loop the disk.
    ERROR_BACKOFF_SECONDS = 30
    # Results that skipped an offline root live only this long, matching the
    # ``VolumeReachability`` offline TTL so a reconnected share is re-walked
    # on the same schedule it is re-probed.
    PARTIAL_RESULT_TTL_SECONDS = 30

    # Full-library walks can take minutes on network storage. Automatic
    # refreshes share the missing-originals check's half-hour cadence;
    # imports/scans and explicit rechecks still invalidate immediately.
    def __init__(self, ttl_seconds=1800):
        self._ttl = ttl_seconds
        # key=(db_path, workspace_id) -> (result_dict, set_at_monotonic)
        self._entries = {}
        # key=(db_path, workspace_id) -> int
        self._generations = {}
        # key=(db_path, workspace_id) -> (Event, generation) for the in-flight
        # background compute. Only one compute per key runs at a time; stale-
        # generation kickoffs queue a rerun token rather than spawning a
        # parallel walk (see ``_rerun_pending``).
        self._inflight = {}
        # key=(db_path, workspace_id) -> (compute_fn, on_spawn, can_start) for a rerun. Set
        # when a kickoff arrives during an in-flight stale-generation compute:
        # rather than fan out a second concurrent ``os.walk`` (a real risk on
        # the scan path where ``invalidate_workspaces`` fires per discovered
        # folder), we let the current thread finish and have it spawn one
        # follow-up using the latest compute_fn. Multiple kickoffs collapse to
        # the same slot — last writer wins, so we never queue a backlog.
        self._rerun_pending = {}
        # key=(db_path, workspace_id) -> (error_message, set_at_monotonic).
        # Recent failures suppress retries within ``ERROR_BACKOFF_SECONDS``.
        self._errors = {}
        self._lock = threading.Lock()

    def get(self, db_path, workspace_id):
        key = (db_path, workspace_id)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            result, set_at = entry
            ttl = self._ttl
            if result.get("unreachable_roots"):
                # A walk that skipped an offline volume is a *partial* answer.
                # Holding it for the full TTL would keep reporting the folder
                # offline (and hide its new files) for minutes after the share
                # reconnects, so it expires at the reachability gate's own
                # offline retry cadence instead.
                ttl = min(ttl, self.PARTIAL_RESULT_TTL_SECONDS)
            if time.monotonic() - set_at > ttl:
                del self._entries[key]
                return None
            return result

    def has_inflight(self, db_path, workspace_id):
        """Return True if a background compute is currently running for
        ``(db_path, workspace_id)``. Callers use this to coalesce onto an
        existing walk instead of tearing it down with a fresh invalidation.
        """
        key = (db_path, workspace_id)
        with self._lock:
            return key in self._inflight

    def get_generation(self, db_path, workspace_id):
        """Return the current generation for ``(db_path, workspace_id)`` (0 if unseen)."""
        key = (db_path, workspace_id)
        with self._lock:
            return self._generations.get(key, 0)

    def set(self, db_path, workspace_id, result, generation=None):
        """Store ``result`` for ``(db_path, workspace_id)``.

        If ``generation`` is provided and no longer matches the current
        generation for the key (i.e. an invalidation ran after the
        caller snapshotted it), the write is silently dropped. Callers that
        don't care about the race can omit ``generation`` and the write is
        unconditional.
        """
        key = (db_path, workspace_id)
        with self._lock:
            if generation is not None:
                current = self._generations.get(key, 0)
                if generation != current:
                    return
            self._entries[key] = (result, time.monotonic())

    def invalidate_workspaces(self, db_path, workspace_ids):
        with self._lock:
            for wid in workspace_ids:
                key = (db_path, wid)
                self._entries.pop(key, None)
                # Drop any recorded failure too: the cache key has moved on
                # (folder/workspace change, completed scan), so the old error
                # no longer reflects the current state and must not gate a
                # fresh recompute via the 30s backoff window in
                # ``kickoff_compute``.
                self._errors.pop(key, None)
                self._generations[key] = self._generations.get(key, 0) + 1

    def get_recent_error(self, db_path, workspace_id):
        """Return the most recent compute error if it's still inside the
        backoff window, else None. Stale entries are cleared lazily."""
        key = (db_path, workspace_id)
        with self._lock:
            entry = self._errors.get(key)
            if entry is None:
                return None
            err_msg, set_at = entry
            if time.monotonic() - set_at > self.ERROR_BACKOFF_SECONDS:
                del self._errors[key]
                return None
            return err_msg

    def kickoff_compute(self, db_path, workspace_id, compute_fn, on_spawn=None, can_start=None):
        """Ensure a background compute is running for ``(db_path, workspace_id)``.

        If a recent compute failed (within ``ERROR_BACKOFF_SECONDS``), no new
        compute is started — readers should call :meth:`get_recent_error` and
        surface the failure instead of looping pending forever. Returns a
        pre-set Event so callers that ``wait`` don't block.

        Otherwise: if no compute is in flight, spawn a daemon thread that
        calls ``compute_fn()`` and stores the result in the cache. If one is
        already in flight — for the current or an older generation — the
        existing thread is reused; the caller waits on its event. When the
        in-flight thread is stale (an ``invalidate_workspaces`` ran mid-walk),
        the latest ``compute_fn`` is stashed as a deferred rerun token and
        the worker spawns one follow-up after it finishes. This keeps at most
        one walk per key in flight even when generations advance repeatedly
        (e.g. the scan path's per-folder ``invalidate_workspaces``), avoiding
        a fan-out of concurrent ``os.walk`` jobs that would thrash disk/CPU
        on large libraries.

        ``on_spawn`` is an optional callable invoked exactly once if (and
        only if) this kickoff causes a new background worker to be spawned
        — not when an in-flight worker is reused or when the error backoff
        short-circuits the call. It is called after the in-flight slot has
        been claimed but before the worker thread starts, with the spawned
        ``threading.Event`` as its only argument so the caller can block on
        it (e.g. from a separate JobRunner thread that wants to mirror the
        worker's lifecycle). If ``on_spawn`` returns a callable, that
        callable is used as a ``progress_callback(files_checked, new_found)``
        and passed to ``compute_fn`` via keyword. Use this to register a
        transparency-only job entry that streams progress while the walk
        runs.

        Returns an ``Event`` that fires when the in-flight compute finishes
        so the caller can ``Event.wait(timeout=...)`` to optionally block
        briefly for the result. After a stale-generation compute finishes,
        the deferred rerun runs asynchronously — callers re-poll to pick up
        its result, exactly as they re-poll any ``pending: true`` response.

        The generation snapshot is taken inside the lock at kickoff time so a
        concurrent invalidation can still drop the stale write — same race
        protection as :meth:`get_new_images_for_workspace`.

        ``can_start`` optionally defers automatic work while foreground jobs
        are active. It is rechecked for deferred reruns, which retain their
        ``on_spawn`` registration and progress callback.
        """
        # Automatic discovery yields to foreground work, including reruns
        # requested while an earlier generation was still walking. Polling
        # will retry once the foreground job finishes.
        if can_start is not None and not can_start():
            done = threading.Event()
            done.set()
            return done
        key = (db_path, workspace_id)
        with self._lock:
            # Race guard: a worker finishing between the caller's
            # ``cache.get()`` (which returned None) and this call would
            # write the result to ``_entries`` and then clear
            # ``_inflight`` — under separate lock acquires — so by the
            # time we arrive both are visible but the ``_inflight`` slot
            # is gone. Without this check we would spawn a redundant walk
            # that duplicates the just-finished one. If a fresh entry is
            # already cached, return an already-set event so the caller's
            # follow-up ``cache.get()`` picks it up.
            entry = self._entries.get(key)
            if entry is not None:
                _result, set_at = entry
                if time.monotonic() - set_at <= self._ttl:
                    done = threading.Event()
                    done.set()
                    return done

            err_entry = self._errors.get(key)
            if err_entry is not None:
                _err_msg, set_at = err_entry
                if time.monotonic() - set_at <= self.ERROR_BACKOFF_SECONDS:
                    # Suppress retry inside the backoff window.
                    done = threading.Event()
                    done.set()
                    return done
                # Backoff window elapsed — let a fresh attempt run.
                del self._errors[key]

            # Skip the spawn if a fresh, complete walk already sits in the
            # cache. Without this, a caller that saw an empty cache moments
            # ago can race the in-flight worker's finally block: worker
            # populates the cache, then clears its in-flight slot; the
            # caller then arrives here and sees no in-flight, so we'd fan
            # out a second walk over the same directories. All async walks
            # run with sample_limit=None (so sample_complete=True), so
            # gating on that flag is a precise "a completed walk exists"
            # check — sync ``get_new_images_for_workspace`` writes at
            # sample_limit=5 (sample_complete potentially False) and must
            # still fall through to spawn a real walk.
            entry = self._entries.get(key)
            if entry is not None:
                cached_result, set_at = entry
                if (time.monotonic() - set_at <= self._ttl
                        and cached_result.get("sample_complete")):
                    done = threading.Event()
                    done.set()
                    return done

            generation = self._generations.get(key, 0)
            existing = self._inflight.get(key)
            if existing is not None:
                existing_event, existing_generation = existing
                if existing_generation != generation:
                    # Stale-generation compute is running. Coalesce: stash the
                    # latest compute_fn so the worker spawns one follow-up
                    # when it finishes, instead of starting a parallel walk.
                    # Last writer wins — multiple kickoffs collapse to one
                    # rerun, so a burst of polls during scan-time invalidation
                    # storms can't queue a backlog of walks.
                    self._rerun_pending[key] = (compute_fn, on_spawn, can_start)
                return existing_event
            event = threading.Event()
            self._inflight[key] = (event, generation)

        # Run on_spawn outside the cache lock so user code (e.g. JobRunner
        # registration) cannot deadlock the cache on a contended lock.
        progress_cb = None
        if on_spawn is not None:
            try:
                progress_cb = on_spawn(event)
            except Exception:
                log.exception(
                    "new-images on_spawn callback raised; continuing without it"
                )
                progress_cb = None

        def worker():
            try:
                if progress_cb is not None:
                    result = compute_fn(progress_callback=progress_cb)
                else:
                    result = compute_fn()
                self.set(db_path, workspace_id, result, generation=generation)
                # Successful compute clears any prior failure so a transient
                # error doesn't keep suppressing retries after recovery.
                # Generation-guarded so a stale thread finishing after a
                # fresh one started doesn't wipe the fresh thread's error.
                with self._lock:
                    if self._generations.get(key, 0) == generation:
                        self._errors.pop(key, None)
            except Exception as e:
                log.exception(
                    "new-images background compute failed for %s ws=%s",
                    db_path, workspace_id,
                )
                with self._lock:
                    # Drop the failure if the generation moved while we were
                    # running. Mirrors the stale-write guard in :meth:`set`:
                    # if ``invalidate_workspaces`` ran mid-compute (workspace
                    # switched, scan completed), this error is for a key that
                    # has already moved on and must not force the next
                    # request into the 30s backoff window.
                    if self._generations.get(key, 0) == generation:
                        self._errors[key] = (str(e) or e.__class__.__name__,
                                             time.monotonic())
            finally:
                with self._lock:
                    # Only clear the in-flight slot if it still belongs to
                    # this thread. Defensive: under the coalescing design
                    # nothing else writes to this slot while we're alive,
                    # but the identity check keeps the invariant locally
                    # checkable rather than relying on global reasoning.
                    current = self._inflight.get(key)
                    if current is not None and current[0] is event:
                        del self._inflight[key]
                    rerun_fn = self._rerun_pending.pop(key, None)
                event.set()
                if rerun_fn is not None:
                    # A kickoff arrived during this compute while the
                    # generation was stale. Fire the deferred rerun now that
                    # the in-flight slot is free; it picks up the current
                    # generation inside the lock. Recursion depth is bounded:
                    # each rerun consumes its token and a new one is only
                    # added by another stale-generation kickoff arriving
                    # during the next worker.
                    compute_again, spawn_again, start_again = rerun_fn
                    self.kickoff_compute(
                        db_path, workspace_id, compute_again,
                        on_spawn=spawn_again, can_start=start_again,
                    )

        threading.Thread(
            target=worker, daemon=True, name="new-images-compute",
        ).start()
        return event

    def clear(self):
        with self._lock:
            self._entries.clear()
            self._generations.clear()
            self._inflight.clear()
            self._errors.clear()
            self._rerun_pending.clear()


_shared_cache = NewImagesCache()


def get_shared_cache():
    """Return the process-wide shared NewImagesCache.

    Per-thread and per-request Database instances all reference the same
    cache so invalidation from scan workers is visible to API readers.
    """
    return _shared_cache


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
    # Canonicalize the root to match what the scanner stores. scanner.scan passes
    # folder paths through str(Path(...)), which strips trailing slashes but
    # preserves `..` segments. Using os.path.normpath here would resolve `..` and
    # produce a mismatch against the stored path, leaving the cache stale after a
    # successful scan.
    root = str(Path(root))
    # LIKE wildcards (%, _) in `root` are not escaped. Worst case is a harmless
    # over-invalidation that triggers a re-walk. The descendant pattern uses
    # os.sep so it matches what the scanner stores via str(Path(...)) on both
    # POSIX and Windows.
    touched_ids = [r["id"] for r in db.conn.execute(
        "SELECT id FROM folders WHERE path = ? OR path LIKE ?",
        (root, root.rstrip("/\\") + os.sep + "%"),
    ).fetchall()]
    db.invalidate_new_images_cache_for_folders(touched_ids)
