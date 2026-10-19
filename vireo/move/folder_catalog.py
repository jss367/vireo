"""``move_folder``'s catalog rewrite (``_FolderCatalogRewrite``).

Includes planning the ``file_mtime`` corrections that the rewrite applies
once the rows point at the destination.
"""

import logging
import os

try:
    from ..db import _chunks, _join_subtree_path, _subtree_prefix, _subtree_relative
except ImportError:
    from db import _chunks, _join_subtree_path, _subtree_prefix, _subtree_relative
from .paths import _case_insensitive_root, _path_equal_or_descends

# The package's one logger, named as before the split.
log = logging.getLogger("move")


def _plan_moved_file_mtimes(db, src_path, dest_path, progress_cb=None):
    """Plan re-stamping ``photos.file_mtime`` from the copy at ``dest_path``.

    A copy does not always carry the source's timestamp across. rsync
    writes each file under a temp name, sets the times on it, then renames
    it into place -- and a rename is not guaranteed to preserve the mtime.
    On a macOS smbfs mount against a Synology share (measured: ``copy2``
    to the final name keeps the timestamp, the same file renamed into
    place comes back stamped with the current time) every transferred file
    lands with a fresh mtime, so the catalog is left describing a
    timestamp that no file on disk has.

    That matters because the incremental scan's "unchanged" test is
    ``file_mtime`` + ``file_size`` (see ``scanner.scan``). A folder whose
    recorded timestamps no longer match disk is re-hashed and re-phashed
    in full on the next scan, only to discover that nothing changed --
    tens of GB read back over the wire for a folder of RAWs on a network
    mount.

    Only rows the catalog still describes accurately are re-stamped: the
    source file must still carry the stored ``file_mtime`` and
    ``file_size``. A photo edited since its last scan fails that test and
    keeps its stale timestamp, so the next scan still reprocesses it.
    Adopting the destination's timestamp for those would be worse than the
    problem being fixed here -- the row would look current while its hash,
    phash and metadata still described the pre-edit bytes, and no later
    scan would ever revisit it.

    A source file that cannot be stat'd is skipped: verification walked the
    source tree, so it never made a claim about a row whose file is already
    gone, and there is nothing to re-stamp. A DESTINATION file that cannot
    be stat'd is different -- see below.

    Call after the copy is verified and before the originals are removed,
    while both sides are still readable and the rows still name the source
    files.

    Returns ``(updates, unreadable)``. ``updates`` is ``executemany``
    parameters for the rows to re-stamp; read-only itself, so the caller
    applies them once the catalog update it belongs with has gone through
    and a cascade that fails leaves no half-applied timestamps behind.
    ``problem`` describes a destination file that contradicts what
    verification established -- missing, unreadable, or a different size
    from the source still sitting next to it -- or None.

    That second return value exists because this pass is the last thing
    that looks at the destination before ``shutil.rmtree`` deletes the
    originals. Verification established "every source file is present at
    the destination" minutes earlier; a source that is still here whose
    copy has since vanished or gone unreadable means that no longer holds,
    and ``check_staged_mount`` will not notice -- it re-checks mount
    identity and availability, not the files. Swallowing that would throw
    away first-hand evidence at the worst possible moment, so it is handed
    back for the caller to treat as a verification failure.
    """
    rows = _moved_tree_photo_rows(db, src_path)
    updates = []
    # Hoisted: the probe is per-ancestor, and the containment answer depends
    # only on the folder, so a tree of thousands of photos costs one realpath
    # per distinct folder rather than one per photo.
    ci_root = _case_insensitive_root(src_path)
    contained = {}
    total_rows = len(rows)
    # Announced even when there is nothing to check, so the step always
    # starts and finishes rather than being skipped over.
    if progress_cb:
        progress_cb(0, total_rows, "", "Checking timestamps")
    for index, row in enumerate(rows):
        # One stat per side per photo, on a mount that may be slow enough
        # for that to be visible, so count the rows as they go.
        if progress_cb and index % 100 == 0:
            progress_cb(index, total_rows, row["filename"],
                        "Checking timestamps")
        folder_path = row["folder_path"]
        if folder_path not in contained:
            contained[folder_path] = _path_equal_or_descends(
                folder_path, src_path, ci_root)
        if not contained[folder_path]:
            # Prefilter slack: this row is not actually in the moved tree.
            continue
        update, problem = _moved_row_mtime_update(row, src_path, dest_path)
        if problem is not None:
            return [], problem
        if update is not None:
            updates.append(update)
    if progress_cb:
        progress_cb(total_rows, total_rows, "", "Checking timestamps")
    return updates, None


def _moved_tree_photo_rows(db, src_path):
    """Photo rows (with their folder path) at or under ``src_path``."""
    # The repo's literal subtree predicate, not a raw LIKE: LIKE would read
    # ``_`` and ``%`` in a real folder path as wildcards and match
    # case-insensitively, pulling in sibling trees this move never touched --
    # and their rows would then resolve to files outside ``dest_path``. It
    # also normalizes separators, so Windows descendants stored with
    # backslashes are matched rather than silently skipped.
    #
    # It is a PREFILTER, not the authority. ``_path_for_subtree_match`` folds
    # ``\\`` to ``/`` on every platform, and ``\\`` is a legal filename
    # character on POSIX -- so moving ``/photos/shoot\\1`` normalizes to the
    # same prefix as the unrelated ``/photos/shoot/1`` tree. Every row is
    # re-checked below against the move module's own alias-folding
    # containment test (symlinks, Windows case folding, case-insensitive
    # POSIX), which is FS truth rather than string shape.
    prefix = _subtree_prefix(src_path)
    return db.moves_merge.subtree_photo_rows(src_path, prefix)


def _moved_row_mtime_update(row, src_path, dest_path):
    """Check one moved photo row against its copy at ``dest_path``.

    Returns ``(update, problem)``: ``update`` is the row's ``executemany``
    parameters when its copy needs re-stamping, else None; ``problem``
    describes a destination copy that contradicts verification, else None.
    See ``_plan_moved_file_mtimes``.
    """
    folder_path = row["folder_path"]
    stored_mtime, stored_size = row["file_mtime"], row["file_size"]
    if stored_mtime is None or stored_size is None:
        return None, None
    src_file = os.path.join(folder_path, row["filename"])
    if os.path.islink(src_file):
        # Scanner discovery admits file symlinks, and ``os.stat`` below
        # follows them -- but a relative target resolves against the
        # directory holding the link, so the same target string can point
        # at a different file once the link has moved. Re-stamping from
        # whatever the destination-side link resolves to would tell the
        # incremental scanner that bytes it has never seen are unchanged,
        # leaving this row's hash and metadata describing the wrong file.
        # Leave it stale; a rescan resolves the link itself.
        return None, None
    # Prefix-strip rather than ``os.path.relpath``: relpath is happy to
    # walk out of the subtree with ``..`` if a row ever slipped past the
    # predicate above, which would point this at a file the move never
    # copied.
    relative = _subtree_relative(folder_path, src_path)
    dst_file = _join_subtree_path(
        dest_path,
        f"{relative}/{row['filename']}" if relative else row["filename"],
    )
    try:
        src_st = os.stat(src_file)
    except OSError:
        # A row whose file is already gone: not something this move
        # copied, and not something verification vouched for.
        return None, None
    try:
        dst_st = os.stat(dst_file)
    except OSError:
        return None, (f"'{dst_file}' is missing or unreadable at the "
                      f"destination")
    if src_st.st_mtime != stored_mtime or src_st.st_size != stored_size:
        # The row does not describe the file being moved -- it was
        # edited (or replaced) since its last scan. Leave it stale so
        # the scan that would have caught that still does.
        return None, None
    if dst_st.st_size != stored_size:
        # The guard above already confirmed the source still matches the
        # row, so a differently sized copy differs from the file this
        # move is about to delete. On a merge the verification below
        # rejects it; a fresh local move only compares file counts, so
        # nothing else ever would, and the rmtree would take the intact
        # original with it. Skipping the row would leave that damage
        # unreported -- the same mistake as swallowing a failed stat.
        return None, (f"'{dst_file}' does not match the source's size at "
                      f"the destination")
    if dst_st.st_mtime == stored_mtime:
        # The timestamp survived the copy: nothing to correct.
        return None, None
    return (dst_st.st_mtime, row["id"], stored_mtime, stored_size), None


class _FolderCatalogRewrite:
    """Repoint the catalog at the verified copy, then rebase what is keyed to it.

    Owns the moved-photo count, the tracked-merge counts and the number of
    ``file_mtime`` corrections applied. ``update_catalog`` is the commit
    point; ``apply_mtime_corrections`` and ``relocate_developed_dirs`` run
    after it and never abort the move. Reads the destination views and the
    tracked-merge decision from ``target``.
    """

    def __init__(self, db, folder_id, src_path, landing_name, target, *,
                 progress, pre_commit_check, thumb_cache_dir, developed_dir):
        self.db = db
        self.folder_id = folder_id
        self.src_path = src_path
        self.landing_name = landing_name
        self.target = target
        self.progress = progress
        self.pre_commit_check = pre_commit_check
        self.thumb_cache_dir = thumb_cache_dir
        self.developed_dir = developed_dir
        self.total_photos = 0
        self.merge_counts = None
        self.mtimes_refreshed = 0

    def update_catalog(self):
        """Update DB first: cascade folder paths.

        Safer — if rmtree fails, the old folder becomes an orphan on disk
        rather than the DB pointing to deleted paths. Unless the caller opted
        into merging (``merge_into_tracked``), a merge into an already-tracked
        destination is refused above, so catalog_path is never a different
        existing folder row in the cascade branch and that cascade (root + all
        descendants) cannot collide with folders.path UNIQUE. When merging into
        a tracked archive we instead reconcile the staged rows into the
        existing archive rows below. For a remote move catalog_path is the
        local mount path, so the catalog keeps resolving to the photos whenever
        the NAS is mounted.
        """
        db = self.db
        src_path = self.src_path
        # Count photos for progress
        all_photos = db.moves_merge.photo_ids_under_path(src_path)
        self.total_photos = len(all_photos)

        # One database transaction: no per-item count to show.
        self.progress(0, 0, "", "Updating catalog")
        if self.pre_commit_check:
            self.pre_commit_check()
        if self.target.merge_into_tracked is not None:
            # Destination is a tracked archive and the caller opted into merging:
            # fold the staged folder/photo rows into the existing archive rows
            # instead of a path cascade (which would collide on folders.path).
            # ``merge_reconcile_base`` is the STORED tracked path for an exact
            # overlap (so alias/case-fold destinations still match the existing
            # rows) and ``catalog_path`` for the ancestor case; see where it is set.
            self.merge_counts = db.merge_staged_tree_into_archive(
                self.folder_id, self.target.merge_reconcile_base)
        else:
            db.moves_merge.move_folder_path(self.folder_id, self.target.catalog_path,
                                new_name=self.landing_name)
        db.folders.update_counts()

    def apply_mtime_corrections(self, mtime_updates):
        """Only now that the rows live at the destination.

        Guarded on the values the plan was built from, so a concurrent scan
        that committed a fresh stat of its own meanwhile wins instead of being
        overwritten from a stale snapshot -- and an id the merge dropped as an
        already-present collision simply matches nothing.
        """
        db = self.db
        if not mtime_updates:
            return
        # Carry the mtime-pinned working-copy markers along with the
        # correction. Both record "the ``file_mtime`` this decision was made
        # against": ``working_copy_evicted_mtime`` marks a rendition the
        # quota deliberately dropped (scanner's backfill clause treats
        # ``!= file_mtime`` as "the file changed, redo it"), and
        # ``working_copy_failed_mtime`` marks one whose extraction failed
        # (``render_source`` retries as soon as the two differ). Re-stamping
        # ``file_mtime`` alone would silently invalidate both, and every
        # moved folder would re-read its RAWs over the NAS to regenerate
        # renditions that were dropped on purpose, or to retry extractions
        # that will fail exactly as before.
        #
        # Only a marker pinned to the timestamp being corrected moves. The
        # bytes are unchanged, so those decisions still hold; anything
        # recorded against a different timestamp -- including the ``-1``
        # sentinel used when a row had no mtime at all -- was made against a
        # state this correction knows nothing about, and is left alone.
        self.mtimes_refreshed = db.moves_merge.restamp_photo_mtimes(
            mtime_updates)
        # Same reasoning as the working-copy markers, one table over.
        # ``offline_originals.source_mtime`` records the ``file_mtime`` its
        # cached copy was taken from, and ``offline_cache`` treats a
        # mismatch as "the original changed" -- so correcting ``file_mtime``
        # alone would mark every cached original stale and have the next
        # cache preparation re-copy bytes that never changed. Only a row
        # pinned to the timestamp being corrected, for a file of the same
        # size, moves with it.
        db.moves_merge.restamp_offline_original_mtimes(mtime_updates)
        db.commit()
        log.info(
            "Re-stamped file_mtime for %d photo(s) under %s from the copy "
            "at %s -- the transfer did not carry their timestamps across",
            self.mtimes_refreshed, self.src_path, self.target.catalog_path,
        )
        if self.thumb_cache_dir and mtime_updates:
            self._align_thumbnail_mtimes(mtime_updates)

    def _align_thumbnail_mtimes(self, mtime_updates):
        """Touch each cached thumbnail to its photo's corrected timestamp.

        The thumbnail endpoint gates cache freshness on ``cached_mtime >=
        photos.file_mtime``, and ``generate_thumbnail`` pegs a rendered
        thumbnail's file mtime to the source ``file_mtime`` it was made
        from. Advancing ``file_mtime`` alone would leave every cached
        thumbnail in the moved folder pinned below the new value: the next
        fetch would treat it as stale and regenerate, even though the
        pixels still describe the same unchanged bytes. Touch each
        thumbnail file to the corrected timestamp so the invariant
        continues to hold. Best-effort: a failure to touch is non-fatal --
        the worst case is a one-shot regeneration on next access, exactly
        the pre-fix behavior.

        Everything here runs AFTER the catalog commit above, so it is
        wrapped whole: an exception escaping at this point would leave the
        catalog repointed at the destination, the originals still sitting
        at the source, and the move reported as failed. Nothing about
        thumbnail freshness is worth that half-state, so any failure is
        logged and swallowed -- the cost is one regeneration on next
        access, which is exactly the behaviour without this block.
        """
        db = self.db
        try:
            fresh_by_id = {photo_id: fresh
                           for fresh, photo_id, _, _ in mtime_updates}
            # Chunked: a folder of a few thousand photos would otherwise
            # bind one variable per id and trip SQLite's
            # SQLITE_MAX_VARIABLE_NUMBER (999 on older builds) -- and the
            # transfer this exists for moved 1099 in one go.
            thumb_rows = []
            for chunk in _chunks(list(fresh_by_id)):
                thumb_rows.extend(db.moves_merge.thumb_paths_for_ids(chunk))
            for row in thumb_rows:
                fresh = fresh_by_id.get(row["id"])
                if fresh is None:
                    continue
                thumb_file = os.path.join(
                    self.thumb_cache_dir, row["thumb_path"])
                try:
                    os.utime(thumb_file, (fresh, fresh))
                except OSError:
                    log.debug(
                        "Could not align thumbnail mtime for photo %s "
                        "at %s", row["id"], thumb_file, exc_info=True,
                    )
        except Exception:
            log.exception(
                "Could not align thumbnail mtimes under %s after the "
                "move; they will regenerate on next access", self.src_path,
            )

    def relocate_developed_dirs(self):
        """Rebase any developed-output subdirs nested under the configured
        darktable_output_dir.

        `developed_folder_key` hashes the folder's path, so the DB update above
        just invalidated the old subdir's implicit key — rename it on disk to
        match the new path, and cascade to any descendant folders whose paths
        also shifted.

        On the merge path the catalog was reparented onto
        ``merge_reconcile_base``, not ``catalog_path`` — those two only
        differ when the tracked destination was reached via a symlink or
        case-alias (see where ``merge_reconcile_base`` is set), but when they
        do differ the developed-dir key is derived from the STORED path, not
        the alias. Relocating from ``src_path`` to the aliased
        ``catalog_path`` would move renders under the alias-path hash and
        exports (which read the catalog's stored path) would look under the
        stored-path hash, miss them, and fall back to RAW. Use the same
        reconciled base the catalog uses.
        """
        src_path = self.src_path
        developed_dir = self.developed_dir
        developed_base = (self.target.merge_reconcile_base
                          if self.target.merge_into_tracked is not None
                          else self.target.catalog_path)
        if developed_dir:
            from export import relocate_developed_dir
            relocate_developed_dir(developed_dir, src_path, developed_base)
            # Each descendant's old path is its path relative to the new base,
            # rejoined under the source in the source's own separator, so a
            # Windows catalog (``\\``) rebuilds the exact string its developed
            # key was hashed from.
            for new_child, relative in self.db.moves_merge.folder_paths_below(
                    developed_base):
                # POSIX backslashes belong to names, including at the end
                # of the source root or inside the relative child path.
                old_child = (src_path.rstrip("/") + "/" + relative
                             if src_path.startswith("/")
                             else _join_subtree_path(src_path, relative))
                relocate_developed_dir(developed_dir, old_child, new_child)
