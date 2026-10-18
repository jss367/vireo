"""``move_folder``'s destination and preflight checks (``_FolderMoveTarget``)."""

import os
import posixpath

from .paths import (
    _destination_overlaps_source,
    _path_equal_or_descends,
    _rebase_under_stored_ancestor,
    _tracked_destination_ancestor,
    _tracked_destination_overlap,
)
from .remote import _remote_dir_exists, rsync_dest_spec
from .verify import _find_content_conflict, _find_remote_content_conflict


class _FolderMoveTarget:
    """Where one ``move_folder`` copy lands, and the preflight that decides it.

    Owns the three destination views (``transfer_dest``, ``rsync_target``,
    ``catalog_path``), whether the destination already exists, and the
    tracked-archive merge decision (``merge_into_tracked``,
    ``merge_reconcile_base``). The later phases only read them. Each check
    returns the result dict to abort with, or None to continue; none of
    them copies, deletes or commits anything.
    """

    def __init__(self, db, folder_id, src_path, landing_name, *, merge, remote,
                 reject_tracked_ancestor, allow_tracked_merge):
        self.db = db
        self.folder_id = folder_id
        self.src_path = src_path
        self.landing_name = landing_name
        self.merge = merge
        self.remote = remote
        self.reject_tracked_ancestor = reject_tracked_ancestor
        self.allow_tracked_merge = allow_tracked_merge
        self.transfer_dest = None
        self.rsync_target = None
        self.catalog_path = None
        self.merge_into_tracked = None
        self.merge_reconcile_base = None
        self.dest_exists = False

    def resolve_destination_views(self, destination):
        """Resolve the three destination views.

          transfer_dest — where rsync writes (NAS-side path for remote, local
            path otherwise); also the path used for existence/verify.
          rsync_target  — transfer_dest addressed for rsync (user@host:path
            remote, bare path local).
          catalog_path  — where the catalog points AFTER the move (the local
            mount path for remote, same as transfer_dest local). The local
            destination and catalog coincide; for remote they diverge because
            the NAS path isn't reachable through the local filesystem.
        """
        remote = self.remote
        landing_name = self.landing_name
        if remote:
            ssh_base = remote.get("ssh_dest_base") or ""
            # Same hazard as the mount-path check below, on the NAS side: a
            # relative ssh_dest_base like "Photos" would ship to rsync as
            # ``user@host:Photos/<folder>`` and resolve under the SSH user's
            # remote cwd — but the catalog gets repointed to the absolute
            # mount_dest_base, so a verified copy can live at a different
            # remote location than the path Vireo records before originals are
            # deleted. ``_coerce_remote_target`` already drops relative-path
            # entries at the config boundary; this is the defense-in-depth
            # check for callers (tests, direct use) that build a remote dict
            # themselves. POSIX-absolute (startswith "/") because the NAS is
            # POSIX — os.path.isabs would accept ``C:\foo`` on Windows.
            if not ssh_base.startswith("/"):
                return {"moved": 0, "errors": [
                    "Remote target needs an absolute remote (NAS) path before "
                    "moving files — otherwise rsync would write under the SSH "
                    "user's cwd, not where the catalog will point. Set the "
                    "remote path under Settings → Remote targets."
                ]}
            mount_base = remote.get("mount_dest_base") or ""
            # Without an absolute mount path, resolve_folder_dest below would
            # produce a relative catalog_path like 'Birds' — then after the SSH
            # copy succeeds and originals are deleted, the catalog row points at
            # a non-resolving location relative to the server cwd. The
            # /api/jobs/move-folder route also validates this, but move_folder is
            # called directly from tests and other code paths; checking here too
            # means the bug can't slip past whichever caller forgets.
            if not os.path.isabs(mount_base):
                return {"moved": 0, "errors": [
                    "Remote target needs an absolute local mount path before "
                    "moving files — otherwise the catalog would point at a "
                    "relative location after the move. Set the mount path under "
                    "Settings → Remote targets."
                ]}
            # The NAS side is POSIX, so the SSH dest must be joined with '/' even
            # when this code runs on Windows; os.path.join would produce a
            # backslash and rsync would treat it as a single path segment.
            self.transfer_dest = posixpath.join(remote["ssh_dest_base"], landing_name)
            # Join landing_name directly rather than routing it back through
            # resolve_folder_dest: that helper calls normalize_destination_name,
            # which would re-trim/reject a value we've already resolved. When the
            # user didn't request a rename, landing_name is the raw folder_name
            # (potentially with surrounding whitespace, or POSIX-legal ``:``/``\``
            # on Linux/macOS filesystems that allow them). Preflight preserves
            # those characters — the move job must too, or the copy lands at a
            # different path than preflight showed and the catalog repoints to
            # yet another (trimmed) path.
            self.catalog_path = os.path.join(mount_base, landing_name)
            self.rsync_target = rsync_dest_spec(remote, self.transfer_dest)
        else:
            self.transfer_dest = os.path.join(destination, landing_name)
            self.catalog_path = self.transfer_dest
            self.rsync_target = self.transfer_dest
        return None

    def refuse_source_overlap(self):
        """Refuse a destination that overlaps the source.

        Moving a folder into itself (or into one of its own descendants) would
        make the post-copy rmtree(src) delete the only copy of the files. This
        is especially dangerous for a merge, where a destination equal to the
        source passes verification trivially (every source file is already
        "there") before the delete wipes everything. See
        _destination_overlaps_source for the alias surface (symlinks, Windows
        case folding, case-insensitive POSIX).

        The NAS-side transfer_dest can't alias the local source tree for a
        remote move — but the LOCAL MOUNT PATH the catalog is repointed to
        (catalog_path) absolutely can if the source already lives on the same
        mount. e.g. src=/Volumes/Photography/trip with remote mount_path=
        /Volumes/Photography would copy a tree onto itself over SSH; the
        checksum verify passes (everything is "already there") and then the
        rmtree(src) deletes the only copy. So check the local-facing path:
        transfer_dest for local moves, catalog_path for remote.
        """
        overlap_src_check = (self.catalog_path if self.remote
                             else self.transfer_dest)
        if _destination_overlaps_source(self.src_path, overlap_src_check):
            return {"moved": 0, "errors": [
                f"Destination overlaps the source folder: {overlap_src_check}"
            ]}
        return None

    def resolve_tracked_destination(self):
        """Refuse moving into — or around — a destination Vireo already tracks.

        A tracked folder is refused regardless of whether that path currently
        exists on disk. A correct tracked-tree merge needs recursive
        folder/photo reconciliation we don't do here; a partial attempt would
        leave folders pointing at the deleted source path, or collide on the
        folders.path UNIQUE constraint when the source's children cascade onto
        a tracked descendant. Match the destination itself and anything below
        it. The cases this feature exists for — resuming an interrupted move,
        or moving into an untracked folder — never hit this.

        For remote, check against `catalog_path` (the local mount path the
        catalog is repointed to after the move) rather than `transfer_dest` (the
        NAS-side path, which isn't in the local catalog). Without this guard, a
        remote move into a mount path that overlaps an already-scanned folder
        would copy the whole tree over SSH and then hit folders.path UNIQUE on
        the post-move db.move_folder_path cascade.

        Comparison goes through _path_equal_or_descends so symlink aliases,
        Windows case folding, AND case-only aliases on case-insensitive POSIX
        (default macOS APFS) all collapse to the same tracked row — otherwise
        a destination reached via any of those would slip past and leave two
        folder rows managing the same on-disk tree.

        When ``allow_tracked_merge`` is set (the local-processing archive commit
        opts in), a tracked destination is NOT an error: instead we remember the
        tracked path in ``merge_into_tracked`` and, after the verified file copy,
        reconcile the catalog by folding the staged folder/photo rows into the
        existing archive rows rather than calling ``db.move_folder_path`` (which
        would collide on folders.path UNIQUE). Default (flag off) behaviour is
        byte-for-byte unchanged: both tracked-destination cases refuse the move.
        """
        db = self.db
        overlap_check_path = (self.catalog_path if self.remote
                              else self.transfer_dest)
        # ``merge_reconcile_base`` is the catalog path the staged tree is
        # reconciled ONTO. Distinct from ``merge_into_tracked`` (the user-facing
        # "existing archive" label): for an exact overlap the reconciliation
        # base must be the STORED tracked path, not ``catalog_path``, because
        # ``catalog_path`` may be an alias (symlink / case-only fold) of the
        # tracked folder. The files rsync to the same on-disk location either
        # way, but ``merge_staged_tree_into_archive`` does exact
        # ``WHERE path = ?`` catalog lookups that only match the row stored under
        # the tracked path — rebasing onto the alias would miss it and create a
        # second folder row for the same archive.
        tracked = _tracked_destination_overlap(
            db, self.folder_id, overlap_check_path)
        if tracked:
            # ``_tracked_destination_overlap`` returns any tracked row at-or-below
            # ``overlap_check_path``. The opt-in merge only covers the "into an
            # existing archive" case where the tracked row IS the destination;
            # a tracked row STRICTLY BELOW the destination is the "wrap a fresh
            # parent around an existing tracked subtree" case (e.g. /Photos/USA
            # tracked, destination /Photos). The reconciliation would rebase the
            # staged tree onto the wrapper path and leave the pre-existing tracked
            # descendant with unchanged parentage — two overlapping catalog
            # subtrees managing the same on-disk area. Refuse even when
            # ``allow_tracked_merge`` is set. Uses the same alias-folding surface
            # as the overlap probe (symlinks, Windows case-fold, case-insensitive
            # POSIX) so a case-only alias of the destination still counts as the
            # tracked row itself, not a wrapping parent.
            tracked_is_destination = _path_equal_or_descends(
                overlap_check_path, tracked["path"],
            )
            if not self.allow_tracked_merge or not tracked_is_destination:
                return {"moved": 0, "errors": [
                    f"Destination overlaps a folder Vireo already manages "
                    f"({tracked['path']}). Merging into or around a tracked folder "
                    f"isn't supported."
                ]}
            self.merge_into_tracked = tracked["path"]
            # Reconcile onto the STORED tracked path (not the possibly-aliased
            # ``catalog_path``) so the existing archive rows are found, not
            # duplicated. See the ``merge_reconcile_base`` note above.
            self.merge_reconcile_base = tracked["path"]
        if self.reject_tracked_ancestor and self.merge_into_tracked is None:
            return self._resolve_tracked_ancestor(overlap_check_path)
        return None

    def _resolve_tracked_ancestor(self, overlap_check_path):
        """Refuse, or with ``allow_tracked_merge`` merge into, a tracked
        folder that contains the destination."""
        ancestor = _tracked_destination_ancestor(
            self.db, self.folder_id, overlap_check_path)
        if ancestor:
            if not self.allow_tracked_merge:
                return {"moved": 0, "errors": [
                    f"Destination is inside a folder Vireo already manages "
                    f"({ancestor['path']}). Pick an untracked archive destination."
                ]}
            # Merge into the existing archive root that contains the
            # destination. The staged tree lands at its own resolved
            # catalog_path (inside the tracked ancestor); the reconciliation
            # rebases staged rows onto that path and leaves the ancestor's own
            # rows untouched. The user-facing "existing archive" base we
            # report is the managed-archive root (``ancestor["path"]``), not
            # the staged landing path inside it.
            #
            # The reconciliation base is the STORED ancestor's path with the
            # relative-below-ancestor suffix appended, NOT the user-entered
            # ``catalog_path`` — those two only agree when the ancestor probe
            # matched by pure string prefix. When it matched via a symlink,
            # Windows case-fold, or POSIX case-only alias (catalog stores
            # ``/Photos``, user selects ``/photos/NewShoot``),
            # ``catalog_path`` has an alias prefix that ``merge_staged_tree_
            # into_archive``'s exact ``WHERE path = ?`` parent lookups miss.
            # That would land the staged root with ``parent_id=NULL`` under an
            # alias-prefixed path, spawning a parallel row set outside the
            # managed archive tree. Fold the alias prefix to the stored form
            # here.
            self.merge_into_tracked = ancestor["path"]
            self.merge_reconcile_base = _rebase_under_stored_ancestor(
                self.catalog_path, ancestor["path"])
        return None

    def probe_destination(self):
        remote = self.remote
        transfer_dest = self.transfer_dest
        if remote:
            probe = _remote_dir_exists(remote, transfer_dest)
            if probe is None:
                # Refuse rather than proceed as a fresh transfer: a transient SSH
                # failure on a real existing destination would otherwise omit
                # --ignore-existing and let rsync overwrite same-name files before
                # the post-transfer --checksum verify could preserve the originals.
                return {"moved": 0, "errors": [
                    f"Couldn't probe remote destination via SSH: "
                    f"{rsync_dest_spec(remote, transfer_dest)}. "
                    f"Refusing the move so a transient SSH error isn't confused "
                    f"with an absent destination."
                ]}
            self.dest_exists = probe
        else:
            self.dest_exists = os.path.exists(transfer_dest)
        if self.dest_exists and not self.merge:
            return {
                "moved": 0,
                "errors": [f"Destination already exists: {transfer_dest}"],
                "needs_merge": True,
            }
        return None

    def refuse_content_conflict(self):
        """Refuse if any same-name file already at the destination differs.

        Never overwrite or later delete the user's data over a real
        collision — only files that are byte-identical (a genuine resume)
        may be treated as already-moved. Both branches enforce the same
        contract: a content conflict cancels the move with NOTHING copied
        or deleted on either end. Finder ``.DS_Store`` files are ignored on
        both sides; see ``FINDER_METADATA_FILES``.
        """
        if not self.dest_exists:
            return None
        src_path = self.src_path
        remote = self.remote
        if remote:
            # The destination lives on the NAS, so the walk is delegated to
            # rsync over SSH: ``-an --existing --checksum`` inspects only
            # files that already exist at the receiver and reports any whose
            # bytes don't match. Without this pre-copy check, the actual
            # transfer (--ignore-existing + --partial-dir) would still copy
            # every MISSING source file before the post-transfer --checksum
            # verify could surface the conflict — leaving stray newly-copied
            # files orphaned on the NAS instead of cancelling cleanly. A
            # probe error is also treated as a refusal so a flaky link can't
            # downgrade the "nothing changed" guarantee.
            remote_rsync = remote.get("rsync_bin")
            if not remote_rsync:
                return {"moved": 0, "errors": [
                    "No usable GNU rsync binary is available for remote moves. "
                    "Install it or set the GNU rsync path in Settings."
                ]}
            conflict = _find_remote_content_conflict(
                remote_rsync, src_path, self.rsync_target, remote)
            if conflict is not None:
                name, detail = conflict
                if name == "__ERROR__":
                    return {"moved": 0, "errors": [
                        f"Pre-merge content check could not run ({detail}). "
                        f"Nothing was copied — re-run when the connection is "
                        f"stable."
                    ]}
                return {"moved": 0, "errors": [
                    f"Conflict: '{name}' already exists at the remote "
                    f"destination with different content. Nothing was copied "
                    f"or deleted."
                ]}
        else:
            conflict = _find_content_conflict(src_path, self.transfer_dest)
            if conflict is not None:
                return {"moved": 0, "errors": [
                    f"Conflict: '{conflict}' already exists at the destination "
                    f"with different content. Nothing was copied or deleted."
                ]}
        return None
