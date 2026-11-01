"""Moving a whole folder tree (``move_folder``).

``move_folder`` resolves the source and landing name, then ``_FolderMove``
runs the move's phases in order through three collaborators:
``_FolderMoveTarget`` (``folder_target``), ``_FolderTransfer``
(``folder_transfer``) and ``_FolderCatalogRewrite`` (``folder_catalog``).
"""

import logging
import os
import shutil
from contextlib import nullcontext
from dataclasses import dataclass

from .folder_catalog import _FolderCatalogRewrite
from .folder_target import _FolderMoveTarget
from .folder_transfer import _FolderTransfer
from .paths import normalize_destination_name

# The package's one logger, named as before the split.
log = logging.getLogger("move")


@dataclass(frozen=True)
class FolderMoveOptions:
    """Folder transfer settings, separate from progress and job controls."""

    merge: bool = False
    remote: dict | None = None
    reject_tracked_ancestor: bool = False
    allow_tracked_merge: bool = False
    destination_name: str = ""
    verify_contents: bool = False
    pre_commit_check: object = None
    thumb_cache_dir: str | None = None


def move_folder(db, folder_id, destination, progress_cb=None, developed_dir="",
                *, control=None, **options):
    """Move an entire folder (and subfolders) to a destination.

    The folder is placed inside the destination, preserving its name unless
    ``destination_name`` explicitly renames it. E.g., moving /local/birds to
    /nas/photos creates /nas/photos/birds by default.

    Args:
        db: Database instance
        folder_id: ID of the source folder
        destination: absolute path to parent destination directory. Ignored
            for a remote move (the destination comes from ``remote``).
        destination_name: optional new name for the folder at the destination.
            Must be one path component. Empty preserves the source name.
        verify_contents: verify every destination file's contents before
            updating the catalog and deleting the source originals. Mounted
            NAS destinations use checksums over a proven saved SSH route
            when available, with local byte comparison as the fallback.
        pre_commit_check: optional callback that raises if the destination
            is no longer safe, after verification and before catalog changes.
        progress_cb: optional callback(current, total, filename)
        merge: when False (default), refuse to write into a destination
            that already exists — the safe all-or-nothing behavior. When
            True, merge/resume into the existing destination: a pre-copy scan
            refuses the merge if any same-name file differs in content;
            otherwise rsync (``--ignore-existing``) copies only the files
            missing at the destination and never overwrites one already there
            (this is how an interrupted move is resumed). Originals are deleted
            only after every source file is verified present at the
            destination. A failed merge never removes the destination, since it
            may hold the user's pre-existing files. Finder ``.DS_Store`` files
            are ignored on both sides: the copy skips them, the conflict and
            verification checks skip them, and the source's copy is discarded
            only when the whole source tree is removed after success — a
            failed merge leaves the source ``.DS_Store`` in place.
        developed_dir: optional path to the configured
            `darktable_output_dir`. When set, the folder's developed
            subdirectory — nested under a hash of its source path, see
            `export.developed_folder_key` — is rebased to match the new
            path after the move. Without this, exports silently fall
            back to RAW for every previously-developed photo in the
            moved folder.
        remote: optional dict to transfer over SSH instead of to a local
            path. Keys: ``host``, ``user``, ``port``, ``ssh_key``,
            ``bwlimit_kbps``, ``rsync_bin`` (a GNU rsync path), and the two
            destination *parents* — ``ssh_dest_base`` (the NAS-side
            filesystem path rsync writes to) and ``mount_dest_base`` (the
            local path, e.g. an SMB mount, where Vireo can read those same
            files afterward). The transfer and verification use the SSH
            path; the catalog is repointed at the mount path once the move
            succeeds, so the photos stay in the library and resolve whenever
            the NAS is mounted.
        reject_tracked_ancestor: when True, also reject a destination inside
            another tracked folder. Normal user-initiated moves can validly
            move into a tracked destination parent; local-processing archive
            commits opt into this stricter guard because they create a new
            top-level archive root and cannot be reparented under the
            existing catalog row.
        allow_tracked_merge: when True, a tracked destination (overlap or, with
            ``reject_tracked_ancestor``, ancestor) is no longer an error.
            Instead, after the verified file copy, the staged folder/photo rows
            are folded into the existing archive rows via
            ``moves_merge.merge_staged_tree_into_archive`` (new folders repointed,
            identical-filename photos dropped) rather than a path cascade. Only
            the local-processing archive commit opts in; manual and remote
            moves keep the default refusal. The result then carries ``merge``
            (the reconciliation counts) and ``merged_into_existing`` (the
            tracked archive path).
        thumb_cache_dir: optional path to the thumbnail cache directory. When
            a timestamp-losing transfer advances a row's ``file_mtime``, the
            thumbnail endpoint's freshness invariant (``cached_mtime >=
            file_mtime``) treats the existing cached thumbnail as stale and
            regenerates on first access. If this is set, the corresponding
            thumbnail files are ``os.utime``d alongside the DB update so the
            already-correct pixels stay served without a regeneration.

    Returns dict with keys: moved (int), errors (list of str). When the
    catalog has already been repointed at the new destination but deleting
    the source originals afterwards fails, an extra ``cleanup_error`` (str)
    is included — the archive is committed and ``errors`` stays empty, but
    callers should still surface the leftover originals to the user.
    """
    options = FolderMoveOptions(**options)
    src_path, folder_name, landing_name, error = _folder_move_source(
        db, folder_id, options.destination_name)
    if error is not None:
        return error

    move = _FolderMove(
        db, folder_id, src_path, landing_name,
        progress_cb=progress_cb, developed_dir=developed_dir,
        options=options, control=control,
    )
    return move.run(destination, folder_name)


def _folder_move_source(db, folder_id, destination_name):
    """Resolve ``move_folder``'s source folder and landing name.

    Returns ``(src_path, folder_name, landing_name, error)``; ``error`` is
    ``move_folder``'s refusal dict (the other three None), else None.
    """
    folder = db.moves_merge.folder_source_row(folder_id)
    if not folder:
        return None, None, None, {"moved": 0, "errors": ["Folder not found"]}

    src_path = folder["path"]
    # rstrip separators before basename() so a legacy row stored with a
    # trailing '/' or '\\' still yields the folder leaf; without it a
    # nameless folder row falls back to an empty landing_name here, and the
    # copy lands directly in the selected parent (or merges into it) even
    # though preflight — which uses the same rstrip in resolve_folder_dest
    # and the remote branch — approves ``<parent>/<source-leaf>``.
    folder_name = folder["name"] or os.path.basename(src_path.rstrip("/\\"))
    try:
        landing_name = normalize_destination_name(destination_name) or folder_name
    except ValueError as exc:
        return None, None, None, {"moved": 0, "errors": [str(exc)]}
    return src_path, folder_name, landing_name, None


class _FolderMove:
    """Coordinate one ``move_folder`` call.

    The phases live on three collaborators, each owning only the state it
    writes:

      target   (``_FolderMoveTarget``) — the destination views and the
        preflight checks that resolve them;
      transfer (``_FolderTransfer``) — the copy and its verification;
      catalog  (``_FolderCatalogRewrite``) — the catalog rewrite and the
        post-commit rebases keyed to it.

    ``run`` calls them in order. Each pre-commit phase returns the result
    dict to abort with, or None to continue; ``run`` returns the first one
    it gets. The post-commit phases never abort. Deleting the originals
    and building the result stay here.
    """

    def __init__(self, db, folder_id, src_path, landing_name, *, progress_cb,
                 developed_dir, options, control):
        self.src_path = src_path
        self.progress_cb = progress_cb
        self.control = control
        self.source_identity = None
        self.target = _FolderMoveTarget(
            db, folder_id, src_path, landing_name, merge=options.merge, remote=options.remote,
            reject_tracked_ancestor=options.reject_tracked_ancestor,
            allow_tracked_merge=options.allow_tracked_merge,
            control=control,
        )
        self.transfer = _FolderTransfer(
            db, src_path, self.target, remote=options.remote,
            verify_contents=options.verify_contents, progress_cb=progress_cb,
            progress=self.progress, control=control,
        )
        self.catalog = _FolderCatalogRewrite(
            db, folder_id, src_path, landing_name, self.target,
            progress=self.progress, pre_commit_check=options.pre_commit_check,
            thumb_cache_dir=options.thumb_cache_dir, developed_dir=developed_dir,
        )
        self.cleanup_error = None

    def progress(self, current, total, filename, phase):
        if self.control and (phase in ("Checking timestamps", "Checking NAS verification")
                             or phase.startswith("Verifying copy")):
            self.control.checkpoint()
        if self.progress_cb:
            self.progress_cb(current, total, filename, phase)

    def run(self, destination, folder_name):
        """Run the checks and phases in order; return the move's result."""
        target, transfer, catalog = self.target, self.transfer, self.catalog
        error = target.resolve_destination_views(destination)
        if error is not None:
            return error

        # Validation (overlap, tracked-folder, and the per-file content-conflict
        # scan a merge runs) can take a noticeable moment on a large tree, so name
        # the phase before it starts rather than leaving the bar blank.
        self.progress(0, 0, "", "Checking destination")
        for check in (target.refuse_source_overlap,
                      target.resolve_tracked_destination,
                      target.probe_destination,
                      target.refuse_content_conflict):
            if self.control:
                self.control.checkpoint()
            error = check()
            if error is not None:
                return error

        log.info("%s folder %s -> %s",
                 "Merging" if target.dest_exists else "Moving", self.src_path,
                 target.rsync_target)

        if self.control:
            source_stat = os.stat(self.src_path)
            self.source_identity = (source_stat.st_dev, source_stat.st_ino)

        for phase in (transfer.ensure_remote_parent, transfer.copy_tree):
            if self.control:
                self.control.checkpoint()
                error = self.validate_source()
                if error:
                    return error
            error = phase()
            if error is not None:
                return error

        guard = self.control.commit_context if self.control else nullcontext
        with guard():
            error = self.verify_and_begin_commit()
            if error is not None:
                return error
            catalog.update_catalog()
            catalog.apply_mtime_corrections(transfer.mtime_updates)
            catalog.relocate_developed_dirs()
            self.remove_originals()
            self.progress(transfer.total_files, transfer.total_files, folder_name,
                          "Done")
        return self.result()

    def verify_and_begin_commit(self):
        """Never delete originals using verification from before a pause."""
        while True:
            if self.control:
                self.control.checkpoint()
                error = self.validate_source()
                if error:
                    return error
                if self.control.generation:
                    # A paused transfer no longer exclusively owns a fresh
                    # destination. Preserve every existing file and recheck
                    # catalog ownership before publishing or cleaning up.
                    self.target.dest_exists = True
                    self.transfer.verify_contents = True
                    error = self.target.resolve_tracked_destination()
                    if error:
                        return error
            generation = self.control.generation if self.control else 0
            for phase in (self.transfer.plan_mtime_corrections,
                          self.transfer.verify_copy):
                error = phase()
                if error is not None:
                    return error
            if self.control is None:
                return None
            if generation == self.control.generation and self.control.try_commit():
                return None
            self.control.checkpoint()

    def validate_source(self):
        """A released transfer slot must not let us commit another move's tree."""
        row = self.catalog.db.moves_merge.folder_source_row(self.catalog.folder_id)
        try:
            st = os.stat(self.src_path)
            same_directory = (st.st_dev, st.st_ino) == self.source_identity
        except OSError:
            same_directory = False
        if row is None or row["path"] != self.src_path or not same_directory:
            return {"moved": 0, "errors": [
                "The source folder changed during the transfer. "
                "The catalog was not updated and source files were not removed."
            ]}
        return None

    def remove_originals(self):
        """Delete originals.

        The catalog already points at the new destination, so anything that
        goes wrong from here is post-commit: the archive is already published.
        Record a ``cleanup_error`` so the caller can warn the user about
        leftover originals without misreporting the move as failed (which
        would also leave the archive's tracked row in place while telling the
        user their data is still in staging).
        """
        src_path = self.src_path
        # A single rmtree: no per-item count to show.
        self.progress(0, 0, "", "Removing originals")
        log.info("Verification passed, deleting originals: %s", src_path)
        try:
            shutil.rmtree(src_path)
        except OSError as e:
            log.exception("Post-commit cleanup of %s failed", src_path)
            self.cleanup_error = str(e)

    def result(self):
        merge_counts = self.catalog.merge_counts
        result = {"moved": self.catalog.total_photos, "errors": [],
                  "mtimes_refreshed": self.catalog.mtimes_refreshed}
        if self.target.merge_into_tracked is not None:
            # ``dropped_photo_ids`` is a cleanup handle for the caller (thumbnails,
            # previews, offline copies of the deleted staged photos), not a
            # user-facing count. Lift it off ``merge_counts`` so ``result["merge"]``
            # stays a stable dict of display numbers that gets serialized straight
            # into the archive-stage summary/API payload.
            dropped = merge_counts.pop("dropped_photo_ids", None) or []
            # ``preserved_edit_count`` is likewise a caller-facing signal (the
            # NAS transfer's residual check adds it to "still need a sync"), not
            # a user-facing display number, so lift it off ``merge_counts`` too.
            # ``preserved_off_staging_identities`` is the subset the caller adds
            # to residual -- see ``merge_staged_tree_into_archive`` for why the
            # full count would double-report the phantom/intra-staged remaps.
            # Reported as identities (not a raw rowcount) so the caller can
            # filter out rows the pre-transfer drain already classified as
            # undeliverable and rows in sibling workspaces this sync would not
            # have written anyway.
            preserved_edits = merge_counts.pop("preserved_edit_count", 0) or 0
            preserved_off_staging_identities = merge_counts.pop(
                "preserved_off_staging_identities", None) or []
            result["merge"] = merge_counts
            result["merged_into_existing"] = self.target.merge_into_tracked
            # On the merge path ``total_photos`` counts every staged source photo,
            # including identical ones that were dropped as ``already_present``.
            # Report ``moved`` as the photos actually added to the archive.
            result["moved"] = merge_counts["new_photos"]
            if dropped:
                result["dropped_photo_ids"] = dropped
            if preserved_edits:
                result["preserved_edit_count"] = preserved_edits
            if preserved_off_staging_identities:
                result["preserved_off_staging_identities"] = (
                    preserved_off_staging_identities)
        if self.cleanup_error is not None:
            result["cleanup_error"] = self.cleanup_error
        return result
