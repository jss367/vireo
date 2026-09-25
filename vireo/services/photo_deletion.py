"""Batch photo deletion shared by the sync, batch and job delete endpoints.

``PhotoDeletion.run_batch_delete`` is the one implementation behind
``/api/batch/delete``, the batch-delete job and the job launchers: it moves
files to the Trash (or deletes them permanently), removes catalog rows only
once a photo's files reached the requested end state, prunes the pipeline
cache and sweeps cached thumbnails, previews and working copies.
``PhotoDeletion.cleanup_cached_files_for_deleted_photos`` is that cache sweep
on its own, also used by the audit, duplicates and folders routes after they
drop rows.

Nothing here touches ``request`` or builds an HTTP response. ``create_app``
builds one ``PhotoDeletion``; the filesystem primitives it depends on
(``_chunked``, ``_trash_paths``, ``_snapshot_parent_device``,
``_path_confirmed_gone``) still live in ``app.py`` and are injected as
late-binding callables, so tests that monkeypatch them on the ``app`` module
keep reaching this code.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def reraise_fatal_cleanup_error(exc):
    """Re-raise interrupts that best-effort cleanup must never swallow."""
    if isinstance(exc, (KeyboardInterrupt, GeneratorExit)):
        raise exc


class PhotoDeletion:
    """Photo deletion bound to one app's config and filesystem helpers.

    ``config`` is the app's config mapping; ``THUMB_CACHE_DIR`` is read on
    every call, as it was when this lived in ``create_app``.
    """

    def __init__(
        self, config, *, chunked, trash_paths, snapshot_parent_device,
        path_confirmed_gone,
    ):
        self._config = config
        self._chunked = chunked
        self._trash_paths = trash_paths
        self._snapshot_parent_device = snapshot_parent_device
        self._path_confirmed_gone = path_confirmed_gone

    def cleanup_cached_files_for_deleted_photos(self, files, progress_callback=None):
        try:
            # Imported lazily: a broken ``preview_cache`` import (e.g. a moved
            # packaged executable) must be logged, not fail the delete.
            from preview_cache import cleanup_cached_files_for_deleted_photos
            cleanup_cached_files_for_deleted_photos(
                self._config["THUMB_CACHE_DIR"], files,
                progress_callback=progress_callback,
            )
        except BaseException as exc:
            reraise_fatal_cleanup_error(exc)
            log.exception("Failed to clean cached files after delete")

    def run_batch_delete(
        self, db, photo_ids, mode="vireo", include_companions=False,
        progress_callback=None,
    ):
        """Delete photos using the same phases for sync and job endpoints.

        Only photos visible in ``db``'s active workspace are acted on: photos
        are global, so an id from another workspace would otherwise let one
        workspace delete (or trash) another's originals. Ids that are not
        visible are dropped before anything is resolved.

        Disk modes keep a photo's catalog row until its files reach the
        requested end state, so a failed Trash is retried by photo id. There
        is deliberately no retry by raw path: the catalog row is what vouches
        for a file, and a client-supplied path has nothing to vouch for it.
        """

        def emit(
            phase, current=0, total=0, current_file="", detail="", failed=0,
            stage_failures=None,
        ):
            if progress_callback:
                payload = {
                    "phase": phase,
                    "current": current,
                    "total": total,
                    "current_file": current_file,
                    "detail": detail,
                    "failed": failed,
                }
                # ``stage_failures`` lets a single emit attribute failure counts
                # to specific stages so the frontend does not have to rely on
                # a specific per-stage emit having arrived first. The disk and
                # catalog phases each report their own count while Finishing
                # sends the merged map, so a dropped intermediate event cannot
                # silently promote a partial stage to green complete.
                if stage_failures:
                    payload["stage_failures"] = dict(stage_failures)
                progress_callback(payload)

        if not photo_ids:
            raise ValueError("photo_ids required")
        if not isinstance(photo_ids, (list, tuple)):
            raise ValueError("photo_ids must be a list")
        if mode not in ("vireo", "disk", "disk_permanent"):
            raise ValueError("mode must be 'vireo', 'disk', or 'disk_permanent'")
        requested_ids = []
        for raw_id in photo_ids:
            # ``int(raw_id)`` would silently truncate a float like ``1.9``
            # to ``1``: the workspace filter would then accept photo 1 and
            # the mode could delete it and its original even though the
            # caller never asked for it. Require an actual int (matching
            # ``parse_selection_photo_ids``).
            if isinstance(raw_id, bool) or not isinstance(raw_id, int):
                raise ValueError("photo_ids must be integers")
            requested_ids.append(raw_id)
        photo_ids = db.filter_photo_ids_in_workspace(requested_ids)
        if not photo_ids:
            emit("Finishing", 1, 1)
            return {
                "ok": True,
                "deleted": 0,
                "trashed": 0,
                "trash_failed": [],
                "failed_photo_ids": [],
            }

        def remove_catalog_rows(ids, *, expand_companions, revalidate_identity=None):
            """Atomically remove resolved catalog rows, then clean caches.

            ``revalidate_identity`` is an optional ``{photo_id: (folder_id,
            filename, folder_path)}`` map captured at the start of the disk
            operation. When provided, each row is re-checked inside the delete
            transaction and skipped if any of those three fields has changed
            since — a concurrent ``/api/jobs/move-photos`` can commit a new
            ``folder_id`` while the disk-delete's stale-path
            ``os.path.isfile`` check is already reporting "gone", and a
            concurrent ``/api/jobs/move-folder`` can leave ``folder_id`` and
            ``filename`` unchanged while renaming the underlying
            ``folders.path``. Deleting by id alone in either case would
            discard the row for a photo that now lives at a completely
            different path. Skipped ids are returned as ``skipped_ids`` so
            the caller can surface them alongside filesystem failures.
            """
            result = {
                "deleted": 0, "ids": [], "files": [], "skipped_ids": [],
            }
            skipped_ids = []
            total = len(ids)
            prepared = 0
            emit(
                "Removing from Vireo", 0, total,
                detail="Preparing database changes; nothing is committed yet.",
            )
            revalidating = bool(revalidate_identity)
            # BEGIN IMMEDIATE takes the write lock up front so no other writer
            # (notably move-photos or move-folder) can commit an identity
            # change between the revalidation SELECT and the DELETE that
            # follows. Without it, a move committed after the SELECT but
            # before the DELETE would let us delete a row that no longer
            # matches the identity we verified.
            if revalidating:
                db.conn.execute("BEGIN IMMEDIATE")
            try:
                if revalidating:
                    verified_ids = []
                    for chunk in self._chunked(ids):
                        placeholders = ",".join("?" for _ in chunk)
                        current = {
                            row["id"]: (
                                row["folder_id"],
                                row["filename"],
                                row["folder_path"],
                                row["companion_path"],
                            )
                            for row in db.conn.execute(
                                f"SELECT p.id, p.folder_id, p.filename, "
                                f"p.companion_path, f.path AS folder_path "
                                f"FROM photos p "
                                f"JOIN folders f ON f.id = p.folder_id "
                                f"WHERE p.id IN ({placeholders})",
                                list(chunk),
                            )
                        }
                        for photo_id in chunk:
                            expected = revalidate_identity.get(photo_id)
                            actual = current.get(photo_id)
                            if actual is None:
                                # Row already gone — a concurrent delete beat
                                # us to it. The requested end state already
                                # holds, so treat it as successfully deleted
                                # rather than a stale identity we couldn't
                                # verify. Reporting it in ``failed_photo_ids``
                                # here would leave the client showing a photo
                                # that is absent from both catalog and disk
                                # until reload.
                                continue
                            if expected is not None and actual == expected:
                                verified_ids.append(photo_id)
                            else:
                                skipped_ids.append(photo_id)
                    ids_to_delete = verified_ids
                else:
                    ids_to_delete = list(ids)

                for chunk in self._chunked(ids_to_delete):
                    chunk_result = db.delete_photos(
                        chunk,
                        include_companions=expand_companions,
                        commit=False,
                    )
                    result["deleted"] += chunk_result["deleted"]
                    result["ids"].extend(chunk_result["ids"])
                    result["files"].extend(chunk_result["files"])
                    prepared += len(chunk)
                    emit(
                        "Removing from Vireo", min(prepared, total), total,
                        detail=(
                            "Preparing database changes; nothing is committed yet."
                        ),
                    )
                db.conn.commit()
            except Exception:
                db.conn.rollback()
                raise
            result["skipped_ids"] = skipped_ids

            emit(
                "Removed from Vireo", result["deleted"], result["deleted"],
                detail="Database changes committed.",
            )
            emit("Pruning pipeline cache", 0, 1)
            try:
                db.prune_pipeline_cache_for_ids(result["ids"])
            except BaseException as exc:
                reraise_fatal_cleanup_error(exc)
                log.exception("Failed to prune pipeline cache after delete")
            emit("Pruning pipeline cache", 1, 1)

            def cache_progress(current, total_files, filename):
                emit("Cleaning cached files", current, total_files, filename)

            emit("Cleaning cached files", 0, len(result["files"]))
            self.cleanup_cached_files_for_deleted_photos(
                result["files"], progress_callback=cache_progress,
            )
            return result

        # Database-only mode has no filesystem prerequisite and retains the
        # original all-or-nothing catalog transaction.
        if mode == "vireo":
            result = remove_catalog_rows(
                photo_ids, expand_companions=include_companions,
            )
            emit("Finishing", 1, 1)
            return {
                "ok": True,
                "deleted": result["deleted"],
                "trashed": 0,
                "trash_failed": [],
                "failed_photo_ids": [],
            }

        # Disk modes resolve paths without changing SQLite. A photo's catalog
        # row is removed only after its primary file reached the requested end
        # state. A companion without its own photo row is processed first; if
        # that fails, its primary is left untouched and its row remains
        # retryable.
        resolved = db.resolve_photos_for_delete(
            photo_ids, include_companions=include_companions,
        )
        files = resolved["files"]
        primary_paths = {
            f["photo_id"]: os.path.join(f["folder_path"], f["filename"])
            for f in files
        }
        # Snapshot each row's identity so the catalog-removal step can verify
        # the row still points at the same file it did when we resolved paths.
        # Without this, a concurrent /api/jobs/move-photos can commit a new
        # folder_id and remove the source file mid-run; our stale-path
        # os.path.isfile would then read "already gone" and we would delete
        # a row that now represents the moved file at a different location.
        # The folder_path is included so a concurrent /api/jobs/move-folder,
        # which keeps folder_id and filename unchanged while renaming
        # ``folders.path``, is also caught — otherwise the row would still be
        # deleted even though the copied file remains at the new path.
        # ``companion_path`` is included so a concurrent scan that pairs a
        # RAW with a JPEG mid-delete (scanner.py: ``UPDATE photos SET
        # companion_path`` on the primary, ``DELETE FROM photos`` on the
        # merged companion) is also caught — otherwise the primary's tuple
        # would still match, and we'd trash only the pre-pair paths and
        # remove the primary row, orphaning the newly-paired companion file
        # on disk with no catalog entry.
        resolved_identity = {
            f["photo_id"]: (
                f["folder_id"], f["filename"], f["folder_path"],
                f["companion_path"],
            )
            for f in files
        }
        catalog_primary_paths = set(primary_paths.values())
        extra_companions = {}
        if include_companions:
            for f in files:
                companion_path = f.get("companion_path")
                if not companion_path:
                    continue
                # ``companion_path`` is stored as either a relative filename
                # inside the same folder or an absolute path (see the same
                # resolution in new_images.py:35-40). Joining an absolute
                # path with the folder path would silently target the wrong
                # file — the disk op would either fail or, worse, trash the
                # wrong photo without ever pruning the correct catalog row.
                companion = (
                    companion_path if os.path.isabs(companion_path)
                    else os.path.join(f["folder_path"], companion_path)
                )
                if companion not in catalog_primary_paths:
                    extra_companions.setdefault(f["photo_id"], set()).add(companion)

        disk_phase = (
            "Moving files to Trash"
            if mode == "disk" else "Deleting files permanently"
        )
        all_disk_paths = list(dict.fromkeys(
            [path for paths_for_id in extra_companions.values() for path in paths_for_id]
            + list(primary_paths.values())
        ))
        emit(disk_phase, 0, len(all_disk_paths))

        disk_paths_finished = 0

        def operate(paths_to_change):
            nonlocal disk_paths_finished
            if not paths_to_change:
                return 0, set(), []
            if mode == "disk":
                progress_offset = disk_paths_finished

                def trash_progress(current, _total, filename):
                    emit(
                        disk_phase, progress_offset + current,
                        len(all_disk_paths), filename,
                    )

                result = self._trash_paths(
                    paths_to_change, progress_callback=trash_progress,
                )
                disk_paths_finished += len(paths_to_change)
                return result
            successful = set()
            failures = []
            removed = 0
            # Snapshot each parent's st_dev before deletion so a mount that
            # vanishes mid-batch can be detected even when the mount point
            # remains visible on the underlying local FS (see
            # ``_snapshot_parent_device``).
            parent_devs = {
                path: self._snapshot_parent_device(path)
                for path in paths_to_change
            }
            for filepath in paths_to_change:
                if not os.path.isfile(filepath):
                    # Same live-parent gate as ``_trash_paths`` — a
                    # disconnected mount also makes ``os.path.isfile``
                    # return False, and treating that as "already gone"
                    # would prune the catalog row for a photo that
                    # reappears when the volume comes back.
                    if self._path_confirmed_gone(
                        filepath, parent_devs.get(filepath),
                    ):
                        log.warning("File already missing: %s", filepath)
                        successful.add(filepath)
                    else:
                        log.warning(
                            "Permanent delete preflight: source "
                            "unreachable for %s", filepath,
                        )
                        failures.append({
                            "path": filepath,
                            "error": "Source path is unreachable",
                        })
                    disk_paths_finished += 1
                    emit(
                        disk_phase, disk_paths_finished,
                        len(all_disk_paths), os.path.basename(filepath),
                    )
                    continue
                try:
                    os.remove(filepath)
                    successful.add(filepath)
                    removed += 1
                except OSError as exc:
                    log.warning(
                        "Permanent delete failed for %s", filepath,
                        exc_info=True,
                    )
                    failures.append({"path": filepath, "error": str(exc)})
                disk_paths_finished += 1
                emit(
                    disk_phase, disk_paths_finished,
                    len(all_disk_paths), os.path.basename(filepath),
                )
            return removed, successful, failures

        companion_paths = list(dict.fromkeys(
            path for paths_for_id in extra_companions.values()
            for path in paths_for_id
        ))
        trashed, companion_success, companion_failures = operate(companion_paths)
        eligible_ids = {
            photo_id for photo_id in resolved["ids"]
            if extra_companions.get(photo_id, set()) <= companion_success
        }
        eligible_primary_paths = [
            primary_paths[photo_id] for photo_id in resolved["ids"]
            if photo_id in eligible_ids and photo_id in primary_paths
        ]
        primary_moved, primary_success, primary_failures = operate(
            eligible_primary_paths,
        )
        trashed += primary_moved
        successful_ids = [
            photo_id for photo_id in resolved["ids"]
            if photo_id in eligible_ids
            and primary_paths.get(photo_id) in primary_success
        ]
        successful_id_set = set(successful_ids)
        failed_ids = [
            photo_id for photo_id in resolved["ids"]
            if photo_id not in successful_id_set
        ]

        failure_by_path = {
            failure["path"]: failure
            for failure in companion_failures + primary_failures
        }
        trash_failed = []
        for photo_id in failed_ids:
            failed_paths = [
                path for path in extra_companions.get(photo_id, set())
                if path not in companion_success
            ]
            primary = primary_paths.get(photo_id)
            if not failed_paths and primary not in primary_success:
                failed_paths.append(primary)
            for filepath in failed_paths:
                detail = dict(failure_by_path.get(filepath) or {
                    "path": filepath,
                    "error": "A companion file could not be removed",
                })
                detail["photo_id"] = photo_id
                trash_failed.append(detail)

        disk_failed_photos = len(failed_ids)
        emit(
            disk_phase, len(all_disk_paths), len(all_disk_paths),
            detail=(
                f"{len(successful_ids)} photo(s) ready for catalog removal; "
                f"{disk_failed_photos} retained after filesystem errors."
            ),
            failed=disk_failed_photos,
            stage_failures={"files": disk_failed_photos},
        )
        result = remove_catalog_rows(
            successful_ids,
            expand_companions=False,
            revalidate_identity=resolved_identity,
        )
        # Rows whose identity changed between resolve and catalog-removal
        # weren't deleted — surface them alongside filesystem failures so
        # the client keeps them visible and doesn't report them as trashed.
        skipped_ids = result.get("skipped_ids", []) or []
        catalog_failed_photos = len(skipped_ids)
        if skipped_ids:
            already_failed = set(failed_ids)
            for photo_id in skipped_ids:
                trash_failed.append({
                    "photo_id": photo_id,
                    "path": primary_paths.get(photo_id, ""),
                    "error": (
                        "Photo was moved to a new folder during this delete; "
                        "the catalog row was preserved"
                    ),
                })
            failed_ids = failed_ids + [
                photo_id for photo_id in skipped_ids
                if photo_id not in already_failed
            ]
            # Re-emit the catalog stage with the retained count so the frontend
            # transitions it from complete → partial. Without this, the initial
            # "Removed from Vireo" event marks the stage green and Finishing
            # later carries a higher failed count that the frontend can't
            # attribute to any stage.
            emit(
                "Removed from Vireo",
                result["deleted"], result["deleted"] + catalog_failed_photos,
                detail=(
                    f"{catalog_failed_photos} photo(s) retained "
                    "due to concurrent moves."
                ),
                failed=catalog_failed_photos,
                stage_failures={"catalog": catalog_failed_photos},
            )
        emit(
            "Finishing", 1, 1,
            failed=len(failed_ids),
            stage_failures={
                "files": disk_failed_photos,
                "catalog": catalog_failed_photos,
            },
        )
        return {
            "ok": True,
            "deleted": result["deleted"],
            "trashed": trashed,
            "trash_failed": trash_failed,
            "failed_photo_ids": failed_ids,
        }
