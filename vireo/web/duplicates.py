"""Duplicate-file resolution: apply, keep, bulk-resolve, trash losers, summaries.

The ``/api/duplicates/*`` routes back the Duplicates page and the navbar
cleanup banner: applying resolver decisions to hash groups, keeping a chosen
copy instead of the current one, force-resolving groups by folder, moving
loser files to the OS Trash (and dropping their catalog rows), restoring the
last completed scan, and summarizing how much loser data may still be on
disk. The scan itself is a job launcher in
``web/jobs.py``.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

from duplicate_scan import attach_workspace_names, catalog_cleanup_result, revalidate_scan_result
from flask import Blueprint, jsonify, request
from photo_payload import attach_nested_edit_recipes
from services import catalog_deletion, duplicate_resolution
from services.same_shot_reviews import record_same_shot_review
from sql_chunks import chunked

if TYPE_CHECKING:
    from db import Database
    from flask import Response
    from flask.typing import ResponseReturnValue
    from services.service_types import ErrorResponse, JsonDict, JsonError
    from web.web_types import AppTrashPaths, CleanupCachedFiles, GetDb, InvalidateMissingOriginals, NetworkRoots

log = logging.getLogger(__name__)

# How long "keep this copy instead" waits for a loser cleanup to finish.
KEEP_CLEANUP_WAIT_SECS = 5.0


def create_duplicates_blueprint(
    get_db: GetDb,
    json_error: JsonError,
    *,
    trash_paths: AppTrashPaths,
    network_volume_roots: Callable[[], NetworkRoots],
    path_on_network_volume: Callable[[str, NetworkRoots], bool],
    cleanup_cached_files_for_deleted_photos: CleanupCachedFiles,
    invalidate_missing_originals: InvalidateMissingOriginals,
) -> Blueprint:
    """Build the duplicates blueprint.

    ``trash_paths``, ``network_volume_roots`` and ``path_on_network_volume``
    are app.py's mount-aware Trash helpers, late-bound through that module so
    tests that patch them there still reach these routes.
    ``cleanup_cached_files_for_deleted_photos`` (the app's ``PhotoDeletion``)
    unlinks the thumbnails, previews and working copies of deleted photos,
    and ``invalidate_missing_originals`` (the app's ``MissingOriginals``)
    drops the missing-originals cache after the catalog changes.
    """
    blueprint = Blueprint("duplicates", __name__)
    # Held by "keep this copy instead" and by loser cleanup from classification
    # through the row delete, so a copy cleanup already chose to trash can't
    # be promoted to the kept one before its file and row are gone.
    loser_cleanup_lock = threading.Lock()

    @blueprint.route("/api/duplicates/same-shot/review", methods=["POST"])
    def api_same_shot_review() -> ResponseReturnValue:
        """Record or reopen a library-wide review note; never modify photos."""
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("photo_ids, review_key and decision required")
        ids, key, decision = body.get("photo_ids"), body.get("review_key"), body.get("decision")
        if (not isinstance(ids, list) or not 2 <= len(ids) <= 10000
                or any(type(pid) is not int or not 0 < pid < 2**63 for pid in ids) or len(set(ids)) != len(ids)):
            return json_error("photo_ids must contain 2 to 10000 distinct positive integers")
        if not isinstance(key, str) or len(key) != 64:
            return json_error("review_key required")
        if "decision" not in body or decision not in (None, "reviewed", "keep_all"):
            return json_error("decision must be reviewed, keep_all or null")
        group = record_same_shot_review(get_db(), ids, key, decision)
        if group is None:
            return json_error("same-shot group changed", 409, message="This group changed. Reload or scan again before reviewing it.")
        return jsonify({"ok": True, "group": group})

    @blueprint.route("/api/duplicates/apply", methods=["POST"])
    def api_duplicates_apply() -> ResponseReturnValue:
        """Apply resolver decisions for the given list of file hashes.

        Body: {"hashes": ["<hash>", ...]}. For each hash we look up every
        non-rejected photo sharing it and hand that set to
        apply_duplicate_resolution, which picks a winner via the pure
        resolver and flags the losers as rejected. Returns the total number
        of photos rejected across all hashes, plus ``deferred_hashes``:
        the subset the resolver refused to touch because at least one
        candidate was offline or still awaiting its sidecar import. The UI keeps those groups on
        the page (state unknown, not resolved).
        """
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("hashes required")
        hashes = body.get("hashes")
        if not isinstance(hashes, list) or not hashes:
            return json_error("hashes required")

        for h in hashes:
            if not isinstance(h, str) or not h:
                return json_error("hashes must be a list of non-empty strings")

        db = get_db()
        total_rejected = 0
        deferred_hashes = []
        for h in hashes:
            ids = db.duplicates.live_ids_for_hash(h)
            if len(ids) < 2:
                continue
            result = duplicate_resolution.apply_duplicate_resolution(db, ids)
            total_rejected += result.get("rejected", 0)
            if result.get("deferred"):
                deferred_hashes.append(h)
        return jsonify({
            "rejected_count": total_rejected,
            "deferred_hashes": deferred_hashes,
        })

    # Why a copy can't become the kept one, as the Duplicates page says it.
    keep_refusals = {
        "not found": (404, "That photo is no longer in the catalog."),
        "not a duplicate": (409, "That photo no longer has a duplicate copy."),
        "sidecar keywords pending": (
            409, "This group's sidecar keywords have not been imported yet. Retry after the scan recovers.",
        ),
        "missing on disk": (
            409, "This copy's file is missing on disk, so it can't be the one you keep.",
        ),
        "volume offline": (
            409, "This copy is on a volume that isn't connected. Reconnect it first.",
        ),
    }

    @blueprint.route("/api/duplicates/keep", methods=["POST"])
    def api_duplicates_keep() -> ResponseReturnValue:
        """Keep this copy of a duplicate group instead of the current one.

        Body: ``{"photo_id": int}``. The copy is un-rejected if needed and
        every other kept copy of the same file is rejected, with rating and
        keywords merged onto it (``duplicate_resolution.keep_duplicate_copy``). Returns
        ``{"ok": True, "file_hash", "winner_id", "loser_ids", "rating",
        "flag"}``; a copy missing on disk or on an offline volume is refused
        with 409 so it can never displace the copy that exists.
        """
        body = request.get_json(silent=True)
        photo_id = body.get("photo_id") if isinstance(body, dict) else None
        if not isinstance(photo_id, int) or isinstance(photo_id, bool):
            return json_error("photo_id required")
        # A cleanup batch normally finishes within this wait; a slow network
        # Trash call can outlast it, and the user retries once it's done.
        if not loser_cleanup_lock.acquire(timeout=KEEP_CLEANUP_WAIT_SECS):
            return json_error(
                "cleanup in progress", 409,
                message="Files are being moved to Trash. Try again when that finishes.",
            )
        try:
            result = duplicate_resolution.keep_duplicate_copy(get_db(), photo_id)
        finally:
            loser_cleanup_lock.release()
        reason = result.get("error")
        if reason is not None:
            status, message = keep_refusals[reason]
            return json_error(reason, status, message=message)
        return jsonify(dict(result, ok=True))

    @blueprint.route("/api/duplicates/bulk-resolve", methods=["POST"])
    def api_duplicates_bulk_resolve() -> ResponseReturnValue:
        """Force-resolve a batch of duplicate groups by keeping the photo
        whose folder matches ``keep_folder``.

        Body: ``{"file_hashes": [str, ...], "keep_folder": str}``. For each
        hash, the photo in ``keep_folder`` becomes the kept winner; every
        other non-rejected photo sharing the hash becomes rejected, with
        rating/keywords merged onto the winner.

        Returns ``{"ok": True, "resolved_count": int, "resolved":
        [{"file_hash", "winner_id", "loser_ids"}], "skipped":
        [{"file_hash", "reason"}]}``. ``loser_ids`` is surfaced so the UI
        can chain into ``/api/duplicates/delete-loser-files`` when the
        user opted in to immediate trash.
        """
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("file_hashes and keep_folder required")
        file_hashes = body.get("file_hashes")
        keep_folder = body.get("keep_folder")
        if not isinstance(file_hashes, list) or not file_hashes:
            return json_error("file_hashes required")
        if not isinstance(keep_folder, str) or not keep_folder:
            return json_error("keep_folder required")
        for h in file_hashes:
            if not isinstance(h, str) or not h:
                return json_error("file_hashes must be a list of non-empty strings")

        db = get_db()
        result = duplicate_resolution.bulk_resolve_by_folder(db, file_hashes, keep_folder)
        return jsonify({
            "ok": True,
            "resolved_count": len(result["resolved"]),
            "resolved": result["resolved"],
            "skipped": result["skipped"],
        })

    def _loser_photo_ids_from_request() -> tuple[list[int], ErrorResponse | None]:
        """The validated ``photo_ids`` list, or ``([], error_response)``."""
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return [], json_error("photo_ids required")
        photo_ids = body.get("photo_ids")
        if not isinstance(photo_ids, list) or not photo_ids:
            return [], json_error("photo_ids required")
        for pid in photo_ids:
            # ``bool`` is a subclass of ``int`` in Python, so a bare
            # ``isinstance(pid, int)`` would accept ``True``/``False`` as
            # valid ids — and ``True`` would then be treated as photo id 1.
            # Reject booleans explicitly so ``{"photo_ids": [true]}`` can't
            # trick the endpoint into trashing whichever rejected row
            # happens to have id 1.
            if isinstance(pid, bool) or not isinstance(pid, int):
                return [], json_error("photo_ids must be a list of integers")
        return photo_ids, None

    def _classify_loser_candidates(
        db: Database, photo_ids: list[int],
    ) -> tuple[list[tuple[int, str]], list[int], list[JsonDict], NetworkRoots]:
        """Split the request into trashable losers and skipped ids.

        Returns ``(trash_candidates, trashed_pids, skipped, network_roots)``:
        ``(pid, filepath)`` pairs to send to Trash, ids whose local file is
        already gone (their rows still drop), skip entries, and the mount
        table snapshot the Trash call must reuse.
        """
        # The lookup is chunked — bulk cleanup actions may hand us thousands
        # of ids at once, and SQLite builds with the legacy 999-parameter cap
        # would otherwise fail before any cleanup runs.
        rows_by_id = db.duplicates.loser_candidate_rows(photo_ids)

        # One query per distinct hash to find kept-row anchors. Cheap because
        # the hash column is indexed and a typical bulk action shares hashes
        # across many photo_ids only when the user clicks "trash all losers"
        # for one library — still a small number of distinct hashes.
        hashes = {r["file_hash"] for r in rows_by_id.values() if r["file_hash"]}
        anchors_by_hash: dict[str, list[str]] = {}
        for h in hashes:
            anchors = db.duplicates.live_paths_for_hash(h)
            anchors_by_hash[h] = [os.path.join(a["path"], a["filename"]) for a in anchors]

        trash_candidates: list[tuple[int, str]] = []
        trashed_pids: list[int] = []
        skipped: list[JsonDict] = []
        # Classify paths via the bounded kernel mount table BEFORE any
        # per-path stat. Calling ``os.path.isfile`` on an unhealthy SMB path
        # can block indefinitely inside this endpoint, meaning the new
        # network routing inside ``trash_paths`` would never even be
        # reached; a stat failure would also be misread as "already
        # missing" and drop the DB row for a photo that reappears on
        # remount. Local paths keep the existing preflight — a fast, safe
        # stat on the local FS — so the "file already missing" reporting
        # contract for manually-cleaned local losers is preserved.
        network_roots = network_volume_roots()
        for pid in photo_ids:
            row = rows_by_id.get(pid)
            if row is None:
                skipped.append({"id": pid, "reason": "photo not found"})
                continue
            if row["flag"] != "rejected":
                skipped.append({"id": pid, "reason": "photo is not rejected"})
                continue
            if not row["file_hash"] or not anchors_by_hash.get(row["file_hash"]):
                # No kept row shares this hash — refuse to trash. Treat as
                # "not a duplicate loser" so the user can't accidentally use
                # this endpoint to delete files for unrelated rejected rows.
                skipped.append({"id": pid, "reason": "no duplicate winner exists"})
                continue
            filepath = os.path.join(row["folder_path"] or "", row["filename"] or "")
            network = path_on_network_volume(filepath, network_roots)
            if not network:
                from audit import confirmed_orphan_ids

                if confirmed_orphan_ids(db, [pid]):
                    skipped.append({"id": pid, "reason": "file already missing"})
                    trashed_pids.append(pid)
                    continue
            from file_identity import distinct_existing_file

            anchors = anchors_by_hash[row["file_hash"]]
            # Reject aliases as well as unavailable winners: every retained
            # row must continue to name a distinct, existing regular file.
            # Missing network sources still need Finder's bounded check and
            # mounted-volume revalidation before their catalog rows can go.
            if not all(distinct_existing_file(
                filepath, anchor,
                timeout=2.0 if network or path_on_network_volume(anchor, network_roots) else None,
                allow_missing_source=network,
            ) for anchor in anchors):
                skipped.append({"id": pid, "reason": "no verified distinct duplicate winner exists"})
                continue
            trash_candidates.append((pid, filepath))
        return trash_candidates, trashed_pids, skipped, network_roots

    def _record_trash_outcomes(
        trash_candidates: list[tuple[int, str]], successful_paths: set[str], already_missing_paths: set[str],
        trash_failures: list[JsonDict], trashed_pids: list[int], skipped: list[JsonDict], failed: list[JsonDict],
    ) -> None:
        """Sort each Trash candidate into ``trashed_pids`` or ``failed``."""
        failure_by_path = {
            failure["path"]: failure for failure in trash_failures
        }
        for pid, filepath in trash_candidates:
            if filepath in successful_paths:
                if filepath in already_missing_paths:
                    skipped.append({
                        "id": pid, "reason": "file already missing",
                    })
                trashed_pids.append(pid)
                continue
            failure = failure_by_path.get(filepath) or {}
            failed.append({
                "id": pid,
                "path": filepath,
                "error": failure.get("error", "Trash operation failed"),
            })

    def _drop_trashed_loser_rows(db: Database, trashed_pids: list[int]) -> None:
        """Delete the trashed losers' rows and cached derivatives, chunked."""
        all_files = []
        deleted_rows = 0
        for chunk in chunked(trashed_pids):
            result = catalog_deletion.delete_photos(db, chunk)
            all_files.extend(result.get("files", []))
            deleted_rows += result.get("deleted", 0)
        cleanup_cached_files_for_deleted_photos(all_files)
        if deleted_rows:
            invalidate_missing_originals()

    @blueprint.route("/api/duplicates/delete-loser-files", methods=["POST"])
    def api_duplicates_delete_loser_files() -> ResponseReturnValue:
        """Move duplicate loser files to OS Trash and remove their DB rows.

        Body: {"photo_ids": [int, ...]}. For each id we require:
          - the row's flag is 'rejected' (already auto-resolved or
            user-applied), AND
          - at least one OTHER photo with the same ``file_hash`` is NOT
            rejected (the kept "winner" anchor that makes this row a
            duplicate-loser rather than an unrelated rejection).

        Validating both conditions prevents this endpoint from being misused
        to trash files for arbitrary rejected photos (e.g. a photo the user
        manually rejected for non-duplicate reasons).

        After a successful trash we also delete the loser's photo row (and
        its cached thumbnail / preview / working-copy files). Without that,
        ``/api/duplicates/disk-cleanup-summary`` would keep reporting the
        same count forever — the summary predicate can't cheaply tell that
        the on-disk file has been removed (stat'ing every loser path on a
        slow network volume would make the banner poll expensive). Deleting
        the row makes the count correct without a stat. The keywords/rating
        were merged onto the winner during ``apply_duplicate_resolution``,
        so nothing of value is lost. If the user later restores the file
        from Trash and re-scans, the auto-resolve hook re-creates the row
        and the cycle is idempotent.

        Returns ``{trashed: N, skipped: [{id, reason}, ...],
        failed: [{id, path, error}, ...]}``.
        """
        photo_ids, err = _loser_photo_ids_from_request()
        if err is not None:
            return err

        with loser_cleanup_lock:
            return _trash_loser_files(get_db(), photo_ids)

    def _trash_loser_files(db: Database, photo_ids: list[int]) -> ErrorResponse | Response:
        """Body of ``delete-loser-files``, run under ``loser_cleanup_lock``."""
        trash_candidates, trashed_pids, skipped, network_roots = (
            _classify_loser_candidates(db, photo_ids)
        )
        failed: list[JsonDict] = []

        # Track paths whose end state already held (local preflight found
        # them absent, or Finder reported them missing on a still-mounted
        # network volume). Without this the network path would silently
        # succeed with ``trashed: 0`` and empty ``skipped``/``failed``,
        # leaving the UI stuck on "Moving to Trash..." — the local branch
        # already surfaces "file already missing" and the network branch
        # must match so cleanup is a terminal state either way.
        already_missing_paths: set[str] = set()
        # Pass through our mount-table snapshot — including an explicit
        # ``None`` when our own query failed — so ``trash_paths`` does
        # not re-query and overwrite our fail-closed classification. If
        # its second query happened to succeed after the share detached,
        # a custom-mount path we already flagged network (via the
        # ``/Volumes`` fallback in ``path_on_network_volume``) would be
        # silently reclassified as local, reintroducing the unbounded-I/O
        # hang this routing exists to prevent.
        trashed, successful_paths, trash_failures = trash_paths(
            [filepath for _pid, filepath in trash_candidates],
            already_missing_out=already_missing_paths,
            network_roots=network_roots,
        )
        _record_trash_outcomes(
            trash_candidates, successful_paths, already_missing_paths,
            trash_failures, trashed_pids, skipped, failed,
        )

        # Drop DB rows + cached derivatives for every photo whose file is now
        # gone. Chunked so ``delete_photos``' five internal IN-clause queries
        # can't trip the SQLite parameter cap on large bulk actions; without
        # chunking, a 1000+ id request would raise OperationalError on legacy
        # builds AFTER files were already trashed, leaving the DB inconsistent.
        if trashed_pids:
            try:
                _drop_trashed_loser_rows(db, trashed_pids)
            except Exception:
                # broad: files are already in Trash; if the row delete fails we
                # surface a 500 so the caller knows reconciliation is
                # incomplete. Without raising, the summary count would stay
                # inflated and the caller would have no signal that the
                # cleanup is half-done.
                log.exception(
                    "DB row delete failed after trashing %d files", len(trashed_pids),
                )
                return jsonify({
                    "ok": False,
                    "error": "trashed files but failed to clean up DB rows",
                    "trashed": trashed,
                    "skipped": skipped,
                    "failed": failed,
                }), 500

        return jsonify({
            "ok": True,
            "trashed": trashed,
            "skipped": skipped,
            "failed": failed,
        })

    @blueprint.route("/api/duplicates/cleanup", methods=["GET"])
    def api_duplicates_cleanup() -> ResponseReturnValue:
        """Current catalog cleanup candidates, independent of saved scans."""
        return jsonify(catalog_cleanup_result(get_db()))

    @blueprint.route("/api/duplicates/last-scan", methods=["GET"])
    def api_duplicates_last_scan() -> ResponseReturnValue:
        """Return the most recent completed duplicate-scan's result.

        The /duplicates page only holds proposals in JS memory, so
        navigating away and back used to require a full rescan. This
        lets the page restore prior results from ``job_history`` instead.

        Library-wide on purpose: duplicate detection itself ignores
        workspace scope (photos are global), so the result of any
        completed scan is valid for any active workspace — even though
        the row carries the triggering workspace's id.

        Groups whose photos no longer match the catalog (an extra copy
        deleted since, its id reused by a later import) are dropped before
        serving; ``result.stale_group_count`` says how many.

        Response: ``{found: false}`` or
        ``{found: true, job_id, started_at, finished_at, result}``.
        """
        db = get_db()
        row = db.job_history.last_completed_with_result("duplicate-scan")
        if row is None:
            return jsonify({"found": False})
        try:
            result = json.loads(row["result"])
        except (json.JSONDecodeError, TypeError):
            return jsonify({"found": False})
        if not isinstance(result, dict):
            return jsonify({"found": False})
        result = revalidate_scan_result(db, result)
        attach_nested_edit_recipes(db, result)
        attach_workspace_names(db, result["proposals"])
        return jsonify({
            "found": True,
            "job_id": row["id"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "result": result,
        })

    @blueprint.route("/api/duplicates/disk-cleanup-summary", methods=["GET"])
    def api_duplicates_disk_cleanup_summary() -> ResponseReturnValue:
        """Return counts of duplicate-loser files that may still be on disk.

        Body: ``{count: int, total_size: int, file_hashes: [str, ...]}``.

        Powers the navbar banner that surfaces the volume of cleanup
        available — without it, auto-resolved duplicates from scan are
        invisible to the user.

        ``count`` is the number of rejected photo rows whose hash is also
        held by a non-rejected row (i.e. duplicate losers, not unrelated
        rejections). ``total_size`` is the sum of their stored ``file_size``
        — a best-effort estimate; we do NOT stat each path here because
        slow network volumes (e.g. SMB) would make this endpoint expensive
        on every banner poll. The bulk-trash endpoint validates each file
        exists before trashing.
        """
        db = get_db()
        row = db.duplicates.loser_disk_summary()
        return jsonify({
            "count": row["n"],
            "total_size": row["total_bytes"],
        })

    return blueprint
