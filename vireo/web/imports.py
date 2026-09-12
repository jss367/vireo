"""Import domain: previews, readiness, and the archive/in-place import jobs.

Step 2 of the ``create_app`` split. Everything here moved verbatim out of
``app.py``; the closure helpers the routes shared with other domains are
injected through the factory instead of captured from ``create_app``:

- ``invalidate_missing_originals`` / ``reject_visual_collection`` /
  ``metadata_repair_count`` / ``bulk_gps_location_payload`` still live in
  ``app.py`` because other domains call them too.
- ``enqueue_process_job`` and ``chain_after_move`` come from
  ``services.pipeline_launch.PipelineChain``, the job-thread side of the
  after-import chain; ``resolve_remote_archive_target`` is imported from
  the same service.

The request-parsing halves of ``api_job_import_photos`` and
``api_job_import_in_place`` are the next extraction target (a pure
``(body, db, cfg) -> params | error`` resolver under ``services/``).
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import path_guard
import source_discovery
from db import Database
from flask import Blueprint, Response, abort, jsonify, make_response, request
from keyword_normalization import keyword_match_key, normalize_keyword_display
from metadata import scan_metadata_warning
from new_images import invalidate_new_images_after_scan
from services.pipeline_launch import resolve_remote_archive_target
from web.background_jobs import make_background_job

log = logging.getLogger(__name__)


# strftime directives grouped by what they can render. Used by
# ``_strftime_template_can_render`` so the NAS-mount overlap guard does not
# treat every ``%``-bearing template component as matching whatever the mount
# has at that depth — ``%Y`` renders only 4 digits and can never equal a
# letter-only mount leaf like ``NAS``. Missing tokens fall back to ``.*`` so
# unknown/exotic directives keep the pre-fix conservative wildcard behavior.
_STRFTIME_TOKEN_RE = {
    "Y": r"\d{4}",
    "y": r"\d{2}",
    "C": r"\d{2}",
    "m": r"\d{1,2}",
    "d": r"\d{1,2}",
    "e": r"[ \d]{1,2}",
    "H": r"\d{1,2}",
    "k": r"[ \d]{1,2}",
    "I": r"\d{1,2}",
    "l": r"[ \d]{1,2}",
    "M": r"\d{2}",
    "S": r"\d{2}",
    "j": r"\d{3}",
    "U": r"\d{2}",
    "W": r"\d{2}",
    "V": r"\d{2}",
    "G": r"\d{4}",
    "g": r"\d{2}",
    "u": r"\d",
    "w": r"\d",
    "s": r"\d+",
    "f": r"\d{6}",
    "z": r"[+\-]\d{4}(?:\d{2})?",
    "%": r"%",
    "n": r"\s",
    "t": r"\s",
    # Locale-dependent renders are unknowable at request time — keep them
    # wildcard-matching so the guard stays at least as strict as the
    # pre-fix behavior for these directives.
    "A": r".+", "a": r".+",
    "B": r".+", "b": r".+", "h": r".+",
    "p": r".*", "P": r".*",
    "Z": r".*",
    "c": r".+", "x": r".+", "X": r".+",
    "D": r".+", "F": r".+", "T": r".+", "R": r".+", "r": r".+",
    "+": r".+",
}


def _strftime_template_can_render(template_component, target):
    """Return True if a strftime render of ``template_component`` could
    equal ``target`` (case-insensitively, to match case-alias filesystems).

    Compile the template component into a regex whose token character
    classes are the strftime directives' actual output shapes, then match
    ``target`` against it. A template component without ``%`` is a pure
    literal and only equals a case-folded copy of itself.

    Unknown or locale-varying directives fall back to ``.*``/``.+`` so the
    guard remains conservative — never LESS strict than the pre-fix
    wildcard behavior for those tokens.
    """
    if "%" not in template_component:
        return template_component.casefold() == target.casefold()
    parts = []
    i = 0
    n = len(template_component)
    while i < n:
        c = template_component[i]
        if c == "%" and i + 1 < n:
            j = i + 1
            # Skip glibc pad / case / E / O modifiers before the directive
            # letter: %_d, %-d, %0d, %^d, %#d, %Ed, %Od.
            while j < n and template_component[j] in "_-0^#EO":
                j += 1
            if j < n:
                parts.append(
                    _STRFTIME_TOKEN_RE.get(template_component[j], r".*"))
                i = j + 1
            else:
                # Trailing "%" with no directive — treat as literal.
                parts.append(re.escape(c))
                i += 1
        else:
            parts.append(re.escape(c))
            i += 1
    pattern = "".join(parts)
    try:
        return re.fullmatch(pattern, target, re.IGNORECASE) is not None
    except re.error:
        # Malformed pattern — be conservative and treat as renderable so
        # the guard errs on rejection rather than silently accepting.
        return True


# Adaptive flush cadence for the duplicate-check SSE stream. Byte-for-byte
# hashes on slow cards spend far more than this per file, so each check ends
# its own event and cancellation is bounded by one file. Metadata-only
# checks on a 100k-file source blow through hundreds of files inside one
# window and coalesce naturally, keeping event count in the low hundreds
# instead of one-per-file. Module-level so tests can override it.
DUPLICATE_CHECK_FLUSH_INTERVAL_SECONDS = 0.1

# Chunk size for the duplicate-check prep phase (EXIF batching and, in
# verify_by_hash + recovery mode, source_capture_timestamps for folder
# planning). Small enough that a superseded browser request cancels within
# one chunk's worth of I/O — otherwise a card with tens of thousands of
# files spends the entire prep phase reading metadata before the WSGI layer
# ever gets a chance to observe the disconnect. Aligned with
# metadata._BATCH_SIZE so an outer chunk maps to at most one ExifTool
# subprocess for its leftovers, keeping subprocess overhead unchanged.
# Module-level so tests can override it.
DUPLICATE_CHECK_PREP_BATCH_SIZE = 100


def create_imports_blueprint(
    get_db,
    json_error,
    get_runner,
    db_path,
    config,
    *,
    invalidate_missing_originals,
    reject_visual_collection,
    metadata_repair_count,
    enqueue_process_job,
    chain_after_move,
    bulk_gps_location_payload,
    guard_move_folder,
):
    """Build the imports blueprint.

    ``config`` is the Flask app's config mapping (read at request and job
    time for ``THUMB_CACHE_DIR`` and ``REQUIRE_EXIFTOOL_FOR_IMPORT``, so
    runtime overrides keep working). The keyword arguments are the
    ``create_app`` closures this domain still shares with others; see the
    module docstring.
    """
    blueprint = Blueprint("imports", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)
    archive_dispatch_lock = threading.Lock()

    @blueprint.get("/api/import/pending-archives")
    def api_pending_archives():
        from pending_archives import active_archive_jobs
        db = get_db()
        jobs = active_archive_jobs(get_runner(), db._ws_id())
        rows = db.conn.execute(
            "SELECT a.*, c.id AS review_collection_id, c.name AS collection_name FROM pending_archives a "
            "LEFT JOIN collections c ON c.id = a.collection_id AND c.workspace_id = a.workspace_id "
            "WHERE a.workspace_id = ? AND a.state != 'complete' ORDER BY a.created_at",
            (db._ws_id(),),
        ).fetchall()
        items = []
        for row in rows:
            sending = any(j.get("type") == "send-to-nas"
                          and (j.get("config") or {}).get("pending_archive_id") == row["id"] for j in jobs)
            items.append({
                "id": row["id"], "destination": row["destination"],
                "source_available": os.path.isdir(row["staging_destination"]),
                "collection_id": row["review_collection_id"], "name": row["collection_name"] or "Imported photos",
                "state": "sending" if sending else "waiting" if jobs else "ready",
                "error": row["error"] or (
                    "The previous transfer was interrupted. Local originals are retained; try sending again."
                    if row["state"] == "sending" and not sending else ""),
            })
        return jsonify({"items": items})

    @blueprint.post("/api/import/pending-archives/<archive_id>/discard")
    def api_discard_pending_archive(archive_id):
        from pending_archives import active_archive_jobs, get_pending_archive
        db = get_db()
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict) or body.get("confirmed") is not True:
            return json_error("Confirm removal of this missing NAS transfer record", 400)
        with archive_dispatch_lock:
            archive = get_pending_archive(db, archive_id)
            if archive is None:
                return json_error("Pending NAS transfer not found in this workspace", 404)
            if active_archive_jobs(get_runner(), db._ws_id()):
                return json_error("Wait for running jobs to finish before removing this transfer record", 409)
            if os.path.isdir(archive["staging_destination"]):
                return json_error("Local originals are available. Send them to NAS before removing this transfer", 409)
            # Forget only the transfer, never files or catalog entries. This is
            # explicit recovery for lost storage, including interrupted sends.
            db.conn.execute(
                "DELETE FROM pending_archives WHERE id = ? AND workspace_id = ?",
                (archive_id, db._ws_id()),
            )
            db.conn.commit()
        return jsonify({"ok": True})

    @blueprint.post("/api/import/pending-archives/<archive_id>/send")
    def api_send_pending_archive(archive_id):
        from pending_archives import active_archive_jobs, get_pending_archive, send_pending_archive
        db = get_db()
        runner = get_runner()
        workspace_id = db._ws_id()
        with archive_dispatch_lock:
            archive = get_pending_archive(db, archive_id)
            if archive is None:
                return json_error("Pending NAS transfer not found in this workspace", 404)
            if archive["state"] == "complete":
                return jsonify({"already_sent": True})
            active = active_archive_jobs(runner, workspace_id)
            existing = next((j for j in active if j.get("type") == "send-to-nas"
                             and (j.get("config") or {}).get("pending_archive_id") == archive_id), None)
            if existing:
                return jsonify({"job_id": existing["id"]})
            if active:
                return json_error("Wait for running jobs to finish before sending these photos to NAS", 409)

            def work(job):
                with Database(db_path) as thread_db:
                    thread_db.set_active_workspace(workspace_id)
                    thread_db.conn.execute(
                        "UPDATE pending_archives SET state = 'sending', error = '' WHERE id = ?",
                        (archive_id,),
                    )
                    thread_db.conn.commit()
                    try:
                        if not runner.begin_uncancellable(job["id"]):
                            raise ValueError("Transfer cancelled before it started. Local originals are retained.")

                        def progress(current, total, filename, phase="Sending to NAS"):
                            job["progress"].update(current=current, total=total, current_file=filename)
                            runner.push_event(job["id"], "progress", {
                                "current": current, "total": total, "current_file": filename, "phase": phase,
                            })

                        result = send_pending_archive(
                            thread_db, archive, vireo_dir=os.path.dirname(config["THUMB_CACHE_DIR"]),
                            guard_folder=guard_move_folder, progress_cb=progress,
                        )
                        thread_db.conn.execute(
                            "UPDATE pending_archives SET state = 'complete', error = '' WHERE id = ?", (archive_id,),
                        )
                        thread_db.conn.commit()
                        with contextlib.suppress(Exception):
                            invalidate_missing_originals()
                        return result
                    except Exception as e:
                        thread_db.conn.execute(
                            "UPDATE pending_archives SET state = 'pending', error = ? WHERE id = ?",
                            (str(e), archive_id),
                        )
                        thread_db.conn.commit()
                        raise

            job_id, _, _ = runner.start_singleton(
                "send-to-nas", work, singleton_key=archive_id,
                workspace_id=workspace_id,
                exclusive_workspace=True,
                config={"pending_archive_id": archive_id, "destination": archive["destination"]},
            )
        return jsonify({"job_id": job_id})
    # Import workers run on separate threads. Serialize execution of the same
    # frozen snapshot so the second worker observes the first worker's catalog
    # admissions and reports an idempotent replay instead of claiming them too.
    snapshot_import_locks = {}
    snapshot_import_locks_guard = threading.Lock()

    def _gps_location_chunks(values, size=800):
        values = list(values)
        for idx in range(0, len(values), size):
            yield values[idx:idx + size]

    def _validate_import_tag_options(body):
        """Normalize optional tags attached to a photo import request."""
        raw_tags = body.get("tags", [])
        if not isinstance(raw_tags, list):
            return None, None, json_error("tags must be a list of names")
        if len(raw_tags) > 50:
            return None, None, json_error("at most 50 import tags are allowed")

        tags = []
        seen = set()
        for raw in raw_tags:
            if not isinstance(raw, str):
                return None, None, json_error(
                    "tags must contain only strings"
                )
            name = normalize_keyword_display(raw)
            if not name:
                return None, None, json_error("import tags must not be empty")
            if len(name) > 200:
                return None, None, json_error(
                    "import tags must be 200 characters or fewer"
                )
            match_key = keyword_match_key(name) or name.casefold()
            if match_key not in seen:
                seen.add(match_key)
                tags.append(name)

        location_from_gps = body.get("location_from_gps", False)
        if not isinstance(location_from_gps, bool):
            return None, None, json_error(
                "location_from_gps must be a boolean"
            )
        return tags, location_from_gps, None

    def _queue_import_keyword_add(
        db, photo_id, keyword_name, workspace_id, *, commit=False,
    ):
        """Thread-safe equivalent of _queue_keyword_add for import jobs."""
        removed = db.remove_pending_changes(
            photo_id, "keyword_remove", keyword_name,
            workspace_id=workspace_id, _commit=commit,
        )
        if removed == 0:
            db.queue_change(
                photo_id, "keyword_add", keyword_name,
                workspace_id=workspace_id, _commit=commit,
            )

    def _queue_import_location_sync(
        db, photo_id, workspace_id, *, commit=False,
    ):
        """Thread-safe equivalent of _queue_location_sync_if_enabled."""
        db.remove_pending_changes(
            photo_id, "location", workspace_id=workspace_id, _commit=commit,
        )
        db.queue_change(
            photo_id, "location", "effective",
            workspace_id=workspace_id, _commit=commit,
        )

    def _apply_import_tags(
        workspace_id, photo_ids, tags, location_from_gps, result,
        *, job=None, runner=None,
    ):
        """Apply requested common tags and per-photo GPS locations.

        Tagging is deliberately post-import: ``photo_ids`` is the importer's
        authoritative set of successfully cataloged photos, so skipped
        archive duplicates never gain tags. Failures here do not change the
        copy/verification result; they are reported separately in the job.
        """
        if not tags and not location_from_gps:
            return

        summary = {
            "requested_tags": list(tags),
            "tagged_photos": 0,
            "location_requested": bool(location_from_gps),
            "locations_added": 0,
            "locations_unresolved": 0,
            "locations_skipped": 0,
            "errors": [],
        }
        result["tagging"] = summary

        def cancel_requested(*, pause_safe=True):
            runner_check = None
            if job is not None and runner is not None:
                runner_check = (
                    runner.is_cancelled
                    if pause_safe
                    else runner.cancellation_requested
                )
            cancelled = result.get("cancelled") or (
                runner_check is not None and runner_check(job["id"])
            )
            if cancelled:
                # The cancellation may arrive after the importer itself has
                # returned a successful result. Persist it on the shared
                # result so downstream after-import chaining also stops.
                result["cancelled"] = True
            return cancelled

        if cancel_requested():
            summary["skipped"] = "import cancelled"
            return
        if not photo_ids:
            summary["skipped"] = "no new photos"
            return

        if job is not None and runner is not None:
            phase = (
                "Adding tags and GPS locations"
                if tags and location_from_gps
                else "Adding GPS locations"
                if location_from_gps
                else "Adding tags"
            )
            runner.push_event(job["id"], "progress", {
                "current": job["progress"].get("current", 0),
                "total": job["progress"].get("total", 0),
                "current_file": "",
                "phase": phase,
            })

        thread_db = Database(db_path)
        thread_db.set_active_workspace(workspace_id)
        tagged_photo_ids = set()

        for requested_name in tags:
            if cancel_requested():
                summary["skipped"] = "import cancelled"
                break
            try:
                keyword_id = thread_db.add_keyword(
                    requested_name, kw_type="general", _commit=False,
                )
                stored = thread_db.conn.execute(
                    "SELECT name, parent_id, type FROM keywords WHERE id = ?",
                    (keyword_id,),
                ).fetchone()
                keyword_name = (
                    stored["name"] if stored and stored["name"]
                    else requested_name
                )
                items = []
                for photo_id in photo_ids:
                    if cancel_requested(pause_safe=False):
                        break
                    exists = thread_db.conn.execute(
                        "SELECT 1 FROM photo_keywords "
                        "WHERE photo_id = ? AND keyword_id = ?",
                        (photo_id, keyword_id),
                    ).fetchone()
                    if exists is not None:
                        continue
                    thread_db.tag_photo(
                        photo_id, keyword_id, source="manual", _commit=False,
                    )
                    _queue_import_keyword_add(
                        thread_db, photo_id, keyword_name, workspace_id,
                    )
                    items.append({
                        "photo_id": photo_id,
                        "old_value": "",
                        "new_value": str(keyword_id),
                    })
                if cancel_requested(pause_safe=False):
                    thread_db.conn.rollback()
                    summary["skipped"] = "import cancelled"
                    break
                if items:
                    thread_db.record_edit(
                        "keyword_add",
                        f'Added "{keyword_name}" during import to '
                        f"{len(items)} photos",
                        str(keyword_id), items, is_batch=True, _commit=False,
                    )
                thread_db.conn.commit()
                tagged_photo_ids.update(item["photo_id"] for item in items)
            except Exception as exc:
                thread_db.conn.rollback()
                log.exception("Failed to add import tag %r", requested_name)
                summary["errors"].append(
                    f'Could not add tag "{requested_name}": {exc}'
                )
        summary["tagged_photos"] = len(tagged_photo_ids)
        tagging_cancelled = cancel_requested()

        if location_from_gps and not tagging_cancelled:
            unresolved = 0
            skipped = 0
            added = 0
            cancelled_during_gps = False
            # Resolve imports in bounded chunks to limit each payload's memory
            # use and give cancellation a chance between large batches while
            # sharing the persistent ~110 m geocode cache.
            for photo_chunk in _gps_location_chunks(photo_ids, size=10000):
                if cancel_requested():
                    cancelled_during_gps = True
                    break
                try:
                    payload, error = bulk_gps_location_payload(
                        thread_db, {"photo_ids": photo_chunk},
                        cancel_check=cancel_requested,
                    )
                    if error is not None:
                        raise RuntimeError("location resolution was rejected")
                    if payload.pop("cancelled", False) or cancel_requested():
                        cancelled_during_gps = True
                        break
                    details_by_place_id = payload.pop(
                        "_details_by_place_id", {}
                    )
                    unresolved += len(payload["unresolved"])
                    skipped += len(payload["skipped"])
                    for group in payload["groups"]:
                        if cancel_requested(pause_safe=False):
                            cancelled_during_gps = True
                            break
                        details = details_by_place_id.get(group["place_id"])
                        if not details:
                            unresolved += len(group["photo_ids"])
                            continue
                        try:
                            leaf_id = thread_db.upsert_place_chain(details)
                        except Exception as exc:
                            log.exception(
                                "Failed to create GPS import location %s",
                                group["place_id"],
                            )
                            summary["errors"].append(
                                f"Could not create location "
                                f"{group.get('summary') or group['place_id']}: "
                                f"{exc}"
                            )
                            unresolved += len(group["photo_ids"])
                            continue
                        location_items = []
                        for photo_id in group["photo_ids"]:
                            if cancel_requested(pause_safe=False):
                                cancelled_during_gps = True
                                break
                            thread_db.set_photo_location(photo_id, leaf_id)
                            _queue_import_location_sync(
                                thread_db, photo_id, workspace_id,
                            )
                            location_items.append({
                                "photo_id": photo_id,
                                "old_value": "",
                                "new_value": str(leaf_id),
                            })
                        added += len(location_items)
                        if location_items:
                            thread_db.record_edit(
                                "location_set",
                                f"Added GPS location during import to "
                                f"{len(location_items)} photos",
                                "from_exif", location_items,
                                is_batch=True, _commit=False,
                            )
                    thread_db.conn.commit()
                    if cancel_requested():
                        cancelled_during_gps = True
                    if cancelled_during_gps:
                        break
                except Exception as exc:
                    thread_db.conn.rollback()
                    log.exception("Failed to add GPS locations during import")
                    summary["errors"].append(
                        f"Could not add GPS locations: {exc}"
                    )
                    unresolved += len(photo_chunk)
            summary["locations_added"] = added
            summary["locations_unresolved"] = unresolved
            summary["locations_skipped"] = skipped
            if cancelled_during_gps:
                summary["skipped"] = "import cancelled"
        elif location_from_gps:
            summary["skipped"] = "import cancelled"
        thread_db.conn.close()

    @blueprint.route("/api/import/preview", methods=["POST"])
    def api_import_preview():
        db = get_db()
        body = request.get_json(silent=True) or {}
        catalogs = body.get("catalogs", [])
        if not catalogs:
            return json_error("catalogs required")
        try:
            from importer import preview_import

            result = preview_import(catalogs, db)
            return jsonify(result)
        except Exception as e:
            return json_error(str(e), 500)

    @blueprint.route("/api/import/folder-preview-stream", methods=["POST"])
    def api_import_folder_preview_stream():
        """One storage-aware traversal streams per-folder scan progress and
        ends with the preview payload — the source-row counters and the
        preview grid share a single walk instead of scanning twice."""
        body = request.get_json(silent=True) or {}
        folders = body.get("folders", [])
        if (
            not isinstance(folders, list)
            or not folders
            or any(not isinstance(path, str) or not path for path in folders)
        ):
            return json_error("folders must be a non-empty list of paths", 400)
        file_types = body.get("file_types", [])
        return Response(
            source_discovery.stream_folder_preview(
                folders,
                file_types=file_types if file_types else "both",
                recursive=bool(body.get("recursive", True)),
            ),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @blueprint.route("/api/import/new-images-preview", methods=["POST"])
    def api_import_new_images_preview():
        """Preview grid data for a new-images snapshot, matching the
        folder-preview response shape so the same client renderer works."""
        body = request.get_json(silent=True) or {}
        snapshot_id = body.get("snapshot_id")
        if not isinstance(snapshot_id, int):
            return json_error("snapshot_id required", 400)

        db = get_db()
        if db._active_workspace_id is None:
            abort(404)
        try:
            snap = db.get_new_images_snapshot(snapshot_id)
        except OverflowError:
            snap = None
        if snap is None:
            abort(404)

        # Use user-facing roots (not every auto-linked descendant) so
        # grouping matches the source folders. Include roots currently marked
        # missing: a snapshot preview must retain the folder and unavailable
        # file the banner promised instead of silently losing its provenance.
        from new_images import mapped_roots as _ni_mapped_roots

        root_paths = [
            r["path"]
            for r in _ni_mapped_roots(
                db, db._active_workspace_id, include_missing=True,
            )
        ]

        # Build unique display names across roots by taking the shortest
        # trailing path segments that are unique — so /mnt/cardA/DCIM and
        # /mnt/cardB/DCIM become cardA/DCIM and cardB/DCIM rather than
        # colliding on "DCIM". Mirrors folder-preview's disambiguation.
        root_names = {}
        if len(root_paths) > 1:
            parts = [Path(rp).parts for rp in root_paths]
            for depth in range(1, max(len(p) for p in parts) + 1):
                suffixes = [str(Path(*p[-depth:])) for p in parts]
                if len(set(suffixes)) == len(suffixes):
                    for rp, suffix in zip(root_paths, suffixes, strict=True):
                        root_names[rp] = suffix
                    break
            else:
                for rp in root_paths:
                    root_names[rp] = rp
        else:
            for rp in root_paths:
                root_names[rp] = os.path.basename(rp.rstrip("/")) or rp

        roots = sorted(
            [(rp, root_names[rp]) for rp in root_paths],
            key=lambda pn: len(pn[0]),
            reverse=True,
        )

        def _subfolder_for(path):
            for root_path, root_name in roots:
                try:
                    rel = Path(path).parent.relative_to(root_path)
                except ValueError:
                    continue
                rel_str = str(rel)
                return root_name if rel_str == "." else os.path.join(root_name, rel_str)
            return os.path.dirname(path) or "."

        files = []
        type_breakdown = {}
        total_size = 0
        unavailable_count = 0
        for path in snap["file_paths"]:
            try:
                stat = os.stat(path)
            except OSError:
                unavailable_count += 1
                ext = os.path.splitext(path)[1].lower()
                files.append({
                    "path": path,
                    "filename": os.path.basename(path),
                    "subfolder": _subfolder_for(path),
                    "size": 0,
                    "extension": ext,
                    "mtime": None,
                    "available": False,
                    "error": "File is no longer available",
                })
                type_breakdown[ext] = type_breakdown.get(ext, 0) + 1
                continue
            ext = os.path.splitext(path)[1].lower()
            files.append({
                "path": path,
                "filename": os.path.basename(path),
                "subfolder": _subfolder_for(path),
                "size": stat.st_size,
                "extension": ext,
                "mtime": stat.st_mtime,
                "available": True,
                "thumb_url": "/api/import/folder-preview/thumbnail?path=" + quote(path),
            })
            type_breakdown[ext] = type_breakdown.get(ext, 0) + 1
            total_size += stat.st_size

        return jsonify({
            "total_count": snap["file_count"],
            "available_count": len(files) - unavailable_count,
            "unavailable_count": unavailable_count,
            "total_size": total_size,
            "type_breakdown": type_breakdown,
            "duplicate_count": 0,
            "files": files,
        })

    @blueprint.route("/api/import/check-duplicates", methods=["POST"])
    def api_import_check_duplicates():
        """Stream duplicate detection results via SSE.

        Accepts {"paths": [...], "verify_by_hash": bool} and streams
        batches of duplicate paths back to the client. Uses the same
        DuplicateChecker as ingest() — metadata-first with a content-hash
        fallback by default, hash-everything when verify_by_hash — so the
        preview's DUPLICATE badges and count are exactly the files the
        import step will skip.

        Optional {"destination": abs path, "folder_template": str} turns on
        destination-recovery detection: non-duplicate files whose planned
        destination folder already holds a byte-identical file at the
        primary name — or at a numeric-suffix slot the run would adopt
        (``name_1.ext``, ``name_2.ext``, ...) — are streamed as
        ``recovered`` (with a final ``recovered_count``). Those are the
        files a cancelled/crashed prior run left at the destination —
        the import adopts them via crash recovery (verify + catalog, no
        re-copy), so counting them "to copy" overstates the transfer.

        A size match is the cheap gate: same-size candidates are the only
        ones the run would even byte-verify, so hashing is scoped to that
        subset (rare in a fresh import; proportional to actual collisions
        in a retry). A same-size candidate whose bytes disagree advances
        the walk the same way ``import_job`` does on a hash mismatch —
        otherwise the preview would tell the user "not re-copied" for a
        file the run is about to suffix-copy under a numbered name.

        Optional {"skip_duplicates": false} matches the import job's own
        gate: when the run has duplicate skipping off, the library-dedup
        checker isn't consulted (library duplicates get copied anyway),
        but crash-recovery adoption of byte-identical files at the
        destination still fires. Passing skip_duplicates=false here
        mirrors that: no ``duplicates`` are streamed, but ``recovered``
        still is — so the retry preview after a cancelled dedup-off run
        doesn't overstate the transfer.
        """
        body = request.get_json(silent=True) or {}
        paths = body.get("paths", [])
        verify_by_hash = bool(body.get("verify_by_hash"))
        # Default True is the import job's default and preserves the
        # pre-existing endpoint contract; only the dedup-off preview
        # branch passes False.
        skip_duplicates = bool(body.get("skip_duplicates", True))
        if not paths:
            return json_error("paths required", 400)

        from import_dedup import (
            CatalogIndex,
            DuplicateChecker,
            source_capture_timestamps,
        )
        from ingest import _is_unsafe_path, build_destination_path
        from scanner import compute_file_hash

        recovery_base = (body.get("destination") or "").strip()
        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        if recovery_base and not os.path.isabs(recovery_base):
            return json_error("destination must be an absolute path", 400)
        if recovery_base and folder_template and _is_unsafe_path(
                folder_template):
            return json_error(
                "folder_template must be a relative path without '..' "
                "or backslashes", 400)

        db = get_db()
        # The checker still runs when skip_duplicates=False, but only for
        # its EXIF-batching side effect (needed by _recovery_candidate in
        # default mode). check_and_record() is skipped in the generator
        # below so no library-dedup verdict is produced — matching the
        # import job, which doesn't create the checker at all in that
        # mode.
        checker = DuplicateChecker(
            CatalogIndex.from_db(db), verify_by_hash=verify_by_hash,
        )

        # str(path) -> capture datetime for recovery folder planning when
        # verify_by_hash disables the checker's own EXIF batching.
        recovery_times = {}

        # name -> size per planned destination folder, one scandir each —
        # the destination may be a network mount, so listings are batched
        # rather than stat'ing per candidate file (count round trips).
        dir_listings = {}

        def _planned_folder_listing(folder):
            if folder not in dir_listings:
                entries = {}
                try:
                    with os.scandir(folder) as it:
                        for entry in it:
                            try:
                                # Symlinks are deliberately EXCLUDED, and
                                # this is a considered trade, not an
                                # oversight. The import walk follows them
                                # (os.stat) and adopts a symlink to an
                                # off-card regular file whose bytes match,
                                # so excluding them makes the preview
                                # UNDER-report recovery for that geometry
                                # — it says "will copy" for a file the run
                                # adopts. That is the safe direction to be
                                # wrong in.
                                #
                                # Following them was tried (PR 7b) and
                                # reverted: the walk refuses a candidate
                                # resolving under ANY source root, while
                                # this endpoint's ``_is_source`` can only
                                # compare ``samefile`` against the CURRENT
                                # source file. A symlink to a *different*
                                # card file with identical bytes therefore
                                # slipped through and was reported as
                                # recovered — an OVER-claim, promising
                                # "already safe at the destination" for
                                # bytes that live only on the card. This
                                # endpoint receives ``paths``, not the
                                # import's source roots, so it cannot
                                # reconstruct that guard; doing this right
                                # needs the shared walk, i.e. the PR 8
                                # de-mirror. Trading a safe under-report
                                # for an unsafe over-report is not worth
                                # it in the meantime.
                                # Codex review of PR #1450, rounds 4-5.
                                if entry.is_file(follow_symlinks=False):
                                    entries[entry.name] = entry.stat(
                                        follow_symlinks=False).st_size
                            except OSError:
                                continue
                except OSError:
                    pass  # missing/unreadable folder -> nothing to adopt
                dir_listings[folder] = entries
            return dir_listings[folder]

        def _recovery_candidate(path):
            """True when the planned destination already holds a byte-
            identical file at the primary name OR at any suffix slot the
            run would adopt — mirrors ``import_job``'s adopt precondition
            (size match then byte-verify). A size-matching candidate whose
            bytes disagree advances the walk the same way a hash mismatch
            does in the run: otherwise the preview would subtract the
            file from "to copy" and promise "not re-copied" for a file
            the run will suffix-copy under a numbered name."""
            if not recovery_base:
                return False
            source_file = Path(path)
            try:
                size = source_file.stat().st_size
            except OSError:
                return False
            # NOTE: zero-byte sources are NOT special-cased here. They
            # used to return False on the reasoning that "the duplicate
            # checker gives them no identity either" — but that conflates
            # duplicate identity with crash-recovery adoption, which is
            # what this preview is about. ``_resolve_dest_collision``
            # adopts an empty candidate for an empty source at every
            # candidate position on both transports (spec PR 7b flip A;
            # the local primary-name case predates it), so returning
            # False here left the preview counting those files as
            # transfers the run would never perform. The generic path
            # below gets this right on its own: ``_src_hash`` uses
            # ``compute_file_hash``, so an empty source hashes to
            # EMPTY_FILE_SHA256 rather than the checker's None, and it
            # matches an empty candidate. Non-regular entries (FIFOs,
            # device nodes) stay excluded because
            # ``_planned_folder_listing`` only records
            # ``is_file(follow_symlinks=False)`` entries — which is also
            # what the run's own S_ISREG guard does. Codex review of
            # PR #1450.
            # Folder planning mirrors ingest._source_file_timestamps:
            # EXIF capture time falling back to file mtime. In the default
            # mode checker.prepare() already batched the EXIF reads and
            # capture_time() is a cache hit; in verify mode prepare() is a
            # no-op, so the times come from this request's own batch
            # (below) — never resolved lazily one file at a time.
            if verify_by_hash:
                ts = recovery_times.get(str(source_file))
            else:
                ts = checker.capture_time(source_file)
            if ts is None:
                with contextlib.suppress(OSError, ValueError,
                                         OverflowError):
                    ts = datetime.fromtimestamp(
                        source_file.stat().st_mtime)
            try:
                rel_folder = build_destination_path(ts, folder_template)
            except ValueError:
                return False
            folder = (
                recovery_base if rel_folder in ("", ".")
                else os.path.join(recovery_base, rel_folder)
            )
            listing = _planned_folder_listing(folder)
            primary_name = source_file.name

            # Lazy source-hash: only computed once, and only if we hit a
            # size-matching candidate that needs verifying. A typical
            # fresh import has no size collisions and skips hashing
            # entirely.
            src_hash_cache = []

            def _src_hash():
                if not src_hash_cache:
                    try:
                        src_hash_cache.append(compute_file_hash(
                            str(source_file)))
                    except OSError:
                        src_hash_cache.append(None)
                return src_hash_cache[0]

            def _is_source(cand_path):
                # Reject destination candidates that ARE the source file
                # itself — the run rejects that self-copy overlap
                # (destination is an ancestor of the source AND the
                # folder template renders back onto the source folder,
                # e.g. importing /archive/2026/2026-07-03/IMG.jpg into
                # /archive with %Y/%Y-%m-%d) rather than adopting it, so
                # the preview must not promise "verified & adopted, not
                # re-copied" and subtract it from "to copy" for a file
                # the run will fail. Mirrors import_job's samefile guard
                # with the same normalized-path fallback for paths that
                # can't be stat'd.
                try:
                    return (
                        os.path.exists(cand_path)
                        and os.path.samefile(str(source_file), cand_path)
                    )
                except OSError:
                    return (
                        os.path.normpath(str(source_file))
                        == os.path.normpath(cand_path)
                    )

            def _bytes_match(cand_path):
                if _is_source(cand_path):
                    return False
                sh = _src_hash()
                if sh is None:
                    return False
                try:
                    return compute_file_hash(cand_path) == sh
                except OSError:
                    return False

            primary_size = listing.get(primary_name)
            primary_path = os.path.join(folder, primary_name)
            if primary_size == size and _is_source(primary_path):
                # Destination candidate at the primary slot IS the
                # source file. The run fails this file entirely rather
                # than walking suffixes; report as not recovered instead
                # of falling through to the suffix walk (which could
                # find a coincidental byte-identical sibling in the
                # source folder and wrongly claim adoption).
                return False
            if primary_size == size:
                if _bytes_match(primary_path):
                    return True
                # Same size, different bytes at the primary slot: the run
                # will hash-mismatch and advance to the suffix walk.
            elif primary_size is None:
                # No collision on the primary name — the run copies to the
                # primary slot without walking suffixes.
                return False
            # Primary slot is taken by a different-sized (or same-sized-
            # different-bytes) file. Mirror import_job's collision walk
            # (``name_1.ext``, ``name_2.ext``, ...): stop at the first
            # free slot (the run would land a fresh copy there — not
            # recovered), or claim recovery at the first byte-identical
            # candidate (the run would adopt it). Size-mismatched slots
            # advance the counter; same-size-different-bytes slots also
            # advance, mirroring the run's hash-mismatch skip.
            stem, suffix_ext = os.path.splitext(primary_name)
            counter = 1
            while True:
                candidate = f"{stem}_{counter}{suffix_ext}"
                cand_size = listing.get(candidate)
                if cand_size is None:
                    return False
                if cand_size == size and _bytes_match(
                        os.path.join(folder, candidate)):
                    return True
                counter += 1

        def generate():
            total = len(paths)
            duplicate_count = 0
            recovered_count = 0
            batch_duplicates = []
            batch_recovered = []
            # Batch the EXIF header reads up front in bounded chunks (no-op
            # in verify_by_hash mode). Intra-run duplicate tracking lives in
            # the checker: identical source files not yet in the DB are
            # reported as duplicates of each other, matching the actual
            # import step. Chunking with a yield between each chunk lets a
            # superseded browser request stop this phase within one chunk's
            # worth of I/O — a single upfront prepare() over tens of
            # thousands of files would otherwise ignore the disconnect
            # entirely until the per-file loop begins.
            prep_paths = [Path(p) for p in paths]
            prep_batch = DUPLICATE_CHECK_PREP_BATCH_SIZE
            for prep_start in range(0, len(prep_paths), prep_batch):
                chunk = prep_paths[prep_start:prep_start + prep_batch]
                checker.prepare(chunk)
                if recovery_base and verify_by_hash:
                    # prepare() skipped the EXIF batch (verify mode's
                    # identity is the hash), but recovery folder planning
                    # still needs capture times — resolve them alongside
                    # the same chunk so both prep paths share the same
                    # cancellation cadence.
                    recovery_times.update({
                        str(f): dt
                        for f, dt in source_capture_timestamps(chunk).items()
                    })
                # Cheap heartbeat frame the client can render as
                # "preparing metadata…" and, more importantly, the yield
                # that lets the WSGI server notice a disconnected client
                # between chunks instead of after the entire prep phase.
                prepared = prep_start + len(chunk)
                yield f"data: {json.dumps({'preparing': prepared, 'total': total})}\n\n"

            last_flush = time.monotonic()
            for checked, path in enumerate(paths, 1):
                # Zero-byte placeholders are non-duplicates (the checker
                # gives them no identity), and unreadable/missing files
                # are skipped; both fall through so the batch-yield block
                # below still runs. A `continue` would swallow any
                # already-queued `batch_duplicates` whenever such a file
                # landed on the last path or on a batch boundary, leaving
                # the UI unable to deselect those known dupes.
                try:
                    # When skip_duplicates=False, the import run doesn't
                    # consult the library-dedup checker at all — every
                    # source file goes on to the recovery/adopt gate. Skip
                    # check_and_record() here so a cataloged twin that
                    # also sits at the destination is streamed as
                    # ``recovered`` (matching what the run will actually
                    # do) instead of ``duplicates`` (which the client
                    # would then not subtract from the transfer count).
                    if skip_duplicates and checker.check_and_record(
                            Path(path)):
                        batch_duplicates.append(path)
                        duplicate_count += 1
                    elif _recovery_candidate(path):
                        # Duplicate gate first, recovery second — same
                        # order as the import run, so a cataloged twin
                        # that also sits at the destination stays a
                        # duplicate here and a skip there.
                        batch_recovered.append(path)
                        recovered_count += 1
                except OSError:
                    pass  # Skip unreadable/missing files

                # The yield is both how the client learns progress and how
                # the WSGI server notices that a superseded browser request
                # disconnected — cheap checks may finish dozens of files
                # inside one window (a single event covers them all), while
                # a slow byte-for-byte hash spends longer than the window on
                # one file (that file gets its own event and cancellation
                # stops within the next check). ``checked == total``
                # guarantees the last progress event always ships so the
                # client sees ``checked == total`` before ``done``.
                now = time.monotonic()
                if (
                    checked == total
                    or now - last_flush
                    >= DUPLICATE_CHECK_FLUSH_INTERVAL_SECONDS
                ):
                    yield f"data: {json.dumps({'duplicates': batch_duplicates, 'recovered': batch_recovered, 'checked': checked, 'total': total})}\n\n"
                    batch_duplicates = []
                    batch_recovered = []
                    last_flush = now

            yield f"data: {json.dumps({'done': True, 'duplicate_count': duplicate_count, 'recovered_count': recovered_count, 'checked': total, 'total': total})}\n\n"

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @blueprint.route("/api/import/collection-preview", methods=["POST"])
    def api_import_collection_preview():
        """Return preview data for photos in a collection."""
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        if not collection_id:
            return json_error("collection_id required", 400)

        db = get_db()
        err = reject_visual_collection(db, collection_id)
        if err is not None:
            return err
        try:
            photos = db.get_collection_photos(collection_id, page=1, per_page=100000)
        except ValueError as e:
            log.exception(
                "Collection %s has unresolvable rules", collection_id
            )
            return json_error(f"collection rules cannot be resolved: {e}", 400)

        folder_rows = db.conn.execute("SELECT id, path, name FROM folders").fetchall()
        folder_map = {r["id"]: dict(r) for r in folder_rows}

        files = []
        type_breakdown = {}
        total_size = 0

        for p in photos:
            folder = folder_map.get(p["folder_id"], {})
            folder_name = folder.get("name", "Unknown")
            ext = (p["extension"] or "").lower()
            size = p["file_size"] or 0
            folder_path = folder.get("path", "")
            full_path = os.path.join(folder_path, p["filename"]) if folder_path else p["filename"]

            files.append({
                "path": full_path,
                "filename": p["filename"],
                "subfolder": folder_name,
                "size": size,
                "extension": ext,
                "mtime": p["file_mtime"] or 0,
                "thumb_url": f"/thumbnails/{p['id']}.jpg",
                "duplicate": False,
                "photo_id": p["id"],
            })

            type_breakdown[ext] = type_breakdown.get(ext, 0) + 1
            total_size += size

        return jsonify({
            "total_count": len(files),
            "total_size": total_size,
            "type_breakdown": type_breakdown,
            "duplicate_count": 0,
            "files": files,
        })

    @blueprint.route("/api/import/destination-preview", methods=["POST"])
    def api_import_destination_preview():
        """Preview destination folder structure without copying files."""
        body = request.get_json(silent=True) or {}
        sources = body.get("sources", [])
        destination = body.get("destination", "")
        if not sources:
            return json_error("sources required", 400)
        if not destination:
            return json_error("destination required", 400)
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path", 400)

        from ingest import _is_unsafe_path, preview_destination

        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        if folder_template and _is_unsafe_path(folder_template):
            return json_error("folder_template must be a relative path without '..' or backslashes", 400)

        try:
            result = preview_destination(
                sources=sources,
                destination=destination,
                folder_template=folder_template,
                file_types=body.get("file_types", "both"),
                recursive=body.get("recursive", True),
                exclude_paths=body.get("exclude_paths"),
            )
        except ValueError as e:
            return json_error(str(e), 400)

        # Transparency: if the destination is (or sits inside) a folder Vireo
        # already manages, surface it as an existing archive so the UI can
        # frame the import as a merge rather than a fresh copy. Do NOT accept
        # the inverse relationship (a tracked folder somewhere below the
        # selected destination): selecting a broad mount such as
        # /Volumes/Photography while a managed archive lives at
        # /Volumes/Photography/Raw Files/USA does not mean a new import into
        # /Volumes/Photography/2026 will merge into that nested archive.
        # Calling the overlap helper here produced exactly that contradictory
        # preview. Pure catalog read — no file I/O beyond the tracked-folder
        # probe.
        from move import _tracked_destination_ancestor
        db = get_db()
        from db import _subtree_prefix

        def _archive_photo_count(archive_path):
            prefix = _subtree_prefix(archive_path)
            # Count only ok/partial folders — the same set ingest treats as
            # "the archive" — so the callout's "N photos" matches what the
            # merge actually considers present. Pure catalog read; no on-disk
            # check.
            return db.conn.execute(
                """SELECT COUNT(*) AS c
                     FROM photos p JOIN folders f ON f.id = p.folder_id
                    WHERE (f.path = ?
                           OR substr(REPLACE(f.path, '\\', '/'), 1, ?) = ?)
                      AND f.status IN ('ok', 'partial')""",
                (archive_path, len(prefix), prefix),
            ).fetchone()["c"]

        managed_archives = []
        managed_archive = None

        dest_tracked = _tracked_destination_ancestor(db, -1, destination)
        if dest_tracked is not None:
            # The destination itself is (or sits inside) a tracked archive, so
            # every generated folder lands inside it — full coverage.
            managed_archive = {
                "path": dest_tracked["path"],
                "photo_count": _archive_photo_count(dest_tracked["path"]),
            }
            managed_archives = [dict(managed_archive, coverage="full")]
        else:
            # The destination itself may sit above every tracked archive while
            # the folder template still maps generated files into one — e.g.
            # destination /Photography with a tracked root /Photography/2026
            # and template 2026/%Y-%m-%d lands every file inside the managed
            # /Photography/2026 archive. Checking only the destination's
            # ancestors leaves managed_archive None even though the import IS
            # a merge into a tracked archive. Walk the concrete full_path
            # values from preview_destination() so we catch that case, while
            # still avoiding the sibling-folder false positive the
            # destination-only guard was written to prevent (a broad mount
            # whose tracked archive is a sibling of the generated folders
            # never becomes an ancestor of any full_path).
            #
            # Do NOT collapse mixed coverage to a single archive. A source
            # spanning multiple date-templated folders can split so that some
            # generated folders land inside a tracked archive and others land
            # outside it, or land in DIFFERENT tracked archives — for example
            # files from 2025 and 2026 with destination /Photography, template
            # %Y/%Y-%m-%d, and only /Photography/2026 tracked (2025 files land
            # outside the archive), or the same source but with both
            # /Photography/2025 and /Photography/2026 tracked (each subset
            # lands in a different archive). Breaking at the first match and
            # asserting "this import lands inside archive X" for the whole
            # preview lies about the other subsets. Aggregate the matches and
            # expose partial/multiple overlaps distinctly.
            folder_entries = result.get("folders") or []
            per_archive = {}  # archive_path -> matched_folder_count
            unmatched = 0
            considered = 0
            for entry in folder_entries:
                full_path = entry.get("full_path")
                if not full_path:
                    continue
                considered += 1
                candidate = _tracked_destination_ancestor(db, -1, full_path)
                if candidate is None:
                    unmatched += 1
                else:
                    key = candidate["path"]
                    per_archive[key] = per_archive.get(key, 0) + 1
            for archive_path, matched in per_archive.items():
                # A single archive covers "all" generated folders only when it
                # matched every considered entry AND nothing landed outside a
                # tracked archive AND no other tracked archive claimed any
                # folder. Anything else is a partial overlap for that archive.
                full = (
                    unmatched == 0
                    and len(per_archive) == 1
                    and matched == considered
                    and considered > 0
                )
                managed_archives.append({
                    "path": archive_path,
                    "photo_count": _archive_photo_count(archive_path),
                    "coverage": "full" if full else "partial",
                })
            if (
                len(managed_archives) == 1
                and managed_archives[0]["coverage"] == "full"
            ):
                managed_archive = {
                    "path": managed_archives[0]["path"],
                    "photo_count": managed_archives[0]["photo_count"],
                }

        result["managed_archive"] = managed_archive
        # ``managed_archives`` is the authoritative list — the UI should
        # prefer it so partial or multi-archive overlaps are described
        # honestly. ``managed_archive`` stays populated only for the
        # single-archive full-coverage case so older clients keep working.
        result["managed_archives"] = managed_archives
        return jsonify(result)

    @blueprint.route("/api/import/folder-preview/thumbnail")
    def api_import_folder_preview_thumbnail():
        """Generate an on-the-fly thumbnail for a source file (not yet imported).

        Cache policy: only success responses are cacheable. Failures emit
        ``Cache-Control: no-store`` so a transient libraw I/O glitch (NAS
        contention, network blip) doesn't pin question marks in the user's
        preview grid for the cache lifetime — the next page load retries
        and typically succeeds.
        """
        file_path = request.args.get("path", "")
        if not file_path:
            return json_error("path parameter required", 400)
        if not os.path.isfile(file_path):
            resp = make_response("", 404)
            resp.cache_control.no_store = True
            return resp

        from image_loader import load_image
        img = load_image(file_path, max_size=200)
        if img is None:
            resp = make_response("", 404)
            resp.cache_control.no_store = True
            return resp

        import io
        buf = io.BytesIO()
        img.save(buf, "JPEG", quality=70)
        buf.seek(0)

        resp = make_response(buf.read())
        resp.content_type = "image/jpeg"
        resp.cache_control.public = True
        resp.cache_control.max_age = 300  # 5 min — these are ephemeral
        return resp

    @blueprint.route("/api/import/readiness")
    def api_import_readiness():
        """Report metadata tooling and repairable degraded-import rows."""
        from image_loader import is_excluded_scan_path
        from metadata import exiftool_status

        db = get_db()
        active_ws = db._active_workspace_id
        status = exiftool_status()
        roots = [r["path"] for r in db.get_workspace_folder_roots(active_ws)]
        # Filter macOS app-managed library bundles before ``os.path.isdir``:
        # stat-ing a ``.photoslibrary`` root (or a symlink into one) itself
        # trips the "access data from other apps" TCC prompt, and this
        # readiness call fires automatically as soon as the Import page
        # opens. See ``api_job_scan`` for the same guard on user-supplied
        # roots; legacy workspace roots need the same treatment.
        reachable_roots = [
            root for root in roots
            if not is_excluded_scan_path(root) and os.path.isdir(root)
        ]
        # Scope the count to reachable roots so an offline drive can't
        # inflate ``metadata_repair_count`` and enable a Repair button
        # for a job that would immediately no-op on the photos it can't
        # touch. When there are no reachable roots the scoped count is
        # 0, which itself gates ``metadata_repair_available`` — no need
        # for the previous explicit ``reachable_roots`` conjunction.
        repair_count = metadata_repair_count(db, active_ws, reachable_roots)
        return jsonify({
            "exiftool": status,
            "requires_exiftool": config["REQUIRE_EXIFTOOL_FOR_IMPORT"],
            "metadata_repair_count": repair_count,
            "metadata_repair_available": bool(
                status["available"] and repair_count
            ),
            "reachable_root_count": len(reachable_roots),
        })

    @blueprint.route("/api/jobs/import-full", methods=["POST"])
    @background_job
    def api_job_import_full(ctx):
        """Full-chain import: copy files -> scan -> create collection."""
        body = request.get_json(silent=True) or {}
        source = body.get("source", "")
        destination = body.get("destination", "")
        file_types = body.get("file_types", "both")
        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        skip_duplicates = body.get("skip_duplicates", True)
        verify_by_hash = bool(body.get("verify_by_hash"))
        copy = body.get("copy", True)
        exclude_paths = set(body.get("exclude_paths", []))

        if not source:
            return json_error("source is required")
        from image_loader import is_excluded_scan_path
        # See api_job_scan for why this must run before os.path.isdir.
        if is_excluded_scan_path(source):
            return json_error(
                f"source is inside a macOS app-managed library and cannot "
                f"be imported: {source}"
            )
        if not os.path.isdir(source):
            return json_error(f"source directory not found: {source}")
        if copy:
            if not destination:
                return json_error("source and destination are required")
            if not os.path.isabs(destination):
                return json_error("destination must be an absolute path")
            from ingest import _is_unsafe_path
            if folder_template and _is_unsafe_path(folder_template):
                return json_error("folder_template must be a relative path without '..' or backslashes")

        def work(job):
            from scanner import scan as do_scan
            from thumbnails import generate_all

            thread_db = ctx.thread_db()
            # Check folder health before scanning to prevent duplicate imports
            if thread_db.check_folder_health():
                invalidate_missing_originals()
            job["_start_time"] = time.time()

            scan_target = str(Path(source))  # normalize (strips trailing slash)
            # restrict_dirs narrows the post-ingest scan to just the subfolders
            # that received files, instead of walking the full destination
            # tree. Populated in the copy branch from ingest_result's
            # copied_paths (parent dirs) and duplicate_folders. Left as None
            # for copy=false so scan-in-place keeps its original full-tree
            # behavior.
            restrict_dirs = None

            # Define steps based on whether we're copying
            steps = []
            if copy:
                steps.append({"id": "ingest", "label": "Import photos"})
            steps.extend([
                {"id": "scan", "label": "Scan photos"},
                {"id": "thumbnails", "label": "Generate thumbnails"},
                {"id": "collection", "label": "Create collection"},
            ])
            ctx.runner.set_steps(job["id"], steps)

            if copy:
                from ingest import ingest as do_ingest

                # Phase 1: Copy files
                ctx.runner.update_step(job["id"], "ingest", status="running")

                def ingest_cb(current, total, filename):
                    job["progress"]["current"] = current
                    job["progress"]["total"] = total
                    job["progress"]["current_file"] = filename
                    ctx.runner.push_event(job["id"], "progress", {
                        "current": current, "total": total,
                        "current_file": filename,
                        "phase": "Importing photos",
                    })

                ingest_result = do_ingest(
                    source_dir=source,
                    destination_dir=destination,
                    db=thread_db,
                    file_types=file_types,
                    folder_template=folder_template,
                    skip_duplicates=skip_duplicates,
                    verify_by_hash=verify_by_hash,
                    progress_callback=ingest_cb,
                    pause_callback=lambda: ctx.checkpoint(job),
                    skip_paths=exclude_paths or None,
                )
                copied_paths = ingest_result.get("copied_paths", [])
                duplicate_folders = ingest_result.get("duplicate_folders", [])
                scan_target = destination

                # Build restrict_dirs from the folders ingest actually touched
                # so the post-ingest scan doesn't re-walk the entire
                # destination tree. Without this, importing ~2k RAWs into a
                # populated library caused scanner.scan to enumerate tens of
                # thousands of already-indexed files (observed: 59k). Mirrors
                # the same pattern in pipeline_job.py. Only paths under the
                # normalized destination are included; ".." tricks cannot
                # escape. If nothing was copied and no duplicate folders were
                # reported, restrict_dirs stays an empty list — scanner.scan
                # then has no directories to enumerate, which matches intent
                # (there is nothing new to index).
                dest_normalized = Path(os.path.normpath(destination))

                def _under_destination(path_str):
                    try:
                        return Path(os.path.normpath(path_str)).is_relative_to(
                            dest_normalized
                        )
                    except ValueError:
                        return False

                restrict_set = set()
                for cp in copied_paths:
                    parent = str(Path(cp).parent)
                    if _under_destination(parent):
                        restrict_set.add(parent)
                for folder in duplicate_folders:
                    if _under_destination(folder):
                        restrict_set.add(folder)
                restrict_dirs = sorted(restrict_set)

                ctx.runner.update_step(job["id"], "ingest", status="completed",
                                   summary=f"{ingest_result.get('copied', 0)} copied")

            # Phase 2: Scan to index into DB
            ctx.checkpoint(job)
            ctx.runner.update_step(job["id"], "scan", status="running")

            def scan_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                ctx.runner.push_event(job["id"], "progress", {
                    "current": current, "total": total,
                    "current_file": "",
                    "phase": "Scanning photos",
                })

            # ``import-photos`` is a pausable job (registered as a
            # pause participant by the runner). Without these probes
            # the scan phase would keep hashing on its process pool
            # and hold its CPU lease across the entire pause, ignoring
            # the pause signal until the current source finishes.
            # Same wiring the in-place import path picked up in
            # 37e0e3a0 for the identical reason.
            #
            # ``ctx.runner.is_cancelled`` internally parks on Pause via
            # ``wait_if_paused``. Wrap the parking call in
            # ``suspend_resource_wait_timing`` so an hour-long pause
            # while a scan is waiting for CPU permits does not persist
            # as an hour of "resource contention" on the job's
            # diagnostics. The context manager is a no-op when no
            # ledger wait is active, so it's safe on non-pausable
            # invocations too. Mirrors what ``_pause_checkpoint``
            # does for pipeline participants (pipeline_job.py:1567).
            def scan_cancel_check():
                from resource_ledger import suspend_resource_wait_timing
                with suspend_resource_wait_timing():
                    return ctx.runner.is_cancelled(job["id"])

            def scan_pause_check():
                return ctx.runner.pause_requested(job["id"])

            def scan_cancel_only_check():
                return ctx.runner.cancellation_requested(job["id"])

            vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
            try:
                # copy=false: scan_target is the source and restrict_dirs is
                #   None, so scanner walks the full source tree (unchanged).
                # copy=true: scan_target is the destination (folder hierarchy
                #   root, for parent-folder chain creation), but restrict_dirs
                #   narrows enumeration to only the subfolders ingest wrote
                #   into. An empty list means "nothing new to scan" — a no-op
                #   inside scanner.scan.
                do_scan(
                    scan_target, thread_db,
                    progress_callback=scan_cb,
                    skip_paths=exclude_paths or None,
                    vireo_dir=vireo_dir,
                    thumb_cache_dir=config["THUMB_CACHE_DIR"],
                    restrict_dirs=restrict_dirs,
                    cancel_check=scan_cancel_check,
                    pause_check=scan_pause_check,
                    cancel_only_check=scan_cancel_only_check,
                )
            finally:
                # scanner.scan commits photo rows incrementally, so even a mid-scan
                # failure can leave DB state that invalidates cached new-image counts.
                invalidate_new_images_after_scan(thread_db, scan_target)
                # scanner.scan touches disk and may reconcile ghost rows
                # (e.g. a user restored an original before running import).
                # The pre-scan health-check invalidation only fires when a
                # folder flips missing/ok, so also drop the missing-originals
                # cache once the scan itself has run — even on partial
                # failure, since rows are committed incrementally.
                try:
                    invalidate_missing_originals()
                except Exception:
                    log.exception(
                        "Failed to invalidate missing-originals cache after import scan of %s",
                        scan_target,
                    )
            scan_count = job["progress"].get("total", 0)
            scan_summary = f"{scan_count} photos"
            metadata_warning = scan_metadata_warning()
            if metadata_warning:
                scan_summary += f" — {metadata_warning}"
            ctx.runner.update_step(job["id"], "scan", status="completed",
                               summary=scan_summary)

            # Phase 3: Generate thumbnails
            ctx.checkpoint(job)
            ctx.runner.update_step(job["id"], "thumbnails", status="running")
            ctx.runner.push_event(job["id"], "progress", {
                "current": 0, "total": 0,
                "current_file": "Checking for new thumbnails...",
                "phase": "Generating thumbnails",
            })

            def thumb_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                ctx.runner.push_event(job["id"], "progress", {
                    "current": current, "total": total,
                    "current_file": "",
                    "phase": "Generating thumbnails",
                })

            thumb_result = generate_all(
                thread_db, config["THUMB_CACHE_DIR"],
                progress_callback=thumb_cb,
                cancel_check=lambda: ctx.runner.is_cancelled(job["id"]),
                vireo_dir=vireo_dir,
            )
            from thumbnails import format_summary as thumb_summary
            ctx.runner.update_step(job["id"], "thumbnails", status="completed",
                               summary=thumb_summary(thumb_result))

            # Phase 4: Create collection
            ctx.checkpoint(job)
            ctx.runner.update_step(job["id"], "collection", status="running")
            photo_ids = []
            if copy:
                # Collection from copied files (existing logic)
                if copied_paths:
                    thread_db.conn.execute(
                        "CREATE TEMP TABLE IF NOT EXISTS _imported_paths (dirpath TEXT, fname TEXT)"
                    )
                    thread_db.conn.execute("DELETE FROM _imported_paths")
                    thread_db.conn.executemany(
                        "INSERT INTO _imported_paths (dirpath, fname) VALUES (?, ?)",
                        [(os.path.dirname(p), os.path.basename(p)) for p in copied_paths],
                    )
                    rows = thread_db.conn.execute(
                        """SELECT p.id FROM photos p
                           JOIN folders f ON p.folder_id = f.id
                           JOIN _imported_paths ip ON f.path = ip.dirpath
                                                   AND p.filename = ip.fname"""
                    ).fetchall()
                    photo_ids = [r["id"] for r in rows]
                    thread_db.conn.execute("DROP TABLE IF EXISTS _imported_paths")
            else:
                # Collection from all photos in the scanned folder
                rows = thread_db.conn.execute(
                    """SELECT p.id FROM photos p
                       JOIN folders f ON p.folder_id = f.id
                       WHERE f.path = ? OR f.path LIKE ?""",
                    (scan_target, scan_target.rstrip("/") + "/%"),
                ).fetchall()
                photo_ids = [r["id"] for r in rows]

            collection_id = None
            collection_name = None
            if photo_ids:
                from datetime import datetime as dt
                collection_name = "Import " + dt.now().strftime("%Y-%m-%d %H:%M")
                collection_id = thread_db.add_collection(
                    collection_name,
                    json.dumps([{"field": "photo_ids", "value": photo_ids}]),
                )

            col_summary = collection_name if collection_name else "no photos"
            ctx.runner.update_step(job["id"], "collection", status="completed",
                               summary=col_summary)

            result = {
                "photos_indexed": len(photo_ids),
                "collection_id": collection_id,
                "collection_name": collection_name,
            }
            if copy:
                result["copied"] = ingest_result.get("copied", 0)
                result["skipped_duplicate"] = ingest_result.get("skipped_duplicate", 0)
                result["failed"] = ingest_result.get("failed", 0)
                result["total"] = ingest_result.get("total", 0)

            return result

        return ctx.start(
            "import-full", work, pausable=True,
            config={"source": source, "destination": destination, "copy": copy, "file_types": file_types},
        )

    @blueprint.route("/api/jobs/import", methods=["POST"])
    @background_job
    def api_job_import(ctx):
        body = request.get_json(silent=True) or {}
        catalogs = body.get("catalogs", [])
        strategy = body.get("strategy", "merge_all")
        write_xmp = body.get("write_xmp", False)
        if not catalogs:
            return json_error("catalogs required")

        def work(job):
            from importer import execute_import

            thread_db = ctx.thread_db()

            def progress_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                    },
                )

            return execute_import(
                catalogs,
                thread_db,
                write_xmp=write_xmp,
                strategy=strategy,
                progress_callback=progress_cb,
                pause_callback=lambda: ctx.checkpoint(job),
            )

        return ctx.start(
            "import", work, pausable=True, config={"catalogs": catalogs, "strategy": strategy},
        )

    @blueprint.route("/api/import/orphaned-staging", methods=["GET"])
    def api_import_orphaned_staging():
        """List old pipeline staging folders that need verified recovery."""
        from staging_recovery import discover_orphaned_staging

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        return jsonify({"items": discover_orphaned_staging(vireo_dir)})

    @blueprint.route("/api/import/orphaned-staging/verify", methods=["POST"])
    @background_job
    def api_import_orphaned_staging_verify(ctx):
        """Start a verification job for one old pipeline staging folder."""
        from staging_recovery import verify_orphaned_staging

        body = request.get_json(silent=True) or {}
        path = body.get("path")
        if not isinstance(path, str) or not path:
            return json_error("path required")

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])

        def work(job):
            thread_db = ctx.thread_db()
            return verify_orphaned_staging(
                thread_db, vireo_dir, path, pause_callback=lambda: ctx.checkpoint(job),
            )

        return ctx.start(
            "staging-verify",
            work,
            pausable=True,
            config={"path": path},
        )

    @blueprint.route("/api/import/orphaned-staging", methods=["DELETE"])
    def api_import_orphaned_staging_delete():
        """Delete old staging only when a fresh verification is fully green."""
        from staging_recovery import delete_verified_staging

        body = request.get_json(silent=True) or {}
        path = body.get("path")
        if not isinstance(path, str) or not path:
            return json_error("path required")
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        db = get_db()
        try:
            result = delete_verified_staging(db, vireo_dir, path)
        except ValueError as exc:
            return json_error(str(exc), status=409)
        return jsonify(result)

    def _prepare_import_workspace(db, body):
        """Return the workspace id an import should write to.

        `new_workspace_name` mirrors the normal workspace creation route
        so import-to-new-workspace jobs get default collections and do not
        inherit stale per-workspace caches from a reused SQLite rowid.
        """
        if "new_workspace_name" not in body:
            return db._active_workspace_id, None, None
        raw_name = body.get("new_workspace_name")
        if not isinstance(raw_name, str):
            return None, None, json_error("new_workspace_name must be a string")
        name = raw_name.strip()
        if not name:
            return None, None, json_error("new_workspace_name is required")
        try:
            from datetime import datetime

            ws_id = db.create_workspace(name)
            invalidate_missing_originals(workspace_ids=[ws_id])
            db.create_default_collections(workspace_id=ws_id)
            db.set_active_workspace(ws_id)
            db.update_workspace(ws_id, last_opened_at=datetime.now().isoformat())
            ws = db.get_workspace(ws_id)
            return ws_id, dict(ws) if ws else {"id": ws_id, "name": name}, None
        except Exception as e:
            return None, None, json_error(str(e))

    def _remote_target_snapshot(remote_archive_config):
        """Freeze the parts of a resolved remote target that decide where
        files land, for retry-time comparison against the parent job.

        Returns None when the current request is not a remote-archive
        import — a retry that swapped from remote to local (or vice
        versa) will already fail the exact-equal comparison against a
        parent snapshot of the opposite shape. See
        ``api_job_import_photos``'s parent_import_job_id check.
        """
        if remote_archive_config is None:
            return None
        target = remote_archive_config["target"]
        return {
            "host": target.get("host", ""),
            "user": target.get("user", ""),
            "port": int(target.get("port") or 22),
            "remote_path": target.get("remote_path", ""),
            "mount_path": target.get("mount_path", ""),
            "subpath": remote_archive_config.get("subpath", ""),
        }

    def _move_target_snapshot(target):
        """Freeze the parts of a chained after_process_move target that
        decide where files land, for retry-time comparison.

        ``local_archive_root`` and ``mount_path`` set the local staging
        →NAS boundary the move sweeps across; ``host``/``user``/
        ``port``/``remote_path`` set where the NAS transfer actually
        lands. All are captured so any Settings edit that would
        redirect the chained move triggers a decline on retry. Returns
        None when no chained-move target is present.
        """
        if target is None:
            return None
        return {
            "id": target.get("id", ""),
            "host": target.get("host", ""),
            "user": target.get("user", ""),
            "port": int(target.get("port") or 22),
            "remote_path": target.get("remote_path", ""),
            "mount_path": target.get("mount_path", ""),
            "local_archive_root": target.get("local_archive_root", ""),
        }

    def _capture_photo_fingerprints_for_ids(db, ids):
        """Return the current fingerprint string for each ID.

        Same shape as ``import_job._capture_photo_fingerprints`` (which
        owns the string format via ``_fingerprint_for_row``) but at
        request time on caller-supplied IDs, so a recovery retry can
        detect when SQLite has reused an ID for an unrelated photo
        since the parent import ran. The fingerprint includes
        ``file_size`` and ``file_hash`` alongside the path, catching
        the case where a delete-then-import put an unrelated file at
        the same path — path alone would then match falsely and the
        retry's after-import chain (and any ``after_process_move``)
        would sweep up the imposter.

        Size and hash come from **the file on disk right now**, not the
        catalog. When a destination file is overwritten at the same
        path between the parent run and this retry without a rescan,
        ``photos.file_size`` / ``photos.file_hash`` still carry the
        parent's values — comparing two copies of the same cached row
        would then admit the changed bytes into the retry's chain and
        NAS-move scope. Reading the file forces a byte-identity check
        that catches a stealth overwrite; a missing or unreadable file
        yields empty size/hash so the fingerprint won't match the
        parent's and the retry fails-closed. Missing IDs are simply
        absent from the result — the caller decides how to treat that.
        """
        from import_job import _fingerprint_for_row
        from scanner import compute_file_hash

        cleaned = []
        for pid in ids or []:
            if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0:
                cleaned.append(pid)
        if not cleaned:
            return {}
        fingerprints = {}
        # SQLite default bound-param cap is 999; 500 stays well under
        # that and mirrors the sibling helper in import_job.
        for start in range(0, len(cleaned), 500):
            chunk = cleaned[start:start + 500]
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                f"""SELECT p.id AS id,
                           f.path AS folder_path,
                           p.filename AS filename
                    FROM photos p
                    JOIN folders f ON f.id = p.folder_id
                    WHERE p.id IN ({placeholders})""",
                list(chunk),
            ).fetchall()
            for row in rows:
                folder_path = row["folder_path"] or ""
                filename = row["filename"] or ""
                if not folder_path or not filename:
                    continue
                file_path = os.path.join(folder_path, filename)
                current_size = None
                current_hash = ""
                # Missing / unreadable file falls through with
                # empty size + hash so the fingerprint won't
                # match the parent's stored value.
                with contextlib.suppress(OSError):
                    current_size = os.path.getsize(file_path)
                if current_size is not None:
                    # Match the scanner's empty-file convention: a zero-byte
                    # file's stored ``file_hash`` is NULL (rendered as
                    # ``h=`` in ``_fingerprint_for_row``), not the SHA-256
                    # of empty content. Hashing it here would produce a
                    # different fingerprint from the parent's and stall
                    # an otherwise-valid retry of an import that had
                    # successfully landed a zero-byte image. See PR #1387
                    # Codex review; scanner.py resets ``file_hash`` on
                    # ``size == 0 and hash == EMPTY_FILE_SHA256``.
                    if current_size == 0:
                        current_hash = ""
                    else:
                        try:
                            current_hash = compute_file_hash(file_path)
                        except OSError:
                            current_hash = ""
                fp = _fingerprint_for_row({
                    "folder_path": folder_path,
                    "filename": filename,
                    "file_size": current_size,
                    "file_hash": current_hash,
                })
                if fp is None:
                    continue
                fingerprints[row["id"]] = fp
        return fingerprints

    def _validate_parent_import_job(parent_id, active_ws, db):
        """Resolve a retry's parent_import_job_id into the scope this
        retry is allowed to inherit.

        Returns ``(parent_config, allowed_ids, allowed_fingerprints,
        parent_source_snapshots, None)`` on success or ``(None, None,
        None, None, error_response)`` when the parent can't be used.
        ``parent_config`` is the parent job's persisted config dict (it
        also carries ``root_import_job_id`` when the parent is itself
        a retry, so the caller can persist a single root pointer
        regardless of how deep the retry chain goes); ``allowed_ids``
        is the set of photo IDs a retry may include in
        ``carry_photo_ids`` — the parent's own imported IDs plus any
        the parent itself inherited from an earlier retry.
        ``allowed_fingerprints`` maps those IDs to the stable
        ``folder_path/filename|size|hash`` recorded at parent-run time,
        so the retry can refuse a carry ID whose current row belongs to
        an unrelated photo that happened to reuse the numeric ID.
        ``parent_source_snapshots`` is the parent's
        ``result["source_snapshots"]`` (``{source_str: {count,
        signature}}``) so the caller can verify the retry's sources
        still hold the same contents the parent enumerated — refusing
        a retry against a different SD card mounted at the same path,
        or a source whose files were edited between runs.

        Cross-workspace parents are refused: photos and folders live
        globally, so a caller who names a parent from another workspace
        would smuggle that workspace's photos into this workspace's
        after-import chain and (with after_process_move) its NAS
        transfer scope. Falls through to job_history when the runner
        has already pruned the finished job.
        """
        runner = get_runner()
        parent = runner.get(parent_id)
        parent_config = None
        parent_result = None
        parent_workspace = None
        parent_type = None
        parent_status = None
        if parent is not None:
            parent_config = parent.get("config") or {}
            parent_result = parent.get("result") or {}
            parent_workspace = parent.get("workspace_id")
            parent_type = parent.get("type")
            parent_status = parent.get("status")
        else:
            row = db.conn.execute(
                "SELECT type, status, workspace_id, config, result "
                "FROM job_history WHERE id = ?",
                (parent_id,),
            ).fetchone()
            if row is None:
                return None, None, None, None, json_error(
                    "parent_import_job_id not found — the original import "
                    "may have aged out of history; start a new import",
                    404,
                )
            parent_type = row["type"]
            parent_status = row["status"]
            parent_workspace = row["workspace_id"]
            try:
                parent_config = json.loads(row["config"] or "{}") or {}
            except (json.JSONDecodeError, TypeError):
                parent_config = {}
            try:
                parent_result = json.loads(row["result"] or "{}") or {}
            except (json.JSONDecodeError, TypeError):
                parent_result = {}
        if parent_type != "import":
            return None, None, None, None, json_error(
                "parent_import_job_id must reference an import job "
                f"(got type {parent_type!r})"
            )
        if parent_status not in {"completed", "failed", "cancelled"}:
            return None, None, None, None, json_error(
                "parent_import_job_id is still active "
                f"(status {parent_status!r}); wait for the original import "
                "to finish before retrying",
                409,
            )
        if parent_workspace != active_ws:
            return None, None, None, None, json_error(
                "parent_import_job_id belongs to a different workspace "
                "than the active one; switch workspaces or start a new "
                "import instead of retrying"
            )
        allowed_ids = set()
        for source in (
            parent_result.get("photo_ids") or [],
            parent_result.get("carried_photo_ids") or [],
            parent_config.get("carry_photo_ids") or [],
        ):
            for pid in source:
                if (
                    isinstance(pid, int)
                    and not isinstance(pid, bool)
                    and pid > 0
                ):
                    allowed_ids.add(pid)
        # Stable-identity map so the retry can verify each carried ID
        # still points at the same file. ``photos.id`` is a bare
        # ``INTEGER PRIMARY KEY`` — SQLite is free to reuse the numeric
        # ID after a delete, so an ID that legitimately named one of the
        # parent's imports can later name an unrelated photo. Merges
        # every fingerprint hop persisted alongside the ID sources
        # above; missing keys just fall through the verify step below
        # (legacy parents from before this fix keep working with the
        # same integer-ID trust).
        allowed_fingerprints = {}
        for source in (
            parent_result.get("photo_fingerprints") or {},
            parent_result.get("carried_photo_fingerprints") or {},
            parent_config.get("carry_photo_fingerprints") or {},
        ):
            if not isinstance(source, dict):
                continue
            for key, value in source.items():
                try:
                    pid = int(key)
                except (TypeError, ValueError):
                    continue
                if pid <= 0 or not isinstance(value, str) or not value:
                    continue
                allowed_fingerprints.setdefault(pid, value)
        parent_source_snapshots = parent_result.get("source_snapshots")
        if not isinstance(parent_source_snapshots, dict):
            parent_source_snapshots = None
        return (
            parent_config,
            allowed_ids,
            allowed_fingerprints,
            parent_source_snapshots,
            None,
        )

    def _validate_after_import(value, db, *, allow_missing=False):
        """Return a JSON error response for a bad after_import spec, else None.

        Shared by both import endpoints: null means import-only; a non-null
        value must be a saved-process id that exists, so chained processing
        can't fail hours later on a dangling id the enqueue step could have
        caught. ``allow_missing`` waives the existence check — used by the
        recovery-retry path when the parent import already captured a
        frozen ``after_import_snapshot`` for this exact id, so a Settings
        delete between the failed run and the retry no longer strands the
        retry outright.
        """
        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool):
            return json_error(
                "after_import must be a process id or null, got "
                f"{type(value).__name__}"
            )
        if not allow_missing and db.get_saved_process(value) is None:
            return json_error(f"unknown process id: {value}")
        return None

    def _validate_after_process_move(
        value, after_import, destination, folder_template,
    ):
        """Validate an after_process_move spec; return (target_snapshot, error).

        ``None`` value → (None, None). Otherwise the value must name a saved
        remote target that has a local_archive_root containing ``destination``,
        and the run must chain a process (the move fires from the process
        job's completion hook — an import-only move is just the Move page).
        The returned target dict is the enqueue-time snapshot: a Settings
        edit mid-chain must not redirect the move (same rationale as
        remote_target_snapshot).
        """
        if value is None:
            return None, None
        if not isinstance(value, dict):
            return None, json_error(
                "after_process_move must be an object or null, got "
                f"{type(value).__name__}")
        raw = value.get("remote_target_id")
        if raw is not None and not isinstance(raw, str):
            return None, json_error(
                "after_process_move.remote_target_id must be a string, got "
                f"{type(raw).__name__}")
        target_id = (raw or "").strip()
        if not target_id:
            return None, json_error(
                "after_process_move.remote_target_id required")
        if after_import is None:
            return None, json_error(
                "after_process_move requires after_import — the move chains "
                "off the processing run; for a move without processing use "
                "the Move page")
        import config as cfg
        target = cfg.get_remote_target(target_id)
        if target is None:
            return None, json_error(f"unknown remote target: {target_id}")
        root = (target.get("local_archive_root") or "").strip()
        if not root:
            return None, json_error(
                "this remote target has no local archive root — set one "
                "under Settings → Remote targets")
        # The move-folder endpoint rejects a missing/relative mount_path
        # (see api_job_move_folder), but only when the move job is created —
        # for a chained run that's after the import and processing have
        # already finished, so the photos would sit in the local archive
        # despite the accepted chain. The move target is snapshotted here,
        # so a Settings edit mid-run can't repair it either. Reject up front
        # alongside the archive-root check.
        mount = (target.get("mount_path") or "").strip()
        if not mount:
            return None, json_error(
                "this remote target has no local mount path — the chained "
                "move would have nowhere to land photos. Set one under "
                "Settings → Remote targets")
        if not os.path.isabs(mount):
            return None, json_error(
                "this remote target's local mount path isn't absolute "
                f"(\"{mount}\") — the chained move would be repointed to a "
                "path relative to the server's working directory and photos "
                "would appear missing. Set an absolute mount path under "
                "Settings → Remote targets")
        # Containment goes through move.py's alias-folding helper, not a raw
        # commonpath: on the default case-insensitive macOS/Windows volumes a
        # destination typed with different casing than the saved root (e.g.
        # "/volumes/photos/…" vs "/Volumes/Photos") is the same directory,
        # and realpath does not fold case on POSIX — a byte compare would
        # falsely reject it.
        from move import _path_equal_or_descends
        dest_real = os.path.realpath(destination)
        root_real = os.path.realpath(root)
        if not _path_equal_or_descends(destination, root):
            return None, json_error(
                "destination is not inside the remote target's local "
                f"archive root ({root})")
        # A local_archive_root broader than the target's mount_path (mount
        # nested inside the root) can put a destination inside BOTH: the
        # import would land straight on the NAS mount, and the chained move
        # would then treat "<mount leaf>/…" as an archive-relative subpath
        # and re-copy the already-on-NAS files under
        # remote_path/<mount leaf>/… — nesting duplicates instead of moving
        # local staging. The chain stages locally and moves TO the mount, so
        # a destination on the mount is never valid for it.
        if _path_equal_or_descends(destination, mount):
            return None, json_error(
                "destination is inside the target's NAS mount "
                f"({mount}) — the import would land directly on the NAS and "
                "the chained move would duplicate it under the remote path; "
                "pick a local archive folder outside the mount")
        # The rendered template can still put the import folder in the same
        # tree as the mount even when ``destination`` itself is safely
        # outside. Two overlapping failure modes:
        #   (a) The render lands AT or UNDER the mount — e.g.
        #       destination=/Users/me/Photos, folder_template="NAS/%Y",
        #       mount=/Users/me/Photos/NAS. The import lands directly on
        #       the NAS mount and the chained move duplicates it under
        #       remote_path.
        #   (b) The render lands ABOVE the mount so the mount ends up
        #       INSIDE the import source tree — e.g. destination=/Photos,
        #       folder_template="%Y", mount=/Photos/2026/07. The %Y render
        #       creates ``/Photos/2026`` as the import folder; the chained
        #       move then computes the NAS-side destination as
        #       ``mount_path/2026`` = ``/Photos/2026/07/2026``, which is
        #       INSIDE the source ``/Photos/2026`` so ``move_folder``
        #       rejects mid-run and the photos sit in the local archive.
        # strftime tokens make the exact render unknowable at request time,
        # but the tokens themselves narrow what strftime can produce —
        # ``%Y`` renders only 4 digits, ``%m`` only 2, and so on. Use
        # ``_strftime_template_can_render`` to ask, per overlap position,
        # whether the template component can actually produce the mount's
        # component: an earlier guard treated every ``%``-bearing component
        # as an unconditional wildcard and rejected the default
        # ``%Y/%Y-%m-%d`` template against a mount leaf like ``NAS`` even
        # though ``%Y`` can never render letters. If every overlap position
        # can render, SOME real strftime output overlaps the mount in one
        # of the two directions above and the request must be rejected.
        # Locale-dependent directives (``%B``, ``%A``, ``%Z``, …) whose
        # renders are truly unknowable fall back to a wildcard pattern, so
        # the guard stays at least as strict as before for those tokens.
        #
        # Normalize the template with ``os.path.normpath`` before splitting
        # so ``.`` components collapse the same way the import path does
        # when it joins the render under ``destination`` — otherwise a
        # template like ``./NAS/%Y`` would raw-split to
        # ``[".", "NAS", "%Y"]`` and the leading ``.`` would misalign with
        # the mount's ``["NAS"]``, letting the guard miss even though the
        # rendered ``./NAS/2026`` lands directly on the mount. ``..`` is
        # already rejected upstream by ``_is_unsafe_path``, so normpath
        # can only collapse ``.``/empties here.
        normalized_template = os.path.normpath(folder_template or ".")
        template_components = [
            c for c in normalized_template.split(os.sep)
            if c and c != "."
        ]
        # Test the template's reach against the mount via
        # ``_path_equal_or_descends`` on a constructed candidate path,
        # not a byte-wise ``os.path.normcase`` compare of the leaf
        # components. On default case-insensitive POSIX volumes (macOS
        # APFS) ``normcase`` is a no-op, so ``normcase("nas") !=
        # normcase("NAS")`` and a template ``nas/%Y`` against mount leaf
        # ``NAS`` slips past the guard — the import then resolves onto
        # the existing NAS alias and the chained move re-copies the
        # on-mount files under ``remote_path/nas/…``. ``samefile`` folds
        # by device+inode on any case-insensitive volume regardless of
        # platform, and ``_path_equal_or_descends`` also carries the
        # case-fold string fallback for the missing-leaves subtree that
        # ``os.path.normcase`` skips on POSIX.
        mount_real = os.path.realpath(mount)
        if _path_equal_or_descends(mount_real, dest_real) \
                and not _path_equal_or_descends(dest_real, mount_real):
            try:
                mount_rel = os.path.relpath(mount_real, dest_real)
            except ValueError:
                mount_rel = ""
            mount_rel_parts = [
                c for c in mount_rel.split(os.sep) if c and c != ".."
            ]
            if mount_rel_parts:
                # For each ``%``-bearing overlap position, ask whether the
                # template component can ACTUALLY produce the mount's
                # component. ``%Y`` renders four digits only, so it cannot
                # equal a letter-only mount leaf like ``NAS``; treating
                # every ``%``-bearing component as an unconditional wildcard
                # (the pre-fix behavior) falsely rejected the default
                # ``%Y/%Y-%m-%d`` template against such mounts. Locale-
                # dependent directives (``%B``, ``%A``, ``%Z``, …) whose
                # renders are truly unknowable fall back to a ``.+`` pattern
                # so the guard stays at least as strict as the wildcard
                # behavior for those tokens. Literal template components
                # are excluded from the renderability filter — filesystem
                # case-alias awareness for those goes through the
                # ``_path_equal_or_descends`` check on the built candidate
                # path below, which honours the volume's real case
                # sensitivity via inode/samefile.
                overlap = min(
                    len(template_components), len(mount_rel_parts))
                all_percent_reachable = all(
                    _strftime_template_can_render(tc, mc)
                    for tc, mc in zip(
                        template_components[:overlap],
                        mount_rel_parts[:overlap],
                        strict=True,
                    )
                    if "%" in tc
                )
                # Substitute the mount's actual component at ``%``-bearing
                # positions — we just verified strftime CAN produce that
                # value there, so the candidate is an honest example
                # render. Keep literals as-is; extend past the mount depth
                # with a placeholder for ``%``-bearing tails so the
                # candidate stays inside the mount subtree for case (a).
                # An empty ``template_components`` (folder_template = "" /
                # ".") produces ``candidate = dest_real``, which the outer
                # condition already says wraps the mount — case (b).
                candidate_parts = [
                    mc if "%" in tc else tc
                    for tc, mc in zip(
                        template_components[:overlap],
                        mount_rel_parts[:overlap],
                        strict=True,
                    )
                ]
                for tc in template_components[overlap:]:
                    candidate_parts.append("x" if "%" in tc else tc)
                candidate = os.path.join(dest_real, *candidate_parts)
                # Reject if the candidate lands AT/UNDER the mount (case
                # a) OR wraps the mount (case b). ``_path_equal_or_descends``
                # is alias-aware in both directions, so literal template
                # components that differ from a mount component only by
                # case on a case-insensitive volume still trigger rejection.
                if all_percent_reachable and (
                        _path_equal_or_descends(candidate, mount_real)
                        or _path_equal_or_descends(mount_real, candidate)):
                    if template_components:
                        detail = (
                            f"the components in \"{folder_template}\" can "
                            f"produce a path matching \"{mount_rel}\" under "
                            "the destination, so some renders would land on "
                            "the NAS or wrap the mount"
                        )
                    else:
                        detail = (
                            f"the folder template (\"{folder_template}\") "
                            "leaves the import at the destination itself, "
                            f"and the mount sits at \"{mount_rel}\" under "
                            "it — the mount ends up inside the import "
                            "source tree"
                        )
                    return None, json_error(
                        "folder_template can render the import into the "
                        f"same tree as the target's NAS mount ({mount}) — "
                        f"{detail} and the chained move would either "
                        "duplicate them under the remote path or be refused "
                        "as a destination inside the source; pick a "
                        "template or destination that stays outside the "
                        "mount")
        dest_is_root = _path_equal_or_descends(root, destination)
        # Root-level import with a folder template that resolves to "." lands
        # photos on the local_archive_root itself. The chained move
        # deliberately skips the root (moving it would sweep unrelated shoots
        # into the transfer), so the chain would accept the request and later
        # silently move nothing. Reject up front instead. Empty and "." both
        # produce a rel of "." in the import job's ``or "."`` fallback.
        template_stripped = (folder_template or "").strip()
        if dest_is_root and template_stripped in ("", "."):
            return None, json_error(
                "after_process_move requires a folder_template when the "
                "destination is the target's local archive root — a template "
                "that resolves to \".\" would land photos on the root itself, "
                "which the chained move deliberately skips")
        if not (dest_real == root_real
                or dest_real.startswith(root_real.rstrip(os.sep) + os.sep)):
            # The destination reaches the root only via an alias (case fold
            # on a case-insensitive volume). The catalog folders this import
            # creates will be spelled like the DESTINATION, and
            # minimal_move_set compares them byte-wise against the snapshot
            # root at chain time — so respell the snapshot root as the
            # destination's own prefix (same component count; realpath has
            # already folded symlinks on both sides, leaving case as the
            # only difference).
            n = len(root_real.rstrip(os.sep).split(os.sep))
            target = dict(target)
            target["local_archive_root"] = os.sep.join(
                dest_real.split(os.sep)[:n])
        return target, None

    def _validate_import_metadata_dependency(body):
        """Require working metadata extraction unless explicitly overridden.

        Import can be repaired later, but proceeding silently loses capture
        dates, GPS, camera data, and date-based archive placement.  Keep an
        advanced escape hatch for unusual recovery workflows while making the
        safe behavior the API default (not merely a client-side convention).
        """
        allow_missing = body.get("allow_missing_exiftool", False)
        if not isinstance(allow_missing, bool):
            return json_error("allow_missing_exiftool must be a boolean")
        if not config["REQUIRE_EXIFTOOL_FOR_IMPORT"] or allow_missing:
            return None

        from metadata import exiftool_status

        status = exiftool_status()
        if status["available"]:
            return None
        return jsonify({
            "error": (
                "ExifTool is required for import so Vireo can preserve "
                "capture dates, GPS, and camera metadata. Repair ExifTool "
                "or explicitly choose Import without metadata in Advanced."
            ),
            "code": "exiftool_required",
            "exiftool": status,
        }), 409

    def _create_import_collection(thread_db, photo_ids):
        """Create the static collection that records one completed import.

        Collection creation belongs to the import itself, not to optional
        after-import processing.  Keeping it separate ensures "Import only"
        runs remain discoverable in Browse while processed imports can reuse
        the exact same scope for their chained pipeline job.
        """
        collection_name = "Import " + datetime.now().strftime("%Y-%m-%d %H:%M")
        collection_id = thread_db.add_collection(
            collection_name,
            json.dumps([{"field": "photo_ids", "value": photo_ids}]),
        )
        return collection_id, collection_name

    def _record_import_collection(result, workspace_id, chain_photo_ids=None):
        """Attach a collection to a complete, successful import result.

        ``chain_photo_ids`` optionally extends the collection scope beyond
        the files newly imported by this run. Recovery-retry imports pass
        the photo IDs earlier attempts already landed so the after-import
        chain processes the complete original scope instead of only the
        newly-recovered files. The carry list is recorded on the result as
        ``carried_photo_ids`` for transparency; ``result["photo_ids"]``
        keeps meaning "files this run imported", so downstream counters
        and retry helpers don't double-count on repeated retries.
        """
        photo_ids = list(result.get("photo_ids") or [])
        seen = set(photo_ids)
        carried = []
        if chain_photo_ids:
            for pid in chain_photo_ids:
                if pid in seen:
                    continue
                seen.add(pid)
                carried.append(pid)
        if carried:
            result["carried_photo_ids"] = carried
        collection_ids = photo_ids + carried
        if (
            not result.get("ok")
            or result.get("cancelled")
            or not collection_ids
        ):
            return None, None
        try:
            thread_db = Database(db_path)
            thread_db.set_active_workspace(workspace_id)
            collection_id, collection_name = _create_import_collection(
                thread_db, collection_ids,
            )
            result["collection_id"] = collection_id
            result["collection_name"] = collection_name
            return thread_db, collection_id
        except Exception as e:
            log.exception("import collection creation failed")
            result["collection_error"] = str(e)
            return None, None

    @blueprint.route("/api/jobs/import-in-place", methods=["POST"])
    def api_job_import_in_place():
        """Import existing folders or a new-images snapshot without copying.

        This is the in-place companion to ``/api/jobs/import-photos``: scan
        selected source folders into the active workspace, leave originals at
        their current paths, and optionally enqueue the same after-import
        processing strategy used by archive-copy imports. Snapshot mode is the
        catalog-admission boundary for files discovered below registered roots:
        it scans only the frozen paths and never promotes their leaf folders to
        additional workspace roots.
        """
        from image_loader import is_excluded_scan_path

        body = request.get_json(silent=True) or {}
        source_snapshot_id = body.get("source_snapshot_id")
        if body.get("local_processing"):
            return json_error("Local processing with temporary storage requires Copy to archive")
        if body.get("defer_nas_transfer"):
            return json_error("Keeping photos local before NAS transfer requires Copy to archive")
        if body.get("after_process_move") is not None:
            return json_error(
                "after_process_move is not supported for import-in-place — "
                "photos stay where they are; use Copy to archive"
            )
        dependency_error = _validate_import_metadata_dependency(body)
        if dependency_error is not None:
            return dependency_error
        sources = body.get("sources")
        if isinstance(sources, str):
            sources = [sources]
        snapshot_paths = None

        if source_snapshot_id is not None:
            if sources:
                return json_error(
                    "source_snapshot_id cannot be combined with sources"
                )
            if "new_workspace_name" in body:
                return json_error(
                    "a new-images snapshot belongs to the active workspace "
                    "and cannot be imported into a new workspace"
                )
            if (
                isinstance(source_snapshot_id, bool)
                or not isinstance(source_snapshot_id, int)
            ):
                return json_error("source_snapshot_id must be an integer")
        else:
            if not sources or not isinstance(sources, list) or not all(
                isinstance(s, str) and s for s in sources
            ):
                return json_error("sources must be a non-empty list of paths")
            for s in sources:
                if is_excluded_scan_path(s):
                    return json_error(
                        f"source is inside a macOS app-managed library and "
                        f"cannot be imported: {s}"
                    )
                if not os.path.isdir(s):
                    return json_error(f"source directory not found: {s}")

        recursive = bool(body.get("recursive", True))
        import_tags, location_from_gps, tag_options_err = (
            _validate_import_tag_options(body)
        )
        if tag_options_err is not None:
            return tag_options_err
        db = get_db()
        if source_snapshot_id is not None:
            snap = db.get_new_images_snapshot(source_snapshot_id)
            if snap is None:
                return json_error(
                    f"source_snapshot_id {source_snapshot_id} not found",
                    status=404,
                )
            snapshot_paths = list(snap["file_paths"])

            # Resolve each frozen path to one of the workspace's registered
            # roots. The snapshot was created from these roots, but validate
            # again at enqueue time so a stale/crafted snapshot can never use
            # Import as a path-admission escape hatch after workspace roots
            # change.
            from new_images import mapped_roots as _mapped_new_image_roots

            registered_roots = sorted(
                (
                    (os.path.normpath(r["path"]), r["path"])
                    for r in _mapped_new_image_roots(
                        db, db._active_workspace_id, include_missing=True,
                    )
                ),
                key=lambda item: len(item[0]),
                reverse=True,
            )

            def _registered_root_for(path):
                candidate = os.path.normpath(path)
                for normalized_root, stored_root in registered_roots:
                    try:
                        if (
                            os.path.commonpath([candidate, normalized_root])
                            == normalized_root
                        ):
                            # Containment uses normalized paths, but scanner's
                            # parent walk is lexical. Preserve the spelling
                            # that produced the frozen snapshot paths so a
                            # registered root containing ".." still meets its
                            # restricted descendants exactly.
                            return stored_root
                    except ValueError:
                        continue
                return None

            snapshot_paths_by_root = {}
            for path in snapshot_paths:
                root = _registered_root_for(path)
                if root is None:
                    return json_error(
                        "new-images snapshot contains a path outside the "
                        f"active workspace's registered folders: {path}"
                    )
                snapshot_paths_by_root.setdefault(root, []).append(path)
            sources = sorted(snapshot_paths_by_root)
        else:
            snapshot_paths_by_root = None
        # Preflight an explicit after_import before creating a workspace so
        # a bad value doesn't leave an orphan Card Import behind. The
        # omitted branch has to wait until AFTER the workspace switch — see
        # below.
        explicit_after_import = "after_import" in body
        if explicit_after_import:
            after_import = body.get("after_import")
            err = _validate_after_import(after_import, db)
            if err is not None:
                return err

        active_ws, created_workspace, workspace_err = (
            _prepare_import_workspace(db, body)
        )
        if workspace_err is not None:
            return workspace_err

        # Resolve the omitted-default AFTER the workspace switch. Reading
        # pipeline.default_process_id off the previously-active workspace
        # would leak that workspace's override into a new-workspace import.
        if not explicit_after_import:
            import config as cfg

            effective_cfg = db.get_effective_config(cfg.load())
            after_import = (
                effective_cfg.get("pipeline", {}).get("default_process_id")
            )
            err = _validate_after_import(after_import, db)
            if err is not None:
                return err

        # Snapshot the chosen saved process's stage flags at enqueue time
        # so a mid-import edit or delete can't silently change (or void)
        # the after-import run the user already accepted. An in-place
        # import can take many minutes on a full card, and until the
        # chain hook fires the pipeline_job's actual toggles are still up
        # for grabs — resolving here freezes them.
        after_import_snapshot = None
        if after_import is not None:
            try:
                after_import_snapshot = db.resolve_process(after_import)
            except ValueError as e:
                return json_error(str(e), 404)

        runner = get_runner()
        thumb_cache_dir = config["THUMB_CACHE_DIR"]
        vireo_dir = os.path.dirname(thumb_cache_dir)
        snapshot_import_lock = None
        if source_snapshot_id is not None:
            snapshot_lock_key = (active_ws, source_snapshot_id)
            with snapshot_import_locks_guard:
                snapshot_import_lock = (
                    snapshot_import_locks.setdefault(
                        snapshot_lock_key, threading.Lock(),
                    )
                )

        def _chain_after_import(job, result):
            photo_ids = result.get("photo_ids") or []

            # A frozen snapshot can still yield existing IDs through the
            # scanner callback when it is replayed. Those IDs make tagging
            # idempotent, but they must not create another import collection
            # or enqueue an expensive Process run: this Import admitted
            # nothing new.
            if (
                result.get("ok")
                and result.get("source_snapshot_id") is not None
                and result.get("imported") == 0
            ):
                result["after_import_skipped"] = (
                    "import-only" if after_import is None else "no new photos"
                )
                return

            carry_photo_ids = list(
                (job.get("config") or {}).get("carry_photo_ids") or []
            )
            thread_db, col_id = _record_import_collection(
                result, active_ws, chain_photo_ids=carry_photo_ids,
            )
            chain_scope = photo_ids + list(
                result.get("carried_photo_ids") or []
            )

            if after_import is None:
                result["after_import_skipped"] = "import-only"
                return
            if not result.get("ok"):
                result["after_import_skipped"] = "import failed"
                return
            if result.get("cancelled"):
                result["after_import_skipped"] = "import cancelled"
                return
            if not chain_scope:
                result["after_import_skipped"] = "no photos"
                return
            if col_id is None:
                result["after_import_skipped"] = (
                    "failed to create import collection"
                )
                return
            try:
                process_job_id, model_warning, process_blocker = enqueue_process_job(
                    thread_db, runner, active_ws,
                    collection_id=col_id,
                    process_id=after_import,
                    chained_from=job["id"],
                    expanded=after_import_snapshot,
                )
                if process_blocker:
                    result["after_import_skipped"] = process_blocker
                    return
                result["process_job_id"] = process_job_id
                if model_warning:
                    result["model_warning"] = model_warning
            except Exception as e:
                log.exception("after-import chaining failed")
                result["after_import_skipped"] = (
                    f"failed to enqueue processing: {e}"
                )

        def _import_in_place(job, thread_db):
            import errno as errno_mod

            import config as cfg
            from ingest import discover_source_files
            from pipeline_job import (
                _archive_mount_baseline,
                _changed_mount_since_baseline,
                _load_known_mount_roots,
                _mount_identity,
                _mount_identity_baseline,
                _record_known_mount_roots,
                _unmounted_since_baseline,
            )
            from scanner import (
                ScanCancelled,
                _extract_working_copies,
                is_excluded_scan_path,
            )
            from scanner import (
                scan as do_scan,
            )

            thread_db.set_active_workspace(active_ws)
            if thread_db.check_folder_health():
                invalidate_missing_originals()
            effective_cfg = thread_db.get_effective_config(cfg.load())
            pipeline_cfg = effective_cfg.get("pipeline", {})

            job["_start_time"] = time.time()
            runner.set_steps(job["id"], [
                {"id": "scan", "label": "Import in place"},
            ])
            runner.update_step(job["id"], "scan", status="running")

            photo_ids = []
            seen_photo_ids = set()
            indexed_paths = set()
            root_errors = []
            scan_acc = {
                "prior": 0,
                "last_current": 0,
                "last_total": 0,
                "overall_total": 0,
                "source_index": 0,
            }
            working_copy_scope = []
            working_copy_scope_baselines = {}
            working_copy_scope_identities = {}
            known_mount_roots = _load_known_mount_roots(thread_db)
            source_mount_baselines = {}
            source_mount_identities = {}
            for source in sources:
                source_key = str(Path(source))
                baseline = _archive_mount_baseline(
                    source, known_mount_roots,
                )
                source_mount_baselines[source_key] = baseline
                identities = _mount_identity_baseline(baseline)
                # Mount roots catch detach/remount; the source directory's
                # own inode also catches a local root renamed and replaced
                # while later sources are still scanning.
                identities[source_key] = _mount_identity(source_key)
                source_mount_identities[source_key] = identities
                _record_known_mount_roots(thread_db, baseline)

            snapshot_requested = len(snapshot_paths or [])
            snapshot_missing = []
            snapshot_unreadable = []
            snapshot_eligible = set(snapshot_paths or [])
            snapshot_known_before = {}

            def active_photo_ids_by_path():
                """Map primary and companion paths to active photo records."""
                rows = thread_db.conn.execute(
                    """SELECT p.id, p.filename, p.companion_path,
                              f.path AS folder_path
                       FROM photos p
                       JOIN folders f ON f.id = p.folder_id
                       JOIN workspace_folders wf ON wf.folder_id = f.id
                       WHERE wf.workspace_id = ?""",
                    (active_ws,),
                ).fetchall()
                result = {}
                for row in rows:
                    result[
                        os.path.join(row["folder_path"], row["filename"])
                    ] = row["id"]
                    if row["companion_path"]:
                        companion_path = row["companion_path"]
                        if not os.path.isabs(companion_path):
                            companion_path = os.path.join(
                                row["folder_path"], companion_path,
                            )
                        result[companion_path] = row["id"]
                return result

            if snapshot_paths is not None:
                # Freeze execution outcomes before scanning. Missing and
                # unreadable files stay visible in the result instead of
                # silently shrinking the banner's promised count.
                snapshot_eligible.clear()
                for path in snapshot_paths:
                    if not os.path.isfile(path):
                        snapshot_missing.append(path)
                        continue
                    try:
                        with open(path, "rb") as fh:
                            fh.read(1)
                    except OSError:
                        snapshot_unreadable.append(path)
                        continue
                    snapshot_eligible.add(path)

                # Record active-workspace membership before the scan so a
                # concurrent/repeated import is reported as idempotent rather
                # than as a newly admitted photo.
                snapshot_known_before = {
                    path: photo_id
                    for path, photo_id in active_photo_ids_by_path().items()
                    if path in snapshot_eligible
                }

            def photo_cb(photo_id, path):
                if photo_id not in seen_photo_ids:
                    seen_photo_ids.add(photo_id)
                    photo_ids.append(photo_id)
                indexed_paths.add(path)
                runner.update_step(
                    job["id"], "scan", current_file=os.path.basename(path),
                )

            def progress_cb(current, total):
                scan_acc["last_current"] = current
                scan_acc["last_total"] = total
                cum_current = scan_acc["prior"] + current
                cum_total = scan_acc["overall_total"]
                job["progress"]["current"] = cum_current
                job["progress"]["total"] = cum_total
                runner.update_step(
                    job["id"], "scan",
                    progress={"current": cum_current, "total": cum_total},
                )
                runner.push_event(job["id"], "progress", {
                    "current": cum_current,
                    "total": cum_total,
                    "current_file": job["progress"].get("current_file", ""),
                    "phase": "Importing in place",
                    # Explicitly clear a completed discovery/metadata phase.
                    # JobRunner mirrors progress by merging keys, so omitting
                    # these would leave the old phase active for poll clients.
                    "phase_current": None,
                    "phase_total": None,
                    "phase_label": None,
                })

            def status_cb(message, phase_current=None, phase_total=None, phase_label=None):
                visible_phase_label = phase_label
                if (
                    phase_label
                    and phase_label != "Generating working copies"
                    and len(sources) > 1
                ):
                    visible_phase_label = (
                        f"{phase_label} — source "
                        f"{scan_acc['source_index']} of {len(sources)}"
                    )
                job["progress"]["current_file"] = message
                step_update = {"current_file": message}
                if (
                    phase_total
                    and phase_label == "Generating working copies"
                ):
                    # The scan counter can already be complete while working
                    # copies are still being generated. Put the active phase
                    # on the expanded Jobs-page step too, rather than leaving
                    # that row pinned at a misleading 100%.
                    step_update["progress"] = {
                        "current": phase_current or 0,
                        "total": phase_total,
                    }
                runner.update_step(job["id"], "scan", **step_update)
                runner.push_event(job["id"], "progress", {
                    "current": job["progress"].get("current", 0),
                    "total": job["progress"].get("total", 0),
                    "current_file": message,
                    "phase": visible_phase_label or message,
                    "phase_current": phase_current,
                    "phase_total": phase_total,
                    "phase_label": visible_phase_label,
                })

            def advance_scan_acc():
                scan_acc["prior"] += scan_acc["last_current"]
                scan_acc["last_current"] = 0
                scan_acc["last_total"] = 0

            def cancel_check():
                return runner.is_cancelled(job["id"])

            def pause_check():
                return runner.pause_requested(job["id"])

            def cancel_only_check():
                return runner.cancellation_requested(job["id"])

            # Discover every source before processing any of them. Previously
            # each scan discovered its source just-in-time, so the UI called a
            # partial denominator "Overall" and then moved backward when the
            # next source added more files. Freezing these manifests makes the
            # total stable and also excludes files that arrive mid-import.
            source_manifests = {}
            # Per-source dict of directory-path → mount identity captured at
            # discovery time. Discovery walks each source and produces a
            # frozen list of files, but only the source root's identity is
            # baselined earlier (source_mount_identities). Later sources can
            # wait minutes behind earlier ones, and in that window a nested
            # child dir or symlink under an already-discovered source may be
            # swapped for an ordinary photo subtree with the same name — a
            # replacement is_excluded_scan_path cannot recognize and the
            # source-root check does not see. Recording an identity per
            # unique parent directory of the manifest lets the scan loop
            # reject the source before scanner.stat()s the frozen filenames
            # under the replacement.
            source_manifest_dir_identities = {}
            source_discovery_failures = set()
            cancelled = False

            def emit_discovery(source_index, source, checked=0, found=0):
                source_name = os.path.basename(os.path.normpath(source)) or source
                message = (
                    f"Discovering source {source_index} of {len(sources)}: "
                    f"{source_name}"
                )
                if checked:
                    message += f" ({found:,} files found)"
                runner.update_step(job["id"], "scan", current_file=message)
                runner.push_event(job["id"], "progress", {
                    "current": 0,
                    "total": 0,
                    "current_file": message,
                    "phase": message,
                    "phase_current": source_index - 1,
                    "phase_total": len(sources),
                    "phase_label": "Discovering sources",
                })

            for source_index, source in enumerate(sources, 1):
                if cancel_check():
                    cancelled = True
                    break
                emit_discovery(source_index, source)
                try:
                    if snapshot_paths_by_root is not None:
                        manifest = sorted(
                            Path(path)
                            for path in snapshot_paths_by_root[source]
                            if path in snapshot_eligible
                        )
                    else:
                        def discovery_onerror(exc):
                            if exc.errno in (errno_mod.EPERM, errno_mod.EACCES):
                                raise exc
                            log.warning(
                                "Import-in-place discovery error at %s: %s",
                                exc.filename, exc,
                            )

                        manifest = discover_source_files(
                            source,
                            file_types="both",
                            recursive=recursive,
                            onerror=discovery_onerror,
                            cancel_check=cancel_check,
                            progress_callback=lambda checked, found, i=source_index,
                            s=source: emit_discovery(i, s, checked, found),
                        )
                except ScanCancelled:
                    cancelled = True
                    break
                except Exception as exc:
                    log.exception(
                        "In-place import discovery failed for source %s", source,
                    )
                    msg = f"[{source}] discovery failed: {exc}"
                    root_errors.append(msg)
                    if msg not in job["errors"]:
                        job["errors"].append(msg)
                    source_discovery_failures.add(source)
                    manifest = []
                source_manifests[source] = manifest
                # Baseline the identity of each unique parent directory in
                # the manifest. Deduplicating first bounds this to the tree
                # depth actually observed, not the file count. Only applied
                # to sources whose manifest came from a filesystem walk here:
                # snapshot-mode manifests take an explicit path list captured
                # earlier by the caller, and their per-directory identity is
                # already captured just before scan by restricted_dir_identities
                # and rechecked post-scan when scoping working-copy extraction.
                # See source_manifest_dir_identities for the full rationale.
                if snapshot_paths_by_root is None:
                    manifest_dir_identities = {}
                    for manifest_path in manifest:
                        parent_key = str(Path(manifest_path).parent)
                        if parent_key in manifest_dir_identities:
                            continue
                        manifest_dir_identities[parent_key] = _mount_identity(
                            parent_key,
                        )
                    source_manifest_dir_identities[source] = manifest_dir_identities
                scan_acc["overall_total"] += len(manifest)
                runner.push_event(job["id"], "progress", {
                    "current": 0,
                    "total": 0,
                    "current_file": (
                        f"Discovered {len(manifest):,} files in source "
                        f"{source_index} of {len(sources)}"
                    ),
                    "phase": "Discovering sources",
                    "phase_current": source_index,
                    "phase_total": len(sources),
                    "phase_label": "Discovering sources",
                })

            # Publish the overall denominator only after every source has
            # contributed. From this event onward it never changes.
            job["progress"]["current"] = 0
            job["progress"]["total"] = scan_acc["overall_total"]
            runner.update_step(
                job["id"], "scan",
                progress={
                    "current": 0,
                    "total": scan_acc["overall_total"],
                },
                current_file="",
            )
            runner.push_event(job["id"], "progress", {
                "current": 0,
                "total": scan_acc["overall_total"],
                "current_file": "",
                "phase": "Importing in place",
                "phase_current": None,
                "phase_total": None,
                "phase_label": None,
            })

            for idx, source in enumerate(sources, 1):
                # Discovery can observe a transient pause request and raise
                # ScanCancelled before the runner settles into its paused
                # state. Keep the local outcome authoritative even if the
                # runner is resumed before this loop checks again; later
                # sources do not have frozen manifests in that case.
                if cancelled or cancel_check():
                    cancelled = True
                    break
                if source in source_discovery_failures:
                    continue
                scan_acc["source_index"] = idx
                scan_acc["last_current"] = 0
                scan_acc["last_total"] = len(source_manifests[source])
                restricted_files = None
                restricted_dirs = None
                restricted_dir_identities = {}
                if snapshot_paths_by_root is not None:
                    restricted_files = {
                        path for path in snapshot_paths_by_root[source]
                        if path in snapshot_eligible
                    }
                    if not restricted_files:
                        # The snapshot may consist entirely of files that
                        # vanished after discovery. No scanner call will run,
                        # but the cached banner count is still stale.
                        try:
                            invalidate_new_images_after_scan(thread_db, source)
                        except Exception:
                            log.exception(
                                "Failed to invalidate new-image cache for %s",
                                source,
                            )
                        continue
                    restricted_dirs = sorted(
                        {os.path.dirname(path) for path in restricted_files}
                    )
                    restricted_dir_identities = {
                        str(Path(directory)): _mount_identity(directory)
                        for directory in restricted_dirs
                    }
                phase = (
                    f"Importing source {idx} of {len(sources)}: {source}"
                    if len(sources) > 1 else "Importing in place"
                )
                runner.update_step(
                    job["id"], "scan",
                    current_file=phase,
                    source_index=idx,
                )
                runner.push_event(job["id"], "progress", {
                    "current": job["progress"].get("current", 0),
                    "total": job["progress"].get("total", 0),
                    "current_file": phase,
                    "phase": phase,
                    "phase_current": None,
                    "phase_total": None,
                    "phase_label": None,
                })
                try:
                    # Revalidate the source's mount identity before replaying
                    # its frozen manifest. Between discovery and scan a
                    # removable/network source (or a local directory) can be
                    # detached and replaced at the same path while later
                    # sources are still being discovered; ``root_path.is_dir()``
                    # inside scanner.scan would still be true, so common camera
                    # filenames such as ``DCIM/.../IMG_0001.JPG`` would be
                    # cataloged from the wrong volume. The post-scan check that
                    # already gates working-copy extraction runs too late to
                    # prevent that catalog contamination — do the check here.
                    changed_source_mount = _changed_mount_since_baseline(
                        source_mount_identities.get(str(Path(source)), {}),
                    )
                    if changed_source_mount is not None:
                        raise FileNotFoundError(
                            errno_mod.ENOENT,
                            (
                                "source mount changed since discovery "
                                f"({changed_source_mount}); refusing to "
                                "replay frozen manifest against replacement "
                                "filesystem"
                            ),
                            source,
                        )
                    # The source-root check above cannot see a nested
                    # directory, mount, or symlink that was replaced with an
                    # ordinary photo subtree after discovery: the root's own
                    # inode stayed the same. Revalidate every parent
                    # directory of the frozen manifest here, so the scanner
                    # cannot stat the frozen filenames under a substituted
                    # subtree and catalog the wrong files.
                    changed_manifest_dir = _changed_mount_since_baseline(
                        source_manifest_dir_identities.get(source, {}),
                    )
                    if changed_manifest_dir is not None:
                        raise FileNotFoundError(
                            errno_mod.ENOENT,
                            (
                                "nested directory changed since discovery "
                                f"({changed_manifest_dir}); refusing to "
                                "replay frozen manifest against replacement "
                                "subtree"
                            ),
                            source,
                        )
                    # scan() commits rows incrementally and can raise after
                    # thousands have landed, so read counts from a sink dict
                    # rather than the return value — the same pattern the
                    # multi-root scan job uses. Frozen manifests widen the
                    # window in which promised files can vanish before their
                    # source is processed; the vanished bucket must be
                    # surfaced as a source failure or a successful import
                    # report would silently follow a partial catalog.
                    source_scan_counts = {}
                    do_scan(
                        source, thread_db,
                        # Photos are cataloged globally. Reuse unchanged
                        # records when linking them into another workspace
                        # instead of rereading metadata and hashes over NAS.
                        incremental=True,
                        repair_missing_metadata=True,
                        progress_callback=progress_cb,
                        extract_full_metadata=pipeline_cfg.get(
                            "extract_full_metadata", True,
                        ),
                        photo_callback=photo_cb,
                        status_callback=status_cb,
                        recursive=recursive,
                        restrict_dirs=restricted_dirs,
                        restrict_files=restricted_files,
                        vireo_dir=vireo_dir,
                        thumb_cache_dir=thumb_cache_dir,
                        cancel_check=cancel_check,
                        pause_check=pause_check,
                        cancel_only_check=cancel_only_check,
                        # Pair companions during each scan, but defer RAW
                        # working-copy generation until every source has been
                        # cataloged. One combined pass gives the UI a truthful
                        # total instead of restarting a 0..N phase per source.
                        skip_working_copies=True,
                        register_restrict_dirs_as_roots=(
                            snapshot_paths_by_root is None
                        ),
                        discovered_files=source_manifests[source],
                        counts=source_scan_counts,
                    )
                    vanished_count = source_scan_counts.get("vanished", 0)
                    if vanished_count:
                        msg = (
                            f"[{source}] {vanished_count} file(s) vanished "
                            "between discovery and scan; import is incomplete"
                        )
                        log.warning(
                            "In-place import: %d file(s) promised by the "
                            "frozen manifest for %s were missing at scan "
                            "time",
                            vanished_count, source,
                        )
                        root_errors.append(msg)
                        if msg not in job["errors"]:
                            job["errors"].append(msg)
                    # Retain extraction scope only after the scan returns and
                    # only while its paths are still valid. A selected volume
                    # can disappear after request validation; handing that
                    # stale scope to the deferred extractor would mark every
                    # pre-existing RAW row as failed for 24 hours.
                    if restricted_dirs is not None:
                        for directory in restricted_dirs:
                            if (
                                not is_excluded_scan_path(Path(directory))
                                and os.path.isdir(directory)
                            ):
                                entry = (directory, "exact")
                                working_copy_scope.append(entry)
                                working_copy_scope_baselines[entry] = (
                                    source_mount_baselines.get(
                                        str(Path(source)), {},
                                    )
                                )
                                entry_identities = dict(
                                    source_mount_identities.get(
                                        str(Path(source)), {},
                                    )
                                )
                                directory_key = str(Path(directory))
                                entry_identities[directory_key] = (
                                    restricted_dir_identities.get(directory_key)
                                )
                                working_copy_scope_identities[entry] = entry_identities
                    elif (
                        not is_excluded_scan_path(Path(source))
                        and os.path.isdir(source)
                    ):
                        # scanner.scan converts its root to Path before it
                        # catalogs folder strings, which removes lexical
                        # trailing separators and ``.`` components. Use that
                        # exact spelling for the deferred SQL scope too.
                        normalized_source = str(Path(source))
                        entry = (
                            (normalized_source, "exact")
                            if not recursive else normalized_source
                        )
                        working_copy_scope.append(entry)
                        working_copy_scope_baselines[entry] = (
                            source_mount_baselines.get(normalized_source, {})
                        )
                        working_copy_scope_identities[entry] = (
                            source_mount_identities.get(normalized_source, {})
                        )
                except Exception as exc:
                    if isinstance(exc, ScanCancelled) and cancel_check():
                        cancelled = True
                        break
                    log.exception("In-place import failed for source %s", source)
                    msg = f"[{source}] {exc}"
                    root_errors.append(msg)
                    if msg not in job["errors"]:
                        job["errors"].append(msg)
                    # Advance the counter past this source's frozen
                    # manifest so the overall denominator is still
                    # reached even when the scan failed — a source that
                    # disconnected after discovery (scanner raises
                    # FileNotFoundError on the missing root) would
                    # otherwise leave the progress bar permanently below
                    # its promised total. scan_acc["last_total"] holds
                    # this source's frozen size; the ``finally`` block
                    # below rolls it into ``prior`` via advance_scan_acc.
                    if scan_acc["last_current"] < scan_acc["last_total"]:
                        scan_acc["last_current"] = scan_acc["last_total"]
                        # Emit a progress event now so the bar visibly
                        # moves past this source instead of only jumping
                        # once a later source's photo_cb re-publishes.
                        # The frozen denominator is preserved; ``current``
                        # advances by the failed source's manifest size.
                        cum_current = (
                            scan_acc["prior"] + scan_acc["last_current"]
                        )
                        cum_total = scan_acc["overall_total"]
                        job["progress"]["current"] = cum_current
                        job["progress"]["total"] = cum_total
                        runner.update_step(
                            job["id"], "scan",
                            progress={"current": cum_current, "total": cum_total},
                        )
                        runner.push_event(job["id"], "progress", {
                            "current": cum_current,
                            "total": cum_total,
                            "current_file": job["progress"].get("current_file", ""),
                            "phase": "Importing in place",
                            "phase_current": None,
                            "phase_total": None,
                            "phase_label": None,
                        })
                finally:
                    try:
                        invalidate_new_images_after_scan(thread_db, source)
                    except Exception as cache_exc:
                        log.exception(
                            "Failed to invalidate new-image cache for %s", source,
                        )
                        msg = (
                            f"[{source}] cache invalidation failed after import: "
                            f"{cache_exc}"
                        )
                        root_errors.append(msg)
                        if msg not in job["errors"]:
                            job["errors"].append(msg)
                    # scanner.scan touches disk and may reconcile ghost rows
                    # (e.g. a user restored an original before running
                    # import-in-place). The pre-scan health-check invalidation
                    # only fires when a folder flips missing/ok, so also drop
                    # the missing-originals cache once the scan itself has
                    # run — even on partial failure, since rows are committed
                    # incrementally.
                    try:
                        invalidate_missing_originals()
                    except Exception:
                        log.exception(
                            "Failed to invalidate missing-originals cache after in-place import scan of %s",
                            source,
                        )
                    advance_scan_acc()

            if not cancelled and working_copy_scope:
                # A removable source can disappear after its scan succeeded
                # but before the aggregate extract pass runs (later sources
                # were still scanning, or a card was pulled between the loop
                # ending and this call). Revalidate every retained scope
                # entry now so the extractor never reads a vanished volume
                # and stamps 24h ``working_copy_failed_at`` markers on its
                # pre-existing catalog rows.
                revalidated_scope = []
                for entry in working_copy_scope:
                    if isinstance(entry, tuple):
                        path = entry[0]
                    else:
                        path = entry
                    try:
                        detached_mount = _unmounted_since_baseline(
                            working_copy_scope_baselines.get(entry, {}),
                        )
                        changed_mount = _changed_mount_since_baseline(
                            working_copy_scope_identities.get(entry, {}),
                        )
                        still_available = (
                            not is_excluded_scan_path(Path(path))
                            and detached_mount is None
                            and changed_mount is None
                            and os.path.isdir(path)
                        )
                    except OSError:
                        still_available = False
                        detached_mount = None
                        changed_mount = None
                    if still_available:
                        revalidated_scope.append(entry)
                    else:
                        log.info(
                            "Skipping deferred working-copy scope %s: no "
                            "longer present, excluded, or mount changed%s",
                            path,
                            (
                                f" ({detached_mount or changed_mount})"
                                if detached_mount or changed_mount
                                else ""
                            ),
                        )
                if revalidated_scope:
                    try:
                        _extract_working_copies(
                            thread_db,
                            vireo_dir,
                            status_callback=status_cb,
                            scope=revalidated_scope,
                            cancel_check=cancel_check,
                        )
                    except Exception as exc:
                        log.exception(
                            "In-place import working-copy generation failed",
                        )
                        msg = f"[working copies] {exc}"
                        root_errors.append(msg)
                        if msg not in job["errors"]:
                            job["errors"].append(msg)

            if snapshot_paths is not None:
                # Pairing can fold a newly scanned JPEG into an existing RAW
                # row and delete the temporary JPEG row after photo_cb saw it.
                # Resolve the frozen paths again so collections, tags, and a
                # chained Process job receive durable catalog IDs rather than
                # a deleted transient ID.
                catalog_ids_after = active_photo_ids_by_path()
                photo_ids = list(dict.fromkeys(
                    catalog_ids_after[path]
                    for path in snapshot_paths
                    if path in indexed_paths and path in catalog_ids_after
                ))
            indexed = len(photo_ids)
            snapshot_unindexed = sorted(
                snapshot_eligible - indexed_paths
            ) if snapshot_paths is not None else []
            if cancelled or cancel_check():
                runner.update_step(
                    job["id"], "scan", status="cancelled",
                    summary=f"{indexed} photos (cancelled)",
                )
                result = {
                    "mode": "in_place",
                    "ok": False,
                    "cancelled": True,
                    "discovered": indexed,
                    "indexed": indexed,
                    "failed": len(root_errors),
                    "errors": root_errors,
                    "photo_ids": photo_ids,
                }
                if snapshot_paths is not None:
                    result.update({
                        "source_snapshot_id": source_snapshot_id,
                        "requested": snapshot_requested,
                        "imported": len(
                            indexed_paths - set(snapshot_known_before)
                        ),
                        "already_cataloged": len(
                            indexed_paths & set(snapshot_known_before)
                        ),
                        "missing": len(snapshot_missing),
                        "missing_paths": snapshot_missing[:100],
                        "unreadable": len(snapshot_unreadable),
                        "unreadable_paths": snapshot_unreadable[:100],
                        "unindexed": len(snapshot_unindexed),
                        "unindexed_paths": snapshot_unindexed[:100],
                    })
                _apply_import_tags(
                    active_ws, photo_ids, import_tags, location_from_gps,
                    result, job=job, runner=runner,
                )
                return result

            metadata_warning = scan_metadata_warning()
            summary = f"{indexed} photos"
            if metadata_warning:
                summary += f" — {metadata_warning}"
            snapshot_failures = (
                len(snapshot_missing)
                + len(snapshot_unreadable)
                + len(snapshot_unindexed)
            )
            all_errors = list(root_errors)
            if snapshot_missing:
                all_errors.append(
                    f"{len(snapshot_missing)} snapshot file"
                    f"{'s were' if len(snapshot_missing) != 1 else ' was'} "
                    "missing at import time"
                )
            if snapshot_unreadable:
                all_errors.append(
                    f"{len(snapshot_unreadable)} snapshot file"
                    f"{'s were' if len(snapshot_unreadable) != 1 else ' was'} "
                    "unreadable at import time"
                )
            if snapshot_unindexed:
                all_errors.append(
                    f"{len(snapshot_unindexed)} available snapshot file"
                    f"{'s were' if len(snapshot_unindexed) != 1 else ' was'} "
                    "not indexed"
                )
            runner.update_step(
                job["id"], "scan",
                status="failed" if all_errors else "completed",
                summary=summary,
                error=all_errors[0] if all_errors else None,
                error_count=len(all_errors) if all_errors else None,
            )
            result = {
                "mode": "in_place",
                "ok": not all_errors,
                "discovered": indexed,
                "indexed": indexed,
                "failed": (
                    snapshot_failures
                    if snapshot_paths is not None else len(root_errors)
                ),
                "errors": all_errors,
                "photo_ids": photo_ids,
            }
            if snapshot_paths is not None:
                result.update({
                    "source_snapshot_id": source_snapshot_id,
                    "requested": snapshot_requested,
                    "imported": len(indexed_paths - set(snapshot_known_before)),
                    "already_cataloged": len(
                        indexed_paths & set(snapshot_known_before)
                    ),
                    "missing": len(snapshot_missing),
                    "missing_paths": snapshot_missing[:100],
                    "unreadable": len(snapshot_unreadable),
                    "unreadable_paths": snapshot_unreadable[:100],
                    "unindexed": len(snapshot_unindexed),
                    "unindexed_paths": snapshot_unindexed[:100],
                })
            _apply_import_tags(
                active_ws, photo_ids, import_tags, location_from_gps, result,
                job=job, runner=runner,
            )
            # Pause before publishing the collection or handing off to a
            # child job. Once the handoff starts, late parent requests must
            # not claim they can stop the independently running child.
            if not runner.begin_uncancellable(job["id"]):
                result["cancelled"] = True
            _chain_after_import(job, result)
            return result

        def _run_import_in_place(job):
            # Scan callbacks form reference cycles that can outlive the job.
            # Release SQLite files on every return/error without waiting for GC.
            with Database(db_path) as thread_db:
                return _import_in_place(job, thread_db)

        def work(job):
            if snapshot_import_lock is None:
                return _run_import_in_place(job)
            with snapshot_import_lock:
                return _run_import_in_place(job)

        job_config = {
            "sources": sources,
            "source_snapshot_id": source_snapshot_id,
            "destination": None,
            "recursive": recursive,
            "after_import": after_import,
            "tags": import_tags,
            "location_from_gps": location_from_gps,
            "allow_missing_exiftool": bool(
                body.get("allow_missing_exiftool", False)
            ),
            "mode": "in_place",
            "workspace_id": active_ws,
            "created_workspace": created_workspace,
        }
        # Snapshot-backed imports serialize on ``snapshot_import_lock`` for
        # the entire worker call. Pausing inside that critical section would
        # sleep while holding the shared lock, so a queued second import for
        # the same snapshot would block on the bare lock acquisition and
        # could not reach any runner checkpoint — cancelling it would have
        # no effect until the first import resumed. Keep this mode
        # non-pausable while the lock is in play; other in-place imports
        # remain pausable.
        job_id = runner.start(
            "import-in-place", work, config=job_config, workspace_id=active_ws,
            pausable=snapshot_import_lock is None,
        )
        response = {"job_id": job_id}
        if created_workspace is not None:
            response["workspace"] = created_workspace
        return jsonify(response)

    @blueprint.route("/api/jobs/import-photos", methods=["POST"])
    def api_job_import_photos():
        """Photo import job: copy card -> archive, hash-verify, catalog
        incrementally (import/process split PR 2).

        Distinct from ``POST /api/jobs/import`` (Lightroom catalog import),
        which keeps its route and shape. No pipeline slot involvement —
        imports are I/O-bound and must not queue behind a GPU run; that
        coupling is exactly what the split removes.
        """
        from image_loader import is_excluded_scan_path
        from ingest import _is_unsafe_path

        body = request.get_json(silent=True) or {}
        dependency_error = _validate_import_metadata_dependency(body)
        if dependency_error is not None:
            return dependency_error

        sources = body.get("sources")
        if isinstance(sources, str):
            sources = [sources]
        if not sources or not isinstance(sources, list) or not all(
            isinstance(s, str) and s for s in sources
        ):
            return json_error("sources must be a non-empty list of paths")
        for s in sources:
            # Pre-stat rejection of other-app bundles: os.path.isdir on a
            # .photoslibrary path itself trips the macOS TCC prompt.
            if is_excluded_scan_path(s):
                return json_error(
                    f"source is inside a macOS app-managed library and "
                    f"cannot be imported: {s}"
                )
            if not os.path.isdir(s):
                return json_error(f"source directory not found: {s}")

        include_paths = body.get("include_paths")
        previewed_count = body.get("previewed_count")
        checked_count = body.get("checked_count")

        # All three travel together or none do. A partial set would fabricate
        # drift figures and then blow up on None inside the job.
        provided = [v is not None
                    for v in (include_paths, previewed_count, checked_count)]
        if any(provided) and not all(provided):
            return json_error(
                "include_paths, previewed_count and checked_count must be "
                "sent together", 400,
            )

        if include_paths is not None:
            if not isinstance(include_paths, list) or not include_paths:
                return json_error("include_paths must be a non-empty list", 400)
            if any(not isinstance(p, str) or not p for p in include_paths):
                return json_error(
                    "include_paths must contain non-empty strings", 400,
                )
            # bool is a subclass of int — {"previewed_count": true} would
            # otherwise sail through as 1.
            for name, val in (("previewed_count", previewed_count),
                              ("checked_count", checked_count)):
                if type(val) is not int or val < 0:
                    return json_error(
                        f"{name} must be a non-negative integer", 400,
                    )
            include_paths = set(include_paths)
            if len(include_paths) > previewed_count:
                return json_error(
                    "include_paths cannot exceed previewed_count", 400,
                )
            if checked_count > len(include_paths):
                return json_error(
                    "checked_count cannot exceed include_paths", 400,
                )

            # Containment is LEXICAL by design. normpath catches the real
            # threat (a client naming files outside the chosen folders:
            # "/src/../etc/passwd" collapses and fails). It deliberately does
            # not resolve symlinks — discover_source_files returns symlinked
            # files inside a source and ingest() copies them today, so
            # realpath here would newly reject working imports. Do not
            # "harden" this without reading the spec's §4.
            norm_sources = [os.path.normpath(s) for s in sources]
            for p in include_paths:
                np = os.path.normpath(p)
                try:
                    ok = any(os.path.commonpath([np, s]) == s
                             for s in norm_sources)
                except ValueError:
                    # Mixed absolute/relative — a containment failure, not a 500.
                    ok = False
                if not ok:
                    return json_error(
                        f"include_paths contains a path outside the selected "
                        f"source folders: {p}", 400,
                    )

        # Remote (SSH) archive destination — mirrors the pipeline route's
        # remote-target request shape (remote_target_id + subpath). The card
        # is rsynced to remote_path/subpath and cataloged at
        # mount_path/subpath; ``destination`` is set to the resolved local
        # mount path so every downstream guard (destination-inside-source,
        # scan, catalog) applies to the mount exactly as for a local import.
        remote_target_id = (body.get("remote_target_id") or "").strip()
        remote_subpath = body.get("remote_subpath", "")
        if remote_subpath and not isinstance(remote_subpath, str):
            return json_error("remote_subpath must be a string")
        destination = body.get("destination")
        remote_archive_config = None
        if remote_target_id and destination:
            return json_error(
                "destination and remote_target_id are mutually exclusive — "
                "pick a local archive path or a saved remote target, not both"
            )
        if remote_subpath and not remote_target_id:
            return json_error("remote_subpath requires remote_target_id")
        after_process_move = body.get("after_process_move")
        local_processing = body.get("local_processing", False)
        if not isinstance(local_processing, bool):
            return json_error("local_processing must be a boolean")
        defer_nas_transfer = body.get("defer_nas_transfer", False)
        if not isinstance(defer_nas_transfer, bool):
            return json_error("defer_nas_transfer must be a boolean")
        if defer_nas_transfer and not local_processing:
            return json_error("Keeping photos local until review requires local processing")
        if local_processing and after_process_move is not None:
            return json_error("Choose automatic local processing or a local archive move, not both")
        if after_process_move is not None and remote_target_id:
            return json_error(
                "after_process_move requires a local archive destination — "
                "a remote-destination import already lands on the NAS"
            )
        if remote_target_id:
            # Refuse at request time when no GNU rsync exists or the
            # target is unknown/unsafe — starting a job guaranteed to
            # fail its transfer helps nobody (mirrors the pipeline and
            # move-folder endpoints).
            remote_archive_config, rsync_bin, err = (
                resolve_remote_archive_target(
                    get_db(), remote_target_id, remote_subpath,
                    json_error=json_error,
                )
            )
            if err is not None:
                return err
            # Catalog at the resolved local mount path.
            destination = remote_archive_config["mount_final"]

        if not destination:
            return json_error("destination required")
        if not os.path.isabs(destination):
            return json_error("destination must be an absolute path")

        # Reject destinations that are equal to, or nested under, any source
        # (after realpath so a symlink can't slip past). The importer copies
        # every card file into the destination and marks the card safe to
        # format once ``copied + skipped_duplicate == discovered``; if the
        # destination lives inside the card, formatting the card also erases
        # the supposed archive copy, so allowing this is a data-loss trap.
        # Case-fold handling (darwin/win32 unconditional, Linux per-mount
        # probe) lives in path_guard — see its module docstring and the
        # fs_is_case_insensitive() docstring for the full rationale
        # (PR #1107 review).
        try:
            dest_real = os.path.realpath(destination)
        except OSError as e:
            return json_error(f"destination cannot be resolved: {e}")
        for s in sources:
            try:
                source_real = os.path.realpath(s)
            except OSError:
                # Source unresolvable — the os.path.isdir check above
                # already handled non-existent sources; nothing more to say.
                continue
            if path_guard.contains_resolved(source_real, dest_real):
                return json_error(
                    f"destination cannot be inside a source directory "
                    f"(destination={destination!r}, source={s!r}); "
                    f"formatting the card would erase the archive copy"
                )

        folder_template = body.get("folder_template", "%Y/%Y-%m-%d")
        if folder_template and _is_unsafe_path(folder_template):
            return json_error(
                "folder_template must be a relative path without '..' or "
                "backslashes"
            )

        db = get_db()
        # After-import strategy: validated at enqueue (failing the chain
        # hours later is the old pipeline's mistake). Key present -> null
        # means import-only, a string must name a real strategy. Key
        # omitted -> default from the workspace's pipeline.default_process_id
        # (nullable, same vocabulary). Stored in the job config for the
        # PR 3 chaining hook; the import job itself never reads it.
        # Resolve and validate BEFORE creating any new workspace so a bad
        # value doesn't leave an orphan Archive Import behind. When the
        # request omits the key and asks to create a new workspace, we can't
        # read the workspace-scoped default (the workspace doesn't exist yet,
        # and reading get_effective_config off the previously-active
        # workspace would leak its override into the new-workspace import).
        # A brand-new workspace has no config_overrides, so the effective
        # default is just the global config value — read that directly.
        import config as cfg

        explicit_after_import = "after_import" in body
        if explicit_after_import:
            after_import = body.get("after_import")
        elif "new_workspace_name" in body:
            after_import = (
                cfg.load().get("pipeline", {}).get("default_process_id")
            )
        else:
            effective_cfg = db.get_effective_config(cfg.load())
            after_import = (
                effective_cfg.get("pipeline", {}).get("default_process_id")
            )
        # Recovery-retry imports carry forward the photo IDs a previous
        # attempt already landed, so the after-import chain covers the
        # complete original scope even though those files are skipped as
        # duplicates in this run. Validated at request time so a
        # malformed retry body is rejected before a job is created.
        #
        # ``parent_import_job_id`` binds this retry to the failed run
        # whose scope it inherits: the server verifies the parent is an
        # import job in the same workspace, then constrains
        # ``carry_photo_ids`` to IDs the parent actually imported (or
        # itself inherited from an earlier retry). Without this binding
        # an API caller could inject arbitrary positive integers into
        # the after-import chain scope — those IDs would then flow into
        # ``_record_import_collection``'s scope and, with an
        # ``after_process_move`` chain, into the folder lookup that
        # decides which folders the NAS transfer sweeps up, potentially
        # moving folders outside the active workspace. Refuse the
        # request rather than silently permit it.
        #
        # Resolved BEFORE ``_validate_after_import`` so a retry whose saved
        # process was deleted between the failed run and this retry can
        # still start: the parent's frozen ``after_import_snapshot`` is a
        # legitimate substitute for the deleted process's stage flags, and
        # rejecting the retry with "unknown process id" here would strand
        # every deleted-process retry outright.
        parent_id_raw = body.get("parent_import_job_id")
        parent_config = None
        parent_allowed_ids = None
        parent_allowed_fingerprints = None
        parent_source_snapshots = None
        if parent_id_raw is not None:
            if not isinstance(parent_id_raw, str) or not parent_id_raw.strip():
                return json_error(
                    "parent_import_job_id must be a non-empty string"
                )
            if "new_workspace_name" in body:
                return json_error(
                    "A recovery retry cannot create a new workspace; "
                    "retry in the original import's workspace or start "
                    "a new import instead."
                )
            (
                parent_config,
                parent_allowed_ids,
                parent_allowed_fingerprints,
                parent_source_snapshots,
                parent_err,
            ) = _validate_parent_import_job(
                parent_id_raw.strip(), db._active_workspace_id, db,
            )
            if parent_err is not None:
                return parent_err

        # Skip the "process still exists" check ONLY when the parent has a
        # frozen snapshot for THIS exact process id — that snapshot will
        # substitute for db.resolve_process() below. A retry that changed
        # the process id must still go through normal existence validation.
        parent_has_snapshot_for_after_import = (
            parent_config is not None
            and parent_config.get("after_import") == after_import
            and parent_config.get("after_import_snapshot") is not None
        )
        err = _validate_after_import(
            after_import, db,
            allow_missing=parent_has_snapshot_for_after_import,
        )
        if err is not None:
            return err

        file_types = body.get("file_types", "both")
        skip_duplicates = bool(body.get("skip_duplicates", True))
        verify_by_hash = bool(body.get("verify_by_hash", False))
        trust_likely_duplicates = bool(
            body.get("trust_likely_duplicates", False)
        ) and not verify_by_hash
        recursive = bool(body.get("recursive", True))
        import_tags, location_from_gps, tag_options_err = (
            _validate_import_tag_options(body)
        )
        if tag_options_err is not None:
            return tag_options_err

        carry_raw = body.get("carry_photo_ids")
        if carry_raw is None:
            carry_photo_ids = None
        else:
            if not isinstance(carry_raw, list) or not all(
                isinstance(x, int) and not isinstance(x, bool) and x > 0
                for x in carry_raw
            ):
                return json_error(
                    "carry_photo_ids must be a list of positive integers"
                )
            # A caller-supplied carry list must be bound to a real
            # parent import job. See parent_import_job_id above.
            if parent_allowed_ids is None:
                return json_error(
                    "carry_photo_ids requires parent_import_job_id "
                    "(the failed import this retry inherits scope from)"
                )
            invalid = [pid for pid in carry_raw if pid not in parent_allowed_ids]
            if invalid:
                return json_error(
                    "carry_photo_ids contains IDs the parent import did "
                    f"not land or inherit: {invalid[:5]}"
                )
            # Stable-identity re-check for each carried ID. ``photos.id``
            # is a bare ``INTEGER PRIMARY KEY`` — SQLite reuses freed IDs
            # on the next insert, so an ID that legitimately named one of
            # the parent's imports at parent-run time can, after a delete,
            # name an unrelated photo by retry-run time. Compare each
            # carried ID's CURRENT ``folder_path/filename`` against the
            # fingerprint recorded when the parent landed it; refuse a
            # mismatch rather than letting the after-import chain (and
            # any ``after_process_move``) sweep up the unrelated row.
            # Parents from before this fix carry no fingerprints at all
            # (empty dict), and are exempted — hard-rejecting every
            # legacy failed job's retry would be a bigger regression than
            # the narrow race window this protects.
            if parent_allowed_fingerprints:
                current_fingerprints = _capture_photo_fingerprints_for_ids(
                    db, carry_raw,
                )
                stale = []
                for pid in carry_raw:
                    expected = parent_allowed_fingerprints.get(pid)
                    if expected is None:
                        # Parent tracked fingerprints for some IDs but not
                        # this one — an ID the parent's ``allowed_ids``
                        # blessed but never fingerprinted (e.g. inherited
                        # from a legacy grandparent). Accept, matching
                        # the same rationale as the empty-dict case above.
                        continue
                    current = current_fingerprints.get(pid)
                    if current != expected:
                        stale.append(pid)
                if stale:
                    return json_error(
                        "carry_photo_ids no longer matches the files the "
                        "parent import landed (the numeric IDs now point "
                        "at different photos, likely because the parent's "
                        f"photos were deleted and re-imported): {stale[:5]}"
                    )
            # Preserve caller order but deduplicate — the chain does not
            # need repeats and a giant duplicated list wastes work.
            seen_carry = set()
            carry_photo_ids = []
            for pid in carry_raw:
                if pid in seen_carry:
                    continue
                seen_carry.add(pid)
                carry_photo_ids.append(pid)

        # A recovery retry must not silently import files the parent
        # never saw. Without this guard a retry against a source whose
        # contents changed since the failed run — a different SD card
        # mounted at the same path, or new photos added to the same
        # card — would silently enumerate every current file and copy
        # the newly-appeared ones, then flow their IDs into the
        # carried processing / NAS-move scope. The button says "Retry
        # failed files", not "Import whatever's at this path now".
        #
        # Each parent source's ``count`` + ``signature`` (sha256 over
        # the sorted ``(rel_path, size, mtime_ns)`` list) is captured
        # at parent DISCOVERY time in
        # ``import_job._capture_source_snapshots`` and persisted on
        # ``result["source_snapshots"]``. ``mtime_ns`` is in the tuple
        # so a same-size in-place replacement doesn't slip past the
        # size check; discovery-time capture keeps a card ejected
        # mid-copy from stamping ``-1`` sizes that would refuse a
        # legitimate reinsert-and-retry recovery. Here we recompute
        # the current signature for every retry source that shares a
        # path with a parent source and refuse the retry when any
        # signature has drifted. Legacy parents from before this fix
        # have no snapshots and fall through unchanged.
        if parent_source_snapshots:
            from import_job import _capture_source_snapshots
            from ingest import discover_source_files

            # Fail-closed on retry sources the parent never enumerated:
            # skipping validation for unknown paths would let a retry
            # import every file at that path (no prior catalog entry
            # gates them via ``skip_duplicates``) and stream those IDs
            # into the after-import chain / NAS-move scope, even though
            # the "Retry failed files" button only ever promised the
            # parent's failed files. See PR #1387 Codex review.
            parent_sources = {
                src for src, snapshot in parent_source_snapshots.items()
                if isinstance(snapshot, dict) and snapshot
            }
            requested_sources = set(sources)
            unknown_sources = sorted(requested_sources - parent_sources)
            if unknown_sources:
                unknown_source_text = ", ".join(unknown_sources[:3])
                return json_error(
                    "Retry submitted source paths the original import "
                    "never enumerated (a different card mounted at the "
                    "same path, or a new source added): "
                    f"{unknown_source_text}. Start a new import instead "
                    "of retrying."
                )
            missing_sources = sorted(parent_sources - requested_sources)
            if missing_sources:
                missing_source_text = ", ".join(missing_sources[:3])
                return json_error(
                    "A recovery retry must include every source from the "
                    "original import. These sources are missing: "
                    f"{missing_source_text}. Reconnect all original "
                    "sources, or start a new import instead of retrying."
                )

            drifted_sources = []
            for src in sources:
                parent_snap = parent_source_snapshots.get(src)
                # Reuse the same discovery + snapshot helpers the parent
                # ran through so a mismatch here reflects a genuine
                # source change, not a difference in enumeration logic.
                try:
                    current_files = discover_source_files(
                        src, file_types, recursive=recursive,
                    )
                except Exception:
                    log.exception(
                        "Failed to re-enumerate source for retry snapshot "
                        "check: %s", src,
                    )
                    drifted_sources.append(src)
                    continue
                current_snap = _capture_source_snapshots(
                    current_files, [src],
                ).get(src) or {}
                if current_snap.get("signature") != parent_snap.get(
                    "signature",
                ):
                    drifted_sources.append(src)
            if drifted_sources:
                return json_error(
                    "The source contents have changed since the original "
                    "import (a different SD card at the same path, or new "
                    "or missing files). Retrying would import files the "
                    "original run never saw. Verify the source, then "
                    "start a new import instead of retrying: "
                    f"{drifted_sources[:3]}"
                )

        # A retry that keeps the same remote target must land on the
        # same host/root/mount as the failed run — otherwise the retry
        # would copy the remaining files to a different NAS, or (if
        # only the mount changed) skip prior successes based on the
        # old catalog view while transferring failures to a new
        # location. When the parent recorded a remote_target_snapshot,
        # verify that the current resolution matches; refuse the retry
        # if the target has been edited since. The Import-page and
        # Jobs-page retry helpers both send parent_import_job_id, so
        # both go through this check.
        if parent_config is not None:
            parent_snapshot = parent_config.get("remote_target_snapshot")
            parent_remote_target_id = parent_config.get("remote_target_id")
            if parent_snapshot is not None:
                current_snapshot = _remote_target_snapshot(
                    remote_archive_config,
                )
                if current_snapshot != parent_snapshot:
                    return json_error(
                        "The remote target for the original import has "
                        "changed (host, path, mount, or subpath differs). "
                        "Verify Settings → Remote targets and start a new "
                        "import instead of retrying."
                    )
            elif parent_remote_target_id:
                # Parent job was a remote import from before the
                # snapshot check landed, so its persisted config
                # records only the target ID. Both retry helpers
                # reconstruct the request from that ID, and
                # ``_resolve_remote_archive_target`` then walks the
                # CURRENT Settings entry — a Settings edit since the
                # original run would silently redirect the transfer
                # to a different host/root/mount. Refuse the retry
                # instead of trusting the current resolution.
                return json_error(
                    "The original remote import predates the recovery-"
                    "retry safety check (no remote-target snapshot was "
                    "recorded). Verify Settings → Remote targets still "
                    "point at the intended host, then start a new "
                    "import instead of retrying."
                )

        # Snapshot the chosen saved process's stage flags at enqueue time
        # so a mid-import edit or delete can't silently change (or void)
        # the after-import run the user already accepted. An archive-copy
        # import from a full card can take many minutes, and until the
        # chain hook fires the pipeline_job's actual toggles are still up
        # for grabs — resolving here freezes them (mirrors the remote-
        # transport snapshot below).
        #
        # A recovery retry must inherit the parent's frozen snapshot when
        # the retry still points at the same process id: the original
        # enqueue already froze the stages the user accepted, and
        # resolving again would silently pick up any Settings edit made
        # after the failure (or fail outright if the process was
        # deleted). Re-resolve only when the retry deliberately switches
        # to a different process id — then the user is asking for
        # whatever that process currently is.
        after_import_snapshot = None
        if after_import is not None:
            reused_parent_snapshot = None
            if parent_config is not None:
                parent_after_import = parent_config.get("after_import")
                parent_snapshot_process = (
                    parent_config.get("after_import_snapshot")
                )
                if (
                    parent_snapshot_process is not None
                    and parent_after_import == after_import
                ):
                    reused_parent_snapshot = parent_snapshot_process
            if reused_parent_snapshot is not None:
                after_import_snapshot = reused_parent_snapshot
            else:
                try:
                    after_import_snapshot = db.resolve_process(after_import)
                except ValueError as e:
                    return json_error(str(e), 404)

        # Validate the optional NAS move that chains after processing
        # completes. Requires after_import (the move fires from the process
        # job's completion hook) and a destination inside the target's
        # local archive root; snapshotted now so a mid-chain Settings edit
        # can't redirect the move. Runs before workspace creation so a bad
        # target/root/destination doesn't leave an orphan workspace behind.
        managed_staging = None
        parent_staging = (parent_config or {}).get("managed_staging")
        if parent_config is not None and bool(parent_config.get("defer_nas_transfer")) != defer_nas_transfer:
            return json_error("A recovery retry must preserve the original NAS transfer timing")
        if bool(parent_staging) != local_processing and parent_config is not None:
            return json_error("A recovery retry must preserve the original local processing choice")
        if local_processing:
            if after_import is None:
                return json_error("Local processing requires an after-import process")
            from import_staging import plan_staged_import
            from pipeline_job import _load_known_mount_roots
            try:
                managed_staging, move_target_snapshot = plan_staged_import(
                    os.path.dirname(config["THUMB_CACHE_DIR"]), destination,
                    remote_archive_config, parent_staging, _load_known_mount_roots(db),
                )
                staging_real = os.path.realpath(managed_staging["destination"])
                if any(path_guard.contains_resolved(os.path.realpath(s), staging_real) for s in sources):
                    return json_error("Temporary processing storage cannot be inside a source directory")
            except (ValueError, OSError, RuntimeError) as e:
                return json_error(str(e))
        else:
            move_target_snapshot, move_err = _validate_after_process_move(
                after_process_move, after_import, destination, folder_template,
            )
            if move_err is not None:
                return move_err

        # A retry with a chained NAS move must land on the same
        # host/root/mount as the parent. When the parent recorded a
        # ``target_snapshot`` for the chained target, verify the
        # currently-resolved target matches — a Settings edit since
        # enqueue-time could otherwise silently redirect the chained
        # transfer even though the primary snapshot check above
        # accepted the request. When the parent had a chained move but
        # no snapshot (predates this check), refuse rather than trust
        # the current resolution — same reasoning as the primary
        # remote-target legacy check.
        if parent_config is not None:
            parent_move_cfg = parent_config.get("after_process_move") or {}
            parent_move_snapshot = parent_move_cfg.get("target_snapshot")
            parent_move_target_id = parent_move_cfg.get("remote_target_id")
            if parent_move_snapshot is not None:
                current_move_snapshot = _move_target_snapshot(
                    move_target_snapshot,
                )
                if current_move_snapshot != parent_move_snapshot:
                    return json_error(
                        "The chained NAS-move target for the original "
                        "import has changed (host, path, mount, or archive "
                        "root differs). Verify Settings → Remote targets "
                        "and start a new import instead of retrying."
                    )
            elif parent_move_target_id:
                return json_error(
                    "The original import's chained NAS-move target "
                    "predates the recovery-retry safety check (no target "
                    "snapshot was recorded). Verify Settings → Remote "
                    "targets still point at the intended host, then start "
                    "a new import instead of retrying."
                )

        pending_archive_id = (
            os.path.basename(move_target_snapshot["managed_staging_root"]) if defer_nas_transfer else None
        )
        if pending_archive_id:
            completed = db.conn.execute(
                "SELECT 1 FROM pending_archives WHERE id = ? AND state = 'complete'", (pending_archive_id,),
            ).fetchone()
            if completed:
                return json_error("These photos have already been sent to NAS. Start a new import instead of retrying.")

        active_ws, created_workspace, workspace_err = (
            _prepare_import_workspace(db, body)
        )
        if workspace_err is not None:
            return workspace_err

        runner = get_runner()
        thumb_cache_dir = config["THUMB_CACHE_DIR"]
        vireo_dir = os.path.dirname(thumb_cache_dir)

        # Snapshot the resolved remote transport at enqueue time so a
        # settings edit between click-Start and job-run can't redirect the
        # archive to a different host/mount than the panel is showing
        # (mirrors the pipeline route's remote_target_snapshot).
        remote_target = None
        if remote_archive_config is not None and not local_processing:
            import move as move_mod

            spec = move_mod.build_remote_move_spec(
                remote_archive_config["target"],
                remote_archive_config["subpath"],
                rsync_bin,
            )
            remote_target = {
                "rsync_bin": rsync_bin,
                "remote": spec,
                "ssh_base": remote_archive_config["ssh_final"],
                "mount_base": remote_archive_config["mount_final"],
            }

        job_config = {
            "sources": sources,
            "destination": destination,
            "local_processing": local_processing,
            "managed_staging": managed_staging,
            "defer_nas_transfer": defer_nas_transfer,
            "pending_archive_id": pending_archive_id,
            "folder_template": folder_template,
            "file_types": file_types,
            "skip_duplicates": skip_duplicates,
            "verify_by_hash": verify_by_hash,
            "trust_likely_duplicates": trust_likely_duplicates,
            "recursive": recursive,
            "after_import": after_import,
            # Persist the enqueue-time snapshot alongside the process id so a
            # recovery retry can reuse the exact stages the user accepted.
            # Without this a retry silently re-resolves the current process
            # (an edit or delete between the failed run and the retry would
            # otherwise change or void what runs); see the reuse block above
            # and the corresponding remote-target snapshot handling.
            "after_import_snapshot": after_import_snapshot,
            "tags": import_tags,
            "location_from_gps": location_from_gps,
            "allow_missing_exiftool": bool(
                body.get("allow_missing_exiftool", False)
            ),
            "remote_target_id": remote_target_id or None,
            "remote_subpath": remote_subpath or None,
            "remote_target_snapshot": _remote_target_snapshot(
                remote_archive_config,
            ),
            "workspace_id": active_ws,
            "created_workspace": created_workspace,
            "carry_photo_ids": carry_photo_ids,
            # Fingerprint sidecar to carry_photo_ids so a retry-of-retry
            # can still verify the inherited scope by stable identity
            # even after the grandparent's job has aged out of history.
            # Keyed by str(id) for JSON round-tripping; empty for
            # first-attempt imports or legacy parents with no
            # fingerprints of their own.
            "carry_photo_fingerprints": (
                {
                    str(pid): parent_allowed_fingerprints[pid]
                    for pid in (carry_photo_ids or [])
                    if parent_allowed_fingerprints
                    and pid in parent_allowed_fingerprints
                }
                if parent_allowed_fingerprints
                else {}
            ),
            # Persist the parent's id so the Jobs page's parallel-retry
            # gate (``hasActiveRetryFor``) can recognize this retry as
            # belonging to that parent and suppress a second Retry button
            # click while this run is still in flight. Without the field
            # on ``job_config`` the gate reads ``undefined`` on every
            # active job, so reselecting the failed parent renders a
            # live Retry button that would race the in-flight retry.
            "parent_import_job_id": (
                parent_id_raw.strip() if parent_id_raw else None
            ),
            # Root of the retry chain: the original failed import that
            # every retry (and retry-of-retry) descends from. Persisted
            # separately from ``parent_import_job_id`` so the Jobs page
            # can gate parallel launches on the original selection even
            # when the active job's direct parent is another retry.
            # Without this a retry-of-retry's ``parent_import_job_id``
            # points at the first retry, so reselecting the original
            # failed import still renders a live Retry button that would
            # race the in-flight retry. Inherits the parent's root when
            # the parent already carries one; otherwise the parent IS
            # the root of this chain.
            "root_import_job_id": (
                (parent_config.get("root_import_job_id")
                 or parent_id_raw.strip())
                if parent_config is not None and parent_id_raw
                else None
            ),
        }
        if include_paths is not None:
            job_config["previewed_count"] = previewed_count
            job_config["checked_count"] = checked_count
            # Persist the actual path list so a recovery retry can
            # reconstruct the original selection. Without this a
            # ``retryBodyFromFinishedJob``-driven retry would either be
            # rejected by the source-signature drift check (parent
            # snapshot is now over the pre-selection set — see
            # ``_capture_source_snapshots`` — but the ``include_paths``
            # the parent actually ran is the useful thing to compare
            # against a retry that also filters) or, once that hurdle is
            # cleared, silently re-import the files the user deliberately
            # deselected. Stored as a sorted list so the JSON round-trips
            # deterministically; the set is rebuilt in ``_apply_selection``.
            # Size cost is bounded by ``previewed_count`` (thousands of
            # short path strings in the worst realistic case) and is
            # accepted deliberately as the price of a retry that stays
            # true to the parent's scope.
            job_config["include_paths"] = sorted(include_paths)
        if move_target_snapshot is not None and not local_processing:
            job_config["after_process_move"] = {
                "remote_target_id": move_target_snapshot["id"],
                "target_name": move_target_snapshot["name"],
                # Persisted alongside the id so a recovery retry can
                # detect a Settings edit that would redirect the
                # chained transfer to a different host or root.
                # Parallels ``remote_target_snapshot`` for the primary
                # remote destination. See the parent-verification
                # block above.
                "target_snapshot": _move_target_snapshot(
                    move_target_snapshot,
                ),
            }

        def _chain_after_import(job, result):
            """Create the import collection and optionally enqueue processing.

            Every skip is written to the result as ``after_import_skipped``
            so the jobs panel shows exactly why processing did not run.  The
            collection is independent: every successful import with new
            photos gets one, including the import-only choice.
            """
            photo_ids = result.get("photo_ids") or []
            carry_photo_ids = list(
                (job.get("config") or {}).get("carry_photo_ids") or []
            )
            thread_db, col_id = _record_import_collection(
                result, active_ws, chain_photo_ids=carry_photo_ids,
            )
            if pending_archive_id:
                if thread_db is not None:
                    thread_db.conn.execute(
                        "UPDATE pending_archives SET collection_id = COALESCE(?, collection_id) WHERE id = ?",
                        (col_id, pending_archive_id),
                    )
                    thread_db.conn.commit()
                result["nas_transfer_deferred"] = True
                result["pending_archive_id"] = pending_archive_id
            # Recovery-retry imports may carry forward files earlier
            # attempts landed. The original failed run skipped its
            # after-import chain because ``ok`` was False, so those files
            # were never processed. Roll them into the chain scope now so
            # the collection AND the folder-based after-move both cover
            # the complete original import, not just the newly-recovered
            # files. ``carried_photo_ids`` on the result is the validated
            # subset actually included, so a stale ID never leaks into
            # this scope.
            chain_scope = photo_ids + list(
                result.get("carried_photo_ids") or []
            )

            if after_import is None:
                result["after_import_skipped"] = "import-only"
                return
            if not result.get("ok"):
                result["after_import_skipped"] = "import failed"
                return
            if result.get("cancelled"):
                result["after_import_skipped"] = "import cancelled"
                return
            if not chain_scope:
                result["after_import_skipped"] = "no new photos"
                return
            if col_id is None:
                result["after_import_skipped"] = (
                    "failed to create import collection"
                )
                return
            try:
                # Which imported folders must the chained NAS move relocate?
                # Computed here — not at request time — because the archive
                # folder rows only exist once the copy has landed. Minimal
                # non-nested set: moving an ancestor also moves its
                # descendants. The target itself is the enqueue-time
                # snapshot, so a Settings edit mid-chain can't redirect the
                # move. Uses ``chain_scope`` so a recovery retry moves the
                # folders holding the original run's successful files too.
                after_move = None
                if move_target_snapshot is not None and not defer_nas_transfer:
                    from import_chain import minimal_move_set
                    folder_rows = []
                    for i in range(0, len(chain_scope), 500):
                        chunk = chain_scope[i:i + 500]
                        ph = ",".join("?" * len(chunk))
                        folder_rows.extend(thread_db.conn.execute(
                            "SELECT DISTINCT f.id, f.path FROM photos p "
                            "JOIN folders f ON f.id = p.folder_id "
                            f"WHERE p.id IN ({ph})", chunk).fetchall())
                    root = move_target_snapshot["local_archive_root"]
                    moves, move_skips = minimal_move_set(
                        root, [(r["id"], r["path"]) for r in folder_rows])
                    after_move = {
                        "target": move_target_snapshot,
                        "folders": moves,
                    }
                    if move_skips:
                        # Importing straight into the archive root with a
                        # template that renders empty catalogs photos ON
                        # the root folder itself; minimal_move_set refuses
                        # to move the root (it would sweep unrelated
                        # shoots into the transfer). Say so instead of a
                        # bare "no folders to move" — the user accepted a
                        # chain that ends on the NAS, and these photos
                        # won't get there.
                        prefix = "photos" if not moves else "some photos"
                        if any(s["reason"] == "root" for s in move_skips):
                            after_move["skip_note"] = (
                                prefix + " landed directly in the archive "
                                "root — moving the root would sweep "
                                "unrelated shoots into the transfer, so "
                                "they stay local; move them from the Move "
                                "page")
                        else:
                            after_move["skip_note"] = (
                                prefix + " landed outside the archive "
                                "root, so they stay local; move them from "
                                "the Move page")
                process_job_id, model_warning, process_blocker = (
                    enqueue_process_job(
                        thread_db, runner, active_ws,
                        collection_id=col_id,
                        process_id=after_import,
                        chained_from=job["id"],
                        expanded=after_import_snapshot,
                        after_move=after_move,
                    )
                )
                if process_blocker:
                    result["after_import_skipped"] = process_blocker
                    if after_move:
                        # Processing is paused before any pipeline job was
                        # enqueued (e.g. Classify needs a species list the
                        # user hasn't downloaded yet), so the finally-hook
                        # that normally fires the chained NAS move never
                        # runs. But the user accepted a chain that ends on
                        # the NAS — leaving these photos in the local
                        # archive without saying so would break the promise
                        # and hide the outcome behind an "after_import
                        # skipped" pill that doesn't mention the move. Fire
                        # the move here off the import job itself, so the
                        # "photos end on the NAS" invariant holds the same
                        # way it does for a runtime process failure. The
                        # hook writes move_job_ids / after_move_errors on
                        # the import result and adds a "Move to NAS" step
                        # to the import job's tree, so the outcome is
                        # visible either way.
                        chain_after_move(
                            job, result, after_move, active_ws)
                    return
                result["process_job_id"] = process_job_id
                if after_move is not None:
                    # Surface the planned move on the import's result card so
                    # the user can see what will happen before it fires —
                    # including, honestly, that nothing (or not everything)
                    # will move when folders were skipped.
                    result["after_process_move_planned"] = {
                        "target_name": move_target_snapshot["name"],
                        "folders": after_move["folders"],
                    }
                    if after_move.get("skip_note"):
                        result["after_process_move_planned"]["note"] = (
                            after_move["skip_note"])
                if model_warning:
                    result["model_warning"] = model_warning
            except Exception as e:
                # The import itself succeeded — record the chaining failure
                # rather than flipping the whole job red, but never
                # silently: the user asked for processing and must see it
                # didn't start.
                log.exception("after-import chaining failed")
                result["after_import_skipped"] = (
                    f"failed to enqueue processing: {e}"
                )

        def work(job):
            from import_job import ImportParams, run_import_job

            import_destination = destination
            if managed_staging:
                from local_processing import selected_source_files, storage_plan, total_file_bytes
                import_destination = managed_staging["destination"]
                files = selected_source_files(sources, file_types, recursive)
                if include_paths is not None:
                    files = [p for p in files if str(p) in include_paths]
                space = storage_plan(vireo_dir, total_file_bytes(files))
                if not space["enough"]:
                    raise ValueError("Not enough free space on this computer for temporary processing. Free up space or import fewer photos.")
                os.makedirs(import_destination, exist_ok=True)
                if pending_archive_id:
                    from pending_archives import register_pending_archive
                    with Database(db_path) as pending_db:
                        pending_db.set_active_workspace(active_ws)
                        register_pending_archive(
                            pending_db, pending_archive_id, destination, import_destination, move_target_snapshot,
                        )

            params = ImportParams(
                sources=sources,
                destination=import_destination,
                folder_template=folder_template,
                file_types=file_types,
                skip_duplicates=skip_duplicates,
                verify_by_hash=verify_by_hash,
                trust_likely_duplicates=trust_likely_duplicates,
                recursive=recursive,
                after_import=after_import,
                remote_target=remote_target,
                vireo_dir=vireo_dir,
                thumb_cache_dir=thumb_cache_dir,
                include_paths=include_paths,
                previewed_count=previewed_count,
                checked_count=checked_count,
            )
            try:
                result = run_import_job(
                    job, runner, db_path, active_ws, params,
                )
                if managed_staging:
                    result["local_processing"] = True
                    result["final_destination"] = destination
                    result["staging_destination"] = import_destination
                _apply_import_tags(
                    active_ws, result.get("photo_ids") or [], import_tags,
                    location_from_gps, result, job=job, runner=runner,
                )
                # Atomically honor a pending pause/cancel before collection
                # publication and child-job handoff. The shared runner gate
                # rejects new requests once this final phase begins.
                if not runner.begin_uncancellable(job["id"]):
                    result["cancelled"] = True
                _chain_after_import(job, result)
                return result
            finally:
                # run_import_job can flip destination folders from
                # ``missing`` to ``ok`` and re-scans landed files, so a
                # ready /api/photos/missing cache computed before the
                # import can now list rows whose originals are back on
                # disk. The other scan/import paths (rescan-this-folder,
                # import-in-place) already invalidate the cache after
                # they touch disk; do the same here so the banner/modal
                # stop offering ghosts for photos this job just restored,
                # even if the job failed part-way (rows land
                # incrementally). Best-effort: never let a cache-drop
                # failure mask the underlying import result.
                try:
                    invalidate_missing_originals()
                except Exception:
                    log.exception(
                        "Failed to invalidate missing-originals cache "
                        "after import-photos job",
                    )

        # Server-side retry-exclusivity gate: refuse a retry whose
        # root ancestor already has an active retry in flight, even
        # when the Jobs-page UI gate was bypassed (two tabs, a stale
        # UI that didn't observe the sibling retry yet, or a direct
        # API caller). Without this an overlapping retry can enqueue
        # a second import against the same source and destination and
        # race the chained after-import / NAS move against the first
        # one. Runs after all snapshot validation so a rejected
        # request costs no worker thread. Best-effort against
        # exact-simultaneous starts — ``list_jobs`` releases the
        # runner lock before ``start`` takes it — which is acceptable
        # for the double-click / two-tab case this fix targets; the
        # follow-up work is to fold both under one runner-side call.
        # See PR #1387 Codex review.
        retry_root = job_config.get("root_import_job_id")
        if retry_root:
            for other in runner.list_jobs():
                if other.get("type") != "import":
                    continue
                # ``pausing`` is a live status: ``pause_job`` publishes
                # it immediately, but the worker keeps running until it
                # reaches ``is_cancelled`` and only then flips to
                # ``paused``. Treating ``pausing`` as inactive would let a
                # second retry slip in during that window and race the
                # first one's chained processing / NAS move.
                if other.get("status") not in (
                    "queued", "running", "pausing", "paused",
                ):
                    continue
                other_cfg = other.get("config") or {}
                other_root = (
                    other_cfg.get("root_import_job_id")
                    or other_cfg.get("parent_import_job_id")
                )
                # Only reject a competing RETRY — one whose own
                # ancestry root matches ours. The failed parent itself
                # (still in ``_jobs`` briefly after finishing) has no
                # ancestry field so it never matches here, letting the
                # first retry through. The parent's own liveness is
                # gated separately by ``_validate_parent_import_job``.
                if other_root and other_root == retry_root:
                    return json_error(
                        "Another retry for the same failed import is "
                        "already in flight; wait for it to finish "
                        "before starting a new one.",
                        409,
                    )

        job_id = runner.start(
            "import", work, config=job_config, workspace_id=active_ws,
            pausable=True,
        )
        response = {"job_id": job_id}
        if created_workspace is not None:
            response["workspace"] = created_workspace
        return jsonify(response)

    return blueprint
