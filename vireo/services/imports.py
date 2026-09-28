"""Import admission and shared job services, independent of HTTP.

One instance belongs to one app. Runtime settings and callbacks stay live,
while each launch receives its request database explicitly. Workers open their
own connections; snapshot admission locks are shared across launches in the app.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime

from db import Database
from keyword_normalization import keyword_match_key, normalize_keyword_display

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ImportFailure:
    """An admission failure for the caller to present in its own transport.

    ``details`` preserves the structured metadata-dependency diagnostic.
    Ordinary failures use the caller's standard error envelope.
    """

    message: str
    status: int = 400
    details: dict | None = None


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


class ImportService:
    """Validate and launch in-place and archive imports for one app."""

    def __init__(
        self, get_runner, db_path, config, *, invalidate_missing_originals,
        enqueue_process_job, chain_after_move, bulk_gps_location_payload,
    ):
        self.get_runner = get_runner
        self.db_path = db_path
        self.config = config
        self.invalidate_missing_originals = invalidate_missing_originals
        self.enqueue_process_job = enqueue_process_job
        self.chain_after_move = chain_after_move
        self.bulk_gps_location_payload = bulk_gps_location_payload
        # Serialize workers importing the same frozen snapshot so replays
        # observe prior catalog admissions instead of claiming them again.
        self.snapshot_import_locks = {}
        self.snapshot_import_locks_guard = threading.Lock()

    def import_in_place(self, db: Database, body: dict) -> dict | ImportFailure:
        from services.import_in_place import enqueue_import_in_place

        return enqueue_import_in_place(self, db, body)

    def import_photos(self, db: Database, body: dict) -> dict | ImportFailure:
        from services.import_photos import enqueue_import_photos

        return enqueue_import_photos(self, db, body)

    def _gps_location_chunks(self, values, size=800):
        values = list(values)
        for idx in range(0, len(values), size):
            yield values[idx:idx + size]

    def _validate_import_tag_options(self, body):
        """Normalize optional tags attached to a photo import request."""
        raw_tags = body.get("tags", [])
        if not isinstance(raw_tags, list):
            return None, None, ImportFailure("tags must be a list of names")
        if len(raw_tags) > 50:
            return None, None, ImportFailure("at most 50 import tags are allowed")

        tags = []
        seen = set()
        for raw in raw_tags:
            if not isinstance(raw, str):
                return None, None, ImportFailure(
                    "tags must contain only strings"
                )
            name = normalize_keyword_display(raw)
            if not name:
                return None, None, ImportFailure("import tags must not be empty")
            if len(name) > 200:
                return None, None, ImportFailure(
                    "import tags must be 200 characters or fewer"
                )
            match_key = keyword_match_key(name) or name.casefold()
            if match_key not in seen:
                seen.add(match_key)
                tags.append(name)

        location_from_gps = body.get("location_from_gps", False)
        if not isinstance(location_from_gps, bool):
            return None, None, ImportFailure(
                "location_from_gps must be a boolean"
            )
        return tags, location_from_gps, None

    def _queue_import_keyword_add(
        self,     db, photo_id, keyword_name, workspace_id, *, commit=False,
    ):
        """Thread-safe equivalent of queue_keyword_add for import jobs."""
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
        self,     db, photo_id, workspace_id, *, commit=False,
    ):
        """Thread-safe equivalent of queue_location_sync_if_enabled."""
        db.remove_pending_changes(
            photo_id, "location", workspace_id=workspace_id, _commit=commit,
        )
        db.queue_change(
            photo_id, "location", "effective",
            workspace_id=workspace_id, _commit=commit,
        )

    def _apply_import_tags(
        self,     workspace_id, photo_ids, tags, location_from_gps, result,
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

        thread_db = Database(self.db_path)
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
                    self._queue_import_keyword_add(
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
            for photo_chunk in self._gps_location_chunks(photo_ids, size=10000):
                if cancel_requested():
                    cancelled_during_gps = True
                    break
                try:
                    payload, error = self.bulk_gps_location_payload(
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
                            self._queue_import_location_sync(
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

    def _prepare_import_workspace(self, db, body):
        """Return the workspace id an import should write to.

        Returns ``(active_ws, created_workspace, previous_active_ws, err)``:
        ``previous_active_ws`` is the workspace that was active before this
        call, so a later admission failure inside the atomic stage-boundary
        block can undo a freshly-created workspace and restore the user's
        previous active state through ``_rollback_import_workspace``.

        ``new_workspace_name`` mirrors the normal workspace creation route
        so import-to-new-workspace jobs get default collections and do not
        inherit stale per-workspace caches from a reused SQLite rowid.
        """
        previous_active_ws = db._active_workspace_id
        if "new_workspace_name" not in body:
            active_ws = previous_active_ws
            if active_ws is None:
                # Without a target workspace ``run_import_job`` would bind
                # ``active_ws=None`` and its batch scans would insert
                # folders/photos while ``Database.add_folder`` skipped the
                # workspace link, leaving catalog rows invisible to every
                # workspace. Reject at the route boundary so the request
                # never enqueues instead.
                return None, None, previous_active_ws, ImportFailure(
                    "no active workspace", 400,
                )
            return active_ws, None, previous_active_ws, None
        raw_name = body.get("new_workspace_name")
        if not isinstance(raw_name, str):
            return None, None, previous_active_ws, ImportFailure(
                "new_workspace_name must be a string",
            )
        name = raw_name.strip()
        if not name:
            return None, None, previous_active_ws, ImportFailure(
                "new_workspace_name is required",
            )
        try:
            from datetime import datetime

            ws_id = db.create_workspace(name)
            self.invalidate_missing_originals(workspace_ids=[ws_id])
            db.create_default_collections(workspace_id=ws_id)
            db.set_active_workspace(ws_id)
            db.update_workspace(ws_id, last_opened_at=datetime.now().isoformat())
            ws = db.get_workspace(ws_id)
            return (
                ws_id,
                dict(ws) if ws else {"id": ws_id, "name": name},
                previous_active_ws,
                None,
            )
        except Exception as e:
            return None, None, previous_active_ws, ImportFailure(str(e))

    def _rollback_import_workspace(self, db, created_workspace, previous_active_ws):
        """Undo ``_prepare_import_workspace`` when a later admission check fails.

        A ``new_workspace_name`` import that passes the pre-flight but is
        later rejected inside the atomic stage-boundary block has already
        committed a workspace row and switched active-workspace to it.
        Returning 409 without this rollback would leak that state: an
        orphan workspace with no import attached, plus a silent change of
        the user's active workspace even though no job was queued.
        Restore the previous active workspace and delete the freshly
        created one so the 409 is state-neutral.
        """
        if created_workspace is None:
            return
        try:
            db.set_active_workspace(previous_active_ws)
        except Exception:
            log.exception(
                "Failed to restore active workspace after import conflict",
            )
        try:
            db.delete_workspace(int(created_workspace["id"]))
        except Exception:
            log.exception(
                "Failed to delete created import workspace after conflict",
            )

    def _remote_target_snapshot(self, remote_archive_config):
        """Freeze the parts of a resolved remote target that decide where
        files land, for retry-time comparison against the parent job.

        Returns None when the current request is not a remote-archive
        import — a retry that swapped from remote to local (or vice
        versa) will already fail the exact-equal comparison against a
        parent snapshot of the opposite shape. See
        ``enqueue_import_photos``'s parent_import_job_id check.
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

    def _move_target_snapshot(self, target):
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

    def _capture_photo_fingerprints_for_ids(self, db, ids):
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

    def _validate_parent_import_job(self, parent_id, active_ws, db):
        """Resolve a retry's parent_import_job_id into the scope this
        retry is allowed to inherit.

        Returns ``(parent_config, allowed_ids, allowed_fingerprints,
        parent_source_snapshots, parent_resume, None)`` on success or
        ``(None, None, None, None, None, error_response)`` when the parent
        can't be used. ``parent_resume`` is None unless a Vireo restart
        interrupted the parent; then it holds what only that parent's own
        records can say: ``landed_paths`` (every destination path the
        parent, or an interrupted run it resumed, recorded before
        cataloging) and ``untagged_ids`` (photos the parent landed or
        inherited as untagged, whose tag/GPS pass it never reached).
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
        runner = self.get_runner()
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
                return None, None, None, None, None, ImportFailure(
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
            return None, None, None, None, None, ImportFailure(
                "parent_import_job_id must reference an import job "
                f"(got type {parent_type!r})"
            )
        if parent_status not in {"completed", "failed", "cancelled"}:
            return None, None, None, None, None, ImportFailure(
                "parent_import_job_id is still active "
                f"(status {parent_status!r}); wait for the original import "
                "to finish before retrying",
                409,
            )
        if parent_workspace != active_ws:
            return None, None, None, None, None, ImportFailure(
                "parent_import_job_id belongs to a different workspace "
                "than the active one; switch workspaces or start a new "
                "import instead of retrying"
            )
        allowed_ids = set()
        for source in (
            parent_result.get("photo_ids") or [],
            parent_result.get("carried_photo_ids") or [],
            parent_result.get("recovered_photo_ids") or [],
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
            self._interrupted_parent_resume(parent_config, parent_result),
            None,
        )

    @staticmethod
    def _interrupted_parent_resume(parent_config, parent_result):
        """What a resume inherits from an interrupted parent (see
        ``_validate_parent_import_job``); None for any other parent."""
        if not parent_result.get("interrupted"):
            return None

        def ids(values):
            return [
                pid for pid in values or []
                if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0
            ]

        def paths(values):
            return [p for p in values or [] if isinstance(p, str) and p]

        return {
            "landed_paths": sorted(set(
                paths(parent_result.get("landed_paths"))
                + paths(parent_config.get("recover_landed_paths"))
            )),
            "untagged_ids": sorted(set(
                ids(parent_result.get("photo_ids"))
                + ids(parent_config.get("untagged_photo_ids"))
            )),
        }

    def _validate_after_import(self, value, db, *, allow_missing=False):
        """Return an admission failure for a bad after_import spec, else None.

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
            return ImportFailure(
                "after_import must be a process id or null, got "
                f"{type(value).__name__}"
            )
        if not allow_missing and db.get_saved_process(value) is None:
            return ImportFailure(f"unknown process id: {value}")
        return None

    def _validate_after_process_move(
        self,     value, after_import, destination, folder_template, file_types=None,
    ):
        """Validate an after_process_move spec; return (target_snapshot, error).

        ``None`` value → (None, None). Otherwise the value must name a saved
        remote target that has a local_archive_root containing ``destination``,
        and the run must chain a process (the move fires from the process
        job's completion hook — an import-only move is just the Move page).
        The returned target dict is the enqueue-time snapshot: a Settings
        edit mid-chain must not redirect the move (same rationale as
        remote_target_snapshot).

        ``file_types`` narrows the mount-overlap check to the folders the run
        can actually create — a RAW-only import can't render a ``JPEG`` folder
        even when the template contains ``{file_type}``, so a JPEG-only mount
        must not block it. ``None`` (the recursive-call default, since the
        recursion path has already picked a concrete category) falls back to
        every supported category.
        """
        if value is None:
            return None, None
        if "{file_type}" in folder_template:
            from ingest import destination_file_types_for

            # Check every possible render against the NAS mount guard below.
            # Treating the token as a literal would miss e.g. a mount at RAW/.
            # Derive the categories from ``file_types`` so we only check
            # renders the import can actually produce.
            snapshot = None
            for file_type in destination_file_types_for(file_types):
                templates = [folder_template.replace("{file_type}", file_type)]
                if "%" in folder_template:
                    templates.append(f"{file_type}/unsorted")
                for template in templates:
                    snapshot, error = self._validate_after_process_move(
                        value, after_import, destination, template,
                    )
                    if error is not None:
                        return None, error
            return snapshot, None
        if not isinstance(value, dict):
            return None, ImportFailure(
                "after_process_move must be an object or null, got "
                f"{type(value).__name__}")
        raw = value.get("remote_target_id")
        if raw is not None and not isinstance(raw, str):
            return None, ImportFailure(
                "after_process_move.remote_target_id must be a string, got "
                f"{type(raw).__name__}")
        target_id = (raw or "").strip()
        if not target_id:
            return None, ImportFailure(
                "after_process_move.remote_target_id required")
        if after_import is None:
            return None, ImportFailure(
                "after_process_move requires after_import — the move chains "
                "off the processing run; for a move without processing use "
                "the Move page")
        import config as cfg
        target = cfg.get_remote_target(target_id)
        if target is None:
            return None, ImportFailure(f"unknown remote target: {target_id}")
        root = (target.get("local_archive_root") or "").strip()
        if not root:
            return None, ImportFailure(
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
            return None, ImportFailure(
                "this remote target has no local mount path — the chained "
                "move would have nowhere to land photos. Set one under "
                "Settings → Remote targets")
        if not os.path.isabs(mount):
            return None, ImportFailure(
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
            return None, ImportFailure(
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
            return None, ImportFailure(
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
                    return None, ImportFailure(
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
            return None, ImportFailure(
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

    def _validate_import_metadata_dependency(self, body):
        """Require working metadata extraction unless explicitly overridden.

        Import can be repaired later, but proceeding silently loses capture
        dates, GPS, camera data, and date-based archive placement.  Keep an
        advanced escape hatch for unusual recovery workflows while making the
        safe behavior the API default (not merely a client-side convention).
        """
        allow_missing = body.get("allow_missing_exiftool", False)
        if not isinstance(allow_missing, bool):
            return ImportFailure("allow_missing_exiftool must be a boolean")
        if not self.config["REQUIRE_EXIFTOOL_FOR_IMPORT"] or allow_missing:
            return None

        from metadata import exiftool_status

        status = exiftool_status()
        if status["available"]:
            return None
        return ImportFailure(
            (
                "ExifTool is required for import so Vireo can preserve "
                "capture dates, GPS, and camera metadata. Repair ExifTool "
                "or explicitly choose Import without metadata in Advanced."
            ),
            status=409,
            details={"code": "exiftool_required", "exiftool": status},
        )

    def _create_import_collection(self, thread_db, photo_ids):
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

    def _record_import_collection(self, result, workspace_id, chain_photo_ids=None):
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
            thread_db = Database(self.db_path)
            thread_db.set_active_workspace(workspace_id)
            collection_id, collection_name = self._create_import_collection(
                thread_db, collection_ids,
            )
            result["collection_id"] = collection_id
            result["collection_name"] = collection_name
            return thread_db, collection_id
        except Exception as e:
            log.exception("import collection creation failed")
            result["collection_error"] = str(e)
            return None, None
