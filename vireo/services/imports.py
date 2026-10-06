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


def _json_dict(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
    return value if isinstance(value, dict) else {}


def _has_failed_files(result):
    failed = result.get("failed")
    return (
        isinstance(failed, (int, float)) and not isinstance(failed, bool)
        and failed > 0
    )


def import_resume_takeover(parent_id, parent_result, rows, parent_config=None):
    """Whether a later run took over an import's Resume or Retry.

    Two rows offer to continue an import: one a Vireo restart interrupted
    (Resume) and one that failed files (Retry). An interrupted import owes
    the tag/GPS pass (``tags_applied``) and the chain that records its
    collection and queues processing (``chained``); a failed one skipped
    processing, so it owes the chain over its photos plus the failed
    files. The run that continues it marks each step on its own row, never
    on the parent's, so the parent's row alone cannot tell it was already
    resumed or retried. This reads the parent's descendants in ``rows``
    (finished ``job_history`` import rows): those naming it as
    ``root_import_job_id`` or reaching it through ``parent_import_job_id``
    links. Each run's scope is cumulative (carried ids, untagged ids and
    landed files are inherited), so a descendant's marks discharge the
    parent's debts too.

    Returns ``{"tags_applied", "chained", "by", "by_started_at", "kind",
    "parent_interrupted"}``. The marks are the parent's own merged with
    what its descendants did, for a Resume to replay only what's owed.
    ``by`` is the descendant that took over, or None while the parent is
    still the place to continue from (always None for a row offering
    neither):

    * ``"done"``: descendants did the owed work, so with the parent's own
      marks nothing is left. Going again would re-tag the photos
      (overwriting locations corrected since) and process them twice.
    * ``"resume"`` / ``"retry"``: a descendant is the next step. Either it
      was itself interrupted with its landed photos recorded (resume it;
      it carries the parent's scope), or it failed files after its tag
      pass, so its own Retry carries the parent's photos to processing.
      Continuing from the parent as well would fork the chain.

    A descendant that crashed or was cancelled before its tag pass did
    none of the work and leaves the parent as the place to continue from.
    Mirrored by ``importResumeTakeover`` in
    ``vireo/static/jobs/import-retry.js``; keep the two equivalent.
    """
    parent_result = _json_dict(parent_result)
    parent_interrupted = bool(parent_result.get("interrupted"))
    if not (parent_interrupted or _has_failed_files(parent_result)):
        # Neither Resume nor Retry applies to this row.
        return {
            "tags_applied": bool(parent_result.get("tags_applied")),
            "chained": bool(parent_result.get("chained")),
            "by": None, "by_started_at": None, "kind": None,
            "parent_interrupted": False,
        }
    never_started_processes = {
        row.get("id") for row in rows or []
        if row.get("type") == "pipeline" and _json_dict(row.get("result")).get("never_started")
    }
    children = {}
    candidates = []
    for row in rows or []:
        if row.get("type") != "import" or row.get("id") == parent_id:
            continue
        if row.get("status") not in ("completed", "failed", "cancelled"):
            continue
        cfg = _json_dict(row.get("config"))
        result = _json_dict(row.get("result"))
        if result.get("never_started"):
            continue
        entry = {
            "id": row.get("id"),
            "status": row.get("status"),
            "started_at": row.get("started_at") or "",
            "result": result,
            "config": cfg,
            "parent": cfg.get("parent_import_job_id"),
            "root": cfg.get("root_import_job_id"),
        }
        candidates.append(entry)
        children.setdefault(entry["parent"], []).append(entry)
    descendants = {e["id"]: e for e in candidates if e["root"] == parent_id}
    frontier = [parent_id, *descendants]
    while frontier:
        for child in children.get(frontier.pop(), []):
            if child["id"] not in descendants:
                descendants[child["id"]] = child
                frontier.append(child["id"])
    descendants = list(descendants.values())

    for e in descendants:
        result = e["result"]
        if (
            "tags_applied" in result or "chained" in result
            or result.get("interrupted")
        ):
            tags = bool(result.get("tags_applied"))
            chain_step = bool(result.get("chained"))
        else:
            # Finished before the marks were kept on the final row. Only a
            # run that passed its tag pass and reached the chain after a
            # clean import records an import collection.
            tag_only_completed = (
                e["status"] == "completed"
                and result.get("after_import_skipped")
                == "chain already ran on the interrupted parent"
                and not (result.get("tagging") or {}).get("errors")
            )
            tags = tag_only_completed or (
                result.get("collection_id") is not None
                and not (result.get("tagging") or {}).get("errors")
            )
            skipped = result.get("after_import_skipped")
            chain_step = tag_only_completed or (
                not result.get("cancelled") and (
                    result.get("process_job_id") is not None
                    or skipped in ("import-only", "no new photos")
                )
            )
        e["tags_applied"] = tags
        # The chain step also runs, and marks, after a failed import, but
        # then skips the collection and processing: that debt moves to the
        # run's own Retry instead of being paid.
        e["chained"] = (
            chain_step and result.get("ok") is not False
            and result.get("process_job_id") not in never_started_processes
        )
        e["resumable"] = (
            e["status"] == "failed"
            and bool(result.get("interrupted"))
            and isinstance(result.get("photo_ids"), list)
            and not (tags and e["chained"])
        )
        e["has_failed_files"] = _has_failed_files(result)

    # What a Resume of the parent replays: its own marks plus what
    # descendants paid (``_interrupted_parent_resume``).
    def tag_scope(config, result):
        scope, identities = set(), {}
        fingerprints = {}
        for value in (config.get("carry_photo_fingerprints"),
                      result.get("photo_fingerprints"), result.get("carried_photo_fingerprints")):
            if isinstance(value, dict):
                fingerprints.update(value)
        for values in (result.get("photo_ids"), result.get("carried_photo_ids"),
                       result.get("recovered_photo_ids"), config.get("carry_photo_ids"),
                       config.get("untagged_photo_ids")):
            for pid in values or []:
                if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0:
                    fp = fingerprints.get(str(pid)) or fingerprints.get(pid)
                    identity = fp.rsplit("|s=", 1)[-1] if isinstance(fp, str) else ""
                    token = ("photo", pid, identity)
                    scope.add(token)
                    identities[pid] = token
        for value in (config.get("recover_landed_files"), result.get("landed_files")):
            if isinstance(value, dict):
                for path, identity in value.items():
                    if isinstance(identity, list) and len(identity) >= 3:
                        scope.add(("file", identity[2] or path))
        return scope, identities

    def paid_carried_scope(config, identities):
        paid = set()
        carried = set(config.get("carry_photo_ids") or [])
        carried -= set(config.get("untagged_photo_ids") or [])
        carried |= set(config.get("paid_tag_photo_ids") or [])
        for pid in carried:
            if pid not in identities:
                continue
            token = identities[pid]
            paid.add(token)
            if "|h=" in token[2] and token[2].rsplit("|h=", 1)[1]:
                paid.add(("file", token[2].rsplit("|h=", 1)[1]))
        return paid

    parent_config = parent_config or {}
    parent_scope, tag_identities = tag_scope(parent_config, parent_result)
    paid_scope = set(parent_scope) if parent_result.get("tags_applied") else set()
    paid_scope |= paid_carried_scope(parent_config, tag_identities)
    all_scope = set(parent_scope)
    inherited_scopes = {parent_id: parent_scope}
    for entry in sorted(descendants, key=lambda e: e["started_at"]):
        scope, identities = tag_scope(entry["config"], entry["result"])
        scope |= inherited_scopes.get(entry["parent"], inherited_scopes.get(entry["root"], set()))
        inherited_scopes[entry["id"]] = scope
        tag_identities.update(identities)
        all_scope |= scope
        if entry["tags_applied"]:
            paid_scope |= scope
        paid_scope |= paid_carried_scope(entry["config"], identities)
    unpaid_scope = all_scope - paid_scope
    tags_applied = (bool(parent_result.get("tags_applied")) or any(
        e["tags_applied"] for e in descendants
    )) and not unpaid_scope
    paid_tag_ids = sorted(pid for pid, token in tag_identities.items() if token in paid_scope)
    unpaid_tag_ids = sorted(pid for pid, token in tag_identities.items() if token not in paid_scope)
    # The parent's chain step marks after a failed import too, but then
    # skips the collection and processing; apply the same ``ok`` filter to
    # the parent's own mark as to a descendant's (above) so a crash between
    # that checkpoint and the terminal row can't make the resume believe
    # the chain already ran and skip processing on recovery. That is also
    # why a failed-files parent still owes processing to its Retry.
    chained = (
        bool(parent_result.get("chained"))
        and parent_result.get("ok") is not False
    ) or any(e["chained"] for e in descendants)
    # A Retry of a parent that was not interrupted replays no tags, so only
    # processing is owed there.
    tags_paid = (tags_applied or not parent_interrupted) and not unpaid_scope
    marked = [e for e in descendants if e["tags_applied"] or e["chained"]]
    # A descendant that failed files after its tag pass offers its own
    # Retry, which carries the parent's photos to processing. One that was
    # cancelled or crashed before its tag pass did none of the work, so
    # the parent stays the place to continue from.
    next_steps = [
        e for e in descendants
        if e["resumable"] or (e["has_failed_files"] and e["tags_applied"])
    ]

    def newest(entries):
        return max(entries, key=lambda e: e["started_at"])

    descendant_landings = {}
    descendant_fingerprints = {}
    for entry in sorted(descendants, key=lambda e: e["started_at"]):
        for value in (entry["config"].get("carry_photo_fingerprints"),
                      entry["result"].get("photo_fingerprints"),
                      entry["result"].get("carried_photo_fingerprints")):
            if isinstance(value, dict):
                descendant_fingerprints.update(value)
        for value in (entry["config"].get("recover_landed_files"),
                      entry["result"].get("landed_files")):
            if isinstance(value, dict):
                descendant_landings.update(value)
    by, kind = None, None
    if marked and tags_paid and chained:
        by, kind = newest(marked), "done"
    elif next_steps:
        by = newest(next_steps)
        kind = "resume" if by["resumable"] else "retry"
    return {
        "tags_applied": tags_applied,
        "chained": chained,
        "by": by["id"] if by else None,
        "by_started_at": by["started_at"] if by else None,
        "kind": kind,
        "parent_interrupted": parent_interrupted,
        "descendant_landed_files": descendant_landings,
        "descendant_photo_fingerprints": descendant_fingerprints,
        "paid_tag_photo_ids": paid_tag_ids,
        "unpaid_tag_photo_ids": unpaid_tag_ids,
    }


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
        ws_id = None
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
            log.warning("Could not create the import's new workspace %r", name, exc_info=True)
            # ``create_workspace`` commits, so a failure in a later setup
            # step (default collections, the switch) would otherwise leave
            # a half-made workspace behind, possibly still active.
            if ws_id is not None:
                self._rollback_import_workspace(
                    db, {"id": ws_id}, previous_active_ws,
                )
            return None, None, previous_active_ws, ImportFailure(str(e))

    def _admit_into_import_workspace(
        self, db, created_workspace, previous_active_ws, admit,
    ):
        """Run everything an import does after ``_prepare_import_workspace``.

        ``admit()`` holds every check between the workspace switch and the
        job's registration, and returns either an ``ImportFailure`` or the
        id ``runner.start`` returned, which must be its last step. Any
        failure, returned or raised, rolls back the workspace the request
        created and restores the previously active one, so a check added
        later cannot leave an orphan workspace and a silently changed
        active workspace behind. Once ``admit`` has returned a job id the
        job owns the workspace and nothing is rolled back.
        """
        try:
            outcome = admit()
        except BaseException:
            self._rollback_import_workspace(
                db, created_workspace, previous_active_ws,
            )
            raise
        if isinstance(outcome, ImportFailure):
            self._rollback_import_workspace(
                db, created_workspace, previous_active_ws,
            )
            return outcome
        response = {"job_id": outcome}
        if created_workspace is not None:
            response["workspace"] = created_workspace
        return response

    def _rollback_import_workspace(self, db, created_workspace, previous_active_ws):
        """Undo ``_prepare_import_workspace`` when a later admission check fails.

        A ``new_workspace_name`` import that is rejected after the workspace
        step has already committed a workspace row and switched
        active-workspace to it. Returning the error without this rollback
        would leak that state: an orphan workspace with no import attached,
        plus a silent change of the user's active workspace even though no
        job was queued. Restore the previous active workspace and delete the
        freshly created one so the failure is state-neutral. Callers reach
        this through ``_admit_into_import_workspace``.
        """
        if created_workspace is None:
            return
        try:
            db.set_active_workspace(previous_active_ws)
        except Exception:
            log.exception(
                "Failed to restore active workspace after a rejected import",
            )
        try:
            db.delete_workspace(int(created_workspace["id"]))
        except Exception:
            log.exception(
                "Failed to delete created import workspace after a rejected import",
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

    def _validate_parent_import_job(self, parent_id, active_ws, db, *,
                                    relocate_landings=True, snapshot_out=None):
        """Resolve a retry's parent_import_job_id into the scope this
        retry is allowed to inherit.

        Returns ``(parent_config, allowed_ids, allowed_fingerprints,
        parent_source_snapshots, parent_resume, None)`` on success or
        ``(None, None, None, None, None, error_response)`` when the parent
        can't be used. ``parent_resume`` is None unless a Vireo restart
        interrupted the parent; then it holds what only that parent's own
        records can say: ``landed_files`` (every file the parent, or an
        interrupted run it resumed, recorded as landed before cataloging,
        with its size and mtime) and ``untagged_ids`` (photos the parent landed or
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
        takeover = None
        if parent_result.get("interrupted") or _has_failed_files(parent_result):
            takeover = import_resume_takeover(
                parent_id, parent_result,
                self._import_resume_rows(
                    db, parent_id, parent_config, parent_workspace,
                ), parent_config,
            )
            if takeover["by"] is not None:
                return None, None, None, None, None, ImportFailure(
                    self._resume_takeover_message(takeover), 409,
                    details={
                        "code": (
                            "import_already_resumed"
                            if takeover["parent_interrupted"]
                            else "import_already_retried"
                        ),
                        "taken_over_by_job_id": takeover["by"],
                        "takeover": takeover["kind"],
                    },
                )
        if snapshot_out is not None:
            # Freeze the cheap evidence before any disk-based path recovery.
            snapshot_out.update({
                "parent_result": parent_result,
                "takeover": json.loads(json.dumps(takeover)),
            })
        if takeover and relocate_landings:
            self._recover_relocated_descendant_landings(
                db, takeover, allowed_fingerprints,
            )
        parent_resume = self._interrupted_parent_resume(
            parent_config, parent_result, takeover,
        )
        if (
            parent_resume is not None
            and not parent_resume.get("chain_already_ran")
            and self._chained_job_exists(db, parent_id)
        ):
            # The restart landed after the parent handed its photos to
            # processing but before its ``chained`` mark reached the row.
            # Resuming would collect and process them a second time.
            # (A resume that DOES know the chain ran — ``chain_already_ran``
            # from the parent's own ``chained`` mark — is fine: it replays
            # only the owed tag pass and skips re-chaining below.)
            return None, None, None, None, None, ImportFailure(
                "This import had already started processing its photos "
                "before Vireo restarted, so there is nothing to resume. "
                "Check the Jobs page for that processing run.",
                409,
            )
        return (
            parent_config,
            allowed_ids,
            allowed_fingerprints,
            parent_source_snapshots,
            parent_resume,
            None,
        )

    def _chained_job_exists(self, db, parent_id):
        """Whether any job records ``parent_id`` as the import it was
        chained from (the after-import processing run). ``enqueue_pipeline``
        persists the queued row, so this survives a restart.

        Skips history rows the startup sweep marked ``never_started``: if
        both pipeline slots were occupied when the parent import checkpointed
        its chain-enqueue, the child stayed ``queued`` and never picked up a
        slot before a restart. ``JobRunner._startup_sweep`` fails such rows
        as "before it started"; treating them as proof that processing began
        would strand the parent's photos with no way to resume them.
        """
        for job in self.get_runner().list_jobs():
            if (job.get("config") or {}).get("chained_from") == parent_id:
                return True
        return db.conn.execute(
            "SELECT 1 FROM job_history "
            "WHERE json_extract(config, '$.chained_from') = ? "
            "  AND COALESCE(json_extract(result, '$.never_started'), 0) = 0 "
            "LIMIT 1",
            (parent_id,),
        ).fetchone() is not None

    def _recover_relocated_descendant_landings(
        self, db, takeover, allowed_fingerprints=None,
    ):
        """Keep a moved descendant's scope only when its bytes still match.

        Also rebases ``allowed_fingerprints`` (the parent-carried IDs'
        expected fingerprints) in place when a descendant's
        ``after_process_move`` relocated a photo the parent carried: the
        current fingerprint uses the new path, but its size+hash still
        match the parent's old-path record, so comparing the parent's
        stored value in ``validate_carry_photo_ids`` would reject the
        carry as stale and strand the remaining processing work after
        the move. Point ``allowed_fingerprints`` at the current path so
        the carry re-validates. Both lookups share one fingerprint
        capture; this hashing already runs outside the admission lock.
        """
        descendant_expected = {}
        for key, fingerprint in takeover.get("descendant_photo_fingerprints", {}).items():
            try:
                pid = int(key)
            except (TypeError, ValueError):
                continue
            if pid > 0 and isinstance(fingerprint, str):
                descendant_expected[pid] = fingerprint
        all_pids = set(descendant_expected)
        if allowed_fingerprints:
            all_pids.update(allowed_fingerprints)
        if not all_pids:
            return
        current = self._capture_photo_fingerprints_for_ids(db, list(all_pids))
        for pid, fingerprint in current.items():
            new_parts = fingerprint.rsplit("|s=", 1)
            if len(new_parts) != 2 or "|h=" not in new_parts[1]:
                continue
            new_identity = new_parts[1]
            # Fingerprints retain a slash before the filename on every
            # platform. Recovery indexes paths built by os.path.join, so
            # use native separators here without changing stored identities.
            new_path = os.path.normpath(new_parts[0])
            file_hash = new_identity.rsplit("|h=", 1)[1]
            # Descendant moved: keep its scope under the current path.
            expected_descendant = descendant_expected.get(pid)
            if expected_descendant is not None:
                old_parts = expected_descendant.rsplit("|s=", 1)
                if (
                    len(old_parts) == 2
                    and old_parts[1] == new_identity
                    and file_hash
                ):
                    takeover["descendant_landed_files"][new_path] = [
                        -1, -1, file_hash,
                    ]
            # Parent-carried moved: rebase the expected fingerprint to
            # the current path so the carry validates after the move.
            if allowed_fingerprints is not None:
                expected_parent = allowed_fingerprints.get(pid)
                if (
                    expected_parent is not None
                    and expected_parent != fingerprint
                ):
                    old_parts = expected_parent.rsplit("|s=", 1)
                    if (
                        len(old_parts) == 2
                        and old_parts[1] == new_identity
                        and file_hash
                    ):
                        allowed_fingerprints[pid] = fingerprint

    def _import_resume_rows(self, db, parent_id, parent_config, workspace_id):
        """Finished import rows that may descend from ``parent_id``, for
        ``import_resume_takeover``. Every descendant inherits the chain's
        root, so matching it (or a direct parent link, for retries older
        than ``root_import_job_id``) finds them all; the walk itself picks
        out the actual descendants. Terminal runner snapshots override
        history while the final row is still being persisted.
        """
        root = parent_config.get("root_import_job_id") or parent_id
        runner = self.get_runner()
        terminal_imports = [
            job for job in (runner.list_jobs() if runner is not None else [])
            if job.get("type") == "import"
            and job.get("status") in ("completed", "failed", "cancelled")
            and job.get("workspace_id") == workspace_id
        ]
        runner_seeds = "".join(" UNION SELECT ?" for _ in terminal_imports)
        rows = [
            dict(row) for row in db.conn.execute(
                "WITH RECURSIVE lineage(id) AS ("
                " SELECT id FROM job_history WHERE type='import' AND workspace_id IS ?"
                " AND (id IN (?, ?) OR json_extract(config, '$.root_import_job_id') = ?"
                " OR json_extract(config, '$.parent_import_job_id') = ?)"
                + runner_seeds +
                " UNION SELECT child.id FROM job_history child JOIN lineage"
                " ON json_extract(child.config, '$.parent_import_job_id') = lineage.id"
                " WHERE child.type='import' AND child.workspace_id IS ?"
                ") SELECT id, type, status, started_at, config, result FROM job_history"
                " WHERE id IN (SELECT id FROM lineage)"
                " AND status IN ('completed', 'failed', 'cancelled')",
                (workspace_id, root, parent_id, root, parent_id,
                 *(job["id"] for job in terminal_imports), workspace_id),
            ).fetchall()
        ]

        by_id = {row["id"]: row for row in rows}
        for job in terminal_imports:
            # Include terminal snapshots before following parent links:
            # a mixed-version grandchild can name the legacy child as root.
            by_id[job["id"]] = job
        # A queued child can be marked never_started by startup. Its
        # import's chain checkpoint is then unpaid, just as the parent's
        # own _chained_job_exists guard treats that child.
        process_ids = {
            _json_dict(row.get("result")).get("process_job_id")
            for row in by_id.values()
        } - {None}
        if process_ids:
            placeholders = ",".join("?" for _ in process_ids)
            for row in db.conn.execute(
                f"SELECT id, type, status, started_at, config, result FROM job_history "
                f"WHERE type='pipeline' AND workspace_id IS ? AND id IN ({placeholders})",
                (workspace_id, *process_ids),
            ).fetchall():
                by_id[row["id"]] = dict(row)
            for job in runner.list_jobs() if runner is not None else []:
                if job.get("id") in process_ids and job.get("workspace_id") == workspace_id:
                    by_id[job["id"]] = job
        return list(by_id.values())

    @staticmethod
    def _resume_takeover_message(takeover):
        started = (takeover.get("by_started_at") or "").replace("T", " ")[:16]
        which = (
            f"the import started {started} (job {takeover['by']})"
            if started else f"job {takeover['by']}"
        )
        if takeover["parent_interrupted"]:
            verb, step, again, also = (
                "resumed", "resume",
                "Resuming again would tag and process the same photos a "
                "second time.",
                " too",
            )
        else:
            verb, step, again, also = (
                "retried", "retry",
                "Retrying again would process the same photos a second "
                "time.",
                "",
            )
        if takeover["kind"] == "done":
            return (
                f"This import was already {verb} by {which}, which "
                "finished the work it owed, so there is nothing left to "
                f"{step}. {again}"
            )
        if takeover["kind"] == "resume":
            return (
                f"This import was already {verb} by {which}, and that "
                f"run was interrupted{also}. Resume that import from the "
                "Jobs page instead; it carries this import's photos."
            )
        return (
            f"This import was already {verb} by {which}, which had "
            "files fail. Retry its failed files from the Jobs page "
            "instead; that retry carries this import's photos."
        )

    @staticmethod
    def _interrupted_parent_resume(parent_config, parent_result, takeover=None):
        """Remaining work/scope inherited from an interrupted parent or a
        failed-files parent whose descendants landed additional photos.

        ``takeover`` (from ``import_resume_takeover``) supplies the
        parent's post-import marks merged with its descendants', so work
        an earlier resume already did is not replayed.
        """
        if not parent_result.get("interrupted") and not (
            _has_failed_files(parent_result)
            and takeover and takeover.get("descendant_landed_files")
        ):
            return None
        if takeover is None:
            takeover = {
                "tags_applied": bool(parent_result.get("tags_applied")),
                # Discount a ``chained`` mark on a failed row (see
                # ``import_resume_takeover``): the chain step marked but
                # skipped processing when ``ok`` was False.
                "chained": (
                    bool(parent_result.get("chained"))
                    and parent_result.get("ok") is not False
                ),
            }
        tags_applied = bool(takeover["tags_applied"])
        chain_already_ran = bool(takeover["chained"])
        # Both post-import steps ran — nothing left to resume; the row
        # only missed the final write. A ``chained`` mark without
        # ``tags_applied`` means a Stop cut the tag pass short or an
        # error left tags/GPS owed after the chain enqueued; that stays
        # resumable as a tag-only replay (the chain isn't re-run).
        if chain_already_ran and tags_applied:
            return None

        def ids(values):
            return [
                pid for pid in values or []
                if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0
            ]

        def files(values):
            if not isinstance(values, dict):
                return {}
            return {
                path: list(identity) for path, identity in values.items()
                if isinstance(path, str) and path
                and isinstance(identity, list) and len(identity) == 3
                and all(isinstance(v, int) for v in identity[:2])
                and isinstance(identity[2], str)
            }

        return {
            "landed_files": {
                **files(parent_config.get("recover_landed_files")),
                **files(parent_result.get("landed_files")),
                **files(takeover.get("descendant_landed_files")),
            },
            # Its tag/GPS pass covered everything it owed once it ran.
            "tags_applied": tags_applied,
            "paid_tag_photo_ids": takeover.get("paid_tag_photo_ids", []),
            # The chain already ran (collection created, processing
            # child enqueued) — the resume must not re-chain, only
            # replay the owed tag pass.
            "chain_already_ran": chain_already_ran,
            # The parent's own landings, minus photos it only carried (a
            # retry with duplicate skipping off adopts those into its
            # photo_ids, but their own import already tagged them), plus
            # what it inherited as untagged.
            "untagged_ids": [] if tags_applied else sorted(
                ((set(ids(parent_result.get("photo_ids")))
                  - set(ids(parent_config.get("carry_photo_ids"))))
                 | set(ids(parent_config.get("untagged_photo_ids")))
                 | set(ids(takeover.get("unpaid_tag_photo_ids"))))
                - set(ids(takeover.get("paid_tag_photo_ids")))
            ),
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
