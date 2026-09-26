"""Batch edits over a photo selection: every ``/api/batch/*`` route.

Location (free text, a Google place or saved location keyword, and each
photo's own EXIF GPS resolved to a place), Best Batch flag/reject, keyword
add and remove, wildlife-classification exclusion, and delete. Request
parsing and payloads come from ``web.location_edits`` and
``web.request_args``; sidecar queueing from ``services.pending_changes``;
Google result-language handling from ``places``. The per-photo equivalents
live in ``web.photo_location_keywords``.
"""

import math

import places
from db import KEYWORD_TYPES
from flask import Blueprint, jsonify, request
from services.pending_changes import (
    queue_keyword_add,
    queue_keyword_remove,
    queue_location_sync_if_enabled,
)
from web.location_edits import (
    LOCATION_NAME_PIPE_ERROR,
    extract_keyword_id,
    extract_place_id,
    location_name_conflict_payload,
    location_name_conflict_response,
    normalize_client_place_details,
    serialize_photo_location,
)
from web.request_args import parse_selection_photo_ids


def create_batch_blueprint(
    get_db,
    json_error,
    *,
    location_errors,
    bulk_gps_location_payload,
    run_batch_delete,
    invalidate_missing_originals,
):
    """Build the batch-edit blueprint.

    Everything injected is shared with routes or startup work still in
    ``create_app`` (or with other blueprints), so it is passed in rather than
    moved: ``location_errors`` is the app's one
    ``web.location_edits.LocationErrors`` (bound to ``json_error``);
    ``bulk_gps_location_payload`` resolves a selection's EXIF GPS to places
    and is shared with the import blueprint; ``run_batch_delete`` runs the
    delete phases shared with the photos blueprint and the batch-delete job
    launcher; ``invalidate_missing_originals`` drops the missing-originals
    cache after photos are deleted.
    """
    blueprint = Blueprint("batch", __name__)

    @blueprint.route("/api/batch/location/text", methods=["POST"])
    def api_batch_set_photo_location_text():
        """Attach one free-text location keyword to multiple photos."""
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("photo_ids", [])
        if not isinstance(raw_ids, list) or not raw_ids:
            return json_error("photo_ids required", 400)

        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return json_error("photo_ids must contain only integers", 400)
            if raw not in seen:
                photo_ids.append(raw)
                seen.add(raw)
        if not photo_ids:
            return json_error("photo_ids required", 400)
        if len(photo_ids) > 1000:
            return json_error("too many photo_ids", 400)

        name = body.get("name") or ""
        if not name.strip():
            return json_error("missing name", 400)
        stripped = name.strip()
        # Reject at assignment time: Lightroom reserves ``|`` for the
        # hierarchy delimiter, and ``SidecarEditor.set_location_keywords``
        # cannot round-trip it.
        if "|" in stripped:
            return json_error(LOCATION_NAME_PIPE_ERROR, 400)
        latitude = body.get("latitude")
        longitude = body.get("longitude")
        if (latitude is None) != (longitude is None):
            return json_error("latitude and longitude must be provided together", 400)
        if latitude is not None:
            try:
                latitude = float(latitude)
                longitude = float(longitude)
            except (TypeError, ValueError):
                return json_error("invalid coordinates", 400)
            if (
                not math.isfinite(latitude)
                or not math.isfinite(longitude)
                or not -90 <= latitude <= 90
                or not -180 <= longitude <= 180
            ):
                return json_error("invalid coordinates", 400)

        db = get_db()
        for pid in photo_ids:
            edit_error = location_errors.photo_location_edit_error(db, pid)
            if edit_error is not None:
                return edit_error

        try:
            leaf_id = db.get_or_create_text_location(stripped)
        except ValueError:
            return json_error("missing name", 400)
        if latitude is not None:
            # A custom name chosen from the map should be available as a
            # nearby saved suggestion next time. Preserve an existing
            # location's established map point when the name is reused.
            db.conn.execute(
                "UPDATE keywords SET latitude = COALESCE(latitude, ?), "
                "longitude = COALESCE(longitude, ?) WHERE id = ?",
                (latitude, longitude, leaf_id),
            )
            db.conn.commit()

        items = []
        for pid in photo_ids:
            db.set_photo_location(pid, leaf_id)
            queue_location_sync_if_enabled(db, pid, _commit=False)
            items.append({
                "photo_id": pid,
                "old_value": "",
                "new_value": str(leaf_id),
            })
        if items:
            db.record_edit(
                "location_set",
                f"set location: {stripped} on {len(items)} photos",
                str(leaf_id),
                items,
                is_batch=True,
                _commit=False,
            )
        db.conn.commit()
        db._prune_edit_history()
        return jsonify({
            "ok": True,
            "updated": len(items),
            "location": serialize_photo_location(db, photo_ids[0]),
        })

    @blueprint.route("/api/batch/location", methods=["POST"])
    def api_batch_set_photo_location():
        """Attach one Google place location to multiple photos."""
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("photo_ids", [])
        if not isinstance(raw_ids, list) or not raw_ids:
            return json_error("photo_ids required", 400)

        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return json_error("photo_ids must contain only integers", 400)
            if raw not in seen:
                photo_ids.append(raw)
                seen.add(raw)
        if not photo_ids:
            return json_error("photo_ids required", 400)
        if len(photo_ids) > 1000:
            return json_error("too many photo_ids", 400)

        keyword_id = extract_keyword_id(body)
        place_id = extract_place_id(body)
        if keyword_id is None and not place_id:
            return json_error("missing place_id", 400)

        db = get_db()
        for pid in photo_ids:
            edit_error = location_errors.photo_location_edit_error(db, pid)
            if edit_error is not None:
                return edit_error

        if keyword_id is not None:
            keyword_error = location_errors.location_keyword_edit_error(db, keyword_id)
            if keyword_error is not None:
                return keyword_error

            location_name = "unknown"
            items = []
            for pid in photo_ids:
                db.set_photo_location(pid, keyword_id)
                queue_location_sync_if_enabled(db, pid, _commit=False)
                loc = serialize_photo_location(db, pid)
                if loc and location_name == "unknown":
                    location_name = loc.get("name") or "unknown"
                items.append({
                    "photo_id": pid,
                    "old_value": "",
                    "new_value": str(keyword_id),
                })
            if items:
                db.record_edit(
                    "location_set",
                    f"set location: {location_name} on {len(items)} photos",
                    str(keyword_id),
                    items,
                    is_batch=True,
                    _commit=False,
                )
            db.conn.commit()
            db._prune_edit_history()
            return jsonify({
                "ok": True,
                "updated": len(items),
                "location": serialize_photo_location(db, photo_ids[0]),
            })

        details = normalize_client_place_details(body)
        if details is None:
            import config as cfg
            maps_config = cfg.load()
            key = maps_config.get("google_maps_api_key", "")
            if not key:
                return location_errors.google_maps_not_configured_error()
            details = places.place_details_for_language(
                place_id,
                key,
                places.result_language(maps_config),
            )
            if details is None:
                return location_errors.google_place_not_found_error()

        try:
            leaf_id = db.upsert_place_chain(details)
        except RuntimeError as err:
            return location_name_conflict_response(err)

        items = []
        for pid in photo_ids:
            db.set_photo_location(pid, leaf_id)
            queue_location_sync_if_enabled(db, pid, _commit=False)
            items.append({
                "photo_id": pid,
                "old_value": "",
                "new_value": str(leaf_id),
            })
        if items:
            db.record_edit(
                "location_set",
                f"set location: {details.get('name', 'unknown')} on {len(items)} photos",
                str(leaf_id),
                items,
                is_batch=True,
                _commit=False,
            )
        db.conn.commit()
        db._prune_edit_history()
        return jsonify({
            "ok": True,
            "updated": len(items),
            "location": serialize_photo_location(db, photo_ids[0]),
        })

    @blueprint.route("/api/batch/location/from-exif", methods=["POST"])
    def api_batch_set_photo_locations_from_exif():
        """Resolve each photo's EXIF GPS to a place and optionally apply it.

        Body:
          - ``{"photo_ids": [1, 2], "apply": false}``
          - ``{"collection_id": 7, "apply": true}``

        Preview mode performs/caches reverse-geocode lookups and returns
        grouped assignments without linking keywords. Apply mode reuses the
        same resolution path, then writes each photo's own resolved place.
        """
        body = request.get_json(silent=True) or {}
        apply_changes = body.get("apply") is True
        db = get_db()

        payload, error = bulk_gps_location_payload(db, body)
        if error is not None:
            return error
        if not apply_changes:
            payload.pop("_details_by_place_id", None)
            return jsonify(payload)

        details_by_place_id = payload.pop("_details_by_place_id", {})
        keyword_id_by_place_id = {}
        items = []
        group_errors = []
        for group in payload["groups"]:
            place_id = group["place_id"]
            details = details_by_place_id.get(place_id)
            if not details:
                group_errors.append({
                    "place_id": place_id,
                    "summary": group.get("summary") or place_id,
                    "error": "missing_details",
                    "code": "missing_details",
                    "message": (
                        "Google Maps didn’t return enough information for this "
                        "place. Search again and choose another result."
                    ),
                })
                continue
            try:
                leaf_id = db.upsert_place_chain(details)
            except RuntimeError as err:
                group_errors.append({
                    "place_id": place_id,
                    "summary": group.get("summary") or place_id,
                    **location_name_conflict_payload(err),
                })
                continue
            keyword_id_by_place_id[place_id] = leaf_id

            for pid in group["photo_ids"]:
                db.set_photo_location(pid, leaf_id)
                queue_location_sync_if_enabled(db, pid, _commit=False)
                items.append({
                    "photo_id": pid,
                    "old_value": "",
                    "new_value": str(leaf_id),
                })

        if items:
            place_count = len(keyword_id_by_place_id)
            db.record_edit(
                "location_set",
                (
                    f"resolved GPS locations for {len(items)} "
                    f"{'photo' if len(items) == 1 else 'photos'} "
                    f"across {place_count} "
                    f"{'place' if place_count == 1 else 'places'}"
                ),
                "from_exif",
                items,
                is_batch=True,
                _commit=False,
            )
        db.conn.commit()
        if items:
            db._prune_edit_history()

        payload["updated"] = len(items)
        payload["group_errors"] = group_errors
        payload["keyword_ids"] = {
            place_id: keyword_id
            for place_id, keyword_id in keyword_id_by_place_id.items()
        }
        return jsonify(payload)

    @blueprint.route("/api/batch/best-batch-flags", methods=["POST"])
    def api_batch_best_batch_flags():
        db = get_db()
        body = request.get_json(silent=True) or {}
        best_photo_id = body.get("best_photo_id")
        reject_photo_ids = body.get("reject_photo_ids", [])
        if isinstance(best_photo_id, bool) or not isinstance(best_photo_id, int):
            return json_error("best_photo_id must be an integer")
        if not isinstance(reject_photo_ids, list):
            return json_error("reject_photo_ids must be a list")
        normalized_reject_ids = []
        seen_reject_ids = set()
        for pid in reject_photo_ids:
            if isinstance(pid, bool) or not isinstance(pid, int):
                return json_error("reject_photo_ids must contain only integers")
            if pid == best_photo_id:
                return json_error("best_photo_id cannot also be rejected")
            if pid not in seen_reject_ids:
                normalized_reject_ids.append(pid)
                seen_reject_ids.add(pid)

        desired_flags = {best_photo_id: "flagged"}
        desired_flags.update({pid: "rejected" for pid in normalized_reject_ids})
        photo_ids = list(desired_flags.keys())
        photos_map = db.get_photos_by_ids(photo_ids)
        if len(photos_map) != len(photo_ids):
            return json_error("One or more photos were not found", 404)
        try:
            for pid in photo_ids:
                db._verify_photo_in_workspace(pid)
        except ValueError as e:
            return json_error(str(e), 403)

        items = []
        for pid in photo_ids:
            new_flag = desired_flags[pid]
            old_flag = photos_map[pid]["flag"]
            db.conn.execute(
                "UPDATE photos SET flag = ? WHERE id = ?",
                (new_flag, pid),
            )
            db.queue_flag_change_if_enabled(pid, new_flag, _commit=False)
            items.append({
                "photo_id": pid,
                "old_value": old_flag,
                "new_value": new_flag,
            })
        reject_count = len(normalized_reject_ids)
        description = (
            f"Best Batch: flagged best photo and rejected {reject_count} "
            f"{'photo' if reject_count == 1 else 'photos'}"
        )
        db.record_edit(
            "flag",
            description,
            "best_batch_apply",
            items,
            is_batch=True,
            _commit=False,
        )
        db.conn.commit()
        db._prune_edit_history()
        return jsonify({
            "ok": True,
            "updated": len(items),
            "photos": {
                str(pid): {"flag": desired_flags[pid]}
                for pid in photo_ids
            },
        })

    @blueprint.route("/api/batch/keyword", methods=["POST"])
    def api_batch_keyword():
        db = get_db()
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        photo_ids, error = parse_selection_photo_ids(
            db, body, json_error=json_error, limit=None,
        )
        if error is not None:
            return error
        keyword_id = body.get("keyword_id")
        name = body.get("name", "")
        name = name.strip() if isinstance(name, str) else ""
        if not photo_ids:
            return json_error("photo_ids required")
        if keyword_id is not None:
            if isinstance(keyword_id, bool) or not isinstance(keyword_id, int):
                return json_error("keyword_id must be an integer")
            keyword_row = db.conn.execute(
                "SELECT id, name FROM keywords WHERE id = ?", (keyword_id,)
            ).fetchone()
            if keyword_row is None:
                return json_error("keyword not found", 404)
            kid = keyword_row["id"]
            name = keyword_row["name"]
        else:
            if not name:
                return json_error("photo_ids and name required")
            # Route kw_type through add_keyword so its type-reconciliation logic
            # runs (preserves existing user-typed rows; only upgrades 'general').
            # See api_add_keyword for the rationale.
            kw_type_raw = body.get("type")
            kw_type = (
                kw_type_raw
                if isinstance(kw_type_raw, str) and kw_type_raw in KEYWORD_TYPES
                else None
            )
            try:
                kid = db.add_keyword(name, kw_type=kw_type)
            except ValueError as exc:
                return json_error(str(exc))
            # Queue/record the stored spelling (see api_add_keyword).
            stored = db.conn.execute(
                "SELECT name FROM keywords WHERE id = ?", (kid,)
            ).fetchone()
            if stored:
                name = stored["name"]

        already_tagged = set()
        batch_size = 800
        for i in range(0, len(photo_ids), batch_size):
            chunk = list(photo_ids[i:i + batch_size])
            placeholders = ",".join("?" for _ in chunk)
            existing_rows = db.conn.execute(
                f"""SELECT photo_id FROM photo_keywords
                    WHERE keyword_id = ? AND photo_id IN ({placeholders})""",
                [kid] + chunk,
            ).fetchall()
            already_tagged.update(row["photo_id"] for row in existing_rows)
        added_ids = [pid for pid in photo_ids if pid not in already_tagged]

        with db.conn:
            for pid in added_ids:
                db.tag_photo(pid, kid, source='manual', _commit=False)
                queue_keyword_add(db, pid, name, _commit=False)
            items = [{'photo_id': pid, 'old_value': '', 'new_value': str(kid)} for pid in added_ids]
            if items:
                db.record_edit('keyword_add', f'Added "{name}" to {len(added_ids)} photos',
                               str(kid), items, is_batch=True, _commit=False)
        if items:
            db._prune_edit_history()
        return jsonify({"ok": True, "updated": len(added_ids)})

    @blueprint.route("/api/batch/wildlife-excluded", methods=["POST"])
    def api_batch_wildlife_excluded():
        """Include or exclude a selection from wildlife processing.

        Missing or out-of-workspace IDs are skipped rather than rejecting
        the entire batch so the endpoint stays consistent with
        ``/api/selection/wildlife-state``: the state endpoint aggregates
        over the accessible subset, and this endpoint applies the change
        to the same subset instead of failing when the panel-supplied
        selection contains a stray ID. The count of skipped IDs is
        returned so the caller can surface it in the same transparent way
        as the state endpoint's ``missing_count``.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("photo_ids", [])
        excluded = body.get("excluded")
        if not isinstance(raw_ids, list) or not raw_ids:
            return json_error("photo_ids required")
        if not isinstance(excluded, bool):
            return json_error("excluded must be a boolean")

        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return json_error("photo_ids must be integers")
            if raw not in seen:
                photo_ids.append(raw)
                seen.add(raw)

        ws_id = db._ws_id()
        accessible_state = {}
        batch_size = 800
        for i in range(0, len(photo_ids), batch_size):
            chunk = photo_ids[i:i + batch_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                f"""SELECT p.id, COALESCE(p.wildlife_excluded, 0) AS excluded
                    FROM photos p
                    WHERE p.id IN ({placeholders})
                      AND EXISTS (
                          SELECT 1 FROM workspace_folders wf
                          WHERE wf.folder_id = p.folder_id
                            AND wf.workspace_id = ?
                      )""",
                [*chunk, ws_id],
            ).fetchall()
            for row in rows:
                accessible_state[row["id"]] = int(row["excluded"])
        accessible_ids = [pid for pid in photo_ids if pid in accessible_state]
        skipped_count = len(photo_ids) - len(accessible_ids)

        desired = 1 if excluded else 0
        changed_ids = [
            photo_id
            for photo_id in accessible_ids
            if accessible_state[photo_id] != desired
        ]
        items = []
        for photo_id in changed_ids:
            old_value = "1" if accessible_state[photo_id] else "0"
            db.conn.execute(
                "UPDATE photos SET wildlife_excluded = ? WHERE id = ?",
                (desired, photo_id),
            )
            items.append({
                "photo_id": photo_id,
                "old_value": old_value,
                "new_value": "1" if excluded else "0",
            })

        if items:
            description = (
                "Excluded from wildlife classification"
                if excluded
                else "Included in wildlife classification"
            )
            db.record_edit(
                "wildlife_excluded",
                f"{description}: {len(items)} photos",
                "1" if excluded else "0",
                items,
                is_batch=True,
                _commit=False,
            )
            db.conn.commit()
            db._prune_edit_history()

        return jsonify({
            "ok": True,
            "updated": len(changed_ids),
            "wildlife_excluded": excluded,
            "photo_ids": changed_ids,
            "skipped_count": skipped_count,
        })

    @blueprint.route("/api/batch/keyword-remove", methods=["POST"])
    def api_batch_keyword_remove():
        db = get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        keyword_id = body.get("keyword_id")
        if not isinstance(photo_ids, list) or not photo_ids:
            return json_error("photo_ids required")
        if isinstance(keyword_id, bool) or not isinstance(keyword_id, int):
            return json_error("keyword_id must be an integer")

        clean_ids = []
        seen = set()
        for raw in photo_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return json_error("photo_ids must be integers")
            if raw not in seen:
                clean_ids.append(raw)
                seen.add(raw)
        if not clean_ids:
            return json_error("photo_ids required")

        keyword_row = db.conn.execute(
            "SELECT id, name, type FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        if keyword_row is None:
            return json_error("keyword not found", 404)

        for pid in clean_ids:
            if not db._photo_in_workspace(pid):
                return json_error(
                    f"Photo {pid} does not belong to the active workspace", 403
                )

        tagged_ids = []
        batch_size = 800
        for i in range(0, len(clean_ids), batch_size):
            chunk = clean_ids[i:i + batch_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                f"""SELECT photo_id FROM photo_keywords
                    WHERE keyword_id = ? AND photo_id IN ({placeholders})""",
                [keyword_id] + chunk,
            ).fetchall()
            tagged_ids.extend(row["photo_id"] for row in rows)

        tagged_set = set(tagged_ids)
        removed_ids = [pid for pid in clean_ids if pid in tagged_set]
        name = keyword_row["name"]
        is_location = (keyword_row["type"] or "") == "location"
        for pid in removed_ids:
            db.untag_photo(pid, keyword_id)
            queue_keyword_remove(db, pid, name)
            # See ``api_remove_keyword``: a ``keyword_remove`` on a
            # ``type='location'`` tag leaves the sidecar's
            # ``vireo:locationKeywords`` marker and ownership claim in
            # place. Queue a ``location`` change so the next sync clears
            # the marker (or rewrites it to a still-tagged location).
            if is_location:
                queue_location_sync_if_enabled(db, pid)

        items = [
            {"photo_id": pid, "old_value": str(keyword_id), "new_value": ""}
            for pid in removed_ids
        ]
        if items:
            db.record_edit(
                "keyword_remove",
                f'Removed "{name}" from {len(removed_ids)} photos',
                str(keyword_id),
                items,
                is_batch=True,
            )
        return jsonify({"ok": True, "updated": len(removed_ids)})

    @blueprint.route("/api/batch/delete", methods=["POST"])
    def api_batch_delete():
        """Delete photos from Vireo, optionally moving files to trash."""
        db = get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids", [])
        mode = body.get("mode", "vireo")
        include_companions = body.get("include_companions", False)

        try:
            result = run_batch_delete(db, photo_ids, mode, include_companions)
        except ValueError as exc:
            return json_error(str(exc))
        if result.get("deleted"):
            invalidate_missing_originals()
        return jsonify(result)

    return blueprint
