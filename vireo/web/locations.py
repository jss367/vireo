"""Locations: place lookups, keyword-to-place linking, and location review.

``/api/places/reverse-geocode`` turns a coordinate into a Google place
suggestion through the SQLite grid cache. ``/api/keywords/<id>/link-place``
attaches Google place data to an existing location keyword; it sits here
rather than in ``web/keywords.py`` because it is a place operation (Google
lookup, location error responses, location sidecar queueing). The
``/api/location-review/*`` routes back the Review Photo Locations page:
grouping photos by coordinates, capture time, or GPS discrepancy for
review, recording keep/assigned decisions on discrepancies, and nearby
saved-place suggestions.
"""

from __future__ import annotations

import math

import location_review
import places
from flask import Blueprint, current_app, g, jsonify, request
from services.pending_changes import queue_location_sync_if_enabled
from web.location_edits import (
    extract_place_id,
    location_name_conflict_response,
    normalize_client_place_details,
    walk_parent_chain,
)
from xmp import read_sync_preview_metadata


def _location_review_groups(photos, cluster_radius_m=750.0):
    """Cluster photos by their observed coordinates for human review.

    Reverse-geocoded place ids are deliberately not involved: they are
    suggestions for the reviewer, not the identity of a coordinate group.
    A small spatial hash keeps the neighbor search close to O(n) for large
    libraries while connected components allow a photographed route to
    remain one reviewable area.
    """
    valid = []
    unresolved = []
    for order, photo in enumerate(photos):
        lat = photo["latitude"]
        lng = photo["longitude"]
        if lat is None or lng is None:
            unresolved.append({
                "photo_id": photo["id"],
                "filename": photo["filename"],
                "reason": "missing_gps",
            })
            continue
        try:
            lat = float(lat)
            lng = float(lng)
        except (TypeError, ValueError):
            lat = lng = math.nan
        if (
            not math.isfinite(lat)
            or not math.isfinite(lng)
            or not -90 <= lat <= 90
            or not -180 <= lng <= 180
        ):
            unresolved.append({
                "photo_id": photo["id"],
                "filename": photo["filename"],
                "reason": "invalid_gps",
            })
            continue
        valid.append({
            "photo": photo,
            "order": order,
            "lat": lat,
            "lng": lng,
        })

    if not valid:
        return [], unresolved

    earth_radius_m = 6_371_000.0
    reference_lat = math.radians(
        sum(item["lat"] for item in valid) / len(valid)
    )
    reference_lng = valid[0]["lng"]

    def projected(item):
        lng_delta = ((item["lng"] - reference_lng + 180.0) % 360.0) - 180.0
        return (
            earth_radius_m * math.radians(lng_delta) * math.cos(reference_lat),
            earth_radius_m * math.radians(item["lat"]),
        )

    parents = list(range(len(valid)))

    def find(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left, right):
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    buckets = {}
    radius_sq = cluster_radius_m * cluster_radius_m
    for index, item in enumerate(valid):
        x_coord, y_coord = projected(item)
        item["x"] = x_coord
        item["y"] = y_coord
        cell = (
            math.floor(x_coord / cluster_radius_m),
            math.floor(y_coord / cluster_radius_m),
        )
        for x_offset in (-1, 0, 1):
            for y_offset in (-1, 0, 1):
                for neighbor in buckets.get(
                    (cell[0] + x_offset, cell[1] + y_offset), []
                ):
                    dx = x_coord - valid[neighbor]["x"]
                    dy = y_coord - valid[neighbor]["y"]
                    if dx * dx + dy * dy <= radius_sq:
                        union(index, neighbor)
        buckets.setdefault(cell, []).append(index)

    components = {}
    for index, item in enumerate(valid):
        components.setdefault(find(index), []).append(item)

    ordered = sorted(
        components.values(),
        key=lambda items: min(item["order"] for item in items),
    )
    groups = []
    for group_index, items in enumerate(ordered, start=1):
        center_lat = sum(item["lat"] for item in items) / len(items)
        # Longitudes were normalized around the first input point above;
        # use the same frame so groups around the antimeridian do not get
        # a center near Greenwich.
        lng_deltas = [
            ((item["lng"] - reference_lng + 180.0) % 360.0) - 180.0
            for item in items
        ]
        center_lng = (
            reference_lng + sum(lng_deltas) / len(lng_deltas) + 180.0
        ) % 360.0 - 180.0
        center_x = sum(item["x"] for item in items) / len(items)
        center_y = sum(item["y"] for item in items) / len(items)
        spread_m = max(
            math.hypot(item["x"] - center_x, item["y"] - center_y)
            for item in items
        )
        timestamps = sorted(
            item["photo"]["timestamp"]
            for item in items
            if item["photo"]["timestamp"]
        )
        photo_data = [{
            "id": item["photo"]["id"],
            "filename": item["photo"]["filename"],
            "companion_path": item["photo"]["companion_path"],
            "timestamp": item["photo"]["timestamp"],
            "latitude": item["lat"],
            "longitude": item["lng"],
        } for item in sorted(items, key=lambda item: item["order"])]
        groups.append({
            "id": f"group-{group_index}",
            "count": len(photo_data),
            "photo_ids": [photo["id"] for photo in photo_data],
            "photos": photo_data,
            "sample_filenames": [
                photo["filename"] for photo in photo_data[:3]
            ],
            "center": {"lat": center_lat, "lng": center_lng},
            "bounds": {
                "south": min(item["lat"] for item in items),
                "west": min(item["lng"] for item in items),
                "north": max(item["lat"] for item in items),
                "east": max(item["lng"] for item in items),
            },
            "spread_m": round(spread_m, 1),
            "captured_from": timestamps[0] if timestamps else None,
            "captured_to": timestamps[-1] if timestamps else None,
        })
    return groups, unresolved


def _serialize_keyword(db, keyword_id):
    """Return a summary dict for a single ``type='location'`` keyword row.

    Same shape as :func:`serialize_photo_location` (leaf fields + a
    broadest-first parent chain), but keyed on the keyword id directly
    rather than via a photo. Used by the ``link-place`` route, which
    operates on a keyword and isn't tied to a photo.

    Returns ``None`` if the keyword does not exist.
    """
    leaf = db.conn.execute(
        "SELECT id, name, place_id, latitude, longitude, parent_id "
        "FROM keywords WHERE id = ?",
        (keyword_id,),
    ).fetchone()
    if leaf is None:
        return None
    return {
        "keyword_id": leaf["id"],
        "name": leaf["name"],
        "place_id": leaf["place_id"],
        "latitude": leaf["latitude"],
        "longitude": leaf["longitude"],
        "parent_chain": walk_parent_chain(db, leaf["parent_id"]),
    }


def create_locations_blueprint(
    get_db,
    json_error,
    *,
    location_errors,
    normalize_photo_id_list,
    bulk_gps_location_source_ids,
    location_keyword_photo_ids,
    google_reverse_geocode,
    decode_cached_reverse_geocode,
    encode_cached_reverse_geocode,
    summarize_details,
):
    """Build the locations blueprint.

    ``location_errors`` is the app's one ``web.location_edits.LocationErrors``.
    The rest are shared with the EXIF-GPS batch location route
    (``/api/batch/location/from-exif`` and its ``_bulk_gps_location_payload``)
    still in ``create_app``, so they are injected rather than moved:
    ``normalize_photo_id_list`` and ``bulk_gps_location_source_ids`` parse the
    photo selection, ``location_keyword_photo_ids`` finds photos that already
    have a location, ``google_reverse_geocode`` is the result-language-aware
    Google lookup, ``decode_cached_reverse_geocode`` /
    ``encode_cached_reverse_geocode`` read and write the language-tagged
    reverse-geocode cache rows, and ``summarize_details`` renders the
    one-line place summary.
    """
    blueprint = Blueprint("locations", __name__)

    @blueprint.route("/api/location-review/preview", methods=["POST"])
    def api_location_review_preview():
        """Preview coordinate, capture-time, or GPS discrepancy groups without edits."""
        body = request.get_json(silent=True) or {}
        mode = body.get("mode", "coordinates")
        if mode not in ("coordinates", "time", "discrepancies"):
            return json_error("mode must be coordinates, time, or discrepancies", 400)
        minimum_distance = body.get("minimum_distance_m", 500)
        if type(minimum_distance) not in (int, float) or not 0 <= minimum_distance <= 20015087:
            return json_error("minimum_distance_m must be between 0 and 20015087 meters", 400)
        include_reviewed = body.get("include_reviewed", False)
        if type(include_reviewed) is not bool:
            return json_error("include_reviewed must be a boolean", 400)
        gap_minutes = body.get("gap_minutes", 60)
        if type(gap_minutes) is not int or gap_minutes not in (15, 30, 60, 120):
            return json_error("gap_minutes must be 15, 30, 60, or 120", 400)
        db = get_db()
        if body.get("scope") == "all":
            if "photo_ids" in body or "collection_id" in body:
                return json_error("Choose all photos, photo_ids, or collection_id, not multiple sources", 400)
            photo_ids, error = db.get_photo_ids(), None
        else:
            photo_ids, error = bulk_gps_location_source_ids(db, body)
        if error is not None:
            return error
        if not photo_ids:
            return jsonify({
                "total": 0,
                "reviewable": 0,
                "groups": [],
                "unresolved": [],
                "skipped": [],
            })

        photos_map = db.get_photos_by_ids(photo_ids)
        if len(photos_map) != len(photo_ids):
            return json_error("One or more photos were not found", 404)
        for photo_id in photo_ids:
            edit_error = location_errors.photo_location_edit_error(db, photo_id)
            if edit_error is not None:
                return edit_error

        if mode == "discrepancies":
            photos = location_review.gps_discrepancies(db, photo_ids, minimum_distance, include_reviewed)
            return jsonify({
                "total": len(photo_ids), "reviewable": len(photos),
                "groups": location_review.discrepancy_groups(photos), "unresolved": [], "skipped": [],
            })

        assigned_ids = location_keyword_photo_ids(db, photo_ids)
        skipped = [{
            "photo_id": photo_id,
            "filename": photos_map[photo_id]["filename"],
            "reason": "already_has_location",
        } for photo_id in photo_ids if photo_id in assigned_ids]
        review_photos = [
            photos_map[photo_id]
            for photo_id in photo_ids
            if photo_id not in assigned_ids
        ]
        if mode == "time":
            groups = location_review.time_review_groups(review_photos, gap_minutes)
            unresolved = []
            skipped.extend({
                "photo_id": photo["id"], "filename": photo["filename"],
                "reason": "has_coordinates",
            } for photo in review_photos if location_review.has_usable_coordinates(photo))
        else:
            groups, unresolved = _location_review_groups(review_photos)
        return jsonify({
            "total": len(photo_ids),
            "reviewable": sum(group["count"] for group in groups),
            "groups": groups,
            "unresolved": unresolved,
            "skipped": skipped,
        })

    @blueprint.route("/api/location-review/resolve-discrepancies", methods=["POST"])
    def api_resolve_location_discrepancies():
        """Remember an explicit keep decision or re-queue approved GPS corrections."""
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict) or body.get("action") not in ("keep", "assigned"):
            return json_error("action must be keep or assigned", 400)
        photo_ids, error = normalize_photo_id_list(body.get("photo_ids"))
        if error is not None:
            return error
        if len(photo_ids) > 100:
            return json_error("Review at most 100 photos at a time", 400)
        fingerprints = body.get("fingerprints")
        if not isinstance(fingerprints, dict):
            return json_error("Preview fingerprints are required", 400)
        db = get_db()
        # Network sidecars can take longer than SQLite's busy timeout. Read
        # them before taking the writer lock, after authorizing the selection.
        for photo_id in photo_ids:
            error = location_errors.photo_location_edit_error(db, photo_id)
            if error is not None:
                return error
        sidecars = {}

        def capture_sidecar(path):
            if path not in sidecars:
                sidecars[path] = read_sync_preview_metadata(path)
            return sidecars[path]

        location_review.gps_discrepancies(
            db, photo_ids, 0, include_reviewed=True, sidecar_reader=capture_sidecar,
        )
        # Serialize validation and queueing with concurrent assignment edits.
        db.conn.execute("BEGIN IMMEDIATE")
        try:
            for photo_id in photo_ids:
                error = location_errors.photo_location_edit_error(db, photo_id)
                if error is not None:
                    db.conn.rollback()
                    return error
            photos = location_review.gps_discrepancies(
                db, photo_ids, 0, include_reviewed=True, sidecar_reader=sidecars.get,
            )
            current = {photo["id"]: photo for photo in photos}
            if any(pid not in current or fingerprints.get(str(pid)) != current[pid]["fingerprint"] for pid in photo_ids):
                db.conn.rollback()
                return json_error("Location data changed. Reload this review before applying a decision.", 409)
            if body["action"] == "assigned":
                import config as cfg
                if not db.get_effective_config(cfg.load()).get("write_assigned_location_to_xmp", False):
                    db.conn.rollback()
                    return json_error("Enable Write assigned locations to XMP in Settings before queueing corrections.", 409)
                for photo_id in photo_ids:
                    queue_location_sync_if_enabled(db, photo_id, _commit=False)
                    db.conn.execute("DELETE FROM location_gps_reviews WHERE photo_id = ?", (photo_id,))
            else:
                for photo_id in photo_ids:
                    if db.conn.execute(
                        "SELECT 1 FROM pending_changes WHERE photo_id = ? AND change_type = 'location' LIMIT 1",
                        (photo_id,),
                    ).fetchone():
                        db.conn.rollback()
                        return json_error("A selected photo has a pending location change. Review that change before keeping its GPS.", 409)
                db.conn.executemany(
                    "INSERT OR REPLACE INTO location_gps_reviews(photo_id, fingerprint) VALUES (?, ?)",
                    [(pid, current[pid]["fingerprint"]) for pid in photo_ids],
                )
            db.record_edit(
                'location_gps_review',
                'Queue assigned place GPS' if body["action"] == "assigned" else 'Keep photo GPS',
                body["action"],
                [{'photo_id': pid, 'old_value': current[pid]["fingerprint"], 'new_value': body["action"]}
                 for pid in photo_ids],
                is_batch=len(photo_ids) > 1, _commit=False,
            )
            db.conn.commit()
        except BaseException:
            db.conn.rollback()
            raise
        db._prune_edit_history()
        return jsonify({"reviewed": len(photo_ids), "queued": len(photo_ids) if body["action"] == "assigned" else 0})

    @blueprint.route("/api/location-review/saved-suggestions")
    def api_location_review_saved_suggestions():
        """Return nearby location keywords already used in this workspace."""
        try:
            lat = float(request.args.get("lat", ""))
            lng = float(request.args.get("lng", ""))
            radius_m = float(request.args.get("radius_m", "25000"))
        except (TypeError, ValueError):
            return json_error("invalid coordinates", 400)
        if (
            not math.isfinite(lat)
            or not math.isfinite(lng)
            or not math.isfinite(radius_m)
            or not -90 <= lat <= 90
            or not -180 <= lng <= 180
            or radius_m <= 0
        ):
            return json_error("invalid coordinates", 400)
        radius_m = min(radius_m, 100_000.0)

        db = get_db()
        rows = db.conn.execute(
            """SELECT k.id, k.name, k.place_id, k.latitude, k.longitude,
                      k.parent_id, COUNT(DISTINCT pk.photo_id) AS photo_count
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ? AND k.type = 'location'
                 AND k.latitude IS NOT NULL AND k.longitude IS NOT NULL
               GROUP BY k.id""",
            (db._ws_id(),),
        ).fetchall()

        def distance_m(row):
            lat1 = math.radians(lat)
            lat2 = math.radians(float(row["latitude"]))
            delta_lat = lat2 - lat1
            delta_lng = math.radians(float(row["longitude"]) - lng)
            value = (
                math.sin(delta_lat / 2) ** 2
                + math.cos(lat1) * math.cos(lat2)
                * math.sin(delta_lng / 2) ** 2
            )
            return 2 * 6_371_000.0 * math.asin(min(1.0, math.sqrt(value)))

        suggestions = []
        for row in rows:
            distance = distance_m(row)
            if distance > radius_m:
                continue
            suggestions.append({
                "keyword_id": row["id"],
                "name": row["name"],
                "place_id": row["place_id"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "photo_count": row["photo_count"],
                "distance_m": round(distance, 1),
                "parent_chain": walk_parent_chain(db, row["parent_id"]),
            })
        suggestions.sort(key=lambda item: (item["distance_m"], -item["photo_count"], item["name"]))
        return jsonify({"suggestions": suggestions[:8]})

    @blueprint.route("/api/places/reverse-geocode", methods=["GET"])
    def api_reverse_geocode():
        """Reverse-geocode (lat, lng) via Google, with a SQLite grid cache.

        Query params: ``lat``, ``lng`` (floats). Returns
        ``{"place_id": <str|null>, "summary": <str|null>}``.

        Cache layer is keyed on ~110m grid (see ``Database._reverse_geocode_grid``).
        A row with ``place_id=None`` is a cached negative — Google was previously
        asked and had no match, and we serve null without re-asking.

        When no ``google_maps_api_key`` is configured we degrade to ``null``
        WITHOUT writing to the cache. Caching null in that branch would make
        already-asked grids stay null forever once the user finally adds a
        key, which is exactly the wrong UX.
        """
        try:
            lat = float(request.args.get("lat", ""))
            lng = float(request.args.get("lng", ""))
        except (TypeError, ValueError):
            return json_error("invalid coords", 400)
        # float() accepts "nan" and "inf"; both blow up downstream in
        # _reverse_geocode_grid's int(round(...)). Reject explicitly.
        if not (math.isfinite(lat) and math.isfinite(lng)):
            return json_error("invalid coords", 400)

        import config as cfg

        maps_config = cfg.load()
        language = places.result_language(maps_config)
        db = get_db()
        cached = db.reverse_geocode_cache_get(lat, lng)
        if cached is not None:
            language_matches, details = decode_cached_reverse_geocode(
                cached, language,
            )
            if language_matches:
                if cached["place_id"] is None:
                    # Cached negative — Google previously had no match here.
                    return jsonify({"place_id": None, "summary": None})
                return jsonify({
                    "place_id": cached["place_id"],
                    "summary": summarize_details(details),
                })

        # Cache miss.
        key = maps_config.get("google_maps_api_key", "")
        if not key:
            # Don't cache here — see docstring.
            return jsonify({"place_id": None, "summary": None})

        try:
            details = google_reverse_geocode(lat, lng, key, language)
        except places.PlacesTransientError:
            # OVER_QUERY_LIMIT / REQUEST_DENIED / network blip — Google
            # may answer this later, so do NOT cache. Returning null here
            # just suppresses the EXIF suggestion for this request; the
            # next request will retry Google.
            current_app.logger.warning(
                "reverse_geocode transient failure for lat=%s lng=%s — not caching",
                lat, lng,
            )
            return jsonify({"place_id": None, "summary": None})

        cache_place_id = details.get("place_id") if details else None
        db.reverse_geocode_cache_put(
            lat, lng,
            place_id=cache_place_id,
            response_json=encode_cached_reverse_geocode(details, language),
        )
        if details is None:
            return jsonify({"place_id": None, "summary": None})
        return jsonify({
            "place_id": cache_place_id,
            "summary": summarize_details(details),
        })

    @blueprint.route("/api/keywords/<int:keyword_id>/link-place", methods=["POST"])
    def api_link_keyword_to_place(keyword_id):
        """Attach Google place data to an existing keyword.

        Body: ``{"place_id": "ChIJ..."}``, optionally with a normalized
        ``place`` object from Google Maps JS autocomplete. When the client
        does not provide details, looks up the place via
        :func:`places.place_details` and delegates to
        :meth:`Database.link_keyword_to_place`, which UPDATEs the target row
        in-place — or, if another keyword already owns this ``place_id``,
        re-points the target's ``photo_keywords`` rows onto the canonical row
        and deletes the now-empty target.

        Response: ``{"keyword": <serialized leaf+chain>, "merged": <bool>}``.
        ``merged`` is True when an existing place-bearing row absorbed the
        target.

        Error modes:
        - 400 ``missing place_id`` — empty body.
        - 400 ``no_api_key`` — config has no ``google_maps_api_key``.
        - 404 ``place_not_found`` — Google had no record of ``place_id``.
        - 404 ``keyword_not_found`` — ``keyword_id`` doesn't exist.
        - 409 ``name_conflict`` — the parent chain would clash with an
          existing keyword of a different ``type`` at the same
          ``(name, parent_id)``. Carries a user-facing ``message`` plus an
          ``error_detail`` string from the underlying RuntimeError for
          debugging.
        """
        body = request.get_json(silent=True) or {}
        place_id = extract_place_id(body)
        if not place_id:
            return json_error("missing place_id", 400)

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

        db = get_db()
        try:
            result = db.link_keyword_to_place(keyword_id, details)
        except ValueError as err:
            # Database raises ValueError both for "missing id" and "wrong
            # type". Distinguish so callers can tell a 404 from a 400.
            msg = str(err)
            if "is type" in msg:
                return jsonify({
                    "error": "wrong_keyword_type",
                    "code": "invalid_request",
                    "message": (
                        "Only location keywords can be linked to Google Maps "
                        "places."
                    ),
                    "error_detail": msg,
                    "request_id": getattr(g, "request_id", None),
                }), 400
            return location_errors.keyword_not_found_error()
        except RuntimeError as err:
            # _upsert_one_keyword raises RuntimeError when the parent-chain
            # build hits an existing keyword of a different type at the same
            # (name, parent_id). Surface the message for debugging.
            return location_name_conflict_response(err)

        # Audit log: this action isn't tied to a single photo (it operates on
        # a keyword), so we omit the per-photo items list. ``photo_id`` in
        # ``edit_history_items`` is FK-constrained to ``photos.id``, so a
        # placeholder like 0 would IntegrityError. The ``new_value`` column on
        # the parent ``edit_history`` row carries the canonical keyword id;
        # the ``description`` carries the place name and the source-id pair.
        db.record_edit(
            'location_link',
            (
                f"linked keyword {keyword_id} to place: "
                f"{details.get('name', 'unknown')} "
                f"(canonical keyword_id={result['keyword_id']}, "
                f"merged={result['merged']})"
            ),
            str(result['keyword_id']),
            [],
        )

        photo_rows = db.conn.execute(
            """SELECT DISTINCT pk.photo_id, wf.workspace_id
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE pk.keyword_id = ?""",
            (result["keyword_id"],),
        ).fetchall()
        for row in photo_rows:
            queue_location_sync_if_enabled(
                db, row["photo_id"],
                workspace_id=row["workspace_id"],
                _commit=False,
            )
        if photo_rows:
            db.conn.commit()

        return jsonify({
            "keyword": _serialize_keyword(db, result["keyword_id"]),
            "merged": result["merged"],
        })

    return blueprint
