"""Per-photo keyword and location edits under ``/api/photos/<id>``.

Add / remove one keyword (``/keywords``), set a photo's location from a place
or keyword (``/location``), from free text (``/location/text``), and clear it.
Request parsing and payloads come from ``web.location_edits``; sidecar
queueing from ``services.pending_changes``. The batch equivalents stay with
the batch routes.
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
    location_name_conflict_response,
    normalize_client_place_details,
    serialize_photo_location,
)
from web.responses import photo_not_found_error


def create_photo_location_keywords_blueprint(
    get_db, json_error, *, location_errors,
):
    """Build the per-photo keyword/location edit blueprint.

    ``location_errors`` is the app's one ``web.location_edits.LocationErrors``
    (bound to ``json_error``), shared with the batch, place and
    location-review routes.
    """
    blueprint = Blueprint("photo_location_keywords", __name__)

    @blueprint.route("/api/photos/<int:photo_id>/keywords", methods=["POST"])
    def api_add_keyword(photo_id):
        db = get_db()
        if db.get_photo(photo_id, verify_workspace=True) is None:
            return photo_not_found_error()
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        keyword_id = body.get("keyword_id")
        name = body.get("name", "")
        name = name.strip() if isinstance(name, str) else ""
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
                return json_error("name required")
            # Validate kw_type at the boundary (isinstance guard against
            # non-hashable JSON; membership against the canonical enum).
            # Pass it through to add_keyword so its type-reconciliation logic
            # runs — a post-update SQL `UPDATE keywords SET type = ?` would
            # silently rewrite an existing user-typed row (e.g. someone's
            # 'individual' Charlie) and bypass the taxonomy/general upgrade
            # rules in add_keyword.
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
            # Queue/record the stored spelling: add_keyword normalizes
            # punctuation and applies the species casing convention, so it
            # can differ from the raw request name.
            stored = db.conn.execute(
                "SELECT name FROM keywords WHERE id = ?", (kid,)
            ).fetchone()
            if stored:
                name = stored["name"]
        # tag_photo is INSERT OR IGNORE, so a repeated Add click on a
        # keyword the photo already carries would still queue a
        # keyword_add sidecar change and record a keyword_add edit whose
        # undo calls untag_photo — removing the pre-existing tag. Skip
        # the pending/history bookkeeping when the row already exists, so
        # the second click is a true no-op (mirrors the batch route's
        # already_tagged precheck).
        already_tagged = db.conn.execute(
            "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (photo_id, kid),
        ).fetchone() is not None
        if already_tagged:
            return jsonify({"ok": True, "keyword_id": kid})
        # source='manual': a person clicked Add. Stamping the association
        # itself keeps authorship recoverable after _prune_edit_history()
        # drops the keyword_add entry recorded just below.
        db.tag_photo(photo_id, kid, source='manual')
        queue_keyword_add(db, photo_id, name)
        db.record_edit('keyword_add', f'Added keyword "{name}"', str(kid),
                       [{'photo_id': photo_id, 'old_value': '', 'new_value': str(kid)}])
        return jsonify({"ok": True, "keyword_id": kid})

    @blueprint.route(
        "/api/photos/<int:photo_id>/keywords/<int:keyword_id>", methods=["DELETE"]
    )
    def api_remove_keyword(photo_id, keyword_id):
        db = get_db()
        if db.get_photo(photo_id, verify_workspace=True) is None:
            return photo_not_found_error()
        keywords = db.get_photo_keywords(photo_id)
        kw_name = ""
        kw_type = ""
        for k in keywords:
            if k["id"] == keyword_id:
                kw_name = k["name"]
                kw_type = k["type"] or ""
                break
        else:
            return jsonify({"ok": True})
        db.untag_photo(photo_id, keyword_id)
        queue_keyword_remove(db, photo_id, kw_name)
        # A ``keyword_remove`` on a ``type='location'`` tag strips the flat
        # and hierarchical entries but leaves ``vireo:locationKeywords`` and
        # its ownership claim in the sidecar. If the user later recreates
        # that keyword in Lightroom and assigns another place in Vireo,
        # ``set_location_keywords`` treats the stale marker as authoritative
        # and can delete the user's new entry. Queue a ``location`` change so
        # ``sync_to_xmp`` clears the marker (or rewrites it to a still-
        # tagged location, if the photo has one) on the next sync.
        if kw_type == "location":
            queue_location_sync_if_enabled(db, photo_id)
        db.record_edit('keyword_remove', f'Removed keyword "{kw_name}"', str(keyword_id),
                       [{'photo_id': photo_id, 'old_value': str(keyword_id), 'new_value': ''}])
        return jsonify({"ok": True})

    # Location keywords don't propagate to dc:subject sidecars — structured XMP (exif:GPS*, Iptc4xmpCore:Location) is a future feature.
    @blueprint.route("/api/photos/<int:photo_id>/location", methods=["POST"])
    def api_set_photo_location(photo_id):
        """Attach a Google place to ``photo_id`` via the autocomplete flow.

        Body: ``{"place_id": "ChIJ..."}``. Looks up the place via
        :func:`places.place_details`, upserts the leaf + parent chain into
        ``keywords``, and links the leaf to the photo (replacing any existing
        ``type='location'`` link).
        """
        body = request.get_json(silent=True) or {}
        keyword_id = extract_keyword_id(body)
        place_id = extract_place_id(body)
        if keyword_id is None and not place_id:
            return json_error("missing place_id", 400)

        db = get_db()
        # Guard against stale clients (e.g. tab open after photo deleted).
        # Without this, set_photo_location's INSERT into photo_keywords
        # raises a FK IntegrityError that surfaces as a 500.
        edit_error = location_errors.photo_location_edit_error(db, photo_id)
        if edit_error is not None:
            return edit_error

        if keyword_id is not None:
            keyword_error = location_errors.location_keyword_edit_error(db, keyword_id)
            if keyword_error is not None:
                return keyword_error
            db.set_photo_location(photo_id, keyword_id)
            queue_location_sync_if_enabled(db, photo_id)
            location = serialize_photo_location(db, photo_id)
            db.record_edit(
                'location_set',
                f"set location: {location.get('name', 'unknown') if location else 'unknown'}",
                str(keyword_id),
                [{'photo_id': photo_id, 'old_value': '', 'new_value': str(keyword_id)}],
            )
            return jsonify({"location": location})

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
            # _upsert_one_keyword raises RuntimeError when the parent-chain
            # build hits an existing keyword of a different type at the same
            # (name, parent_id). Mirror /api/keywords/<id>/link-place's 409.
            return location_name_conflict_response(err)
        db.set_photo_location(photo_id, leaf_id)
        queue_location_sync_if_enabled(db, photo_id)
        db.record_edit(
            'location_set',
            f"set location: {details.get('name', 'unknown')}",
            str(leaf_id),
            [{'photo_id': photo_id, 'old_value': '', 'new_value': str(leaf_id)}],
        )
        return jsonify({"location": serialize_photo_location(db, photo_id)})

    @blueprint.route("/api/photos/<int:photo_id>/location/text", methods=["POST"])
    def api_set_photo_location_text(photo_id):
        """Attach a free-text location keyword (no Google data) to ``photo_id``.

        Body: ``{"name": "the meadow behind the cabin"}``. Used when the user
        types a name and hits Enter without picking a Google suggestion, or
        when no API key is configured. Reuses the ``location_set`` audit
        action_type so the audit log filters consistently across both paths.
        """
        body = request.get_json(silent=True) or {}
        name = body.get("name") or ""
        if not name.strip():
            return json_error("missing name", 400)
        stripped = name.strip()
        # Reject at assignment time: Lightroom reserves ``|`` for the
        # hierarchy delimiter, and ``SidecarEditor.set_location_keywords``
        # cannot round-trip it. Catching it here means the pending change
        # never gets queued in the first place, so no later sync silently
        # loses it.
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
        edit_error = location_errors.photo_location_edit_error(db, photo_id)
        if edit_error is not None:
            return edit_error
        try:
            leaf_id = db.get_or_create_text_location(stripped)
        except ValueError:
            # Defensive: validation above should already catch empty input
            # and pipe characters.
            return json_error("missing name", 400)
        if latitude is not None:
            db.conn.execute(
                "UPDATE keywords SET latitude = COALESCE(latitude, ?), "
                "longitude = COALESCE(longitude, ?) WHERE id = ?",
                (latitude, longitude, leaf_id),
            )
            db.conn.commit()
        db.set_photo_location(photo_id, leaf_id)
        queue_location_sync_if_enabled(db, photo_id)
        db.record_edit(
            'location_set',
            f"set location: {stripped}",
            str(leaf_id),
            [{'photo_id': photo_id, 'old_value': '', 'new_value': str(leaf_id)}],
        )
        return jsonify({"location": serialize_photo_location(db, photo_id)})

    @blueprint.route("/api/photos/<int:photo_id>/location", methods=["DELETE"])
    def api_clear_photo_location(photo_id):
        """Remove all ``type='location'`` keyword links for ``photo_id``."""
        db = get_db()
        edit_error = location_errors.photo_location_edit_error(db, photo_id)
        if edit_error is not None:
            return edit_error
        db.clear_photo_location(photo_id)
        queue_location_sync_if_enabled(db, photo_id)
        db.record_edit(
            'location_clear',
            "cleared location",
            '',
            [{'photo_id': photo_id, 'old_value': '', 'new_value': ''}],
        )
        return jsonify({"ok": True})

    return blueprint
