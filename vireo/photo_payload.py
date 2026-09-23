"""Attach derived per-photo fields to API photo dicts, in place.

These enrichers are shared by Browse, Highlights, Misses, Predictions and
the Process Review payloads. Each takes the request ``Database`` and a list
of photo dicts keyed by ``id`` and batches its lookups so a page of results
costs a handful of queries, not one per photo. Only
``attach_species_representatives`` and ``attach_nested_edit_recipes`` also
accept the ``photo_id`` shape used by cached pipeline results.
``prepare_browse_photo_dicts`` combines them into the Browse grid payload.
"""

import hashlib


def render_key_for_recipe(recipe):
    """Fingerprint an edit recipe for cache-busting rendered-image URLs.

    Thumbnails are served ``Cache-Control: public, max-age=86400``
    (``serve_thumbnail``), so the URL is the only thing that tells a browser
    its copy is out of date: the server deleting the cached JPEG on save is
    invisible to a cache that never asks. Clients append this key to
    ``/thumbnails/<id>.jpg`` so an edit produces a URL the browser has never
    seen.

    Derived from the *whole* canonical recipe rather than a hand-listed
    subset of fields. A subset silently stops busting the cache the moment
    the recipe grows a key nobody remembered to add -- which is how
    mask-based ``local`` adjustments came to leave the grid showing pre-edit
    pixels for a day. ``EDIT_MATH_VERSION`` is folded in for the same reason
    it keys the server-side render caches: a change to the rendering math
    produces different bytes for an unchanged recipe.

    Returns ``None`` for a recipe that renders as a no-op, which leaves the
    URL bare -- correct, because an unedited photo's thumbnail is the same
    image it has always been.
    """
    from image_edits import EDIT_MATH_VERSION, RecipeError, recipe_to_json

    try:
        canonical = recipe_to_json(recipe)
    except (RecipeError, ValueError, TypeError):
        # A recipe the current schema rejects still has to bust the cache:
        # whatever the renderer makes of it, it is not the unedited image.
        # Fingerprint the raw value so the URL at least changes with it.
        canonical = repr(recipe)
    if not canonical:
        return None
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
    return f"{digest}.m{EDIT_MATH_VERSION}"


def attach_species(db, photo_dicts):
    """Attach species keyword names to a list of photo dicts (in-place)."""
    if not photo_dicts:
        return photo_dicts
    ids = [p["id"] for p in photo_dicts]
    species_map = db.get_species_keywords_for_photos(ids)
    for p in photo_dicts:
        p["species"] = species_map.get(p["id"], [])
    return photo_dicts

def attach_location_statuses(db, photo_dicts):
    """Attach the effective coordinate source used by Browse and Map UI."""
    if not photo_dicts:
        return photo_dicts
    ids = [p["id"] for p in photo_dicts if isinstance(p.get("id"), int)]
    statuses = db.get_photo_location_statuses(ids)
    for photo in photo_dicts:
        photo["location_status"] = statuses.get(photo.get("id"), "none")
    return photo_dicts

def attach_species_representatives(db, photo_dicts):
    """Attach species representative state to photo dicts (in-place)."""
    if not photo_dicts:
        return photo_dicts
    ids = []
    for p in photo_dicts:
        pid = p.get("photo_id", p.get("id"))
        if isinstance(pid, int) and not isinstance(pid, bool):
            ids.append(pid)
    species_map = db.get_species_keywords_for_photos(ids)
    # Gate representatives on current DB eligibility so a stale preference
    # row (photo later rejected, folder removed from workspace, or species
    # keyword untagged) doesn't light up a Representative badge on views
    # whose photo dicts lack the `flag` column (notably /api/predictions,
    # whose SELECT only pulls filename/timestamp from photos). The
    # in-loop `p.get("flag") == "rejected"` shortcut still short-circuits
    # rejected photos for views that DO include flag, so this only shifts
    # behavior for the payloads that were previously reading raw prefs.
    representatives = db.get_species_representatives(eligible_only=True)
    for p in photo_dicts:
        pid = p.get("photo_id", p.get("id"))
        if p.get("flag") == "rejected":
            species = []
        else:
            species = species_map.get(pid, [])
        entries = [
            {
                "species": s,
                "is_current_photo": representatives.get(s) == pid,
                "is_species_representative": representatives.get(s) == pid,
            }
            for s in species
        ]
        p["life_list"] = entries
        p["species_representatives"] = entries
        p["is_species_representative"] = any(
            entry["is_species_representative"] for entry in entries
        )
    return photo_dicts

def attach_detections(db, photo_dicts):
    """Attach detection bounding boxes to a list of photo dicts (in-place).

    Each photo gets a `detections` list of {x, y, w, h, confidence,
    category} dicts, with the selected primary first. Photos with no
    detections get an empty list.
    """
    if not photo_dicts:
        return photo_dicts
    ids = [p["id"] for p in photo_dicts]
    det_map = db.get_detections_for_photos(ids)
    for p in photo_dicts:
        p["detections"] = det_map.get(p["id"], [])
    return photo_dicts

def attach_prediction_confidence(db, photo_dicts):
    """Attach the top prediction's confidence to photo dicts (in-place).

    Each photo gets ``prediction_confidence``: a float in [0, 1], or None
    when the photo has no current, unrejected species prediction. This is
    the same value the ``prediction_confidence`` Browse sorts order by, so a
    card showing it explains its own position in the grid.
    """
    if not photo_dicts:
        return photo_dicts
    ids = [p["id"] for p in photo_dicts]
    conf_map = db.get_top_prediction_confidences(ids)
    for p in photo_dicts:
        p["prediction_confidence"] = conf_map.get(p["id"])
    return photo_dicts

def attach_edit_recipes(db, photo_dicts):
    """Attach non-destructive edit recipes to photo dicts (in-place)."""
    if not photo_dicts:
        return photo_dicts
    ids = [p["id"] for p in photo_dicts]
    recipe_map = db.get_photo_edit_recipes(ids)
    for p in photo_dicts:
        recipe = recipe_map.get(p["id"])
        p["edit_recipe"] = recipe
        p["render_key"] = render_key_for_recipe(recipe)
    return photo_dicts

def attach_nested_edit_recipes(db, payload):
    """Attach edit recipes to nested photo-like dicts in an API payload."""
    refs = []

    def visit(value):
        if isinstance(value, dict):
            pid = value.get("photo_id", value.get("id"))
            if (
                isinstance(pid, int)
                and not isinstance(pid, bool)
                and "filename" in value
            ):
                refs.append((value, pid))
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    if not refs:
        return payload
    recipe_map = db.get_photo_edit_recipes(sorted({pid for _, pid in refs}))
    for photo, pid in refs:
        recipe = recipe_map.get(pid)
        photo["edit_recipe"] = recipe
        photo["render_key"] = render_key_for_recipe(recipe)
    attach_species_representatives(db, [photo for photo, _pid in refs])
    return payload


def prepare_browse_photo_dicts(db, photos, stack_items=None):
    """Normalize photo rows and attach optional Browse stack summaries."""
    photo_dicts = [dict(photo) for photo in photos]
    stacks_by_cover = {
        item["cover_id"]: item for item in (stack_items or [])
    }
    # Under a prediction-confidence sort the stacked query reports the
    # score that positioned each item — read off the stack's *leading*
    # member, which is usually not the quality-ranked cover. Keep it so
    # the badge names the number that decided the card's place instead of
    # the cover's own (Codex P2 on PR #1670). Absent for every other sort
    # and for unstacked reads, where the card's own score is the one that
    # positioned it.
    stack_lead_confidence = {}
    for photo in photo_dicts:
        if "_stack_lead_prediction_confidence" in photo:
            stack_lead_confidence[photo.get("id")] = photo[
                "_stack_lead_prediction_confidence"
            ]
        projected = (
            stack_items is not None
            or "_browse_stack_kind" in photo
        )
        kind = photo.pop("_browse_stack_kind", None)
        raw_count = photo.pop("_browse_stack_count", None)
        raw_ids = photo.pop("_browse_stack_member_ids", None)
        # Strip every SQL-only stack helper before the response is
        # serialized. Their names and values are implementation details;
        # Browse consumes only the stable summary below.
        for key in list(photo):
            if key.startswith("_"):
                photo.pop(key, None)
        item = stacks_by_cover.get(photo.get("id"))
        if item is not None:
            kind = item.get("kind")
            member_ids = list(item.get("member_ids") or [])
        else:
            member_ids = []
            if raw_ids:
                member_ids = [
                    int(value) for value in str(raw_ids).split(",") if value
                ]
        count = len(member_ids) if member_ids else int(raw_count or 1)
        if projected:
            photo["browse_stack"] = (
                {
                    "kind": kind,
                    "count": count,
                    "photo_ids": member_ids,
                }
                if kind and count >= 2
                else None
            )
    attach_location_statuses(db, photo_dicts)
    attach_species(db, photo_dicts)
    attach_species_representatives(db, photo_dicts)
    attach_detections(db, photo_dicts)
    attach_prediction_confidence(db, photo_dicts)
    for photo in photo_dicts:
        if photo.get("id") not in stack_lead_confidence:
            continue
        photo["prediction_confidence"] = stack_lead_confidence[photo["id"]]
        # Say, per card, that this number came off the stack's leading
        # frame rather than the cover in the thumbnail — the client must
        # not infer it from the sort dropdown. A healthy visual clause
        # keeps results similarity-ranked no matter what the dropdown
        # says, and that path builds its stacks in Python with only the
        # cover's own score, so a select-derived label would explain the
        # relevance order with a number that did not produce it (Codex
        # P2 on PR #1670).
        if (photo.get("browse_stack") or {}).get("count", 0) >= 2:
            photo["prediction_confidence_is_stack_lead"] = True
    attach_edit_recipes(db, photo_dicts)
    return photo_dicts
