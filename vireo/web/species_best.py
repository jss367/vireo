"""Best photo of each species: ``/api/species-best`` and its choose route.

``GET /api/species-best`` lists the best photos of each species within a
scope (a folder, a collection, a universal-filter rule tree, or everything),
each with the decision that put its first photo there
(``species_best.build_species_best_payload``).

``POST /api/species-best/choose`` records that one photo is the best of a
species among others (``services.species_choices.choose_best``): over the
photos named in ``over``, or over every other chosen photo of the species
within the scope sent. ``source`` and ``reason`` say who decided and why,
with the bounds a flag write has.

Both are aliased under ``/api/v1`` for headless reviewers.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from flask import Blueprint, jsonify, request
from services.species_choices import MAX_CHOICE_OVER, PhotoScope, choose_best
from services.visual_scope import coerce_collection_id, inject_active_visual_model
from species_best import SpeciesBestQuery, build_species_best_payload
from web.photo_review import flag_provenance
from web.request_args import reject_visual_collection, request_bool_arg

if TYPE_CHECKING:
    from collections.abc import Mapping

    from db import Database
    from flask.typing import ResponseReturnValue
    from services.service_types import ErrorResponse, JsonDict, JsonError
    from web.web_types import GetDb


def _valid_photo_id(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 < value < 2**63


def _scope_from(
    db: Database, values: Mapping[str, object], *, json_error: JsonError,
) -> tuple[PhotoScope, None] | tuple[None, ErrorResponse]:
    """``(scope, None)`` from ``folder_id`` / ``collection_id`` / ``rules``,
    or ``(None, response)``. ``rules`` may be a JSON string (a query
    parameter) or an already-parsed tree (a JSON body)."""
    raw_folder = values.get("folder_id")
    folder_id = None
    if raw_folder not in (None, ""):
        if isinstance(raw_folder, bool) or not isinstance(raw_folder, (int, str)):
            return None, json_error("folder_id must be an integer", 400)
        try:
            folder_id = int(raw_folder)
        except ValueError:
            return None, json_error("folder_id must be an integer", 400)
    invalid = reject_visual_collection(
        db, values.get("collection_id"), json_error=json_error,
    )
    if invalid is not None:
        return None, invalid
    collection_id = coerce_collection_id(values.get("collection_id"))
    rules = values.get("rules")
    if isinstance(rules, str):
        if not rules.strip():
            rules = None
        else:
            try:
                rules = json.loads(rules)
            except ValueError:
                return None, json_error("rules must be valid JSON", 400)
    if rules is not None:
        rules = inject_active_visual_model(rules)
    return PhotoScope(
        folder_id=folder_id,
        collection_id=collection_id if isinstance(collection_id, int) else None,
        rules=rules,
    ), None


def _parse_over(body: JsonDict) -> tuple[list[int] | None, str | None]:
    """``(over, None)`` (None when absent), or ``(None, message)``."""
    raw = body.get("over")
    if raw is None:
        return None, None
    if not isinstance(raw, list) or not all(_valid_photo_id(v) for v in raw):
        return None, "over must be a list of photo ids"
    over = list(dict.fromkeys(raw))
    if len(over) > MAX_CHOICE_OVER:
        return None, f"over may name at most {MAX_CHOICE_OVER} photos"
    return over, None


def _photos_error(db: Database, photo_ids: list[int]) -> tuple[str, int] | None:
    """``(message, status)`` when a photo is unknown or outside the workspace."""
    found = db.photos.existing_ids(photo_ids)
    missing = [pid for pid in photo_ids if pid not in found]
    if missing:
        return f"Unknown photo ids: {missing}", 404
    workspace_id = db.require_workspace_id()
    outside = [
        pid for pid in photo_ids
        if not db.workspace_folders.photo_in_workspace(pid, workspace_id)
    ]
    if outside:
        return f"Photo ids do not belong to the active workspace: {outside}", 403
    return None


def create_species_best_blueprint(get_db: GetDb, json_error: JsonError) -> Blueprint:
    """Build the best-photo-of-each-species blueprint."""
    blueprint = Blueprint("species_best", __name__)

    @blueprint.route("/api/species-best")
    def api_species_best() -> ResponseReturnValue:
        db = get_db()
        scope, invalid = _scope_from(db, request.args, json_error=json_error)
        if scope is None:
            return invalid
        species = (request.args.get("species") or "").strip() or None
        query = SpeciesBestQuery(
            scope=scope,
            species=species,
            per_species=request.args.get("per_species", 1, type=int),
            include_unconfirmed=request_bool_arg("include_unconfirmed"),
            confidence_threshold=request.args.get(
                "confidence_threshold", 0.70, type=float,
            ),
        )
        try:
            payload = build_species_best_payload(db, query)
        except ValueError as exc:
            return json_error(str(exc), 400)
        return jsonify(payload)

    @blueprint.route("/api/species-best/choose", methods=["POST"])
    def api_species_best_choose() -> ResponseReturnValue:
        db = get_db()
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error("JSON object body required", 400)
        species = body.get("species")
        species = species.strip() if isinstance(species, str) else ""
        species = db.keyword_taxa.resolve_species_display(species) if species else ""
        if not species:
            return json_error("species required", 400)
        photo_id = body.get("photo_id")
        if not _valid_photo_id(photo_id):
            return json_error("photo_id must be an integer", 400)
        over, error = _parse_over(body)
        if error:
            return json_error(error, 400)
        provenance, error = flag_provenance(body)
        if provenance is None:
            return json_error(error or "invalid source or reason", 400)
        scope, invalid = _scope_from(db, body, json_error=json_error)
        if scope is None:
            return invalid
        if over is not None and not scope.is_everything:
            return json_error(
                "send over (the photos it was chosen over) or a scope "
                "(folder_id, collection_id, rules), not both", 400,
            )
        photos_error = _photos_error(db, [photo_id, *(over or [])])
        if photos_error:
            return json_error(*photos_error)
        if not db.species_curation.photo_is_life_list_preference_eligible(
            species, photo_id,
        ):
            return json_error(
                f"photo {photo_id} is not a confirmed, unrejected photo of "
                f"{species}; confirm the species on it first", 400,
            )
        source, reason = provenance
        try:
            with db.transaction():
                choice = choose_best(
                    db, species, photo_id, over=over, scope=scope,
                    source=source, reason=reason,
                )
        except ValueError as exc:
            return json_error(str(exc), 400)
        return jsonify({"ok": True, **choice})

    return blueprint
