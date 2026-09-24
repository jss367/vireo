"""Visual-search clauses and collection scoping shared by photo endpoints.

A *visual clause* (``{"prompt": ..., "strength": ...}``) narrows a
universal-filter rule tree to the photos whose embeddings match a text
prompt. Browse, Map, Review, Misses, Dashboard counts and the site export all
resolve clauses the same way, so the validation, the active-model injection
for ``has_visual_index`` rules and the resolver live here rather than in any
one route group.

Nothing here touches ``request`` or builds an HTTP response; routes parse
their inputs and turn ``ValueError`` / the unhealthy-status dicts into
responses themselves. ``VisualScope`` is the one stateful piece: it owns the
query-text embedding cache, so ``create_app`` builds a single instance and
hands it to the routes that resolve clauses.
"""

from __future__ import annotations

import json
import logging

log = logging.getLogger(__name__)

VISUAL_STRENGTH_THRESHOLDS = {"broad": 0.10, "balanced": 0.15, "strict": 0.22}

VISUAL_COLLECTION_MSG = (
    "This collection has a visual-search clause. The rules-only "
    "endpoint would silently widen the scope to every metadata "
    "match, so it is refused here. Open the collection from Browse "
    "to see the visual results."
)

# Stored in ``has_visual_index`` rules when no visual model is active. It can
# never match a ``photo_embeddings.model`` value, so the rule fails closed.
_NO_ACTIVE_VISUAL_MODEL = "__no_active_visual_model__"

# Bound on cached query-text embeddings before the cache is reset.
_TEXT_QUERY_CACHE_LIMIT = 64


def validate_visual_arg(visual):
    """Normalize a visual clause payload or raise ValueError."""
    if visual is None:
        return None
    if not isinstance(visual, dict):
        raise ValueError("visual must be an object")
    prompt = visual.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("visual.prompt must be a non-empty string")
    strength = visual.get("strength", "balanced")
    # An unhashable strength (e.g. ``["broad"]``) makes ``strength in
    # VISUAL_STRENGTH_THRESHOLDS`` raise ``TypeError``, which would
    # escape the 400 handler in the collection create/update paths as
    # a 500 (CodeRabbit review r3620473547 outside-diff note).
    if not isinstance(strength, str):
        raise ValueError("visual.strength must be a string")
    if strength not in VISUAL_STRENGTH_THRESHOLDS:
        raise ValueError("visual.strength must be broad, balanced, or strict")
    return {"prompt": prompt.strip(), "strength": strength}


def inject_active_visual_model(rules):
    """Fill in the active visual model on ``has_visual_index`` leaves
    that don't name one.

    ``/api/filters/fields`` advertises ``Has visual index`` as a plain
    boolean field, so the UI-emitted rule has no ``model`` key. The
    rule engine's fallback then matches any ``photo_embeddings`` row,
    so a photo with only stale embeddings from a previously-active
    model satisfies ``has_visual_index is true`` even though visual
    search (which only loads embeddings for the active model — see
    ``api_photo_text_search``) can't use it. Inject the current active
    model here so the filter and visual search agree on what "has an
    index" means; smart-collection storage paths (POST/PUT
    ``/api/collections``) also normalize before persisting so a
    collection's saved rules match what the preview counter showed at
    save time (Codex review r3621749904) — otherwise the sidebar
    count and the reopened result set silently diverge from the
    preview when embeddings from multiple models exist.

    When no active model is configured (fresh library, or every
    installed model was removed while cached embeddings remain), the
    UI-emitted rule must fail closed instead of falling through to
    "any embedding row" — otherwise ``/api/photos/query`` would
    report photos as visually indexed while ``/api/photos/search``
    returns ``no_model``. Inject a sentinel model name that can never
    match a stored ``photo_embeddings.model`` value, so
    ``has_visual_index is true`` matches nothing and
    ``has_visual_index is false`` matches everything — both correct
    for "there is no usable visual index right now".
    """
    try:
        from models import get_active_model
        active = get_active_model()
    except Exception:
        active = None
    model_name = active.get("name") if active else None
    injected_model = model_name or _NO_ACTIVE_VISUAL_MODEL

    def _walk(node):
        if isinstance(node, dict):
            if "rules" in node and "field" not in node:
                inner = node.get("rules")
                # Malformed group (e.g. ``{"mode":"all","rules":null}``):
                # leave it untouched so ``Database._validate_node`` can
                # raise a ``ValueError`` that the route turns into a 400.
                # Iterating ``None`` here would raise ``TypeError`` and
                # bypass the 400 handler.
                if not isinstance(inner, list):
                    return node
                return {**node, "rules": [_walk(r) for r in inner]}
            if node.get("field") == "has_visual_index" and "model" not in node:
                return {**node, "model": injected_model}
            return node
        if isinstance(node, list):
            return [_walk(r) for r in node]
        return node

    return _walk(rules)


def coerce_collection_id(raw):
    """Parse an optional collection_id from a request body.

    Returns ``None`` if absent/blank, an ``int`` if valid, or the
    sentinel ``False`` if present but unparseable (so callers can
    distinguish "not provided" from "invalid"). ``bool`` is rejected
    because it's an ``int`` subclass.
    """
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return False
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        try:
            return int(raw)
        except ValueError:
            return False
    return False


def collection_row(db, collection_id):
    """Return the (workspace-scoped) collection row or None."""
    return db.conn.execute(
        "SELECT id, name, rules, visual_json FROM collections "
        "WHERE id = ? AND workspace_id = ?",
        (collection_id, db._ws_id()),
    ).fetchone()


def collection_rules_state(db, rules_json):
    """Return parsed collection rules and whether they are degraded."""
    try:
        parsed_rules = json.loads(rules_json)
    except (TypeError, ValueError):
        return None, True
    return parsed_rules, not db.rules_resolvable(parsed_rules)


class VisualScope:
    """Resolve visual clauses against a rule tree's candidate photos.

    One instance per app: it holds the query-text embedding cache keyed by
    ``(model, prompt)``. The ONNX text encode is the expensive step and the
    same prompt is re-resolved by every surface (grid, summary, calendar,
    facet counts) plus each typeahead keystroke. Methods take the database
    explicitly, so they are safe to call from job threads (the site export
    resolves collection clauses off the request thread).
    """

    def __init__(self):
        self._text_query_cache = {}

    def resolve(
        self, db, rules, visual, collection_id=None, folder_id=None,
        candidate_photo_ids=None,
        include_offline_folders=False,
    ):
        """Run a visual clause over the rule tree's candidate photos.

        Returns ``(info, ordered_ids, sims_by_pid)``. ``info["status"]`` is
        ``ok`` with ids ordered by similarity, or an unhealthy state
        (``no_model`` / ``model_no_text_search`` / ``no_embeddings`` /
        ``encoding_failed``) with ``ordered_ids is None`` — callers then
        apply metadata rules only and surface the state. Never silently
        zero results (design hard requirement).

        ``candidate_photo_ids`` further intersects the rule-derived
        candidate set. The Map endpoint passes plottable ids so a
        workspace whose only embeddings sit on non-plottable photos
        surfaces as ``no_embeddings`` (metadata fallback) instead of
        returning ``ok`` with ids that ``get_geolocated_photos`` will
        silently intersect down to zero.
        """
        import numpy as np
        from models import get_active_model
        base = {"prompt": visual["prompt"], "strength": visual["strength"]}
        active = get_active_model()
        if not active:
            return {**base, "status": "no_model", "model": None}, None, None
        model_name = active["name"]
        base["model"] = model_name
        if active.get("model_type", "bioclip") == "timm":
            return {**base, "status": "model_no_text_search"}, None, None
        candidates = db.query_photo_ids(
            rules, collection_id=collection_id, folder_id=folder_id,
            include_offline_folders=include_offline_folders,
        )
        if candidate_photo_ids is not None:
            restrict = set(candidate_photo_ids)
            candidates = [pid for pid in candidates if pid in restrict]
        emb_pairs = db.get_photos_with_embedding(
            model_name,
            photo_ids=candidates,
            include_offline_folders=include_offline_folders,
        )
        if not emb_pairs:
            return (
                {**base, "status": "no_embeddings",
                 "candidates": len(candidates), "indexed": 0},
                None, None,
            )
        cache_key = (model_name, visual["prompt"])
        query_vec = self._text_query_cache.get(cache_key)
        if query_vec is None:
            from text_encoder import encode_text
            try:
                query_vec = encode_text(
                    visual["prompt"],
                    model_str=active["model_str"],
                    pretrained_str=active.get("weights_path", ""),
                )
            except Exception as exc:
                log.exception("Visual clause text encoding failed: %r", visual["prompt"])
                return (
                    {**base, "status": "encoding_failed", "error": str(exc)},
                    None, None,
                )
            if len(self._text_query_cache) > _TEXT_QUERY_CACHE_LIMIT:
                self._text_query_cache.clear()
            self._text_query_cache[cache_key] = query_vec
        pids = [pid for pid, _ in emb_pairs]
        emb_matrix = np.stack(
            [np.frombuffer(blob, dtype=np.float32) for _, blob in emb_pairs]
        )
        sims = emb_matrix @ query_vec
        threshold = VISUAL_STRENGTH_THRESHOLDS[visual["strength"]]
        ordered_ids = []
        sims_by_pid = {}
        for i in np.argsort(sims)[::-1]:
            if sims[i] < threshold:
                break
            pid = pids[int(i)]
            ordered_ids.append(pid)
            sims_by_pid[pid] = round(float(sims[i]), 4)
        info = {**base, "status": "ok", "matched": len(ordered_ids),
                "candidates": len(candidates), "indexed": len(emb_pairs)}
        return info, ordered_ids, sims_by_pid

    def apply_to_rules(
        self, db, rules, visual, collection_id=None, folder_id=None,
        candidate_photo_ids=None,
    ):
        """For GET consumers (summary/calendar/values/geo/predictions):
        when the visual clause is healthy, restrict the rules to the
        matched ids (inlined photo_ids — no bound-parameter cap) so
        counts describe the same photos the visually-filtered grid
        shows. Unhealthy → rules unchanged, matching the grid's
        metadata-only fallback.

        Returns ``(rules, visual_info)``. ``visual_info`` is ``None`` when
        no visual clause was requested; otherwise it is the ``resolve``
        status dict — ``{status: "ok", matched, …}`` on a successful
        clause, or an unhealthy state (``no_model`` /
        ``model_no_text_search`` / ``no_embeddings`` / ``encoding_failed``)
        when the clause fell back to metadata-only. Endpoints whose UI
        renders the visual chip (Map, Browse, Review) must surface this
        so the chip does not silently broaden results.

        ``candidate_photo_ids`` narrows the pre-embedding candidate set —
        used by ``/api/photos/geo`` to keep the visual search inside the
        Map's plottable scope so a workspace with embeddings only on
        non-plottable photos surfaces as ``no_embeddings`` instead of
        returning ``ok`` ids that the endpoint then intersects away.
        """
        if visual is None:
            return rules, None
        base_rules = rules if rules is not None else []
        info, ordered_ids, _sims = self.resolve(
            db, base_rules, visual,
            collection_id=collection_id, folder_id=folder_id,
            candidate_photo_ids=candidate_photo_ids,
        )
        if ordered_ids is None:
            return rules, info
        rules_list = (
            [] if rules is None
            else ([rules] if isinstance(rules, dict) else list(rules))
        )
        return rules_list + [{"field": "photo_ids", "value": ordered_ids}], info
