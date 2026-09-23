"""Photo data API: the ``/api/photos`` listing, query, and per-photo routes.

Browse listing and paging (``/api/photos``, ``/api/photos/query``,
``/api/photos/ids``, ``/api/photos/by-ids``, ``/api/photos/calendar``,
``/api/photos/geo``, ``/api/photos/companion-count``), photo detail
(``/api/photos/<id>``, also served as ``/api/v1/photos[/<id>]`` by the alias
table in ``create_app``), text and similarity search, best-batch scoring,
region sharpness, subjects and primary subject, the wildlife-exclusion toggle,
open-in-external-editor, supported extensions, and the Missing Originals
routes (``/api/photos/missing*``).

Per-photo edit recipes live in ``web/photo_edit_recipes.py``; per-photo
keyword and location edits in ``web/photo_location_keywords.py``.
"""

import contextlib
import json
import logging
import math
import os
import tempfile

import config as cfg
from camera_denoise import render_cache_fields as _camera_render_cache_fields
from db import Database
from flask import Blueprint, after_this_request, jsonify, request
from photo_payload import (
    attach_detections,
    attach_edit_recipes,
    attach_location_statuses,
    attach_nested_edit_recipes,
    attach_prediction_confidence,
    attach_species,
    attach_species_representatives,
    prepare_browse_photo_dicts,
    render_key_for_recipe,
)
from proc import no_window_kwargs
from render_source import (
    companion_image_can_replace_raw_result as _companion_image_can_replace_raw_result,
)
from render_source import (
    image_is_smaller_than_expected as _image_is_smaller_than_expected,
)
from render_source import path_satisfies_recipe_render as _path_satisfies_recipe_render
from render_source import (
    recipe_render_source as _recipe_render_source,
)
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)
from services.visual_scope import inject_active_visual_model, validate_visual_arg
from web.background_jobs import make_background_job
from web.location_edits import (
    serialize_photo_location,
)
from web.request_args import (
    MAX_FOCUS_PHOTO_IDS,
    focus_candidate_ids,
    reject_visual_collection,
    request_bool_arg,
    request_rules_arg,
    request_visual_arg,
)
from working_copy_cache import working_copy_publication_guard

log = logging.getLogger(__name__)


# A Leaflet marker is substantially heavier than its JSON source row. Keep a
# hard ceiling as a final safety net for very large libraries; the Map UI
# clearly reports truncation and lets users narrow the shared filters.
MAP_RENDER_PHOTO_LIMIT = 10_000


def create_photos_blueprint(
    get_db,
    json_error,
    get_runner,
    db_path,
    config,
    *,
    visual_scope,
    photo_not_found_error,
    max_per_page,
    request_flag_filter,
    request_location_status_filter,
    parse_missing_originals_folder_id,
    missing_originals_payload,
    start_missing_originals_scan,
    invalidate_missing_originals,
    run_batch_delete,
    photo_highlight_entries,
    best_batch_scope,
    build_best_batch_response,
):
    """Build the ``/api/photos`` data blueprint.

    ``config`` is ``app.config`` (``THUMB_CACHE_DIR`` is read at request
    time). The subject-analysis launcher builds its own ``background_job``
    from ``get_runner`` / ``db_path``, as ``web/export.py`` does.

    Injected from ``create_app`` because other route groups still use them:
    ``visual_scope`` (the app's one ``VisualScope``, which owns the per-app
    query-text embedding cache), ``photo_not_found_error``,
    ``max_per_page`` (the page-size cap shared with browse init and
    collection photos), the ``request_flag_filter`` /
    ``request_location_status_filter`` request parsers, the Missing
    Originals cache (``parse_missing_originals_folder_id``,
    ``missing_originals_payload``, ``start_missing_originals_scan``,
    ``invalidate_missing_originals`` -- its state lives on ``app``), and
    ``run_batch_delete`` (shared with ``/api/batch/delete`` and the
    batch-delete job). ``photo_highlight_entries``, ``best_batch_scope`` and
    ``build_best_batch_response`` are module-level ``app.py`` helpers built
    on highlight and burst utilities the Highlights routes still share.
    """
    blueprint = Blueprint("photos", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)

    @blueprint.route("/api/photos/extensions")
    def api_photos_extensions():
        """Return the distinct lowercased file extensions present in the
        active workspace, sorted.

        The smart-collection rule editor uses this to populate the value
        dropdown for the Extension field. Free-text was silent-failure
        prone — typing ``JPG`` matched nothing because rows are stored as
        ``.jpg``.
        """
        db = get_db()
        return jsonify(db.get_workspace_extensions())

    @blueprint.route("/api/photos/missing")
    def api_photos_missing():
        """Return cached Missing Originals scan status without filesystem work."""
        db = get_db()
        try:
            folder_id = parse_missing_originals_folder_id(db)
        except ValueError as exc:
            return json_error(str(exc))
        except LookupError:
            return json_error("folder not found", 404)
        return jsonify(missing_originals_payload(db, folder_id))

    @blueprint.route("/api/photos/missing/check", methods=["POST"])
    def api_photos_missing_check():
        """Start or reuse a background Missing Originals scan."""
        db = get_db()
        try:
            folder_id = parse_missing_originals_folder_id(db)
        except ValueError as exc:
            return json_error(str(exc))
        except LookupError:
            return json_error("folder not found", 404)
        body = request.get_json(silent=True) or {}
        automatic = bool(body.get("automatic"))
        payload = start_missing_originals_scan(
            db,
            folder_id=folder_id,
            automatic=automatic,
        )
        status_code = 202 if payload.get("pending") else 200
        return jsonify(payload), status_code

    @blueprint.route("/api/photos/missing/delete-sidecars", methods=["POST"])
    def api_photos_missing_delete_sidecars():
        """Delete leftover .xmp sidecar files for photos whose original is gone.

        Body: ``{"photo_ids": [int, ...]}``.

        IDs are resolved server-side against the active workspace — the
        client never names paths directly. Earlier drafts accepted
        ``(folder_path, filename)`` from the body and were usable to
        unlink any ``.xmp`` on disk as long as the paired non-xmp path
        was absent. Now we only touch sidecars whose photo row is in
        the active workspace AND whose source is verified missing.
        """
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids") or []
        if not isinstance(photo_ids, list):
            return json_error("photo_ids must be a list")

        db = get_db()
        ws_id = db._active_workspace_id
        deleted = 0
        skipped = 0
        folder_online_cache = {}
        for raw_id in photo_ids:
            try:
                pid = int(raw_id)
            except (TypeError, ValueError):
                skipped += 1
                continue
            row = db.conn.execute(
                """SELECT p.filename, f.path AS folder_path
                   FROM photos p
                   JOIN folders f ON p.folder_id = f.id
                   JOIN workspace_folders wf ON wf.folder_id = f.id
                   WHERE p.id = ? AND wf.workspace_id = ?""",
                (pid, ws_id),
            ).fetchone()
            if not row:
                skipped += 1
                continue
            folder_path = row["folder_path"]
            folder_online = folder_online_cache.get(folder_path)
            if folder_online is None:
                # Match /api/photos/missing/remove: a path that stats as a
                # directory but can't be enumerated (NAS/share ACL denial,
                # missing search permission) makes ``os.path.exists(src)``
                # unreliable for every child, so we'd otherwise unlink
                # sidecars belonging to originals we couldn't actually
                # verify were missing.
                folder_online = False
                if os.path.isdir(folder_path):
                    try:
                        with os.scandir(folder_path):
                            folder_online = True
                    except OSError:
                        folder_online = False
                folder_online_cache[folder_path] = folder_online
            if not folder_online:
                # Folder is currently unreachable — we can't tell whether
                # the original was restored before the mount went offline,
                # so skip the sidecar (a valid original could have a
                # matching .xmp we would wrongly delete once the volume
                # comes back).
                skipped += 1
                continue
            src = os.path.join(folder_path, row["filename"])
            if os.path.exists(src):
                # Original came back — refuse to touch the sidecar.
                skipped += 1
                continue
            stem, _ = os.path.splitext(src)
            removed_one = False
            for candidate in (stem + ".xmp", stem + ".XMP",
                              src + ".xmp", src + ".XMP"):
                if os.path.isfile(candidate):
                    try:
                        os.remove(candidate)
                        removed_one = True
                    except OSError as e:
                        log.warning("Failed to delete sidecar %s: %s", candidate, e)
            if removed_one:
                deleted += 1
            else:
                skipped += 1
        return jsonify({"deleted": deleted, "skipped": skipped})

    @blueprint.route("/api/photos/missing/remove", methods=["POST"])
    def api_photos_missing_remove():
        """Delete Vireo rows for photos whose originals are still gone.

        Body: ``{"photo_ids": [int, ...], "delete_sidecars": bool,
        "mode": "vireo"|"disk"|"disk_permanent"}``.

        A ready ``/api/photos/missing`` cache is served for up to
        ``_MISSING_ORIGINALS_STALE_SECONDS`` (30 min) without a filesystem
        recheck, so a photo whose original came back between the last
        scan and the user clicking Remove would otherwise be deleted
        from Vireo by trusting the cache. This endpoint pre-checks each
        photo's source on disk in the active workspace and only forwards
        the still-missing IDs to the shared batch-delete implementation.
        Sidecar cleanup follows the same guard, so a restored original
        never loses its ``.xmp`` either.

        Response: ``{deleted, restored: [ids], skipped, sidecars_deleted,
        sidecars_skipped, mode}``. ``restored`` lets the client show which
        photos were saved from the cache-driven delete so the modal can
        surface them (e.g. via toast) instead of silently discarding the
        request.
        """
        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids") or []
        if not isinstance(photo_ids, list):
            return json_error("photo_ids must be a list")
        delete_sidecars = bool(body.get("delete_sidecars"))
        mode = body.get("mode", "vireo")
        if mode not in ("vireo", "disk", "disk_permanent"):
            return json_error(
                "mode must be 'vireo', 'disk', or 'disk_permanent'"
            )

        db = get_db()
        ws_id = db._active_workspace_id
        confirmed_ids = []
        restored_ids = []
        folder_offline_ids = []
        skipped = 0
        sidecar_targets = []
        folder_online_cache = {}

        def folder_accessible(folder_path):
            cached = folder_online_cache.get(folder_path)
            if cached is not None:
                return cached
            accessible = False
            if os.path.isdir(folder_path):
                try:
                    with os.scandir(folder_path):
                        accessible = True
                except OSError:
                    accessible = False
            folder_online_cache[folder_path] = accessible
            return accessible

        for raw_id in photo_ids:
            try:
                pid = int(raw_id)
            except (TypeError, ValueError):
                skipped += 1
                continue
            row = db.conn.execute(
                """SELECT p.filename, f.path AS folder_path
                   FROM photos p
                   JOIN folders f ON p.folder_id = f.id
                   JOIN workspace_folders wf ON wf.folder_id = f.id
                   WHERE p.id = ? AND wf.workspace_id = ?""",
                (pid, ws_id),
            ).fetchone()
            if not row:
                skipped += 1
                continue
            folder_path = row["folder_path"]
            if not folder_accessible(folder_path):
                # The parent folder/NAS mount is currently unreachable, so
                # ``os.path.exists(src)`` would return False for every row
                # regardless of whether the original was restored before
                # the volume went offline. Deleting under that ambiguity
                # would remove valid Vireo rows; leave the decision for a
                # later scan (once folder health flips or the volume comes
                # back) instead.
                folder_offline_ids.append(pid)
                continue
            src = os.path.join(folder_path, row["filename"])
            if os.path.exists(src):
                # Original is back on disk — the cached "missing" verdict
                # was stale. Keep the Vireo row (and its sidecar).
                restored_ids.append(pid)
                continue
            confirmed_ids.append(pid)
            if delete_sidecars:
                sidecar_targets.append(src)

        sidecars_deleted = 0
        sidecars_skipped = 0
        if delete_sidecars:
            for src in sidecar_targets:
                stem, _ = os.path.splitext(src)
                removed_one = False
                for candidate in (stem + ".xmp", stem + ".XMP",
                                  src + ".xmp", src + ".XMP"):
                    if os.path.isfile(candidate):
                        try:
                            os.remove(candidate)
                            removed_one = True
                        except OSError as e:
                            log.warning(
                                "Failed to delete sidecar %s: %s", candidate, e
                            )
                if removed_one:
                    sidecars_deleted += 1
                else:
                    sidecars_skipped += 1

        deleted_count = 0
        if confirmed_ids:
            try:
                result = run_batch_delete(
                    db, confirmed_ids, mode, include_companions=False,
                )
            except ValueError as exc:
                return json_error(str(exc))
            deleted_count = int(result.get("deleted") or 0)
            if deleted_count:
                invalidate_missing_originals()

        # A cache invalidation is still worth doing when everything came
        # back online: the ready payload we just refused to trust for
        # deletion is also the one the banner will keep serving until the
        # next scan. Dropping it now forces a fresh check the next time
        # the banner/modal asks, so the restored photos stop appearing
        # as ghosts. Do the same when we deferred rows because a folder
        # was offline: the cache is unreliable evidence for those IDs and
        # the next scan (or health flip) needs to replace it.
        if deleted_count == 0 and (restored_ids or folder_offline_ids):
            invalidate_missing_originals()

        return jsonify({
            "deleted": deleted_count,
            "restored": restored_ids,
            "folder_offline": folder_offline_ids,
            "skipped": skipped,
            "sidecars_deleted": sidecars_deleted,
            "sidecars_skipped": sidecars_skipped,
            "mode": mode,
        })

    @blueprint.route("/api/photos")
    def api_photos():
        import config as cfg
        db = get_db()
        page = request.args.get("page", 1, type=int)
        default_per_page = cfg.load().get("photos_per_page", 50)
        per_page = max(1, min(request.args.get("per_page", default_per_page, type=int), max_per_page))
        sort = request.args.get("sort", "date")
        folder_id = request.args.get("folder_id", None, type=int)
        collection_id = request.args.get("collection_id", None, type=int)
        # Rules-only path: ``db.get_photos``'s collection restriction is
        # built by ``_build_collection_query`` and evaluates ``rules`` alone.
        # ``/api/v1/photos`` aliases this view, so a headless caller that
        # scopes to a visual collection here would silently widen to every
        # metadata match instead of the saved visual result set (Codex
        # review r3621634298).
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        rating_min = request.args.get("rating_min", None, type=int)
        date_from = request.args.get("date_from", None)
        date_to = request.args.get("date_to", None)
        keyword = request.args.get("keyword", None)
        keyword_match_case = request_bool_arg("keyword_match_case")
        keyword_whole_word = request_bool_arg("keyword_whole_word")
        color_label = request.args.get("color_label", None)
        try:
            flag = request_flag_filter()
            location_status = request_location_status_filter()
        except ValueError as e:
            return json_error(str(e), 400)

        try:
            photos = db.get_photos(
                folder_id=folder_id,
                collection_id=collection_id,
                page=page,
                per_page=per_page,
                sort=sort,
                rating_min=rating_min,
                date_from=date_from,
                date_to=date_to,
                keyword=keyword,
                keyword_match_case=keyword_match_case,
                keyword_whole_word=keyword_whole_word,
                color_label=color_label,
                flag=flag,
                location_status=location_status,
            )
        except ValueError as exc:
            return json_error(str(exc), 400)

        # Total count — use count_photos for unfiltered, otherwise use efficient COUNT query
        if not any([folder_id, collection_id, rating_min, date_from, date_to, keyword, color_label, flag, location_status]):
            total = db.count_photos()
        else:
            try:
                total = db.count_filtered_photos(
                    folder_id=folder_id,
                    collection_id=collection_id,
                    rating_min=rating_min,
                    date_from=date_from,
                    date_to=date_to,
                    keyword=keyword,
                    keyword_match_case=keyword_match_case,
                    keyword_whole_word=keyword_whole_word,
                    color_label=color_label,
                    flag=flag,
                    location_status=location_status,
                )
            except ValueError as exc:
                return json_error(str(exc), 400)

        photo_dicts = [dict(p) for p in photos]
        attach_location_statuses(db, photo_dicts)
        attach_species(db, photo_dicts)
        attach_species_representatives(db, photo_dicts)
        attach_detections(db, photo_dicts)
        attach_edit_recipes(db, photo_dicts)

        return jsonify(
            {
                "photos": photo_dicts,
                "total": total,
                "page": page,
                "per_page": per_page,
            }
        )

    @blueprint.route("/api/photos/query", methods=["POST"])
    def api_photos_query():
        """Universal-filter photo query: a smart-collection rule tree plus
        sort and paging. Response shape matches /api/photos so pages can
        switch fetch paths without re-plumbing their renderers.

        ``focus_photo_id`` (optional) serves the page holding that photo
        rather than the requested one and adds ``focus_index`` /
        ``focus_page`` to the response — what Browse re-sorts around so a
        change of sort order keeps the user's selected photo on screen
        without paging forward until it appears. ``page`` reports the page
        actually served, so it equals ``focus_page`` whenever the photo was
        found. A photo the query does not match reports ``focus_index:
        null`` and leaves the requested page alone.

        Design: docs/plans/2026-07-19-universal-filters-design.md.
        """
        db = get_db()
        payload = request.get_json(silent=True)
        if payload is None or not isinstance(payload, dict):
            return json_error("request body must be a JSON object", 400)
        rules = payload.get("rules")
        if rules is None:
            rules = []
        page = payload.get("page", 1)
        per_page = payload.get("per_page", 50)
        sort = payload.get("sort", "date")
        stacks = payload.get("stacks", False)
        if not isinstance(page, int) or isinstance(page, bool) or page < 1:
            return json_error("page must be a positive integer", 400)
        if not isinstance(per_page, int) or isinstance(per_page, bool) or per_page < 1:
            return json_error("per_page must be a positive integer", 400)
        # sort feeds an unhashable-unsafe ``sort_map.get(sort, ...)`` in
        # ``query_photos``; a JSON array/object here would raise TypeError
        # (not ValueError) and bypass the 400 handler below.
        if not isinstance(sort, str):
            return json_error("sort must be a string", 400)
        if not isinstance(stacks, bool):
            return json_error("stacks must be a boolean", 400)
        per_page = min(per_page, max_per_page)
        collection_id = payload.get("collection_id")
        if collection_id is not None and (
            not isinstance(collection_id, int) or isinstance(collection_id, bool)
        ):
            return json_error("collection_id must be an integer", 400)
        # Rules-only scoping path: ``query_photo_ids`` / ``query_photos`` /
        # ``count_photos_for_rules`` all funnel through
        # ``_append_collection_restriction`` → ``_build_collection_query``,
        # which reads only ``collections.rules``. An API/headless caller
        # posting ``{"collection_id": <visual id>}`` would therefore silently
        # widen to every metadata match instead of the saved visual result
        # set; the Browse reopen path already sends the stored ``rules`` +
        # ``visual`` without relying on ``collection_id`` (Codex review
        # r3621903988).
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        folder_id = payload.get("folder_id")
        if folder_id is not None and (
            not isinstance(folder_id, int) or isinstance(folder_id, bool)
        ):
            return json_error("folder_id must be an integer", 400)
        # Browse sends ``focus_photo_id`` when a re-sort has to stay with the
        # photo the user has selected: serve the page that photo landed on
        # instead of page 1, and report where it is so the caller can anchor
        # its loaded window there. A photo that no longer matches reports
        # ``focus_index: null`` and the requested page — never a silent
        # substitution the grid would have no way to notice.
        #
        # ``focus_photo_ids`` is the same question asked of several photos at
        # once, for a caller holding a card that stands for more than one:
        # every frame of a selected stack shares the card's position, so any
        # frame this result set still contains can place it. The response
        # names the one that answered, since the caller cannot assume it was
        # the frame it would have asked about first.
        focus_photo_id = payload.get("focus_photo_id")
        if focus_photo_id is not None and (
            not isinstance(focus_photo_id, int) or isinstance(focus_photo_id, bool)
        ):
            return json_error("focus_photo_id must be an integer", 400)
        focus_photo_ids = payload.get("focus_photo_ids")
        if focus_photo_ids is not None and (
            not isinstance(focus_photo_ids, list)
            or len(focus_photo_ids) > MAX_FOCUS_PHOTO_IDS
            or any(not isinstance(pid, int) or isinstance(pid, bool)
                   for pid in focus_photo_ids)
        ):
            return json_error(
                "focus_photo_ids must be a list of at most "
                f"{MAX_FOCUS_PHOTO_IDS} integers", 400,
            )
        focus_candidates = focus_candidate_ids(focus_photo_id, focus_photo_ids)
        # One name for "is this a focused request" from here down, so the
        # snapshot, the lookups and the response stay in step.
        focus_photo_id = focus_candidates[0] if focus_candidates else None
        rules = inject_active_visual_model(rules)
        try:
            visual = validate_visual_arg(payload.get("visual"))
        except ValueError as exc:
            return json_error(str(exc), 400)

        include_offline = payload.get("include_offline", False)
        include_availability = payload.get("include_availability", False)
        if not isinstance(include_offline, bool):
            return json_error("include_offline must be a boolean", 400)
        if not isinstance(include_availability, bool):
            return json_error("include_availability must be a boolean", 400)
        # Offline rows are display-only. IDs-only responses feed selection
        # and bulk actions, so those remain restricted to actionable photos.
        if payload.get("ids_only"):
            include_offline = False

        # Offset pagination is only self-consistent within one snapshot, and
        # that applies to the visual path too: a deletion landing between the
        # id ordering and ``get_photos_by_ids`` drops rows out of the page the
        # focused position described, so the photo Browse is holding onto goes
        # missing and the selection is cleared. Open the snapshot here — ahead
        # of the visual clause, which returns before the rules path's ``BEGIN``
        # — and release it on every exit, error paths included (Codex review on
        # PR #1658). Reads only; ``rollback`` is what releases it.
        if focus_photo_id is not None:
            db.conn.execute("BEGIN")

            @after_this_request
            def _release_focus_snapshot(response):
                if db.conn.in_transaction:
                    db.conn.rollback()
                return response

        visual_info = None
        if visual is not None:
            try:
                visual_info, ordered_ids, sims_by_pid = visual_scope.resolve(
                    db, rules, visual,
                    collection_id=collection_id, folder_id=folder_id,
                    include_offline_folders=(
                        include_offline or include_availability
                    ),
                )
            except ValueError as exc:
                return json_error(str(exc), 400)
            if ordered_ids is not None:
                # Healthy visual clause: results are the similarity-ranked
                # matches; sort is relevance by design while it is active.
                availability = None
                folder_statuses = None
                if include_offline or include_availability:
                    inventory_ids = ordered_ids
                    folder_statuses = db.get_photo_folder_statuses(inventory_ids)
                    available_ids = [
                        pid for pid in inventory_ids
                        if folder_statuses.get(pid) in ("ok", "partial")
                    ]
                    if not include_offline:
                        ordered_ids = available_ids
                    availability = {
                        "inventory_total": len(inventory_ids),
                        "available_total": len(available_ids),
                        "offline_total": len(inventory_ids) - len(available_ids),
                    }
                if payload.get("ids_only"):
                    if stacks:
                        # Same cover-first flattening the paginated visual
                        # branch below applies, so Select-all-matching
                        # inserts each stack's visible cover ahead of its
                        # hidden members and Best Batch, burst-review,
                        # and export preview start from the visible first
                        # card rather than a hidden burst frame (Codex
                        # P2 on PR #1561).
                        stack_items = db.collapse_browse_stack_photo_ids(
                            ordered_ids,
                            stack_config=db.browse_stack_settings(cfg.load()),
                        )
                        stacked_ids = []
                        seen_ids = set()
                        for item in stack_items:
                            cover = item["cover_id"]
                            if cover not in seen_ids:
                                stacked_ids.append(cover)
                                seen_ids.add(cover)
                            for member in item["member_ids"]:
                                if member in seen_ids:
                                    continue
                                stacked_ids.append(member)
                                seen_ids.add(member)
                        return jsonify({"ids": stacked_ids,
                                        "total": len(stacked_ids),
                                        "visual": visual_info})
                    return jsonify({"ids": ordered_ids, "total": len(ordered_ids),
                                    "visual": visual_info})
                # Offline members are display-only, so they never join a
                # stack: each one stays its own item and is left out of the
                # duplicate / burst tallies. Same rule the SQL projection
                # applies in ``_browse_stack_query_parts``.
                offline_ids = (
                    [
                        pid for pid in ordered_ids
                        if folder_statuses.get(pid) not in ("ok", "partial")
                    ]
                    if include_offline and folder_statuses is not None
                    else None
                )
                stack_items = (
                    db.collapse_browse_stack_photo_ids(
                        ordered_ids, standalone_ids=offline_ids,
                        stack_config=db.browse_stack_settings(cfg.load()),
                    )
                    if stacks else None
                )
                logical_ids = (
                    [item["cover_id"] for item in stack_items]
                    if stack_items is not None else ordered_ids
                )
                # A healthy visual clause has every matching id in memory
                # already, so the focused page is a list index rather than
                # another query.
                focus_index = None
                focus_resolved_id = None
                if focus_candidates:
                    # Earliest-placed candidate, so a stack whose first
                    # frames this clause dropped is still placed by the ones
                    # it kept — the same rule the SQL path applies.
                    wanted = set(focus_candidates)
                    if stack_items is not None:
                        for item_index, item in enumerate(stack_items):
                            hit = wanted.intersection(
                                [item["cover_id"], *item["member_ids"]]
                            )
                            if hit:
                                focus_index = item_index
                                focus_resolved_id = min(hit)
                                break
                    else:
                        placed = [
                            (logical_ids.index(pid), pid)
                            for pid in wanted if pid in logical_ids
                        ]
                        if placed:
                            focus_index, focus_resolved_id = min(placed)
                    if focus_index is not None:
                        page = focus_index // per_page + 1
                start = (page - 1) * per_page
                page_ids = logical_ids[start:start + per_page]
                photos_map = db.get_photos_by_ids(page_ids)
                page_stack_items = (
                    stack_items[start:start + per_page]
                    if stack_items is not None else None
                )
                photo_dicts = prepare_browse_photo_dicts(
                    db,
                    [photos_map[pid] for pid in page_ids if pid in photos_map],
                    stack_items=page_stack_items,
                )
                similarity_by_cover = {
                    item["cover_id"]: max(
                        (
                            sims_by_pid[pid]
                            for pid in item["member_ids"]
                            if pid in sims_by_pid
                        ),
                        default=None,
                    )
                    for item in (page_stack_items or [])
                }
                for entry in photo_dicts:
                    entry["similarity"] = similarity_by_cover.get(
                        entry["id"], sims_by_pid.get(entry["id"]),
                    )
                    if include_offline and folder_statuses is not None:
                        entry["folder_status"] = folder_statuses.get(entry["id"])
                response = {
                    "photos": photo_dicts,
                    "total": len(logical_ids),
                    "page": page,
                    "per_page": per_page,
                    "visual": visual_info,
                }
                if focus_candidates:
                    response["focus_index"] = focus_index
                    response["focus_page"] = page
                    response["focus_photo_id"] = focus_resolved_id
                if stacks:
                    # Availability totals below are photo counts, so the
                    # underlying (unstacked) total is what they must agree
                    # with — never the stack count.
                    response["underlying_total"] = len(ordered_ids)
                    response["stack_count"] = sum(
                        len(item["member_ids"]) > 1 for item in stack_items
                    )
                if availability is not None:
                    response.update(availability)
                return jsonify(response)
            # Unhealthy: fall through to metadata-only results with the
            # status attached so the UI can say why — never silently zero.

        if payload.get("ids_only"):
            # Select-all and other bulk flows need the complete matching id
            # set; it must resolve exactly the photos the filtered grid
            # shows, so it shares this endpoint rather than a legacy path.
            # Under ``stacks=true`` emit each stack's cover ahead of its
            # hidden members — matching the collection ``/photo-ids``
            # projection — so Best Batch, burst-review, and export
            # preview start from the visible first card rather than a
            # hidden burst frame (Codex P2 on PR #1561).
            try:
                if stacks:
                    ids = db.query_photo_ids_stacked(
                        rules, sort=sort,
                        collection_id=collection_id, folder_id=folder_id,
                        stack_config=db.browse_stack_settings(cfg.load()),
                    )
                else:
                    ids = db.query_photo_ids(rules, sort=sort, collection_id=collection_id,
                                             folder_id=folder_id)
            except ValueError as exc:
                return json_error(str(exc), 400)
            payload_out = {"ids": ids, "total": len(ids)}
            if visual_info is not None:
                payload_out["visual"] = visual_info
            return jsonify(payload_out)
        # A target sitting on a page boundary can move onto the adjacent
        # page when ingestion or deletion commits between the position lookup
        # and the page fetch below. The response would still carry a valid
        # ``focus_page`` while the rows it returns omit the photo — and
        # Browse deliberately does not page towards a focused target, so it
        # would clear the very selection this path exists to preserve. Hold
        # one SQLite read snapshot across the lookup, the counts and the page,
        # the same guarantee ``/api/browse/init`` gives its focused first
        # paint. Opt-in, so ordinary Browse paging keeps its
        # transaction-free behaviour.
        #
        # ``try``/``finally`` rather than ``/api/browse/init``'s rollback
        # before each error return: the enrichers and count queries below can
        # raise something other than ValueError, and the snapshot has to be
        # released on those paths too.
        focus_snapshot = focus_photo_id is not None
        if focus_snapshot and not db.conn.in_transaction:
            db.conn.execute("BEGIN")
        try:
            focus_index = None
            focus_resolved_id = None
            try:
                # One stack configuration for the whole request. The position
                # below has to be read under exactly the grouping the page
                # fetch uses, or a focused re-sort lands on the wrong page.
                stack_cfg = db.browse_stack_settings(cfg.load()) if stacks else None
                if focus_candidates:
                    # Stacked Browse pages logical items, so a hidden member
                    # resolves to the page its cover sits on.
                    found = (
                        db.query_browse_stack_position_first(
                            rules, focus_candidates, sort=sort,
                            collection_id=collection_id, folder_id=folder_id,
                            include_offline_folders=include_offline,
                            stack_config=stack_cfg,
                        )
                        if stacks
                        else db.query_photo_position_first(
                            rules, focus_candidates, sort=sort,
                            collection_id=collection_id, folder_id=folder_id,
                            include_offline_folders=include_offline,
                        )
                    )
                    if found is not None:
                        focus_resolved_id, focus_index = found
                        page = focus_index // per_page + 1
                underlying_total = db.count_photos_for_rules(
                    rules,
                    collection_id=collection_id,
                    folder_id=folder_id,
                    include_offline_folders=include_offline,
                )
                if stacks:
                    photos = db.query_browse_stacks(
                        rules, sort=sort, page=page, per_page=per_page,
                        collection_id=collection_id, folder_id=folder_id,
                        include_offline_folders=include_offline,
                        stack_config=stack_cfg,
                    )
                    stack_totals = db.browse_stack_totals(
                        rules, collection_id=collection_id, folder_id=folder_id,
                        include_offline_folders=include_offline,
                        stack_config=stack_cfg,
                    )
                    total = stack_totals["total"]
                    stack_count = stack_totals["stack_count"]
                else:
                    photos = db.query_photos(
                        rules, sort=sort, page=page, per_page=per_page,
                        collection_id=collection_id, folder_id=folder_id,
                        include_offline_folders=include_offline,
                    )
                    total = underlying_total
            except ValueError as exc:
                return json_error(str(exc), 400)

            photo_dicts = prepare_browse_photo_dicts(db, photos)

            response = {
                "photos": photo_dicts,
                "total": total,
                "page": page,
                "per_page": per_page,
            }
            if focus_candidates:
                response["focus_index"] = focus_index
                response["focus_page"] = page
                response["focus_photo_id"] = focus_resolved_id
            if stacks:
                response["underlying_total"] = underlying_total
                response["stack_count"] = stack_count
            if include_offline or include_availability:
                # Availability is always reported in photos, never in stacks:
                # the notice reads "N of M photos available", so it has to agree
                # with the sidebar collection count and with ``underlying_total``
                # — ``total`` is the logical item count once Stacks collapses it.
                try:
                    inventory_total = (
                        underlying_total
                        if include_offline
                        else db.count_photos_for_rules(
                            rules,
                            collection_id=collection_id,
                            folder_id=folder_id,
                            include_offline_folders=True,
                        )
                    )
                    available_total = (
                        db.count_photos_for_rules(
                            rules,
                            collection_id=collection_id,
                            folder_id=folder_id,
                        )
                        if include_offline
                        else underlying_total
                    )
                except ValueError as exc:
                    return json_error(str(exc), 400)
                response.update({
                    "inventory_total": inventory_total,
                    "available_total": available_total,
                    "offline_total": max(0, inventory_total - available_total),
                })
            if visual_info is not None:
                response["visual"] = visual_info
            return jsonify(response)
        finally:
            # rollback(), not commit(): this endpoint is read-only, and the
            # rollback is what releases the snapshot.
            if focus_snapshot and db.conn.in_transaction:
                db.conn.rollback()

    @blueprint.route("/api/photos/ids")
    def api_photo_ids():
        """Return every photo ID matching the current Browse filters."""
        db = get_db()
        sort = request.args.get("sort", "date")
        folder_id = request.args.get("folder_id", None, type=int)
        collection_id = request.args.get("collection_id", None, type=int)
        # Rules-only path — see ``api_photos``. ``db.get_photo_ids`` also
        # goes through ``_build_collection_query``, so a visual collection
        # id would silently widen to every metadata match (Codex review
        # r3621634298).
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        rating_min = request.args.get("rating_min", None, type=int)
        date_from = request.args.get("date_from", None)
        date_to = request.args.get("date_to", None)
        keyword = request.args.get("keyword", None)
        keyword_match_case = request_bool_arg("keyword_match_case")
        keyword_whole_word = request_bool_arg("keyword_whole_word")
        color_label = request.args.get("color_label", None)
        try:
            flag = request_flag_filter()
            location_status = request_location_status_filter()
        except ValueError as e:
            return json_error(str(e), 400)

        try:
            photo_ids = db.get_photo_ids(
                folder_id=folder_id,
                collection_id=collection_id,
                sort=sort,
                rating_min=rating_min,
                date_from=date_from,
                date_to=date_to,
                keyword=keyword,
                keyword_match_case=keyword_match_case,
                keyword_whole_word=keyword_whole_word,
                color_label=color_label,
                flag=flag,
                location_status=location_status,
            )
        except ValueError as exc:
            return json_error(str(exc), 400)
        return jsonify({"photo_ids": photo_ids, "total": len(photo_ids)})

    @blueprint.route("/api/photos/calendar")
    def api_photos_calendar():
        db = get_db()
        from datetime import date

        year = request.args.get("year", date.today().year, type=int)
        folder_id = request.args.get("folder_id", None, type=int)
        collection_id = request.args.get("collection_id", None, type=int)
        # ``get_calendar_data`` narrows through ``_build_collection_query``,
        # which reads only ``collections.rules``. Without ``visual``, a saved
        # visual collection would silently widen calendar counts to every
        # metadata match (Codex review r3622521597).
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        try:
            rules = request_rules_arg()
            visual = request_visual_arg()
            rules, _visual_info = visual_scope.apply_to_rules(
                db, rules, visual,
                collection_id=collection_id, folder_id=folder_id,
            )
        except ValueError as e:
            return json_error(str(e), 400)
        try:
            data = db.get_calendar_data(
                year=year, folder_id=folder_id,
                collection_id=collection_id,
                rules=rules,
            )
        except ValueError as e:
            return json_error(str(e), 400)
        return jsonify(data)

    @blueprint.route("/api/photos/<int:photo_id>")
    def api_photo_detail(photo_id):
        db = get_db()
        # verify_workspace: this response exposes the absolute path and
        # xmp_path, so a photo outside the active workspace must 404 —
        # mirrors serve_thumbnail / api_files_reveal.
        photo = db.get_photo(photo_id, verify_workspace=True)
        if not photo:
            return photo_not_found_error(legacy_error="not found")

        result = dict(photo)
        attach_location_statuses(db, [result])

        # Parse exif_data JSON into metadata field
        raw_exif = result.pop("exif_data", None)
        if raw_exif:
            try:
                result["metadata"] = json.loads(raw_exif)
            except (ValueError, TypeError):
                result["metadata"] = None
        else:
            result["metadata"] = None

        keywords = db.get_photo_keywords(photo_id)
        result["keywords"] = [dict(k) for k in keywords]
        # Export naming uses the species-rank keyword list, which is narrower
        # than the detail payload's life-list identifications (that list can
        # also contain genus/family rows). Expose the exact export metadata so
        # editor filename previews agree with the file the worker will write.
        result["species"] = db.get_species_keywords_for_photos(
            [photo_id]
        ).get(photo_id, [])

        # Representative block: the photo's eligible species plus whether this
        # photo is the primary species representative. Only the primary (item 0
        # of get_species_representative_lists, which sorts newest-selection
        # first) flips these flags — secondary reps stay false so shared UI
        # code (context menus, panel buttons) can still offer to promote them
        # via set_species_representative's re-select-to-newest behavior.
        # Keep the legacy ``life_list`` key because shared lightbox/menu code
        # reads it.
        representatives = db.get_species_representative_lists(eligible_only=True)
        life_list_species = db.get_photo_life_list_species(photo_id)
        primary_by_species = {
            species: (photo_ids[0] if photo_ids else None)
            for species, photo_ids in representatives.items()
        }
        result["life_list"] = [
            {
                "species": s,
                "is_current_photo": photo_id == primary_by_species.get(s),
                "is_species_representative": photo_id == primary_by_species.get(s),
            }
            for s in life_list_species
        ]
        result["species_representatives"] = result["life_list"]
        result["highlight_list"] = photo_highlight_entries(db, photo_id)

        # Location section: pre-resolved leaf + parent chain so the photo
        # detail panel can render the filled state without a second roundtrip.
        result["location"] = serialize_photo_location(db, photo_id)
        result["edit_recipe"] = db.get_photo_edit_recipe(photo_id)
        result["render_key"] = render_key_for_recipe(result["edit_recipe"])
        from camera_denoise import resolve_profile
        result["denoise_profile"] = resolve_profile(photo)
        # The shared lightbox normally warms /original after /full settles.
        # In full-resolution preview mode /full already redirects to /original,
        # so tell the client not to repeat that potentially expensive RAW work.
        import config as cfg
        preview_max_size = db.get_effective_config(cfg.load()).get("preview_max_size")
        result["full_uses_original"] = preview_max_size == 0
        # A small photo's natural dimensions cannot reveal the configured cap.
        result["full_preview_max_size"] = 1920 if preview_max_size is None else preview_max_size

        # Read XMP sidecar keywords
        folder = db.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
        ).fetchone()
        if folder:
            # Full on-disk path: mirrors the folder-join logic in
            # api_files_reveal. Exposed so the browse-grid "Copy Path"
            # right-click action can read a real filesystem path from the
            # detail response.
            result["path"] = os.path.join(folder["path"], photo["filename"])
            result["folder_name"] = os.path.basename(folder["path"])
            xmp_path = os.path.join(
                folder["path"],
                os.path.splitext(photo["filename"])[0] + ".xmp",
            )
            xmp_keywords = []
            xmp_exists = os.path.exists(xmp_path)
            if xmp_exists:
                from xmp import read_keywords

                xmp_keywords = sorted(read_keywords(xmp_path))
            result["xmp_exists"] = xmp_exists
            result["xmp_keywords"] = xmp_keywords
            result["xmp_path"] = xmp_path
        else:
            result["path"] = ""
            result["folder_name"] = ""
            result["xmp_exists"] = False
            result["xmp_keywords"] = []
            result["xmp_path"] = ""

        return jsonify(result)

    @blueprint.route("/api/photos/by-ids", methods=["POST"])
    def api_photos_by_ids():
        """Return selected photos in caller order, scoped to the active workspace."""
        db = get_db()
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("photo_ids", [])
        if not isinstance(raw_ids, list):
            return json_error("photo_ids must be a list", 400)
        if not raw_ids:
            return json_error("photo_ids required", 400)
        if len(raw_ids) > 500:
            return json_error("too many photo_ids", 400)

        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return json_error("photo_ids must be integers", 400)
            if raw in seen:
                continue
            seen.add(raw)
            photo_ids.append(raw)

        photos = []
        for pid in photo_ids:
            photo = db.get_photo(pid, verify_workspace=True)
            if photo:
                photos.append(dict(photo))
        attach_location_statuses(db, photos)
        attach_species(db, photos)
        attach_species_representatives(db, photos)
        attach_detections(db, photos)
        attach_prediction_confidence(db, photos)
        attach_edit_recipes(db, photos)
        return jsonify({"photos": photos})

    @blueprint.route("/api/photos/companion-count", methods=["POST"])
    def api_photos_companion_count():
        """How many of these photos have a companion file.

        The delete dialog's "Also delete N companion files" checkbox is a
        claim about the whole selection, and Browse can hold ids it has never
        loaded — every frame of a collapsed stack, or a Select all that
        reaches past the loaded page. Counting client-side over the loaded
        ones only hid the checkbox and left those companions on disk, so the
        count is taken here instead, against the same rows the delete will
        resolve.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("photo_ids", [])
        if not isinstance(raw_ids, list):
            return json_error("photo_ids must be a list", 400)
        photo_ids = []
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return json_error("photo_ids must be integers", 400)
            photo_ids.append(raw)
        return jsonify({"count": db.count_photos_with_companions(photo_ids)})

    @blueprint.route("/api/photos/<int:photo_id>/best-batch")
    def api_photo_best_batch(photo_id):
        """Find neighboring burst frames and rank the best photo in that batch."""
        db = get_db()
        max_gap = request.args.get("gap_seconds", 8.0, type=float)
        max_photos = request.args.get("max_photos", 120, type=int)
        max_gap = max(0.0, min(max_gap, 120.0))
        max_photos = max(2, min(max_photos, 500))

        rows, method_or_error = best_batch_scope(
            db,
            photo_id,
            max_gap_seconds=max_gap,
            max_photos=max_photos,
        )
        if rows is None:
            return json_error(method_or_error, 404)

        result, error = build_best_batch_response(db, photo_id, rows)
        if error:
            return json_error(error, 404)
        result["scope_method"] = method_or_error
        attach_nested_edit_recipes(db, result)
        return jsonify(result)

    @blueprint.route("/api/photos/best-batch", methods=["POST"])
    def api_selected_photos_best_batch():
        """Rank an explicit selected set as a temporary best-batch group."""
        db = get_db()
        body = request.get_json(silent=True) or {}
        raw_ids = body.get("photo_ids", [])
        if not isinstance(raw_ids, list) or not raw_ids:
            return json_error("photo_ids required")
        if len(raw_ids) > 500:
            return json_error("too many photo_ids", 400)

        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return json_error("photo_ids must be integers", 400)
            if raw in seen:
                continue
            seen.add(raw)
            photo_ids.append(raw)
        if len(photo_ids) < 2:
            return json_error("at least two selected photos are required", 400)

        placeholders = ",".join("?" for _ in photo_ids)
        rows = db.conn.execute(
            f"""SELECT p.id, p.folder_id, p.filename, p.extension, p.timestamp,
                      p.flag, p.rating, p.quality_score, p.sharpness
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ? AND p.id IN ({placeholders})""",
            (db._ws_id(), *photo_ids),
        ).fetchall()
        by_id = {row["id"]: row for row in rows}
        rows = [by_id[pid] for pid in photo_ids if pid in by_id]
        if len(rows) < 2:
            return json_error("at least two selected photos are required", 400)

        result, error = build_best_batch_response(db, photo_ids[0], rows)
        if error:
            return json_error(error, 404)
        result["scope_method"] = "selected_photos"
        attach_nested_edit_recipes(db, result)
        return jsonify(result)

    @blueprint.route("/api/photos/geo")
    def api_photos_geo():
        db = get_db()
        folder_id = request.args.get("folder_id", None, type=int)
        focus_photo_id = request.args.get("photo_id", None, type=int)
        try:
            rules = request_rules_arg()
            visual = request_visual_arg()
            # Fill in the active visual model on any UI-emitted
            # ``has_visual_index`` rule that omits it. Without this a Map
            # filter for "has index" (rule sent without a ``model`` key)
            # would match any embedding row, so a workspace with stale
            # embeddings from an inactive model would surface photos that
            # are not indexed for the currently-active visual search —
            # /api/photos/query already injects here (line 5691), so the
            # Map path must match to stay consistent.
            rules = inject_active_visual_model(rules)
            # Restrict the visual candidate set to plottable photos so a
            # workspace whose only embeddings live on non-plottable photos
            # doesn't return ``status: ok`` with ids that
            # ``get_geolocated_photos`` then intersects to zero — the user
            # would see a "visual match" chip over an empty map with no
            # fallback / no-index warning.
            plottable_ids = (
                db.get_plottable_photo_ids(folder_id=folder_id)
                if visual is not None else None
            )
            rules, visual_info = visual_scope.apply_to_rules(
                db, rules, visual, folder_id=folder_id,
                candidate_photo_ids=plottable_ids,
            )
        except ValueError as e:
            return json_error(str(e), 400)

        try:
            photos = db.get_geolocated_photos(
                folder_id=folder_id,
                rules=rules,
            )
        except ValueError as e:
            return json_error(str(e), 400)

        total_photos = db.count_photos()
        total_without_coordinates = db.count_photos_without_coordinates()
        total_geolocated = total_photos - total_without_coordinates

        total_filtered = len(photos)
        visible_photos = list(photos[:MAP_RENDER_PHOTO_LIMIT])
        # A deep-linked photo must remain reachable even when it falls beyond
        # the safety ceiling in the default date ordering. Replace the last
        # visible row rather than exceeding the bound.
        if (
            focus_photo_id is not None
            and visible_photos
            and all(p["id"] != focus_photo_id for p in visible_photos)
        ):
            focused = next((p for p in photos if p["id"] == focus_photo_id), None)
            if focused is not None:
                visible_photos[-1] = focused

        photo_dicts = [dict(p) for p in visible_photos]
        attach_edit_recipes(db, photo_dicts)

        response = {
            "photos": photo_dicts,
            "total_filtered": total_filtered,
            "total_rendered": len(photo_dicts),
            "render_limit": MAP_RENDER_PHOTO_LIMIT,
            "truncated": total_filtered > len(photo_dicts),
            "total_photos": total_photos,
            "total_geolocated": total_geolocated,
            "total_without_coordinates": total_without_coordinates,
            # Compatibility for older clients; new UI uses coordinate-neutral
            # names because assigned locations are included in these totals.
            "total_with_gps": total_geolocated,
            "total_without_gps": total_without_coordinates,
        }
        # Surface the visual clause's status so the filter bar's visual
        # chip can warn on fallback — without this the chip advertises a
        # visual search that silently returned metadata-only matches.
        if visual_info is not None:
            response["visual"] = visual_info
        return jsonify(response)

    @blueprint.route("/api/photos/<int:photo_id>/wildlife_excluded", methods=["POST"])
    def api_set_wildlife_excluded(photo_id):
        db = get_db()
        body = request.get_json(silent=True) or {}
        excluded = body.get("excluded")
        if not isinstance(excluded, bool):
            return json_error("excluded must be a boolean")
        old = db.get_photo(photo_id)
        if not old:
            return photo_not_found_error(legacy_error="not found")
        old_value = "1" if old["wildlife_excluded"] else "0"
        new_value = "1" if excluded else "0"
        try:
            db.update_photo_wildlife_excluded(photo_id, excluded)
        except ValueError as e:
            return json_error(str(e), 403)
        db.record_edit(
            "wildlife_excluded",
            "Excluded from wildlife classification" if excluded else "Included in wildlife classification",
            new_value,
            [{"photo_id": photo_id, "old_value": old_value, "new_value": new_value}],
        )
        return jsonify({"ok": True, "wildlife_excluded": excluded})

    @blueprint.route("/api/photos/sharpness/regions", methods=["POST"])
    def api_photo_region_sharpness():
        """Score sharpness for explicit per-photo image regions.

        The burst-review UI sends the visible crop inside each comparison
        thumbnail, expressed in the coordinate space of the image currently
        laid out in the browser. We map that crop onto the 1024px working
        image used by the existing sharpness scorer.
        """
        db = get_db()
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object", 400)
        regions = body.get("regions")
        if not isinstance(regions, list):
            return json_error("regions must be a list")
        max_regions = 200
        if len(regions) > max_regions:
            return json_error(f"too many regions (max {max_regions})", 400)

        from image_loader import load_working_image
        from sharpness import _score_from_pil

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        results = []

        for raw in regions:
            if not isinstance(raw, dict):
                results.append({
                    "photo_id": None,
                    "sharpness": None,
                    "error": "region must be an object",
                })
                continue
            raw_photo_id = raw.get("photo_id")
            if isinstance(raw_photo_id, bool):
                results.append({
                    "photo_id": None,
                    "sharpness": None,
                    "error": "invalid photo_id",
                })
                continue
            try:
                photo_id = int(raw_photo_id)
            except (TypeError, ValueError):
                results.append({
                    "photo_id": None,
                    "sharpness": None,
                    "error": "invalid photo_id",
                })
                continue
            photo = db.get_photo(photo_id, verify_workspace=True)
            if not photo:
                results.append({"photo_id": photo_id, "sharpness": None, "error": "not found"})
                continue

            folder = db.conn.execute(
                "SELECT id, path FROM folders WHERE id = ?", (photo["folder_id"],)
            ).fetchone()
            if not folder:
                results.append({"photo_id": photo_id, "sharpness": None, "error": "folder not found"})
                continue

            photo_dict = dict(photo)
            folders = {folder["id"]: folder["path"]}
            # A user reviewing a burst is actively using these working
            # copies; stamp them so quota eviction does not treat them as
            # the least recently used files in the cache.
            img = load_working_image(
                photo_dict, vireo_dir, max_size=1024, folders=folders,
                record_access=True,
            )
            if img is None:
                results.append({"photo_id": photo_id, "sharpness": None, "error": "could not load image"})
                continue

            def _float(name, default, region=raw):
                raw_value = region.get(name, default)
                try:
                    value = float(raw_value)
                except (TypeError, ValueError):
                    raise ValueError(f"invalid {name}") from None
                if not math.isfinite(value):
                    raise ValueError(f"invalid {name}")
                return value

            try:
                source_w = _float("source_w", photo["width"] or img.width)
                source_h = _float("source_h", photo["height"] or img.height)
                x = _float("x", 0)
                y = _float("y", 0)
                w = _float("w", source_w)
                h = _float("h", source_h)
            except ValueError as exc:
                results.append({
                    "photo_id": photo_id,
                    "sharpness": None,
                    "error": str(exc),
                })
                continue

            if source_w <= 0 or source_h <= 0:
                source_w, source_h = img.width, img.height

            scale_x = img.width / source_w
            scale_y = img.height / source_h
            try:
                ix = max(0, min(img.width - 1, int(round(x * scale_x))))
                iy = max(0, min(img.height - 1, int(round(y * scale_y))))
                iw = max(1, int(round(w * scale_x)))
                ih = max(1, int(round(h * scale_y)))
            except OverflowError:
                results.append({
                    "photo_id": photo_id,
                    "sharpness": None,
                    "error": "invalid region bounds",
                })
                continue
            if ix + iw > img.width:
                iw = img.width - ix
            if iy + ih > img.height:
                ih = img.height - iy

            if iw < 2 or ih < 2:
                score = None
            else:
                score = _score_from_pil(img, (ix, iy, iw, ih))
            results.append({
                "photo_id": photo_id,
                "sharpness": score,
                "region": {"x": ix, "y": iy, "w": iw, "h": ih},
            })

        return jsonify({"results": results})

    @blueprint.route("/api/photos/<int:photo_id>/subjects")
    def api_photo_subjects(photo_id):
        from subjects import payload
        db = get_db()
        if db.get_photo(photo_id, verify_workspace=True) is None:
            return json_error("not found", 404)
        return jsonify(payload(db, photo_id))

    @blueprint.route("/api/photos/<int:photo_id>/subjects/analyze", methods=["POST"])
    @background_job
    def api_analyze_photo_subjects(ctx, photo_id):
        db = get_db()
        if db.get_photo(photo_id, verify_workspace=True) is None:
            return json_error("not found", 404)
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])

        def work(job):
            from image_loader import get_canonical_image_path
            from subjects import analyze_photo
            thread_db = ctx.thread_db()
            try:
                ctx.checkpoint(job)
                photo = thread_db.get_photo(photo_id, verify_workspace=True)
                if photo is None:
                    raise ValueError("Photo is no longer in this workspace")
                folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
                path = get_canonical_image_path(photo, vireo_dir, folders)
                count = analyze_photo(thread_db, photo_id, path,
                                      checkpoint=lambda: ctx.checkpoint(job))
                job["result"] = {"photo_id": photo_id, "subjects_analyzed": count}
            finally:
                thread_db.close()

        return ctx.start("analyze-subjects", work, config={"photo_id": photo_id})

    @blueprint.route("/api/photos/<int:photo_id>/primary-subject", methods=["PUT"])
    def api_primary_subject(photo_id):
        from subjects import payload, select_primary
        db = get_db()
        if db.get_photo(photo_id, verify_workspace=True) is None:
            return json_error("not found", 404)
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or "detection_id" not in body:
            return json_error("detection_id is required; use null for automatic selection")
        from pipeline_locks import acquire_photo_mask
        lock = acquire_photo_mask(photo_id)
        if not lock.acquire(blocking=False):
            return json_error("Subject analysis is running for this photo. Try again when it finishes.", 409)
        try:
            select_primary(db, photo_id, body["detection_id"])
        except ValueError as exc:
            return json_error(str(exc))
        finally:
            lock.release()
        return jsonify(payload(db, photo_id))

    @blueprint.route("/api/photos/open-external", methods=["POST"])
    def api_photos_open_external():
        import subprocess
        import sys

        import config as cfg

        body = request.get_json(silent=True) or {}
        photo_ids = body.get("photo_ids")
        if not isinstance(photo_ids, list) or not photo_ids:
            return json_error("photo_ids required")
        if not all(isinstance(pid, int) for pid in photo_ids):
            return json_error("photo_ids must be a list of integers")

        db = get_db()
        folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        photo_paths = []
        for pid in photo_ids:
            photo = db.get_photo(pid)
            if not photo:
                continue
            folder_path = folders.get(photo["folder_id"], "")
            if folder_path:
                photo_paths.append((
                    photo,
                    os.path.join(folder_path, photo["filename"]),
                ))

        if not photo_paths:
            return json_error("No photos found", 404)

        def _external_edit_recipe_source(photo, recipe, fallback_path):
            from image_loader import RAW_EXTENSIONS

            primary_is_raw = (
                os.path.splitext(photo["filename"])[1].lower() in RAW_EXTENSIONS
            )

            if recipe.get("crop"):
                source_path, using_working_copy = _recipe_render_source(
                    photo, recipe, 0, vireo_dir, folders,
                )
                return source_path or fallback_path, bool(using_working_copy)

            # For RAW primaries, skip the working-copy short-circuit while the
            # RAW source is available: legacy working copies predate the
            # highlight-preserving RAW decode and would feed the editor a
            # clipped JPEG to apply the recipe to. If the RAW source is
            # offline/missing, the working copy is the only local fallback.
            wc_rel = photo["working_copy_path"]
            if wc_rel and (
                not primary_is_raw or not os.path.exists(fallback_path)
            ):
                wc_path = (
                    wc_rel if os.path.isabs(wc_rel)
                    else os.path.join(vireo_dir, wc_rel)
                )
                if (
                    os.path.exists(wc_path)
                    and _path_satisfies_recipe_render(wc_path, photo, recipe, 0)
                ):
                    return wc_path, True

            folder_path = folders.get(photo["folder_id"])
            if folder_path:
                raw_source_available = os.path.exists(fallback_path)
                # For RAW primaries, skip the companion JPEG while the RAW is
                # available so edits render from the preserve-highlights decode.
                # If the RAW volume is offline, a full-size sidecar is the best
                # remaining local source.
                if not primary_is_raw or not raw_source_available:
                    companion_path = photo["companion_path"]
                    if companion_path:
                        companion = os.path.join(folder_path, companion_path)
                        if (
                            os.path.exists(companion)
                            and _path_satisfies_recipe_render(
                                companion, photo, recipe, 0,
                            )
                        ):
                            return companion, False
                original = os.path.join(folder_path, photo["filename"])
                if os.path.exists(original):
                    return original, False
            return fallback_path, False

        def _external_edit_handoff_path(photo, fallback_path):
            recipe = db.get_photo_edit_recipe(photo["id"])
            if not recipe:
                return fallback_path, None

            import local_masks as _local_masks
            from image_edits import (
                EDIT_MATH_VERSION,
                apply_recipe_to_loaded_image,
                recipe_to_json,
            )
            from image_loader import (
                RAW_DECODE_LINEAR,
                RAW_EXTENSIONS,
                load_image,
            )

            source_path, using_working_copy = _external_edit_recipe_source(
                photo, recipe, fallback_path,
            )
            fallback_available = bool(
                fallback_path and os.path.isfile(fallback_path)
            )
            # When the original is offline the working copy is the only usable
            # local source; the retry-from-original recovery below cannot save
            # us. Hold the publication/eviction guard through the exists →
            # getmtime → decode window so quota enforcement cannot unlink the
            # working copy mid-handoff. Guard is an RLock, so nested
            # acquisitions inside load_image are safe.
            pin_working_copy = using_working_copy and not fallback_available
            source_pin_cm = (
                working_copy_publication_guard()
                if pin_working_copy else contextlib.nullcontext()
            )
            with source_pin_cm:
                if not source_path or not os.path.isfile(source_path):
                    if using_working_copy and fallback_available:
                        # Quota enforcement can unlink the working copy between
                        # recipe-source resolution and this existence check.
                        # Fall back to the original before reporting missing.
                        source_path = fallback_path
                        using_working_copy = False
                    else:
                        return None, f"{photo['filename']}: source file missing"

                out_dir = os.path.join(vireo_dir, "external-edits")
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(out_dir, f"{photo['id']}.jpg")
                meta_path = os.path.join(out_dir, f"{photo['id']}.json")
                try:
                    source_mtime = os.path.getmtime(source_path)
                except FileNotFoundError:
                    if using_working_copy and fallback_available:
                        source_path = fallback_path
                        using_working_copy = False
                        source_mtime = os.path.getmtime(source_path)
                    else:
                        return None, f"{photo['filename']}: source file missing"
                recipe_json = recipe_to_json(recipe) or ""
                # Include the edit-math version so a math bump invalidates this
                # handoff render: the JPEG is keyed by recipe/source/mtime,
                # none of which change when only the per-pixel rendering math
                # changes, so without this we'd keep handing editors the stale
                # render.
                expected_meta = {
                    "recipe": recipe_json,
                    "source_path": source_path,
                    "source_mtime": source_mtime,
                    "edit_math_version": EDIT_MATH_VERSION,
                    **_camera_render_cache_fields(photo, recipe),
                }
                try:
                    if os.path.isfile(out_path) and os.path.isfile(meta_path):
                        with open(meta_path, encoding="utf-8") as f:
                            cached_meta = json.load(f)
                        if cached_meta == expected_meta:
                            return out_path, None
                except (OSError, ValueError, TypeError):
                    pass

                # Derive the decode mode from the primary photo's extension
                # rather than source_path so a future change to source_path
                # resolution (working copy, companion JPEG fallback, etc.)
                # cannot silently bypass RAW_DECODE_LINEAR for a
                # RAW primary.
                primary_is_raw = (
                    os.path.splitext(photo["filename"])[1].lower()
                    in RAW_EXTENSIONS
                )
                raw_decode = (
                    RAW_DECODE_LINEAR if primary_is_raw else None
                )
                load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
                img = load_image(source_path, max_size=None, **load_kwargs)
                if img is None and using_working_copy and fallback_path and (
                    os.path.isfile(fallback_path)
                    and os.path.abspath(fallback_path)
                    != os.path.abspath(source_path)
                ):
                    # Working copy was evicted between validation and decode.
                    # Retry from the original before the companion fallback so
                    # RAW primaries still use the highlight-preserving decode.
                    retry_img = load_image(
                        fallback_path, max_size=None, **load_kwargs,
                    )
                    if retry_img is not None:
                        img = retry_img
                        source_path = fallback_path
                        using_working_copy = False
                        with contextlib.suppress(OSError):
                            source_mtime = os.path.getmtime(source_path)
                        expected_meta["source_path"] = source_path
                        expected_meta["source_mtime"] = source_mtime
            expected_w, expected_h = 0, 0
            needs_companion = False
            if primary_is_raw:
                # libraw may return the embedded JPEG when it cannot
                # demosaic — that preview is typically much smaller than the
                # full-size companion JPEG, so the handoff would apply the
                # recipe to clipped pixels even when a usable sidecar exists.
                # Trigger the companion fallback when the RAW failed outright
                # or came back undersized (both axes checked, shared helper).
                expected_w, expected_h = _recipe_source_dimensions(photo)
                needs_companion = img is None or _image_is_smaller_than_expected(
                    img, expected_w, expected_h,
                )
            if needs_companion:
                # libraw can't decode this RAW (unsupported variant, corrupt
                # sensor data, no usable embedded JPEG) or only produced an
                # undersized embedded preview. Fall back to the full-size
                # companion JPEG so Open External still works for RAW+JPEG
                # pairs that have a usable handoff JPEG. Cache key stays on
                # the RAW source so a future RAW replacement (mtime bump)
                # re-tries the RAW; we accept that companion-only edits
                # won't invalidate this render — matching the cache
                # contract used elsewhere for RAW-primary photos.
                folder_path = folders.get(photo["folder_id"])
                companion_path = photo["companion_path"]
                if folder_path and companion_path:
                    companion_abs = os.path.join(folder_path, companion_path)
                    if (
                        os.path.exists(companion_abs)
                        and os.path.abspath(companion_abs)
                        != os.path.abspath(source_path)
                    ):
                        companion_img = load_image(companion_abs, max_size=None)
                        # Prefer companion when it covers the expected size on
                        # both axes — a long-edge-only check misses cases like
                        # a 6000x3376 embedded preview "tying" a 6000x4000
                        # sidecar and losing the short-edge content.
                        if _companion_image_can_replace_raw_result(
                            companion_img, img, expected_w, expected_h,
                        ):
                            if img is None:
                                log.info(
                                    "External-edit RAW decode failed for "
                                    "photo %s; falling back to companion "
                                    "JPEG %s",
                                    photo["id"], companion_abs,
                                )
                            else:
                                log.info(
                                    "External-edit RAW decode fell back to "
                                    "undersized embedded JPEG (%dx%d) for "
                                    "photo %s; using companion JPEG %s "
                                    "(%dx%d)",
                                    img.size[0], img.size[1], photo["id"],
                                    companion_abs,
                                    companion_img.size[0],
                                    companion_img.size[1],
                                )
                                img.close()
                            img = companion_img
                        elif companion_img is not None:
                            companion_img.close()
            if img is None:
                return None, f"{photo['filename']}: failed to load image"
            rendered = None
            tmp_path = None
            fd = None
            try:
                rendered = apply_recipe_to_loaded_image(
                    img, recipe,
                    camera_metadata=photo,
                    native_size=_recipe_source_dimensions(photo),
                    local_mask=_local_masks.load_snapshot(
                        vireo_dir, photo["id"], recipe,
                    ),
                )
                quality = cfg.load().get("working_copy_quality", 92)
                fd, tmp_path = tempfile.mkstemp(
                    prefix=f".{photo['id']}.", suffix=".jpg.tmp", dir=out_dir,
                )
                os.close(fd)
                fd = None
                rendered.save(tmp_path, format="JPEG", quality=quality)
                os.replace(tmp_path, out_path)
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(expected_meta, f, sort_keys=True)
            except Exception:
                if fd is not None:
                    with contextlib.suppress(OSError):
                        os.close(fd)
                with contextlib.suppress(OSError):
                    if tmp_path:
                        os.unlink(tmp_path)
                raise
            finally:
                if rendered is not None:
                    rendered.close()
                if rendered is not img:
                    img.close()
            return out_path, None

        editors = cfg.get_editors()
        selected_editor = None
        editor_index = body.get("editor_index")
        if editor_index is None:
            # No index = use the first configured editor (or fall through to
            # the OS default if no editors are configured at all).
            selected_editor = editors[0] if editors else None
            editor = selected_editor["path"] if selected_editor else ""
        else:
            if not isinstance(editor_index, int) or isinstance(editor_index, bool):
                return json_error("editor_index must be an integer")
            if not editors:
                return json_error(
                    "No external editors configured. Add one in Settings.", 400
                )
            if editor_index < 0 or editor_index >= len(editors):
                return json_error(
                    f"editor_index {editor_index} out of range "
                    f"(have {len(editors)} editor(s))", 400
                )
            selected_editor = editors[editor_index]
            editor = selected_editor["path"]
        editor_path = os.path.expanduser(editor) if editor else ""

        # On macOS, an .app bundle is a directory — execing it raises EACCES.
        # Resolve the bundle when the user gives the parent folder (e.g.
        # /Applications/Adobe Lightroom Classic/) and route through `open -a`.
        app_bundle = None
        if sys.platform == "darwin" and editor_path:
            if editor_path.endswith(".app"):
                app_bundle = editor_path
            elif os.path.isdir(editor_path):
                try:
                    bundles = [
                        entry for entry in sorted(os.listdir(editor_path))
                        if entry.endswith(".app")
                    ]
                except OSError:
                    bundles = []
                if len(bundles) == 1:
                    app_bundle = os.path.join(editor_path, bundles[0])
                elif len(bundles) > 1:
                    return json_error(
                        f"Multiple .app bundles found in {editor_path} "
                        f"({', '.join(bundles)}). "
                        "Set the editor to a specific .app bundle.",
                        500,
                    )
                else:
                    return json_error(
                        f"No .app bundle found in {editor_path}. "
                        "Set the editor to the .app bundle directly.",
                        500,
                    )

        def _is_darktable_editor():
            parts = [
                selected_editor.get("name", "") if selected_editor else "",
                editor_path,
                app_bundle or "",
            ]
            return "darktable" in " ".join(parts).lower()

        file_paths = []
        for photo, path in photo_paths:
            handoff_path, handoff_error = _external_edit_handoff_path(photo, path)
            if handoff_error:
                return json_error(handoff_error, 500)
            file_paths.append(handoff_path)
        if _is_darktable_editor():
            from develop import convert_to_dng, is_nikon_high_efficiency_nef

            converted_paths = []
            for (photo, _original_path), input_path in zip(
                photo_paths, file_paths, strict=True,
            ):
                try:
                    metadata = (
                        json.loads(photo["exif_data"])
                        if photo["exif_data"] else None
                    )
                except (TypeError, json.JSONDecodeError):
                    metadata = None

                if not is_nikon_high_efficiency_nef(input_path, metadata=metadata):
                    converted_paths.append(input_path)
                    continue

                out_dir = os.path.join(vireo_dir, "external-dng", str(photo["id"]))
                stem = os.path.splitext(os.path.basename(input_path))[0]
                cached = os.path.join(out_dir, f"{stem}.dng")
                cached_alt = os.path.join(out_dir, f"{stem}.DNG")
                source_mtime = (
                    os.path.getmtime(input_path)
                    if os.path.exists(input_path) else 0
                )
                fresh_cached = None
                for candidate in (cached, cached_alt):
                    if (
                        os.path.isfile(candidate)
                        and os.path.getmtime(candidate) >= source_mtime
                    ):
                        log.info(
                            "Using cached DNG for darktable external editor: %s",
                            candidate,
                        )
                        fresh_cached = candidate
                        break
                if fresh_cached:
                    converted_paths.append(fresh_cached)
                    continue

                log.info(
                    "Converting Nikon HE NEF for darktable external editor: %s",
                    input_path,
                )
                conversion = convert_to_dng(
                    cfg.get("dng_converter_bin") or "",
                    input_path,
                    out_dir,
                )
                if not conversion["success"]:
                    return json_error(
                        "Nikon High Efficiency NEF detected, but DNG conversion failed: "
                        f"{conversion['error']}",
                        500,
                    )
                converted_paths.append(conversion["output_path"])
            file_paths = converted_paths

        try:
            if app_bundle:
                # `open -a` returns quickly after launching; capture the exit
                # so launch failures surface instead of disappearing silently.
                result = subprocess.run(
                    ["open", "-a", app_bundle] + file_paths,
                    capture_output=True, text=True, timeout=30,
                    **no_window_kwargs(),
                )
                if result.returncode != 0:
                    err = (result.stderr or result.stdout or "open failed").strip()
                    log.warning("open -a %s failed: %s", app_bundle, err)
                    return json_error(err, 500)
            elif editor_path:
                subprocess.Popen([editor_path] + file_paths, **no_window_kwargs())
            elif sys.platform == "darwin":
                result = subprocess.run(
                    ["open"] + file_paths,
                    capture_output=True, text=True, timeout=30,
                    **no_window_kwargs(),
                )
                if result.returncode != 0:
                    err = (result.stderr or result.stdout or "open failed").strip()
                    log.warning("open %s failed: %s", file_paths, err)
                    return json_error(err, 500)
            elif sys.platform == "win32":
                for fp in file_paths:
                    os.startfile(fp)
            else:
                for fp in file_paths:
                    subprocess.Popen(["xdg-open", fp], **no_window_kwargs())
        except Exception as e:
            log.warning("Failed to open external editor: %s", e)
            return json_error(str(e), 500)

        return jsonify({"opened": len(file_paths)})

    @blueprint.route("/api/photos/search")
    def api_photo_text_search():
        """Search photos by text query using CLIP cosine similarity."""
        import numpy as np

        query = request.args.get("q", "").strip()
        if not query:
            return json_error("Missing query parameter 'q'")

        limit = min(max(1, request.args.get("limit", 50, type=int)), 1000)
        threshold = request.args.get("threshold", 0.15, type=float)
        folder_id = request.args.get("folder_id", None, type=int)
        collection_id = request.args.get("collection_id", None, type=int)
        ids_only = request.args.get("ids_only", "").lower() in ("1", "true", "yes")

        db = get_db()
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err

        # Determine current model
        from models import get_active_model
        active_model = get_active_model()
        if not active_model:
            return jsonify({"results": [], "total_matches": 0, "model_used": None,
                            "reason": "no_model"})

        model_name = active_model["name"]
        model_type = active_model.get("model_type", "bioclip")

        # timm models don't produce CLIP embeddings — text search unsupported
        if model_type == "timm":
            return jsonify({"results": [], "total_matches": 0, "model_used": model_name,
                            "reason": "model_no_text_search"})

        try:
            scope_rules = request_rules_arg()
        except ValueError as e:
            return json_error(str(e), 400)

        candidate_photo_ids = None
        if scope_rules is not None:
            try:
                candidate_photo_ids = db.query_photo_ids(
                    scope_rules,
                    collection_id=collection_id,
                    folder_id=folder_id,
                )
            except ValueError as e:
                return json_error(str(e), 400)
        elif collection_id is not None:
            candidate_photo_ids = db.get_collection_photo_ids(collection_id)
        elif folder_id is not None:
            # Candidate scope arrives as rules / a collection / a folder —
            # the legacy per-field scope params were removed in Phase 5
            # (no caller sends them; visual search runs through
            # /api/photos/query).
            candidate_photo_ids = db.get_photo_ids(folder_id=folder_id)

        if candidate_photo_ids == []:
            if ids_only:
                return jsonify({
                    "photo_ids": [],
                    "total_matches": 0,
                    "model_used": model_name,
                })
            return jsonify({
                "results": [],
                "total_matches": 0,
                "model_used": model_name,
            })

        # Load embeddings for current model
        emb_pairs = db.get_photos_with_embedding(
            model_name, photo_ids=candidate_photo_ids
        )
        if not emb_pairs:
            return jsonify({"results": [], "total_matches": 0, "model_used": model_name,
                            "reason": "no_embeddings"})

        # Encode query text
        from text_encoder import encode_text
        model_str = active_model["model_str"]
        weights_path = active_model.get("weights_path", "")
        try:
            query_vec = encode_text(query, model_str=model_str, pretrained_str=weights_path)
        except Exception as e:
            log.exception("Text encoding failed for query=%r model=%s", query, model_name)
            return json_error(f"Text encoding failed: {e}", status=500)

        # Build matrix and compute similarities
        photo_ids = [pid for pid, _ in emb_pairs]
        emb_matrix = np.stack(
            [np.frombuffer(blob, dtype=np.float32) for _, blob in emb_pairs]
        )
        similarities = emb_matrix @ query_vec

        # Filter and sort
        mask = similarities >= threshold
        filtered_ids = [photo_ids[i] for i in range(len(photo_ids)) if mask[i]]
        filtered_sims = similarities[mask]
        total_matches = len(filtered_ids)

        # Top-N by similarity
        if total_matches > 0:
            ranked_indices = np.argsort(filtered_sims)[::-1]
            if ids_only:
                return jsonify({
                    "photo_ids": [filtered_ids[idx] for idx in ranked_indices],
                    "total_matches": total_matches,
                    "model_used": model_name,
                })
            top_indices = ranked_indices[:limit]
            top_pids = [filtered_ids[idx] for idx in top_indices]
            top_sims = [float(filtered_sims[idx]) for idx in top_indices]
            photos_map = db.get_photos_by_ids(top_pids)
            photo_dicts = []
            sims_by_pid = {}
            for pid, sim in zip(top_pids, top_sims, strict=False):
                if pid in photos_map:
                    photo_dicts.append(dict(photos_map[pid]))
                    sims_by_pid[pid] = round(sim, 4)
            attach_species(db, photo_dicts)
            attach_location_statuses(db, photo_dicts)
            attach_species_representatives(db, photo_dicts)
            attach_detections(db, photo_dicts)
            attach_edit_recipes(db, photo_dicts)
            results = [
                {
                    "photo": photo,
                    "similarity": sims_by_pid[photo["id"]],
                }
                for photo in photo_dicts
            ]
        else:
            if ids_only:
                return jsonify({
                    "photo_ids": [],
                    "total_matches": 0,
                    "model_used": model_name,
                })
            results = []

        return jsonify({
            "results": results,
            "total_matches": total_matches,
            "model_used": model_name,
        })

    @blueprint.route("/api/photos/<int:photo_id>/similar")
    def api_photo_similar(photo_id):
        """Find photos with similar embeddings to the given photo."""
        import numpy as np

        db = get_db()
        limit = min(max(1, request.args.get("limit", 20, type=int)), 1000)

        # Compare against the active classifier's embedding. The per-model
        # cache means a photo classified under BioCLIP-2 cannot be compared
        # to one classified under BioCLIP-3 — that mixing is exactly what
        # Phase 1 of the storage philosophy refactor stops.
        from models import get_active_model
        active_model = get_active_model()
        if not active_model:
            return json_error("No active classifier configured")
        model_name = active_model["name"]
        if active_model.get("model_type", "bioclip") == "timm":
            return json_error("Active classifier does not produce embeddings")

        source_blob = db.get_photo_embedding(photo_id, model_name)
        if not source_blob:
            return json_error(
                f"No {model_name} embedding for this photo — "
                "run classification first"
            )
        source_emb = np.frombuffer(source_blob, dtype=np.float32)

        # Load all workspace embeddings for the same model, then drop the
        # source photo before stacking.
        rows = [
            (pid, blob)
            for pid, blob in db.get_photos_with_embedding(model_name)
            if pid != photo_id
        ]

        if not rows:
            return jsonify({"similar": [], "total_compared": 0})

        # Compute cosine similarities (embeddings are already normalized)
        photo_ids = []
        embeddings = []
        for pid, blob in rows:
            photo_ids.append(pid)
            embeddings.append(np.frombuffer(blob, dtype=np.float32))

        emb_matrix = np.stack(embeddings)
        similarities = emb_matrix @ source_emb

        # Get top-N most similar
        top_indices = np.argsort(similarities)[::-1][:limit]
        top_pids = [photo_ids[idx] for idx in top_indices]
        top_sims = [float(similarities[idx]) for idx in top_indices]
        photos_map = db.get_photos_by_ids(top_pids)

        results = []
        for pid, sim in zip(top_pids, top_sims, strict=False):
            if pid in photos_map:
                results.append(
                    {
                        "photo": dict(photos_map[pid]),
                        "similarity": round(sim, 4),
                    }
                )
        attach_nested_edit_recipes(db, results)

        return jsonify(
            {
                "similar": results,
                "total_compared": len(rows),
            }
        )

    return blueprint
