"""Keywords: the keyword tree, identity reconciliation, and keyword edits.

The ``/api/keywords/*`` routes back the Keywords page and the keyword
pickers: listing (``/api/keywords``, ``/api/keywords/all``), identity
grouping and location-match reconciliation, merge preview/apply,
case-insensitive duplicate detection and cleanup, and per-keyword rename,
retype and delete. Renames and deletes queue the XMP sidecar changes the
edit implies (``keyword_add`` / ``keyword_remove`` and, for location
keywords, a ``location`` change for every descendant-tagged photo).

Linking a keyword to a Google place (``/api/keywords/<id>/link-place``)
lives with the other place routes in ``web/locations.py``.
"""

from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request
from services.pending_changes import queue_keyword_add, queue_keyword_remove
from web.settings import LOCATION_KEYWORDS_SETTING, workspace_effective_setting

log = logging.getLogger(__name__)


def create_keywords_blueprint(get_db, json_error):
    """Build the keywords blueprint.

    Nothing beyond ``get_db`` and ``json_error`` is injected: sidecar
    queueing comes from ``services.pending_changes`` and the per-workspace
    location-keywords setting from ``web.settings``.
    """
    blueprint = Blueprint("keywords", __name__)

    @blueprint.route("/api/keywords/all")
    def api_all_keywords():
        db = get_db()
        keywords = db.get_all_keywords()
        return jsonify([dict(k) for k in keywords])

    @blueprint.route('/api/keywords/identities')
    def api_keyword_identities():
        from keyword_identity import grouped_keywords
        return jsonify(grouped_keywords(get_db()))

    @blueprint.route('/api/keywords/location-matches')
    def api_keyword_location_matches():
        from keyword_identity import location_candidates
        return jsonify(location_candidates(get_db()))

    @blueprint.route('/api/keywords/reconcile-location', methods=['POST'])
    def api_reconcile_keyword_location():
        from keyword_identity import reconcile_location
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or any(
            type(body.get(key)) is not int for key in ('source_id', 'target_id')
        ):
            return json_error('source_id and target_id must be integers', 400)
        try:
            result = reconcile_location(get_db(), body['source_id'], body['target_id'])
        except ValueError as error:
            return json_error(str(error), 400)
        return jsonify(result)

    @blueprint.route('/api/keywords/merge-preview', methods=['POST'])
    @blueprint.route('/api/keywords/merge', methods=['POST'])
    def api_merge_keywords():
        from keyword_identity import merge_keywords, preview_keyword_merge
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error('Expected a keyword selection and a keyword to keep.', 400)
        overrides = body.get('overrides')
        if overrides is not None and not isinstance(overrides, dict):
            return json_error('Expected the merge settings as an object.', 400)
        try:
            args = (get_db(), body.get('keyword_ids'), body.get('target_id'))
            if request.path.endswith('/merge-preview'):
                result = preview_keyword_merge(*args, overrides)
            else:
                result = merge_keywords(*args, body.get('preview_token'), overrides)
        except ValueError as error:
            return json_error(str(error), 400)
        return jsonify(result)

    @blueprint.route("/api/keywords")
    def api_keywords():
        db = get_db()
        keywords = db.get_keyword_tree()
        return jsonify([dict(k) for k in keywords])

    @blueprint.route("/api/keywords/duplicates")
    def api_keyword_duplicates():
        """Find case-insensitive duplicate keywords within current workspace.

        Groups by the same slot key as merge_duplicate_keywords()
        — (LOWER(name), parent_id, type, species-bearing) — so the UI does
        not report legitimate same-name-different-slot rows (a taxonomy
        `Robin` and an individual `Robin`, a legacy species-bearing general
        and an ordinary general homonym, or leaves under different parents)
        as duplicates the cleanup endpoint can never actually merge.
        """
        db = get_db()
        ws = db._active_workspace_id
        dupes = db.conn.execute(
            """SELECT LOWER(k.name) as lname, GROUP_CONCAT(k.id) as ids,
                      GROUP_CONCAT(k.name, ' | ') as names, COUNT(DISTINCT k.id) as cnt
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               WHERE wf.workspace_id = ?
               GROUP BY LOWER(k.name), k.parent_id, k.type,
                        CASE WHEN k.type = 'taxonomy' OR k.is_species = 1
                             THEN 1 ELSE 0 END
               HAVING COUNT(DISTINCT k.id) > 1""",
            (ws,),
        ).fetchall()
        results = []
        for d in dupes:
            ids = list(set(int(x) for x in d["ids"].split(",")))
            # Count photos per variant within this workspace
            variants = []
            for kid in ids:
                row = db.conn.execute(
                    """SELECT k.name, COUNT(pk.photo_id) as cnt
                       FROM keywords k
                       JOIN photo_keywords pk ON pk.keyword_id = k.id
                       JOIN photos p ON p.id = pk.photo_id
                       JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                       WHERE k.id = ? AND wf.workspace_id = ?""",
                    (kid, ws),
                ).fetchone()
                if row and row["cnt"] > 0:
                    variants.append({"id": kid, "name": row["name"], "photo_count": row["cnt"]})
            if len(variants) > 1:
                results.append({"variants": variants, "keep": variants[0]["name"]})
        return jsonify(results)

    @blueprint.route("/api/keywords/clean", methods=["POST"])
    def api_clean_keywords():
        """Merge case-insensitive duplicate keywords."""
        db = get_db()
        merged = db.merge_duplicate_keywords()
        log.info("Keyword cleanup: merged %d duplicates", merged)
        return jsonify({"ok": True, "merged": merged})

    @blueprint.route("/api/keywords/<int:keyword_id>", methods=["PUT"])
    def api_update_keyword(keyword_id):
        db = get_db()
        body = request.get_json(silent=True) or {}
        # Capture old identity and tagged photos BEFORE the update: a rename
        # or retype can merge this row into a normalized same-slot peer, in
        # which case the original id and its photo_keywords rows are gone
        # afterwards (update_keyword returns the surviving id).
        old_row = db.conn.execute(
            """SELECT name, is_species, type
               FROM keywords WHERE id = ?""",
            (keyword_id,),
        ).fetchone()
        affected = []
        # Capture affected photos whenever a rename OR a retype is being
        # requested: update_keyword's merge-into-peer path can move photo
        # tags for either kind of change (a type-only PUT with an existing
        # same-name peer under the new type moves the tagged photos onto the
        # peer's stored spelling), and without the snapshot the
        # keyword_remove/keyword_add sidecar queueing below iterates an
        # empty list and XMP keeps exporting the old spelling until another
        # edit occurs.
        if body.get("name") or body.get("type"):
            affected = db.conn.execute(
                """SELECT pk.photo_id, wf.workspace_id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   WHERE pk.keyword_id = ?""",
                (keyword_id,),
            ).fetchall()
        # Additionally snapshot photos whose SIDECAR LOCATION path would go
        # stale from this update. A location-keyword rename does not just
        # change ``dc:subject`` (the flat leaf, which the ``affected`` set
        # above covers): it also invalidates the sidecar's
        # ``vireo:locationKeywords`` marker and ``lr:hierarchicalSubject``
        # entry. Renaming an ANCESTOR is worse still -- no photo is tagged
        # with the ancestor directly, so ``affected`` is empty and the
        # sidecar's hierarchy keeps the old ancestor name forever. Recurse
        # down from ``keyword_id`` to catch every descendant-leaf-tagged
        # photo. The gate on ``old_row["type"] == "location"`` covers a
        # rename or a retype away from location; a retype INTO location is
        # rare but also queued so a sidecar written under a former
        # non-location role gets its marker cleaned up on the next sync.
        location_affected = []
        if old_row is not None and (
            old_row["type"] == "location"
            or (isinstance(body.get("type"), str) and body["type"] == "location")
        ):
            location_affected = db.conn.execute(
                """WITH RECURSIVE tree(id) AS (
                       SELECT ?
                       UNION ALL
                       SELECT k.id FROM keywords k
                       JOIN tree t ON k.parent_id = t.id
                   )
                   SELECT DISTINCT p.id AS photo_id, wf.workspace_id
                   FROM photos p
                   JOIN photo_keywords pk ON pk.photo_id = p.id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   JOIN tree t ON t.id = pk.keyword_id""",
                (keyword_id,),
            ).fetchall()
        # Reject '|' in a rename that lands on a location keyword before we
        # ever touch the row. ``get_or_create_text_location`` refuses pipes
        # at creation time because XMP keyword hierarchies reserve it as the
        # delimiter and there is no reversible XMP encoding, but the update
        # path used to accept them. Once such a rename landed, every sync
        # of a photo tagged with the row raised in
        # ``SidecarEditor.set_location_keywords``, leaving the ``location``
        # change queued forever and blocking any other edit that shared
        # its sidecar transaction. Guard both a name-only rename of an
        # existing location and a retype-into-location that also renames.
        rename_target = body.get("name")
        if isinstance(rename_target, str) and "|" in rename_target:
            effective_type = body.get("type")
            if not isinstance(effective_type, str):
                effective_type = old_row["type"] if old_row is not None else None
            if effective_type == "location":
                return json_error(
                    "location name may not contain '|' -- XMP keyword "
                    "hierarchies reserve it as the level delimiter",
                    400,
                )
        # Apply the update first — if it raises, no sidecar changes are queued
        try:
            effective_id = db.update_keyword(keyword_id, **body)
        except ValueError as e:
            return json_error(str(e), 400)
        # Queue sidecar updates only after a successful DB update, using the
        # STORED spelling of the surviving row — update_keyword normalizes
        # the requested name, so it can differ from the raw request value.
        new_row = db.conn.execute(
            """SELECT name, is_species, type
               FROM keywords WHERE id = ?""",
            (effective_id,),
        ).fetchone()
        if (
            old_row is not None and new_row is not None
            and old_row["name"] != new_row["name"]
        ):
            old_name = old_row["name"]
            new_name = new_row["name"]
            old_is_species_keyword = (
                old_row["is_species"] == 1 or old_row["type"] == "taxonomy"
            )
            new_is_species_keyword = (
                new_row["is_species"] == 1 or new_row["type"] == "taxonomy"
            )
            if old_is_species_keyword and new_is_species_keyword:
                pairs = [
                    (row["photo_id"], row["workspace_id"]) for row in affected
                ]
                db.rename_photo_preferences_species(
                    old_name,
                    new_name,
                    pairs,
                )
                db.rename_species_highlights_species(
                    old_name,
                    new_name,
                    pairs,
                )
            # A pure location-to-location rename is handled entirely by the
            # ``location`` change queued below WHEN
            # ``write_location_keywords_to_xmp`` is on for the affected
            # workspace: set_location_keywords() writes both flat and
            # hierarchical entries under the new leaf and claims full
            # marker ownership because the sidecar is stripped of the old
            # marker's entries first. Queueing a keyword_add for the new
            # leaf in that case would land it in ``dc:subject`` BEFORE
            # set_location_keywords() runs, at which point it looks
            # pre-existing (existed_flat=True) and gets only hierarchical
            # ownership; a later clear would then leave the renamed flat
            # leaf in XMP indefinitely.
            #
            # When the setting is OFF for a workspace, however, the queued
            # ``location`` change only runs marker cleanup at sync time;
            # set_location_keywords() never writes the new flat leaf, so
            # a pre-existing flat XMP keyword under the OLD name (from a
            # manual entry or an earlier period when the setting was on)
            # would stay behind indefinitely. Fall back to the ordinary
            # keyword_remove + keyword_add for those workspaces so the
            # flat leaf still gets renamed in XMP.
            location_to_location_rename = (
                old_row["type"] == "location"
                and new_row["type"] == "location"
            )
            skip_keyword_requeue_by_ws = {}
            if location_to_location_rename:
                import config as cfg

                try:
                    global_cfg = cfg.load()
                except Exception:
                    global_cfg = {}
                for row in affected:
                    ws_id = row["workspace_id"]
                    if ws_id in skip_keyword_requeue_by_ws:
                        continue
                    ws = db.get_workspace(ws_id)
                    raw = ws["config_overrides"] if ws else None
                    skip_keyword_requeue_by_ws[ws_id] = (
                        workspace_effective_setting(
                            raw, global_cfg, LOCATION_KEYWORDS_SETTING,
                        )
                    )
            for row in affected:
                if skip_keyword_requeue_by_ws.get(row["workspace_id"], False):
                    continue
                queue_keyword_remove(db, row["photo_id"], old_name, workspace_id=row["workspace_id"])
                queue_keyword_add(db, row["photo_id"], new_name, workspace_id=row["workspace_id"])
        # A location→non-location retype with no name change queues a
        # ``location`` change below but no keyword_add — the name-change
        # block above didn't run. sync_to_xmp() therefore resolves no
        # location path for the photo and remove_vireo_location_keywords()
        # strips the marker-owned flat leaf, leaving XMP without a keyword
        # that the DB still assigns to the photo. Queue an ordinary
        # keyword_add so the flat leaf survives the location cleanup as
        # the user's newly ``general`` keyword. Name-changed retypes
        # already had the add queued for the new spelling above.
        if (
            old_row is not None and new_row is not None
            and old_row["type"] == "location"
            and new_row["type"] != "location"
            and old_row["name"] == new_row["name"]
        ):
            for row in affected:
                queue_keyword_add(
                    db, row["photo_id"], new_row["name"],
                    workspace_id=row["workspace_id"],
                )
        # If a location keyword's name or type changed, requeue a
        # ``location`` change for every descendant-tagged photo so
        # ``sync_to_xmp`` rewrites the sidecar's hierarchy path and
        # ``vireo:locationKeywords`` marker under the new spelling.
        # ``keyword_remove``/``keyword_add`` above only rewrites the flat
        # ``dc:subject`` entry (and only for a leaf rename); the hierarchy
        # and marker require the ``location`` change type. ``queue_change``
        # dedupes silently, so re-running an already-queued photo is
        # harmless.
        if location_affected and old_row is not None and new_row is not None:
            old_was_location = old_row["type"] == "location"
            new_is_location = new_row["type"] == "location"
            name_changed = old_row["name"] != new_row["name"]
            type_changed = old_row["type"] != new_row["type"]
            if (old_was_location or new_is_location) and (
                name_changed or type_changed
            ):
                for row in location_affected:
                    db.queue_change(
                        row["photo_id"],
                        "location",
                        "effective",
                        workspace_id=row["workspace_id"],
                        _commit=False,
                    )
                db.conn.commit()
        # keywords.html's updateType/renameKeyword/bulk-apply handlers refetch
        # only when `merged` is truthy; without it the UI keeps the deleted
        # source id and its next edit/delete would 404 or hit the wrong row.
        merged = effective_id != keyword_id
        return jsonify({"ok": True, "keyword_id": effective_id, "merged": merged})

    @blueprint.route("/api/keywords/<int:keyword_id>", methods=["DELETE"])
    def api_delete_keyword(keyword_id):
        db = get_db()
        # Queue sidecar removals for all affected workspaces
        kw_row = db.conn.execute(
            "SELECT name, type FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        if kw_row:
            affected = db.conn.execute(
                """SELECT pk.photo_id, wf.workspace_id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                   WHERE pk.keyword_id = ?""",
                (keyword_id,),
            ).fetchall()
            for row in affected:
                queue_keyword_remove(db, row["photo_id"], kw_row["name"], workspace_id=row["workspace_id"])
            # Deleting a location ancestor detaches its children (they
            # become root-level keywords) but no photo is tagged with the
            # ancestor directly, so ``affected`` is empty and no
            # ``keyword_remove`` reaches the sidecar. Meanwhile the
            # descendant leaves that ARE tagged still export their old
            # hierarchy path (e.g. ``France|Paris``) and the
            # ``vireo:locationKeywords`` marker keeps pointing at the
            # deleted ancestor. Snapshot every descendant-tagged photo
            # (recursively, including the keyword itself for the leaf
            # case) and queue a ``location`` change so ``sync_to_xmp``
            # rewrites the hierarchy path and marker under the surviving
            # ancestor chain on the next sync. ``queue_change`` dedupes,
            # so the same photo appearing under ``affected`` above is
            # harmless.
            if kw_row["type"] == "location":
                location_descendants = db.conn.execute(
                    """WITH RECURSIVE tree(id) AS (
                           SELECT ?
                           UNION ALL
                           SELECT k.id FROM keywords k
                           JOIN tree t ON k.parent_id = t.id
                       )
                       SELECT DISTINCT p.id AS photo_id, wf.workspace_id
                       FROM photos p
                       JOIN photo_keywords pk ON pk.photo_id = p.id
                       JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                       JOIN tree t ON t.id = pk.keyword_id""",
                    (keyword_id,),
                ).fetchall()
                for row in location_descendants:
                    db.queue_change(
                        row["photo_id"],
                        "location",
                        "effective",
                        workspace_id=row["workspace_id"],
                        _commit=False,
                    )
        db.conn.execute("UPDATE keywords SET parent_id = NULL WHERE parent_id = ?", (keyword_id,))
        db.conn.execute("DELETE FROM photo_keywords WHERE keyword_id = ?", (keyword_id,))
        db.conn.execute("DELETE FROM keywords WHERE id = ?", (keyword_id,))
        db.conn.commit()
        return jsonify({"ok": True})

    return blueprint
