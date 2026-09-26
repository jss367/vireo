"""Encounter species confirmation from the pipeline review page.

``/api/encounters/species`` confirms (replaces, adds or removes) the species
for every photo in a cached encounter or a single burst: it tags the photos,
queues the sidecar keyword changes, rewrites the pipeline results cache and
records a grouping-history edit. It takes the workspace regroup lock and the
shared prediction-decision lock through ``services.prediction_decisions``.
"""

from __future__ import annotations

import copy
import json
import logging
import os

from flask import Blueprint, jsonify, request
from keyword_normalization import keyword_match_key, normalize_keyword_display
from pipeline_results import auto_detach_burst_for_species
from services import prediction_decisions
from services.pending_changes import queue_keyword_add, queue_keyword_remove
from sql_chunks import chunked

log = logging.getLogger(__name__)


def create_encounters_blueprint(get_db, json_error, db_path):
    """Build the encounter species-confirmation blueprint.

    ``db_path`` locates the pipeline results cache (it lives next to the
    database).
    """
    blueprint = Blueprint("encounters", __name__)

    @blueprint.route("/api/encounters/species", methods=["POST"])
    def api_encounter_species():
        """Confirm species for all photos in an encounter or a single burst.

        Expects JSON: {"species": "Blue Jay", "photo_ids": [1, 2, 3],
                       "burst_index": <int|null>,
                       "add": <bool>, "remove": <bool>,
                       "previous_species": <str|null>}

        Creates the species keyword, tags photos, and queues a sidecar add.
        If the encounter (or burst) was previously confirmed as a different
        species, also untags that species and queues a sidecar remove — or
        cancels the still-pending add if it hadn't synced yet — so the XMP
        doesn't accumulate stale species keywords.

        A burst can legitimately hold two species (two subjects), so the
        confirmation is a *set* edit with three modes:

        * replace (default): ``species`` swaps out ``previous_species`` —
          the burst's primary confirmed species unless the client names
          another entry of the current list.
        * ``add``: ``species`` joins the current list; nothing is untagged
          and the burst is never auto-detached.
        * ``remove``: ``species`` is untagged from the submitted photos and
          dropped from the list; no keyword is added.
        """
        from pipeline_locks import acquire_workspace_regroup

        db = get_db()
        with acquire_workspace_regroup(db._ws_id()):
            return prediction_decisions.under_prediction_decision_lock(
                db, lambda: _confirm_encounter_species(db),
                json_error=json_error,
            )

    def _confirm_encounter_species(db):
        """Confirm species while holding the grouping and database writer locks."""
        body = request.get_json(silent=True) or {}
        species = body.get("species", "").strip()
        photo_ids = body.get("photo_ids", [])
        burst_index = body.get("burst_index")
        add_mode = bool(body.get("add"))
        remove_mode = bool(body.get("remove"))
        requested_previous = body.get("previous_species")
        if add_mode and remove_mode:
            return json_error("add and remove are mutually exclusive")
        if requested_previous is not None and (add_mode or remove_mode):
            return json_error("previous_species only applies to a replace")

        # Normalize up front: this route compares the requested species
        # against stored keyword rows and previous_species cache values
        # below, and add_keyword would normalize on insert anyway — keep one
        # spelling throughout. Rejects quote-only input as empty.
        species = normalize_keyword_display(species)
        if not species:
            return json_error("species is required")
        if not photo_ids:
            return json_error("photo_ids is required")

        # Validate all photo_ids exist before mutating. Chunked so the
        # IN-clause stays under SQLite's bound-parameter cap.
        found_ids = set()
        for chunk in chunked(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                f"SELECT id FROM photos WHERE id IN ({placeholders})", chunk
            ).fetchall()
            found_ids.update(r["id"] for r in rows)
        missing = [pid for pid in photo_ids if pid not in found_ids]
        if missing:
            return json_error(f"Unknown photo_ids: {missing}")
        # Photos are global; a confirmation tags keywords and queues sidecar
        # writes under the active workspace, so it may only touch photos
        # that workspace can see.
        visible_ids = set(db.filter_photo_ids_in_workspace(photo_ids))
        foreign = [pid for pid in photo_ids if pid not in visible_ids]
        if foreign:
            return json_error(
                f"photo_ids not in the active workspace: {foreign}", 403
            )

        # Surface photos whose only detections are below the workspace's
        # detector_confidence threshold, but do not drop them. This endpoint
        # represents an explicit user confirmation, and that assertion should
        # win over a weak detector box. Fully automatic labeling paths should
        # do their own filtering before calling lower-level tag helpers.
        import config as cfg
        effective_cfg = db.get_effective_config(cfg.load())
        det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)
        det_rows = []
        for chunk in chunked(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            det_rows.extend(db.conn.execute(
                f"""SELECT photo_id,
                           MAX(detector_confidence) AS max_conf,
                           COUNT(*) AS n
                    FROM detections WHERE photo_id IN ({placeholders})
                    GROUP BY photo_id""",
                chunk,
            ).fetchall())
        low_confidence_photo_ids = [
            r["photo_id"] for r in det_rows
            if r["n"] > 0 and (r["max_conf"] or 0) < det_conf_threshold
        ]
        from pipeline import load_results_raw, save_results_raw
        from pipeline_results import (
            build_species_override,
            burst_species_list,
            empty_species_override,
            encounter_confirmed_species_list,
            set_encounter_confirmed_species,
            species_key_set,
            updated_species_list,
        )

        cache_dir = os.path.dirname(db_path)
        cached = load_results_raw(cache_dir, db._active_workspace_id)
        before_cached = copy.deepcopy(cached)
        cache_saved = False
        previous_species = None
        current_species_list = []
        target_enc = None
        target_enc_idx = None
        if cached:
            photo_id_set = set(photo_ids)
            for enc_idx, enc in enumerate(cached.get("encounters", [])):
                enc_ids = set(enc.get("photo_ids", []))
                if not photo_id_set.issubset(enc_ids):
                    continue
                target_enc = enc
                target_enc_idx = enc_idx
                break

        # If this is a burst-scoped request, the burst must actually exist in
        # the cached encounter AND the submitted photo_ids must be a subset of
        # that burst's photos. Otherwise a stale client (e.g. one that still
        # holds a burst index from before a regrouping) could retag photos
        # that don't belong to this burst while the cache update below touches
        # the wrong override.
        if burst_index is not None:
            bursts = target_enc.get("bursts") if target_enc else None
            if not bursts or not (0 <= burst_index < len(bursts)):
                return json_error(
                    f"Unknown burst_index {burst_index} for submitted photos",
                )
            burst_photo_ids = set(bursts[burst_index].get("photo_ids", []))
            if not set(photo_ids).issubset(burst_photo_ids):
                return json_error(
                    f"photo_ids are not members of bursts[{burst_index}]",
                )

        if target_enc is not None:
            if burst_index is not None:
                # A burst override wins when recorded; otherwise the burst
                # inherits the encounter's confirmed species, which is what
                # those photos were actually tagged with.
                current_species_list = burst_species_list(
                    target_enc, target_enc["bursts"][burst_index],
                )
            else:
                current_species_list = encounter_confirmed_species_list(
                    target_enc,
                )

        # The cached list comes from the pipeline cache on disk, which the
        # v5 DB migration does not touch — a pre-normalization cache can
        # still carry a quoted spelling like `‘Apapane`. Normalize before
        # comparing/looking up against stored keyword rows, which are always
        # clean.
        current_species_list = [
            name for name in (
                normalize_keyword_display(s) for s in current_species_list
            ) if name
        ]

        if remove_mode:
            # Only a currently confirmed species can be removed; otherwise a
            # stale client could strip an unrelated keyword from the photos
            # while the cache list (which has nothing to drop) still reports
            # success.
            requested_key = keyword_match_key(species)
            previous_species = next(
                (
                    s for s in current_species_list
                    if keyword_match_key(s) == requested_key
                ),
                None,
            )
            if previous_species is None:
                return json_error(
                    f'"{species}" is not a confirmed species of the '
                    "submitted photos",
                )
            species = previous_species
        elif add_mode:
            previous_species = None
        elif requested_previous is not None:
            requested_previous = normalize_keyword_display(
                str(requested_previous),
            )
            if not requested_previous:
                return json_error("previous_species must not be empty")
            # The named species must be one the burst is currently confirmed
            # as — including when nothing is confirmed (or there is no
            # cache), otherwise a stale client could have an arbitrary
            # keyword untagged from the submitted photos.
            requested_key = keyword_match_key(requested_previous)
            previous_species = next(
                (
                    s for s in current_species_list
                    if keyword_match_key(s) == requested_key
                ),
                None,
            )
            if previous_species is None:
                return json_error(
                    f'"{requested_previous}" is not a confirmed species '
                    "of the submitted photos",
                )
        else:
            previous_species = (
                current_species_list[0] if current_species_list else None
            )

        ws_id = db._ws_id()

        old_kid_row = None
        # ``per_photo_old_row`` maps each submitted photo to the effective
        # old-species keyword row the removal loop should match against.
        # For root-lookup (parent_id IS NULL) hits every photo shares the
        # same row; for the hierarchy-leaf fallback, catalogs can hold the
        # same alias under different taxa across submitted photos (a
        # legitimate homonym; duplicate repair deliberately preserves such
        # rows), so each photo gets its own resolved row. A single
        # ``best_taxon`` used to select one row for the whole batch would
        # leave every photo on a different taxon with its old leaf still
        # attached — the removal loop matches by ``taxon_id``.
        per_photo_old_row = {}
        # Compare using the ASCII-only fold SQLite's `COLLATE NOCASE` (which
        # add_keyword's dedupe relies on) uses — Python's str.lower() folds
        # non-ASCII letters that SQLite treats as distinct. Without this, a
        # cache-recorded confirmed species `Éclair` and a user-submitted
        # `éclair` would match here, the replacement path would be skipped,
        # yet add_keyword's NOCASE lookup would still create/tag a separate
        # `éclair` row — leaving the photo with both taxonomy tags and no
        # remove queued for the old one.
        is_replacement = remove_mode or (
            previous_species is not None
            and keyword_match_key(previous_species) != keyword_match_key(species)
        )
        if is_replacement:
            # Match add_keyword's write path: species keywords live as root
            # keywords (parent_id IS NULL). Looking up by name alone could
            # collide with a non-species homonym nested under another keyword
            # (schema allows UNIQUE(name, parent_id)). Accept taxonomy-typed
            # rows with is_species=0 too: update_keyword with an explicit
            # type='taxonomy' (the Keywords type dropdown) doesn't set the
            # legacy is_species column, and the rest of the app treats
            # (is_species = 1 OR type = 'taxonomy') as species.
            old_kid_row = db.conn.execute(
                """SELECT id, name, taxon_id FROM keywords
                   WHERE name = ? COLLATE NOCASE
                     AND parent_id IS NULL
                     AND (is_species = 1 OR type = 'taxonomy')""",
                (previous_species,),
            ).fetchone()
            if old_kid_row is not None and old_kid_row["taxon_id"] is None:
                # Unlinked root: ``previous_species`` names a specific
                # legacy row whose identity is that row's own id, not a
                # taxon. Assign it to every submitted photo — the removal
                # loop below relies on this exact-row identity to route
                # through the ``eff_taxon_id is None`` branch of its
                # homonym-conflict guard, which distinguishes the unlinked
                # legacy species from any linked same-name row on the
                # photo (a distinct species that must be preserved). A
                # per-photo resolution would pick up the attached linked
                # homonym row and treat that linked species AS the
                # previous species, queueing it for removal.
                for pid in photo_ids:
                    per_photo_old_row[pid] = old_kid_row
            else:
                # Linked root (its identity IS the taxon, and taxon-based
                # matching in the removal loop is what actually decides
                # equivalence) OR no root at all: resolve per-photo from
                # the rows actually attached to each submitted photo. A
                # single catalog-wide root assignment would map every
                # submitted photo to that root's taxon; the removal loop
                # matches attached rows by ``taxon_id``, so any submitted
                # photo whose only same-name tag is a hierarchy leaf
                # under a different taxon (a legitimate homonym —
                # duplicate repair deliberately preserves these rows)
                # would keep its old leaf attached while the new species
                # is added on top, leaving a stale duplicate. The
                # per-photo query naturally covers both attached shapes:
                # its ``ORDER BY parent_id IS NULL`` puts an attached
                # root first (fast common case) and otherwise picks the
                # attached hierarchy leaf on the photo's own taxon.
                #
                # Filter to species-rank (or NULL-rank) taxonomy-typed
                # rows and accept ``is_species = 1`` rows too — for the
                # same taxonomy-typed vs. legacy-species reason as the
                # root lookup.
                #
                # When a linked root exists, ALSO match rows whose
                # ``taxon_id`` equals the root's taxon even if the leaf
                # display name differs from ``previous_species`` — this
                # is how repaired hierarchy leaves (e.g. ``Desert Verdin``
                # under root ``Verdin`` after duplicate repair detached
                # the redundant root) get resolved. Without it, the
                # removal loop can't find any old row on the photo and
                # the endpoint records a plain add, leaving the photo
                # tagged with both the old leaf and the new species.
                linked_root_taxon = (
                    old_kid_row["taxon_id"] if old_kid_row is not None else None
                )
                old_kid_row = None
                candidate_rows = []
                for chunk in chunked(photo_ids):
                    placeholders_ids = ",".join("?" for _ in chunk)
                    if linked_root_taxon is not None:
                        # Match by the linked root's taxon OR by
                        # ``previous_species`` name. The taxon arm catches
                        # repaired aliases (e.g. ``Desert Verdin`` sharing
                        # the root's taxon); the name arm preserves the
                        # existing behavior for attached same-name rows
                        # under a different taxon (a legitimate homonym
                        # duplicate repair leaves alone).
                        candidate_rows.extend(
                            db.conn.execute(
                                f"""SELECT k.id, k.name, k.taxon_id, pk.photo_id
                                    FROM photo_keywords pk
                                    JOIN keywords k ON k.id = pk.keyword_id
                                    LEFT JOIN taxa t ON t.id = k.taxon_id
                                    WHERE pk.photo_id IN ({placeholders_ids})
                                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                                      AND (t.rank = 'species' OR t.rank IS NULL)
                                      AND (k.taxon_id = ?
                                           OR k.name = ? COLLATE NOCASE)
                                    ORDER BY CASE WHEN k.parent_id IS NULL
                                                  THEN 0 ELSE 1 END,
                                             k.id""",
                                [*chunk, linked_root_taxon, previous_species],
                            ).fetchall()
                        )
                    else:
                        candidate_rows.extend(
                            db.conn.execute(
                                f"""SELECT k.id, k.name, k.taxon_id, pk.photo_id
                                    FROM photo_keywords pk
                                    JOIN keywords k ON k.id = pk.keyword_id
                                    LEFT JOIN taxa t ON t.id = k.taxon_id
                                    WHERE pk.photo_id IN ({placeholders_ids})
                                      AND k.name = ? COLLATE NOCASE
                                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                                      AND (t.rank = 'species' OR t.rank IS NULL)
                                    ORDER BY CASE WHEN k.parent_id IS NULL
                                                  THEN 0 ELSE 1 END,
                                             k.id""",
                                [*chunk, previous_species],
                            ).fetchall()
                        )
                for row in candidate_rows:
                    # SQL orders root rows first, then by id — first
                    # candidate per photo is the most canonical.
                    per_photo_old_row.setdefault(row["photo_id"], row)
                if per_photo_old_row:
                    # ``old_kid_row`` gates the removal block below.
                    # ``old_target_key`` is derived from
                    # ``previous_species`` (not this representative row)
                    # so the NULL-taxon fallback in the removal loop
                    # stays keyed to the requested display name even
                    # when the resolved row is a differently-named
                    # hierarchy alias.
                    old_kid_row = next(iter(per_photo_old_row.values()))

        # Run all mutations in a single transaction so that a mid-loop failure
        # (SQLite lock, disk error, etc.) can't leave half the photos retagged
        # while the other half still carry the old species.
        try:
            # Resolve the target species keyword id up front so we can precheck
            # which photos already carry it. add_keyword is idempotent and
            # returns the existing id when the species already exists.
            if remove_mode:
                # Nothing is being added: never create a keyword row just to
                # remove it. Report the existing root's id when one exists.
                stored = db.conn.execute(
                    """SELECT id, name FROM keywords
                       WHERE name = ? COLLATE NOCASE
                         AND parent_id IS NULL
                         AND (is_species = 1 OR type = 'taxonomy')
                       ORDER BY (type = 'taxonomy') DESC, id""",
                    (species,),
                ).fetchone()
                kid = stored["id"] if stored else None
                already_has_new = set()
                newly_tagged = []
            else:
                kid = db.add_keyword(species, is_species=True, _commit=False)
                # Queue/record the stored spelling (see api_add_keyword).
                stored = db.conn.execute(
                    "SELECT name FROM keywords WHERE id = ?", (kid,)
                ).fetchone()
                if stored and stored["name"]:
                    species = stored["name"]

                # A species can already be attached through a hierarchical
                # keyword row with a different id/casing. Compare by taxon_id
                # (or normalized name for taxonomy-less legacy rows), otherwise
                # a confirmation creates a redundant top-level association.
                already_has_new = db.get_photos_with_equivalent_species(
                    photo_ids, kid,
                )
                newly_tagged = [
                    pid for pid in photo_ids if pid not in already_has_new
                ]

            # Resolve every attached keyword row equivalent to the previous
            # species, including nested hierarchy leaves. Multiple hierarchy
            # placements are deliberate and survive duplicate repair, so a
            # replacement must remove and record all of them for undo/redo.
            old_rows_by_photo = {}
            if is_replacement and old_kid_row is not None:
                # ``old_target_key`` keys the NULL-taxon fallback in the
                # removal loop below. Derive it from ``previous_species``
                # (the requested display name) rather than the resolved
                # representative row's stored name — with the taxon-based
                # per-photo resolution above, that row can be a hierarchy
                # alias whose leaf name differs from what the user typed
                # (e.g. resolved ``Desert Verdin`` for requested
                # ``Verdin``), and the NULL-taxon fallback should still
                # match a legacy ``Verdin`` row on the photo.
                old_target_key = keyword_match_key(previous_species)
                # When the previous species is linked to a taxon and another
                # taxonomy row anywhere in the catalog shares the same
                # normalized name but points at a different taxon (e.g.
                # legacy ``Robin`` alongside taxonomy ``robin``), an
                # unlinked NULL-taxon row could belong to either species.
                # Treating it as the old species would queue a legacy
                # homonym tag for removal. Mirror the guard in
                # get_photos_with_equivalent_species.
                #
                # The same guard applies when the *previous species* is
                # unlinked: any distinct linked same-key row is a different
                # species, and folding it in would let encounter replacement
                # delete a taxonomy species from the photo.
                #
                # Cache the check per (taxon_id, kid_id) — hierarchy fallback
                # can resolve different taxa across submitted photos, and
                # each unique taxon gets its own homonym conflict answer.
                homonym_conflict_cache = {}

                def _homonym_conflict(target_taxon_id, target_kid_id):
                    cache_key = (
                        target_taxon_id,
                        None if target_taxon_id is not None else target_kid_id,
                    )
                    if cache_key in homonym_conflict_cache:
                        return homonym_conflict_cache[cache_key]
                    conflict = False
                    if target_taxon_id is not None:
                        hrows = db.conn.execute(
                            """SELECT name FROM keywords
                               WHERE (is_species = 1 OR type = 'taxonomy')
                                 AND taxon_id IS NOT NULL
                                 AND taxon_id != ?""",
                            (target_taxon_id,),
                        ).fetchall()
                    else:
                        hrows = db.conn.execute(
                            """SELECT name FROM keywords
                               WHERE (is_species = 1 OR type = 'taxonomy')
                                 AND taxon_id IS NOT NULL
                                 AND id != ?""",
                            (target_kid_id,),
                        ).fetchall()
                    for hrow in hrows:
                        if keyword_match_key(hrow["name"]) == old_target_key:
                            conflict = True
                            break
                    homonym_conflict_cache[cache_key] = conflict
                    return conflict

                for chunk in chunked(photo_ids):
                    placeholders_ids = ",".join("?" for _ in chunk)
                    rows = db.conn.execute(
                        f"""SELECT pk.photo_id, k.id, k.name, k.taxon_id
                            FROM photo_keywords pk
                            JOIN keywords k ON k.id = pk.keyword_id
                            LEFT JOIN taxa t ON t.id = k.taxon_id
                            WHERE pk.photo_id IN ({placeholders_ids})
                              AND (k.is_species = 1 OR k.type = 'taxonomy')
                              AND (t.rank = 'species' OR t.rank IS NULL)
                            ORDER BY pk.photo_id,
                                     CASE WHEN k.parent_id IS NULL THEN 0 ELSE 1 END,
                                     k.id""",
                        list(chunk),
                    ).fetchall()
                    for row in rows:
                        photo_old = per_photo_old_row.get(row["photo_id"])
                        if photo_old is None:
                            # ``previous_species`` did not resolve to any
                            # row attached to this photo (no root-lookup
                            # hit, no candidate leaf); nothing to remove.
                            continue
                        eff_taxon_id = photo_old["taxon_id"]
                        eff_kid_id = photo_old["id"]
                        eff_homonym_conflict = _homonym_conflict(
                            eff_taxon_id, eff_kid_id,
                        )
                        if eff_taxon_id is None:
                            if eff_homonym_conflict:
                                # Unlinked previous species with a linked
                                # same-key homonym in the catalog: only the
                                # exact resolved old row is safe to remove.
                                same_species = row["id"] == eff_kid_id
                            else:
                                same_species = (
                                    keyword_match_key(row["name"]) == old_target_key
                                )
                        else:
                            same_species = (
                                row["taxon_id"] == eff_taxon_id
                            ) or (
                                not eff_homonym_conflict
                                and row["taxon_id"] is None
                                and keyword_match_key(row["name"]) == old_target_key
                            )
                        if same_species:
                            old_rows_by_photo.setdefault(row["photo_id"], []).append(row)

                # Same-taxon replacements (e.g., renaming to a scientific-name
                # alias) make the earlier taxon-equivalence precheck stale:
                # every equivalent row is scheduled for removal, so the photo
                # would end up carrying no species keyword. Recompute
                # equivalence while ignoring the rows about to be untagged and
                # move any now-uncovered photo back into ``newly_tagged`` so
                # the tag_photo/keyword_add loop below still writes ``kid``.
                excluded_ids = {
                    row["id"]
                    for rows in old_rows_by_photo.values()
                    for row in rows
                }
                if excluded_ids:
                    replacement_covered = list(
                        already_has_new & set(old_rows_by_photo)
                    )
                    if replacement_covered:
                        survivors = db.get_photos_with_equivalent_species(
                            replacement_covered, kid,
                            exclude_keyword_ids=excluded_ids,
                        )
                        lost_equivalence = set(replacement_covered) - survivors
                        if lost_equivalence:
                            already_has_new -= lost_equivalence
                            newly_tagged = [
                                pid for pid in photo_ids
                                if pid not in already_has_new
                            ]

                for pid, old_rows in old_rows_by_photo.items():
                    remove_names = []
                    for old in old_rows:
                        db.untag_photo(pid, old["id"], _commit=False)
                        # A same-taxon alias replace re-adds ``kid`` right
                        # after, so its sidecar remove is skipped; a remove
                        # must queue every untagged row.
                        if (remove_mode or old["id"] != kid) and (
                            old["name"] not in remove_names
                        ):
                            remove_names.append(old["name"])
                    for old_name in remove_names:
                        queue_keyword_remove(
                            db, pid, old_name, workspace_id=ws_id, _commit=False,
                        )

            had_old = set(old_rows_by_photo)

            for pid in newly_tagged:
                db.tag_photo(pid, kid, source="manual", _commit=False)
                queue_keyword_add(
                    db, pid, species, workspace_id=ws_id, _commit=False,
                )

            photo_edit_id = None
            if is_replacement and had_old:
                # Photos that actually changed: had the old keyword (so the
                # remove side fired) and/or newly gained the new one. Use the
                # union so undo restores the exact state we mutated.
                newly_set = set(newly_tagged)
                changed = [
                    pid for pid in photo_ids if pid in had_old or pid in newly_set
                ]
                items = []
                for pid in changed:
                    old_ids = [row["id"] for row in old_rows_by_photo.get(pid, [])]
                    if len(old_ids) > 1:
                        old_value = json.dumps({
                            "keyword_id": old_ids[0],
                            "keyword_ids": old_ids,
                        }, sort_keys=True)
                    else:
                        old_value = str(old_ids[0]) if old_ids else ""
                    items.append({
                        "photo_id": pid,
                        "old_value": old_value,
                        "new_value": str(kid) if pid in newly_set else "",
                    })
                if remove_mode:
                    description = (
                        f'Removed species "{species}" from {len(changed)} photos'
                    )
                else:
                    description = (
                        f'Replaced species "{previous_species}" with '
                        f'"{species}" on {len(changed)} photos'
                    )
                photo_edit_id = db.record_edit(
                    "species_replace",
                    description,
                    str(kid) if kid is not None else "",
                    items,
                    is_batch=len(changed) > 1,
                    _commit=False,
                )
            elif newly_tagged:
                items = [
                    {"photo_id": pid, "old_value": "", "new_value": str(kid)}
                    for pid in newly_tagged
                ]
                photo_edit_id = db.record_edit(
                    "keyword_add",
                    f'Confirmed species "{species}" on {len(newly_tagged)} photos',
                    str(kid),
                    items,
                    is_batch=len(newly_tagged) > 1,
                    _commit=False,
                )
            # The confirmed set after this edit, for the cache, the history
            # entry and the response.
            new_species_list = updated_species_list(
                current_species_list, species, previous_species,
                add=add_mode, remove=remove_mode,
            )
            if remove_mode:
                cache_description = f'Removed species "{species}" from '
            elif add_mode:
                cache_description = f'Added species "{species}" on '
            else:
                cache_description = f'Confirmed species "{species}" on '
            new_burst_override = None
            will_auto_detach = False
            new_encounter_state = None
            if cached and target_enc is not None:
                # The override's confirmed state comes from the database (read
                # inside this transaction, so it sees the tags just written),
                # the same rule serialize_results applies on regroup: a burst
                # is confirmed as the species every frame carries, and only
                # when every entry of the edited list is on every frame. Per-
                # frame extras ([A, C] beside [A]) do not unconfirm it — the
                # user just confirmed the set on all of it — and the list is
                # the cache's ordering plus any extra the frames all share
                # that the cache had not recorded. An edit that leaves some
                # frame without the full set records the list unconfirmed;
                # it still stays authoritative (burst_species_list), because
                # None would make the burst inherit the encounter's stale
                # list and resurrect a species this edit removed. An emptied
                # burst keeps an explicit empty override for the same reason.
                actual_by_photo = db.get_species_keywords_for_photos(photo_ids)
                actual_sets = [
                    species_key_set(actual_by_photo.get(pid, []))
                    for pid in photo_ids
                ]
                shared_keys = (
                    set.intersection(*actual_sets) if actual_sets else set()
                )
                recorded = species_key_set(new_species_list)
                frames_share = (
                    bool(actual_sets)
                    and all(actual_sets)
                    and bool(shared_keys)
                    and recorded <= shared_keys
                )
                if burst_index is not None:
                    if not new_species_list:
                        new_burst_override = empty_species_override()
                    elif frames_share:
                        extras = [
                            s for s in actual_by_photo.get(photo_ids[0], [])
                            if keyword_match_key(s) in shared_keys
                            and keyword_match_key(s) not in recorded
                        ]
                        new_burst_override = build_species_override(
                            new_species_list + extras,
                        )
                    else:
                        new_burst_override = build_species_override(
                            new_species_list, confirmed=False,
                        )
                    # Auto-detach if the burst's species no longer overlap
                    # its encounter's — splits it out and merges into an
                    # adjacent encounter of the same confirmed species when
                    # one exists. Compare with keyword_match_key so a cached
                    # pre-normalization spelling (e.g. ‘Apapane) does not
                    # trigger a needless split against the stored species
                    # (Apapane); the DB write already normalized to the
                    # canonical form. Adding a second species never
                    # detaches (it can only widen overlap); a remove
                    # detaches only when it leaves a non-empty set that
                    # shares nothing with the encounter. An emptied burst
                    # stays put: there is nothing to file it under.
                    enc_species_list = encounter_confirmed_species_list(target_enc)
                    if not enc_species_list and target_enc.get("species"):
                        enc_species_list = [target_enc["species"][0]]
                    will_auto_detach = (
                        not add_mode
                        and bool(new_species_list)
                        and bool(enc_species_list)
                        and not (
                            species_key_set(new_species_list)
                            & species_key_set(enc_species_list)
                        )
                        and len(target_enc["bursts"]) > 1
                    )
                else:
                    new_encounter_state = {
                        "confirmed_species": (
                            new_species_list[0] if new_species_list else None
                        ),
                        "confirmed_species_list": list(new_species_list),
                        "species_confirmed": bool(new_species_list) and frames_share,
                    }

            cache_only_write = (
                not (is_replacement and had_old)
                and not newly_tagged
                and cached
                and target_enc is not None
            )
            if cache_only_write:
                # No keyword row was recorded (every photo already carries
                # this species), but the cache mutation below will still
                # write ``confirmed_species`` / ``species_confirmed`` (or a
                # burst ``species_override``). Without a matching history
                # entry, a preceding grouping edit would remain the newest
                # undoable row and its undo would silently discard this
                # confirmation because grouping signatures ignore these
                # species fields by design. Record the cache-only write so
                # LIFO undo reverts it first. When auto-detach will fire
                # the structural change and confirmation share one grouping
                # history entry further down instead.
                from services.grouping_history import (
                    record_species_confirm_cache,
                )
                if burst_index is not None:
                    burst_target = target_enc["bursts"][burst_index]
                    current_override = burst_target.get("species_override")
                    if current_override != new_burst_override and not will_auto_detach:
                        record_species_confirm_cache(
                            db, species=species, target_enc=target_enc,
                            burst_index=burst_index,
                            submitted_photo_ids=photo_ids,
                            new_override=new_burst_override,
                            description=cache_description + "1 burst",
                        )
                else:
                    current_state = {
                        "confirmed_species": target_enc.get("confirmed_species"),
                        "confirmed_species_list": list(
                            encounter_confirmed_species_list(target_enc)
                        ),
                        "species_confirmed": bool(target_enc.get("species_confirmed")),
                    }
                    if current_state != new_encounter_state:
                        record_species_confirm_cache(
                            db, species=species, target_enc=target_enc,
                            burst_index=None,
                            submitted_photo_ids=photo_ids,
                            new_encounter_state=new_encounter_state,
                            description=(
                                cache_description + f"{len(photo_ids)} photos"
                            ),
                        )

            # Apply the pipeline-cache mutation and persist inside the same
            # transaction so a failed ``save_results_raw`` rolls back the
            # species/keyword edits that would otherwise leave undo pointing
            # at a change that never landed on disk. ``burst_index`` was
            # validated above, so the branch here is unambiguous: burst-
            # scoped requests only touch the burst override, encounter-
            # scoped requests only touch the encounter.
            if cached and target_enc is not None:
                if burst_index is not None:
                    target_enc["bursts"][burst_index]["species_override"] = (
                        new_burst_override
                    )
                    if will_auto_detach:
                        auto_detach_burst_for_species(
                            cached, target_enc_idx, burst_index,
                            new_species_list[0],
                        )
                        change = {
                            "before": before_cached["encounters"],
                            "after": cached["encounters"],
                        }
                        if photo_edit_id is None:
                            from services.grouping_history import record_grouping_edit
                            record_grouping_edit(
                                db, cache_description + "1 burst", change, [],
                            )
                else:
                    set_encounter_confirmed_species(target_enc, new_species_list)
                    target_enc["species_confirmed"] = bool(
                        new_encounter_state["species_confirmed"]
                    )
                    # Child bursts may carry overrides serialize_results (or
                    # an earlier burst edit) materialized for the pre-edit
                    # species. Both review pages read those before the
                    # encounter, so leaving them would show the old species
                    # on every burst and let a flag-only Rapid apply re-tag
                    # it. Rebuild each fully-submitted burst's override from
                    # what its frames carry now (read inside this
                    # transaction); bursts without an override keep
                    # inheriting the encounter's new list.
                    from pipeline import derive_burst_override
                    submitted = set(photo_ids)
                    for child in target_enc.get("bursts") or []:
                        if child.get("species_override") is None:
                            continue
                        child_ids = child.get("photo_ids") or []
                        if not child_ids or not set(child_ids) <= submitted:
                            continue
                        child["species_override"] = derive_burst_override(
                            [
                                {"confirmed_species_list": actual_by_photo.get(pid, [])}
                                for pid in child_ids
                            ],
                            preferred_order=new_species_list,
                        )
                if photo_edit_id is not None and before_cached["encounters"] != cached["encounters"]:
                    # Labels and confirmation counts are part of the same user
                    # action even when no burst moves to another encounter.
                    photo_edit = db.conn.execute(
                        "SELECT action_type, new_value FROM edit_history WHERE id = ?",
                        (photo_edit_id,),
                    ).fetchone()
                    change = {
                        "before": before_cached["encounters"],
                        "after": cached["encounters"],
                        "photo_edit": dict(photo_edit),
                    }
                    from services.grouping_history import convert_to_grouping_edit
                    convert_to_grouping_edit(db, photo_edit_id, change)
                save_results_raw(cached, cache_dir, db._active_workspace_id)
                cache_saved = True

            db.conn.commit()
        except Exception:
            db.conn.rollback()
            if cache_saved:
                try:
                    save_results_raw(before_cached, cache_dir, db._active_workspace_id)
                except Exception:
                    log.exception("Failed to restore pipeline cache after species confirmation failed")
            raise
        # Prune oldest edit-history rows now that the transaction has landed.
        db._prune_edit_history()

        # Report `replaced` consistent with the actual replacement decision
        # (is_replacement, which uses keyword_match_key to match SQLite's
        # ASCII-only NOCASE). Python's `.lower()` folds non-ASCII pairs like
        # `Éclair`/`éclair` — which SQLite/add_keyword keep as distinct
        # keyword rows — so a `.lower()` comparison here would report
        # replaced=None on a request that actually untagged the previous
        # species row and tagged a new one.
        replaced = (
            previous_species if is_replacement and not remove_mode else None
        )
        response = {
            "ok": True,
            "species": species,
            "keyword_id": kid,
            "photo_count": len(photo_ids),
            "previous_species": replaced,
            "mode": "remove" if remove_mode else "add" if add_mode else "replace",
            "species_list": new_species_list,
            "low_confidence_photo_ids": low_confidence_photo_ids,
            # Kept for older clients/tests that checked this field; explicit
            # confirmations no longer skip submitted photos.
            "skipped_photo_ids": [],
        }
        if cached:
            response["encounters"] = cached.get("encounters", [])
            response["summary"] = cached.get("summary", {})
        return jsonify(response)

    return blueprint
