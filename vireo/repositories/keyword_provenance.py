"""Persistence for keyword provenance: the association writers and their flows.

This module is the single home, behind the ``Database`` façade, of every
``photo_keywords`` writer that creates or converges an association, plus the
flows that call those writers in the middle of their own transaction:

- ``tag``: the one association writer (``Database.tag_photo``), whose
  upsert folds the incoming stamp against the existing row;
- ``merge_keyword_into``: a keyword merge, which repoints associations onto
  the survivor and folds provenance where a photo carried both keywords;
- ``link_keyword_to_place``: attaching a Google place to a keyword, which
  may repoint its associations onto the canonical place row;
- ``retire_builtin_wildlife_genre``: the one-shot retirement pass, which
  latches authorship onto ``photo_keywords.source`` before it deletes
  anything;
- the flows that call a writer mid-flight: ``upsert_one_keyword`` (merges
  through ``Database._merge_keyword_into``) and ``accept_prediction`` (tags through
  ``Database.tag_photo``, then queues the sidecar change).

``test_keyword_provenance_contract`` keys the convergence points to this
module by their method names here (``tag``, ``merge_keyword_into``,
``link_keyword_to_place``, ``retire_builtin_wildlife_genre``); the only
writer outside it is the RAW/JPEG companion pairing in ``scanner.py``.
``Database._apply_winner_loser_merge`` and ``set_photo_location`` stay on
``Database`` and re-tag through ``Database.tag_photo``.

The method bodies were moved verbatim from ``Database``. The only edits are
the method names above (non-underscore, and ``tag_photo`` -> ``tag`` so the
façade's forwarding call is not itself a ``.tag_photo(...)`` call site the
contract would have to allowlist); ``tag`` takes ``source`` keyword-only and
without a default, because its default (``KEYWORD_SOURCE_MANUAL``) is a
``db`` constant that can't be written in the def line here -- the façade
keeps that fail-safe default and always passes it; ``self._ws_id()`` ->
``self.workspace_id``; ``NAME`` -> ``self.NAME`` for the ``db`` module
helpers and constants listed in ``__init__`` (the provenance fold among
them, which stays defined once in ``db.py``); and the two places a body
handed the ``Database`` itself to a helper
(``keywords_claim_different_taxa(self, ...)`` in ``merge_keyword_into``,
``SpeciesResolver(db=self)`` in ``accept_prediction``) now pass ``self.db``,
the façade, since ``self`` is the repository here.
``retire_builtin_wildlife_genre`` still reads ``self._active_workspace_id``,
which here is a property over the façade's attribute, read at the same
point. The SQL text, parameter order, chunk sizes and commit placement are
unchanged.

What deliberately stays on ``Database``:

- Every public and private method name, as a one-line wrapper with the
  identical signature -- ``tag_photo`` keeps ``source=KEYWORD_SOURCE_MANUAL``
  (pinned by ``test_tag_photo_defaults_to_manual_provenance``), and
  ``accept_prediction`` keeps its name for ``test_route_contract``.
- The provenance lattice (``KEYWORD_SOURCE_*``, ``keyword_source_max*``,
  ``KEYWORD_SOURCE_CONFLICT_SQL``): public ``db`` API, injected here.
- The active-workspace state. ``workspace_id`` is resolved lazily through
  ``Database._ws_id`` at exactly the point ``accept_prediction`` always
  called it.
- Composition. Every façade method a moved body calls is bound from the
  ``Database`` instance under its own name (see ``FACADE_METHODS``) and
  called as ``self.<name>(...)``, so monkeypatches of ``Database`` methods
  keep reaching the moved code. That includes the calls these methods make
  to each other: ``merge_keyword_into`` recurses, and
  ``upsert_one_keyword`` merges, through ``self._merge_keyword_into``, and ``accept_prediction`` tags through
  ``self.tag_photo``.

``_commit`` flags are carried through unchanged: ``_commit=False`` means the
caller owns the transaction, and no method here commits unless the
``Database`` method it backs did.
"""

import json
import logging
import os
import sqlite3

from keyword_identity import free_sibling_name, keywords_claim_different_taxa
from keyword_normalization import keyword_match_key, normalize_keyword_display

log = logging.getLogger(__name__)

# ``Database`` methods the moved bodies call through the façade.
FACADE_METHODS = (
    # The writers and flows here, reached from one another.
    "tag_photo",
    "_merge_keyword_into",
    # retire_builtin_wildlife_genre
    "get_meta",
    "set_meta",
    "queue_change",
    # upsert_one_keyword / link_keyword_to_place
    "_restore_misclassified_location_ancestor",
    "_upsert_location_parent_chain",
    # merge_keyword_into
    "_edit_prediction_ids",
    "_reparent_disambiguated",
    "rename_photo_preferences_species",
    "rename_species_highlights_species",
    # accept_prediction
    "add_keyword",
    "get_effective_config",
    "get_photos_with_equivalent_species",
    "update_prediction_status",
    "remove_pending_changes",
    "_species_root_name_for_taxon",
)


# -- merge_keyword_into steps -------------------------------------------------
#
# The steps ``KeywordProvenanceRepository.merge_keyword_into`` runs, in order.
# They are module functions rather than repository methods because repository
# methods may not reach one another through ``self``
# (``test_moved_writers_reach_each_other_only_through_the_facade``), so each
# step takes the connection, and any façade method it calls, as arguments.
# None of them commits: the merge's caller does. The ``photo_keywords``
# provenance fold and association move stay inline in ``merge_keyword_into``,
# the convergence point ``test_keyword_provenance_contract`` keys by name.

# ``edit_history`` action types whose entries can name a keyword id: the
# parent ``new_value`` rewrite and the JSON ``old_value`` payload pass both
# scope to these.
_MERGE_KEYWORD_ID_ACTIONS = (
    'keyword_add', 'keyword_remove', 'prediction_accept',
    'species_replace',
)


def _read_merge_rows(conn, src_id, dst_id):
    """Return the ``(src, dst)`` keyword rows the merge reads; either may be None."""
    src = conn.execute(
        "SELECT name, type, is_species, latitude, longitude, taxon_id, "
        "source_taxon_id, place_id FROM keywords WHERE id = ?",
        (src_id,),
    ).fetchone()
    dst = conn.execute(
        "SELECT name, type, is_species, place_id FROM keywords WHERE id = ?",
        (dst_id,),
    ).fetchone()
    return src, dst


def _transfer_place_id(conn, src, dst, src_id, dst_id):
    """Move the source's Google ``place_id`` onto a destination that lacks one.

    Runs before the source row is deleted. Without this, merging a coordless
    sibling on top of a place-bearing sibling (or the reverse) silently drops
    the Google place link — repeat startup repair of duplicate location roots
    would otherwise strip the place ID from a repaired child (e.g. ``United
    States -> California`` present as both a coordless branch and a
    place-bearing branch).

    When the transfer happens, the destination's coordinates are forced to
    match the source's — the metadata fold (``_fold_keyword_metadata``) only
    fills coordless rows via ``COALESCE(latitude, ?)``, so a destination that
    carried unrelated stale coords would otherwise represent the incoming
    Google place at the wrong point (saved-suggestion ranking and map markers
    then use the stale location instead of the place's actual coordinates).
    """
    place_id_transferred = (
        src is not None
        and dst is not None
        and src["place_id"] is not None
        and dst["place_id"] is None
    )
    if place_id_transferred:
        # The partial ``UNIQUE(place_id) WHERE place_id IS NOT NULL`` index
        # requires clearing the source first before moving the value.
        conn.execute(
            "UPDATE keywords SET place_id = NULL WHERE id = ?",
            (src_id,),
        )
        conn.execute(
            "UPDATE keywords SET place_id = ?, latitude = ?, longitude = ? "
            "WHERE id = ?",
            (src["place_id"], src["latitude"], src["longitude"], dst_id),
        )


def _fold_keyword_metadata(conn, src, dst, dst_id):
    """Fold the source's non-link metadata into the destination.

    ``is_species``, coordinates, ``taxon_id`` and ``source_taxon_id`` fill in
    where the destination lacks its own, so deleting the source can't silently
    drop species/location info that only the duplicate carried. The one
    exception is a species-bearing source retyped into a non-taxonomy
    destination, which clears every species claim instead (see below).
    No-op when the source row is missing.
    """
    if src is None:
        return
    # A species-bearing row being RETYPED into a non-taxonomy
    # destination must not leak its species flag or taxon link
    # onto the survivor: species queries `is_species = 1 OR
    # type = 'taxonomy'` would otherwise keep matching every
    # photo already tagged with that individual/general row
    # (see update_keyword's retype-into-peer path, and the
    # migration's general→specific-type fold). "Species-
    # bearing" is `type='taxonomy'` OR `is_species=1` — legacy
    # rows can still be `type='general', is_species=1` on
    # upgraded DBs, and retyping them into an individual/general
    # peer would otherwise take the else branch below and stamp
    # is_species=1 onto the non-taxonomy destination. Gated on
    # `src.type != dst.type` so same-type case-variant collapses
    # (e.g. two `general, is_species=1` rows merging under one
    # normalized spelling) still keep their metadata-fold
    # behavior.
    leaks_species_into_nontaxonomy = (
        dst is not None
        and dst["type"] != "taxonomy"
        and src["type"] != dst["type"]
        and (src["type"] == "taxonomy" or src["is_species"] == 1)
    )
    if leaks_species_into_nontaxonomy:
        # Retype-into-peer path (see update_keyword): the survivor
        # is deliberately non-taxonomy, so the row must not stay
        # matched by species queries. Suppressing the source's
        # is_species/taxon_id is not enough — the destination may
        # carry a legacy is_species=1 (dirty pre-invariant data on
        # 'individual'/'general' rows) or a stale taxon_id, and
        # keeping either lets `is_species = 1 OR type = 'taxonomy'`
        # keep matching every photo that already used the dst row.
        # Clear all species claims (taxon_id AND source_taxon_id)
        # alongside the metadata fold; a lingering iNat
        # source_taxon_id would keep the survivor resolving to a
        # species identity the retype was meant to drop.
        conn.execute(
            """UPDATE keywords
               SET is_species        = 0,
                   latitude          = COALESCE(latitude, ?),
                   longitude         = COALESCE(longitude, ?),
                   taxon_id          = NULL,
                   source_taxon_id   = NULL
               WHERE id = ?""",
            (src["latitude"], src["longitude"], dst_id),
        )
    else:
        # Fold ``source_taxon_id`` alongside ``taxon_id``: a
        # source row can carry an iNat id without a resolved local
        # taxon (see ``_add_source_species_keyword``), and
        # ``keywords_claim_different_taxa`` treats a bare
        # ``source_taxon_id`` as identity. Without this COALESCE
        # the recursive child collapse would drop the only
        # external taxon claim and leave the survivor an unlinked
        # species row.
        conn.execute(
            """UPDATE keywords
               SET is_species        = CASE WHEN ? = 1 THEN 1 ELSE is_species END,
                   latitude          = COALESCE(latitude, ?),
                   longitude         = COALESCE(longitude, ?),
                   taxon_id          = COALESCE(taxon_id, ?),
                   source_taxon_id   = COALESCE(source_taxon_id, ?)
               WHERE id = ?""",
            (src["is_species"], src["latitude"], src["longitude"],
             src["taxon_id"], src["source_taxon_id"], dst_id),
        )


def _retarget_source_spelling(
    conn, src, dst, src_id, dst_id, *, pending_source_only, chunks,
    rename_species_highlights_species, rename_photo_preferences_species,
):
    """Repoint rows keyed on the source's *name* onto the destination's name.

    Covers pending sidecar edits (``_retarget_pending_changes``) and species
    curation (``_retarget_species_curation``). Applies only when both rows
    exist and their names differ.
    """
    if src is not None and dst is not None:
        src_name = src["name"]
        dst_name = dst["name"]
        if src_name and dst_name and src_name != dst_name:
            _retarget_pending_changes(
                conn, src_name, dst_name, src_id, dst_id,
                pending_source_only=pending_source_only, chunks=chunks,
            )
            _retarget_species_curation(
                conn, src, dst, src_name, dst_name, src_id, dst_id,
                rename_species_highlights_species=rename_species_highlights_species,
                rename_photo_preferences_species=rename_photo_preferences_species,
            )


def _retarget_pending_changes(
    conn, src_name, dst_name, src_id, dst_id, *, pending_source_only, chunks,
):
    """Rewrite unsynced keyword_add/keyword_remove rows onto the survivor name.

    Without this, the merge deletes the source row but leaves the pending
    change referring to the old spelling, so the next ``sync_to_xmp`` writes
    a keyword the DB no longer has.

    A pending row that would collide with an existing (photo_id, change_type,
    dst_name) row is dropped rather than duplicated — matches the dedupe
    contract queue_change enforces. The rewrite is scoped to photos actually
    tagged with either row: a value-only rewrite would otherwise affect every
    workspace whose pending_changes carry the same name string for a keyword
    row that was not merged. Captured before the photo_keywords UPDATE so the
    query still sees the src tags.

    Explicit merges set ``pending_source_only`` so only photos currently
    carrying the source have their pending edits rewritten. A photo that
    already removed the source must still remove that old spelling from its
    sidecar, even when it also carries the destination keyword.
    """
    affected_pcx = [
        r["photo_id"] for r in conn.execute(
            "SELECT DISTINCT photo_id FROM photo_keywords WHERE keyword_id IN (?, ?)",
            (src_id, src_id if pending_source_only else dst_id),
        ).fetchall()
    ]
    for chunk in chunks(affected_pcx):
        placeholders = ",".join("?" for _ in chunk)
        conn.execute(
            f"""DELETE FROM pending_changes
                WHERE change_type IN ('keyword_add', 'keyword_remove')
                  AND value = ?
                  AND photo_id IN ({placeholders})
                  AND EXISTS (
                      SELECT 1 FROM pending_changes pc2
                      WHERE pc2.photo_id = pending_changes.photo_id
                        AND pc2.change_type = pending_changes.change_type
                        AND pc2.value = ?
                        AND COALESCE(pc2.workspace_id, -1)
                            = COALESCE(pending_changes.workspace_id, -1)
                  )""",
            [src_name, *chunk, dst_name],
        )
        conn.execute(
            f"""UPDATE pending_changes
                SET value = ?
                WHERE change_type IN ('keyword_add', 'keyword_remove')
                  AND value = ?
                  AND photo_id IN ({placeholders})""",
            [dst_name, src_name, *chunk],
        )


def _retarget_species_curation(
    conn, src, dst, src_name, dst_name, src_id, dst_id, *,
    rename_species_highlights_species, rename_photo_preferences_species,
):
    """Rename species curation rows keyed to the source name onto the survivor.

    Applies when either row is a species/taxonomy keyword. The eligible
    curation queries compare those strings exact against the surviving
    keywords.name, so highlights/representatives keyed to the source spelling
    would silently disappear after a merge even though the tag itself was
    retained. Mirrors the scoped rename _normalize_keyword_row_name runs on
    the survivor; scoped to (photo, workspace) pairs that carried either row
    so an unrelated workspace's same-species curation is not retargeted onto
    a name it doesn't have tagged.
    """
    is_species_merge = (
        src["is_species"] == 1 or src["type"] == "taxonomy"
        or dst["is_species"] == 1 or dst["type"] == "taxonomy"
    )
    if is_species_merge:
        tag_rows = conn.execute(
            """SELECT DISTINCT pk.photo_id, wf.workspace_id
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE pk.keyword_id IN (?, ?)""",
            (src_id, dst_id),
        ).fetchall()
        photo_workspace_pairs = [
            (r["photo_id"], r["workspace_id"]) for r in tag_rows
        ]
        if photo_workspace_pairs:
            rename_species_highlights_species(
                src_name, dst_name,
                photo_workspace_pairs=photo_workspace_pairs,
                _commit=False,
            )
            rename_photo_preferences_species(
                src_name, dst_name,
                photo_workspace_pairs=photo_workspace_pairs,
                _commit=False,
            )


def _retarget_edit_history(conn, src_id, dst_id, *, chunks, edit_prediction_ids):
    """Retarget edit_history entries that name ``src_id`` onto ``dst_id``.

    Undo/redo then lands on the survivor instead of a deleted row. Without
    this, undo of a recent keyword_add / keyword_remove / prediction_accept /
    species_replace looks up src_id, gets no keyword row (it's about to be
    deleted), and marks the entry undone without reversing the effect: the
    tag stays on the photo and the pending sidecar change (already rewritten
    to the survivor spelling by ``_retarget_pending_changes``) is left in
    place. Applies globally across workspaces — workspace_id scopes WHO ran
    the edit, not which keyword row it references.

    First drops (or, for prediction accepts, retires to status-only) the items
    on photos that already carried the survivor, which cannot be retargeted
    honestly (``_drop_unretargetable_history_items``); then rewrites the bare
    ids (``_retarget_bare_history_ids``) and the JSON payloads
    (``_retarget_history_json_payloads``). The survivor's pre-existing photos
    are read here, before the photo_keywords move adds the source's photos
    to them.
    """
    src_str = str(src_id)
    dst_str = str(dst_id)
    preexisting_dst_photos = [
        r["photo_id"] for r in conn.execute(
            "SELECT photo_id FROM photo_keywords WHERE keyword_id = ?",
            (dst_id,),
        ).fetchall()
    ]
    _drop_unretargetable_history_items(
        conn, src_str, dst_str, preexisting_dst_photos,
        chunks=chunks, edit_prediction_ids=edit_prediction_ids,
    )
    _retarget_bare_history_ids(conn, src_str, dst_str)
    _retarget_history_json_payloads(conn, src_id, dst_id, preexisting_dst_photos)


def _retire_tag_mutations(conn, rows, edit_prediction_ids):
    """Delete each history item, or retire a prediction accept to status-only.

    A prediction accept has two effects: the tag and review status. When a
    merge makes its tag redundant, retain the status effect and metadata so
    undo/redo still restores every prediction.
    """
    for row in rows:
        if row["action_type"] != "prediction_accept":
            conn.execute("DELETE FROM edit_history_items WHERE id = ?", (row["id"],))
            continue
        try:
            meta = json.loads(row["old_value"] or "{}")
        except (TypeError, ValueError):
            meta = {}
        if not isinstance(meta, dict):
            meta = {}
        meta["prediction_ids"] = edit_prediction_ids(meta, row["old_value"])
        meta["no_tag"] = True
        conn.execute(
            "UPDATE edit_history_items SET old_value = ? WHERE id = ?",
            (json.dumps(meta), row["id"]),
        )


def _drop_unretargetable_history_items(
    conn, src_str, dst_str, preexisting_dst_photos, *, chunks, edit_prediction_ids,
):
    """Drop history items a src→dst retarget would make lie about the survivor.

    Pre-existing survivor tags: for an edit recorded against src_id, an item
    whose photo already carried dst_id at merge time can't be retargeted
    honestly — the UPDATE OR IGNORE on photo_keywords leaves the survivor row
    untouched and drops the src row, so an undo/redo of the retargeted entry
    would touch the user's pre-existing survivor tag that was never part of
    that edit. Drop those items before retargeting so undo/redo iterates 0 (or
    the still-legitimate) items only. Covers these action types:

      * `keyword_add`: undo calls untag_photo(pid, entry.new_value) per item;
        the retargeted entry.new_value = dst_id would remove the survivor.
      * `prediction_accept`: undo uses item.new_value for the tag. Retire
        only that tag mutation by converting the item to ``no_tag``; its
        prediction-status history remains undoable.
      * `keyword_remove`: undo tags on the survivor (INSERT OR IGNORE — no-op
        if dst pre-existed), BUT redo calls untag_photo(pid, entry.new_value);
        the retargeted entry.new_value = dst_id would strip the survivor on
        redo.
      * `species_replace`: undo calls untag_photo(pid, item.new_value) before
        restoring the old species (see `_apply_undo`); the retargeted
        item.new_value = dst_id would remove the survivor tag the edit never
        actually created. Redo similarly untags item.new_value again.
        Symmetric case on the OLD side: for a prior replace where src was the
        OLD species being swapped out, redo iterates old_kids (bare-string or
        JSON `keyword_ids`) and untags each — a src→dst retarget of those
        references would strip the pre-existing survivor. Drop those items
        too (bare-string in ``_drop_replaced_old_species_items``, JSON in
        ``_retarget_history_json_payloads``).

    Each chunk of pre-existing survivor photos runs the four passes in order.
    """
    for chunk in chunks(preexisting_dst_photos):
        ph = ",".join("?" for _ in chunk)
        _retire_src_adds_on_preexisting_survivor(
            conn, ph, chunk, src_str, dst_str, edit_prediction_ids,
        )
        _retire_later_redundant_adds(
            conn, ph, chunk, src_str, dst_str, edit_prediction_ids,
        )
        _drop_removes_of_preexisting_survivor(conn, ph, chunk, src_str, dst_str)
        _drop_replaced_old_species_items(conn, ph, chunk, src_str)


def _retire_src_adds_on_preexisting_survivor(
    conn, ph, chunk, src_str, dst_str, edit_prediction_ids,
):
    """Retire src-tagging items on photos that already carried the survivor.

    keyword_add + prediction_accept + species_replace: item.new_value =
    str(kid). Deleting a species_replace item here loses the
    retag-old-species side of that per-photo swap on undo/redo, but leaving
    it retargeted would silently untag the user's pre-existing survivor.
    Prediction accepts instead keep a status-only record.

    Identity is per item: for a mixed-alias prediction_accept batch (see
    api_accept_predictions), the parent edit's ``new_value`` records only the
    first alias, while each item's ``new_value`` records its own resolved
    keyword id. Requiring the parent to also equal ``src`` would miss items
    in that batch whose alias is the one being merged, and the survivor
    retarget would then silently untag a pre-existing ``dst`` tag on undo.
    For ``keyword_add`` and ``species_replace`` the parent and item always
    agree, so dropping the parent match only widens coverage where it was
    under-matching before.

    Status-only accepts must retain their prediction undo record, and do not
    count as earlier/later tag additions in these checks.
    """
    _retire_tag_mutations(conn, conn.execute(
        f"""SELECT id, old_value,
                   (SELECT action_type FROM edit_history
                    WHERE id = edit_history_items.edit_id) AS action_type
            FROM edit_history_items
            WHERE new_value = ?
              AND photo_id IN ({ph})
              AND edit_id IN (
                  SELECT id FROM edit_history
                  WHERE action_type IN (
                      'keyword_add', 'species_replace'
                  ) OR (
                      action_type = 'prediction_accept'
                      AND COALESCE(edit_history_items.old_value, '') NOT LIKE '%"no_tag"%'
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM edit_history_items ehi2
                  JOIN edit_history eh2
                    ON eh2.id = ehi2.edit_id
                  WHERE ehi2.photo_id = edit_history_items.photo_id
                    AND ehi2.new_value IN (?, ?)
                    AND eh2.action_type IN (
                        'keyword_add',
                        'prediction_accept',
                        'species_replace'
                    )
                    AND (eh2.action_type != 'prediction_accept'
                         OR COALESCE(ehi2.old_value, '') NOT LIKE '%"no_tag"%')
                    AND ehi2.id > edit_history_items.id
              )""",
        [src_str, *chunk, src_str, dst_str],
    ).fetchall(), edit_prediction_ids)


def _retire_later_redundant_adds(
    conn, ph, chunk, src_str, dst_str, edit_prediction_ids,
):
    """Retire a later add made redundant by an earlier source add.

    When the source add happened first and a later add created the current
    survivor association, the later add becomes the redundant operation after
    src and dst converge. The guarded cleanup in
    ``_retire_src_adds_on_preexisting_survivor`` deliberately preserves the
    earlier source item; retire the later tag mutation instead so
    latest-first undo leaves the merged tag in place until the original
    source add is itself undone. Restrict this to add-like actions whose
    whole per-photo effect is the tag association; species_replace has an
    old-species restoration side that cannot be discarded.

    The earlier-source lookup matches on the item's own ``new_value`` alone,
    not the parent edit's, so a mixed-alias prediction_accept batch (whose
    parent records only the first alias) still counts as the earlier source
    add for a later redundant item.
    """
    _retire_tag_mutations(conn, conn.execute(
        f"""SELECT id, old_value,
                   (SELECT action_type FROM edit_history
                    WHERE id = edit_history_items.edit_id) AS action_type
            FROM edit_history_items
            WHERE photo_id IN ({ph})
              AND new_value IN (?, ?)
              AND edit_id IN (
                  SELECT id FROM edit_history
                  WHERE action_type = 'keyword_add' OR (
                      action_type = 'prediction_accept'
                      AND COALESCE(edit_history_items.old_value, '') NOT LIKE '%"no_tag"%'
                  )
              )
              AND EXISTS (
                  SELECT 1
                  FROM edit_history_items ehi1
                  JOIN edit_history eh1
                    ON eh1.id = ehi1.edit_id
                  WHERE ehi1.photo_id = edit_history_items.photo_id
                    AND ehi1.new_value = ?
                    AND eh1.action_type IN (
                        'keyword_add', 'prediction_accept'
                    )
                    AND (eh1.action_type != 'prediction_accept'
                         OR COALESCE(ehi1.old_value, '') NOT LIKE '%"no_tag"%')
                    AND ehi1.id < edit_history_items.id
              )""",
        [*chunk, src_str, dst_str, src_str],
    ).fetchall(), edit_prediction_ids)


def _drop_removes_of_preexisting_survivor(conn, ph, chunk, src_str, dst_str):
    """Drop src keyword_remove items whose photo genuinely held the survivor.

    keyword_remove: item.new_value is '' by convention (see record_edit call
    sites in app.py); the keyword id lives in item.old_value. Drop the item
    ONLY when the survivor genuinely pre-existed THIS remove — i.e., no later
    edit added the merged keyword back to the same photo. If dst was tagged
    AFTER this remove, the current photo_keywords row does not prove
    pre-existence and dropping the item breaks undo: latest-first undo of the
    later add first strips dst_id, and this remove's undo would then no-op
    (no item), leaving the merged keyword missing when the earlier remove is
    reversed. Keeping the item is safe in that case:

      * undo of remove → tag_photo(pid, dst) is INSERT OR IGNORE and a no-op
        if dst is already present;
      * redo of remove → untag_photo(pid, dst) is consistent with replaying
        the historical remove of what became the merged keyword.

    "Later add" covers keyword_add / prediction_accept and the tagging half
    of species_replace (item.new_value = str(kid)). Src-spelled adds count
    too — pre-migration they refer to what will become the merged keyword.
    """
    conn.execute(
        f"""DELETE FROM edit_history_items
            WHERE old_value = ?
              AND photo_id IN ({ph})
              AND edit_id IN (
                  SELECT id FROM edit_history
                  WHERE new_value = ?
                    AND action_type = 'keyword_remove'
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM edit_history_items ehi2
                  JOIN edit_history eh2
                    ON eh2.id = ehi2.edit_id
                  WHERE ehi2.photo_id = edit_history_items.photo_id
                    AND ehi2.new_value IN (?, ?)
                    AND eh2.action_type IN (
                        'keyword_add',
                        'prediction_accept',
                        'species_replace'
                    )
                    AND (eh2.action_type != 'prediction_accept'
                         OR COALESCE(ehi2.old_value, '') NOT LIKE '%"no_tag"%')
                    AND ehi2.id > edit_history_items.id
              )""",
        [src_str, *chunk, src_str, src_str, dst_str],
    )


def _drop_replaced_old_species_items(conn, ph, chunk, src_str):
    """Drop bare-string species_replace items that swapped src out.

    species_replace: item.old_value = str(old_kid) (bare-string form) for a
    prior replace where src_id was the OLD species being swapped out. The
    bare-string retarget (``_retarget_bare_history_ids``) would rewrite that
    to dst_str; _apply_redo then iterates old_kids=[dst_id] and
    untag_photo(pid, dst_id), stripping the survivor tag that pre-existed the
    merge and was never created by that edit. Drop the item — same tradeoff
    as the new_value / species_replace case in
    ``_retire_src_adds_on_preexisting_survivor``.
    """
    conn.execute(
        f"""DELETE FROM edit_history_items
            WHERE old_value = ?
              AND photo_id IN ({ph})
              AND edit_id IN (
                  SELECT id FROM edit_history
                  WHERE action_type = 'species_replace'
              )""",
        [src_str, *chunk],
    )


def _retarget_bare_history_ids(conn, src_str, dst_str):
    """Rewrite bare keyword-id strings in edit_history and its items."""
    _kw_placeholders = ",".join("?" * len(_MERGE_KEYWORD_ID_ACTIONS))
    # 1) edit_history.new_value: the canonical keyword id per entry.
    conn.execute(
        f"""UPDATE edit_history
            SET new_value = ?
            WHERE new_value = ?
              AND action_type IN ({_kw_placeholders})""",
        (dst_str, src_str, *_MERGE_KEYWORD_ID_ACTIONS),
    )
    # 2) edit_history_items new_value / old_value: bare keyword-id
    #    strings, but only the specific (action_type, column) pairs
    #    that actually store keyword ids. record_edit populates:
    #      keyword_add       → new_value=str(kid), old_value=''
    #      keyword_remove    → old_value=str(kid), new_value=''
    #      species_replace   → old_value=str(old_kid), new_value=str(kid)
    #      prediction_accept → old_value=str(prediction_id),
    #                          new_value=str(kid)
    #    prediction_accept.old_value is the prediction id, NOT a
    #    keyword id (see api_accept_prediction and _edit_prediction_id
    #    which falls back to the bare string). A blanket rewrite over
    #    every column would corrupt any prediction id whose numeric
    #    value happens to equal src_id — undo/redo would then act on
    #    the wrong prediction. Restrict each rewrite to the action
    #    types whose column contains a keyword id.
    _kw_id_by_col = {
        "new_value": (
            "keyword_add", "species_replace", "prediction_accept",
        ),
        "old_value": ("keyword_remove", "species_replace"),
    }
    for col, actions in _kw_id_by_col.items():
        col_placeholders = ",".join("?" * len(actions))
        conn.execute(
            f"""UPDATE edit_history_items
                SET {col} = ?
                WHERE {col} = ?
                  AND edit_id IN (
                      SELECT id FROM edit_history
                      WHERE action_type IN ({col_placeholders})
                  )""",
            (dst_str, src_str, *actions),
        )


def _retarget_history_json_payloads(conn, src_id, dst_id, preexisting_dst_photos):
    """Rewrite ``keyword_id``/``keyword_ids`` in JSON ``old_value`` payloads.

    species_replace and metadata-carrying keyword_add/prediction_accept
    entries store {"keyword_id": ..., "keyword_ids": [...], ...}. Load,
    rewrite, re-serialize per row. Scoped to values that look like JSON so
    bare id strings (already handled by ``_retarget_bare_history_ids``) are
    skipped cheaply. Uses ? for the LIKE prefix to keep the format string
    free of literal SQL wildcard characters.

    For species_replace items whose photo already carried the survivor before
    the merge, a src→dst rewrite of the JSON old_kids would make _apply_redo
    untag the pre-existing survivor (see ``_drop_replaced_old_species_items``);
    drop those items instead of retargeting them.
    """
    _kw_placeholders = ",".join("?" * len(_MERGE_KEYWORD_ID_ACTIONS))
    preexisting_set = set(preexisting_dst_photos)
    json_rows = conn.execute(
        f"""SELECT ehi.id, ehi.photo_id, ehi.old_value, eh.action_type
            FROM edit_history_items ehi
            JOIN edit_history eh ON eh.id = ehi.edit_id
            WHERE eh.action_type IN ({_kw_placeholders})
              AND ehi.old_value IS NOT NULL
              AND ehi.old_value LIKE ?""",
        (*_MERGE_KEYWORD_ID_ACTIONS, '{%'),
    ).fetchall()
    for row in json_rows:
        try:
            data = json.loads(row["old_value"])
        except (TypeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        if (
            _payload_references_keyword(data, src_id)
            and row["action_type"] == "species_replace"
            and row["photo_id"] in preexisting_set
        ):
            conn.execute(
                "DELETE FROM edit_history_items WHERE id = ?",
                (row["id"],),
            )
            continue
        if _rewrite_payload_keyword_ids(data, src_id, dst_id):
            conn.execute(
                "UPDATE edit_history_items SET old_value = ? WHERE id = ?",
                (json.dumps(data, sort_keys=True), row["id"]),
            )


def _payload_references_keyword(data, keyword_id):
    """True if a history payload's ``keyword_id`` or ``keyword_ids`` names it."""
    references_src = False
    raw_kid = data.get("keyword_id")
    if raw_kid is not None:
        try:
            if int(raw_kid) == keyword_id:
                references_src = True
        except (TypeError, ValueError):
            pass
    if not references_src:
        for k in (data.get("keyword_ids") or []):
            try:
                if int(k) == keyword_id:
                    references_src = True
                    break
            except (TypeError, ValueError):
                continue
    return references_src


def _rewrite_payload_keyword_ids(data, src_id, dst_id):
    """Repoint ``src_id`` to ``dst_id`` in a payload, in place; True if changed."""
    dirty = False
    raw_kid = data.get("keyword_id")
    if raw_kid is not None:
        try:
            if int(raw_kid) == src_id:
                data["keyword_id"] = dst_id
                dirty = True
        except (TypeError, ValueError):
            pass
    raw_kids = data.get("keyword_ids")
    if isinstance(raw_kids, list) and raw_kids:
        rewritten = []
        changed = False
        for k in raw_kids:
            try:
                k_int = int(k)
            except (TypeError, ValueError):
                rewritten.append(k)
                continue
            if k_int == src_id:
                k_int = dst_id
                changed = True
            rewritten.append(k_int)
        if changed:
            # Dedup preserving order: if the destination id was
            # already in the list, don't repeat it after rewrite.
            seen = []
            for k in rewritten:
                if k not in seen:
                    seen.append(k)
            data["keyword_ids"] = seen
            dirty = True
    return dirty


def _reparent_children(
    conn, src_id, dst_id, *, pending_source_only, db,
    merge_keyword_into, reparent_disambiguated,
):
    """Move the source's children under the destination before it is deleted.

    Without this the keywords.parent_id FK aborts the merge mid-way. Each
    child is placed by ``_reparent_child``. Cycles are impossible: parent_id
    chains are acyclic by construction.

    Returns the number of keyword rows the recursive child merges folded away
    (0 when none collided into a same-typed sibling).
    """
    merged = 0
    children = conn.execute(
        "SELECT id, name, type, place_id, taxon_id, source_taxon_id, is_species "
        "FROM keywords WHERE parent_id = ?",
        (src_id,),
    ).fetchall()
    for child in children:
        merged += _reparent_child(
            conn, child, dst_id,
            pending_source_only=pending_source_only, db=db,
            merge_keyword_into=merge_keyword_into,
            reparent_disambiguated=reparent_disambiguated,
        )
    return merged


def _reparent_child(
    conn, child, dst_id, *, pending_source_only, db,
    merge_keyword_into, reparent_disambiguated,
):
    """Reparent one child under ``dst_id``, merging or disambiguating on collision.

    A child whose name matches an existing sibling under the destination
    merges into that sibling recursively (through the façade's
    ``_merge_keyword_into``) only when both share the same ``type`` and do
    not claim different taxa or different Google places — "Birds > Heron"
    and "birds > Heron" must converge on one Heron. Match uses
    ``keyword_match_key`` (ASCII case fold on the display-normalized name),
    the same key every lookup and dedup path uses, so a case-only variant is
    a collision even though SQLite's UNIQUE(name, parent_id) index is BINARY.
    When the existing sibling has a different ``type`` (e.g. a 'general'
    Macro vs. a 'genre' Macro), the dedup boundary is (LOWER(name),
    parent_id, type), so they are NOT duplicates; both are preserved by
    disambiguating the migrating child's name with an id suffix.

    Returns the number of keyword rows a recursive merge folded away, else 0.
    """
    # Detect the collision explicitly: SQLite's UNIQUE(name, parent_id)
    # is BINARY, so `foo` reparenting under a destination that already
    # holds `Foo` would UPDATE cleanly and leave two semantic peers no
    # keyword lookup (all folded through ``keyword_match_key``) could
    # tell apart. Fold every sibling's name to check for either shape
    # of collision, and only reparent when the folded slot is free.
    siblings = conn.execute(
        "SELECT id, name, type, place_id, taxon_id, source_taxon_id, is_species "
        "FROM keywords WHERE parent_id = ? AND id != ?",
        (dst_id, child["id"]),
    ).fetchall()
    child_key = keyword_match_key(child["name"])
    existing = next(
        (s for s in siblings if keyword_match_key(s["name"]) == child_key),
        None,
    )
    if existing is None:
        conn.execute(
            "UPDATE keywords SET parent_id = ? WHERE id = ?",
            (dst_id, child["id"]),
        )
        return 0
    # Every disambiguation below has to dodge the whole sibling
    # set, not just the row it collided with: the suffixed name
    # can itself be occupied (a user typed it, or an earlier
    # disambiguation produced it), and a second
    # UNIQUE(name, parent_id) violation here is uncaught.
    taken = {row["name"] for row in siblings}
    if keywords_claim_different_taxa(db, existing, child):
        # Two same-named species rows that resolve to DIFFERENT
        # taxa. A recursive merge keeps the destination's taxon
        # claim (COALESCE folds only fill missing fields), so
        # every photo under the migrating row would silently
        # come out tagged as the other species. Same reasoning
        # as the distinct place case below; keep both rows
        # instead.
        reparent_disambiguated(
            child, dst_id, free_sibling_name(
                taken, child["name"], f"id-{child['id']}"),
        )
    elif (
        existing["type"] == "location"
        and child["type"] == "location"
        and existing["place_id"] is not None
        and child["place_id"] is not None
        and existing["place_id"] != child["place_id"]
    ):
        # Two location siblings sharing (name, parent_id) but
        # pointing at distinct Google places (e.g. two direct
        # ``United States -> Springfield`` rows created from
        # different place IDs). A recursive merge here would
        # delete the migrating row and silently retag its
        # photos onto a sibling that represents a different
        # Google place. Disambiguate the migrating child with a
        # place-id suffix so both Google places survive.
        reparent_disambiguated(
            child, dst_id, free_sibling_name(
                taken, child["name"], child["place_id"][-8:]),
        )
    elif existing["type"] == child["type"]:
        return merge_keyword_into(
            child["id"], existing["id"], pending_source_only=pending_source_only,
        )
    else:
        # Same name + parent but different type: outside the
        # (LOWER(name), parent_id, type) dedup boundary, so
        # preserve both by renaming the migrating child rather
        # than retagging photos across types.
        reparent_disambiguated(
            child, dst_id, free_sibling_name(
                taken, child["name"], f"id-{child['id']}"),
        )
    return 0


class KeywordProvenanceRepository:
    def __init__(
        self,
        conn,
        resolve_workspace_id,
        *,
        chunks,
        log,
        keyword_source_manual,
        keyword_source_conflict_sql,
        keyword_source_max_sql,
        wildlife_retirement_write_chunk_size,
        retired_wildlife_genre_key,
        decided_prediction_statuses,
        facade,
    ):
        self.conn = conn
        self._resolve_workspace_id = resolve_workspace_id
        # ``db`` module helpers and constants, kept under their module names
        # so the moved bodies read them as ``self.<name>``. ``_chunks`` is
        # ``db._chunks`` itself (its size default is bound at import),
        # ``log`` is the ``db`` logger, so log records keep its name, and the
        # provenance fold is ``db``'s own, so there is still exactly one.
        self._chunks = chunks
        self.log = log
        self.KEYWORD_SOURCE_MANUAL = keyword_source_manual
        self.KEYWORD_SOURCE_CONFLICT_SQL = keyword_source_conflict_sql
        self.keyword_source_max_sql = keyword_source_max_sql
        self._WILDLIFE_RETIREMENT_WRITE_CHUNK_SIZE = wildlife_retirement_write_chunk_size
        # ``Database`` class attributes, kept under their class names.
        self._RETIRED_WILDLIFE_GENRE_KEY = retired_wildlife_genre_key
        self.DECIDED_PREDICTION_STATUSES = decided_prediction_statuses
        # The ``Database`` itself, for the two helpers the moved bodies hand
        # it to (``keywords_claim_different_taxa`` and ``SpeciesResolver``).
        self.db = facade
        # Bound ``Database`` methods, kept under their façade names.
        for name in FACADE_METHODS:
            setattr(self, name, getattr(facade, name))

    @property
    def workspace_id(self):
        """The active workspace id, resolved at each read (raises if none)."""
        return self._resolve_workspace_id()

    @property
    def _active_workspace_id(self):
        """The façade's active workspace id (``None`` when unset), read live."""
        return self.db._active_workspace_id

    def tag(
        self, photo_id, keyword_id, *, source, _commit=True,
    ):
        """Associate a keyword with a photo.

        Args:
            source: Durable provenance for the association. The stamp lives on
                    the row, so authorship survives ``_prune_edit_history``
                    discarding the ``keyword_add`` entry.

                    Defaults to ``KEYWORD_SOURCE_MANUAL`` because that is the
                    fail-safe answer: a call site that forgets to declare
                    provenance leaves an association that retirement passes
                    refuse to delete, rather than one they silently erase.
                    Only the sidecar readers (scanner, XMP reconcile) may pass
                    ``KEYWORD_SOURCE_UNKNOWN``, and they must do it
                    explicitly — the contract test enumerates them.

                    An existing stamp is never downgraded: the upsert stores
                    the lattice max of the existing and incoming values, so
                    re-tagging with ``KEYWORD_SOURCE_UNKNOWN`` keeps a
                    recorded ``'manual'`` (and a future ``'accept'`` re-tag
                    would too).
            _commit: If False, skip the internal commit (caller is responsible
                     for committing the transaction).
        """
        self.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id, source) "
            "VALUES (?, ?, ?) " + self.KEYWORD_SOURCE_CONFLICT_SQL,
            (photo_id, keyword_id, source),
        )
        if _commit:
            self.conn.commit()

    def merge_keyword_into(self, src_id, dst_id, *, pending_source_only=False):
        """Merge keyword ``src_id`` into ``dst_id`` and delete the source.

        Repoints everything that refers to the source onto the destination,
        in this order: import aliases; the Google place link and the row's
        species/location metadata; pending sidecar edits and species curation
        keyed on the source spelling; edit history that names the source id;
        the photo associations, folding provenance where a photo carried both
        keywords; and the source's children, which merge recursively into a
        same-named, same-typed sibling and are otherwise reparented. Each
        step's helper documents the rule it enforces.

        Explicit merges set ``pending_source_only`` so only photos currently
        carrying the source have their pending edits rewritten (see
        ``_retarget_pending_changes``).

        Returns the number of keyword rows merged away (>= 1), counting
        recursive child merges. Caller commits.
        """
        merged = 1
        self.conn.execute(
            'UPDATE keyword_import_aliases SET keyword_id = ? WHERE keyword_id = ?',
            (dst_id, src_id),
        )
        src, dst = _read_merge_rows(self.conn, src_id, dst_id)
        _transfer_place_id(self.conn, src, dst, src_id, dst_id)
        _fold_keyword_metadata(self.conn, src, dst, dst_id)
        # The pending-change, curation and edit-history retargets read
        # photo_keywords as it stood before the merge, so they run before the
        # association move below.
        _retarget_source_spelling(
            self.conn, src, dst, src_id, dst_id,
            pending_source_only=pending_source_only,
            chunks=self._chunks,
            rename_species_highlights_species=self.rename_species_highlights_species,
            rename_photo_preferences_species=self.rename_photo_preferences_species,
        )
        _retarget_edit_history(
            self.conn, src_id, dst_id,
            chunks=self._chunks, edit_prediction_ids=self._edit_prediction_ids,
        )
        # Preserve durable authorship before collapsing association conflicts.
        # UPDATE OR IGNORE below leaves the destination row untouched when a
        # photo already carries both keywords; without this fold, deleting the
        # source would also delete its only ``source='manual'`` stamp. The
        # fold runs in both directions — a weaker destination is raised to the
        # source's claim, and a stronger destination keeps its own.
        src_source_sql = (
            "(SELECT src_pk.source FROM photo_keywords src_pk "
            "WHERE src_pk.photo_id = dst_pk.photo_id "
            "AND src_pk.keyword_id = :src_id)"
        )
        self.conn.execute(
            f"""UPDATE photo_keywords AS dst_pk
               SET source = {self.keyword_source_max_sql(
                   "dst_pk.source", src_source_sql,
               )}
               WHERE dst_pk.keyword_id = :dst_id
                 AND EXISTS (
                     SELECT 1 FROM photo_keywords src_pk
                     WHERE src_pk.photo_id = dst_pk.photo_id
                       AND src_pk.keyword_id = :src_id
                 )""",
            {"dst_id": dst_id, "src_id": src_id},
        )
        # Move photo associations (ignore if already exists for dst_id),
        # then drop the leftovers. Non-conflicting rows carry their source
        # column through the UPDATE unchanged.
        self.conn.execute(
            "UPDATE OR IGNORE photo_keywords SET keyword_id = ? WHERE keyword_id = ?",
            (dst_id, src_id),
        )
        self.conn.execute("DELETE FROM photo_keywords WHERE keyword_id = ?", (src_id,))
        merged += _reparent_children(
            self.conn, src_id, dst_id,
            pending_source_only=pending_source_only,
            db=self.db,
            merge_keyword_into=self._merge_keyword_into,
            reparent_disambiguated=self._reparent_disambiguated,
        )
        self.conn.execute("DELETE FROM keywords WHERE id = ?", (src_id,))
        return merged

    def link_keyword_to_place(self, keyword_id, details):
        """Attach Google place data to an existing keyword.

        ``details`` has the same shape as :meth:`upsert_place_chain`'s input.
        Builds the parent chain, then tries to UPDATE the target keyword with
        ``place_id``, coords, name, and the deepest parent's id. If another
        keyword already has the target ``place_id`` (UNIQUE collision on the
        partial index), the existing canonical row absorbs all
        ``photo_keywords`` rows from the target, and the now-empty target
        row is deleted.

        Returns ``{"keyword_id": <final id>, "merged": <bool>}``. ``merged``
        is True when an existing place-bearing row absorbed the target.
        """
        if not details.get("place_id"):
            raise ValueError("link_keyword_to_place requires details['place_id']")

        row = self.conn.execute(
            "SELECT type FROM keywords WHERE id = ?", (keyword_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"keyword id {keyword_id} does not exist")
        # Reject non-location keywords. place_id is globally unique, so
        # attaching one to (say) a species or general keyword would let later
        # location upserts resolve to a non-location row, after which
        # set_photo_location rejects it and the place is effectively unusable
        # until the row is manually cleaned up.
        if row["type"] != "location":
            raise ValueError(
                f"keyword id {keyword_id} is type '{row['type']}', not 'location'"
            )

        place_id = details["place_id"]
        new_name = details.get("name", "")
        lat = details.get("lat")
        lng = details.get("lng")
        components = details.get("address_components") or []

        with self.conn:
            chain = self._upsert_location_parent_chain(
                components,
                leaf_name=new_name,
                leaf_types=details.get("types"),
            )
            parent_id = chain[-1] if chain else None

            # If the chain itself reused this very keyword anywhere — as the
            # deepest parent OR as a non-leaf ancestor (e.g. a free-text
            # "United States" promoted into the country slot while deeper
            # levels like NY/Manhattan were also discovered) — the UPDATE
            # below would create a cycle by reparenting the row onto a
            # descendant of itself. Guard by checking the full visited chain.
            if keyword_id in chain:
                # The keyword we were asked to "link" got reused inside the
                # chain. Nothing to merge from photo_keywords (it is already
                # the canonical row for its slot), so just return it.
                return {"keyword_id": keyword_id, "merged": False}

            update_sql = (
                "UPDATE keywords SET "
                "  place_id = ?, "
                "  latitude = ?, "
                "  longitude = ?, "
                "  name = ?, "
                "  parent_id = ? "
                "WHERE id = ?"
            )
            try:
                self.conn.execute(
                    update_sql,
                    (place_id, lat, lng, new_name, parent_id, keyword_id),
                )
                return {"keyword_id": keyword_id, "merged": False}
            except sqlite3.IntegrityError:
                # Two distinct constraints can fail here:
                #   (a) UNIQUE(place_id) — another row already has this
                #       place_id → merge case.
                #   (b) UNIQUE(name, parent_id) — another row already
                #       owns this (name, parent_id) slot with a different
                #       (or NULL) place_id → name-collision case.
                # Disambiguate by checking which.
                canonical = self.conn.execute(
                    "SELECT id FROM keywords WHERE place_id = ?", (place_id,),
                ).fetchone()
                if canonical is not None and canonical["id"] != keyword_id:
                    # Case (a): merge.
                    canonical_id = canonical["id"]
                    # FK on keywords.parent_id is enforced (foreign_keys=ON),
                    # so any descendants of the old keyword would block the
                    # final DELETE FROM keywords. Reparent them onto the
                    # canonical row first — the canonical row represents the
                    # same place, so its descendants inherit cleanly.
                    # Per-child reparent so a UNIQUE(name, parent_id) clash
                    # in the canonical's existing subtree (a child with the
                    # same name) doesn't blow up the bulk UPDATE. On clash,
                    # disambiguate the migrating child's name with a short
                    # id suffix — preserves both rows' photo links rather
                    # than losing data.
                    children = self.conn.execute(
                        "SELECT id, name FROM keywords WHERE parent_id = ?",
                        (keyword_id,),
                    ).fetchall()
                    for child in children:
                        try:
                            self.conn.execute(
                                "UPDATE keywords SET parent_id = ? WHERE id = ?",
                                (canonical_id, child["id"]),
                            )
                        except sqlite3.IntegrityError:
                            disambiguated = f"{child['name']} (id-{child['id']})"
                            try:
                                self.conn.execute(
                                    "UPDATE keywords SET parent_id = ?, name = ? "
                                    "WHERE id = ?",
                                    (canonical_id, disambiguated, child["id"]),
                                )
                            except sqlite3.IntegrityError as inner_err:
                                raise RuntimeError(
                                    f"child keyword '{child['name']}' "
                                    f"(id={child['id']}) collides with the "
                                    f"canonical row's subtree even after "
                                    f"disambiguation"
                                ) from inner_err
                    # Re-point photo_keywords from old → canonical, carrying
                    # each association's durable provenance with it. On a
                    # conflict the canonical row keeps the stronger of the two
                    # claims rather than silently dropping a stamp.
                    self.conn.execute(
                        f"""INSERT INTO photo_keywords (photo_id, keyword_id, source)
                           SELECT photo_id, ?, source
                           FROM photo_keywords WHERE keyword_id = ?
                           {self.KEYWORD_SOURCE_CONFLICT_SQL}""",
                        (canonical_id, keyword_id),
                    )
                    self.conn.execute(
                        "DELETE FROM photo_keywords WHERE keyword_id = ?",
                        (keyword_id,),
                    )
                    # Preserve remembered import paths before the foreign-key
                    # cascade deletes aliases of the absorbed place.
                    self.conn.execute(
                        'UPDATE keyword_import_aliases SET keyword_id = ? WHERE keyword_id = ?',
                        (canonical_id, keyword_id),
                    )
                    # Delete the now-empty old keyword row.
                    self.conn.execute(
                        "DELETE FROM keywords WHERE id = ?", (keyword_id,),
                    )
                    return {"keyword_id": canonical_id, "merged": True}

                # Case (b): name collision — a *different* keyword already
                # holds the (new_name, parent_id) slot. Disambiguate the
                # name by appending a short place_id suffix and retry.
                # Same approach as _upsert_one_keyword's leaf-collision path.
                if parent_id is None:
                    name_clash = self.conn.execute(
                        "SELECT id FROM keywords "
                        "WHERE name = ? AND parent_id IS NULL AND id != ?",
                        (new_name, keyword_id),
                    ).fetchone()
                else:
                    name_clash = self.conn.execute(
                        "SELECT id FROM keywords "
                        "WHERE name = ? AND parent_id = ? AND id != ?",
                        (new_name, parent_id, keyword_id),
                    ).fetchone()
                if name_clash is None:
                    # Neither place_id nor name conflict — shouldn't happen
                    # but re-raise rather than swallow.
                    raise
                suffix = place_id[-8:]
                disambiguated = f"{new_name} ({suffix})"
                try:
                    self.conn.execute(
                        update_sql,
                        (place_id, lat, lng, disambiguated, parent_id, keyword_id),
                    )
                    return {"keyword_id": keyword_id, "merged": False}
                except sqlite3.IntegrityError as inner_err:
                    raise RuntimeError(
                        f"keyword '{new_name}' (parent_id={parent_id}) "
                        f"collides with an existing row even after disambiguation"
                    ) from inner_err

    def retire_builtin_wildlife_genre(self, force=False):
        """Detach the retired built-in ``Wildlife`` genre from photos.

        Older Vireo versions attached a top-level, ``type='genre'`` Wildlife
        keyword whenever a photo received its first taxonomy keyword. That
        duplicated the taxonomy fact and exposed a misleading independent
        removal action. Wildlife-processing eligibility now lives solely in
        ``photos.wildlife_excluded``.

        Associations are retired only from photos that also carry a taxonomy
        or legacy-species keyword, which is the shape the old automatic rule
        produced. A Wildlife genre on a photo without species metadata may be
        user-authored and is preserved. For retired associations a flat-only
        sidecar removal is queued. Flat-only is important: a user may have a
        real hierarchy such as ``Wildlife|Birds|House Sparrow``; retiring the
        generated flat term must not delete that hierarchy.

        Provenance is *latched*, not re-derived on every pass. Each run first
        stamps ``photo_keywords.source = 'manual'`` on any Wildlife
        association carrying authorship evidence — a not-yet-synced pending
        add, a retained ``keyword_add`` edit, a ``discard`` record for a
        manual add (``/api/sync/discard`` deliberately leaves no sidecar), or
        a flat ``Wildlife`` term in a readable XMP sidecar — commits that, and
        only then considers deletions. The latch matters because every one of
        those signals is transient (``pending_changes`` clears on sync,
        ``edit_history`` is pruned to ``max_edit_history``) while this
        migration legitimately re-runs across sessions: an offline sidecar
        defers the completion marker, and ``force=True`` re-runs it outright.
        Re-deriving authorship from an eroding record means the same
        association can read as manual on one startup and generated on the
        next; a column on the association cannot be pruned, so the first
        run's verdict is the last word. Associations created from here on are
        stamped at write time by ``tag_photo(..., source='manual')``, so the
        evidence hunt only ever applies to pre-existing rows.

        When a same-name top-level keyword of another type (e.g. an
        ``individual`` ``Wildlife`` alongside the generated ``genre`` row) or
        a preserved duplicate genre row still survives on the photo, the flat
        XMP subject represents that survivor too, so the removal is skipped to
        avoid silently stripping the user-authored tag from the sidecar; the
        generated DB association is still detached. The keyword row is
        retained so edit history, manual associations, and user-created
        children remain valid.

        When a photo's sidecar was previously imported (``xmp_mtime`` is
        set) but is currently unavailable (for example, its NAS is offline),
        the association is preserved conservatively AND the run leaves the
        catalog-wide completion marker unset so a subsequent startup — once
        the volume returns — re-inspects that photo. Otherwise the marker
        would freeze the migration in a partially-processed state and any
        genuinely generated Wildlife association on the deferred photos
        would persist forever.
        """
        if (
            not force
            and self.get_meta(self._RETIRED_WILDLIFE_GENRE_KEY) == "1"
        ):
            return 0

        wildlife_rows = self.conn.execute(
            """SELECT id, name FROM keywords
               WHERE name = 'Wildlife' COLLATE NOCASE
                 AND type = 'genre' AND parent_id IS NULL"""
        ).fetchall()
        if not wildlife_rows:
            self.set_meta(self._RETIRED_WILDLIFE_GENRE_KEY, "1")
            return 0

        keyword_ids = [row["id"] for row in wildlife_rows]
        placeholders = ",".join("?" for _ in keyword_ids)
        fallback_workspace = self._active_workspace_id
        if fallback_workspace is None:
            fallback_row = self.conn.execute(
                "SELECT MIN(id) AS id FROM workspaces"
            ).fetchone()
            fallback_workspace = fallback_row["id"] if fallback_row else None

        # Latch authorship BEFORE evaluating any deletion. Each signal below
        # lives in a table that empties or is trimmed over time
        # (``pending_changes`` clears on sync, ``edit_history`` is pruned to
        # ``max_edit_history``), while this migration may re-run on a later
        # startup — a deferred offline sidecar leaves the completion marker
        # unset, and ``force=True`` re-runs it outright. Copying the verdict
        # onto ``photo_keywords.source`` turns eroding evidence into a durable
        # fact at the earliest moment the migration can observe it.
        self.conn.execute(
            f"""UPDATE photo_keywords
                SET source = {self.keyword_source_max_sql(
                    "photo_keywords.source", f"'{self.KEYWORD_SOURCE_MANUAL}'",
                )}
                WHERE keyword_id IN ({placeholders})
                  AND (source IS NULL OR source <> 'manual')
                  AND (
                      EXISTS (
                          SELECT 1
                          FROM pending_changes pending_add
                          WHERE pending_add.photo_id = photo_keywords.photo_id
                            AND pending_add.change_type = 'keyword_add'
                            AND pending_add.value = 'Wildlife' COLLATE NOCASE
                            AND (
                                NOT EXISTS (
                                    -- Pending changes store only a name, not
                                    -- the keyword ID. Use that evidence
                                    -- directly when the association is
                                    -- unambiguous.
                                    SELECT 1
                                    FROM photo_keywords other_pk
                                    JOIN keywords other_k
                                      ON other_k.id = other_pk.keyword_id
                                    WHERE other_pk.photo_id
                                          = photo_keywords.photo_id
                                      AND other_pk.keyword_id
                                          <> photo_keywords.keyword_id
                                      AND other_k.name
                                          = 'Wildlife' COLLATE NOCASE
                                )
                                OR NOT EXISTS (
                                    -- With homonyms, an exact durable source
                                    -- or keyword_add history row identifies
                                    -- the survivor and the broad name must not
                                    -- stamp its generated sibling. If every
                                    -- exact signal has already been pruned,
                                    -- preserve all ambiguous associations:
                                    -- deleting one would risk user metadata.
                                    SELECT 1
                                    FROM photo_keywords evidenced_pk
                                    JOIN keywords evidenced_k
                                      ON evidenced_k.id
                                         = evidenced_pk.keyword_id
                                    WHERE evidenced_pk.photo_id
                                          = photo_keywords.photo_id
                                      AND evidenced_k.name
                                          = 'Wildlife' COLLATE NOCASE
                                      AND (
                                          evidenced_pk.source = 'manual'
                                          OR EXISTS (
                                              SELECT 1
                                              FROM edit_history_items exact_item
                                              JOIN edit_history exact_edit
                                                ON exact_edit.id
                                                   = exact_item.edit_id
                                              WHERE exact_item.photo_id
                                                    = photo_keywords.photo_id
                                                AND exact_edit.action_type
                                                    = 'keyword_add'
                                                AND exact_edit.undone = 0
                                                AND exact_item.new_value
                                                    = CAST(
                                                        evidenced_pk.keyword_id
                                                        AS TEXT
                                                    )
                                          )
                                      )
                                )
                            )
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM edit_history_items manual_item
                          JOIN edit_history manual_edit
                            ON manual_edit.id = manual_item.edit_id
                          WHERE manual_item.photo_id = photo_keywords.photo_id
                            AND manual_edit.action_type = 'keyword_add'
                            AND manual_edit.undone = 0
                            AND manual_item.new_value
                                = CAST(photo_keywords.keyword_id AS TEXT)
                      )
                      OR EXISTS (
                          -- ``/api/sync/discard`` deliberately leaves no
                          -- sidecar but records the discarded add as
                          -- ``keyword_add:<value>`` in a ``discard`` item.
                          SELECT 1
                          FROM edit_history_items discard_item
                          JOIN edit_history discard_edit
                            ON discard_edit.id = discard_item.edit_id
                          WHERE discard_item.photo_id = photo_keywords.photo_id
                            AND discard_edit.action_type = 'discard'
                            AND discard_edit.undone = 0
                            AND discard_item.old_value
                                = 'keyword_add:Wildlife' COLLATE NOCASE
                            AND (
                                discard_item.new_value
                                    = CAST(
                                        photo_keywords.keyword_id AS TEXT
                                    )
                                OR (
                                    COALESCE(discard_item.new_value, '') = ''
                                    AND NOT EXISTS (
                                        -- Older discard rows retained only
                                        -- the name. Treat that evidence as
                                        -- exact only when no homonymous
                                        -- association makes it ambiguous.
                                        SELECT 1
                                        FROM photo_keywords discard_other_pk
                                        JOIN keywords discard_other_k
                                          ON discard_other_k.id
                                             = discard_other_pk.keyword_id
                                        WHERE discard_other_pk.photo_id
                                              = photo_keywords.photo_id
                                          AND discard_other_pk.keyword_id
                                              <> photo_keywords.keyword_id
                                          AND discard_other_k.name
                                              = 'Wildlife' COLLATE NOCASE
                                    )
                                )
                            )
                      )
                  )""",
            keyword_ids,
        )
        # Commit the latch on its own: preserving authorship must survive even
        # if the retirement pass below fails partway through.
        self.conn.commit()

        rows = self.conn.execute(
            f"""SELECT DISTINCT pk.photo_id, pk.keyword_id, pk.source,
                       p.filename, p.xmp_mtime, f.path AS folder_path,
                       EXISTS (
                           SELECT 1
                           FROM photo_keywords survivor_pk
                           JOIN keywords survivor_k
                             ON survivor_k.id = survivor_pk.keyword_id
                           WHERE survivor_pk.photo_id = pk.photo_id
                             AND survivor_k.name = 'Wildlife' COLLATE NOCASE
                             AND (
                                 survivor_k.id NOT IN ({placeholders})
                                 OR (
                                     survivor_k.id <> pk.keyword_id
                                     AND survivor_pk.source = 'manual'
                                 )
                             )
                       ) AS has_scan_survivor
                FROM photo_keywords pk
                JOIN photos p ON p.id = pk.photo_id
                JOIN folders f ON f.id = p.folder_id
                WHERE pk.keyword_id IN ({placeholders})
                  -- Authorship was latched above, so one durable predicate
                  -- replaces the pending/history/discard evidence hunt.
                  AND (pk.source IS NULL OR pk.source <> 'manual')
                  AND EXISTS (
                      SELECT 1
                      FROM photo_keywords species_pk
                      JOIN keywords species_k
                        ON species_k.id = species_pk.keyword_id
                      WHERE species_pk.photo_id = pk.photo_id
                        AND (species_k.type = 'taxonomy'
                             OR species_k.is_species = 1)
                  )""",
            [*keyword_ids, *keyword_ids],
        ).fetchall()

        # Group candidates per photo so their sidecar verdict and locked
        # retirement decision stay consistent across duplicate genre rows.
        photo_workspaces = {}
        # pid -> "manual" (readable sidecar carries a flat Wildlife term),
        # "defer" (sidecar unreadable/corrupt — decide on a later run), or
        # "generated" (readable sidecar with no Wildlife term), or "absent"
        # (no sidecar was ever imported, or it has since been deleted, so a
        # legacy NULL source cannot be distinguished from metadata imported
        # with write_xmp=False).
        sidecar_verdict_by_photo = {}
        unknown_without_sidecar_pairs = []
        deferred_sidecar = False
        for row in rows:
            pid = row["photo_id"]
            if pid not in sidecar_verdict_by_photo:
                base = os.path.splitext(row["filename"])[0]
                xmp_path = os.path.join(row["folder_path"], base + ".xmp")
                if not os.path.exists(xmp_path):
                    # A non-null mtime means a sidecar was imported earlier.
                    # If the original is unreachable too, the volume is
                    # offline (for example, an unmounted NAS): preserve rather
                    # than destroy metadata without being able to inspect its
                    # provenance, and flag the run as deferred so the
                    # completion marker stays unset — otherwise the catalog
                    # would be permanently frozen with this photo's generated
                    # Wildlife association still attached, even after the
                    # volume comes back online. A reachable original means
                    # the sidecar itself is gone, which no later run can
                    # change, so it gets the never-imported verdict instead
                    # of deferring every startup forever.
                    original_reachable = os.path.exists(
                        os.path.join(row["folder_path"], row["filename"])
                    )
                    if row["xmp_mtime"] is not None and not original_reachable:
                        sidecar_verdict_by_photo[pid] = "defer"
                        deferred_sidecar = True
                    else:
                        sidecar_verdict_by_photo[pid] = "absent"
                else:
                    # read_keywords() swallows ``ET.ParseError`` and returns
                    # an empty set for a corrupt sidecar, which is
                    # indistinguishable from a genuinely empty one. Parse
                    # explicitly so a malformed sidecar defers retirement
                    # (like the offline branch above) rather than silently
                    # classifying the tag as generated and stripping it.
                    import xml.etree.ElementTree as _ET
                    try:
                        _ET.parse(xmp_path)
                    except _ET.ParseError:
                        self.log.warning(
                            "Corrupt sidecar %s during Wildlife retirement; deferring",
                            xmp_path,
                        )
                        sidecar_verdict_by_photo[pid] = "defer"
                        deferred_sidecar = True
                    except Exception:
                        self.log.warning(
                            "Could not read sidecar %s during Wildlife retirement; deferring",
                            xmp_path,
                            exc_info=True,
                        )
                        sidecar_verdict_by_photo[pid] = "defer"
                        deferred_sidecar = True
                    else:
                        try:
                            from xmp import read_keywords

                            sidecar_verdict_by_photo[pid] = (
                                "manual"
                                if any(
                                    keyword_match_key(value) == "wildlife"
                                    for value in read_keywords(xmp_path)
                                )
                                else "generated"
                            )
                        except Exception:
                            self.log.warning(
                                "Could not inspect Wildlife provenance in %s; deferring",
                                xmp_path,
                                exc_info=True,
                            )
                            sidecar_verdict_by_photo[pid] = "defer"
                            deferred_sidecar = True
            verdict = sidecar_verdict_by_photo[pid]
            preserve_unknown_without_sidecar = (
                verdict == "absent" and row["source"] is None
            )
            if preserve_unknown_without_sidecar:
                # A legacy NULL-source association with no sidecar is also
                # preserved. Lightroom catalog imports historically called
                # execute_import(write_xmp=False) by default and attached the
                # keyword directly, leaving exactly the same observable shape
                # as the retired automatic rule. There is no safe retroactive
                # discriminator, so prefer metadata preservation and latch
                # the association as manual. New catalog imports are stamped
                # at write time and known generated sources can still retire.
                unknown_without_sidecar_pairs.append(
                    (pid, row["keyword_id"]),
                )
                continue
            if verdict == "defer":
                continue
            entry = photo_workspaces.setdefault(
                pid,
                {
                    "candidate_keyword_ids": set(),
                    "xmp_mtime": row["xmp_mtime"],
                    "manual_sidecar": verdict == "manual",
                    "scan_survivor": bool(row["has_scan_survivor"]),
                },
            )
            entry["candidate_keyword_ids"].add(row["keyword_id"])
            entry["scan_survivor"] = (
                entry["scan_survivor"]
                or bool(row["has_scan_survivor"])
            )

        if unknown_without_sidecar_pairs:
            self.conn.executemany(
                "UPDATE photo_keywords SET source = "
                + self.keyword_source_max_sql(
                    "photo_keywords.source", f"'{self.KEYWORD_SOURCE_MANUAL}'",
                )
                + " WHERE photo_id = ? AND keyword_id = ?",
                unknown_without_sidecar_pairs,
            )
            self.conn.commit()

        # Finalize in bounded writer transactions. The sidecar scan above can
        # take minutes on a large catalog, during which a person may re-add,
        # retype, or create another Wildlife keyword. Each chunk takes the
        # writer lock, revalidates those facts, queues cleanup, and deletes the
        # exact associations atomically. An edit before the lock is observed;
        # an edit after the commit follows the normal route and cancels any
        # now-stale flat removal. Bounding the chunk prevents this background
        # migration from monopolizing SQLite's single writer for the entire
        # catalog and turning otherwise-live edit requests into lock errors.
        retired_photo_ids = set()
        for photo_chunk in self._chunks(
            photo_workspaces.items(),
            self._WILDLIFE_RETIREMENT_WRITE_CHUNK_SIZE,
        ):
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                still_retirable = {}
                for photo_id, entry in photo_chunk:
                    current_photo = self.conn.execute(
                        "SELECT xmp_mtime FROM photos WHERE id = ?",
                        (photo_id,),
                    ).fetchone()
                    if (
                        current_photo is not None
                        and current_photo["xmp_mtime"] != entry["xmp_mtime"]
                    ):
                        # An XMP-to-DB reconciliation won the race with the
                        # sidecar scan. Preserve this association and leave
                        # the marker unset so the next startup inspects the
                        # newly accepted sidecar state.
                        deferred_sidecar = True
                        continue
                    candidate_ids = {
                        keyword_id
                        for keyword_id in entry["candidate_keyword_ids"]
                        if self.conn.execute(
                            """SELECT 1
                               FROM photo_keywords candidate_pk
                               JOIN keywords candidate_k
                                 ON candidate_k.id = candidate_pk.keyword_id
                               WHERE candidate_pk.photo_id = ?
                                 AND candidate_pk.keyword_id = ?
                                 AND (candidate_pk.source IS NULL
                                      OR candidate_pk.source <> 'manual')
                                 AND candidate_k.name
                                     = 'Wildlife' COLLATE NOCASE
                                 AND candidate_k.type = 'genre'
                                 AND candidate_k.parent_id IS NULL""",
                            (photo_id, keyword_id),
                        ).fetchone() is not None
                    }
                    if candidate_ids:
                        candidate_placeholders = ",".join(
                            "?" for _ in candidate_ids
                        )
                        has_current_same_name_survivor = self.conn.execute(
                            f"""SELECT 1
                                FROM photo_keywords survivor_pk
                                JOIN keywords survivor_k
                                  ON survivor_k.id = survivor_pk.keyword_id
                                WHERE survivor_pk.photo_id = ?
                                  AND survivor_k.name
                                      = 'Wildlife' COLLATE NOCASE
                                  AND survivor_pk.keyword_id
                                      NOT IN ({candidate_placeholders})
                                LIMIT 1""",
                            [photo_id, *candidate_ids],
                        ).fetchone() is not None
                        if (
                            entry["manual_sidecar"]
                            and not has_current_same_name_survivor
                            and not entry["scan_survivor"]
                        ):
                            # The sidecar's flat Wildlife term belongs to the
                            # candidate only when no same-name association
                            # currently survives. Decide this under the writer
                            # lock: a survivor may have been added or removed
                            # while the sidecar was being read.
                            self.conn.executemany(
                                "UPDATE photo_keywords SET source = "
                                + self.keyword_source_max_sql(
                                    "photo_keywords.source",
                                    f"'{self.KEYWORD_SOURCE_MANUAL}'",
                                )
                                + " WHERE photo_id = ? AND keyword_id = ?",
                                [
                                    (photo_id, keyword_id)
                                    for keyword_id in candidate_ids
                                ],
                            )
                            continue
                        # A survivor present during the scan but absent now
                        # owned the observed sidecar term. Its concurrent
                        # removal must not promote the obsolete genre; retire
                        # the candidate and let the queued removal (or the
                        # flat-removal fallback below) clean up XMP.
                        still_retirable[photo_id] = (
                            entry,
                            candidate_ids,
                            has_current_same_name_survivor,
                        )

                for photo_id, (
                    _entry,
                    _candidate_ids,
                    has_current_same_name_survivor,
                ) in still_retirable.items():
                    if has_current_same_name_survivor:
                        # Another top-level 'Wildlife' keyword (for example,
                        # type='individual') still owns the flat XMP subject.
                        # Keep its sidecar term while detaching the generated
                        # genre association below.
                        continue
                    # Workspace links may also change while the sidecar scan
                    # is running. Re-read every current owner under this
                    # chunk's writer lock so each workspace's independent
                    # pending queue receives the cleanup.
                    ws_ids = {
                        row["workspace_id"]
                        for row in self.conn.execute(
                            """SELECT wf.workspace_id
                               FROM photos p
                               JOIN photo_workspace_visibility wf
                                 ON wf.photo_id = p.id
                               WHERE p.id = ?""",
                            (photo_id,),
                        ).fetchall()
                    }
                    target_ws_ids = (
                        ws_ids
                        if ws_ids
                        else (
                            {fallback_workspace}
                            if fallback_workspace is not None
                            else set()
                        )
                    )
                    for ws_id in target_ws_ids:
                        existing_remove = self.conn.execute(
                            """SELECT 1 FROM pending_changes
                               WHERE photo_id = ? AND workspace_id = ?
                                 AND change_type IN (
                                     'keyword_remove', 'keyword_remove_flat'
                                 )
                                 AND value = 'Wildlife' COLLATE NOCASE
                               LIMIT 1""",
                            (photo_id, ws_id),
                        ).fetchone()
                        if existing_remove is None:
                            self.queue_change(
                                photo_id,
                                "keyword_remove_flat",
                                "Wildlife",
                                workspace_id=ws_id,
                                _commit=False,
                            )

                retired_associations = [
                    (photo_id, keyword_id)
                    for photo_id, (
                        _entry,
                        candidate_ids,
                        _has_current_same_name_survivor,
                    ) in still_retirable.items()
                    for keyword_id in candidate_ids
                ]
                if retired_associations:
                    # Delete only the exact candidates revalidated under this
                    # chunk's writer lock. The source predicate is repeated as
                    # a final fail-safe against future code movement.
                    self.conn.executemany(
                        """DELETE FROM photo_keywords
                           WHERE photo_id = ? AND keyword_id = ?
                             AND (source IS NULL OR source <> 'manual')""",
                        retired_associations,
                    )
                self.conn.commit()
                retired_photo_ids.update(still_retirable)
            except Exception:
                self.conn.rollback()
                raise
        # Only stamp the completion marker after a final locked population
        # check. A photo can become eligible while the sidecar scan is in
        # progress (for example, when a species is added), and XMP-to-DB
        # reconciliation uses this same writer lock from sidecar read through
        # its mtime stamp. The marker and this query therefore describe one
        # stable catalog state; any remaining or newly eligible candidate
        # leaves the marker unset for the next startup.
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            remaining_candidate = self.conn.execute(
                """SELECT 1
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   WHERE k.name = 'Wildlife' COLLATE NOCASE
                     AND k.type = 'genre'
                     AND k.parent_id IS NULL
                     AND (pk.source IS NULL OR pk.source <> 'manual')
                     AND EXISTS (
                         SELECT 1
                         FROM photo_keywords species_pk
                         JOIN keywords species_k
                           ON species_k.id = species_pk.keyword_id
                         WHERE species_pk.photo_id = pk.photo_id
                           AND (species_k.type = 'taxonomy'
                                OR species_k.is_species = 1)
                     )
                   LIMIT 1""",
            ).fetchone()
            if not deferred_sidecar and remaining_candidate is None:
                self.set_meta(
                    self._RETIRED_WILDLIFE_GENRE_KEY, "1", _commit=False,
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return len(retired_photo_ids)

    def upsert_one_keyword(
        self, name, parent_id, place_id=None, latitude=None, longitude=None,
        reuse_location_component=False,
    ):
        """Insert-or-fetch a single ``type='location'`` keyword row.

        Two dedupe modes:

        * ``place_id`` is given: dedupe on the partial unique index over
          ``place_id``. If a row with that ``place_id`` already exists, update
          its name/parent/coords (the user just re-picked the same Google
          place) and return its id.
        * ``place_id`` is ``None``: dedupe on ``(name, parent_id)`` among rows
          whose ``place_id`` is also NULL. SELECT-then-INSERT (rather than
          ``INSERT OR IGNORE``) so we never collide a coordless parent row
          with a place_id-bearing leaf that happens to share a name+parent.
        * ``reuse_location_component`` is true only for administrative address
          components. In that case, a matching location hierarchy row can be
          reused even if it already carries a place id. A coordless matching
          row can also absorb the selected administrative place's id and
          coordinates instead of creating a suffixed duplicate.

        Cross-type collision handling: the table-level ``UNIQUE(name,
        parent_id)`` constraint doesn't filter by ``type``, so a pre-existing
        keyword of a *different* type with the same ``(name, parent_id)`` can
        cause our INSERT to raise ``sqlite3.IntegrityError``. Rather than
        silently merging into an unrelated keyword (which would corrupt the
        user's existing tags), we catch that error, re-SELECT to confirm
        what's actually there, and raise a descriptive ``RuntimeError``. If
        the existing row turns out to be a coordless ``type='location'``
        row that our narrow SELECT somehow missed, we defensively return its
        id.
        """
        name = normalize_keyword_display(name)
        if not name:
            raise ValueError("keyword name is empty after normalization")
        if place_id is not None:
            if reuse_location_component:
                existing_place = self.conn.execute(
                    "SELECT id FROM keywords WHERE place_id = ?",
                    (place_id,),
                ).fetchone()
                if parent_id is None:
                    existing_component = self.conn.execute(
                        "SELECT id, type FROM keywords "
                        "WHERE name = ? COLLATE NOCASE AND parent_id IS NULL "
                        "  AND place_id IS NULL "
                        "ORDER BY CASE WHEN type = 'location' THEN 0 "
                        "  WHEN type = 'taxonomy' THEN 1 ELSE 2 END, "
                        "  (name = ?) DESC, id "
                        "LIMIT 1",
                        (name, name),
                    ).fetchone()
                else:
                    existing_component = self.conn.execute(
                        "SELECT id, type FROM keywords "
                        "WHERE name = ? COLLATE NOCASE AND parent_id = ? "
                        "  AND place_id IS NULL "
                        "ORDER BY (name = ?) DESC, id "
                        "LIMIT 1",
                        (name, parent_id, name),
                    ).fetchone()
                if (
                    existing_component is not None
                    and (
                        existing_component["type"] == "location"
                        or self._restore_misclassified_location_ancestor(
                            existing_component["id"],
                            allow_leaf=True,
                        )
                    )
                ):
                    if (
                        existing_place is not None
                        and existing_place["id"] != existing_component["id"]
                    ):
                        # Legacy admin disambiguation could leave a suffixed
                        # place-bearing row (e.g. "California (suffix)")
                        # alongside the matching coordless hierarchy row.
                        # Re-selecting the same Google place would otherwise
                        # fail the ON CONFLICT(place_id) DO UPDATE with
                        # UNIQUE(name, parent_id) and re-suffix instead of
                        # reusing the hierarchy row. Merge the suffixed row
                        # into the hierarchy row and let it own the place
                        # metadata so future assignments settle on one row.
                        self._merge_keyword_into(
                            existing_place["id"], existing_component["id"],
                        )
                    self.conn.execute(
                        "UPDATE keywords SET place_id = ?, type = 'location', "
                        "is_species = 0, taxon_id = NULL, latitude = ?, "
                        "longitude = ? WHERE id = ?",
                        (
                            place_id,
                            latitude,
                            longitude,
                            existing_component["id"],
                        ),
                    )
                    return existing_component["id"]
            insert_sql = (
                "INSERT INTO keywords "
                "(name, parent_id, type, place_id, latitude, longitude) "
                "VALUES (?, ?, 'location', ?, ?, ?) "
                "ON CONFLICT(place_id) WHERE place_id IS NOT NULL DO UPDATE SET "
                "  name = excluded.name, "
                "  parent_id = excluded.parent_id, "
                "  type = 'location', "
                "  is_species = 0, "
                "  taxon_id = NULL, "
                "  latitude = excluded.latitude, "
                "  longitude = excluded.longitude "
                "RETURNING id"
            )
            try:
                cur = self.conn.execute(
                    insert_sql, (name, parent_id, place_id, latitude, longitude),
                )
                return cur.fetchone()["id"]
            except sqlite3.IntegrityError:
                if reuse_location_component:
                    place_row = self.conn.execute(
                        "SELECT id FROM keywords WHERE place_id = ?",
                        (place_id,),
                    ).fetchone()
                    if place_row is None:
                        if parent_id is None:
                            clash = self.conn.execute(
                                "SELECT id, type, place_id FROM keywords "
                                "WHERE name = ? AND parent_id IS NULL",
                                (name,),
                            ).fetchone()
                        else:
                            clash = self.conn.execute(
                                "SELECT id, type, place_id FROM keywords "
                                "WHERE name = ? AND parent_id = ?",
                                (name, parent_id),
                            ).fetchone()
                        if (
                            clash is not None
                            and clash["place_id"] is None
                            and (
                                clash["type"] == "location"
                                or self._restore_misclassified_location_ancestor(
                                    clash["id"],
                                    allow_leaf=True,
                                )
                            )
                        ):
                            self.conn.execute(
                                "UPDATE keywords SET place_id = ?, latitude = ?, "
                                "longitude = ? WHERE id = ?",
                                (place_id, latitude, longitude, clash["id"]),
                            )
                            return clash["id"]
                # ON CONFLICT(place_id) handles same-place-id re-picks. The
                # remaining failure mode is the table-level UNIQUE(name,
                # parent_id): a *different* keyword (different place_id, or
                # NULL place_id) already occupies this slot. Disambiguate the
                # new row's name by appending a short place_id suffix and
                # retry. Realistic case: two distinct Google places with the
                # same name under the same parent (e.g. two parks named
                # "Riverside Park" in the same state).
                suffix = place_id[-8:]
                disambiguated = f"{name} ({suffix})"
                try:
                    cur = self.conn.execute(
                        insert_sql,
                        (disambiguated, parent_id, place_id, latitude, longitude),
                    )
                    return cur.fetchone()["id"]
                except sqlite3.IntegrityError as inner_err:
                    raise RuntimeError(
                        f"keyword '{name}' (parent_id={parent_id}) collides "
                        f"with an existing row even after disambiguation"
                    ) from inner_err

        if parent_id is None:
            if reuse_location_component:
                # Google's address_components carry no per-component
                # place_id, so when the DB has any place-bearing root at
                # this name (e.g. Georgia the state saved with a
                # place_id), we cannot prove that a component-only parent
                # of the same name refers to that specific Google place —
                # a later same-name second place (Georgia the country)
                # would misparent under the first one's root. Reuse a
                # coordless root when one exists (safe shared anchor);
                # otherwise fall through to inserting a fresh coordless
                # anchor rather than silently attaching under whichever
                # place-bearing row happens to have the lower id.
                candidates = self.conn.execute(
                    "SELECT id, type, place_id FROM keywords "
                    "WHERE name = ? COLLATE NOCASE AND parent_id IS NULL "
                    "  AND type IN ('location', 'taxonomy') "
                    "ORDER BY "
                    "  CASE WHEN type = 'location' THEN 0 ELSE 1 END, "
                    "  CASE WHEN place_id IS NULL THEN 0 ELSE 1 END, "
                    "  (name = ?) DESC, id",
                    (name, name),
                ).fetchall()
                existing = None
                if candidates:
                    top = candidates[0]
                    if top["place_id"] is None:
                        existing = top
            else:
                # Case-insensitive, like ``add_keyword``: "paris" must
                # reuse the "Paris" row rather than start a second place.
                existing = self.conn.execute(
                    "SELECT id, type FROM keywords "
                    "WHERE name = ? COLLATE NOCASE AND parent_id IS NULL "
                    "  AND type = 'location' AND place_id IS NULL "
                    "ORDER BY (name = ?) DESC, id LIMIT 1",
                    (name, name),
                ).fetchone()
        else:
            existing = self.conn.execute(
                "SELECT id, type FROM keywords "
                "WHERE name = ? COLLATE NOCASE AND parent_id = ? "
                "  AND type = 'location' "
                + ("" if reuse_location_component else "AND place_id IS NULL ")
                + "ORDER BY (name = ?) DESC, id LIMIT 1",
                (name, parent_id, name),
            ).fetchone()
        if existing:
            if existing["type"] == "location":
                return existing["id"]
            if self._restore_misclassified_location_ancestor(
                existing["id"],
                allow_leaf=reuse_location_component,
            ):
                return existing["id"]

        try:
            cur = self.conn.execute(
                "INSERT INTO keywords "
                "(name, parent_id, type, place_id, latitude, longitude) "
                "VALUES (?, ?, 'location', NULL, ?, ?)",
                (name, parent_id, latitude, longitude),
            )
            return cur.lastrowid
        except sqlite3.IntegrityError as integrity_err:
            # UNIQUE(name, parent_id) violated by a row our type-filtered
            # SELECT didn't see. Find out what's actually there.
            if parent_id is None:
                clash = self.conn.execute(
                    "SELECT id, type, place_id FROM keywords "
                    "WHERE name = ? AND parent_id IS NULL",
                    (name,),
                ).fetchone()
            else:
                clash = self.conn.execute(
                    "SELECT id, type, place_id FROM keywords "
                    "WHERE name = ? AND parent_id = ?",
                    (name, parent_id),
                ).fetchone()
            if clash is None:
                # Should be unreachable — re-raise the original error
                # rather than swallow it.
                raise
            if (
                clash["type"] == "location"
                and (
                    clash["place_id"] is None
                    or reuse_location_component
                )
            ):
                # Defensive: our narrow SELECT missed it (shouldn't happen,
                # but reusing it is safe and idempotent).
                return clash["id"]
            if self._restore_misclassified_location_ancestor(
                clash["id"],
                allow_leaf=reuse_location_component,
            ):
                # Older taxonomy marking could retype an administrative
                # location node such as United States -> California even
                # though location children still hung below it. Reuse the
                # repaired node instead of making every later Google-place
                # assignment fail on the stale type.
                return clash["id"]
            raise RuntimeError(
                f"keyword '{name}' (parent_id={parent_id}) exists with "
                f"type={clash['type']!r}, can't reuse for location chain"
            ) from integrity_err

    def accept_prediction(
        self,
        prediction_id,
        replace_species=False,
        photo_ids=None,
        prediction_ids=None,
        _commit=True,
    ):
        """Accept a prediction: mark as accepted and add species keyword.

        If the prediction belongs to a group, derives the consensus species
        from the individual votes and applies that to all photos.

        When ``replace_species`` is True, every photo that receives the new
        keyword first has its existing species/taxonomy keywords removed, so
        grouped photos are replaced consistently rather than accumulating both
        the old and new species tags. Each entry in the returned ``affected``
        list carries the ``old_species`` names that were stripped from that
        photo (empty when ``replace_species`` is False).

        ``photo_ids`` limits a grouped accept to matching group members;
        ``prediction_ids`` is the stricter, row-level form of the same limit.
        Independently of either, group expansion only ever *discovers*
        undecided rows (see ``_acceptance_targets``). Both limits are settled
        before the first write, so a call whose scope excludes every
        candidate row is a true no-op: it returns
        ``accepted_prediction_ids: []`` with ``keyword_id`` set to the
        existing keyword for the resolved species (``None`` when no such
        keyword exists yet).

        The returned ``accepted_prediction_ids`` lists every prediction row
        this call marked accepted, so a caller looping over a submitted batch
        can skip rows a previous grouped accept already covered instead of
        re-accepting them into duplicate history items.

        ``species_key`` is the resolved consensus identity, independent of
        the particular keyword alias used to tag the photos. Batch callers
        compare this key and record each result's actual ``keyword_id`` for
        undo/redo rather than requiring equivalent aliases to share an ID.

        All database changes are performed atomically in a single transaction
        unless ``_commit`` is False and the caller owns the transaction.
        """
        ws = self.workspace_id
        limited_photo_ids = None
        if photo_ids is not None:
            limited_photo_ids = {int(pid) for pid in photo_ids}
        limited_pred_ids = None
        if prediction_ids is not None:
            limited_pred_ids = {int(pid) for pid in prediction_ids}
        replace_taxonomy = (None, None)
        if replace_species:
            replace_taxonomy = _taxonomy_for_species_replacement()
        pred = _fetch_entry_prediction(self.conn, ws, prediction_id)
        if not pred:
            return None
        from species_identity import SpeciesResolver
        identity = SpeciesResolver(db=self.db).consensus(pred)
        has_explicit_evidence, source_taxon_id = _explicit_source_taxon(
            pred, identity,
        )

        try:
            species = _species_label_to_accept(
                pred, identity, has_explicit_evidence, source_taxon_id,
            )
            targets = _acceptance_targets(
                self.conn, ws, pred, prediction_id,
                limited_photo_ids, limited_pred_ids,
                self.DECIDED_PREDICTION_STATUSES,
            )
            if not targets:
                return _out_of_scope_result(self.conn, species, identity)

            source_args = {"source_taxon_id": source_taxon_id} if source_taxon_id is not None else {}
            kid = self.add_keyword(species, is_species=True, _commit=False, **source_args)
            species = _stored_keyword_name(self.conn, kid, species)
            # list of {"photo_id", "prediction_id", "old_species"}
            affected = []
            # Every row this call flips to accepted, including ones that
            # produced no ``affected`` entry (a status-only accept under
            # replace_species). Callers batching over a submitted id list use
            # it to avoid re-entering rows a grouped accept already covered.
            accepted_pred_ids = []

            def _accept_for_photo(photo_id, this_pred_id):
                """Accept one target row and tag its photo; return its
                ``affected`` entry, or ``None`` when there is nothing to
                record (see ``_affected_entry``)."""
                # Every accepted row resolves its own detection's losers,
                # including the entry row. Doing it here rather than up front
                # is what keeps an out-of-scope entry row untouched.
                _reject_detection_alternatives(self.conn, ws, this_pred_id)
                self.update_prediction_status(this_pred_id, "accepted", _commit=False)
                old_species = []
                already_has_species = photo_id in self.get_photos_with_equivalent_species(
                    [photo_id], kid
                )
                if replace_species:
                    old_species = _strip_replaced_species(
                        self.conn, ws, photo_id, this_pred_id, kid, species,
                        replace_taxonomy,
                        get_effective_config=self.get_effective_config,
                        remove_pending_changes=self.remove_pending_changes,
                        queue_change=self.queue_change,
                        species_root_name_for_taxon=self._species_root_name_for_taxon,
                        rename_species_highlights_species=self.rename_species_highlights_species,
                        rename_photo_preferences_species=self.rename_photo_preferences_species,
                    )
                changed_tag = not already_has_species
                if changed_tag:
                    self.tag_photo(
                        photo_id, kid, source="manual", _commit=False,
                    )
                    self.queue_change(photo_id, "keyword_add", species, _commit=False)
                return _affected_entry(
                    photo_id, this_pred_id, old_species, changed_tag,
                    replace_species,
                )

            for target_photo_id, target_pred_id in targets:
                accepted_pred_ids.append(target_pred_id)
                entry = _accept_for_photo(target_photo_id, target_pred_id)
                if entry is not None:
                    affected.append(entry)

            if _commit:
                self.conn.commit()
            return {
                "species": species,
                "species_key": identity.key,
                "keyword_id": kid,
                "affected": affected,
                "accepted_prediction_ids": accepted_pred_ids,
                "photo_ids": list(dict.fromkeys(
                    photo_id for photo_id, _pred_id in targets
                )),
            }
        except Exception:
            if _commit:
                self.conn.rollback()
            raise


# ``accept_prediction``'s steps. They are module functions rather than
# methods because repository methods may not call one another through
# ``self`` (``test_moved_writers_reach_each_other_only_through_the_facade``)
# nor hand ``self`` to a helper; each takes the connection and the bound
# façade methods it calls explicitly.


def _taxonomy_for_species_replacement():
    """Return ``(taxonomy, compare_prediction_to_keywords)`` for replace.

    Load taxonomy once for the whole call so replace_species can protect
    keywords whose relationship to a neighbouring subject's prediction is
    broader/same/narrower — not just exact-text matches. Loaded once per
    accept rather than inside ``_accept_for_photo`` so grouped accepts don't
    repeat the JSON parse per photo. ``(None, None)`` (missing/corrupt file,
    or unrelated import failure) cleanly degrades to exact-text protection.
    """
    try:
        from compare import compare_prediction_to_keywords as _cpk
        from taxonomy import load_local_taxonomy as _llt
        _replace_taxonomy = _llt()
        _compare_pred_to_kws = _cpk
    except Exception:
        log.warning(
            "Taxonomy unavailable for species replacement; "
            "protecting only exact-name keywords",
            exc_info=True,
        )
        _replace_taxonomy = None
        _compare_pred_to_kws = None
    return _replace_taxonomy, _compare_pred_to_kws


def _fetch_entry_prediction(conn, ws, prediction_id):
    """The prediction row the caller named, with its review group and photo."""
    return conn.execute(
        """SELECT pr.*,
                  pr.classifier_model AS model,
                  pr_rev.group_id AS group_id,
                  pr_rev.individual AS individual,
                  d.photo_id
           FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           LEFT JOIN prediction_review pr_rev
             ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
           WHERE pr.id = ?""",
        (ws, prediction_id),
    ).fetchone()


def _explicit_source_taxon(pred, identity):
    """Return ``(has_explicit_evidence, source_taxon_id)`` for the accept.

    A pure name lookup can flag a same-name keyword the async
    ``mark_species_keywords`` pass has not linked yet. Routing the
    accept through ``_add_source_species_keyword`` on that inferred
    id refuses to reuse the unlinked row and mints a suffixed
    duplicate; only explicit prediction evidence (a stored
    ``source_taxon_id`` or a native tol/iNat scientific name)
    earns that source-specific path.
    """
    native_scientific = pred["scientific_name"] if (
        pred["labels_fingerprint"] == "tol" or pred["model"].startswith("iNat")
    ) else None
    has_explicit_evidence = pred["source_taxon_id"] is not None or bool(native_scientific)
    source_taxon_id = identity.taxon_id if has_explicit_evidence else None
    return has_explicit_evidence, source_taxon_id


def _species_label_to_accept(pred, identity, has_explicit_evidence, source_taxon_id):
    """The species name the accept tags, before ``add_keyword`` normalizes it."""
    if has_explicit_evidence:
        species = identity.display_name if source_taxon_id else (identity.scientific_name or identity.display_name)
    else:
        # Preserve the raw label (or burst winner) for legacy
        # predictions: ``add_keyword``'s name-based dedup will
        # reuse an existing same-name keyword, even one
        # ``mark_species_keywords`` has not linked yet, without
        # introducing a suffixed duplicate.
        species = pred["species"]
        if pred["group_id"] and pred["individual"]:
            try:
                votes = json.loads(pred["individual"])
                if isinstance(votes, dict) and votes:
                    species = max(votes, key=lambda sp: votes[sp])
            except (TypeError, ValueError):
                pass
    return species


def _acceptance_targets(
    conn, ws, pred, prediction_id, limited_photo_ids, limited_pred_ids,
    decided_statuses,
):
    """The ``(photo_id, prediction_id)`` rows this accept will act on.

    Settle scope before the first write.

    ``limited_photo_ids`` / ``limited_pred_ids`` exist to make this
    call a no-op outside the caller's submitted scope, so *every*
    mutation — sibling rejection, keyword creation, status flips —
    has to sit behind that decision rather than beside it. This
    method used to reject the entry row's alternatives up front,
    which silently resolved rows the caller never submitted while
    the return value truthfully reported no accepts. The row set is
    therefore computed from reads only; nothing writes until it is
    non-empty.

    ``limited_photo_ids`` (the caller's ``photo_ids``) lets callers apply a
    grouped accept to a filtered subset without changing hidden photos.
    ``limited_pred_ids`` (``prediction_ids``) is the stricter form of the
    same limit, for callers that already know the exact prediction rows
    they are acting on: the grouped accept touches only those rows. Prefer
    it over ``photo_ids`` whenever the caller has a submitted id list. A
    photo is not a unique key for a prediction — one photo can carry
    several rows in the same burst group (one per classifier model, or per
    detection) — so a photo-id limit lets a grouped accept reach a row on
    an allowed photo that the caller never submitted. ``photo_ids``
    remains for callers whose intent really is "these photos" (highlight
    confirm, accept subject), where the row set is chosen by this method.

    Independently of either limit, group expansion only ever *discovers*
    undecided rows: a group member already ``accepted`` or ``rejected`` is
    left alone unless the caller named it (as the entry row, or in
    ``prediction_ids``). Accepting one burst member must not resurrect a
    sibling the user rejected in Review, nor re-flip a long-accepted one
    into a history item whose "previous" status never happened.
    """
    # If grouped, accept every prediction in the group (in this
    # workspace) that survives the caller's scope.
    if pred["group_id"]:
        group_preds = conn.execute(
            """SELECT pr.id, d.photo_id, pr_rev.status AS status
               FROM predictions pr
               JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
               JOIN detections d ON d.id = pr.detection_id
               JOIN photos ph ON ph.id = d.photo_id
               JOIN photo_workspace_visibility wf
                 ON wf.photo_id = ph.id AND wf.workspace_id = ?
               WHERE pr_rev.group_id = ? AND pr.classifier_model = ?""",
            (ws, ws, pred["group_id"], pred["model"]),
        ).fetchall()
        return [
            (gp["photo_id"], gp["id"])
            for gp in group_preds
            if _in_caller_scope(
                gp["photo_id"], gp["id"], limited_photo_ids, limited_pred_ids,
            )
            and _expansion_may_discover(
                gp["id"], gp["status"], prediction_id, limited_pred_ids,
                decided_statuses,
            )
        ]
    if _in_caller_scope(
        pred["photo_id"], prediction_id, limited_photo_ids, limited_pred_ids,
    ):
        return [(pred["photo_id"], prediction_id)]
    return []


def _in_caller_scope(photo_id, this_pred_id, limited_photo_ids, limited_pred_ids):
    """Does the row pass both of the caller's limits?"""
    photo_allowed = (
        limited_photo_ids is None
        or photo_id in limited_photo_ids
    )
    # The row-level limit, checked independently: a group
    # member's photo being in scope does not make every row
    # that member carries in scope.
    row_allowed = (
        limited_pred_ids is None
        or this_pred_id in limited_pred_ids
    )
    return photo_allowed and row_allowed


def _expansion_may_discover(
    this_pred_id, status, prediction_id, limited_pred_ids, decided_statuses,
):
    """May group expansion *discover* this row?

    Group expansion reaches rows the caller never named — that is
    its whole point — so it must not reach rows whose decision is
    already made. Without this, accepting one burst member from
    Review re-accepts a sibling the user explicitly rejected
    earlier (tagging that photo with the species it was denied)
    and re-flips long-accepted siblings, whose "previous" status
    in the resulting history item is then a fiction: undo would
    knock them back to pending. ``reviewed`` is treated the same
    as ``accepted`` / ``rejected`` here: the user marked the row
    reviewed to say "I looked and chose not to act", and
    expanding a group into it would silently flip that decision
    to ``accepted`` without any audit trail describing the
    overwrite.

    Rows the caller *named* are exempt, because then the caller,
    not the expansion, chose them: the entry row itself, and any
    row listed in ``limited_pred_ids``. ``batch-accept`` never
    lists a decided row (``_decided_prediction_ids`` filters them
    out first), so in practice this only ever exempts a row a
    route was pointed at directly.
    """
    if this_pred_id == prediction_id:
        return True
    if (
        limited_pred_ids is not None
        and this_pred_id in limited_pred_ids
    ):
        return True
    return status not in decided_statuses


def _out_of_scope_result(conn, species, identity):
    """The no-op result for an accept whose scope excludes every row.

    Nothing this caller submitted is acceptable here, so leave
    the database exactly as it was and report no changes.
    ``add_keyword`` is a write too (it creates the species row),
    so it also waits behind the scope decision; the read-only
    lookup keeps ``keyword_id``/``species`` meaningful for
    callers reconciling one batch's species without inventing a
    keyword for an accept that never happened.
    """
    display = normalize_keyword_display(species)
    existing = conn.execute(
        """SELECT id, name FROM keywords
           WHERE name = ? COLLATE NOCASE
           ORDER BY id LIMIT 1""",
        (display,),
    ).fetchone()
    return {
        "species": existing["name"] if existing else display,
        "species_key": identity.key,
        "keyword_id": existing["id"] if existing else None,
        "affected": [],
        "accepted_prediction_ids": [],
        "photo_ids": [],
    }


def _stored_keyword_name(conn, kid, species):
    """The stored spelling of the keyword the accept tags with.

    Re-read the stored keyword name so the queued sidecar changes,
    curation renames, and returned history payload all reflect the
    row actually tagged. add_keyword normalizes punctuation and
    applies the species casing convention, so the stored spelling
    can differ from the raw prediction label; using the raw value
    downstream would queue pending add/remove pairs that no longer
    cancel and write the un-normalized label to XMP.
    """
    stored = conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    if stored and stored["name"]:
        species = stored["name"]
    return species


def _reject_detection_alternatives(conn, ws, this_pred_id):
    """Resolve the losing rows on one accepted row's detection.

    Rejects siblings for the same
    (detection, classifier_model, labels_fingerprint) in this
    workspace (covers both accepting an alternative and accepting the
    top-1). Scoping by fingerprint is critical — without it, accepting
    a prediction from a new label set would mark old label-set rows as
    rejected, silently rewriting review state for unrelated
    fingerprints. Review state is workspace-scoped, so we upsert each
    row rather than UPDATE the base predictions table.

    Run per accepted row, and only for rows this call accepts: a
    grouped accept decides every member's detection, so leaving the
    other members' alternatives at 'alternative' would keep photos in
    Review's queue that this call already settled — and would make it
    unsafe for a batch caller to skip a submitted row that a grouped
    accept covered. Equally, a row the caller's scope excludes must
    not have its alternatives resolved, so this never runs for the
    entry row before scope is settled.
    """
    row = conn.execute(
        """SELECT detection_id, classifier_model, labels_fingerprint
           FROM predictions WHERE id = ?""",
        (this_pred_id,),
    ).fetchone()
    if row is None:
        return
    sibs = conn.execute(
        """SELECT pr.id FROM predictions pr
           LEFT JOIN prediction_review pr_rev
             ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
           WHERE pr.detection_id = ?
             AND pr.classifier_model = ?
             AND pr.labels_fingerprint = ?
             AND pr.id != ?
             AND COALESCE(pr_rev.status, 'pending') IN ('pending', 'alternative')""",
        (ws, row["detection_id"], row["classifier_model"],
         row["labels_fingerprint"], this_pred_id),
    ).fetchall()
    for s in sibs:
        conn.execute(
            """INSERT INTO prediction_review
                 (prediction_id, workspace_id, status, reviewed_at)
               VALUES (?, ?, 'rejected', datetime('now'))
               ON CONFLICT(prediction_id, workspace_id)
               DO UPDATE SET status = 'rejected',
                             reviewed_at = datetime('now')""",
            (s["id"], ws),
        )


def _strip_replaced_species(
    conn, ws, photo_id, this_pred_id, kid, species, replace_taxonomy, *,
    get_effective_config, remove_pending_changes, queue_change,
    species_root_name_for_taxon, rename_species_highlights_species,
    rename_photo_preferences_species,
):
    """Strip the photo's stale species keywords for ``replace_species``.

    Removes every species/taxonomy keyword on the photo that is neither the
    target species nor held by another subject on the photo (see
    ``_species_rows_to_replace``), queues the matching sidecar removals, and
    migrates curated species state onto the new species. Returns the names
    of the stripped rows (``old_species``).
    """
    to_remove = _species_rows_to_replace(
        conn, ws, photo_id, this_pred_id, kid, replace_taxonomy,
        get_effective_config=get_effective_config,
    )
    old_species = [row["name"] for row in to_remove]
    for row in to_remove:
        conn.execute(
            """DELETE FROM photo_keywords
               WHERE photo_id = ? AND keyword_id = ?""",
            (photo_id, row["id"]),
        )
    _queue_replaced_species_removals(
        photo_id, old_species, species,
        remove_pending_changes=remove_pending_changes,
        queue_change=queue_change,
    )
    _migrate_replaced_species_curation(
        ws, photo_id, to_remove, species,
        species_root_name_for_taxon=species_root_name_for_taxon,
        rename_species_highlights_species=rename_species_highlights_species,
        rename_photo_preferences_species=rename_photo_preferences_species,
    )
    return old_species


def _species_rows_to_replace(
    conn, ws, photo_id, this_pred_id, kid, replace_taxonomy, *,
    get_effective_config,
):
    """The photo's species keyword rows a replace should strip.

    Replace corrects *this subject's* identity, so it must
    not strip a species that belongs to a different detection
    (another subject) on the same photo. Any species named by
    a live prediction on another box is protected; without
    this, correcting the teal's ID wiped the American Wigeon
    confirmed on the neighbouring box. On a single-detection
    photo no box is protected, so every species keyword is
    replaced exactly as before.
    """
    neighbour_species = _live_neighbour_species(
        conn, ws, photo_id, this_pred_id,
        get_effective_config=get_effective_config,
    )
    protected = {
        keyword_match_key(s) for s in neighbour_species
    }
    existing = conn.execute(
        """SELECT k.id, k.name, k.taxon_id
           FROM photo_keywords pk
           JOIN keywords k ON k.id = pk.keyword_id
           LEFT JOIN taxa t ON t.id = k.taxon_id
           WHERE pk.photo_id = ?
             AND (k.is_species = 1 OR k.type = 'taxonomy')
             AND (t.rank = 'species' OR t.rank IS NULL)""",
        (photo_id,),
    ).fetchall()
    _is_target_species = _target_species_matcher(conn, kid)
    return [
        row for row in existing
        if not _is_target_species(row)
        and keyword_match_key(row["name"]) not in protected
        and not _supported_by_neighbour_taxonomy(
            row["name"], neighbour_species, replace_taxonomy,
        )
    ]


def _live_neighbour_species(conn, ws, photo_id, this_pred_id, *, get_effective_config):
    """Species named by live predictions on the photo's *other* detections.

    Mirror Compare's visibility filter when picking which
    neighbouring predictions may protect a species keyword:
      * skip 'alternative' rows (Compare drops them at
        web/predictions.py's api_predictions_compare, alongside
        'rejected');
      * skip detections below the workspace's effective
        detector_confidence — Compare marks those "dormant"
        and excludes their subjects entirely.
    Without this, a below-threshold neighbour or an
    alternative row on a real neighbour would keep an
    already-stale species keyword on the photo — replace
    would leave it in place and never queue a
    keyword_remove, so the sidecar would still list the
    dead species.
    """
    this_det = conn.execute(
        "SELECT detection_id FROM predictions WHERE id = ?",
        (this_pred_id,),
    ).fetchone()
    this_det_id = this_det["detection_id"] if this_det else None
    import config as _cfg
    _det_threshold = get_effective_config(
        _cfg.load()
    ).get("detector_confidence", 0.2)
    # Restrict to the latest labels_fingerprint per
    # (detection, classifier_model) — mirrors get_predictions
    # and the review/summary paths so stale rows from a prior
    # label set on a re-classified neighbouring detection do
    # not spuriously protect an obsolete species keyword.
    # Fold both sides through keyword_match_key so a raw
    # prediction species like `‘apapane` matches the stored
    # keyword `apapane` (add_keyword normalizes on write, so
    # a lower(trim(species)) SQL fold would otherwise miss
    # the still-live neighbour and queue its removal).
    return [
        row["species"] for row in conn.execute(
            """SELECT DISTINCT pr.species AS species
               FROM predictions pr
               JOIN detections d ON d.id = pr.detection_id
               LEFT JOIN prediction_review pr_rev
                 ON pr_rev.prediction_id = pr.id
                AND pr_rev.workspace_id = ?
               WHERE d.photo_id = ?
                 AND pr.detection_id IS NOT ?
                 AND COALESCE(pr_rev.status, 'pending')
                     NOT IN ('rejected', 'alternative')
                 AND d.detector_confidence >= ?
                 AND pr.labels_fingerprint = (
                     SELECT pr2.labels_fingerprint
                     FROM predictions pr2
                     WHERE pr2.detection_id = pr.detection_id
                       AND pr2.classifier_model
                           = pr.classifier_model
                     ORDER BY pr2.created_at DESC, pr2.id DESC
                     LIMIT 1
                 )""",
            (ws, photo_id, this_det_id, _det_threshold),
        ).fetchall()
        if row["species"]
    ]


def _target_species_matcher(conn, kid):
    """Return a predicate: is this keyword row the target species ``kid``?

    Mirror get_photos_with_equivalent_species: when another
    taxonomy/species keyword row shares the target's match
    key but points at a different taxon (e.g. legacy
    ``Robin`` alongside taxonomy ``robin``), the unlinked
    same-key row on the photo is ambiguous — it could be
    either species. Treating it as the target here would
    exclude it from ``to_remove``, so Replace Keywords
    would leave the wrong species attached while adding
    the correct one. Detect the homonym conflict once and
    gate the NULL-taxon fallback below.

    The same guard applies when the *target* is unlinked:
    a linked same-key row is a distinct species that must
    not be folded into the unlinked target, or Replace
    Keywords would exclude the linked homonym from
    ``to_remove`` and leave the wrong species attached.
    """
    target_row = conn.execute(
        "SELECT name, taxon_id FROM keywords WHERE id = ?",
        (kid,),
    ).fetchone()
    _target_key = keyword_match_key(target_row["name"])
    _target_homonym_conflict = False
    if target_row["taxon_id"] is not None:
        for _hrow in conn.execute(
            """SELECT name FROM keywords
               WHERE (is_species = 1 OR type = 'taxonomy')
                 AND taxon_id IS NOT NULL
                 AND taxon_id != ?""",
            (target_row["taxon_id"],),
        ).fetchall():
            if keyword_match_key(_hrow["name"]) == _target_key:
                _target_homonym_conflict = True
                break
    else:
        for _hrow in conn.execute(
            """SELECT name FROM keywords
               WHERE (is_species = 1 OR type = 'taxonomy')
                 AND taxon_id IS NOT NULL
                 AND id != ?""",
            (kid,),
        ).fetchall():
            if keyword_match_key(_hrow["name"]) == _target_key:
                _target_homonym_conflict = True
                break

    def _is_target_species(row):
        if target_row["taxon_id"] is not None:
            if row["taxon_id"] == target_row["taxon_id"]:
                return True
            return (
                row["taxon_id"] is None
                and not _target_homonym_conflict
                and keyword_match_key(row["name"])
                == _target_key
            )
        # Unlinked target: when a distinct linked row shares
        # this match key, only the exact target keyword row
        # is safe to treat as equivalent.
        if _target_homonym_conflict:
            return row["id"] == kid
        return (
            keyword_match_key(row["name"]) == _target_key
        )

    return _is_target_species


def _supported_by_neighbour_taxonomy(kw_name, neighbour_species, replace_taxonomy):
    """Does a neighbouring subject's prediction support this keyword?

    Compare treats a neighbouring subject's prediction as
    supporting an existing keyword under the taxonomy —
    match (same taxon), refinement (existing is broader
    than the prediction), broader (existing is more
    specific than the prediction). See compare.py's
    compare_prediction_to_keywords and the "keyword
    support" counters in templates/id_conflicts.html. Without
    this, a photo tagged with a broader ancestor keyword
    (e.g. Anatidae) that is only "held down" by a
    neighbour's American Wigeon prediction is stripped
    when a different box is replaced, and its curation
    (highlights, representatives) gets migrated onto the
    new species — the wrong subject. With no taxonomy
    available the check quietly no-ops and we fall back
    to exact-text protection, matching prior behaviour.
    """
    _replace_taxonomy, _compare_pred_to_kws = replace_taxonomy
    if not _replace_taxonomy or not neighbour_species:
        return False
    if _compare_pred_to_kws is None:
        return False
    for pred_species in neighbour_species:
        cmp_result = _compare_pred_to_kws(
            pred_species, [kw_name], _replace_taxonomy,
        )
        if cmp_result["category"] in (
            "match", "refinement", "broader",
        ):
            return True
    return False


def _queue_replaced_species_removals(
    photo_id, old_species, species, *, remove_pending_changes, queue_change,
):
    """Queue a sidecar ``keyword_remove`` for each stripped species.

    The DB rows are gone, but sync_to_xmp only strips a
    keyword from the sidecar when a matching keyword_remove
    pending change exists. Queue one per removed species so a
    "replace" actually clears the stale tags downstream. A
    still-pending add for the same keyword cancels out
    instead of stacking (mirrors queue_keyword_remove).
    """
    new_species_lower = species.lower()
    for old_name in old_species:
        if old_name.lower() == new_species_lower:
            continue
        cancelled = remove_pending_changes(
            photo_id, "keyword_add", old_name, _commit=False,
        )
        if cancelled == 0:
            queue_change(
                photo_id, "keyword_remove", old_name,
                _commit=False,
            )


def _migrate_replaced_species_curation(
    ws, photo_id, to_remove, species, *, species_root_name_for_taxon,
    rename_species_highlights_species, rename_photo_preferences_species,
):
    """Move the photo's curated species state onto the new species.

    Migrate curated species state (representatives and
    ordered highlights) alongside the replaced species
    tag. Without this, a photo highlighted or set as
    representative under the old species keeps rows in
    species_highlights / photo_preferences under a name
    it no longer carries, so it stops driving Highlights
    and Life List for the new species. Mirrors the
    migration in api_highlights_relabel.
    """
    curation_sources = _curation_source_names(
        to_remove, species_root_name_for_taxon,
    )
    for source_name in curation_sources:
        rename_species_highlights_species(
            source_name, species, [(photo_id, ws)],
            _commit=False,
        )
        rename_photo_preferences_species(
            source_name, species, [(photo_id, ws)],
            _commit=False,
        )


def _curation_source_names(to_remove, species_root_name_for_taxon):
    """Every exact species name curation rows for the stripped rows may use.

    Curation is canonicalized on write, so when
    the retired duplicate-species repair detached the
    root ``Verdin`` and leaves a hierarchy alias like
    ``Desert Verdin`` attached, existing highlights and
    representatives remain keyed on the canonical root
    ``Verdin``. Renaming only from the raw removed row
    name (the alias) would miss those rows and strand
    the curation under the old species. Look up the
    canonical root spelling for each removed row's
    taxon and rename from both source names so either
    layout migrates. Sidecar removes still use
    the raw ``old_name`` because the XMP file carries
    the alias, not the root spelling.
    Dedupe by exact source name. Both Python's
    ``str.lower()`` and the ASCII-fold ``keyword_match_key``
    collapse intentionally distinct rows: ``str.lower()``
    folds non-ASCII case (``"Éclair".lower() == "éclair"``),
    and ``keyword_match_key`` folds ASCII case-variant
    homonyms like legacy ``Robin`` vs taxonomy ``robin``
    that ``add_keyword`` deliberately keeps as separate
    rows. Either fold would drop the second distinct
    removed row's spelling from the curation rename source
    list, leaving highlights / representatives keyed on it
    stranded under a species the photo no longer carries.
    Curation rows are keyed by the exact stored species
    name, so exact-string dedup preserves every distinct
    source without renaming the same source twice.
    """
    curation_sources = []
    seen_sources = set()
    for row in to_remove:
        for candidate in (row["name"], species_root_name_for_taxon(row["taxon_id"])):
            if not candidate or candidate in seen_sources:
                continue
            seen_sources.add(candidate)
            curation_sources.append(candidate)
    return curation_sources


def _affected_entry(photo_id, this_pred_id, old_species, changed_tag, replace_species):
    """The ``affected`` entry for one accepted row, or ``None``.

    Record every mutation, and — for regular accepts — also
    record status-only no-ops so the prediction-status flip
    is auditable and undoable. Three cases feed ``affected``:
      * ``changed_tag`` — the target species tag was newly
        added and undo must untag it;
      * ``old_species`` — replace_species stripped stale
        species rows and undo must retag them;
      * neither, with ``replace_species=False`` — the photo
        already carried the target via an equivalent
        hierarchical/root row so nothing was tagged or
        untagged, but ``update_prediction_status`` still
        flipped this prediction to ``accepted``. The accept
        API records ``prediction_accept`` history from
        ``affected`` alone, so without this branch the
        status change would be silently non-auditable and
        undo could not restore ``pending`` on the accepted
        prediction (or its siblings). ``changed_tag=False``
        with empty ``old_species`` marks the entry as
        status-only so ``_apply_undo`` / ``_apply_redo``
        skip tag mutations while still reversing the review
        state.
    For ``replace_species=True``, a total no-op (photo
    already has the target and nothing to remove) is left
    out — the replace endpoint records
    ``prediction_replace_species``, which is not undoable,
    so a status-only aggregate would only produce a
    misleading audit entry with an empty ``old_value``.
    """
    if changed_tag or old_species:
        return {
            "photo_id": photo_id,
            "prediction_id": this_pred_id,
            "old_species": old_species,
            "changed_tag": changed_tag,
        }
    if not replace_species:
        return {
            "photo_id": photo_id,
            "prediction_id": this_pred_id,
            "old_species": [],
            "changed_tag": False,
        }
    return None
