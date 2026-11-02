"""Collapse duplicate keyword rows in the active workspace.

Duplicates are grouped by (normalized name, parent_id, type,
species-bearing) — name alone is not identity: the location system
deliberately creates same-name keywords under different parents
(Springfield under Illinois vs. Missouri), and same-name keywords of
different types (species vs. genre) are distinct by design. Legacy
``type='general', is_species=1`` rows are also distinct from ordinary
general homonyms. Merging across those slots retags photos with the
wrong place/kind.

The scope and liveness reads are ``KeywordTreeRepository``'s
(``duplicate_scope_rows``, ``live_ids``, ``normalize_row_name``); each merge
runs through ``Database._merge_keyword_into``, the provenance-folding
keyword merge. Nothing here commits: the caller's unit of work owns the
pass, and an exception rolls every pending merge back.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from keyword_normalization import keyword_match_key, normalize_keyword_display

if TYPE_CHECKING:
    from db import Database


def merge_duplicate_keywords(db: Database) -> int:
    """Find and merge normalized duplicate keywords in the active workspace.

    A keyword is in scope when it — or any descendant — is tagged on a
    photo in the active workspace. XMP import only tags the leaf of a
    hierarchical keyword ("Birds > Heron" tags Heron, not Birds), so
    duplicate ancestors usually have no photo_keywords rows of their
    own; walking up from the tagged leaves brings them in scope while
    still leaving other workspaces' keywords untouched.

    Keeps the lowest ID (earliest created), moves all photo associations,
    reparents any child keywords onto the survivor (the parent_id FK
    would otherwise block the DELETE), and deletes the duplicates.
    Runs passes until convergence so case-duplicate parent chains
    ("Birds">"Heron" vs "birds">"heron") fully collapse: the children
    only become same-parent duplicates after their parents merge.
    Does not commit. Returns count of merges performed.
    """
    return merge_duplicate_keywords_pass(db, db.require_workspace_id())


def merge_duplicate_keywords_pass(db: Database, ws: int) -> int:
    """Convergence loop of ``merge_duplicate_keywords`` for workspace ``ws``. Caller commits."""
    total_merged = 0
    while True:
        rows = db.keyword_tree.duplicate_scope_rows(ws)
        grouped = {}
        for row in rows:
            key = keyword_match_key(row["name"])
            if not key:
                continue
            species_bearing = (
                row["type"] == "taxonomy" or row["is_species"] == 1
            )
            grouped.setdefault(
                (key, row["parent_id"], row["type"], species_bearing),
                [],
            ).append(row)
        dupes = [
            group for group in grouped.values()
            if len({row["id"] for row in group}) > 1
        ]
        if not dupes:
            break

        for group in dupes:
            # Prefer an already-clean spelling when one exists; otherwise
            # keep the earliest row to preserve the old case-only behavior.
            ordered = sorted(
                group,
                key=lambda row: (
                    normalize_keyword_display(row["name"]) != row["name"],
                    row["id"],
                ),
            )
            keep_id = ordered[0]["id"]
            all_ids = [row["id"] for row in ordered]

            # A prior group in this pass can recursively delete ids
            # from later groups: merging duplicate parents cascades
            # into their duplicate children, so a child group whose
            # keep_id was one of those children is now stale. Skip
            # groups whose keep_id is gone (the next while iteration
            # re-queries and picks a fresh survivor) and drop dead
            # remove_ids so we don't UPDATE photo_keywords toward a
            # non-existent FK target.
            alive = db.keyword_tree.live_ids(all_ids)
            if keep_id not in alive:
                continue
            remove_ids = [x for x in all_ids if x != keep_id and x in alive]

            db.keyword_tree.normalize_row_name(keep_id)
            for rid in remove_ids:
                total_merged += db._merge_keyword_into(
                    rid, keep_id, pending_source_only=True,
                )

    return total_merged
