"""Persistence for photo-keyword associations: which keywords a photo carries.

This module owns the reads of ``photo_keywords`` that answer "which keywords
does this photo carry" and "which photos carry this keyword", plus the two
writes that are not provenance-bearing associations:

- per-photo and per-batch keyword reads (``get_for_photo``,
  ``get_for_photos``) and the import's existence check (``association_row``);
- the photos a keyword is tagged on, in a selection, per workspace, or under
  the keyword's subtree (``photo_ids_tagged_with``,
  ``photo_workspaces_tagged_with``, ``subtree_photo_workspaces``,
  ``workspace_photo_count``), read by the keyword rename, delete and
  location routes;
- the classify job's subject-keyword filter (``filter_out_subject_tagged``);
- ``untag``, the association delete ``Database.untag_photo`` forwards to;
- the scanner's embedded-keyword offer record
  (``photo_embedded_keyword_offered``): ``embedded_offered_keys``,
  ``record_embedded_offered_keys`` and the merge-time
  ``transfer_embedded_offered_keys``.

The species a photo carries (species-rank reads, equivalence, the
confirmation reads) live in ``repositories/photo_species.py``.

What lives in ``repositories/keyword_provenance.py`` instead: every
``photo_keywords`` writer that creates or converges an association
(``tag_photo``, ``_merge_keyword_into``, ``link_keyword_to_place``,
``carry_companion_keywords``), which ``test_keyword_provenance_contract`` keys
to that module. This module never creates an association; a structural test
fails if it references one of those writers.

The method bodies moved verbatim from ``repositories/keywords.py`` (and
before that from ``Database``); only their signatures gained annotations.
``self._chunks``, ``self.KEYWORD_TYPES`` and ``self._FILTER_SUBJECT_CHUNK``
are the ``db`` module helper and constants of the same names, injected by
``Database._photo_keyword_repository``. The SQL text, parameter order, chunk
sizes and commit placement are unchanged.

Callers reach it through the ``db.photo_keywords`` accessor, which builds a
fresh repository per access. Keywords and photos are catalog-wide, so nothing
here needs the active workspace. ``untag_photo`` keeps its ``Database``
wrapper beside ``tag_photo``.
"""

import sqlite3
from collections.abc import Callable, Collection, Iterable, Iterator
from typing import Any


class PhotoKeywordRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        chunks: Callable[..., Iterator[list[Any]]],
        keyword_types: frozenset[str],
        filter_subject_chunk: int,
    ) -> None:
        self.conn = conn
        # ``db`` module helpers and constants, kept under their module names
        # so the moved bodies read them as ``self.<name>``. ``_chunks`` is
        # ``db._chunks`` itself (its size default is bound at import).
        self._chunks = chunks
        self.KEYWORD_TYPES = keyword_types
        # ``Database._FILTER_SUBJECT_CHUNK``, kept under its class name.
        self._FILTER_SUBJECT_CHUNK = filter_subject_chunk

    def get_for_photo(self, photo_id: int) -> list[sqlite3.Row]:
        """Return all keywords for a photo."""
        return self.conn.execute(
            """SELECT k.id, k.name, k.parent_id, k.type
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               WHERE pk.photo_id = ?
               ORDER BY k.name""",
            (photo_id,),
        ).fetchall()


    def get_for_photos(
        self, photo_ids: Collection[int],
    ) -> dict[int, list[dict[str, Any]]]:
        """Return keywords for a batch of photos keyed by photo id."""
        if not photo_ids:
            return {}
        # Dedup-preserving-order: chunking that re-queries the same id
        # in a later chunk would double-append it under setdefault.
        photo_ids = list(dict.fromkeys(photo_ids))
        result = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT pk.photo_id, k.id, k.name, k.parent_id, k.type,
                           k.is_species, k.taxon_id, t.rank AS taxon_rank
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    LEFT JOIN taxa t ON t.id = k.taxon_id
                    WHERE pk.photo_id IN ({placeholders})
                    ORDER BY k.type, k.name""",
                list(chunk),
            ).fetchall()
            for r in rows:
                result.setdefault(r["photo_id"], []).append(dict(r))
        return result


    def photo_ids_tagged_with(
        self, keyword_id: int, photo_ids: Iterable[int],
    ) -> set[int]:
        """The set of ``photo_ids`` that carry ``keyword_id``."""
        tagged = set()
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT photo_id FROM photo_keywords
                    WHERE keyword_id = ? AND photo_id IN ({placeholders})""",
                [keyword_id] + chunk,
            ).fetchall()
            tagged.update(row["photo_id"] for row in rows)
        return tagged


    def photo_workspaces_tagged_with(self, keyword_id: int) -> list[sqlite3.Row]:
        """Rows (``photo_id``, ``workspace_id``): each photo carrying
        ``keyword_id`` once per workspace that can see it."""
        return self.conn.execute(
            """SELECT pk.photo_id, wf.workspace_id
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE pk.keyword_id = ?""",
            (keyword_id,),
        ).fetchall()


    def subtree_photo_workspaces(self, keyword_id: int) -> list[sqlite3.Row]:
        """Distinct rows (``photo_id``, ``workspace_id``) for every photo
        tagged with ``keyword_id`` or any keyword below it, once per
        workspace that can see the photo."""
        return self.conn.execute(
            """WITH RECURSIVE tree(id) AS (
                   SELECT ?
                   UNION ALL
                   SELECT k.id FROM keywords k
                   JOIN tree t ON k.parent_id = t.id
               )
               SELECT DISTINCT p.id AS photo_id, wf.workspace_id
               FROM photos p
               JOIN photo_keywords pk ON pk.photo_id = p.id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               JOIN tree t ON t.id = pk.keyword_id""",
            (keyword_id,),
        ).fetchall()


    def workspace_photo_count(
        self, keyword_id: int, workspace_id: int | None,
    ) -> sqlite3.Row:
        """Row (``name``, ``cnt``): the keyword's tagged photos visible in
        ``workspace_id``. Always one row; ``name`` is NULL when ``cnt`` is 0."""
        return self.conn.execute(
            """SELECT k.name, COUNT(pk.photo_id) as cnt
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE k.id = ? AND wf.workspace_id = ?""",
            (keyword_id, workspace_id),
        ).fetchone()


    def association_row(self, photo_id: int, keyword_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT 1 FROM photo_keywords "
            "WHERE photo_id = ? AND keyword_id = ?",
            (photo_id, keyword_id),
        ).fetchone()


    def filter_out_subject_tagged(
        self, photo_ids: Collection[int], subject_types: Collection[str],
    ) -> list[int]:
        """Return the subset of photo_ids whose photos do NOT have any keyword
        of a type in subject_types. Empty subject_types or empty photo_ids
        returns photo_ids unchanged (preserving input order).

        Photo ids are chunked under SQLite's bind-variable limit so callers
        can safely pass arbitrarily large lists. The classify job sources
        photo ids from get_collection_photos(per_page=999999), which can
        exceed older SQLite builds' 999-variable cap and trip
        OperationalError: too many SQL variables.

        When ``'taxonomy'`` is among the requested types, legacy species rows
        (``is_species=1`` with a non-taxonomy ``type``) also count as
        subject-tagged. Upgraded databases carry these rows until the
        background ``mark_species_keywords`` pass retypes them; without this
        guard, already-identified photos would still be classified and would
        appear in 'Needs Identification' during that window.
        """
        if not subject_types or not photo_ids:
            return list(photo_ids)
        types = [t for t in subject_types if t in self.KEYWORD_TYPES]
        if not types:
            return list(photo_ids)
        type_placeholders = ",".join("?" * len(types))
        type_clause = f"k.type IN ({type_placeholders})"
        if "taxonomy" in types:
            type_clause = f"({type_clause} OR k.is_species = 1)"
        photo_ids_list = list(photo_ids)
        excluded = set()
        chunk_size = self._FILTER_SUBJECT_CHUNK
        for i in range(0, len(photo_ids_list), chunk_size):
            chunk = photo_ids_list[i:i + chunk_size]
            pid_placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"""SELECT DISTINCT pk.photo_id FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    WHERE {type_clause}
                      AND pk.photo_id IN ({pid_placeholders})""",
                types + chunk,
            ).fetchall()
            for r in rows:
                excluded.add(r["photo_id"])
        return [pid for pid in photo_ids_list if pid not in excluded]


    def untag(self, photo_id: int, keyword_id: int) -> None:
        """Remove a keyword association from a photo (no commit)."""
        self.conn.execute(
            "DELETE FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (photo_id, keyword_id),
        )


    def embedded_offered_keys(self, photo_id: int) -> set[str]:
        """Normalized keys the scanner has imported from the image file itself.

        The pending-removal filter (``db.pending_changes.keyword_removal_keys``)
        suppresses a value only until the next XMP sync clears the queue
        entry, but Vireo never writes into image files, so a later full scan
        or image rewrite re-reads the same embedded value. The scanner records every embedded value it offers to
        a photo here so ``_import_embedded_keywords_for_photo`` can filter
        them out on later passes -- a user removal is not silently undone when
        the queued removal has already been synced away.
        """
        return {
            row["keyword_key"]
            for row in self.conn.execute(
                "SELECT keyword_key FROM photo_embedded_keyword_offered "
                "WHERE photo_id = ?",
                (photo_id,),
            )
        }


    def record_embedded_offered_keys(
        self, photo_id: int, keys: Iterable[str],
    ) -> None:
        """Mark these normalized keys as embedded-offered for the photo.

        See ``embedded_offered_keys`` for why the record exists. Idempotent (INSERT OR IGNORE on the composite
        primary key). ``keys`` empty is a no-op. Does not commit.
        """
        rows = [(photo_id, key) for key in keys if key]
        if not rows:
            return
        self.conn.executemany(
            "INSERT OR IGNORE INTO photo_embedded_keyword_offered "
            "(photo_id, keyword_key) VALUES (?, ?)",
            rows,
        )


    def transfer_embedded_offered_keys(self, losing_id: int, surviving_id: int) -> None:
        """Move the losing row's embedded-offered keys onto the survivor.

        Used by every merge path that deletes a photo row with a different
        row inheriting its identity: the suppression record exists so a
        user's keyword removal survives a later rescan, and that intent must
        follow the surviving row rather than be dropped with the losing one.
        No commit; the caller folds this into its own transaction. The
        non-cascading FK on ``photo_embedded_keyword_offered.photo_id``
        would otherwise block the ``DELETE FROM photos``.
        """
        self.conn.execute(
            "INSERT OR IGNORE INTO photo_embedded_keyword_offered "
            "(photo_id, keyword_key) "
            "SELECT ?, keyword_key FROM photo_embedded_keyword_offered "
            "WHERE photo_id = ?",
            (surviving_id, losing_id),
        )
        self.conn.execute(
            "DELETE FROM photo_embedded_keyword_offered WHERE photo_id = ?",
            (losing_id,),
        )
