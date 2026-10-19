"""Persistence for the species photos carry, read through their keywords.

A photo's species is the set of species-bearing keywords (``is_species = 1
OR type = 'taxonomy'``) tagged on it whose linked taxon is species-rank or
unknown. This module owns the reads that answer "which species does this
photo carry":

- ``get_species_for_photos``: the deduplicated species names per photo,
  canonicalized to the same-taxon root spelling the curation tables key on;
- ``species_rank_keywords_for_photo`` and
  ``photo_ids_with_species_rank_keyword``, the Highlights eligibility reads;
- ``get_photos_with_equivalent_species``: which photos already carry a
  target species by taxon (or, unlinked, by normalized name), the read every
  accept and confirm makes before tagging (``Database`` keeps its wrapper,
  which ``repositories/keyword_provenance.py`` binds by name);
- the species-confirmation reads ``/api/encounters/species`` runs
  (``previous_species_candidates``, ``attached_species_rows``);
- ``get_accepted_species``: the species on the active workspace's
  geolocated photos, for the map's species filter.

Nothing here writes. The method bodies moved verbatim from
``repositories/keywords.py`` (and before that from ``Database``); only their
signatures gained annotations. ``self._chunks`` is ``db._chunks``, injected by
``Database._photo_species_repository``, and ``workspace_id`` resolves the
active workspace lazily through ``Database._ws_id`` at exactly the point the
original code called it. The SQL text, parameter order and chunk sizes are
unchanged.

Callers reach it through the ``db.photo_species`` accessor, which builds a
fresh repository per access; do not hold one across a workspace switch.
"""

import sqlite3
from collections.abc import Callable, Collection, Iterable, Iterator
from typing import Any

from keyword_normalization import keyword_match_key


class PhotoSpeciesRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        resolve_workspace_id: Callable[[], int],
        *,
        chunks: Callable[..., Iterator[list[Any]]],
    ) -> None:
        self.conn = conn
        self._resolve_workspace_id = resolve_workspace_id
        # ``db._chunks`` itself (its size default is bound at import), kept
        # under its module name so the moved bodies read ``self._chunks``.
        self._chunks = chunks

    @property
    def workspace_id(self) -> int:
        """The active workspace id, resolved at each read (raises if none)."""
        return self._resolve_workspace_id()

    def get_species_for_photos(
        self, photo_ids: Collection[int], include_identities: bool = False,
    ) -> dict[int, list[Any]]:
        """Return deduplicated species-rank keyword names for photos.

        Returns a dict mapping photo_id -> list of species name strings.
        With include_identities, each entry contains the stored name and a
        source-aware identity key for comparisons that must preserve homonyms.

        A linked taxon must actually have rank ``species``; linked family,
        genus, and other ancestor keywords remain taxonomy keywords but are
        not presented as species. Taxonomy rows without a resolvable taxon
        retain the legacy behavior so user-created/offline species tags do
        not disappear.

        Multiple keyword nodes can represent the same taxon (for example a
        Lightroom hierarchy leaf plus an older top-level confirmation row).
        Collapse those by taxon_id and canonicalize to the same-taxon root's
        stored spelling when one exists — species_representatives,
        species_highlights, and life-list preference rows key on that root
        spelling, so a photo whose only surviving species tag is a hierarchy
        leaf ("verdin" after repair detached the redundant "Verdin" root)
        would otherwise miss those curation lookups. Falls back to the
        row's own name when no root exists, and taxonomy-less legacy rows
        continue to use the normalized keyword name.
        """
        if not photo_ids:
            return {}
        # Dedup-preserving-order: chunking that re-queries the same id
        # in a later chunk would double-append it under setdefault.
        photo_ids = list(dict.fromkeys(photo_ids))
        chosen = {}
        source_keys = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT pk.photo_id, k.id, k.name, k.parent_id,
                           k.taxon_id, k.source_taxon_id, t.inat_id, t.name AS scientific_name, t.rank AS taxon_rank
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    LEFT JOIN taxa t ON t.id = k.taxon_id
                    WHERE pk.photo_id IN ({placeholders})
                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                      AND (t.rank = 'species' OR t.rank IS NULL)
                    ORDER BY pk.photo_id,
                             CASE WHEN k.parent_id IS NULL THEN 1 ELSE 0 END,
                             k.id""",
                list(chunk),
            ).fetchall()
            for r in rows:
                if r["taxon_id"] is not None:
                    identity = ("taxon", r["taxon_id"])
                else:
                    # Preserve exact spelling for NULL-taxon rows: unlinked
                    # curation compares by exact ``k.name``, so a root ``Foo``
                    # and a hierarchy leaf ``foo`` on the same photo must
                    # both appear here — folding by ``keyword_match_key``
                    # would drop the root and strand its representative /
                    # highlight lookups.
                    identity = ("name", r["name"])
                # Track whether the chosen row is itself a root: attached root
                # rows must keep their own stored spelling (curation is keyed
                # by the actually attached ``k.name``, so a same-taxon sibling
                # root row must not rewrite it). Only hierarchy leaves need the
                # root-name fallback.
                is_root = r["parent_id"] is None
                chosen.setdefault(r["photo_id"], {}).setdefault(
                    identity, (r["name"], is_root)
                )
                source_id = r["source_taxon_id"] or r["inat_id"]
                key = f"taxon:{source_id}" if source_id else (
                    "scientific:" + r["scientific_name"].casefold() if r["scientific_name"]
                    else "name:" + keyword_match_key(r["name"])
                )
                source_keys.setdefault((r["photo_id"], identity), key)
        canonical_roots = self._canonical_species_roots(chosen)
        result = {}
        for photo_id, by_identity in chosen.items():
            names = []
            for identity, (name, is_root) in by_identity.items():
                if identity[0] == "taxon" and not is_root:
                    name = canonical_roots.get(identity[1], name)
                names.append({"name": name, "key": source_keys[(photo_id, identity)]} if include_identities else name)
            result[photo_id] = sorted(names, key=lambda n: keyword_match_key(n["name"] if include_identities else n))
        return result


    def _canonical_species_roots(
        self, chosen: dict[int, dict[tuple[str, Any], tuple[str, bool]]],
    ) -> dict[int, str]:
        """``{taxon_id: root spelling}`` for the taxa ``chosen`` links to."""
        taxon_ids = {
            identity[1]
            for by_identity in chosen.values()
            for identity in by_identity
            if identity[0] == "taxon"
        }
        canonical_roots = {}
        if taxon_ids:
            for chunk in self._chunks(list(taxon_ids)):
                placeholders = ",".join("?" for _ in chunk)
                root_rows = self.conn.execute(
                    f"""SELECT taxon_id, name FROM keywords
                        WHERE taxon_id IN ({placeholders})
                          AND parent_id IS NULL
                          AND (is_species = 1 OR type = 'taxonomy')
                        ORDER BY id""",
                    list(chunk),
                ).fetchall()
                for row in root_rows:
                    canonical_roots.setdefault(row["taxon_id"], row["name"])
        return canonical_roots


    def species_rank_keywords_for_photo(self, photo_id: int) -> list[sqlite3.Row]:
        """A photo's species-rank identification keywords.

        Rows (``id``, ``name``, ``is_species``, ``type``) for keywords that are
        ``is_species`` or ``taxonomy`` and linked to a ``species``-rank taxon or
        to none; ``is_species`` rows first, then most recently tagged first.
        """
        return self.conn.execute(
            """SELECT k.id, k.name, k.is_species, k.type
               FROM photo_keywords pk
               JOIN keywords k ON k.id = pk.keyword_id
               LEFT JOIN taxa t ON t.id = k.taxon_id
               WHERE pk.photo_id = ?
                 AND (k.is_species = 1 OR k.type = 'taxonomy')
                 AND (t.rank = 'species' OR t.rank IS NULL)
               ORDER BY k.is_species DESC, pk.rowid DESC""",
            (photo_id,),
        ).fetchall()


    def photo_ids_with_species_rank_keyword(self, photo_ids: Iterable[int]) -> set[int]:
        """Which of ``photo_ids`` carry a species-rank identification keyword.

        Same keyword filter as :meth:`species_rank_keywords_for_photo`.
        """
        found = set()
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            found.update(
                row["photo_id"] for row in self.conn.execute(
                    f"""SELECT DISTINCT pk.photo_id
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        LEFT JOIN taxa t ON t.id = k.taxon_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          AND (t.rank = 'species' OR t.rank IS NULL)""",
                    chunk,
                ).fetchall()
            )
        return found


    def get_photos_with_equivalent_species(
        self, photo_ids: Collection[int], keyword_id: int,
        exclude_keyword_ids: Iterable[int] | None = None,
    ) -> set[int]:
        """Return submitted photo ids already carrying the target species.

        Species identity is the linked taxon when available, not a particular
        keyword row. This lets a hierarchical ``Birds|Verdin`` tag satisfy a
        later confirmation that resolved to the top-level Verdin keyword.
        Unlinked legacy species fall back to the normalized display name.

        When ``exclude_keyword_ids`` is provided, keyword rows with those ids
        are ignored during the match. Callers use this to look past rows that
        are about to be removed — for example, a same-taxon replacement where
        the "already carries this species" answer must reflect only the rows
        that will survive the mutation.
        """
        if not photo_ids:
            return set()
        target = self.conn.execute(
            "SELECT name, taxon_id FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        if target is None:
            return set()
        result = set()
        ids = list(dict.fromkeys(int(pid) for pid in photo_ids))
        target_key = keyword_match_key(target["name"])
        excluded = tuple(dict.fromkeys(int(x) for x in (exclude_keyword_ids or ())))
        excl_placeholders = ",".join("?" for _ in excluded)
        excl_clause = f" AND k.id NOT IN ({excl_placeholders})" if excluded else ""
        homonym_conflict = self._species_homonym_conflict(
            target, keyword_id, target_key,
        )
        for chunk in self._chunks(ids):
            placeholders = ",".join("?" for _ in chunk)
            if target["taxon_id"] is not None:
                result.update(self._linked_species_photo_ids(
                    chunk, placeholders, target, target_key, excl_clause,
                    excluded, homonym_conflict,
                ))
                continue
            # Target is unlinked. When a distinct linked row shares this
            # match key, any same-key row on the photo could be either
            # species; only the exact target keyword row is safe to
            # treat as equivalent.
            if homonym_conflict:
                rows = self.conn.execute(
                    f"""SELECT pk.photo_id
                        FROM photo_keywords pk
                        WHERE pk.photo_id IN ({placeholders})
                          AND pk.keyword_id = ?""",
                    [*chunk, keyword_id],
                ).fetchall()
                result.update(row["photo_id"] for row in rows)
                continue
            rows = self.conn.execute(
                f"""SELECT pk.photo_id, k.name
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    WHERE pk.photo_id IN ({placeholders})
                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                      {excl_clause}""",
                [*chunk, *excluded],
            ).fetchall()
            result.update(
                row["photo_id"] for row in rows
                if keyword_match_key(row["name"]) == target_key
            )
        return result


    def _species_homonym_conflict(
        self, target: sqlite3.Row, keyword_id: int, target_key: str,
    ) -> bool:
        """Whether a distinct linked species shares the target's match key."""
        # When another taxonomy/species keyword row shares the target's
        # match key but points at a different taxon (e.g. legacy
        # ``Robin`` alongside taxonomy ``robin``), the taxon_id-is-NULL
        # fallback below is ambiguous: an unlinked same-key row on the
        # photo could be either species. Treating it as the target would
        # let a confirm/accept skip ``tag_photo``/``queue_change`` and
        # leave the intended species keyword absent. Detect the homonym
        # conflict once and gate the fallback.
        #
        # The same guard applies when the *target* itself is unlinked:
        # if any other same-key row IS linked to a taxon, that linked
        # row is a distinct species that must not be folded into the
        # unlinked target during accept/confirm. In that case the
        # name-only fallback matches only the exact target row.
        homonym_conflict = False
        if target["taxon_id"] is not None:
            for row in self.conn.execute(
                """SELECT name FROM keywords
                   WHERE (is_species = 1 OR type = 'taxonomy')
                     AND taxon_id IS NOT NULL
                     AND taxon_id != ?""",
                (target["taxon_id"],),
            ).fetchall():
                if keyword_match_key(row["name"]) == target_key:
                    homonym_conflict = True
                    break
        else:
            for row in self.conn.execute(
                """SELECT name FROM keywords
                   WHERE (is_species = 1 OR type = 'taxonomy')
                     AND taxon_id IS NOT NULL
                     AND id != ?""",
                (keyword_id,),
            ).fetchall():
                if keyword_match_key(row["name"]) == target_key:
                    homonym_conflict = True
                    break
        return homonym_conflict


    def _linked_species_photo_ids(
        self, chunk: list[int], placeholders: str, target: sqlite3.Row,
        target_key: str, excl_clause: str, excluded: tuple[int, ...],
        homonym_conflict: bool,
    ) -> set[int]:
        """Photos in ``chunk`` carrying a taxon-linked target species."""
        found = set()
        # Match rows linked to the same taxon first — the fast,
        # authoritative case that survives any display-name rename.
        rows = self.conn.execute(
            f"""SELECT DISTINCT pk.photo_id
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND k.taxon_id = ?
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          {excl_clause}""",
            [*chunk, target["taxon_id"], *excluded],
        ).fetchall()
        found.update(row["photo_id"] for row in rows)
        if homonym_conflict:
            return found
        # Fallback: upgraded libraries can carry a hierarchical
        # species leaf typed as taxonomy/is_species that
        # mark_species_keywords hasn't yet linked to a taxon_id
        # (see the explicit ``taxon_id IS NULL`` branch it
        # handles). A strict taxon_id equality above would miss
        # those legacy leaves, so a follow-up
        # confirm/accept-species after add_keyword created a
        # linked top-level root would not recognize the existing
        # hierarchy and would queue a duplicate root tag plus
        # sidecar add. Match unlinked rows by normalized display
        # name to preserve the hierarchy. Guarded above so an
        # ambiguous same-key homonym doesn't get folded in.
        rows = self.conn.execute(
            f"""SELECT pk.photo_id, k.name
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND k.taxon_id IS NULL
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          {excl_clause}""",
            [*chunk, *excluded],
        ).fetchall()
        found.update(
            row["photo_id"] for row in rows
            if keyword_match_key(row["name"]) == target_key
        )
        return found


    def previous_species_candidates(
        self, photo_ids: Iterable[int], name: str, linked_root_taxon: int | None,
    ) -> list[sqlite3.Row]:
        """Species-rank rows on ``photo_ids`` that may stand for species ``name``.

        Rows (``id``, ``name``, ``taxon_id``, ``photo_id``) of attached species
        rows whose taxon is species-rank or unknown, named ``name`` (NOCASE),
        or also linked to ``linked_root_taxon`` when that is set. Each
        chunk's rows come root rows first, then by keyword id.
        """
        rows = []
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            if linked_root_taxon is not None:
                rows.extend(self.conn.execute(
                    f"""SELECT k.id, k.name, k.taxon_id, pk.photo_id
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        LEFT JOIN taxa t ON t.id = k.taxon_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          AND (t.rank = 'species' OR t.rank IS NULL)
                          AND (k.taxon_id = ?
                               OR k.name = ? COLLATE NOCASE)
                        ORDER BY CASE WHEN k.parent_id IS NULL
                                      THEN 0 ELSE 1 END,
                                 k.id""",
                    [*chunk, linked_root_taxon, name],
                ).fetchall())
            else:
                rows.extend(self.conn.execute(
                    f"""SELECT k.id, k.name, k.taxon_id, pk.photo_id
                        FROM photo_keywords pk
                        JOIN keywords k ON k.id = pk.keyword_id
                        LEFT JOIN taxa t ON t.id = k.taxon_id
                        WHERE pk.photo_id IN ({placeholders})
                          AND k.name = ? COLLATE NOCASE
                          AND (k.is_species = 1 OR k.type = 'taxonomy')
                          AND (t.rank = 'species' OR t.rank IS NULL)
                        ORDER BY CASE WHEN k.parent_id IS NULL
                                      THEN 0 ELSE 1 END,
                                 k.id""",
                    [*chunk, name],
                ).fetchall())
        return rows


    def attached_species_rows(self, photo_ids: Iterable[int]) -> list[sqlite3.Row]:
        """Every species-rank (or unranked) species row attached to ``photo_ids``.

        Rows (``photo_id``, ``id``, ``name``, ``taxon_id``); each chunk's rows
        come by photo, then root rows first, then by keyword id.
        """
        rows = []
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(self.conn.execute(
                f"""SELECT pk.photo_id, k.id, k.name, k.taxon_id
                    FROM photo_keywords pk
                    JOIN keywords k ON k.id = pk.keyword_id
                    LEFT JOIN taxa t ON t.id = k.taxon_id
                    WHERE pk.photo_id IN ({placeholders})
                      AND (k.is_species = 1 OR k.type = 'taxonomy')
                      AND (t.rank = 'species' OR t.rank IS NULL)
                    ORDER BY pk.photo_id,
                             CASE WHEN k.parent_id IS NULL THEN 0 ELSE 1 END,
                             k.id""",
                list(chunk),
            ).fetchall())
        return rows


    def get_accepted_species(self) -> list[str]:
        """Return distinct marker species from geolocated photos in the active workspace.

        Uses the same "geolocated" definition as locations.get_geolocated_photos: a
        photo is included if it has EXIF coords OR a ``type='location'``
        keyword with coords. That keeps the species filter dropdown in sync
        with which photos can actually appear as markers — otherwise photos
        placed via location-keyword coords would render on the map but their
        species would be missing from the filter.
        """
        ws = self.workspace_id
        return [
            row[0]
            for row in self.conn.execute(
                """
                SELECT DISTINCT k.name
                FROM photos p
                JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
                JOIN photo_keywords pk ON pk.photo_id = p.id
                JOIN keywords k ON k.id = pk.keyword_id AND k.is_species = 1
                WHERE wf.workspace_id = ?
                  AND (
                    (p.latitude IS NOT NULL AND p.longitude IS NOT NULL)
                    OR EXISTS (
                      SELECT 1
                      FROM photo_keywords pk_loc
                      JOIN keywords k_loc ON k_loc.id = pk_loc.keyword_id
                      WHERE pk_loc.photo_id = p.id
                        AND k_loc.type = 'location'
                        AND k_loc.latitude IS NOT NULL
                        AND k_loc.longitude IS NOT NULL
                    )
                  )
                ORDER BY k.name ASC
                """,
                (ws,),
            ).fetchall()
        ]
