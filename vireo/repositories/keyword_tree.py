"""Persistence for the keyword tree: hierarchy reads, renames, merges, deletes.

This module owns the SQL behind the shape of the keyword hierarchy and the
edits that change it:

- tree and listing reads: the active workspace's tagged keywords with their
  ancestors (``get_tree``, ``list_all`` with photo counts), and every
  keyword's parent link (``parent_rows``) for building keyword paths;
- placement: the top-level species twins of a keyword filed in the tree
  (``taxonomy_root_twins``), and the reparent of a colliding child during a
  merge (``reparent_disambiguated``);
- renames and retypes: the statements ``Database.update_keyword`` issues
  (``get_update_target``, ``same_type_peer``, ``cross_type_peer``,
  ``apply_update``), the rename state the ``/api/keywords`` route compares
  (``rename_state``), and the dependent strings a rename carries along
  (``rename_dependents``, ``normalize_row_name``);
- duplicate merges: ``merge_duplicates`` and the scope and liveness reads of
  ``Database._merge_duplicate_keywords_pass`` (``duplicate_scope_rows``,
  ``live_ids``), plus the duplicate groups the Keywords page lists
  (``workspace_duplicate_groups``);
- ``delete``.

Inserting keyword rows is ``repositories/keywords.py``; the photos a keyword
is tagged on are ``repositories/photo_keywords.py``.

What lives in ``repositories/keyword_provenance.py`` instead: the
``photo_keywords`` writers that create or converge an association, among them
``_merge_keyword_into``, which ``test_keyword_provenance_contract`` keys to
that module. The control flow of ``_merge_duplicate_keywords_pass`` and
``update_keyword`` stays on ``Database`` with its
``self._merge_keyword_into(...)`` call on the façade; they delegate only
their statements here. This module never references one of those writers,
and a structural test fails if it does.

The method bodies moved verbatim from ``repositories/keywords.py`` (and
before that from ``Database``); only their signatures gained annotations.
``self._chunks`` is ``db._chunks``, injected by
``Database._keyword_tree_repository``, and ``workspace_id`` resolves the
active workspace lazily through ``Database._ws_id`` at exactly the points the
original code called it. Every façade method a moved body calls is bound from
the ``Database`` instance under its own name (see ``FACADE_METHODS``) and
called as ``self.<name>(...)``, so monkeypatches of ``Database`` methods keep
reaching the moved code. The SQL text, parameter order, chunk sizes and
commit placement are unchanged: ``_commit=False`` callers own their
transaction, and no method here commits unless the ``Database`` method it
backs did.

Callers reach it through the ``db.keyword_tree`` accessor, which builds a
fresh repository per access; do not hold one across a workspace switch.
``_rename_keyword_dependents`` and ``_reparent_disambiguated`` keep their
``Database`` wrappers, which the keyword repositories bind by name.
"""

import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

from keyword_normalization import normalize_keyword_display
from repositories.identity_alternatives import retarget_candidate_names

# ``Database`` methods the moved bodies call through the façade.
FACADE_METHODS = (
    "_merge_duplicate_keywords_pass",
    "_rename_keyword_dependents",
    "rename_photo_preferences_species",
    "rename_species_highlights_species",
)


class KeywordTreeRepository:
    # Bound ``Database`` methods that ``__init__`` installs from
    # ``FACADE_METHODS``, declared so the type checker sees them.
    _merge_duplicate_keywords_pass: Callable[..., Any]
    _rename_keyword_dependents: Callable[..., Any]
    rename_photo_preferences_species: Callable[..., Any]
    rename_species_highlights_species: Callable[..., Any]

    def __init__(
        self,
        conn: sqlite3.Connection,
        resolve_workspace_id: Callable[[], int],
        *,
        chunks: Callable[..., Iterator[list[Any]]],
        facade: object,
    ) -> None:
        self.conn = conn
        self._resolve_workspace_id = resolve_workspace_id
        # ``db._chunks`` itself (its size default is bound at import), kept
        # under its module name so the moved bodies read ``self._chunks``.
        self._chunks = chunks
        # Bound ``Database`` methods, kept under their façade names.
        for name in FACADE_METHODS:
            setattr(self, name, getattr(facade, name))

    @property
    def workspace_id(self) -> int:
        """The active workspace id, resolved at each read (raises if none)."""
        return self._resolve_workspace_id()

    # -- tree and listing reads ----------------------------------------------

    def get_tree(self) -> list[sqlite3.Row]:
        """Return keywords used by photos in the active workspace, plus ancestors."""
        return self.conn.execute(
            """WITH RECURSIVE
               leaf_kw AS (
                   SELECT DISTINCT pk.keyword_id AS id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                   WHERE wf.workspace_id = ?
               ),
               ancestors AS (
                   SELECT id FROM leaf_kw
                   UNION
                   SELECT k.parent_id
                   FROM keywords k
                   JOIN ancestors a ON a.id = k.id
                   WHERE k.parent_id IS NOT NULL
               )
               SELECT k.id, k.name, k.parent_id, k.type
               FROM keywords k
               JOIN ancestors a ON a.id = k.id
               ORDER BY k.name""",
            (self.workspace_id,),
        ).fetchall()


    def list_all(self) -> list[sqlite3.Row]:
        """Return keywords used in the active workspace (plus ancestors) with photo counts, type, and taxon info.

        Both counts are grouped aggregates over one materialized pass of the
        workspace's keyword links. ``photo_count`` walks each tagged keyword
        up its parent chain (at most 20 levels) and counts distinct photos
        per ancestor, so a photo tagged with two species under one genus
        counts once for the genus. Correlated per-keyword subqueries here
        re-ran the link scan for every row: about 150 s on a 65k-photo
        workspace, which left the Keywords page empty.
        """
        ws = self.workspace_id
        return self.conn.execute(
            """WITH RECURSIVE
               ws_links AS MATERIALIZED (
                   SELECT pk.keyword_id, pk.photo_id
                   FROM photo_keywords pk
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                   WHERE wf.workspace_id = ?
               ),
               ws_kw AS (SELECT DISTINCT keyword_id AS id FROM ws_links),
               ancestors AS (
                   SELECT id FROM ws_kw
                   UNION
                   SELECT k.parent_id
                   FROM keywords k
                   JOIN ancestors a ON a.id = k.id
                   WHERE k.parent_id IS NOT NULL
               ),
               lineage(keyword_id, ancestor_id, depth) AS (
                   SELECT id, id, 0 FROM ws_kw
                   UNION ALL
                   SELECT l.keyword_id, k.parent_id, l.depth + 1
                   FROM lineage l
                   JOIN keywords k ON k.id = l.ancestor_id
                   WHERE k.parent_id IS NOT NULL AND l.depth < 20
               ),
               lineage_pairs AS (
                   SELECT DISTINCT keyword_id, ancestor_id FROM lineage
               ),
               subtree_counts AS (
                   SELECT lp.ancestor_id AS id,
                          COUNT(DISTINCT wl.photo_id) AS photo_count
                   FROM lineage_pairs lp
                   JOIN ws_links wl ON wl.keyword_id = lp.keyword_id
                   GROUP BY lp.ancestor_id
               ),
               direct_counts AS (
                   SELECT keyword_id AS id, COUNT(*) AS direct_photo_count
                   FROM ws_links
                   GROUP BY keyword_id
               )
               SELECT k.id, k.name, k.parent_id, k.type, k.taxon_id,
                      k.latitude, k.longitude, k.place_id,
                      t.name AS taxon_name, t.common_name AS taxon_common_name,
                      COALESCE(sc.photo_count, 0) AS photo_count,
                      COALESCE(dc.direct_photo_count, 0) AS direct_photo_count
               FROM keywords k
               JOIN ancestors a ON a.id = k.id
               LEFT JOIN taxa t ON t.id = k.taxon_id
               LEFT JOIN subtree_counts sc ON sc.id = k.id
               LEFT JOIN direct_counts dc ON dc.id = k.id
               ORDER BY k.name""",
            (ws,),
        ).fetchall()


    def parent_rows(self) -> list[sqlite3.Row]:
        """Every keyword's ``id``, ``name`` and ``parent_id`` (for ``keyword_paths``)."""
        return self.conn.execute(
            'SELECT id, name, parent_id FROM keywords'
        ).fetchall()


    # -- placement -------------------------------------------------------------

    def taxonomy_root_twins(self) -> list[tuple[int, int]]:
        """Childless root twins with one placed row and consistent identity.

        Group by exact spelling before filtering identities: conflicts between
        roots or placed rows must not disappear from the ambiguity check.
        A shared source id proves identity even without a local taxon link.
        """
        groups = {}
        rows = self.conn.execute(
            "SELECT k.id, k.name, k.parent_id, k.taxon_id, k.source_taxon_id, "
            "t.inat_id AS local_source, "
            "EXISTS(SELECT 1 FROM keywords c WHERE c.parent_id = k.id) AS has_children "
            "FROM keywords k LEFT JOIN taxa t ON t.id = k.taxon_id "
            "WHERE k.type = 'taxonomy' AND k.name IS NOT NULL ORDER BY k.id",
        ).fetchall()
        for row in rows:
            groups.setdefault(row["name"], []).append(row)
        pairs = []
        for group in groups.values():
            placed = [row for row in group if row["parent_id"] is not None]
            local_ids = {row["taxon_id"] for row in group if row["taxon_id"] is not None}
            sources = {value for row in group for value in
                       (row["source_taxon_id"], row["local_source"]) if value is not None}
            if len(placed) != 1 or len(local_ids) > 1 or len(sources) > 1:
                continue
            if any(not isinstance(value, int) or value <= 0 for value in sources):
                continue
            target = placed[0]
            target_source = (target["source_taxon_id"] if target["source_taxon_id"] is not None
                             else target["local_source"])
            for root in group:
                if root["parent_id"] is not None or root["has_children"]:
                    continue
                root_source = (root["source_taxon_id"] if root["source_taxon_id"] is not None
                               else root["local_source"])
                same_local = root["taxon_id"] is not None and root["taxon_id"] == target["taxon_id"]
                same_source = root_source is not None and root_source == target_source
                if same_local or same_source:
                    pairs.append((root["id"], target["id"]))
        return sorted(pairs)


    def reparent_disambiguated(
        self, child: sqlite3.Row, dst_id: int, new_name: str,
    ) -> None:
        """Move a colliding child under ``dst_id`` under a free name.

        The three collision branches in ``_merge_keyword_into`` all preserve
        the migrating row rather than folding it away, which means they all
        rename it -- and a rename is never just the ``keywords`` row. Pending
        sidecar edits and the species curation tables key on the spelling, so
        skipping the dependent migration leaves an unsynced ``keyword_add``
        writing the retired name into XMP and drops starred photos out of the
        highlight and life-list queries. Caller commits.
        """
        self.conn.execute(
            "UPDATE keywords SET parent_id = ?, name = ? WHERE id = ?",
            (dst_id, new_name, child["id"]),
        )
        self._rename_keyword_dependents(child["id"], child["name"], new_name)


    # -- renames and retypes ---------------------------------------------------
    #
    # ``Database.update_keyword`` keeps its control flow (and the
    # ``self._merge_keyword_into(...)`` call, which runs the merge in
    # ``repositories/keyword_provenance.py``) in db.py; the statements it
    # issues live here, unchanged.

    def get_update_target(self, keyword_id: int) -> sqlite3.Row | None:
        """The stored (name, type, taxon_id, parent_id) of a keyword, or None."""
        return self.conn.execute(
            "SELECT name, type, taxon_id, parent_id FROM keywords WHERE id = ?",
            (keyword_id,),
        ).fetchone()


    def same_type_peer(
        self, new_name: str, parent_id: int | None, effective_type: str | None,
        keyword_id: int,
    ) -> sqlite3.Row | None:
        """Another keyword of ``effective_type`` in the same (name, parent) slot."""
        if parent_id is None:
            peer = self.conn.execute(
                "SELECT id FROM keywords "
                "WHERE name = ? COLLATE NOCASE "
                "AND parent_id IS NULL AND type = ? AND id != ? LIMIT 1",
                (new_name, effective_type, keyword_id),
            ).fetchone()
        else:
            peer = self.conn.execute(
                "SELECT id FROM keywords "
                "WHERE name = ? COLLATE NOCASE "
                "AND parent_id = ? AND type = ? AND id != ? LIMIT 1",
                (new_name, parent_id, effective_type, keyword_id),
            ).fetchone()
        return peer


    def cross_type_peer(
        self, new_name: str, parent_id: int | None, keyword_id: int,
    ) -> sqlite3.Row | None:
        """Any other keyword occupying (name, parent_id), whatever its type."""
        return self.conn.execute(
            "SELECT id, type FROM keywords "
            "WHERE name = ? COLLATE NOCASE "
            "AND parent_id = ? AND id != ? LIMIT 1",
            (new_name, parent_id, keyword_id),
        ).fetchone()


    def apply_update(self, keyword_id: int, updates: Mapping[str, Any]) -> None:
        """Write the resolved ``updates`` column map to one keyword and commit."""
        previous = self.rename_state(keyword_id) if 'name' in updates else None
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [keyword_id]
        self.conn.execute(f"UPDATE keywords SET {set_clause} WHERE id = ?", values)
        if previous is not None and (previous['is_species'] or previous['type'] == 'taxonomy'):
            retarget_candidate_names(self.conn, previous['name'], updates['name'])
        self.conn.commit()


    def commit(self) -> None:
        """Commit the connection's open transaction."""
        self.conn.commit()


    def rename_state(self, keyword_id: int) -> sqlite3.Row | None:
        """Row (``name``, ``is_species``, ``type``) of one keyword, or None.

        What a rename or retype compares before and after the update.
        """
        return self.conn.execute(
            """SELECT name, is_species, type
               FROM keywords WHERE id = ?""",
            (keyword_id,),
        ).fetchone()


    def rename_dependents(
        self, keyword_id: int, old_name: str | None, new_name: str | None,
    ) -> None:
        """Carry a keyword row's rename into every string that mirrors its name.

        Pending sidecar edits and the species curation tables store the
        keyword's spelling rather than its id, so a row renamed without this
        leaves an unsynced ``keyword_add`` writing the old word into XMP and
        drops starred photos out of the highlight/life-list queries, which
        compare those strings exact against ``keywords.name``.

        Scoped to photos that actually carry ``keyword_id`` (and, for pending
        changes, the workspaces those (photo, keyword) tags belong to), so a
        separate same-spelling keyword row elsewhere in the DB is never
        rewritten by side effect. Safe to call either side of the
        ``keywords`` UPDATE: only ``photo_keywords`` is read, and a name
        change does not touch it. Caller commits.
        """
        if not old_name or not new_name or old_name == new_name:
            return
        current = self.rename_state(keyword_id)
        if current is not None and (current['is_species'] or current['type'] == 'taxonomy'):
            retarget_candidate_names(self.conn, old_name, new_name)
        tag_rows = self.conn.execute(
            """SELECT DISTINCT pk.photo_id, wf.workspace_id
               FROM photo_keywords pk
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE pk.keyword_id = ?""",
            (keyword_id,),
        ).fetchall()
        photo_workspace_pairs = [
            (r["photo_id"], r["workspace_id"]) for r in tag_rows
        ]
        affected_photo_ids = sorted({r["photo_id"] for r in tag_rows})
        # Retarget pending keyword_add/keyword_remove rows queued under the
        # pre-canonical spelling so a still-unsynced sidecar write can't
        # leak the legacy variant after the DB row was rewritten. A pending
        # row that would collide with an existing (photo_id, change_type,
        # new_name) row is dropped rather than duplicated, matching
        # queue_change's dedupe contract.
        if affected_photo_ids:
            for chunk in self._chunks(affected_photo_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
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
                    [old_name, *chunk, new_name],
                )
                self.conn.execute(
                    f"""UPDATE pending_changes
                        SET value = ?
                        WHERE change_type IN ('keyword_add', 'keyword_remove')
                          AND value = ?
                          AND photo_id IN ({placeholders})""",
                    [new_name, old_name, *chunk],
                )
        # Species curation tables key rows by the species name string, which
        # is compared exact against ``keywords.name``. Now that the UPDATE
        # above rewrote this row to the canonical spelling, rows still keyed
        # on the legacy spelling would drop out of the highlight/life-list
        # queries even though the tag was retained. Rename them for the same
        # old→clean mapping, scoped to the tagged (photo, workspace) pairs.
        # ``rename_photo_preferences_species`` also retargets
        # ``species_representatives`` in its scoped branch, so a separate
        # representatives rename isn't needed here.
        if photo_workspace_pairs:
            self.rename_species_highlights_species(
                old_name, new_name,
                photo_workspace_pairs=photo_workspace_pairs, _commit=False,
            )
            self.rename_photo_preferences_species(
                old_name, new_name,
                photo_workspace_pairs=photo_workspace_pairs, _commit=False,
            )


    def normalize_row_name(self, keyword_id: int) -> None:
        """Trim stray edge punctuation from a surviving keyword row name.

        Stored names are already normalized, so this is a no-op in the
        common case — it exists so the duplicate cleanup can canonicalize a
        survivor whose spelling predates normalization.
        ``_rename_keyword_dependents`` then carries the new spelling into
        every string that mirrors it.
        """
        row = self.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        if row is None:
            return
        old_name = row["name"]
        cleaned = normalize_keyword_display(old_name)
        if not cleaned or cleaned == old_name:
            return
        # A same-name row in a different dedupe boundary (another type at
        # the same parent) can still occupy the table-level
        # UNIQUE(name, parent_id) slot. The links still merge correctly;
        # keep the stored spelling unchanged in that case.
        try:
            self.conn.execute(
                "UPDATE keywords SET name = ? WHERE id = ?", (cleaned, keyword_id)
            )
        except sqlite3.IntegrityError:
            return
        # Keep every dependent name string in lockstep with the row.
        self._rename_keyword_dependents(keyword_id, old_name, cleaned)


    # -- duplicate merges ------------------------------------------------------
    #
    # ``Database._merge_duplicate_keywords_pass`` keeps its control flow (and
    # its ``self._merge_keyword_into(...)`` call) in db.py; the scope and
    # liveness reads it issues live here, unchanged.

    def merge_duplicates(self) -> int:
        """Find and merge normalized duplicate keywords in active workspace.

        Duplicates are grouped by (normalized name, parent_id, type,
        species-bearing) — name alone is not identity: the location system
        deliberately creates same-name keywords under different parents
        (Springfield under Illinois vs. Missouri), and same-name keywords of
        different types (species vs. genre) are distinct by design. Legacy
        ``type='general', is_species=1`` rows are also distinct from ordinary
        general homonyms. Merging across those slots retags photos with the
        wrong place/kind.

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
        The whole pass is all-or-nothing: an exception rolls back every
        pending merge instead of leaving a half-merged tree on the
        connection for a later unrelated commit to persist.
        Returns count of merges performed.
        """
        ws = self.workspace_id
        total_merged = 0
        try:
            total_merged = self._merge_duplicate_keywords_pass(ws)
        except Exception:
            self.conn.rollback()
            raise
        if total_merged:
            self.conn.commit()
        return total_merged


    def duplicate_scope_rows(self, ws: int) -> list[sqlite3.Row]:
        """Keywords tagged on a photo in workspace ``ws``, plus their ancestors.

        The candidate set for one ``merge_duplicate_keywords`` pass.
        """
        return self.conn.execute(
            """WITH RECURSIVE
                   tagged AS (
                       SELECT DISTINCT pk.keyword_id AS id
                       FROM photo_keywords pk
                       JOIN photos p ON p.id = pk.photo_id
                       JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                       WHERE wf.workspace_id = ?
                   ),
                   in_scope AS (
                       SELECT id FROM tagged
                       UNION
                       SELECT k.parent_id
                       FROM keywords k
                       JOIN in_scope s ON s.id = k.id
                       WHERE k.parent_id IS NOT NULL
                   )
                   SELECT k.id, k.name, k.parent_id, k.type, k.is_species
                   FROM keywords k
                   JOIN in_scope s ON s.id = k.id""",
            (ws,),
        ).fetchall()


    def live_ids(self, all_ids: Sequence[int]) -> set[int]:
        """The subset of ``all_ids`` that still exist as keyword rows."""
        placeholders = ",".join("?" * len(all_ids))
        return {
            row["id"] for row in self.conn.execute(
                f"SELECT id FROM keywords WHERE id IN ({placeholders})",
                all_ids,
            )
        }


    def workspace_duplicate_groups(self, workspace_id: int | None) -> list[sqlite3.Row]:
        """Rows (``lname``, ``ids``, ``names``, ``cnt``) of keywords tagged in
        ``workspace_id`` that share a merge slot: ``LOWER(name)``,
        ``parent_id``, ``type`` and species-bearing. ``workspace_id=None``
        matches nothing."""
        return self.conn.execute(
            """SELECT LOWER(k.name) as lname, GROUP_CONCAT(k.id) as ids,
                      GROUP_CONCAT(k.name, ' | ') as names, COUNT(DISTINCT k.id) as cnt
               FROM keywords k
               JOIN photo_keywords pk ON pk.keyword_id = k.id
               JOIN photos p ON p.id = pk.photo_id
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE wf.workspace_id = ?
               GROUP BY LOWER(k.name), k.parent_id, k.type,
                        CASE WHEN k.type = 'taxonomy' OR k.is_species = 1
                             THEN 1 ELSE 0 END
               HAVING COUNT(DISTINCT k.id) > 1""",
            (workspace_id,),
        ).fetchall()


    # -- delete ----------------------------------------------------------------

    def delete(self, keyword_id: int) -> None:
        """Delete one keyword and commit.

        Its children become root keywords and its photo associations are
        removed. Statements the caller left uncommitted share the commit.
        """
        self.conn.execute("UPDATE keywords SET parent_id = NULL WHERE parent_id = ?", (keyword_id,))
        self.conn.execute("DELETE FROM photo_keywords WHERE keyword_id = ?", (keyword_id,))
        self.conn.execute("DELETE FROM keywords WHERE id = ?", (keyword_id,))
        self.conn.commit()
