"""Persistence behind ``keyword_identity``: import aliases, identity groups, merges.

``keyword_identity`` keeps the logic of reviewed keyword identities and the
explicit keyword and location merges (planning, the preview token, which
sidecar changes each merge queues); the statements it runs live here and are
reached through the ``db.keyword_merges`` accessor, which builds a fresh
repository per access. The SQL text and parameter order are unchanged from
that module.

This module writes no ``photo_keywords`` association: the merges move tags
through ``Database._merge_keyword_into``, which
``test_keyword_provenance_contract`` keys to ``repositories/keyword_provenance.py``.
The writes here are the durable ``keyword_import_aliases`` rows, the
survivor's chosen identity fields, and the no-op write that reserves the
writer before a merge revalidates its preview. Nothing here commits; the
merges run inside the caller's ``db.transaction()``.
"""

from repositories.keyword_identity_sql import identity_sql


class KeywordMergeRepository:
    def __init__(self, conn):
        self.conn = conn

    # -- keyword rows ---------------------------------------------------------

    def all_rows(self):
        """Every keyword row, whole, in id order."""
        return self.conn.execute('SELECT * FROM keywords ORDER BY id').fetchall()

    def tree_rows(self):
        """``id``, ``name``, ``parent_id`` of every keyword."""
        return self.conn.execute('SELECT id, name, parent_id FROM keywords').fetchall()

    def candidate_rows(self):
        """The columns the location-match preview reads, for every keyword."""
        return self.conn.execute(
            'SELECT id, name, parent_id, type, is_species, place_id FROM keywords'
        ).fetchall()

    def name_and_parent(self, keyword_id):
        """``name`` and ``parent_id`` of one keyword, or None."""
        return self.conn.execute(
            'SELECT name, parent_id FROM keywords WHERE id = ?', (keyword_id,),
        ).fetchone()

    def parent_of(self, keyword_id):
        """The keyword's row holding only ``parent_id``, or None."""
        return self.conn.execute(
            'SELECT parent_id FROM keywords WHERE id = ?', (keyword_id,),
        ).fetchone()

    def type_and_place(self, keyword_id):
        """``type`` and ``place_id`` of one keyword, or None."""
        return self.conn.execute(
            'SELECT type, place_id FROM keywords WHERE id = ?', (keyword_id,),
        ).fetchone()

    def type_of(self, keyword_id):
        """The keyword's row holding only ``type``, or None."""
        return self.conn.execute(
            'SELECT type FROM keywords WHERE id = ?', (keyword_id,),
        ).fetchone()

    def ids_and_names_of_type(self, kw_type):
        """``id`` and ``name`` of every keyword of ``kw_type`` (any type when None)."""
        return self.conn.execute(
            'SELECT id, name FROM keywords WHERE (? IS NULL OR type = ?)', (kw_type, kw_type),
        ).fetchall()

    def identities(self):
        """``id`` and ``identity`` (``identity_sql``) of every keyword."""
        return self.conn.execute(
            f'SELECT k.id, {identity_sql()} AS identity FROM keywords k'
        ).fetchall()

    def identity_photo_counts(self, workspace_id):
        """Per identity, the photos ``workspace_id`` sees under any member's subtree.

        Rows carry ``identity``, ``photo_count`` (the union over every member's
        subtree, so overlapping photos count once) and ``direct_photo_count``
        (photos tagged with a member itself).
        """
        return self.conn.execute(
            f"""WITH RECURSIVE descendants(identity, id) AS (
            SELECT {identity_sql()}, k.id FROM keywords k
            UNION
            SELECT d.identity, k.id FROM descendants d
            JOIN keywords k ON k.parent_id = d.id
        )
        SELECT d.identity, COUNT(DISTINCT pk.photo_id) AS photo_count,
               COUNT(DISTINCT CASE WHEN ({identity_sql()}) = d.identity
                 THEN pk.photo_id END) AS direct_photo_count
        FROM descendants d JOIN photo_keywords pk ON pk.keyword_id = d.id
        JOIN keywords k ON k.id = pk.keyword_id
        JOIN photos p ON p.id = pk.photo_id
        JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
        WHERE wf.workspace_id = ? GROUP BY d.identity""", (workspace_id,),
        ).fetchall()

    def identity_fields(self, keyword_id):
        """The fields a merge's chooser can set, as the survivor holds them now."""
        return self.conn.execute(
            'SELECT name, parent_id, type, place_id, taxon_id, source_taxon_id, '
            'latitude, longitude, is_species FROM keywords WHERE id = ?', (keyword_id,),
        ).fetchone()

    def set_fields(self, keyword_id, changed):
        """Write ``changed`` (column -> value) onto one keyword; no commit.

        The column names come from the merge's fixed field list, never from
        the request.
        """
        assignments = ', '.join(f'{field} = ?' for field in changed)
        self.conn.execute(f'UPDATE keywords SET {assignments} WHERE id = ?',
                          [*changed.values(), keyword_id])

    # -- import aliases ---------------------------------------------------------

    def alias_target(self, path_key, kw_type, linked_locations_only):
        """The keyword an import path's alias names, filtered by type and link, or None."""
        return self.conn.execute(
            "SELECT a.keyword_id FROM keyword_import_aliases a "
            "JOIN keywords k ON k.id = a.keyword_id "
            "WHERE a.path_key = ? AND (? IS NULL OR k.type = ?) "
            "AND (? = 0 OR (k.type = 'location' AND k.place_id IS NOT NULL))",
            (path_key, kw_type, kw_type, linked_locations_only),
        ).fetchone()

    def alias_paths_of_type(self, kw_type):
        """``keyword_id`` and ``path_json`` of every alias onto a ``kw_type`` keyword."""
        return self.conn.execute(
            'SELECT a.keyword_id, a.path_json FROM keyword_import_aliases a '
            'JOIN keywords k ON k.id = a.keyword_id WHERE (? IS NULL OR k.type = ?)',
            (kw_type, kw_type),
        ).fetchall()

    def alias_keys_and_names(self):
        """``path_key`` and the target keyword's ``name`` for every alias."""
        return self.conn.execute(
            'SELECT a.path_key, k.name FROM keyword_import_aliases a '
            'JOIN keywords k ON k.id = a.keyword_id'
        ).fetchall()

    def alias_paths(self):
        """``keyword_id`` and ``path_json`` of every alias."""
        return self.conn.execute(
            'SELECT keyword_id, path_json FROM keyword_import_aliases'
        ).fetchall()

    def alias_owner(self, path_key):
        """The alias row (``keyword_id``) recorded for ``path_key``, or None."""
        return self.conn.execute(
            'SELECT keyword_id FROM keyword_import_aliases WHERE path_key = ?', (path_key,),
        ).fetchone()

    def set_alias(self, path_key, path_json, keyword_id):
        """Point ``path_key`` at ``keyword_id`` (insert or replace); no commit."""
        self.conn.execute(
            'INSERT OR REPLACE INTO keyword_import_aliases(path_key, path_json, keyword_id) '
            'VALUES (?, ?, ?)',
            (path_key, path_json, keyword_id),
        )

    def reserve_writer(self):
        """Take the write lock with a no-op write, before revalidating a preview."""
        self.conn.execute('UPDATE db_meta SET value = value WHERE 0')

    # -- tags -------------------------------------------------------------------

    def pending_merge_values(self, photo_id):
        """The ``value`` of every queued ``keyword_merge`` change for one photo."""
        return self.conn.execute(
            "SELECT value FROM pending_changes WHERE photo_id = ? AND change_type = 'keyword_merge'",
            (photo_id,),
        ).fetchall()

    def linked_locations_on_photo(self, photo_id):
        """``id`` and ``name`` of the place-linked location keywords on one photo."""
        return self.conn.execute(
            "SELECT k.id, k.name FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location' AND k.place_id IS NOT NULL",
            (photo_id,),
        ).fetchall()

    def keyword_names_on_photo(self, photo_id):
        """``name`` of every keyword on one photo."""
        return self.conn.execute(
            'SELECT k.name FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id '
            'WHERE pk.photo_id = ?', (photo_id,),
        ).fetchall()

    def other_keyword_names_on_photo(self, photo_id, keyword_id):
        """``name`` of every keyword on one photo except ``keyword_id``."""
        return self.conn.execute(
            'SELECT k.name FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id '
            'WHERE pk.photo_id = ? AND pk.keyword_id != ?', (photo_id, keyword_id),
        ).fetchall()

    def photos_with_rival_places(self, source_id, same_chain):
        """Photos tagged ``source_id`` that also carry a linked place outside ``same_chain``."""
        placeholders = ','.join('?' for _ in same_chain)
        return self.conn.execute(
            f"""SELECT COUNT(DISTINCT source.photo_id) FROM photo_keywords source
                JOIN photo_keywords other ON other.photo_id = source.photo_id
                JOIN keywords k ON k.id = other.keyword_id
                WHERE source.keyword_id = ? AND k.type = 'location'
                  AND k.place_id IS NOT NULL AND k.id NOT IN ({placeholders})""",
            (source_id, *sorted(same_chain)),
        ).fetchone()[0]

    def pair_photo_counts(self, source_id, target_id):
        """``source_count`` (photos on ``source_id``) and ``combined_count`` (on either)."""
        return self.conn.execute(
            """SELECT COUNT(DISTINCT CASE WHEN keyword_id = ? THEN photo_id END) AS source_count,
                      COUNT(DISTINCT photo_id) AS combined_count
               FROM photo_keywords WHERE keyword_id IN (?, ?)""",
            (source_id, source_id, target_id),
        ).fetchone()

    def location_tags_of_touched_photos(self, keyword_ids):
        """``photo_id``, ``keyword_id`` of the location tags (and the ``keyword_ids``
        tags) on every photo carrying one of ``keyword_ids``."""
        placeholders = ','.join('?' for _ in keyword_ids)
        return self.conn.execute(
            f"""WITH touched AS (
                SELECT DISTINCT photo_id FROM photo_keywords
                WHERE keyword_id IN ({placeholders})
            )
            SELECT pk.photo_id, pk.keyword_id FROM photo_keywords pk
            JOIN touched t ON t.photo_id = pk.photo_id
            JOIN keywords k ON k.id = pk.keyword_id
            WHERE k.type = 'location' OR pk.keyword_id IN ({placeholders})""",
            [*keyword_ids, *keyword_ids],
        ).fetchall()

    def subtree_tags(self, keyword_ids):
        """Every ``photo_keywords`` row (``photo_id``, ``keyword_id``, ``source``)
        under the keywords' subtrees, ordered by photo then keyword."""
        placeholders = ','.join('?' for _ in keyword_ids)
        return self.conn.execute(
            f'''WITH RECURSIVE descendants(id) AS (
                SELECT id FROM keywords WHERE id IN ({placeholders})
                UNION
                SELECT k.id FROM keywords k JOIN descendants d ON k.parent_id = d.id
            )
            SELECT photo_id, keyword_id, source FROM photo_keywords
            WHERE keyword_id IN (SELECT id FROM descendants)
            ORDER BY photo_id, keyword_id''', keyword_ids,
        ).fetchall()

    def distinct_photo_count(self, keyword_ids):
        """How many photos carry at least one of ``keyword_ids``."""
        return self.conn.execute(
            'SELECT COUNT(DISTINCT photo_id) FROM photo_keywords WHERE keyword_id IN '
            f'({",".join("?" for _ in keyword_ids)})', keyword_ids,
        ).fetchone()[0]

    def tag_workspaces_and_types(self, keyword_ids):
        """``keyword_id``, ``photo_id``, ``workspace_id``, ``type`` for each tag of
        ``keyword_ids``, once per workspace that sees the photo."""
        placeholders = ','.join('?' for _ in keyword_ids)
        return self.conn.execute(
            'SELECT pk.keyword_id, pk.photo_id, wf.workspace_id, k.type '
            'FROM photo_keywords pk '
            'JOIN keywords k ON k.id = pk.keyword_id '
            'JOIN photos p ON p.id = pk.photo_id '
            'JOIN photo_workspace_visibility wf ON wf.photo_id = p.id '
            f'WHERE pk.keyword_id IN ({placeholders})', list(keyword_ids),
        ).fetchall()

    def tag_workspaces(self, keyword_ids):
        """``keyword_id``, ``photo_id``, ``workspace_id`` for each tag of
        ``keyword_ids``, once per workspace that sees the photo."""
        placeholders = ','.join('?' for _ in keyword_ids)
        return self.conn.execute(
            'SELECT pk.keyword_id, pk.photo_id, wf.workspace_id FROM photo_keywords pk '
            'JOIN photos p ON p.id = pk.photo_id '
            'JOIN photo_workspace_visibility wf ON wf.photo_id = p.id '
            f'WHERE pk.keyword_id IN ({placeholders})', list(keyword_ids),
        ).fetchall()

    def location_subtree_photo_workspaces(self, parent_id):
        """Distinct ``photo_id``, ``workspace_id`` for photos tagged with a location
        keyword anywhere below ``parent_id``."""
        return self.conn.execute(
            '''WITH RECURSIVE subtree(id) AS (
                       SELECT id FROM keywords WHERE parent_id = ?
                       UNION
                       SELECT k.id FROM keywords k
                       JOIN subtree s ON k.parent_id = s.id
                   )
                   SELECT DISTINCT pk.photo_id, wf.workspace_id
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   JOIN photos p ON p.id = pk.photo_id
                   JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
                   WHERE pk.keyword_id IN (SELECT id FROM subtree)
                     AND k.type = 'location' ''',
            (parent_id,),
        ).fetchall()

    # -- taxa -------------------------------------------------------------------

    def inat_id_of_taxon(self, taxon_id):
        """The taxon's row holding only ``inat_id``, or None."""
        return self.conn.execute(
            'SELECT inat_id FROM taxa WHERE id = ?', (taxon_id,),
        ).fetchone()

    def taxon_names_by_inat_id(self, inat_id):
        """``name`` and ``common_name`` of the taxon with this iNat id, or None."""
        return self.conn.execute(
            'SELECT name, common_name FROM taxa WHERE inat_id = ?', (inat_id,),
        ).fetchone()

    def taxon_names(self, taxon_id):
        """``name`` and ``common_name`` of one taxon, or None."""
        return self.conn.execute(
            'SELECT name, common_name FROM taxa WHERE id = ?', (taxon_id,),
        ).fetchone()
