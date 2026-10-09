"""Persistence for workspace↔folder membership.

The tables here are ``workspace_folders`` (which folders a workspace sees,
and which of them are user-facing roots) and ``workspace_folder_removals``
(read through the ``workspace_removed_folders`` view), plus the
workspace-scoped rows that follow a folder when it moves to another
workspace, and the photo-visibility read behind
``Database._photo_in_workspace``. It also reads a local session's folder ids
from ``local_folder_mappings`` and unlinks or transfers exactly those rows
(no subtree walk, the caller commits) for the folder routes that sweep a
staged descendant session the ``folders.path`` walk can't see. Every method
takes the workspace id explicitly, so the repository is not bound to the
active workspace.

Callers reach the single-statement reads and writes as
``db.workspace_folders`` (a fresh repository per access, see
``Database.workspace_folders``). ``Database`` keeps the composition: subtree
discovery (``_folder_subtree_ids_by_path``, ``_local_source_descendant_ids``),
the removal-set lookup that decides which descendants to link, workspace
existence checks, and the new-images cache invalidation all run in the façade
methods, so monkeypatches of those ``Database`` methods still apply. It also
keeps the reads that default to the active workspace
(``_photo_in_workspace``, ``get_workspace_visible_folder_ids``) and the
merge's ``root_ancestor_exists`` / ``root_descendant_exists`` callbacks.
"""

import sqlite3
from collections.abc import Callable, Iterable, Sequence

from repositories.collections import COMPANION_EXTENSION_SQL


class WorkspaceFolderRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        path_for_subtree_match: Callable[[str], str],
        chunk_size: int = 800,
    ) -> None:
        self.conn = conn
        # ``db._path_for_subtree_match``: folds ``\\`` to ``/`` and strips
        # trailing slashes, passed in so repositories import no ``db`` code.
        self.path_for_subtree_match = path_for_subtree_match
        self.chunk_size = chunk_size

    def commit(self) -> None:
        """Commit the connection's open transaction."""
        self.conn.commit()

    # -- photo visibility ----------------------------------------------------

    def photo_in_workspace(self, photo_id: int, workspace_id: int) -> bool:
        """True if the photo's folder is linked to ``workspace_id``."""
        row = self.conn.execute(
            """SELECT 1 FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               WHERE p.id = ? AND wf.workspace_id = ?""",
            (photo_id, workspace_id),
        ).fetchone()
        return row is not None

    # -- linking -------------------------------------------------------------

    def add_no_commit(
        self, workspace_id: int, folder_id: int, folder_ids: Iterable[int], *,
        is_root: bool = True, restore_removed: bool = False,
    ) -> None:
        """Link ``folder_ids`` (``folder_id``'s subtree) without committing."""
        self.conn.executemany(
            """INSERT OR IGNORE INTO workspace_folders
               (workspace_id, folder_id, is_root)
               SELECT ?, ?, 0 WHERE ? OR NOT EXISTS (
                   SELECT 1 FROM workspace_removed_folders
                   WHERE workspace_id = ? AND folder_id = ?
               )""",
            [(workspace_id, fid, restore_removed or fid == folder_id, workspace_id, fid)
             for fid in folder_ids],
        )
        if is_root:
            for chunk in self._chunks(folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"""UPDATE workspace_folders
                        SET is_root = CASE WHEN folder_id = ? THEN 1 ELSE 0 END
                        WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                    [folder_id, workspace_id] + chunk,
                )

    def add_exact(self, workspace_id: int, folder_id: int, *, is_root: bool = False) -> None:
        """Link exactly one folder, without its descendants, and commit."""
        self.conn.execute(
            """INSERT OR IGNORE INTO workspace_folders
               (workspace_id, folder_id, is_root) VALUES (?, ?, ?)""",
            (workspace_id, folder_id, 1 if is_root else 0),
        )
        if is_root:
            self.conn.execute(
                """UPDATE workspace_folders SET is_root = 1
                   WHERE workspace_id = ? AND folder_id = ?""",
                (workspace_id, folder_id),
            )
        self.conn.commit()

    def mark_roots(self, workspace_id: int, folder_ids: Iterable[int] | None) -> None:
        """Mark specific linked folders as user-facing roots."""
        if not folder_ids:
            return
        for chunk in self._chunks(folder_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"""UPDATE workspace_folders
                    SET is_root = 1
                    WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                [workspace_id] + chunk,
            )
        self.conn.commit()

    # -- removals ------------------------------------------------------------

    def removed_ids(self, workspace_id: int) -> set[int]:
        """Ids of the folders explicitly removed from the workspace."""
        return {
            row["folder_id"] for row in self.conn.execute(
                "SELECT folder_id FROM workspace_removed_folders WHERE workspace_id = ?",
                (workspace_id,),
            )
        }

    def removal_root_ids(self, folder_ids: Iterable[int]) -> set[int]:
        """Find topmost surviving paths without walking every subtree again."""
        paths = []
        for chunk in self._chunks(folder_ids):
            placeholders = ",".join("?" for _ in chunk)
            paths.extend(
                (self.path_for_subtree_match(row["path"]), row["id"])
                for row in self.conn.execute(
                    f"""SELECT f.id, COALESCE(m.source_path, f.path) AS path
                        FROM folders f
                        LEFT JOIN local_folder_mappings m ON m.folder_id = f.id
                        WHERE f.id IN ({placeholders})""", chunk,
                )
            )
        roots = set()
        root_paths = set()
        for path, folder_id in sorted(paths):
            ancestor = path
            while ancestor not in root_paths and "/" in ancestor:
                ancestor = ancestor.rpartition("/")[0]
            if ancestor not in root_paths:
                roots.add(folder_id)
                root_paths.add(path)
        return roots

    def remember_removals(
        self, workspace_id: int, folder_ids: Sequence[int], roots: Iterable[int], *,
        recursive: bool = False,
    ) -> None:
        """Record removals in the caller's unlink/delete transaction."""
        # Keep exact descendant records so importing just one folder does
        # not restore its children. Only topmost surviving folders need a
        # recursive record to cover future discoveries.
        self.conn.executemany(
            """INSERT INTO workspace_folder_removals (workspace_id, folder_id, recursive)
               SELECT ?, id, ? FROM folders WHERE id = ?
               ON CONFLICT(workspace_id, folder_id) DO UPDATE
               SET recursive = CASE WHEN ? THEN excluded.recursive
                                    ELSE MAX(recursive, excluded.recursive) END""",
            [(workspace_id, fid in roots, fid, recursive) for fid in folder_ids],
        )

    def remove(self, workspace_id: int, folder_id: int) -> None:
        """Unlink a single folder and commit."""
        self.conn.execute(
            "DELETE FROM workspace_photos WHERE workspace_id = ? AND photo_id IN "
            "(SELECT id FROM photos WHERE folder_id = ?)", (workspace_id, folder_id),
        )
        self.conn.execute(
            "DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (workspace_id, folder_id),
        )
        self.conn.commit()

    def local_session_folder_ids(self, root_folder_id: int) -> list[int]:
        """Ids of every folder in the local session rooted at ``root_folder_id``.

        Read from ``local_folder_mappings``, whose rows keep a staged
        folder's original ``source_path`` after ``folders.path`` is rebased
        under ``local-folders/``.
        """
        rows = self.conn.execute(
            "SELECT folder_id FROM local_folder_mappings WHERE root_folder_id = ?",
            (root_folder_id,),
        ).fetchall()
        return [int(row["folder_id"]) for row in rows]

    def unlink_exact_no_commit(self, workspace_id: int, folder_ids: Iterable[int]) -> None:
        """Delete exactly these ``workspace_folders`` rows, without committing.

        No subtree walk and no ``workspace_photos`` cleanup; one statement
        per folder.
        """
        for folder_id in folder_ids:
            self.conn.execute(
                "DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
                (workspace_id, folder_id),
            )

    def transfer_exact_no_commit(
        self, source_workspace_id: int, target_workspace_id: int, folder_ids: Iterable[int],
    ) -> None:
        """Move exactly these folder links to another workspace, without committing.

        For each folder in turn, its ``source_workspace_id`` row is deleted
        and a non-root ``target_workspace_id`` row inserted unless one is
        already there. No subtree walk.
        """
        for folder_id in folder_ids:
            self.conn.execute(
                "DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
                (source_workspace_id, folder_id),
            )
            self.conn.execute(
                """INSERT OR IGNORE INTO workspace_folders
                       (workspace_id, folder_id, is_root)
                   VALUES (?, ?, 0)""",
                (target_workspace_id, folder_id),
            )

    def remove_tree(self, workspace_id: int, folder_ids: Sequence[int]) -> None:
        """Unlink ``folder_ids`` (a folder's subtree) and commit."""
        for chunk in self._chunks(folder_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"DELETE FROM workspace_photos WHERE workspace_id = ? AND photo_id IN "
                f"(SELECT id FROM photos WHERE folder_id IN ({placeholders}))", [workspace_id] + chunk,
            )
            self.conn.execute(
                f"""DELETE FROM workspace_folders
                    WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                [workspace_id] + chunk,
            )
        self.conn.commit()

    # -- descendant materialization --------------------------------------------

    def unlinked_descendant_ids(self, workspace_id: int) -> set[int]:
        """Known path descendants of linked folders that are not linked yet."""
        rows = self.conn.execute(
            """SELECT DISTINCT child.id
               FROM workspace_folders wf
               JOIN folders root ON root.id = wf.folder_id
               JOIN folders child
                 ON child.path = root.path
                 OR substr(
                      REPLACE(child.path, '\\', '/'),
                      1,
                      length(RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/')
                    ) = RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/'
               LEFT JOIN workspace_folders existing
                 ON existing.workspace_id = wf.workspace_id
                AND existing.folder_id = child.id
               WHERE wf.workspace_id = ?
                 AND existing.folder_id IS NULL""",
            (workspace_id,),
        ).fetchall()
        return {r["id"] for r in rows}

    def linked_paths(self, workspace_id: int) -> list[str]:
        """``folders.path`` of every folder linked to the workspace."""
        root_rows = self.conn.execute(
            """SELECT f.path FROM workspace_folders wf
               JOIN folders f ON f.id = wf.folder_id
               WHERE wf.workspace_id = ?""",
            (workspace_id,),
        ).fetchall()
        return [root_row["path"] for root_row in root_rows]

    def linked_ids(self, workspace_id: int) -> set[int]:
        return {
            r["folder_id"]
            for r in self.conn.execute(
                "SELECT folder_id FROM workspace_folders WHERE workspace_id = ?",
                (workspace_id,),
            ).fetchall()
        }

    def link_descendants(self, workspace_id: int, candidate_ids: Iterable[int]) -> None:
        """Link discovered descendants as non-roots and commit."""
        # Recheck in the INSERT: a Remove request may have committed after
        # the discovery snapshot. In that case the stale reader must not
        # recreate the link and clear its removal record via the trigger.
        self.conn.executemany(
            """INSERT OR IGNORE INTO workspace_folders
               (workspace_id, folder_id, is_root)
               SELECT ?, ?, 0 WHERE NOT EXISTS (
                   SELECT 1 FROM workspace_removed_folders
                   WHERE workspace_id = ? AND folder_id = ?
               )""",
            [(workspace_id, fid, workspace_id, fid) for fid in candidate_ids],
        )
        self.conn.commit()

    # -- reads -------------------------------------------------------------------

    def list_folders(self, workspace_id: int) -> list[sqlite3.Row]:
        """Return the folder rows linked to the workspace, ordered by path."""
        return self.conn.execute(
            """SELECT f.* FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ?
               ORDER BY f.path""",
            (workspace_id,),
        ).fetchall()

    def list_workspaces_for_folder(self, folder_id: int) -> list[sqlite3.Row]:
        """Return every workspace in which ``folder_id`` is visible.

        Include direct links plus read-only inheritance from recursive roots.
        Do not materialize the inferred descendant row: some import and repair
        paths create deliberately restricted exact non-root links that must not
        expand merely because the user inspected a folder's memberships.
        """
        return self.conn.execute(
            """WITH folder_links AS (SELECT w.id, w.name,
                      MAX(CASE
                            WHEN wf.folder_id = target.id AND wf.is_root = 1
                            THEN 1 ELSE 0
                          END) AS is_root
               FROM workspaces w
               JOIN workspace_folders wf ON wf.workspace_id = w.id
               JOIN folders root ON root.id = wf.folder_id
               JOIN folders target ON target.id = ?
               LEFT JOIN local_folder_mappings target_lfm
                 ON target_lfm.folder_id = target.id
               WHERE (wf.folder_id = target.id
                  OR (
                    wf.is_root = 1
                    AND (
                      REPLACE(target.path, '\\', '/') = REPLACE(root.path, '\\', '/')
                      OR substr(
                           REPLACE(target.path, '\\', '/'),
                           1,
                           length(RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/')
                         ) = RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/'
                      OR REPLACE(target_lfm.source_path, '\\', '/') = REPLACE(root.path, '\\', '/')
                      OR substr(
                           REPLACE(target_lfm.source_path, '\\', '/'),
                           1,
                           length(RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/')
                         ) = RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/'
                    )
                  )
               ) AND NOT EXISTS (
                   SELECT 1 FROM workspace_removed_folders removed
                   WHERE removed.workspace_id = w.id AND removed.folder_id = target.id
               )
               GROUP BY w.id, w.name, w.pinned_at
               )
               SELECT w.id, w.name, MAX(m.is_root) AS is_root
               FROM workspaces w JOIN (
                   SELECT id, is_root FROM folder_links
                   UNION ALL
                   SELECT wp.workspace_id, 0 FROM workspace_photos wp
                   JOIN photos p ON p.id = wp.photo_id WHERE p.folder_id = ?
               ) m ON m.id = w.id
               GROUP BY w.id, w.name, w.pinned_at
               ORDER BY (w.pinned_at IS NULL), LOWER(w.name), w.id""",
            (folder_id, folder_id),
        ).fetchall()

    def has_folder_link(self, workspace_id: int, folder_id: int) -> bool:
        """True iff ``workspace_id`` has a real or inherited folder link.

        A real folder link is a ``workspace_folders`` row for the exact
        folder, or a recursive-root row for an ancestor whose path (or
        rebased ``local_folder_mappings.source_path``) contains the target.
        ``workspace_photos`` grants, which make
        :meth:`list_workspaces_for_folder` report the workspace as
        associated, are deliberately excluded. Folder-wide mutations like
        relocate must gate on this stricter view so a workspace holding only
        a photo-specific grant cannot rewrite paths for the hidden sibling
        photos owned by other workspaces.
        """
        row = self.conn.execute(
            """SELECT 1
               FROM workspace_folders wf
               JOIN folders root ON root.id = wf.folder_id
               JOIN folders target ON target.id = ?
               LEFT JOIN local_folder_mappings target_lfm
                 ON target_lfm.folder_id = target.id
               WHERE wf.workspace_id = ?
                 AND (wf.folder_id = target.id
                    OR (
                      wf.is_root = 1
                      AND (
                        REPLACE(target.path, '\\', '/') = REPLACE(root.path, '\\', '/')
                        OR substr(
                             REPLACE(target.path, '\\', '/'),
                             1,
                             length(RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/')
                           ) = RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/'
                        OR REPLACE(target_lfm.source_path, '\\', '/') = REPLACE(root.path, '\\', '/')
                        OR substr(
                             REPLACE(target_lfm.source_path, '\\', '/'),
                             1,
                             length(RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/')
                           ) = RTRIM(REPLACE(root.path, '\\', '/'), '/') || '/'
                      )
                    )
                 )
                 AND NOT EXISTS (
                     SELECT 1 FROM workspace_removed_folders removed
                     WHERE removed.workspace_id = wf.workspace_id
                       AND removed.folder_id = target.id
                 )
               LIMIT 1""",
            (folder_id, workspace_id),
        ).fetchone()
        return row is not None

    def has_direct_link(self, workspace_id: int | None, folder_id: int) -> bool:
        """True iff ``workspace_folders`` has a row for exactly this folder.

        No inheritance from a recursive root and no photo-only grants: only
        the folder's own membership row counts. A ``None`` workspace matches
        nothing.
        """
        row = self.conn.execute(
            "SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (workspace_id, folder_id),
        ).fetchone()
        return row is not None

    def visible_ids(self, workspace_id: int | None, folder_ids: Iterable[int]) -> set[int]:
        """The subset of ``folder_ids`` the workspace sees, as a set.

        Reads the ``workspace_visible_folders`` view (real links plus
        photo-only grants), one ``IN`` statement per chunk.
        """
        visible = set()
        for chunk in self._chunks(folder_ids):
            marks = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"SELECT folder_id FROM workspace_visible_folders "
                f"WHERE workspace_id = ? AND folder_id IN ({marks})",
                [workspace_id] + list(chunk),
            )
            visible.update(r["folder_id"] for r in rows)
        return visible

    def root_ids(self, workspace_id: int) -> list[int]:
        """Return the ids of the workspace's user-facing roots, by path."""
        rows = self.conn.execute(
            """SELECT f.id
               FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ? AND wf.is_root = 1
               ORDER BY f.path""",
            (workspace_id,),
        ).fetchall()
        return [int(row["id"]) for row in rows]

    def roots(self, workspace_id: int) -> list[sqlite3.Row]:
        """Return real scan/storage roots with their visible photo count.

        Photo-only grants make a folder browsable, but never authorize
        whole-directory scanning or local workspace copying.
        """
        return self.conn.execute(
            """SELECT f.*, (
                   SELECT COUNT(*)
                   FROM photos p
                   JOIN folders cf ON cf.id = p.folder_id
                   JOIN photo_workspace_visibility cwf
                     ON cwf.photo_id = p.id
                    AND cwf.workspace_id = wf.workspace_id
                   LEFT JOIN local_folder_mappings lfm
                     ON lfm.folder_id = cf.id
                   WHERE cf.path = f.path
                      OR substr(
                           REPLACE(cf.path, '\\', '/'),
                           1,
                           length(RTRIM(REPLACE(f.path, '\\', '/'), '/') || '/')
                         ) = RTRIM(REPLACE(f.path, '\\', '/'), '/') || '/'
                      OR lfm.source_path = f.path
                      OR substr(
                           REPLACE(lfm.source_path, '\\', '/'),
                           1,
                           length(RTRIM(REPLACE(f.path, '\\', '/'), '/') || '/')
                         ) = RTRIM(REPLACE(f.path, '\\', '/'), '/') || '/'
               ) AS workspace_photo_count
               FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ? AND wf.is_root = 1
               ORDER BY f.path""",
            (workspace_id,),
        ).fetchall()

    def audit_root_paths(self, workspace_id: int) -> list[str]:
        """Return paths of the workspace's audit scan roots.

        An audit root is a ``workspace_folders`` row whose nearest linked
        ancestor does not exist in the active workspace, so the audit treats
        its path as a storage root to walk the filesystem under. Folders a
        workspace reaches only through ``workspace_photos`` grants are
        deliberately excluded even when they would otherwise appear
        parentless in ``get_folder_tree``, since the workspace does not own
        them as scan roots: including a grant-only folder here would let
        ``/api/audit/untracked`` enumerate hidden sibling files and
        ``/api/audit/import-untracked`` create a real ``workspace_folders``
        link that expands visibility to every photo in the directory.
        """
        rows = self.conn.execute(
            """WITH RECURSIVE
               linked(id) AS (
                   SELECT wf.folder_id FROM workspace_folders wf
                   WHERE wf.workspace_id = ?
               ),
               walk(start_id, current_id) AS (
                   SELECT l.id, f.parent_id
                   FROM linked l
                   JOIN folders f ON f.id = l.id
                   WHERE f.status IN ('ok', 'partial')
                   UNION ALL
                   SELECT w.start_id, f.parent_id
                   FROM walk w
                   JOIN folders f ON f.id = w.current_id
                   WHERE w.current_id IS NOT NULL
                     AND w.current_id NOT IN (SELECT id FROM linked)
               )
               SELECT DISTINCT f.path
               FROM folders f
               JOIN walk w ON w.start_id = f.id
               WHERE w.current_id IS NULL
               ORDER BY f.path""",
            (workspace_id,),
        ).fetchall()
        return [row["path"] for row in rows]

    def extensions(self, workspace_id: int) -> list[str]:
        """Distinct lowercased extensions of the workspace's visible photos.

        A RAW+JPEG pair contributes its companion's extension too, since the
        extension rule matches a photo by either file.
        """
        rows = self.conn.execute(
            f"""SELECT DISTINCT CASE ext_side.side
                       WHEN 0 THEN LOWER(p.extension)
                       ELSE {COMPANION_EXTENSION_SQL} END AS ext
               FROM photos p
               JOIN photo_workspace_visibility wf ON wf.photo_id = p.id
               JOIN folders f ON f.id = p.folder_id
                              AND f.status IN ('ok', 'partial')
               JOIN (SELECT 0 AS side UNION ALL SELECT 1) ext_side
               WHERE wf.workspace_id = ?
                 AND COALESCE(ext, '') != ''
               ORDER BY ext""",
            (workspace_id,),
        ).fetchall()
        return [r["ext"] for r in rows]

    def unlinked_folder_count(self, workspace_id: int, unique: Sequence[str]) -> int:
        """Count paths in ``unique`` not linked to the workspace."""
        BATCH = 800
        linked = set()
        for i in range(0, len(unique), BATCH):
            chunk = unique[i:i + BATCH]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT f.path
                    FROM folders f
                    JOIN workspace_folders wf
                      ON wf.folder_id = f.id AND wf.workspace_id = ?
                    WHERE f.path IN ({placeholders})""",
                (workspace_id, *chunk),
            ).fetchall()
            for r in rows:
                linked.add(r["path"])
        return len(unique) - len(linked)

    # -- moving folders between workspaces ---------------------------------------

    def move_folders(
        self, source_ws_id: int, target_ws_id: int, folder_ids: Iterable[int],
        moved_folder_ids: Sequence[int],
    ) -> tuple[int, int, int]:
        """Move ``moved_folder_ids`` and their workspace-scoped rows, then commit.

        ``folder_ids`` are the user-selected folders (they become roots in
        the target); ``moved_folder_ids`` are those plus their linked
        descendants. Rolls back and re-raises on any failure. Returns
        ``(pending_changes_moved, photo_preferences_moved,
        species_highlights_moved)``.
        """
        try:
            # Move pending_changes
            pending_changes_moved = 0
            for chunk in self._chunks(moved_folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                cur = self.conn.execute(
                    f"""UPDATE pending_changes SET workspace_id = ?
                        WHERE workspace_id = ?
                        AND photo_id IN (SELECT id FROM photos WHERE folder_id IN ({placeholders}))""",
                    [target_ws_id, source_ws_id] + chunk,
                )
                pending_changes_moved += cur.rowcount

            # Move prediction_review rows for predictions whose photo is in
            # the moved folders. Without this the accepted/rejected/group
            # metadata stays attached to the source workspace_id and the
            # target reads all predictions as 'pending' — silently dropping
            # the user's review decisions during a folder move.
            #
            # INSERT OR IGNORE into the target first, then DELETE from the
            # source. That way if the target already has a review row for
            # the same (prediction_id), we keep the target's value rather
            # than overwriting it.
            for chunk in self._chunks(moved_folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"""INSERT OR IGNORE INTO prediction_review
                          (prediction_id, workspace_id, status, reviewed_at,
                           individual, group_id, vote_count, total_votes)
                        SELECT pr_rev.prediction_id, ?, pr_rev.status,
                               pr_rev.reviewed_at, pr_rev.individual,
                               pr_rev.group_id, pr_rev.vote_count,
                               pr_rev.total_votes
                        FROM prediction_review pr_rev
                        JOIN predictions p ON p.id = pr_rev.prediction_id
                        JOIN detections d ON d.id = p.detection_id
                        WHERE pr_rev.workspace_id = ?
                          AND d.photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )""",
                    [target_ws_id, source_ws_id] + chunk,
                )
                self.conn.execute(
                    f"""DELETE FROM prediction_review
                        WHERE workspace_id = ?
                          AND prediction_id IN (
                              SELECT pr_rev.prediction_id
                              FROM prediction_review pr_rev
                              JOIN predictions p ON p.id = pr_rev.prediction_id
                              JOIN detections d ON d.id = p.detection_id
                              WHERE pr_rev.workspace_id = ?
                                AND d.photo_id IN (
                                    SELECT id FROM photos WHERE folder_id IN ({placeholders})
                                )
                          )""",
                    [source_ws_id, source_ws_id] + chunk,
                )

            # Move color labels, which are per (photo, workspace) like the
            # review rows above. A label the target already holds for the
            # photo wins; the source row is dropped either way, since the
            # source can no longer see the photo.
            for chunk in self._chunks(moved_folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"""INSERT OR IGNORE INTO photo_color_labels
                          (photo_id, workspace_id, color)
                        SELECT photo_id, ?, color
                        FROM photo_color_labels
                        WHERE workspace_id = ?
                          AND photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )""",
                    [target_ws_id, source_ws_id] + chunk,
                )
                self.conn.execute(
                    f"""DELETE FROM photo_color_labels
                        WHERE workspace_id = ?
                          AND photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )""",
                    [source_ws_id] + chunk,
                )

            # Move manually selected Life List / Highlights representative
            # photos with the folder. If the target already has a preference
            # for the same (purpose, species), keep the target value and drop
            # the now-stale source row.
            photo_preferences_moved = 0
            species_highlights_moved = 0
            for chunk in self._chunks(moved_folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                cur = self.conn.execute(
                    f"""INSERT OR IGNORE INTO photo_preferences
                          (workspace_id, purpose, species, photo_id,
                           created_at, updated_at)
                        SELECT ?, purpose, species, photo_id,
                               created_at, updated_at
                        FROM photo_preferences
                        WHERE workspace_id = ?
                          AND photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )""",
                    [target_ws_id, source_ws_id] + chunk,
                )
                photo_preferences_moved += cur.rowcount
                self.conn.execute(
                    f"""DELETE FROM photo_preferences
                        WHERE workspace_id = ?
                          AND photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )""",
                    [source_ws_id] + chunk,
                )

                # Append moved highlights after the target workspace's
                # existing rows per species. Preserving the source `rank`
                # verbatim would collide with the target's ranks (rank is
                # not part of the PK), corrupting the curated order the
                # target uses in `ORDER BY rank, created_at, photo_id`.
                src_highlights = self.conn.execute(
                    f"""SELECT species, photo_id, rank, created_at, updated_at
                        FROM species_highlights
                        WHERE workspace_id = ?
                          AND photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )
                        ORDER BY species, rank, created_at, photo_id""",
                    [source_ws_id] + chunk,
                ).fetchall()
                by_species = {}
                for src_row in src_highlights:
                    by_species.setdefault(src_row["species"], []).append(src_row)
                for sp, sp_rows in by_species.items():
                    next_rank = int(self.conn.execute(
                        """SELECT COALESCE(MAX(rank), 0) AS max_rank
                           FROM species_highlights
                           WHERE workspace_id = ? AND species = ?""",
                        (target_ws_id, sp),
                    ).fetchone()["max_rank"] or 0) + 1
                    for src_row in sp_rows:
                        cur = self.conn.execute(
                            """INSERT OR IGNORE INTO species_highlights
                                   (workspace_id, species, photo_id, rank,
                                    created_at, updated_at)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            (
                                target_ws_id,
                                sp,
                                src_row["photo_id"],
                                next_rank,
                                src_row["created_at"],
                                src_row["updated_at"],
                            ),
                        )
                        if cur.rowcount:
                            species_highlights_moved += 1
                            next_rank += 1
                self.conn.execute(
                    f"""DELETE FROM species_highlights
                        WHERE workspace_id = ?
                          AND photo_id IN (
                              SELECT id FROM photos WHERE folder_id IN ({placeholders})
                          )""",
                    [source_ws_id] + chunk,
                )

            # Move workspace_folders: remove from source, add to target
            for chunk in self._chunks(moved_folder_ids):
                placeholders = ",".join("?" for _ in chunk)
                # Moving ownership removes both kinds of source membership.
                # Other workspaces' grants and all target sharing stay intact.
                self.conn.execute(
                    f"DELETE FROM workspace_photos WHERE workspace_id = ? AND photo_id IN "
                    f"(SELECT id FROM photos WHERE folder_id IN ({placeholders}))",
                    [source_ws_id] + chunk,
                )
                self.conn.execute(
                    f"""DELETE FROM workspace_folders
                        WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                    [source_ws_id] + chunk,
                )
            self.conn.executemany(
                """INSERT OR IGNORE INTO workspace_folders
                   (workspace_id, folder_id, is_root) VALUES (?, ?, 0)""",
                [(target_ws_id, fid) for fid in moved_folder_ids],
            )
            for chunk in self._chunks(folder_ids):
                selected_placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"""UPDATE workspace_folders
                        SET is_root = 1
                        WHERE workspace_id = ?
                          AND folder_id IN ({selected_placeholders})""",
                    [target_ws_id] + chunk,
                )

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        return pending_changes_moved, photo_preferences_moved, species_highlights_moved

    # -- merge helpers: root ancestry and pruning --------------------------------

    def root_ancestor_exists(self, workspace_id: int, path: str) -> bool:
        """True if the workspace has a root equal to or above ``path``."""
        target = self.path_for_subtree_match(path)
        rows = self.conn.execute(
            """SELECT f.path FROM workspace_folders wf
               JOIN folders f ON f.id = wf.folder_id
               WHERE wf.workspace_id = ? AND wf.is_root = 1""",
            (workspace_id,),
        ).fetchall()
        for r in rows:
            root = self.path_for_subtree_match(r["path"])
            if target == root or target.startswith(root + "/"):
                return True
        return False

    def root_descendant_exists(self, workspace_id: int, path: str) -> bool:
        """True if the workspace has a strict root descendant of ``path``."""
        target = self.path_for_subtree_match(path)
        rows = self.conn.execute(
            """SELECT f.path FROM workspace_folders wf
               JOIN folders f ON f.id = wf.folder_id
               WHERE wf.workspace_id = ? AND wf.is_root = 1""",
            (workspace_id,),
        ).fetchall()
        prefix = target + "/"
        for r in rows:
            root = self.path_for_subtree_match(r["path"])
            if root.startswith(prefix):
                return True
        return False

    def prune_nonroot_links_outside_roots(self, workspace_id: int, path: str) -> list[int]:
        """Drop uncovered non-root links on ``path``'s ancestry or subtree.

        Commits only when something was pruned; returns the pruned ids.
        """
        target = self.path_for_subtree_match(path)
        roots = [
            self.path_for_subtree_match(r["path"])
            for r in self.conn.execute(
                """SELECT f.path FROM workspace_folders wf
                   JOIN folders f ON f.id = wf.folder_id
                   WHERE wf.workspace_id = ? AND wf.is_root = 1""",
                (workspace_id,),
            ).fetchall()
        ]
        rows = self.conn.execute(
            """SELECT wf.folder_id, f.path FROM workspace_folders wf
               JOIN folders f ON f.id = wf.folder_id
               WHERE wf.workspace_id = ? AND wf.is_root = 0""",
            (workspace_id,),
        ).fetchall()
        target_prefix = target + "/"
        prune_ids = []
        for row in rows:
            current = self.path_for_subtree_match(row["path"])
            current_prefix = current + "/"
            if (current != target
                    and not current.startswith(target_prefix)
                    and not target.startswith(current_prefix)):
                continue
            if any(current == root or current.startswith(root + "/")
                   for root in roots):
                continue
            prune_ids.append(row["folder_id"])

        for chunk in self._chunks(prune_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"""DELETE FROM workspace_folders
                    WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                [workspace_id] + chunk,
            )
        if prune_ids:
            self.conn.commit()
        return prune_ids

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
