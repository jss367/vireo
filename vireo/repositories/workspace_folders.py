"""Persistence for workspace↔folder membership.

The tables here are ``workspace_folders`` (which folders a workspace sees,
and which of them are user-facing roots) and ``workspace_folder_removals``
(read through the ``workspace_removed_folders`` view), plus the
workspace-scoped rows that follow a folder when it moves to another
workspace. Every method takes the workspace id explicitly, so the
repository is not bound to the active workspace.

``Database`` keeps the composition: subtree discovery
(``_folder_subtree_ids_by_path``, ``_local_source_descendant_ids``), the
removal-set lookup that decides which descendants to link, workspace
existence checks, and the new-images cache invalidation all run in the
façade wrappers, so monkeypatches of those ``Database`` methods still apply.
"""


class WorkspaceFolderRepository:
    def __init__(self, conn, *, path_for_subtree_match, chunk_size=800):
        self.conn = conn
        # ``db._path_for_subtree_match``: folds ``\\`` to ``/`` and strips
        # trailing slashes, passed in so repositories import no ``db`` code.
        self.path_for_subtree_match = path_for_subtree_match
        self.chunk_size = chunk_size

    def commit(self):
        self.conn.commit()

    # -- linking -------------------------------------------------------------

    def add_no_commit(self, workspace_id, folder_id, folder_ids, *,
                      is_root=True, restore_removed=False):
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

    def add_exact(self, workspace_id, folder_id, *, is_root=False):
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

    def mark_roots(self, workspace_id, folder_ids):
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

    def removed_ids(self, workspace_id):
        return {
            row["folder_id"] for row in self.conn.execute(
                "SELECT folder_id FROM workspace_removed_folders WHERE workspace_id = ?",
                (workspace_id,),
            )
        }

    def removal_root_ids(self, folder_ids):
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

    def remember_removals(self, workspace_id, folder_ids, roots, *, recursive=False):
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

    def remove(self, workspace_id, folder_id):
        """Unlink a single folder and commit."""
        self.conn.execute(
            "DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (workspace_id, folder_id),
        )
        self.conn.commit()

    def remove_tree(self, workspace_id, folder_ids):
        """Unlink ``folder_ids`` (a folder's subtree) and commit."""
        for chunk in self._chunks(folder_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"""DELETE FROM workspace_folders
                    WHERE workspace_id = ? AND folder_id IN ({placeholders})""",
                [workspace_id] + chunk,
            )
        self.conn.commit()

    # -- descendant materialization --------------------------------------------

    def unlinked_descendant_ids(self, workspace_id):
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

    def linked_paths(self, workspace_id):
        """``folders.path`` of every folder linked to the workspace."""
        root_rows = self.conn.execute(
            """SELECT f.path FROM workspace_folders wf
               JOIN folders f ON f.id = wf.folder_id
               WHERE wf.workspace_id = ?""",
            (workspace_id,),
        ).fetchall()
        return [root_row["path"] for root_row in root_rows]

    def linked_ids(self, workspace_id):
        return {
            r["folder_id"]
            for r in self.conn.execute(
                "SELECT folder_id FROM workspace_folders WHERE workspace_id = ?",
                (workspace_id,),
            ).fetchall()
        }

    def link_descendants(self, workspace_id, candidate_ids):
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

    def list_folders(self, workspace_id):
        """Return the folder rows linked to the workspace, ordered by path."""
        return self.conn.execute(
            """SELECT f.* FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ?
               ORDER BY f.path""",
            (workspace_id,),
        ).fetchall()

    def list_workspaces_for_folder(self, folder_id):
        """Return every workspace in which ``folder_id`` is visible."""
        return self.conn.execute(
            """SELECT w.id, w.name,
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
               ORDER BY (w.pinned_at IS NULL), LOWER(w.name), w.id""",
            (folder_id,),
        ).fetchall()

    def root_ids(self, workspace_id):
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

    def roots(self, workspace_id):
        """Return root folder rows with their linked-subtree photo count."""
        return self.conn.execute(
            """SELECT f.*, (
                   SELECT COUNT(*)
                   FROM photos p
                   JOIN folders cf ON cf.id = p.folder_id
                   JOIN workspace_folders cwf
                     ON cwf.folder_id = cf.id
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

    def extensions(self, workspace_id):
        """Distinct lowercased extensions of the workspace's visible photos."""
        rows = self.conn.execute(
            """SELECT DISTINCT LOWER(p.extension) AS ext
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
               JOIN folders f ON f.id = p.folder_id
                              AND f.status IN ('ok', 'partial')
               WHERE wf.workspace_id = ?
                 AND p.extension IS NOT NULL
                 AND p.extension != ''
               ORDER BY ext""",
            (workspace_id,),
        ).fetchall()
        return [r["ext"] for r in rows]

    def unlinked_folder_count(self, workspace_id, unique):
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

    def move_folders(self, source_ws_id, target_ws_id, folder_ids, moved_folder_ids):
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

    def root_ancestor_exists(self, workspace_id, path):
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

    def root_descendant_exists(self, workspace_id, path):
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

    def prune_nonroot_links_outside_roots(self, workspace_id, path):
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
