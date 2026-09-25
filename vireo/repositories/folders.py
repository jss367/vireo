"""Persistence for folders: rows, trees, parent repair, health, relocation.

``Database`` keeps the active-workspace state, the new-images cache, the
pipeline-cache prune and every call into another domain. Where a folder
method composes other ``Database`` methods mid-transaction (the workspace
link in ``add_folder``, ``delete_photos`` in ``delete_folder``, the GPS
review transfer in a merge, ``nearest_ancestor_folder_id`` in parent
repair), the façade passes the bound method in, so monkeypatches of those
methods still apply and the call happens at the same point in the SQL.
Workspace-scoped reads use ``self.workspace_id``, which the façade resolves
with ``Database._ws_id()``; catalog-wide methods take no workspace.
Workspace-folder membership lives in ``repositories/workspace_folders.py``.
"""

import os

from repositories.collections import remap_collection_photo_ids


class FolderRepository:
    def __init__(self, conn, workspace_id, *, commit_with_retry,
                 path_for_subtree_match, stored_parent_path, subtree_prefix,
                 subtree_relative, join_subtree_path, chunk_size=800):
        self.conn = conn
        self.workspace_id = workspace_id
        # ``db.commit_with_retry``, passed in so repositories import no
        # ``db`` code and a monkeypatch of the module function still applies.
        self.commit_with_retry = commit_with_retry
        # ``db``'s pure path helpers: platform-neutral subtree matching
        # (``\`` folded to ``/``), the stored parent path, and rebasing a
        # descendant path from one subtree root onto another.
        self.path_for_subtree_match = path_for_subtree_match
        self.stored_parent_path = stored_parent_path
        self.subtree_prefix = subtree_prefix
        self.subtree_relative = subtree_relative
        self.join_subtree_path = join_subtree_path
        self.chunk_size = chunk_size

    def _chunks(self, values):
        values = list(values)
        for idx in range(0, len(values), self.chunk_size):
            yield values[idx:idx + self.chunk_size]

    def add(self, path, name=None, parent_id=None):
        """Insert the folder (or backfill a missing parent_id); return its id."""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
            (path, name, parent_id),
        )
        self.commit_with_retry(self.conn)
        if cur.rowcount > 0:
            folder_id = cur.lastrowid
        else:
            row = self.conn.execute(
                "SELECT id, parent_id FROM folders WHERE path = ?", (path,)
            ).fetchone()
            folder_id = row["id"]
            if (
                parent_id is not None
                and row["parent_id"] is None
                and folder_id != parent_id
            ):
                self.conn.execute(
                    "UPDATE folders SET parent_id = ? WHERE id = ?",
                    (parent_id, folder_id),
                )
                self.commit_with_retry(self.conn)
        return folder_id

    def get(self, folder_id):
        """Return one folder row by id, or None."""
        return self.conn.execute(
            "SELECT id, path, name, parent_id, status, photo_count "
            "FROM folders WHERE id = ?",
            (folder_id,),
        ).fetchone()

    def tree(self):
        """Return the workspace's visible folders with parents rewritten to visible ancestors."""
        ws = self.workspace_id
        return self.conn.execute(
            """WITH RECURSIVE
               visible(id, is_workspace_root) AS (
                   SELECT f.id, wf.is_root FROM folders f
                   JOIN workspace_folders wf ON wf.folder_id = f.id
                   WHERE wf.workspace_id = ? AND f.status IN ('ok', 'partial')
               ),
               walk(start_id, current_id) AS (
                   SELECT v.id, f.parent_id
                   FROM visible v
                   JOIN folders f ON f.id = v.id
                   UNION ALL
                   SELECT w.start_id, f.parent_id
                   FROM walk w
                   JOIN folders f ON f.id = w.current_id
                   WHERE w.current_id IS NOT NULL
                     AND w.current_id NOT IN (SELECT id FROM visible)
               ),
               effective AS (
                   SELECT start_id, current_id AS parent_id
                   FROM walk
                   WHERE current_id IS NULL
                      OR current_id IN (SELECT id FROM visible)
               )
               SELECT f.id, f.path, f.name,
                      e.parent_id AS parent_id,
                      f.photo_count, f.status,
                      v.is_workspace_root
               FROM folders f
               JOIN visible v ON v.id = f.id
               JOIN effective e ON e.start_id = f.id
               ORDER BY f.path""",
            (ws,),
        ).fetchall()

    def subtree_ids(self, folder_id):
        """Return folder_id plus descendants reached through workspace-linked nodes."""
        ws = self.workspace_id
        rows = self.conn.execute(
            """WITH RECURSIVE tree(id) AS (
                   SELECT ?
                   UNION ALL
                   SELECT f.id FROM folders f
                   JOIN tree t ON f.parent_id = t.id
                   JOIN workspace_folders wf_t
                     ON wf_t.folder_id = t.id AND wf_t.workspace_id = ?
                   JOIN workspace_folders wf_f
                     ON wf_f.folder_id = f.id AND wf_f.workspace_id = ?
               )
               SELECT id FROM tree""",
            (folder_id, ws, ws),
        ).fetchall()
        return [r["id"] for r in rows]

    def subtree_ids_by_path(self, folder_id, *, local_source_descendant_ids):
        """Return folder_id plus descendants by path prefix and local-copy source path.

        ``local_source_descendant_ids`` is
        ``Database._local_source_descendant_ids``.
        """
        row = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        if row is None or not row["path"]:
            return [folder_id]
        root_path = self.path_for_subtree_match(row["path"])
        prefix = root_path + "/"
        rows = self.conn.execute(
            """SELECT id FROM folders
               WHERE id = ?
                  OR substr(REPLACE(path, '\\', '/'), 1, ?) = ?""",
            (folder_id, len(prefix), prefix),
        ).fetchall()
        ids = {folder_id}
        ids.update(r["id"] for r in rows)
        ids.update(local_source_descendant_ids(row["path"]))
        return list(ids)

    def local_source_descendant_ids(self, root_path):
        """Return folder ids whose local-copy source_path lies under root_path."""
        if not root_path:
            return []
        prefix = self.path_for_subtree_match(root_path) + "/"
        rows = self.conn.execute(
            """SELECT folder_id FROM local_folder_mappings
               WHERE source_path = ?
                  OR substr(REPLACE(source_path, '\\', '/'), 1, ?) = ?""",
            (root_path, len(prefix), prefix),
        ).fetchall()
        return [int(r["folder_id"]) for r in rows]

    def count(self):
        """Return the number of ok/partial folders linked to the workspace."""
        return self.conn.execute(
            """SELECT COUNT(*) FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ? AND f.status IN ('ok', 'partial')""",
            (self.workspace_id,),
        ).fetchone()[0]

    def with_quality_data(self):
        """Return workspace folders with scored photos in their subtree."""
        ws = self.workspace_id
        # The recursive step also joins workspace_folders on the current
        # folder: propagation stops at any ancestor that is not in the active
        # workspace, which matches get_folder_subtree_ids and keeps the
        # dropdown counts aligned with get_highlights_candidates.
        return self.conn.execute(
            """WITH RECURSIVE ancestors(photo_id, folder_id, timestamp) AS (
                   SELECT p.id, p.folder_id, p.timestamp
                   FROM photos p
                   JOIN folders f0 ON f0.id = p.folder_id AND f0.status IN ('ok', 'partial')
                   JOIN workspace_folders wf0
                     ON wf0.folder_id = p.folder_id AND wf0.workspace_id = ?
                   WHERE p.quality_score IS NOT NULL
                   UNION ALL
                   SELECT a.photo_id, f.parent_id, a.timestamp
                   FROM ancestors a
                   JOIN folders f ON f.id = a.folder_id
                   JOIN workspace_folders wf_step
                     ON wf_step.folder_id = f.id AND wf_step.workspace_id = ?
                   WHERE f.parent_id IS NOT NULL
               )
               SELECT f.id, f.path, f.name,
                      COUNT(a.photo_id) as photo_count,
                      MAX(a.timestamp) as latest_photo
               FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               JOIN ancestors a ON a.folder_id = f.id
               WHERE wf.workspace_id = ?
                 AND f.status IN ('ok', 'partial')
               GROUP BY f.id
               ORDER BY latest_photo DESC""",
            (ws, ws, ws),
        ).fetchall()

    def update_counts(self):
        """Recompute photo_count for every folder and commit."""
        self.conn.execute(
            """
            UPDATE folders SET photo_count = (
                SELECT COUNT(*) FROM photos WHERE photos.folder_id = folders.id
            )
        """
        )
        self.conn.commit()

    def nearest_ancestor_id(self, path, exclude_id=None):
        """Return the id of the longest stored proper ancestor of path, or None."""
        target = self.path_for_subtree_match(path)
        best_id = None
        best_len = -1
        for row in self.conn.execute("SELECT id, path FROM folders"):
            if exclude_id is not None and row["id"] == exclude_id:
                continue
            cand = self.path_for_subtree_match(row["path"])
            if target == cand or not target.startswith(cand + "/"):
                continue
            if len(cand) > best_len:
                best_id = row["id"]
                best_len = len(cand)
        return best_id

    def relink_parents_by_path(self, folder_ids, *, nearest_ancestor_id):
        """Re-derive parent_id from each folder's current path; does not commit.

        ``nearest_ancestor_id`` is ``Database.nearest_ancestor_folder_id``.
        """
        for fid in folder_ids:
            row = self.conn.execute(
                "SELECT path FROM folders WHERE id = ?", (fid,)
            ).fetchone()
            if row is None:
                continue
            self.conn.execute(
                "UPDATE folders SET parent_id = ? WHERE id = ?",
                (nearest_ancestor_id(row["path"], exclude_id=fid), fid),
            )

    def repair_missing_parents(self):
        """Fill NULL parent_id from the stored parent path; commit if anything changed."""
        rows = self.conn.execute(
            "SELECT id, path, parent_id FROM folders"
        ).fetchall()
        path_to_id = {r["path"]: r["id"] for r in rows}
        updates = []
        for row in rows:
            if row["parent_id"] is not None:
                continue
            parent_path = self.stored_parent_path(row["path"])
            parent_id = path_to_id.get(parent_path)
            if parent_id is None or parent_id == row["id"]:
                continue
            updates.append((parent_id, row["id"]))
        if not updates:
            return
        self.conn.executemany(
            "UPDATE folders SET parent_id = ? WHERE id = ?",
            updates,
        )
        self.conn.commit()

    def repair_stale_parents(self, *, nearest_ancestor_id):
        """Repair parent links that contradict stored paths; return the updates.

        ``nearest_ancestor_id`` is ``Database.nearest_ancestor_folder_id``,
        called inside the ``BEGIN IMMEDIATE`` snapshot.
        """
        with self.conn:
            # Read and repair one snapshot: a concurrent move or local-copy
            # activation must not change paths after we validate the links.
            self.conn.execute("BEGIN IMMEDIATE")
            managed_ids = {
                row[0] for row in self.conn.execute(
                    "SELECT folder_id FROM local_folder_mappings "
                    "UNION SELECT folder_id FROM local_workspace_folders"
                )
            }
            rows = self.conn.execute(
                "SELECT id, path, parent_id FROM folders"
            ).fetchall()
            paths = {row["id"]: self.path_for_subtree_match(row["path"]) for row in rows}
            updates = []
            for row in rows:
                fid, parent_id = row["id"], row["parent_id"]
                if parent_id is None or fid in managed_ids or parent_id in managed_ids:
                    continue
                parent_path = paths.get(parent_id)
                if parent_path is not None and paths[fid].startswith(parent_path + "/"):
                    continue
                updates.append((nearest_ancestor_id(row["path"], exclude_id=fid), fid))
            self.conn.executemany(
                "UPDATE folders SET parent_id = ? WHERE id = ?", updates,
            )
        return updates

    def check_health(self):
        """Refresh every folder's status from disk; return how many changed."""
        rows = self.conn.execute("SELECT id, path, status FROM folders").fetchall()
        changed = 0
        newly_missing_paths = []
        for row in rows:
            exists = os.path.exists(row["path"])
            if not exists:
                new_status = "missing"
            elif row["status"] == "partial":
                new_status = "partial"
            else:
                new_status = "ok"
            if new_status != row["status"]:
                self.conn.execute(
                    "UPDATE folders SET status = ? WHERE id = ?",
                    (new_status, row["id"]),
                )
                changed += 1
                if new_status == "missing":
                    stored_path = row["path"] or ""
                    if stored_path:
                        newly_missing_paths.append(stored_path)
        # A folder going missing frees its stored path to be reused by an
        # unrelated mount without a corresponding row-delete: the folder row
        # stays put in case the same content comes back, but a different
        # removable card mounted at the same path can later be rescanned
        # into it. Any destination photo whose ``last_move_source_folder_path``
        # still equals that path would then compare equal to the new card's
        # ``src_dir`` in ``move_photos`` and slip past the same-stem developed-
        # render collision guard, letting an unrelated photo share the moved
        # photo's rendered output (developed lookup is only by destination
        # folder + stem). Clear provenance in the same transaction as the
        # status flip so a rollback restores both together.
        for chunk in self._chunks(newly_missing_paths):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"UPDATE photos SET last_move_source_folder_path = NULL "
                f"WHERE last_move_source_folder_path IN ({placeholders})",
                chunk,
            )
        if changed:
            self.conn.commit()
        return changed

    def missing(self):
        """Return the workspace's missing folders with photo counts."""
        return self.conn.execute(
            """SELECT f.id, f.path, f.name, f.parent_id,
                      COUNT(p.id) as photo_count
               FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               LEFT JOIN photos p ON p.folder_id = f.id
               WHERE wf.workspace_id = ? AND f.status = 'missing'
               GROUP BY f.id
               ORDER BY f.path""",
            (self.workspace_id,),
        ).fetchall()

    def photos_in_present_folders(self, subtree_clause, params):
        """Return workspace photos outside 'missing' folders for the missing-originals scan.

        ``params`` starts with the workspace id; ``subtree_clause`` is the
        façade's optional folder restriction (inline ids or a staged
        temp table), with its ids appended to ``params``.
        """
        return self.conn.execute(
            f"""SELECT p.id, p.filename, p.extension, p.file_size,
                      p.timestamp, p.working_copy_path,
                      f.id AS folder_id, f.path AS folder_path
               FROM photos p
               JOIN folders f ON p.folder_id = f.id
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ? AND f.status != 'missing'{subtree_clause}
               ORDER BY f.path, p.filename""",
            params,
        ).fetchall()

    def relocate(self, folder_id, new_path, *, merge_into_existing, relink_parents_by_path):
        """Move a folder row to new_path, cascading missing children; commit.

        ``merge_into_existing`` and ``relink_parents_by_path`` are the
        ``Database`` methods of the same names.
        """
        # Check for duplicate path
        conflict = self.conn.execute(
            "SELECT id FROM folders WHERE path = ? AND id != ?",
            (new_path, folder_id),
        ).fetchone()
        if conflict:
            # Only merge if source folder is missing; for ok folders, reject
            source_row = self.conn.execute(
                "SELECT status, path FROM folders WHERE id = ?", (folder_id,)
            ).fetchone()
            if source_row and source_row["status"] == "missing":
                # Revalidate: if original path came back, refresh status instead
                if os.path.isdir(source_row["path"]):
                    self.conn.execute(
                        "UPDATE folders SET status = 'ok' WHERE id = ?",
                        (folder_id,),
                    )
                    self.conn.commit()
                    raise ValueError(
                        f"Path is already tracked as folder {conflict['id']}"
                    )
                return merge_into_existing(folder_id, conflict["id"], new_path)
            raise ValueError(
                f"Path is already tracked as folder {conflict['id']}"
            )

        old_row = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        old_path = old_row["path"] if old_row else ""

        self.conn.execute(
            "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
            (new_path, folder_id),
        )

        # Check missing children for cascade
        cascaded = []
        skipped_prefixes = []
        children = self.conn.execute(
            """SELECT id, path FROM folders
               WHERE status = 'missing'
                 AND substr(REPLACE(path, '\\', '/'), 1, ?) = ?
               ORDER BY length(REPLACE(path, '\\', '/')),
                        REPLACE(path, '\\', '/')""",
            (len(self.subtree_prefix(old_path)), self.subtree_prefix(old_path)),
        ).fetchall()
        for child in children:
            # Skip descendants of conflicted folders
            child_match_path = self.path_for_subtree_match(child["path"])
            if any(child_match_path.startswith(p + "/") for p in skipped_prefixes):
                continue
            relative = self.subtree_relative(child["path"], old_path)
            candidate = self.join_subtree_path(new_path, relative)
            if os.path.exists(candidate):
                # Skip if another folder already has this path
                child_conflict = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ? AND id != ?",
                    (candidate, child["id"]),
                ).fetchone()
                if child_conflict:
                    skipped_prefixes.append(child_match_path)
                    continue
                self.conn.execute(
                    "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
                    (candidate, child["id"]),
                )
                cascaded.append({"id": child["id"], "old_path": child["path"], "new_path": candidate})

        # Cascade the rebase into ``photos.last_move_source_folder_path``.
        # ``move_folder_path`` already does this for the whole-folder move
        # flow; ``relocate_folder`` runs when a missing folder is remapped
        # to a new location (or a cascaded missing child is rediscovered
        # under the new root), which frees each old path for reuse the same
        # way. Without the rebase, a later scan of an unrelated folder at
        # the freed path would compare equal to a stale stored provenance
        # in ``move_photos`` and slip a same-stem developed-render collision
        # past the guard, letting two unrelated destination rows share the
        # developed-output lookup by folder+stem.
        rebased_paths = [(old_path, new_path)]
        rebased_paths.extend(
            (c["old_path"], c["new_path"]) for c in cascaded
        )
        for prior_path, updated_path in rebased_paths:
            if not prior_path or prior_path == updated_path:
                continue
            self.conn.execute(
                "UPDATE photos SET last_move_source_folder_path = ? "
                "WHERE last_move_source_folder_path = ?",
                (updated_path, prior_path),
            )

        relink_parents_by_path([folder_id] + [c["id"] for c in cascaded])
        self.conn.commit()
        return cascaded

    def merge_into_existing(self, source_folder_id, target_folder_id, new_path, *,
                            commit=True, transfer_gps_review, relink_parents_by_path):
        """Fold a missing folder into the existing folder at new_path.

        ``transfer_gps_review`` is ``Database._transfer_gps_review_for_merge``
        and ``relink_parents_by_path`` is ``Database._relink_parents_by_path``;
        both run inside this transaction. Commits only when ``commit``.
        """
        old_row = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (source_folder_id,)
        ).fetchone()
        old_path = old_row["path"] if old_row else ""

        # Get photos from the missing folder
        source_photos = self.conn.execute(
            "SELECT id, filename FROM photos WHERE folder_id = ?",
            (source_folder_id,),
        ).fetchall()

        # Reassign or drop each photo
        drop_ids = []
        # Where each dropped id's collection memberships go: the duplicate
        # that absorbs it, or nowhere for a phantom.
        collection_remap = {}
        for photo in source_photos:
            existing = self.conn.execute(
                "SELECT id FROM photos WHERE folder_id = ? AND filename = ?",
                (target_folder_id, photo["filename"]),
            ).fetchone()
            if existing:
                transfer_gps_review(photo["id"], existing["id"])
                drop_ids.append(photo["id"])
                collection_remap[photo["id"]] = existing["id"]
            elif os.path.exists(os.path.join(new_path, photo["filename"])):
                self.conn.execute(
                    "UPDATE photos SET folder_id = ? WHERE id = ?",
                    (target_folder_id, photo["id"]),
                )
            else:
                # File doesn't exist on disk at target — drop phantom record
                drop_ids.append(photo["id"])
                collection_remap[photo["id"]] = None

        # Delete duplicate photos and their associated data
        if drop_ids:
            ph = ",".join("?" for _ in drop_ids)
            self.conn.execute(f"DELETE FROM photo_keywords WHERE photo_id IN ({ph})", drop_ids)
            self.conn.execute(f"DELETE FROM pending_changes WHERE photo_id IN ({ph})", drop_ids)
            self.conn.execute(f"DELETE FROM detections WHERE photo_id IN ({ph})", drop_ids)
            self.conn.execute(f"DELETE FROM photos WHERE id IN ({ph})", drop_ids)
            remap_collection_photo_ids(self.conn, collection_remap)

        # Reparent child folders from source to target
        self.conn.execute(
            "UPDATE folders SET parent_id = ? WHERE parent_id = ?",
            (target_folder_id, source_folder_id),
        )

        # Transfer workspace visibility from source to target while preserving
        # whether the source link was a user-facing root or a materialized
        # descendant.
        workspace_links = self.conn.execute(
            "SELECT workspace_id, is_root FROM workspace_folders WHERE folder_id = ?",
            (source_folder_id,),
        ).fetchall()
        for link in workspace_links:
            self.conn.execute(
                """INSERT OR IGNORE INTO workspace_folders
                   (workspace_id, folder_id, is_root) VALUES (?, ?, ?)""",
                (link["workspace_id"], target_folder_id, link["is_root"]),
            )
            if link["is_root"]:
                self.conn.execute(
                    """UPDATE workspace_folders
                       SET is_root = 1
                       WHERE workspace_id = ? AND folder_id = ?""",
                    (link["workspace_id"], target_folder_id),
                )

        # Remove source folder
        self.conn.execute(
            "DELETE FROM workspace_folders WHERE folder_id = ?",
            (source_folder_id,),
        )
        self.conn.execute(
            "DELETE FROM folders WHERE id = ?", (source_folder_id,)
        )
        # Clear stale move provenance keyed on the merged source folder's
        # path. Mirrors ``delete_folder``: once the folder row is gone,
        # any destination photo whose ``last_move_source_folder_path``
        # still points at ``old_path`` would silently match a new
        # unrelated folder that later appears at that same path — the
        # same-stem developed-render collision guard in ``move_photos``
        # compares the stored origin to a candidate move's ``src_dir``
        # by string equality and would let two unrelated destination rows
        # share developed-output lookup by folder+stem. Runs inside the
        # merge's transaction so a rollback restores both together.
        if old_path:
            self.conn.execute(
                "UPDATE photos SET last_move_source_folder_path = NULL "
                "WHERE last_move_source_folder_path = ?",
                (old_path,),
            )

        # Ensure target folder is marked ok and recompute its photo count
        self.conn.execute(
            "UPDATE folders SET status = 'ok', photo_count = "
            "(SELECT COUNT(*) FROM photos WHERE folder_id = ?) "
            "WHERE id = ?",
            (target_folder_id, target_folder_id),
        )

        # Cascade to missing children (same logic as relocate_folder)
        cascaded = []
        skipped_prefixes = []
        children = self.conn.execute(
            """SELECT id, path FROM folders
               WHERE status = 'missing'
                 AND substr(REPLACE(path, '\\', '/'), 1, ?) = ?
               ORDER BY length(REPLACE(path, '\\', '/')),
                        REPLACE(path, '\\', '/')""",
            (len(self.subtree_prefix(old_path)), self.subtree_prefix(old_path)),
        ).fetchall()
        for child in children:
            child_match_path = self.path_for_subtree_match(child["path"])
            if any(child_match_path.startswith(p + "/") for p in skipped_prefixes):
                continue
            relative = self.subtree_relative(child["path"], old_path)
            candidate = self.join_subtree_path(new_path, relative)
            if os.path.exists(candidate):
                child_conflict = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ? AND id != ?",
                    (candidate, child["id"]),
                ).fetchone()
                if child_conflict:
                    skipped_prefixes.append(child_match_path)
                    continue
                self.conn.execute(
                    "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
                    (candidate, child["id"]),
                )
                cascaded.append({"id": child["id"], "old_path": child["path"], "new_path": candidate})

        # The surviving missing-child rows above have moved from each old
        # path to its candidate under the existing target root. Keep any
        # moved-photo provenance aligned with those live source rows. Clearing
        # only the merged root path is insufficient: a same-stem sibling from
        # a cascaded child would otherwise compare against its stale old path,
        # and that freed path could later be reused by unrelated content.
        for child in cascaded:
            self.conn.execute(
                "UPDATE photos SET last_move_source_folder_path = ? "
                "WHERE last_move_source_folder_path = ?",
                (child["new_path"], child["old_path"]),
            )

        relink_parents_by_path([c["id"] for c in cascaded])
        if commit:
            self.conn.commit()
        return cascaded

    def linked_in_other_workspace(self, folder_ids, active_ws):
        """Return the folder ids linked from any workspace other than active_ws."""
        linked = set()
        for chunk in self._chunks(folder_ids):
            placeholders = ",".join("?" for _ in chunk)
            sql = (
                f"SELECT DISTINCT folder_id FROM workspace_folders "
                f"WHERE folder_id IN ({placeholders})"
            )
            params = list(chunk)
            if active_ws is not None:
                sql += " AND workspace_id != ?"
                params.append(active_ws)
            linked.update(
                row["folder_id"]
                for row in self.conn.execute(sql, params).fetchall()
            )
        return linked

    def delete(self, folder_id, active_ws, *, subtree_ids_by_path,
               linked_in_other_workspace, delete_photos, remember_removals):
        """Delete a folder subtree and its photos in one transaction.

        Folders another workspace links to are kept and only unlinked from
        ``active_ws``. The callbacks are the ``Database`` methods
        ``_folder_subtree_ids_by_path``, ``_folders_linked_in_other_workspace``,
        ``delete_photos`` and ``_remember_workspace_folder_removals``.
        Returns ``(deleted_photo_ids, files)``.
        """
        # Everything under the target, by path, including legacy
        # NULL-parent_id descendants a parent_id walk would miss.
        candidates = set(subtree_ids_by_path(folder_id))

        # Candidates that another workspace has a link for are never
        # deleted, and each protected folder keeps its whole subtree. Any
        # foreign link counts — root or scanner-materialized — since either
        # means the folder is still visible in that workspace. When the
        # target itself is protected, the kept set covers every candidate
        # and this degenerates to unlink-only: nothing is deleted, no
        # reparenting happens, and only the active workspace's links go.
        protected = linked_in_other_workspace(candidates, active_ws)
        kept_subtree_ids = set()
        for fid in protected:
            kept_subtree_ids.update(subtree_ids_by_path(fid))
        delete_ids = candidates - kept_subtree_ids

        # Kept folders whose parent row is being deleted must be reparented
        # to NULL before the folder DELETE — folders.parent_id has no ON
        # DELETE action. (Kept folders whose parent also survives keep
        # their chain intact.)
        kept_head_ids = []
        for chunk in self._chunks(kept_subtree_ids):
            placeholders = ",".join("?" for _ in chunk)
            kept_head_ids.extend(
                row["id"]
                for row in self.conn.execute(
                    f"SELECT id, parent_id FROM folders WHERE id IN ({placeholders})",
                    chunk,
                ).fetchall()
                if row["parent_id"] in delete_ids
            )

        # Delete children before parents, else the multi-statement delete
        # trips the parent_id FK at the end of an earlier chunk's statement.
        # Order by path depth descending — a parent's normalized path always
        # has fewer separators than its child's, and parent_id order can't
        # be trusted for the legacy path-only rows.
        depth_by_id = {}
        # Collect the raw stored paths of the folders about to be deleted so we
        # can invalidate stale ``last_move_source_folder_path`` provenance on
        # photos moved out earlier. Without this, a new folder that later ends
        # up at the same path (e.g. a removable card re-mounted at the same
        # spot after the earlier scan was cleared) would compare equal to the
        # stored provenance and silently bypass the same-stem developed-render
        # collision guard in ``move_photos``.
        deleted_folder_paths = []
        for chunk in self._chunks(delete_ids):
            placeholders = ",".join("?" for _ in chunk)
            for row in self.conn.execute(
                f"SELECT id, path FROM folders WHERE id IN ({placeholders})",
                chunk,
            ).fetchall():
                stored_path = row["path"] or ""
                if stored_path:
                    deleted_folder_paths.append(stored_path)
                path = self.path_for_subtree_match(stored_path)
                depth_by_id[row["id"]] = path.count("/")
        ordered_delete_ids = sorted(
            delete_ids, key=lambda fid: depth_by_id.get(fid, 0), reverse=True
        )

        photo_ids = []
        for chunk in self._chunks(ordered_delete_ids):
            placeholders = ",".join("?" for _ in chunk)
            photo_ids.extend(
                row["id"]
                for row in self.conn.execute(
                    f"SELECT id FROM photos WHERE folder_id IN ({placeholders})",
                    chunk,
                ).fetchall()
            )

        # One outer transaction so a failure partway can't commit the photo
        # deletes while leaving the folder rows behind. ``commit=False`` also
        # defers delete_photos' pipeline-cache prune (a non-transactional
        # file write) until after the commit succeeds.
        files = []
        deleted_ids = []
        try:
            for chunk in self._chunks(photo_ids):
                inner = delete_photos(chunk, commit=False)
                files.extend(inner.get("files", []))
                deleted_ids.extend(inner.get("ids", []))
            # Reparent kept subtree heads before any folder DELETE — their
            # parent_id points at a row being deleted, and the FK has no ON
            # DELETE action.
            for chunk in self._chunks(kept_head_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"UPDATE folders SET parent_id = NULL "
                    f"WHERE id IN ({placeholders})",
                    chunk,
                )
            # Kept subtrees disappear from this workspace's view: drop the
            # active workspace's links, leaving the other workspaces' links
            # (and the folder rows and photos) untouched.
            if active_ws is not None:
                remember_removals(active_ws, kept_subtree_ids, recursive=True)
                for chunk in self._chunks(kept_subtree_ids):
                    placeholders = ",".join("?" for _ in chunk)
                    self.conn.execute(
                        f"DELETE FROM workspace_folders WHERE workspace_id = ? "
                        f"AND folder_id IN ({placeholders})",
                        [active_ws] + chunk,
                    )
            for chunk in self._chunks(ordered_delete_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"DELETE FROM workspace_folders WHERE folder_id IN ({placeholders})",
                    chunk,
                )
                self.conn.execute(
                    f"DELETE FROM folders WHERE id IN ({placeholders})",
                    chunk,
                )
            # Clear stale move provenance that would otherwise let a new
            # unrelated folder appearing at one of these deleted paths bypass
            # the same-stem developed-render collision guard in
            # ``move_photos``. Run after the folder DELETEs (nothing left in
            # this transaction can re-populate it) and inside the same outer
            # transaction so a rollback restores both together.
            for chunk in self._chunks(deleted_folder_paths):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"UPDATE photos SET last_move_source_folder_path = NULL "
                    f"WHERE last_move_source_folder_path IN ({placeholders})",
                    chunk,
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return deleted_ids, files
