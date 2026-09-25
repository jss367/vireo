"""Persistence for moving photos/folders and merging staged trees into archives.

Covers saved move rules, the folder-path and photo-folder rewrites a move
performs, the move-rule match query, the sync-only grants a merge records for
sibling workspaces, and the staged-tree -> archive reconciliation with the
per-collision state transfers it runs.

``Database`` keeps the composition. Keyword re-tagging stays on the façade
(``Database.tag_photo`` / ``untag_photo``) so the keyword-provenance fold and
its contract test (``test_keyword_provenance_contract``) keep seeing every
writer in ``db.py``. The merge and ``move_folder_path`` call other domains'
``Database`` methods mid-transaction; those are passed in as callbacks so the
bodies here stay verbatim and patched façade methods still take effect.

Everything is catalog-wide (folders, photos and keywords are global); the few
workspace-scoped reads take the workspace id, or a callable that resolves it,
from the caller. The path helpers come from ``db`` and are injected, since this
module imports no ``db`` code.
"""

import logging
import os

from keyword_normalization import keyword_match_key
from repositories import UNSET
from repositories.collections import remap_collection_photo_ids

log = logging.getLogger(__name__)


class MovesMergeRepository:
    def __init__(self, conn, *, subtree_prefix, subtree_relative, join_subtree_path):
        self.conn = conn
        self._subtree_prefix = subtree_prefix
        self._subtree_relative = subtree_relative
        self._join_subtree_path = join_subtree_path

    def create_rule(self, name, destination, criteria):
        """Insert a move rule and commit. Returns the rule id."""
        import json as _json
        cur = self.conn.execute(
            "INSERT INTO move_rules (name, destination, criteria) VALUES (?, ?, ?)",
            (name, destination, _json.dumps(criteria)),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_rule(self, rule_id):
        """Return one move rule row, or None."""
        return self.conn.execute(
            "SELECT * FROM move_rules WHERE id = ?", (rule_id,)
        ).fetchone()

    def list_rules(self):
        """Return every move rule row ordered by name."""
        return self.conn.execute(
            "SELECT * FROM move_rules ORDER BY name"
        ).fetchall()

    def update_rule(self, rule_id, name=UNSET, destination=UNSET, criteria=UNSET):
        """Update the given fields and commit; no-op when none are given."""
        import json as _json
        sets, params = [], []
        if name is not UNSET:
            sets.append("name = ?")
            params.append(name)
        if destination is not UNSET:
            sets.append("destination = ?")
            params.append(destination)
        if criteria is not UNSET:
            sets.append("criteria = ?")
            params.append(_json.dumps(criteria))
        if not sets:
            return
        params.append(rule_id)
        self.conn.execute(f"UPDATE move_rules SET {', '.join(sets)} WHERE id = ?", params)
        self.conn.commit()

    def delete_rule(self, rule_id):
        """Delete a move rule and commit."""
        self.conn.execute("DELETE FROM move_rules WHERE id = ?", (rule_id,))
        self.conn.commit()

    def touch_rule(self, rule_id):
        """Stamp ``last_run_at`` and commit."""
        self.conn.execute(
            "UPDATE move_rules SET last_run_at = datetime('now') WHERE id = ?",
            (rule_id,),
        )
        self.conn.commit()

    def batch_update_photo_folder(self, photo_ids, target_folder_id):
        """Repoint ``photo_ids`` at ``target_folder_id`` and commit."""
        if not photo_ids:
            return
        placeholders = ",".join("?" for _ in photo_ids)
        self.conn.execute(
            f"UPDATE photos SET folder_id = ? WHERE id IN ({placeholders})",
            [target_folder_id] + list(photo_ids),
        )
        self.conn.commit()

    def filename_collisions(self, photo_ids, target_folder_id):
        """Return ``[{photo_id, filename}]`` for names already in the target folder."""
        if not photo_ids:
            return []
        placeholders = ",".join("?" for _ in photo_ids)
        rows = self.conn.execute(
            f"""SELECT p.id AS photo_id, p.filename
                FROM photos p
                WHERE p.id IN ({placeholders})
                  AND EXISTS (
                    SELECT 1 FROM photos t
                    WHERE t.folder_id = ? AND t.filename = p.filename
                  )""",
            list(photo_ids) + [target_folder_id],
        ).fetchall()
        return [dict(r) for r in rows]

    def move_folder_path(self, folder_id, new_path, new_name=None, *,
                         relink_parents_by_path):
        """Rebase a folder subtree's paths and commit.

        ``relink_parents_by_path`` is ``Database._relink_parents_by_path``; it
        runs after every path has changed and before the provenance rewrite,
        inside the same transaction.
        """
        old_row = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (folder_id,)
        ).fetchone()
        if not old_row:
            return
        old_path = old_row["path"]
        if new_name is None:
            self.conn.execute(
                "UPDATE folders SET path = ? WHERE id = ?", (new_path, folder_id)
            )
        else:
            self.conn.execute(
                "UPDATE folders SET path = ?, name = ? WHERE id = ?",
                (new_path, new_name, folder_id),
            )
        children = self.conn.execute(
            """SELECT id, path FROM folders
               WHERE substr(REPLACE(path, '\\', '/'), 1, ?) = ?""",
            (len(self._subtree_prefix(old_path)), self._subtree_prefix(old_path)),
        ).fetchall()
        rebased_paths = [(old_path, new_path)]
        for child in children:
            child_new = self._join_subtree_path(
                new_path, self._subtree_relative(child["path"], old_path)
            )
            self.conn.execute(
                "UPDATE folders SET path = ? WHERE id = ?", (child_new, child["id"])
            )
            rebased_paths.append((child["path"], child_new))
        # Browse and subtree filters follow parent_id, not path. Re-link only
        # after all paths have changed so the moved tree follows its new
        # ancestors instead of staying nested under the source parent.
        relink_parents_by_path([folder_id] + [c["id"] for c in children])
        # Cascade the rename into ``photos.last_move_source_folder_path`` too.
        # That column stores the STORED source folder path a destination photo
        # was moved from, and the same-stem developed-render collision guard
        # in ``move_photos`` (see ``destination_stem_origins``) compares it to
        # a candidate move's ``src_dir``. Without this cascade, renaming the
        # source folder frees its old path for reuse — e.g. a card remounted
        # at ``/CARD`` after its earlier folder row was renamed to
        # ``/CARD.bak`` — and a new unrelated photo scanned back at ``/CARD``
        # would compare equal to the stale stored origin and slip past the
        # collision guard, letting two unrelated destination rows share the
        # developed-output lookup by folder+stem. Rebasing preserves the real
        # provenance relationship: the destination photo still shares its
        # render with any sibling that stays behind in the RENAMED folder.
        for prior_path, updated_path in rebased_paths:
            if prior_path == updated_path:
                continue
            self.conn.execute(
                "UPDATE photos SET last_move_source_folder_path = ? "
                "WHERE last_move_source_folder_path = ?",
                (updated_path, prior_path),
            )
        self.conn.commit()

    def query_rule_matches(self, criteria, workspace_id, min_detector_confidence):
        """Return photo ids in ``workspace_id`` matching move-rule ``criteria``.

        ``min_detector_confidence`` is the workspace-effective
        ``detector_confidence``, resolved by the caller only when
        ``criteria`` has ``has_predictions``.
        """
        conditions = ["wf.workspace_id = ?"]
        params = [workspace_id]
        joins = ["JOIN workspace_folders wf ON wf.folder_id = p.folder_id",
                 "JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')"]

        if "rating_min" in criteria:
            conditions.append("p.rating >= ?")
            params.append(criteria["rating_min"])
        if "flag" in criteria:
            conditions.append("p.flag = ?")
            params.append(criteria["flag"])
        if "folder_ids" in criteria and criteria["folder_ids"]:
            fph = ",".join("?" for _ in criteria["folder_ids"])
            conditions.append(f"p.folder_id IN ({fph})")
            params.extend(criteria["folder_ids"])
        if "has_predictions" in criteria:
            # Predictions no longer carry photo_id/workspace_id — they reference
            # a global detection, which references the photo. Workspace scoping
            # is already enforced by the outer workspace_folders JOIN, so the
            # EXISTS only needs to link prediction → detection → this photo.
            #
            # Apply the workspace-effective detector_confidence floor so the
            # rule matches what the UI actually shows: a photo whose only
            # predictions sit on below-threshold detections must NOT count
            # as "has predictions".
            move_min_conf = min_detector_confidence
            if criteria["has_predictions"]:
                conditions.append(
                    "EXISTS (SELECT 1 FROM predictions pr "
                    "JOIN detections d ON d.id = pr.detection_id "
                    "WHERE d.photo_id = p.id "
                    "  AND d.detector_confidence >= ?)"
                )
                params.append(move_min_conf)
            else:
                conditions.append(
                    "NOT EXISTS (SELECT 1 FROM predictions pr "
                    "JOIN detections d ON d.id = pr.detection_id "
                    "WHERE d.photo_id = p.id "
                    "  AND d.detector_confidence >= ?)"
                )
                params.append(move_min_conf)
        if "imported_before" in criteria:
            conditions.append("p.timestamp < ?")
            params.append(criteria["imported_before"])
        if "species" in criteria and criteria["species"]:
            sph = ",".join("?" for _ in criteria["species"])
            joins.append("JOIN photo_keywords pk ON pk.photo_id = p.id")
            joins.append("JOIN keywords k ON k.id = pk.keyword_id AND k.is_species = 1")
            conditions.append(f"k.name IN ({sph})")
            params.extend(criteria["species"])

        join_sql = "\n".join(joins)
        where_sql = " AND ".join(conditions)
        rows = self.conn.execute(
            f"SELECT DISTINCT p.id FROM photos p {join_sql} WHERE {where_sql}",
            params,
        ).fetchall()
        return [r["id"] for r in rows]

    def newest_location_change_key(self, photo_id):
        """``(created_at, id)`` of the newest queued location change, or None."""
        row = self.conn.execute(
            "SELECT id, created_at FROM pending_changes "
            "WHERE photo_id = ? AND change_type = 'location' "
            "ORDER BY created_at DESC, id DESC LIMIT 1",
            (photo_id,),
        ).fetchone()
        if row is None:
            return None
        return (row["created_at"] or "", row["id"])

    def prepare_location_transfer(self, losing_id, surviving_id):
        """Return ``losing_id``'s location keyword ids and drop the survivor's.

        Deletes ``surviving_id``'s links to ``type='location'`` keywords without
        committing; the caller re-tags the returned ids through
        ``Database.tag_photo`` so they carry the provenance fold.
        """
        losing_kw_ids = [
            r["keyword_id"] for r in self.conn.execute(
                "SELECT pk.keyword_id FROM photo_keywords pk "
                "JOIN keywords k ON k.id = pk.keyword_id "
                "WHERE pk.photo_id = ? AND k.type = 'location'",
                (losing_id,),
            ).fetchall()
        ]
        self.conn.execute(
            "DELETE FROM photo_keywords WHERE photo_id = ? "
            "AND keyword_id IN ("
            "  SELECT id FROM keywords WHERE type = 'location')",
            (surviving_id,),
        )
        return losing_kw_ids

    def reconcile_conflicting_keyword_edits(self, losing_id, surviving_id, *,
                                            carry_keyword_associations):
        """Drop the older of opposing keyword edits; returns rows dropped.

        ``carry_keyword_associations`` is
        ``Database._carry_keyword_associations_for_merge``, which re-tags
        through the façade. Runs inside the caller's transaction.
        """
        rows = self.conn.execute(
            # ``keyword_remove_flat`` is deliberately absent. It asks for a
            # stale flat ``dc:subject`` line to go without touching the
            # association, and ``_remove_planned_keywords`` folds it
            # together with a paired add -- that combination is the
            # normalization rename working as designed, not two intents
            # competing.
            "SELECT id, photo_id, workspace_id, change_type, value, "
            "       created_at "
            "FROM pending_changes WHERE photo_id IN (?, ?) "
            "  AND change_type IN ('keyword_add', 'keyword_remove')",
            (losing_id, surviving_id),
        ).fetchall()
        if not rows:
            return 0
        # by_key[match_key][photo_id][workspace_id] = {"add": [...],
        # "remove": [...]}. Grouped by workspace inside each photo so the
        # rename-pair exemption below applies within a single workspace's
        # queue -- an add in workspace A and a remove in workspace B on the
        # same photo are two intents competing across workspaces, not a
        # normalization rename.
        by_key = {}
        for r in rows:
            if not r["value"]:
                continue
            key = keyword_match_key(r["value"])
            side = "add" if r["change_type"] == "keyword_add" else "remove"
            slot = by_key.setdefault(
                key, {losing_id: {}, surviving_id: {}})
            ws_slot = slot[r["photo_id"]].setdefault(
                r["workspace_id"], {"add": [], "remove": []})
            ws_slot[side].append(r)

        def _row_stamp(row):
            return (row["created_at"] or "", row["id"])

        def _collect_atoms(slot):
            """Split every row for one match key into atomic keep/remove units.

            A per-workspace rename pair (add + remove for the same key in
            one workspace's queue on ONE photo) is a normalization rename --
            its two rows together assert "keep the tag, canonicalize the
            spelling", so they travel as one keep-asserting atom stamped by
            its newest row. Every other row is a standalone intent and is
            its own atom on the side its ``change_type`` names. Atoms are
            pooled across both photos: once the remap lands them on one
            photo, an add in workspace A and a remove in workspace B are
            two intents for one global ``photo_keywords`` row and one
            sidecar, whether they started on the same photo or different
            photos.
            """
            keep_atoms, remove_atoms = [], []
            for photo_slot in slot.values():
                for ws_sides in photo_slot.values():
                    if ws_sides["add"] and ws_sides["remove"]:
                        rows = ws_sides["add"] + ws_sides["remove"]
                        keep_atoms.append(
                            (max(_row_stamp(r) for r in rows), rows))
                    else:
                        for r in ws_sides["add"]:
                            keep_atoms.append((_row_stamp(r), [r]))
                        for r in ws_sides["remove"]:
                            remove_atoms.append((_row_stamp(r), [r]))
            return keep_atoms, remove_atoms

        dropped = 0
        for match_key, slot in by_key.items():
            keep_atoms, remove_atoms = _collect_atoms(slot)
            if not keep_atoms or not remove_atoms:
                continue
            newest_keep = max(stamp for stamp, _ in keep_atoms)
            newest_remove = max(stamp for stamp, _ in remove_atoms)
            losing_side = (
                remove_atoms if newest_keep > newest_remove else keep_atoms)
            drop = [r for _, rows in losing_side for r in rows]
            for r in drop:
                self.conn.execute(
                    "DELETE FROM pending_changes WHERE id = ?", (r["id"],))
                dropped += 1
            log.info(
                "Merge reconciled opposing %r edits on photos %s/%s: "
                "dropped %d older queue row(s)",
                match_key, losing_id, surviving_id, len(drop),
            )
        carry_keyword_associations(
            losing_id, surviving_id, by_key)
        return dropped

    def live_pending_change_ids(self, losing_id, surviving_id):
        """Ids of the pending changes still queued on either photo."""
        live = {
            r["id"] for r in self.conn.execute(
                "SELECT id FROM pending_changes WHERE photo_id IN (?, ?)",
                (losing_id, surviving_id),
            ).fetchall()
        }
        return live

    def photo_keyword_ids_matching(self, photo_id, match_key):
        """Keyword ids on ``photo_id`` whose name has ``match_key``."""
        return [
            r["keyword_id"] for r in self.conn.execute(
                "SELECT pk.keyword_id AS keyword_id, k.name AS name "
                "FROM photo_keywords pk "
                "JOIN keywords k ON k.id = pk.keyword_id "
                "WHERE pk.photo_id = ?",
                (photo_id,),
            ).fetchall()
            if keyword_match_key(r["name"]) == match_key
        ]

    def transfer_gps_review(self, losing_id, surviving_id):
        """Carry the newest GPS keep decision onto the survivor (no commit)."""
        self.conn.execute("""
            INSERT INTO location_gps_reviews(photo_id, fingerprint, reviewed_at)
            SELECT ?, fingerprint, reviewed_at FROM location_gps_reviews WHERE photo_id = ?
            ON CONFLICT(photo_id) DO UPDATE SET
                fingerprint = excluded.fingerprint, reviewed_at = excluded.reviewed_at
            WHERE excluded.reviewed_at > location_gps_reviews.reviewed_at
        """, (surviving_id, losing_id))

    def transfer_review_state(self, losing_id, surviving_id, *,
                              transfer_gps_review, transfer_edit_recipe):
        """Carry queued rating/flag state onto the survivor; returns rows dropped.

        ``transfer_gps_review`` / ``transfer_edit_recipe`` are the
        ``Database._transfer_*_for_merge`` façade methods. No commit.
        """
        transfer_gps_review(losing_id, surviving_id)
        dropped = 0
        for change_type, column in (("rating", "rating"), ("flag", "flag")):
            rows = self.conn.execute(
                "SELECT id, photo_id, workspace_id, value, created_at "
                "FROM pending_changes "
                "WHERE photo_id IN (?, ?) AND change_type = ?",
                (losing_id, surviving_id, change_type),
            ).fetchall()
            if not any(r["photo_id"] == losing_id for r in rows):
                # Nothing queued on the row being deleted: the survivor's
                # own column and queue already agree with each other.
                continue

            def key(r):
                return (r["created_at"] or "", r["id"])

            newest = max(rows, key=key)
            # Only adjudicate competition the merge itself creates: rows on
            # BOTH photos, which until now described two separate files and
            # from here describe one. Two rows already sharing a photo were
            # in that state before the merge and are not its business --
            # the same line the keyword path draws around a pre-existing
            # rename pair.
            #
            # When they do compete, resolve across every workspace.
            # ``photos.rating`` and ``photos.flag`` are single-valued global
            # columns and both rows now write the same sidecar, so keeping
            # one row per workspace would leave the file's final value to
            # whichever workspace syncs last -- disagreeing with the catalog
            # column either way.
            if len({r["photo_id"] for r in rows}) > 1:
                for r in rows:
                    if r["id"] == newest["id"]:
                        continue
                    self.conn.execute(
                        "DELETE FROM pending_changes WHERE id = ?",
                        (r["id"],))
                    dropped += 1
            value = newest["value"]
            if change_type == "rating":
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    # A malformed queue row; ``sync_to_xmp`` skips it too.
                    continue
            else:
                value = value or "none"
            self.conn.execute(
                f"UPDATE photos SET {column} = ? WHERE id = ?",
                (value, surviving_id),
            )
        dropped += transfer_edit_recipe(
            losing_id, surviving_id)
        if dropped:
            log.info(
                "Merge reconciled queued review state on photos %s/%s: "
                "dropped %d older queue row(s)",
                losing_id, surviving_id, dropped,
            )
        return dropped

    def transfer_edit_recipe(self, losing_id, surviving_id):
        """Rebuild the survivor's edit recipe from the newest queued one (no commit)."""
        rows = self.conn.execute(
            "SELECT id, photo_id, workspace_id, value, created_at "
            "FROM pending_changes "
            "WHERE photo_id IN (?, ?) AND change_type = 'edit_recipe'",
            (losing_id, surviving_id),
        ).fetchall()
        if not any(r["photo_id"] == losing_id for r in rows):
            return 0

        def key(r):
            return (r["created_at"] or "", r["id"])

        dropped = 0
        newest = max(rows, key=key)
        # Same two conditions as the scalar columns: only rows spanning both
        # photos are competition this merge created, and when they do
        # compete it is resolved across every workspace -- one row per photo
        # in ``photo_edit_recipes``, one sidecar on disk.
        if len({r["photo_id"] for r in rows}) > 1:
            for r in rows:
                if r["id"] == newest["id"]:
                    continue
                self.conn.execute(
                    "DELETE FROM pending_changes WHERE id = ?", (r["id"],))
                dropped += 1
        recipe_json = newest["value"] or ""
        if not recipe_json:
            self.conn.execute(
                "DELETE FROM photo_edit_recipes WHERE photo_id = ?",
                (surviving_id,),
            )
        else:
            self.conn.execute(
                """INSERT INTO photo_edit_recipes
                       (photo_id, recipe_json, updated_at)
                   VALUES (?, ?, datetime('now'))
                   ON CONFLICT(photo_id) DO UPDATE SET
                       recipe_json = excluded.recipe_json,
                       updated_at = excluded.updated_at""",
                (surviving_id, recipe_json),
            )
        return dropped

    def link_survivor_for_sibling_edits(self, workspace_id, photo_id):
        """Grant ``workspace_id`` sync-only access to ``photo_id`` (no commit)."""
        row = self.conn.execute(
            "SELECT folder_id FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()
        if row is None or row["folder_id"] is None:
            return False
        # Skip if the workspace already resolves this photo through its
        # normal library membership -- no sync-only grant is needed on top.
        already_linked = self.conn.execute(
            "SELECT 1 FROM workspace_folders "
            "WHERE workspace_id = ? AND folder_id = ?",
            (workspace_id, row["folder_id"]),
        ).fetchone()
        if already_linked is not None:
            return False
        already_grant = self.conn.execute(
            "SELECT 1 FROM workspace_sync_only_photos "
            "WHERE workspace_id = ? AND photo_id = ?",
            (workspace_id, photo_id),
        ).fetchone()
        if already_grant is not None:
            return False
        self.conn.execute(
            "INSERT OR IGNORE INTO workspace_sync_only_photos "
            "(workspace_id, photo_id) VALUES (?, ?)",
            (workspace_id, photo_id),
        )
        log.info(
            "Granted workspace %s sync-only access to photo %s so its "
            "preserved edits stay syncable after the archive merge",
            workspace_id, photo_id,
        )
        return True

    def sync_only_photo_paths(self, workspace_id):
        """Return ``{photo_id: folders.path}`` for ``workspace_id``'s sync-only grants."""
        rows = self.conn.execute(
            "SELECT sop.photo_id AS photo_id, f.path AS path "
            "FROM workspace_sync_only_photos sop "
            "JOIN photos p ON p.id = sop.photo_id "
            "JOIN folders f ON f.id = p.folder_id "
            "WHERE sop.workspace_id = ? "
            "  AND f.status IN ('ok', 'partial')",
            (workspace_id,),
        ).fetchall()
        paths = {r["photo_id"]: r["path"] for r in rows}
        legacy = self.conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='workspace_sync_only_folders'"
        ).fetchone()
        if legacy is not None:
            legacy_rows = self.conn.execute(
                """SELECT DISTINCT pc.photo_id AS photo_id,
                          f.path AS path
                   FROM pending_changes pc
                   JOIN photos p ON p.id = pc.photo_id
                   JOIN folders f ON f.id = p.folder_id
                   JOIN workspace_sync_only_folders sof
                     ON sof.workspace_id = pc.workspace_id
                   LEFT JOIN folders granted
                     ON granted.id = sof.folder_id
                   WHERE pc.workspace_id = ?
                     AND f.status IN ('ok', 'partial')
                     AND (sof.folder_id = p.folder_id
                          OR (granted.path IS NOT NULL
                              AND granted.path
                                  = p.last_move_source_folder_path))""",
                (workspace_id,),
            ).fetchall()
            for r in legacy_rows:
                paths.setdefault(r["photo_id"], r["path"])
        return paths

    def merge_staged_tree_into_archive(
            self, staged_root_id, archive_path, *, workspace_id_fn,
            root_ancestor_exists, root_descendant_exists,
            prune_nonroot_links_outside_roots,
            materialize_workspace_descendants, add_workspace_folder,
            add_workspace_folder_no_commit, case_insensitive_root,
            move_location_state, reconcile_keyword_edits,
            transfer_review_state, link_survivor_for_sibling_edits,
            invalidate_new_images, update_folder_counts):
        """Fold a staged folder subtree into a tracked archive; returns counts.

        See ``Database.merge_staged_tree_into_archive`` for the contract. The
        keyword arguments are the façade methods and hooks the body composes,
        passed in so monkeypatches of ``Database`` methods, the per-instance
        new-images cache and ``move._case_insensitive_root`` still apply, and
        so the workspace is resolved only after the staged-root lookup:

        * ``workspace_id_fn`` -> ``Database._ws_id``
        * ``root_ancestor_exists`` / ``root_descendant_exists`` ->
          ``Database._active_ws_root_{ancestor,descendant}_exists``
        * ``prune_nonroot_links_outside_roots`` ->
          ``Database._prune_ws_nonroot_links_outside_roots``
        * ``materialize_workspace_descendants``, ``add_workspace_folder``,
          ``add_workspace_folder_no_commit``, ``update_folder_counts`` -> the
          ``Database`` methods of the same name
        * ``case_insensitive_root`` -> ``move._case_insensitive_root``
        * ``move_location_state``, ``reconcile_keyword_edits``,
          ``transfer_review_state``, ``link_survivor_for_sibling_edits`` ->
          the ``Database._*_for_merge`` / ``_link_survivor_*`` helpers
        * ``invalidate_new_images(workspace_ids)`` -> the new-images cache
        """
        staged_root = self.conn.execute(
            "SELECT path FROM folders WHERE id = ?", (staged_root_id,)
        ).fetchone()
        if not staged_root:
            return {"new_photos": 0, "new_folders": 0,
                    "merged_folders": 0, "already_present": 0,
                    "dropped_photo_ids": [],
                    "preserved_edit_count": 0,
                    "preserved_off_staging_identities": []}
        staged_root_path = staged_root["path"]
        ws = workspace_id_fn()

        # Ensure the existing archive base — and every folder row already
        # below it — is linked to the active workspace before any staged
        # photo is reparented onto one of those pre-existing folder rows.
        # If the archive was scanned only under a different workspace, the
        # else-branch UPDATE below would move photos onto a ``target["id"]``
        # that has no ``workspace_folders`` row for ``ws``; workspace-scoped
        # photo queries join ``workspace_folders`` on ``p.folder_id`` and
        # would silently drop every merged-in photo. ``add_workspace_folder``
        # pulls the whole subtree (path-prefix), so a single link on the
        # archive base covers every existing descendant the reconciliation
        # can hit.
        #
        # Root the base ONLY when the active workspace has no existing root
        # ancestor of it. For an ancestor merge (``/Photos`` is already a
        # workspace root, base ``/Photos/USA``), rooting the base would create
        # a SECOND overlapping workspace root inside the first — the exact
        # duplicate-root state the tracked-overlap guards exist to prevent
        # (``add_workspace_folder(is_root=True)`` only demotes rows inside the
        # base's own subtree, so the outer ``/Photos`` root would survive). In
        # that case just LINK the base non-root; the existing ancestor root
        # keeps covering it. When there is no root ancestor (the base IS the
        # archive the user imported into), the base is the natural root.
        #
        # When the base is ALREADY a workspace root for ``ws``, skip the
        # ``add_workspace_folder`` call entirely instead of passing
        # ``is_root=False`` — the descendant subtree is already linked from
        # when the base was rooted, and calling with ``is_root=False`` would
        # silently rely on ``add_workspace_folder``'s no-op-on-existing-row
        # behavior to preserve the root flag. Making the "already root" case
        # an explicit skip keeps the merge safe if that invariant ever changes.
        archive_row = self.conn.execute(
            "SELECT id, status FROM folders WHERE path = ?", (archive_path,)
        ).fetchone()
        if archive_row:
            existing_link = self.conn.execute(
                "SELECT is_root FROM workspace_folders "
                "WHERE workspace_id = ? AND folder_id = ?",
                (ws, archive_row["id"]),
            ).fetchone()
            if existing_link is None or existing_link["is_root"] == 0:
                # Not root of ws (unlinked, or linked non-root): link the
                # subtree, and root the base only if it would not replace an
                # existing narrower or broader root. A strict descendant root
                # means the workspace is intentionally scoped inside this
                # archive (e.g. ``/Photos/USA/2026`` while importing into
                # ``/Photos/USA``). In that shape, do not link the broad base
                # at all: even a non-root link materializes every descendant
                # and would make archive siblings part of workspace queries.
                has_root_ancestor = root_ancestor_exists(
                    ws, archive_path)
                has_root_descendant = root_descendant_exists(
                    ws, archive_path)
                if has_root_descendant and not has_root_ancestor:
                    prune_nonroot_links_outside_roots(
                        ws, archive_path)
                    materialize_workspace_descendants(ws)
                else:
                    add_workspace_folder(
                        ws,
                        archive_row["id"],
                        is_root=not has_root_ancestor,
                    )
            # else: base is already a workspace root — nothing to do.
            # If the archive base was marked ``missing`` at a previous health
            # scan (drive unmounted at the time), the storage preflight has
            # since verified the volume is mounted and the rsync copy landed
            # files on disk — flip it back to ``ok`` so ws-scoped photo queries
            # (which filter ``folders.status IN ('ok', 'partial')``) show the
            # merged photos instead of hiding them until the next health scan
            # happens to reconcile. Only migrate ``missing`` → ``ok``: leave
            # ``partial`` alone (the row still has unverified photos) and
            # ``ok`` unchanged.
            if archive_row["status"] == "missing":
                self.conn.execute(
                    "UPDATE folders SET status = 'ok' WHERE id = ?",
                    (archive_row["id"],),
                )
        elif not root_ancestor_exists(ws, archive_path):
            # ``archive_path`` has no folder row yet — it's a brand-new
            # subfolder inside an already-tracked archive that the staged
            # root will be repointed onto below. When the tracked archive
            # was scanned only under a DIFFERENT workspace, the active
            # workspace has no link to any of it, no root ancestor covers
            # ``archive_path``, and the staged-root demotion further down
            # (``UPDATE workspace_folders SET is_root = 0``) leaves ``ws``
            # with no ``is_root=1`` row for the merged tree at all —
            # ``get_workspace_folder_roots()`` filters on ``is_root=1``, so
            # the merged archive silently disappears from the active ws
            # even though the import reports success. Walk up to find the
            # deepest tracked ancestor and root it in ``ws`` so the merged
            # tree has a visible anchor. The intermediate-materialization
            # block below then links each freshly-created intermediate as a
            # non-root descendant under this new root.
            probe = os.path.dirname(archive_path)
            while probe and probe != os.path.dirname(probe):
                ancestor_row = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (probe,)
                ).fetchone()
                if ancestor_row is not None:
                    if not root_descendant_exists(ws, probe):
                        add_workspace_folder(
                            ws, ancestor_row["id"], is_root=True)
                    else:
                        # Descendant-root guard fires: the workspace is
                        # scoped narrower than this ancestor, so rooting
                        # it would widen the scope past the intended root.
                        # But any pre-existing ``is_root=0`` link on this
                        # ancestor (or on descendants below it that no
                        # root still covers) would let
                        # ``_materialize_workspace_descendants`` — called
                        # by later ``get_workspace_folders()`` reads —
                        # pull the broader subtree back into the workspace
                        # and defeat the scoped merge. Prune those
                        # uncovered non-root links now, matching the
                        # cleanup the ``existing_link is None or
                        # is_root == 0`` branch above already performs
                        # via ``_prune_ws_nonroot_links_outside_roots``.
                        prune_nonroot_links_outside_roots(
                            ws, probe)
                    break
                probe = os.path.dirname(probe)

        # Materialize any missing intermediate folder rows between the deepest
        # existing catalog ancestor and ``archive_path``'s parent (inclusive)
        # BEFORE the reconciliation loop reads ``parent_id`` by path.
        #
        # Nested archive destinations expose this gap: when ``/Photos`` is
        # tracked and the user imports to ``/Photos/2026/NewShoot``, the
        # storage preflight materializes ``/Photos/2026`` ON DISK (rsync needs
        # the transfer parent to exist) but never opens a folder row for it —
        # the scanner didn't visit that path. Without the row, the loop's
        # ``WHERE path = ?`` lookup for the staged root's target-parent
        # returns nothing, and the UPDATE below repoints the staged root to
        # ``archive_path`` with ``parent_id=NULL`` — floating it outside the
        # managed archive tree and breaking every parent-based subtree
        # operation (cascade path renames, ``_folder_subtree_ids_by_path``,
        # etc.). Walk up from the archive parent until an existing row shows
        # up (or the filesystem root). When no anchor is found (no tracked
        # ancestor row in the catalog), the destination is a brand-new
        # unrelated root — leave the loop's ``parent_id=NULL`` alone, which
        # is the expected shape for a root. Otherwise insert missing rows
        # top-down so each child's ``parent_id`` resolves to its freshly-
        # created parent, and link each to the active workspace non-root
        # ONLY when an existing workspace root actually covers the
        # intermediate. Linking non-root unconditionally would leak: if the
        # workspace is scoped to a narrower root (e.g. ``/archive/USA/2026``)
        # and the merge target is a sibling like ``/archive/USA/2027/Trip``,
        # the descendant-root guard above suppresses rooting ``/archive/USA``,
        # so no workspace root covers the ``/archive/USA/2027`` intermediate.
        # A non-root link there still makes 2027's subtree visible via
        # ``_materialize_workspace_descendants`` (called by
        # ``get_workspace_folders``), defeating the scoped-merge behavior.
        missing_intermediates = []
        probe = os.path.dirname(archive_path)
        anchor_found = False
        while probe and probe != os.path.dirname(probe):
            row = self.conn.execute(
                "SELECT id FROM folders WHERE path = ?", (probe,)
            ).fetchone()
            if row is not None:
                anchor_found = True
                break
            missing_intermediates.append(probe)
            probe = os.path.dirname(probe)
        if anchor_found:
            for mid_path in reversed(missing_intermediates):
                mid_parent = os.path.dirname(mid_path)
                parent_row = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (mid_parent,)
                ).fetchone()
                parent_id_for_mid = (
                    parent_row["id"] if parent_row else None)
                name = os.path.basename(mid_path) or mid_path
                # Two concurrent local-processing jobs targeting siblings
                # inside the same tracked archive (e.g. ``/Photos/2026/A`` and
                # ``/Photos/2026/B`` while only ``/Photos`` is tracked) can
                # each snapshot the shared intermediate (``/Photos/2026``)
                # as missing above, then race to insert its ``folders`` row
                # here. The final archive paths don't overlap, so the
                # storage-destination reservation doesn't serialize them; the
                # loser's plain INSERT would hit the ``folders.path`` UNIQUE
                # constraint AFTER all staging/processing work is done.
                # ``INSERT OR IGNORE`` + re-query keeps the loser's merge
                # progressing against whichever row won the race — the same
                # intermediate is folder-idempotent (same path, same tracked
                # ancestor parent). ``cur.lastrowid`` is 0 on an ignored
                # insert, so read the id back via ``WHERE path = ?``.
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO folders (path, name, parent_id) "
                    "VALUES (?, ?, ?)",
                    (mid_path, name, parent_id_for_mid),
                )
                if cur.rowcount:
                    mid_id = cur.lastrowid
                else:
                    mid_id = self.conn.execute(
                        "SELECT id FROM folders WHERE path = ?", (mid_path,)
                    ).fetchone()["id"]
                if root_ancestor_exists(ws, mid_path):
                    add_workspace_folder(
                        ws, mid_id, is_root=False)

        # Snapshot staged folders root-first (shallowest path first) so a
        # parent's target row exists before its children are processed.
        prefix = self._subtree_prefix(staged_root_path)
        staged_folders = self.conn.execute(
            """SELECT id, path FROM folders
               WHERE path = ? OR substr(REPLACE(path, '\\', '/'), 1, ?) = ?
               ORDER BY length(path) ASC""",
            (staged_root_path, len(prefix), prefix),
        ).fetchall()

        counts = {"new_photos": 0, "new_folders": 0,
                  "merged_folders": 0, "already_present": 0,
                  "dropped_photo_ids": [],
                  # Pending edits that were queued against a photo the
                  # collision loop is about to delete (a staged row on a
                  # real collision, or the phantom target row on a
                  # replacement), remapped onto the surviving row before
                  # ON DELETE CASCADE could drop them. Reported up so the
                  # NAS transfer's residual check can add them to the
                  # "still need a sync" count instead of silently losing
                  # them to the cascade.
                  "preserved_edit_count": 0,
                  # Active-workspace identities (``change_token`` or
                  # ``("id", id)``, matching ``staged_sync_scope``) of the
                  # off-staging remap subset -- the archive-side survivor
                  # on a real collision. The caller's residual re-read is
                  # scoped by the captured staged ids, so only this subset
                  # is invisible to it and has to be added separately. The
                  # phantom-target branch (survivor is the staged photo)
                  # and the intra-staged case (survivor is another staged
                  # photo already reparented in this pass) are already
                  # covered by the residual re-read, and counting them
                  # here as well would report one edit as two. Restricted
                  # to the active workspace and reported as identities --
                  # not a raw ``rowcount`` -- so the caller can filter out
                  # rows its pre-transfer drain already left as
                  # undeliverable (which are not "queued during transfer",
                  # only edits this workspace declined to write) and
                  # exclude sibling-workspace rows (which this sync would
                  # not have written either).
                  "preserved_off_staging_identities": []}
        # Staged folders that fold into an existing target row are deleted only
        # after every staged folder has been processed. Deleting eagerly would
        # hit a FK violation when a not-yet-reparented staged child still points
        # at the staged parent we are removing.
        to_delete = []
        # ``(workspace_id, survivor_photo_id)`` pairs for pending edits the
        # collision loop remapped out of a workspace other than the one
        # running the merge. Collected here and applied after every folder
        # reparent has settled, because the link has to name the survivor's
        # FINAL folder: an intra-staged or phantom survivor is still sitting
        # in a staged folder when its remap happens and only lands in the
        # archive folder later in the loop. A set, so two edits sharing a
        # survivor write one link.
        sibling_links = set()
        # Collection memberships of every photo row this merge drops, keyed
        # to the survivor that absorbs it; applied once, before the commit.
        collection_remap = {}
        # Map of target-path -> folder id for folders already processed in this
        # run, so a child can fall back to its parent's id (Fix I2) even if the
        # parent's row isn't yet findable by path lookup.
        last_target_parent = {}

        # Wrap the reconciliation body in try/except + rollback so a mid-run
        # exception (unexpected row shape, raised error from the
        # case-insensitivity probe, etc.) can't leave a partially-applied
        # merge sitting on the connection — an unrelated later commit would
        # otherwise persist half-reparented folders/photos. Matches the
        # convention used by other multi-step mutation methods in this file
        # (``delete_folder``, ``move_folders_to_workspace``,
        # ``_merge_duplicate_keywords_pass``). Archive-base linking and
        # missing-intermediate materialization above each commit through
        # their own ``add_workspace_folder`` calls, so their success is
        # persisted independently — that's the desired shape here: partial
        # progress on preparing the archive tree is a valid state a retry
        # can build on, but partial photo/folder reparenting is not.
        try:
            for sf in staged_folders:
                rel = self._subtree_relative(sf["path"], staged_root_path)
                target_path = self._join_subtree_path(archive_path, rel)
                target = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (target_path,)
                ).fetchone()
                parent_path = os.path.dirname(target_path)
                parent_row = self.conn.execute(
                    "SELECT id FROM folders WHERE path = ?", (parent_path,)
                ).fetchone()
                parent_id = parent_row["id"] if parent_row else None

                # Defensive: a non-root staged folder whose target-parent row is
                # missing would silently get parent_id=NULL, breaking the chain.
                # The scanner normally materializes every intermediate, so this
                # is an unenforced invariant — log it, and fall back to the last
                # processed target-parent id when we have one.
                if (parent_id is None
                        and target_path != archive_path
                        and parent_path and parent_path != target_path):
                    fallback = last_target_parent.get(parent_path)
                    if fallback is not None:
                        log.warning(
                            "merge_staged_tree_into_archive: no folder row "
                            "for target parent %r of %r; falling back to "
                            "id %s",
                            parent_path, target_path, fallback,
                        )
                        parent_id = fallback
                    else:
                        log.warning(
                            "merge_staged_tree_into_archive: no folder row "
                            "for target parent %r of %r; leaving parent_id "
                            "NULL",
                            parent_path, target_path,
                        )

                if target is None:
                    target_in_workspace = root_ancestor_exists(
                        ws, target_path)
                    # New folder under the archive: repoint + reparent + link.
                    self.conn.execute(
                        "UPDATE folders SET path = ?, parent_id = ? "
                        "WHERE id = ?",
                        (target_path, parent_id, sf["id"]),
                    )
                    # Use the non-committing variant so the folder path/parent_
                    # id UPDATE just above stays in the outer transaction — the
                    # public ``add_workspace_folder`` commits, and a mid-loop
                    # commit would persist a partial reparent that the outer
                    # rollback (below) could no longer undo if a later staged
                    # folder raised.
                    if target_in_workspace:
                        add_workspace_folder_no_commit(
                            ws, sf["id"], is_root=False)
                        # The staging scan registers each photo-bearing leaf as
                        # its own workspace ROOT (scanner restrict_dirs =>
                        # is_root=1). Once that leaf is folded under the existing
                        # archive base it must become a plain descendant,
                        # otherwise the merge leaves a stray second workspace
                        # root inside the archive — the exact overlap the
                        # tracked-ancestor guard was meant to prevent.
                        # add_workspace_folder's INSERT OR IGNORE can't downgrade
                        # an existing is_root=1 row, so demote it explicitly here.
                        self.conn.execute(
                            "UPDATE workspace_folders SET is_root = 0 "
                            "WHERE workspace_id = ? AND folder_id = ?",
                            (ws, sf["id"]),
                        )
                    else:
                        self.conn.execute(
                            "DELETE FROM workspace_folders "
                            "WHERE workspace_id = ? AND folder_id = ?",
                            (ws, sf["id"]),
                        )
                    # Every staged photo in a brand-new folder is newly
                    # archived.
                    new_count = self.conn.execute(
                        "SELECT COUNT(*) c FROM photos WHERE folder_id = ?",
                        (sf["id"],),
                    ).fetchone()["c"]
                    counts["new_photos"] += new_count
                    counts["new_folders"] += 1
                    # Record this folder's id so a child whose path-parent is
                    # this folder can resolve its parent even before counts
                    # re-query.
                    last_target_parent[target_path] = sf["id"]
                else:
                    # Existing folder: move photos in, drop filename-collisions.
                    # Restore the target row to visible status if it was marked
                    # ``missing`` — same rationale as the archive-base status
                    # flip above. We just verified files exist at
                    # ``target_path`` (rsync + verify), so a lingering
                    # ``missing`` from an earlier health scan would hide the
                    # newly-merged photos from workspace-scoped queries. Only
                    # migrate ``missing`` → ``ok``; leave ``partial``/``ok``
                    # alone. Runs inside the outer try/except so a later
                    # exception still rolls this back.
                    self.conn.execute(
                        "UPDATE folders SET status = 'ok' "
                        "WHERE id = ? AND status = 'missing'",
                        (target["id"],),
                    )
                    staged_photos = list(self.conn.execute(
                        "SELECT id, filename, file_hash, file_size "
                        "FROM photos WHERE folder_id = ?",
                        (sf["id"],),
                    ))
                    # Detect collisions against the ACTUAL target volume's case
                    # rules — not SQLite's default case-sensitive TEXT compare.
                    # On a case-insensitive volume (default macOS APFS, Windows
                    # NTFS), a target row/file named ``IMG.RAF`` and a staged
                    # ``img.raf`` are the same on-disk file: rsync
                    # ``--ignore-existing`` treats them as already present and
                    # skips the copy. A case-sensitive SQL match would miss
                    # that, fall into the else-branch reparent below, and land
                    # TWO catalog rows in one folder pointing at the same
                    # on-disk file (SQLite text-equality is case-sensitive, so
                    # UNIQUE(folder_id, filename) would not fire to catch the
                    # mistake). Build the collision map by normalizing
                    # filenames with the target filesystem's case rules
                    # instead. Probes ``target_path``'s deepest existing
                    # ancestor, so a fresh subfolder inherits its mount's
                    # behavior.
                    target_folds_case = (
                        case_insensitive_root(target_path) is not None)
                    normalize = (str.casefold if target_folds_case
                                 else (lambda s: s))
                    existing_by_key = {}
                    for row in self.conn.execute(
                        "SELECT id, filename, file_hash, file_size "
                        "FROM photos WHERE folder_id = ?",
                        (target["id"],),
                    ):
                        # First writer wins on the (unlikely) chance two
                        # case-alias rows already coexist in the target folder
                        # from a pre-fix catalog.
                        existing_by_key.setdefault(
                            normalize(row["filename"]),
                            {"id": row["id"], "filename": row["filename"],
                             "file_hash": row["file_hash"],
                             "file_size": row["file_size"]},
                        )

                    # A filename-collision alone is NOT enough to drop the
                    # staged photo as ``already_present``. The drop is only
                    # safe when the collision is REAL on disk — i.e. the
                    # target row accurately describes the bytes at
                    # ``target_path/filename``. rsync ``--ignore-existing``
                    # skipped the staged copy only when a byte-identical
                    # archived file was already there (a DIFFERING file
                    # would have aborted the move upstream in the
                    # content-conflict check). If the target catalog row is
                    # stale — its file was MISSING on disk before the
                    # archive step — the upstream check never fired (no
                    # dest file to compare) and rsync COPIED the staged
                    # bytes into place. By the time we run here rsync has
                    # already finished, so ``os.path.exists`` returns True
                    # in BOTH the real-collision and the phantom-row cases
                    # and cannot tell them apart. Require MATCHING recorded
                    # ``file_hash`` on both the staged photo and the target
                    # row to call a collision "real": a hash match means
                    # the row correctly describes what is on disk (dropping
                    # the staged row is safe); a hash mismatch means rsync
                    # replaced a missing file with fresh staged bytes and
                    # the row is stale. When either recorded hash is
                    # missing there is no reliable post-copy signal —
                    # ``file_size`` alone can coincidentally match a
                    # phantom row's stored size (empty XMP sidecars, small
                    # metadata files), and hashing the on-disk file to
                    # compare against the STAGED hash matches trivially in
                    # BOTH the real-collision case (byte-identical by
                    # definition) and the phantom case (rsync wrote the
                    # staged bytes). Default to phantom-replacement below
                    # when unverifiable: it preserves the freshly-imported
                    # pipeline output at the cost of any accumulated
                    # metadata on an unhashed archive row, which is the
                    # strictly-safer direction — silently dropping the
                    # newly-imported photo behind a same-size stale row is
                    # the opposite (and worse) failure. In the phantom
                    # case delete the stale target row and reparent the
                    # staged photo in its place so the surviving catalog
                    # row describes the bytes actually on disk. This also
                    # avoids a UNIQUE(folder_id, filename) violation from
                    # moving the staged row onto a folder that still holds
                    # the same basename.
                    #
                    # ``staged_normalized_claimed`` tracks case-normalized
                    # filenames already reparented into ``target`` in this
                    # pass — the intra-staged analogue of
                    # ``existing_by_key``. On a case-insensitive target
                    # volume, two staged files whose names differ only in
                    # case (e.g. staged on a case-sensitive disk archiving
                    # to APFS/SMB) collide on the same on-disk destination:
                    # rsync ``--ignore-existing`` writes only the FIRST
                    # file and silently skips the rest, so any later
                    # staged row describes bytes that never landed on
                    # disk. Without this tracker every such row also gets
                    # reparented into ``target``, leaving multiple catalog
                    # rows for the same on-disk file (the SQL
                    # ``UNIQUE(folder_id, filename)`` doesn't fire because
                    # the recorded filenames differ in case). Drop
                    # subsequent case-alias staged rows as
                    # ``already_present`` — the safe direction since their
                    # bytes are unrepresented on disk. On case-sensitive
                    # targets ``normalize`` is identity, so different-case
                    # names have different keys and this tracker never
                    # triggers.
                    #
                    # Mapped to the winning ``photos.id`` so a later
                    # intra-staged collision remaps its pending edits
                    # onto that survivor directly, without a follow-up
                    # ``LOWER(filename) = LOWER(?)`` probe: SQLite's
                    # built-in ``LOWER`` is ASCII-only, so
                    # ``LOWER('Ä.raf') != LOWER('ä.raf')`` would leave
                    # ``survivor_id`` unset and the cascade would drop
                    # the edit this branch exists to preserve.
                    staged_normalized_claimed = {}
                    for staged in staged_photos:
                        pid = staged["id"]
                        staged_norm = normalize(staged["filename"])
                        collision = existing_by_key.get(staged_norm)
                        intra_staged_collision = (
                            staged_norm in staged_normalized_claimed)
                        # The archived filename may differ in case from the
                        # staged one; probe for the ACTUAL archived name so
                        # the on-disk existence check matches on
                        # case-sensitive volumes too (where any case-alias
                        # check is pointless anyway).
                        target_filename = (collision["filename"]
                                           if collision else None)
                        target_disk_path = (
                            self._join_subtree_path(target_path, target_filename)
                            if target_filename is not None else None)
                        # A missing file on disk is definitely phantom
                        # (rsync would have written the staged bytes if
                        # this ever ran in production; the code path
                        # tolerates the isolated-unit-test case where no
                        # rsync happened).
                        target_on_disk = (
                            target_disk_path is not None
                            and os.path.exists(target_disk_path))
                        real_collision = False
                        if collision is not None and target_on_disk:
                            staged_hash = staged["file_hash"]
                            target_hash = collision["file_hash"]
                            if staged_hash and target_hash:
                                # Both hashes present → byte-identity
                                # comparison is reliable. Match → the
                                # target row's claim matches the file on
                                # disk (real collision). Mismatch → rsync
                                # replaced a missing file with fresh
                                # bytes; the target row is stale.
                                real_collision = (staged_hash == target_hash)
                            # else: at least one recorded hash is missing.
                            # Leave ``real_collision`` False so the
                            # phantom-replacement branch below runs — see
                            # the outer comment for why size alone (or a
                            # freshly-computed on-disk hash) can't safely
                            # stand in for the recorded-hash comparison
                            # here.
                        if real_collision or intra_staged_collision:
                            # photo_keywords.photo_id has no ON DELETE CASCADE
                            # (unlike every other photo_id FK), so clear
                            # keyword links before deleting the photo or the
                            # FK fires.
                            #
                            # ``intra_staged_collision`` shares this branch
                            # for the same net effect: the staged row's
                            # bytes are not represented on disk (an earlier
                            # staged case-alias already claimed the slot,
                            # rsync ``--ignore-existing`` skipped this
                            # file), so treating it as ``already_present``
                            # is correct.
                            #
                            # Reparent any pending edits queued against the
                            # staged row onto the surviving photo before the
                            # DELETE fires the ON DELETE CASCADE on
                            # ``pending_changes.photo_id`` and drops them.
                            # An edit queued between the pre-transfer sync's
                            # last drain and this reconciliation is still a
                            # write the user asked for: the survivor points
                            # at the same on-disk file (real-collision → the
                            # byte-identical archived row; intra-staged →
                            # the earlier staged twin that already claimed
                            # the normalized slot in ``target["id"]``), so
                            # its sidecar is the one the edit was aimed at.
                            # Without this remap the cascade would silently
                            # discard the edit and the residual re-read
                            # below would find nothing to report.
                            # Prefer the intra-staged winner over a stale
                            # ``collision`` entry. ``existing_by_key`` is
                            # built once from the target folder and is not
                            # refreshed when the phantom-replacement branch
                            # below deletes ``collision["id"]``. If an
                            # earlier iteration hit that branch on the same
                            # case-normalized name, ``existing_by_key[
                            # staged_norm]`` still points at the deleted
                            # phantom while ``staged_normalized_claimed[
                            # staged_norm]`` holds the live winner. Reading
                            # the tracker first also sidesteps SQLite's
                            # ASCII-only ``LOWER``, which cannot match
                            # non-ASCII case aliases like ``Ä.raf`` /
                            # ``ä.raf`` and would otherwise leave the
                            # remap unset.
                            #
                            # An in-staging survivor is still in the
                            # captured ``staged_photo_ids`` and the
                            # residual re-read already picks its remapped
                            # edits up — adding the same count again would
                            # report one edit as two. An off-staging
                            # survivor (the byte-identical archive row) is
                            # invisible to a photo-id-scoped residual
                            # re-read, so the caller has to add its remap
                            # count separately. See
                            # ``_residual_staged_changes``.
                            if intra_staged_collision:
                                survivor_id = staged_normalized_claimed.get(
                                    staged_norm)
                                survivor_off_staging = False
                            else:
                                survivor_id = (
                                    collision["id"] if collision is not None
                                    else None)
                                survivor_off_staging = collision is not None
                            if survivor_id is not None:
                                # A queued ``location`` change stores its
                                # coordinates only in the deleted photo's
                                # ``photo_keywords`` link to a
                                # ``type='location'`` keyword; the delete
                                # below strips those links and
                                # ``sync_to_xmp`` would otherwise derive
                                # coordinates from whatever unrelated
                                # location keyword (or none) the survivor
                                # carries, silently writing the wrong GPS
                                # -- or clearing it -- for the remapped
                                # row. Move the location state across when
                                # the staged row holds the newer queued
                                # assignment; the helper leaves the
                                # survivor's own tags alone when ITS
                                # queued change is newer.
                                move_location_state(pid, survivor_id)
                                # Opposing keyword edits on the two rows
                                # would fold into a rename pair once they
                                # share a photo and cancel the newer one
                                # out. Resolve before anything reads the
                                # queue: the identity capture and the
                                # sibling-workspace scan below must see the
                                # rows that actually survive.
                                reconcile_keyword_edits(
                                    pid, survivor_id)
                                # ``photos.rating`` / ``photos.flag`` live
                                # outside the queue row, so they have to
                                # travel with it or the survivor's catalog
                                # keeps a value the sidecar no longer has.
                                transfer_review_state(
                                    pid, survivor_id)
                                # Capture the identities of the rows this
                                # remap is about to move -- restricted to
                                # the active workspace so sibling-workspace
                                # edits (which this sync would not have
                                # written anyway) don't inflate the caller's
                                # "queued during transfer" count. Read
                                # before the UPDATE, because after it the
                                # rows now live on ``survivor_id`` and the
                                # caller has no way to distinguish them
                                # from anything the survivor already
                                # carried.
                                off_staging_row_identities = []
                                if survivor_off_staging:
                                    off_staging_row_identities = [
                                        (row["change_token"]
                                         or ("id", row["id"]))
                                        for row in self.conn.execute(
                                            "SELECT id, change_token "
                                            "FROM pending_changes "
                                            "WHERE photo_id = ? "
                                            "  AND workspace_id = ?",
                                            (pid, ws),
                                        ).fetchall()
                                    ]
                                # Sibling workspaces owning rows this remap
                                # will move. Read before the UPDATE for the
                                # same reason as above: afterwards these rows
                                # are indistinguishable from the survivor's
                                # own. Their folder link is deferred to the
                                # end of the merge, where the survivor's
                                # final ``folder_id`` is settled.
                                sibling_ws_ids = [
                                    r["workspace_id"] for r in
                                    self.conn.execute(
                                        "SELECT DISTINCT workspace_id "
                                        "FROM pending_changes "
                                        "WHERE photo_id = ? "
                                        "  AND workspace_id IS NOT NULL "
                                        "  AND workspace_id != ?",
                                        (pid, ws),
                                    ).fetchall()
                                ]
                                for sibling_ws in sibling_ws_ids:
                                    sibling_links.add(
                                        (sibling_ws, survivor_id))
                                remap = self.conn.execute(
                                    "UPDATE pending_changes "
                                    "SET photo_id = ? WHERE photo_id = ?",
                                    (survivor_id, pid),
                                )
                                counts["preserved_edit_count"] += (
                                    remap.rowcount or 0)
                                if off_staging_row_identities:
                                    counts[
                                        "preserved_off_staging_identities"
                                    ].extend(off_staging_row_identities)
                            self.conn.execute(
                                "DELETE FROM photo_keywords "
                                "WHERE photo_id = ?",
                                (pid,))
                            self.conn.execute(
                                "DELETE FROM photos WHERE id = ?", (pid,))
                            collection_remap[pid] = survivor_id
                            counts["already_present"] += 1
                            # The staged photo id is now free. Thumbnails,
                            # previews, working copies, and offline cache files
                            # were keyed off this id, and SQLite reuses freed
                            # rowids — a later import that lands on this id
                            # would inherit stale imagery. Report the id up so
                            # the caller can drop those files.
                            counts["dropped_photo_ids"].append(pid)
                        else:
                            if collision is not None:
                                # Filename collided (case-normalized) but the
                                # target row is a phantom — either the
                                # archived file was missing on disk (rsync
                                # copied the staged bytes into the empty
                                # slot) or the file is there but its
                                # bytes-identity (hash/size) doesn't match
                                # the row's claim (rsync replaced a missing
                                # file with fresh staged bytes). Either way
                                # the staged row correctly describes what's
                                # on disk. Drop the phantom by id so the
                                # reparent below can take its (folder_id,
                                # filename) slot and represent the real file.
                                # Deleting by id (not filename) is required on
                                # case-insensitive volumes where the staged
                                # and phantom filenames differ only in case:
                                # the SQL ``filename = ?`` lookup used earlier
                                # would miss the stale row and leave both
                                # intact.
                                #
                                # Reparent the phantom's pending edits onto
                                # the staged row that is about to take its
                                # slot. The staged bytes are what will live
                                # at that (folder_id, filename), so any
                                # queued write is aimed at that sidecar --
                                # letting the cascade drop it would silently
                                # discard the user's edit.
                                #
                                # A queued ``location`` change on the
                                # phantom stores its coordinates only in
                                # the phantom's ``photo_keywords`` link to
                                # a ``type='location'`` keyword; the
                                # DELETE below strips those links and
                                # ``sync_to_xmp`` would otherwise derive
                                # coordinates from whatever unrelated
                                # location tag (or none) the staged
                                # survivor carries, silently writing the
                                # wrong GPS -- or clearing it -- for the
                                # remapped row. Move the phantom's
                                # location keyword links onto the survivor
                                # before the delete so the queued edit's
                                # intent survives -- unless the survivor
                                # holds a NEWER queued location change, the
                                # replacement-import shape where a fresh
                                # assignment on the staged row would be
                                # reverted by the stale archive row's. The
                                # helper resolves that by queue chronology,
                                # identically in the collision→staged
                                # branch above.
                                move_location_state(
                                    collision["id"], pid)
                                # Same reconciliation as the branch above,
                                # and for the same reason it runs here: an
                                # older add on one row must not reverse a
                                # newer remove on the other once the remap
                                # puts them on one photo.
                                reconcile_keyword_edits(
                                    collision["id"], pid)
                                # Same carry-over of the catalog columns the
                                # queue row does not hold.
                                transfer_review_state(
                                    collision["id"], pid)
                                # Sibling workspaces owning phantom rows this
                                # remap will move onto the staged survivor.
                                # Read before the UPDATE; the link itself is
                                # deferred to the end of the merge, after the
                                # survivor has been reparented into the
                                # archive folder.
                                for sibling_ws in [
                                    r["workspace_id"] for r in
                                    self.conn.execute(
                                        "SELECT DISTINCT workspace_id "
                                        "FROM pending_changes "
                                        "WHERE photo_id = ? "
                                        "  AND workspace_id IS NOT NULL "
                                        "  AND workspace_id != ?",
                                        (collision["id"], ws),
                                    ).fetchall()
                                ]:
                                    sibling_links.add((sibling_ws, pid))
                                remap = self.conn.execute(
                                    "UPDATE pending_changes "
                                    "SET photo_id = ? WHERE photo_id = ?",
                                    (pid, collision["id"]),
                                )
                                # No ``preserved_off_staging_identities``
                                # bump: the survivor is ``pid``, still one
                                # of the ids the caller captured before
                                # the merge, so a residual re-read scoped
                                # by those ids already finds the remapped
                                # edits.
                                counts["preserved_edit_count"] += (
                                    remap.rowcount or 0)
                                self.conn.execute(
                                    "DELETE FROM photo_keywords "
                                    "WHERE photo_id = ?", (collision["id"],))
                                self.conn.execute(
                                    "DELETE FROM photos WHERE id = ?",
                                    (collision["id"],))
                                collection_remap[collision["id"]] = pid
                                # The phantom target-row id is likewise freed —
                                # its cache files can be reused for a new
                                # photo. Report it up for cleanup too.
                                counts["dropped_photo_ids"].append(
                                    collision["id"])
                            self.conn.execute(
                                "UPDATE photos SET folder_id = ? "
                                "WHERE id = ?",
                                (target["id"], pid),
                            )
                            # A photo moved into a pre-existing archive folder
                            # is still a newly-archived photo from the user's
                            # view.
                            counts["new_photos"] += 1
                            # Claim the case-normalized slot so a later
                            # staged row whose filename case-folds to this
                            # name is dropped as ``already_present``
                            # instead of adding a second catalog row for
                            # the same on-disk destination.
                            staged_normalized_claimed[staged_norm] = pid
                    to_delete.append(sf["id"])
                    counts["merged_folders"] += 1
                    last_target_parent[target_path] = target["id"]

            # Delete deepest-first: ``staged_folders`` (hence ``to_delete``)
            # is shallowest-first, so reverse to remove children before
            # parents and never orphan a still-referenced ``parent_id``.
            # Drop the folder's workspace links first —
            # ``workspace_folders.folder_id`` has no ON DELETE CASCADE, so
            # the folder delete would hit a FK violation.
            for fid in reversed(to_delete):
                self.conn.execute(
                    "DELETE FROM workspace_folders WHERE folder_id = ?",
                    (fid,))
                self.conn.execute("DELETE FROM folders WHERE id = ?", (fid,))

            # Last, after every survivor's ``folder_id`` is final and the
            # staged folder rows (and their workspace links) are gone: give
            # each sibling workspace whose queued edits were remapped a way
            # to resolve the survivor. Without it those rows stay queued and
            # fail every future sync as inaccessible, with nothing reporting
            # why.
            for sibling_ws, survivor_photo_id in sorted(sibling_links):
                link_survivor_for_sibling_edits(
                    sibling_ws, survivor_photo_id)

            remap_collection_photo_ids(self.conn, collection_remap)

            self.conn.commit()
            invalidate_new_images([ws])
        except Exception:
            self.conn.rollback()
            raise
        update_folder_counts()
        return counts
