"""Persistence for workspaces: rows, config overrides, tabs, and snapshots.

``Database`` owns the active-workspace state and the process-wide new-images
cache; this repository owns the SQL. Methods that act on the active
workspace (tabs, new-images snapshots) use ``self.workspace_id``, which the
façade resolves with ``Database._ws_id()`` when it builds the repository.
Catalog-wide methods take the workspace id as an argument, matching the
``Database`` method they back.
"""

import json
import sqlite3

from repositories import UNSET


class WorkspaceRepository:
    def __init__(self, conn, workspace_id, *, allowed_nav_ids, default_tabs,
                 nav_id_aliases=None, chunk_size=800):
        self.conn = conn
        self.workspace_id = workspace_id
        self.allowed_nav_ids = allowed_nav_ids
        self.default_tabs = list(default_tabs)
        # Map of retired nav id -> current nav id, so a saved tab written
        # under an old id (e.g. "compare" before it was renamed to
        # "id_conflicts") is upgraded on read instead of silently dropped.
        self.nav_id_aliases = dict(nav_id_aliases or {})
        self.chunk_size = chunk_size

    # -- workspace rows ------------------------------------------------------

    def most_recently_opened_id(self):
        """Return the id of the last-opened workspace, or None if none exist."""
        last = self.conn.execute(
            "SELECT id FROM workspaces ORDER BY CASE WHEN last_opened_at IS NULL THEN 0 ELSE 1 END DESC, last_opened_at DESC, id ASC LIMIT 1"
        ).fetchone()
        return None if last is None else last[0]

    def default_id(self):
        """Return the id of the workspace named 'Default', or None."""
        row = self.conn.execute(
            "SELECT id FROM workspaces WHERE name = 'Default'"
        ).fetchone()
        return row[0] if row else None

    def ids_for_folders(self, folder_ids):
        """Return the set of workspace ids linked to any of ``folder_ids``."""
        # Chunk to stay well under SQLite's SQLITE_MAX_VARIABLE_NUMBER (default 999).
        # A scan of a deep tree can auto-register thousands of descendant folders;
        # a single IN (?, ?, ...) across all of them would raise
        # ``OperationalError: too many SQL variables``.
        CHUNK = 500
        ws_ids = set()
        folder_ids = list(folder_ids)
        for i in range(0, len(folder_ids), CHUNK):
            chunk = folder_ids[i:i + CHUNK]
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"SELECT DISTINCT workspace_id FROM workspace_folders "
                f"WHERE folder_id IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
            ws_ids.update(r["workspace_id"] for r in rows)
        return ws_ids

    def create(self, name, config_overrides=None, ui_state=None):
        """Insert a workspace with the default tabs and commit. Returns its id."""
        cur = self.conn.execute(
            """INSERT INTO workspaces (name, config_overrides, ui_state, tabs)
               VALUES (?, ?, ?, ?)""",
            (name,
             json.dumps(config_overrides) if config_overrides else None,
             json.dumps(ui_state) if ui_state else None,
             json.dumps(self.default_tabs)),
        )
        self.conn.commit()
        return cur.lastrowid

    def get(self, workspace_id):
        return self.conn.execute(
            "SELECT * FROM workspaces WHERE id = ?", (workspace_id,)
        ).fetchone()

    def list_all(self):
        """Return all workspaces, pinned first then alphabetical."""
        return self.conn.execute(
            "SELECT * FROM workspaces "
            "ORDER BY (pinned_at IS NULL), LOWER(name)"
        ).fetchall()

    def update(self, workspace_id, name=None, config_overrides=UNSET,
               ui_state=UNSET, last_opened_at=None, pinned_at=UNSET):
        """Update the provided fields; ``None`` clears the JSON/pin columns."""
        updates = []
        params = []
        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if config_overrides is not UNSET:
            updates.append("config_overrides = ?")
            params.append(json.dumps(config_overrides) if config_overrides is not None else None)
        if ui_state is not UNSET:
            updates.append("ui_state = ?")
            params.append(json.dumps(ui_state) if ui_state is not None else None)
        if last_opened_at is not None:
            updates.append("last_opened_at = ?")
            params.append(last_opened_at)
        if pinned_at is not UNSET:
            updates.append("pinned_at = ?")
            params.append(pinned_at)
        if not updates:
            return
        params.append(workspace_id)
        self.conn.execute(
            f"UPDATE workspaces SET {', '.join(updates)} WHERE id = ?", params
        )
        self.conn.commit()

    def delete(self, workspace_id):
        """Delete a workspace (cascading its scoped rows) and commit.

        A pending NAS transfer blocks the delete through a trigger; that
        refusal is raised as ``ValueError`` so routes can show it.
        """
        try:
            self.conn.execute("DELETE FROM workspaces WHERE id = ?", (workspace_id,))
        except sqlite3.IntegrityError as e:
            if "Send pending photos to NAS" in str(e):
                raise ValueError(str(e)) from e
            raise
        self.conn.commit()

    def set_group_state(self, workspace_id, fingerprint, when_ts):
        self.conn.execute(
            "UPDATE workspaces SET last_grouped_at = ?, last_group_fingerprint = ? "
            "WHERE id = ?",
            (when_ts, fingerprint, workspace_id),
        )
        self.conn.commit()

    # -- config overrides ----------------------------------------------------

    def forget_label_file(self, labels_file):
        """Drop ``labels_file`` from every workspace's active_labels override.

        Returns the number of workspaces changed; commits only if any did.
        """
        rows = self.conn.execute(
            "SELECT id, config_overrides FROM workspaces "
            "WHERE config_overrides IS NOT NULL"
        ).fetchall()
        changed = 0
        for row in rows:
            try:
                overrides = json.loads(row["config_overrides"])
            except (TypeError, ValueError):
                continue
            if not isinstance(overrides, dict):
                continue
            active = overrides.get("active_labels")
            if not isinstance(active, list) or labels_file not in active:
                continue
            overrides["active_labels"] = [
                path for path in active if path != labels_file
            ]
            self.conn.execute(
                "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
                (json.dumps(overrides), row["id"]),
            )
            changed += 1
        if changed:
            self.conn.commit()
        return changed

    def rewrite_legacy_miss_thresholds(
        self, legacy_det, legacy_burst, new_det, new_burst
    ):
        """Rewrite the exact legacy miss-threshold pair in every workspace."""
        rows = self.conn.execute(
            "SELECT id, config_overrides FROM workspaces "
            "WHERE config_overrides IS NOT NULL"
        ).fetchall()
        updated = 0
        for row in rows:
            raw = row["config_overrides"]
            try:
                overrides = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(overrides, dict):
                continue
            pipeline = overrides.get("pipeline")
            if not isinstance(pipeline, dict):
                continue
            if (
                pipeline.get("miss_det_confidence") != legacy_det
                or pipeline.get("miss_det_confidence_burst") != legacy_burst
            ):
                continue
            pipeline["miss_det_confidence"] = new_det
            pipeline["miss_det_confidence_burst"] = new_burst
            self.conn.execute(
                "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
                (json.dumps(overrides), row["id"]),
            )
            updated += 1
        if updated:
            self.conn.commit()
        return updated

    def rewrite_legacy_w_species_default(self, legacy, new):
        """Rewrite the exact legacy ``pipeline.w_species`` in every workspace."""
        rows = self.conn.execute(
            "SELECT id, config_overrides FROM workspaces "
            "WHERE config_overrides IS NOT NULL"
        ).fetchall()
        updated = 0
        for row in rows:
            raw = row["config_overrides"]
            try:
                overrides = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(overrides, dict):
                continue
            pipeline = overrides.get("pipeline")
            if not isinstance(pipeline, dict):
                continue
            if pipeline.get("w_species") != legacy:
                continue
            pipeline["w_species"] = new
            self.conn.execute(
                "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
                (json.dumps(overrides), row["id"]),
            )
            updated += 1
        if updated:
            self.conn.commit()
        return updated

    def rewrite_legacy_eye_detect_default(self):
        """Turn a legacy ``eye_detect_enabled=True`` override off, clearing
        the rewritten workspace's group fingerprint."""
        rows = self.conn.execute(
            "SELECT id, config_overrides FROM workspaces "
            "WHERE config_overrides IS NOT NULL"
        ).fetchall()
        updated = 0
        for row in rows:
            raw = row["config_overrides"]
            try:
                overrides = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(overrides, dict):
                continue
            pipeline = overrides.get("pipeline")
            if not isinstance(pipeline, dict):
                continue
            if pipeline.get("eye_detect_enabled") is not True:
                continue
            pipeline["eye_detect_enabled"] = False
            self.conn.execute(
                "UPDATE workspaces "
                "SET config_overrides = ?, last_group_fingerprint = NULL "
                "WHERE id = ?",
                (json.dumps(overrides), row["id"]),
            )
            updated += 1
        if updated:
            self.conn.commit()
        return updated

    def invalidate_group_fingerprints_without_explicit_eye_false(self):
        """Clear ``last_group_fingerprint`` unless the workspace explicitly
        overrides ``pipeline.eye_detect_enabled`` to False."""
        rows = self.conn.execute(
            "SELECT id, config_overrides FROM workspaces "
            "WHERE last_group_fingerprint IS NOT NULL"
        ).fetchall()
        to_invalidate = []
        for row in rows:
            raw = row["config_overrides"]
            has_explicit_false = False
            if raw is not None:
                try:
                    overrides = json.loads(raw) if isinstance(raw, str) else raw
                except (json.JSONDecodeError, TypeError):
                    overrides = None
                if isinstance(overrides, dict):
                    pipeline = overrides.get("pipeline")
                    if isinstance(pipeline, dict) and pipeline.get("eye_detect_enabled") is False:
                        has_explicit_false = True
            if not has_explicit_false:
                to_invalidate.append(row["id"])
        if to_invalidate:
            for chunk in self._chunks(to_invalidate):
                placeholders = ",".join("?" * len(chunk))
                self.conn.execute(
                    f"UPDATE workspaces SET last_group_fingerprint = NULL "
                    f"WHERE id IN ({placeholders})",
                    list(chunk),
                )
            self.conn.commit()
        return len(to_invalidate)

    # -- active workspace: new-images snapshots ------------------------------

    def create_new_images_snapshot(self, file_paths):
        """Persist a deduplicated, sorted snapshot of paths. Returns its id."""
        ws_id = self.workspace_id
        unique_paths = sorted(set(file_paths or []))
        cur = self.conn.execute(
            "INSERT INTO new_image_snapshots (workspace_id, created_at, file_count) "
            "VALUES (?, datetime('now'), ?)",
            (ws_id, len(unique_paths)),
        )
        snap_id = cur.lastrowid
        if unique_paths:
            self.conn.executemany(
                "INSERT INTO new_image_snapshot_files (snapshot_id, file_path) VALUES (?, ?)",
                [(snap_id, p) for p in unique_paths],
            )
        self.conn.commit()
        return snap_id

    def get_new_images_snapshot(self, snapshot_id):
        """Return the active workspace's snapshot metadata and paths, or None.

        ``snapshot_id`` must already be within SQLite's signed 64-bit range;
        the façade checks that before it resolves the active workspace.
        """
        row = self.conn.execute(
            "SELECT id, workspace_id, created_at, file_count "
            "FROM new_image_snapshots WHERE id = ? AND workspace_id = ?",
            (snapshot_id, self.workspace_id),
        ).fetchone()
        if row is None:
            return None
        paths = [
            r["file_path"]
            for r in self.conn.execute(
                "SELECT file_path FROM new_image_snapshot_files WHERE snapshot_id = ? "
                "ORDER BY file_path",
                (snapshot_id,),
            ).fetchall()
        ]
        return {
            "id": row["id"],
            "workspace_id": row["workspace_id"],
            "created_at": row["created_at"],
            "file_count": row["file_count"],
            "file_paths": paths,
        }

    # -- active workspace: navigation tabs -----------------------------------

    def get_tabs(self):
        row = self.conn.execute(
            "SELECT tabs FROM workspaces WHERE id=?", (self.workspace_id,),
        ).fetchone()
        if not row or not row["tabs"]:
            return list(self.default_tabs)
        try:
            value = json.loads(row["tabs"]) if isinstance(row["tabs"], str) else row["tabs"]
        except (json.JSONDecodeError, TypeError):
            return list(self.default_tabs)
        if not isinstance(value, list):
            return list(self.default_tabs)
        result = []
        seen = set()
        for tab in value:
            if not isinstance(tab, str):
                continue
            tab = self.nav_id_aliases.get(tab, tab)
            if tab in self.allowed_nav_ids and tab not in seen:
                seen.add(tab)
                result.append(tab)
        return result

    def set_tabs(self, tabs):
        if not isinstance(tabs, list):
            raise ValueError("tabs must be a list")
        seen = set()
        for nav_id in tabs:
            if not isinstance(nav_id, str):
                raise ValueError(
                    f"tab id must be a string, got {type(nav_id).__name__}"
                )
            if nav_id not in self.allowed_nav_ids:
                raise ValueError(f"{nav_id!r} is not a known nav id")
            if nav_id in seen:
                raise ValueError(f"{nav_id!r} appears more than once")
            seen.add(nav_id)
        self._write(tabs)
        return list(tabs)

    def pin_tab(self, nav_id):
        self._validate_nav_id(nav_id)
        tabs = self.get_tabs()
        if nav_id not in tabs:
            tabs.append(nav_id)
            self._write(tabs)
        return tabs

    def unpin_tab(self, nav_id):
        self._validate_nav_id(nav_id)
        tabs = self.get_tabs()
        if nav_id in tabs:
            tabs = [tab for tab in tabs if tab != nav_id]
            self._write(tabs)
        return tabs

    def _validate_nav_id(self, nav_id):
        if nav_id not in self.allowed_nav_ids:
            raise ValueError(f"{nav_id!r} is not a known nav id")

    def _write(self, tabs):
        self.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(tabs), self.workspace_id),
        )
        self.conn.commit()

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
