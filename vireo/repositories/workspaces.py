"""Persistence for workspaces: rows, config overrides, tabs, and snapshots.

``Database`` owns the active-workspace state and the process-wide new-images
cache; this repository owns the SQL. Methods that act on the active
workspace (tabs, new-images snapshots) resolve it through
``workspace_id_fn`` (``Database._ws_id``) before they validate anything or
run any SQL, so with no workspace active they raise ``RuntimeError`` having
touched nothing. Building the repository never resolves it. Catalog-wide
methods take the workspace id as an argument and work with none active.

Callers reach it as ``db.workspaces`` (a fresh repository per access, see
``Database.workspaces``); there are no forwarding wrappers on ``Database``.
``create_workspace``, ``delete_workspace`` and ``ensure_default_workspace``
stay on ``Database`` because they also maintain the new-images cache (or
create the row through ``create_workspace``); call those rather than
:meth:`create` / :meth:`delete`. ``get_new_images_snapshot`` stays there too,
for its id range check.
"""

import json
import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from repositories import UNSET


class WorkspaceRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        workspace_id_fn: Callable[[], int] | None,
        *,
        allowed_nav_ids: Iterable[str],
        default_tabs: Sequence[str],
        nav_id_aliases: Mapping[str, str] | None = None,
        chunk_size: int = 800,
    ) -> None:
        self.conn = conn
        self.workspace_id_fn = workspace_id_fn
        self.allowed_nav_ids = allowed_nav_ids
        self.default_tabs = list(default_tabs)
        # Map of retired nav id -> current nav id, so a saved tab written
        # under an old id (e.g. "compare" before it was renamed to
        # "id_conflicts") is upgraded on read instead of silently dropped.
        self.nav_id_aliases = dict(nav_id_aliases or {})
        self.chunk_size = chunk_size

    # -- workspace rows ------------------------------------------------------

    def most_recently_opened_id(self) -> int | None:
        """Return the id of the last-opened workspace, or None if none exist."""
        last = self.conn.execute(
            "SELECT id FROM workspaces ORDER BY CASE WHEN last_opened_at IS NULL THEN 0 ELSE 1 END DESC, last_opened_at DESC, id ASC LIMIT 1"
        ).fetchone()
        return None if last is None else last[0]

    def default_id(self) -> int | None:
        """Return the id of the workspace named 'Default', or None."""
        row = self.conn.execute(
            "SELECT id FROM workspaces WHERE name = 'Default'"
        ).fetchone()
        return row[0] if row else None

    def id_for_name(self, name: str) -> int | None:
        """Return the id of the workspace named exactly ``name``, or None."""
        row = self.conn.execute(
            "SELECT id FROM workspaces WHERE name = ?", (name,),
        ).fetchone()
        return None if row is None else row["id"]

    def ids_for_folders(self, folder_ids: Iterable[int]) -> set[int]:
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
                f"SELECT DISTINCT workspace_id FROM workspace_visible_folders "
                f"WHERE folder_id IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
            ws_ids.update(r["workspace_id"] for r in rows)
        return ws_ids

    def create(self, name: str, config_overrides: dict | None = None,
               ui_state: dict | None = None) -> int:
        """Insert a workspace with the default tabs and commit. Returns its id.

        Call ``Database.create_workspace`` instead: it also clears any stale
        new-images cache entry for a reused rowid.
        """
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

    def get(self, workspace_id: int | None) -> sqlite3.Row | None:
        """Return a single workspace by id, or None."""
        return self.conn.execute(
            "SELECT * FROM workspaces WHERE id = ?", (workspace_id,)
        ).fetchone()

    def list_all(self) -> list[sqlite3.Row]:
        """Return all workspaces, pinned first then alphabetical."""
        return self.conn.execute(
            "SELECT * FROM workspaces "
            "ORDER BY (pinned_at IS NULL), LOWER(name)"
        ).fetchall()

    def update(self, workspace_id: int, name: str | None = None,
               config_overrides: Any = UNSET, ui_state: Any = UNSET,
               last_opened_at: str | None = None, pinned_at: Any = UNSET) -> None:
        """Update the provided fields and commit; nothing given, nothing written.

        For ``config_overrides``, ``ui_state`` and ``pinned_at``, pass None to
        clear the column (set it NULL), or omit the argument to leave it
        unchanged.
        """
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

    def delete(self, workspace_id: int) -> None:
        """Delete a workspace (cascading its scoped rows) and commit.

        Call ``Database.delete_workspace`` instead: it also drops the
        workspace's cached new-images payload.

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

    def set_group_state(self, workspace_id: int, fingerprint: str | None,
                        when_ts: int | None) -> None:
        """Record that grouping completed for ``workspace_id`` at ``when_ts``
        with the given ``fingerprint``, and commit. The pipeline page treats
        a fingerprint mismatch as "Outdated" so the user knows a regroup is
        pending.
        """
        self.conn.execute(
            "UPDATE workspaces SET last_grouped_at = ?, last_group_fingerprint = ? "
            "WHERE id = ?",
            (when_ts, fingerprint, workspace_id),
        )
        self.conn.commit()

    # -- config overrides ----------------------------------------------------

    def forget_label_file(self, labels_file: str) -> int:
        """Drop ``labels_file`` from every workspace's active_labels override.

        Deleting a set in Settings removes the file and the global active
        list, but a workspace override pointing at it used to survive: a
        selection naming a file that no longer exists, which no checkbox can
        clear because the UI only lists files it can find. Returns the number
        of workspaces changed; commits only if any did.
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

    # -- active workspace: new-images snapshots ------------------------------

    def create_new_images_snapshot(self, file_paths: Iterable[str] | None) -> int:
        """Persist a deduplicated, sorted snapshot of paths for the active
        workspace, and commit. Returns its id."""
        ws_id = self.workspace_id_fn()
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

    def get_new_images_snapshot(self, snapshot_id: int) -> dict[str, Any] | None:
        """Return the active workspace's snapshot metadata and paths, or None.

        ``snapshot_id`` must already be within SQLite's signed 64-bit range;
        ``Database.get_new_images_snapshot`` checks that before this resolves
        the active workspace.
        """
        ws_id = self.workspace_id_fn()
        row = self.conn.execute(
            "SELECT id, workspace_id, created_at, file_count "
            "FROM new_image_snapshots WHERE id = ? AND workspace_id = ?",
            (snapshot_id, ws_id),
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

    def get_tabs(self) -> list[str]:
        """Return the active workspace's ordered list of pinned tab nav-ids.

        Entries not in ``allowed_nav_ids`` (``ALL_NAV_IDS``) are dropped so
        that pages retired in past releases (e.g. ``zoom_test``) don't leave
        dead slots in the navbar's ``TABS`` array — a dead id makes
        cmd+number reserve a slot that renders nothing and makes
        ``adjacentTabId()`` return an id that ``pageById`` doesn't know, which
        throws on close-adjacent. Retired ids with a successor in
        ``nav_id_aliases`` are upgraded instead.
        """
        return self._read_tabs(self.workspace_id_fn())

    def _read_tabs(self, workspace_id):
        row = self.conn.execute(
            "SELECT tabs FROM workspaces WHERE id=?", (workspace_id,),
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

    def set_tabs(self, tabs: list[str]) -> list[str]:
        """Replace the active workspace's tabs with the given ordered list.

        Validates every entry against ``allowed_nav_ids`` and rejects
        duplicates (``ValueError``), so the UI invariant "each pinned page
        appears exactly once" is enforced at the storage layer. Commits and
        returns the new list.
        """
        workspace_id = self.workspace_id_fn()
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
        self._write(workspace_id, tabs)
        return list(tabs)

    def pin_tab(self, nav_id: str) -> list[str]:
        """Append ``nav_id`` to the active workspace's tabs if not present.

        Raises ``ValueError`` if ``nav_id`` is not a known nav id. Returns the
        new list (committing only when it changed).
        """
        workspace_id = self.workspace_id_fn()
        self._validate_nav_id(nav_id)
        tabs = self._read_tabs(workspace_id)
        if nav_id not in tabs:
            tabs.append(nav_id)
            self._write(workspace_id, tabs)
        return tabs

    def unpin_tab(self, nav_id: str) -> list[str]:
        """Remove ``nav_id`` from the active workspace's tabs if present.

        Raises ``ValueError`` if ``nav_id`` is not a known nav id. Returns the
        new list (committing only when it changed).
        """
        workspace_id = self.workspace_id_fn()
        self._validate_nav_id(nav_id)
        tabs = self._read_tabs(workspace_id)
        if nav_id in tabs:
            tabs = [tab for tab in tabs if tab != nav_id]
            self._write(workspace_id, tabs)
        return tabs

    def _validate_nav_id(self, nav_id):
        if nav_id not in self.allowed_nav_ids:
            raise ValueError(f"{nav_id!r} is not a known nav id")

    def _write(self, workspace_id, tabs):
        self.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(tabs), workspace_id),
        )
        self.conn.commit()

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )

    def summaries_for_ids(self, workspace_ids: Sequence[int]) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in workspace_ids)
        return self.conn.execute(
            f"SELECT id, name FROM workspaces WHERE id IN ({placeholders})",
            tuple(workspace_ids),
        ).fetchall()
