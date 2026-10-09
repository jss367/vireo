"""Photo-specific library membership, independent of folder membership.

Sync-only grants remain separate: this table intentionally grants browse,
edit and library visibility to a selected photo without exposing its siblings.

Callers reach it as ``db.photo_visibility`` (a fresh repository per access,
see ``Database.photo_visibility``); there are no forwarding wrappers on
``Database``. The grant, revoke and folder-status writes take the workspace
id as an argument and never resolve the active one; they run in the caller's
transaction and do not commit. ``visible_photo_ids``, ``affected_workspaces``
and ``preserve_for_move`` act on the active workspace, which they resolve
through ``workspace_id_fn`` (``Database._ws_id``) before running any SQL, so
with no workspace active they raise ``RuntimeError`` having touched nothing.
"""

import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any


class PhotoVisibilityRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        workspace_id_fn: Callable[[], int] | None = None,
        *,
        chunk_size: int = 800,
    ) -> None:
        self.conn = conn
        self.workspace_id_fn = workspace_id_fn
        self.chunk_size = chunk_size

    def visible_photo_ids(self, photo_ids: Iterable[int]) -> list[int]:
        """Existing, active-workspace-visible ids in caller order, deduplicated."""
        workspace_id = self.workspace_id_fn()
        requested = list(dict.fromkeys(photo_ids))
        visible = set()
        for start in range(0, len(requested), self.chunk_size):
            chunk = requested[start:start + self.chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                "SELECT p.id FROM photos p "
                "JOIN photo_workspace_visibility wf ON wf.photo_id = p.id "
                f"WHERE wf.workspace_id = ? AND p.id IN ({placeholders})",
                [workspace_id, *chunk],
            ).fetchall()
            visible.update(row["id"] for row in rows)
        return [photo_id for photo_id in requested if photo_id in visible]

    def grant(self, workspace_id: int, photo_ids: Iterable[int]) -> None:
        for photo_id in dict.fromkeys(photo_ids):
            self.conn.execute(
                "INSERT OR IGNORE INTO workspace_photos (workspace_id, photo_id) VALUES (?, ?)",
                (workspace_id, photo_id),
            )

    def revoke_for_folders(self, workspace_id: int, folder_ids: Iterable[int]) -> None:
        """Revoke only this workspace's grants in the caller's transaction."""
        ids = list(dict.fromkeys(folder_ids))
        for start in range(0, len(ids), 800):
            chunk = ids[start:start + 800]
            marks = ",".join("?" for _ in chunk)
            self.conn.execute(
                "DELETE FROM workspace_photos WHERE workspace_id = ? AND photo_id IN "
                f"(SELECT id FROM photos WHERE folder_id IN ({marks}))",
                [workspace_id, *chunk],
            )

    def grant_verified_twins(
        self, workspace_id: int, rows: Sequence[Mapping[str, Any]],
    ) -> None:
        self.grant(workspace_id, [row["id"] for row in rows])
        for row in rows:
            if row["folder_status"] == "missing":
                self.conn.execute(
                    "UPDATE folders SET status = 'ok' WHERE status = 'missing' AND id = "
                    "(SELECT folder_id FROM photos WHERE id = ?)", (row["id"],),
                )

    def grant_verified_twins_tracked(
        self, workspace_id: int, rows: Sequence[Mapping[str, Any]],
    ) -> tuple[list[int], list[int]]:
        """Like :meth:`grant_verified_twins`, but report what this call changed.

        Returns ``(new_grant_ids, promoted_folder_ids)``:
        - ``new_grant_ids``: photo ids where this call inserted a fresh
          ``workspace_photos`` row (a grant that already existed is not
          included).
        - ``promoted_folder_ids``: folder ids whose ``status`` this call
          flipped from ``'missing'`` to ``'ok'``.

        Lets a caller that may still have to roll the batch back on a
        mount-loss detection (``import_job._rollback_on_mount_loss``) undo
        exactly what it created without disturbing grants or folder
        statuses that existed before.
        """
        new_grant_ids = []
        for photo_id in dict.fromkeys(row["id"] for row in rows):
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO workspace_photos (workspace_id, photo_id) VALUES (?, ?)",
                (workspace_id, photo_id),
            )
            if cur.rowcount > 0:
                new_grant_ids.append(photo_id)
        promoted_folder_ids = []
        for row in rows:
            if row["folder_status"] == "missing":
                cur = self.conn.execute(
                    "UPDATE folders SET status = 'ok' WHERE status = 'missing' AND id = "
                    "(SELECT folder_id FROM photos WHERE id = ?)", (row["id"],),
                )
                if cur.rowcount > 0:
                    folder_row = self.conn.execute(
                        "SELECT folder_id FROM photos WHERE id = ?", (row["id"],),
                    ).fetchone()
                    if folder_row is not None and folder_row["folder_id"] is not None:
                        promoted_folder_ids.append(folder_row["folder_id"])
        return new_grant_ids, list(dict.fromkeys(promoted_folder_ids))

    def revoke_grants(self, workspace_id: int, photo_ids: Iterable[int]) -> None:
        """Delete ``workspace_photos`` rows for exactly these photo ids.

        Unlike :meth:`revoke_for_folders`, this does not expand to siblings
        in the same folder — the caller passes the exact ids it wants to
        revoke. Used by the import rollback to undo grants that
        :meth:`grant_verified_twins_tracked` just created.
        """
        ids = list(dict.fromkeys(photo_ids))
        for start in range(0, len(ids), 800):
            chunk = ids[start:start + 800]
            marks = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"DELETE FROM workspace_photos WHERE workspace_id = ? AND photo_id IN ({marks})",
                [workspace_id, *chunk],
            )

    def demote_folders_to_missing(self, folder_ids: Iterable[int]) -> None:
        """Revert folders to ``status = 'missing'``.

        Only used by the import rollback to undo a status promotion that
        :meth:`grant_verified_twins_tracked` applied earlier in the batch
        on the assumption that an on-archive twin confirmed the folder
        bytes were still reachable; a mount loss invalidates that proof.
        """
        ids = list(dict.fromkeys(folder_ids))
        for start in range(0, len(ids), 800):
            chunk = ids[start:start + 800]
            marks = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"UPDATE folders SET status = 'missing' WHERE id IN ({marks})",
                chunk,
            )

    def affected_workspaces(self, photo_ids: Iterable[int]) -> list[dict[str, Any]]:
        """Other workspaces that can see any of these photos, with how many each sees."""
        active_workspace = self.workspace_id_fn()
        counts = {}
        ids = list(dict.fromkeys(photo_ids))
        for start in range(0, len(ids), 800):
            chunk = ids[start:start + 800]
            marks = ",".join("?" for _ in chunk)
            for row in self.conn.execute(
                "SELECT w.id, w.name, COUNT(DISTINCT pv.photo_id) AS photo_count "
                "FROM photo_workspace_visibility pv JOIN workspaces w ON w.id = pv.workspace_id "
                f"WHERE pv.photo_id IN ({marks}) AND w.id != ? GROUP BY w.id, w.name",
                [*chunk, active_workspace],
            ):
                key = (row["id"], row["name"])
                counts[key] = counts.get(key, 0) + row["photo_count"]
        return [{"id": key[0], "name": key[1], "photo_count": count}
                for key, count in sorted(counts.items(), key=lambda item: (item[0][1], item[0][0]))]

    def preserve_for_move(self, photo_id: int, keep_visible: bool) -> None:
        """Keep (or drop) other workspaces' access to a photo moving out of the active one."""
        active_workspace = self.workspace_id_fn()
        if keep_visible:
            self.conn.execute(
                "INSERT OR IGNORE INTO workspace_photos (workspace_id, photo_id) "
                "SELECT workspace_id, photo_id FROM photo_workspace_visibility "
                "WHERE photo_id = ? AND workspace_id != ?", (photo_id, active_workspace),
            )
        else:
            self.conn.execute(
                "DELETE FROM workspace_photos WHERE photo_id = ? AND workspace_id != ?",
                (photo_id, active_workspace),
            )


def remap_photo_visibility(conn, mapping):
    """Keep each workspace's photo-only access when catalog identities fold."""
    for losing, surviving in mapping.items():
        if surviving is None or losing == surviving:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO workspace_photos (workspace_id, photo_id) "
            "SELECT workspace_id, ? FROM photo_workspace_visibility WHERE photo_id = ?",
            (surviving, losing),
        )
