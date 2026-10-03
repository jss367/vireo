"""Photo-specific library membership, independent of folder membership.

Sync-only grants remain separate: this table intentionally grants browse,
edit and library visibility to a selected photo without exposing its siblings.
"""


class PhotoVisibilityRepository:
    def __init__(self, conn):
        self.conn = conn

    def grant(self, workspace_id, photo_ids):
        for photo_id in dict.fromkeys(photo_ids):
            self.conn.execute(
                "INSERT OR IGNORE INTO workspace_photos (workspace_id, photo_id) VALUES (?, ?)",
                (workspace_id, photo_id),
            )

    def grant_verified_twins(self, workspace_id, rows):
        self.grant(workspace_id, [row["id"] for row in rows])
        for row in rows:
            if row["folder_status"] == "missing":
                self.conn.execute(
                    "UPDATE folders SET status = 'ok' WHERE status = 'missing' AND id = "
                    "(SELECT folder_id FROM photos WHERE id = ?)", (row["id"],),
                )

    def affected_workspaces(self, photo_ids, active_workspace):
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

    def preserve_for_move(self, photo_id, active_workspace, keep_visible):
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
