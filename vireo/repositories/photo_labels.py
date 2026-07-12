"""Persistence for workspace-scoped photo color labels."""


VALID_COLOR_LABELS = ("red", "yellow", "green", "blue", "purple")


class PhotoLabelRepository:
    def __init__(self, conn, workspace_id, *, chunk_size=800):
        self.conn = conn
        self.workspace_id = workspace_id
        self.chunk_size = chunk_size

    def get(self, photo_id):
        row = self.conn.execute(
            "SELECT color FROM photo_color_labels "
            "WHERE photo_id = ? AND workspace_id = ?",
            (photo_id, self.workspace_id),
        ).fetchone()
        return row["color"] if row else None

    def get_for_photos(self, photo_ids):
        if not photo_ids:
            return {}
        labels = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                "SELECT photo_id, color FROM photo_color_labels "
                f"WHERE workspace_id = ? AND photo_id IN ({placeholders})",
                [self.workspace_id, *chunk],
            ).fetchall()
            labels.update({row["photo_id"]: row["color"] for row in rows})
        return labels

    def visible_photo_ids(self, photo_ids):
        """Return existing, workspace-visible IDs in caller order, deduplicated."""
        requested = list(dict.fromkeys(photo_ids))
        visible = set()
        for chunk in self._chunks(requested):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                "SELECT p.id FROM photos p "
                "JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
                f"WHERE wf.workspace_id = ? AND p.id IN ({placeholders})",
                [self.workspace_id, *chunk],
            ).fetchall()
            visible.update(row["id"] for row in rows)
        return [photo_id for photo_id in requested if photo_id in visible]

    def set(self, photo_id, color):
        if color not in VALID_COLOR_LABELS:
            raise ValueError(
                f"Invalid color label: {color}. Must be one of {VALID_COLOR_LABELS}"
            )
        self.conn.execute(
            "INSERT OR REPLACE INTO photo_color_labels "
            "(photo_id, workspace_id, color) VALUES (?, ?, ?)",
            (photo_id, self.workspace_id, color),
        )
        self.conn.commit()

    def remove(self, photo_id):
        self.conn.execute(
            "DELETE FROM photo_color_labels "
            "WHERE photo_id = ? AND workspace_id = ?",
            (photo_id, self.workspace_id),
        )
        self.conn.commit()

    def set_many(self, photo_ids, color):
        if not photo_ids:
            return
        if color is not None and color not in VALID_COLOR_LABELS:
            raise ValueError(
                f"Invalid color label: {color}. Must be one of {VALID_COLOR_LABELS}"
            )
        if color is None:
            for chunk in self._chunks(photo_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    "DELETE FROM photo_color_labels "
                    f"WHERE workspace_id = ? AND photo_id IN ({placeholders})",
                    [self.workspace_id, *chunk],
                )
        else:
            self.conn.executemany(
                "INSERT OR REPLACE INTO photo_color_labels "
                "(photo_id, workspace_id, color) VALUES (?, ?, ?)",
                [(photo_id, self.workspace_id, color) for photo_id in photo_ids],
            )
        self.conn.commit()

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
