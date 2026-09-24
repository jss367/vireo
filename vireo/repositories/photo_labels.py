"""Persistence for workspace-scoped photo color labels.

Color labels and their per-color descriptions are scoped to the active
workspace, so ``Database`` builds the repository with ``self._ws_id()`` and
this class holds the SQL. Descriptions ride in the workspace's
``config_overrides`` JSON blob (see ``get_descriptions`` /
``set_description``); the read fails soft — bad JSON or a stale schema
returns ``{}`` rather than raising — so a corrupt override never blocks
labelling. The color-name whitelist and description length cap
(``VALID_COLOR_LABELS``, ``MAX_COLOR_LABEL_DESCRIPTION_LENGTH``) are module
constants that ``Database`` re-exports so callers can validate before
delegating. ``Database`` keeps the wrappers
(``set_color_label``, ``remove_color_label``, ``get_color_label``,
``get_color_labels_for_photos``, ``filter_photo_ids_in_workspace``,
``batch_set_color_label``, ``get_color_label_descriptions``,
``set_color_label_description``) as one-line delegations and calls in here
for the SQL.
"""

import json

VALID_COLOR_LABELS = ("red", "yellow", "green", "blue", "purple")
MAX_COLOR_LABEL_DESCRIPTION_LENGTH = 120
_DESCRIPTIONS_CONFIG_KEY = "color_label_descriptions"


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

    def get_descriptions(self):
        """Return the active workspace's valid, non-empty color descriptions."""
        row = self.conn.execute(
            "SELECT config_overrides FROM workspaces WHERE id = ?",
            (self.workspace_id,),
        ).fetchone()
        if not row or not row["config_overrides"]:
            return {}
        try:
            overrides = json.loads(row["config_overrides"])
        except (json.JSONDecodeError, TypeError):
            return {}
        if not isinstance(overrides, dict):
            return {}
        raw = overrides.get(_DESCRIPTIONS_CONFIG_KEY)
        if not isinstance(raw, dict):
            return {}
        return {
            color: description.strip()
            for color, description in raw.items()
            if color in VALID_COLOR_LABELS
            and isinstance(description, str)
            and description.strip()
        }

    def set_description(self, color, description):
        """Set or clear one color's description in workspace config metadata."""
        if color not in VALID_COLOR_LABELS:
            raise ValueError(
                f"Invalid color label: {color}. Must be one of {VALID_COLOR_LABELS}"
            )
        if not isinstance(description, str):
            raise ValueError("description must be a string")
        description = " ".join(description.split())
        if len(description) > MAX_COLOR_LABEL_DESCRIPTION_LENGTH:
            raise ValueError(
                "description must be "
                f"{MAX_COLOR_LABEL_DESCRIPTION_LENGTH} characters or fewer"
            )

        row = self.conn.execute(
            "SELECT config_overrides FROM workspaces WHERE id = ?",
            (self.workspace_id,),
        ).fetchone()
        overrides = {}
        if row and row["config_overrides"]:
            try:
                parsed = json.loads(row["config_overrides"])
                if isinstance(parsed, dict):
                    overrides = parsed
            except (json.JSONDecodeError, TypeError):
                pass

        descriptions = overrides.get(_DESCRIPTIONS_CONFIG_KEY)
        descriptions = dict(descriptions) if isinstance(descriptions, dict) else {}
        if description:
            descriptions[color] = description
        else:
            descriptions.pop(color, None)

        if descriptions:
            overrides[_DESCRIPTIONS_CONFIG_KEY] = descriptions
        else:
            overrides.pop(_DESCRIPTIONS_CONFIG_KEY, None)
        self.conn.execute(
            "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
            (json.dumps(overrides) if overrides else None, self.workspace_id),
        )
        self.conn.commit()
        return description

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
