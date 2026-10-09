"""Persistence for workspace-scoped photo color labels.

Color labels and their per-color descriptions are scoped to the active
workspace. ``Database`` builds the repository with ``self._ws_id`` uncalled,
and every public method resolves it first, before validating its arguments
or running any SQL, so with no workspace active each one raises
``RuntimeError`` exactly where the old eager ``_ws_id()`` in the factory did
(including the empty-input reads and writes). Descriptions ride in the
workspace's ``config_overrides`` JSON blob (see ``get_descriptions`` /
``set_description``); the read fails soft — bad JSON or a stale schema
returns ``{}`` rather than raising — so a corrupt override never blocks
labelling. The color-name whitelist and description length cap
(``VALID_COLOR_LABELS``, ``MAX_COLOR_LABEL_DESCRIPTION_LENGTH``) are module
constants that ``Database`` re-exports so callers can validate before
delegating.

Callers reach it as ``db.photo_labels`` (a fresh repository per access, see
``Database.photo_labels``); there are no forwarding wrappers on
``Database``. Every write commits. The workspace visibility filter that used
to live here is ``db.photo_visibility.visible_photo_ids``.
"""

import json
import sqlite3
from collections.abc import Callable, Collection

VALID_COLOR_LABELS = ("red", "yellow", "green", "blue", "purple")
MAX_COLOR_LABEL_DESCRIPTION_LENGTH = 120
_DESCRIPTIONS_CONFIG_KEY = "color_label_descriptions"


class PhotoLabelRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        workspace_id_fn: Callable[[], int],
        *,
        chunk_size: int = 800,
    ) -> None:
        self.conn = conn
        self.workspace_id_fn = workspace_id_fn
        self.chunk_size = chunk_size

    def get(self, photo_id: int) -> str | None:
        """The photo's color label in the active workspace, or None."""
        workspace_id = self.workspace_id_fn()
        row = self.conn.execute(
            "SELECT color FROM photo_color_labels "
            "WHERE photo_id = ? AND workspace_id = ?",
            (photo_id, workspace_id),
        ).fetchone()
        return row["color"] if row else None

    def get_for_photos(self, photo_ids: Collection[int]) -> dict[int, str]:
        """``{photo_id: color}`` for the labelled photos among these, in the active workspace."""
        workspace_id = self.workspace_id_fn()
        if not photo_ids:
            return {}
        labels = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                "SELECT photo_id, color FROM photo_color_labels "
                f"WHERE workspace_id = ? AND photo_id IN ({placeholders})",
                [workspace_id, *chunk],
            ).fetchall()
            labels.update({row["photo_id"]: row["color"] for row in rows})
        return labels

    def set(self, photo_id: int, color: str) -> None:
        """Set a photo's color label in the active workspace and commit."""
        workspace_id = self.workspace_id_fn()
        if color not in VALID_COLOR_LABELS:
            raise ValueError(
                f"Invalid color label: {color}. Must be one of {VALID_COLOR_LABELS}"
            )
        self.conn.execute(
            "INSERT OR REPLACE INTO photo_color_labels "
            "(photo_id, workspace_id, color) VALUES (?, ?, ?)",
            (photo_id, workspace_id, color),
        )
        self.conn.commit()

    def remove(self, photo_id: int) -> None:
        """Remove a photo's color label in the active workspace and commit."""
        workspace_id = self.workspace_id_fn()
        self.conn.execute(
            "DELETE FROM photo_color_labels "
            "WHERE photo_id = ? AND workspace_id = ?",
            (photo_id, workspace_id),
        )
        self.conn.commit()

    def set_many(self, photo_ids: Collection[int], color: str | None) -> None:
        """Set (or, with ``color=None``, remove) several photos' color label and commit."""
        workspace_id = self.workspace_id_fn()
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
                    [workspace_id, *chunk],
                )
        else:
            self.conn.executemany(
                "INSERT OR REPLACE INTO photo_color_labels "
                "(photo_id, workspace_id, color) VALUES (?, ?, ?)",
                [(photo_id, workspace_id, color) for photo_id in photo_ids],
            )
        self.conn.commit()

    def get_descriptions(self) -> dict[str, str]:
        """Return the active workspace's valid, non-empty color descriptions."""
        workspace_id = self.workspace_id_fn()
        row = self.conn.execute(
            "SELECT config_overrides FROM workspaces WHERE id = ?",
            (workspace_id,),
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

    def set_description(self, color: str, description: str) -> str:
        """Set or clear one color's description in workspace config metadata."""
        workspace_id = self.workspace_id_fn()
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
            (workspace_id,),
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
            (json.dumps(overrides) if overrides else None, workspace_id),
        )
        self.conn.commit()
        return description

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
