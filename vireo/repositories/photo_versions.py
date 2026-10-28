"""Independent edits and named snapshots sharing a photo's source and masks."""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from edit_recipe import normalize_recipe


class VersionConflict(ValueError):
    """A different editor has changed the version since it was loaded."""


class PhotoVersionsRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def list(self, photo_id: int) -> list[dict[str, Any]]:
        return [json.loads(row[0]) for row in self.conn.execute(
            "SELECT value_json FROM photo_versions WHERE photo_id = ? ORDER BY rowid",
            (photo_id,),
        )]

    def get(self, photo_id: int, version_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT value_json FROM photo_versions WHERE photo_id = ? AND id = ?",
            (photo_id, version_id),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def counts(self, photo_ids: list[int]) -> dict[int, int]:
        from sql_chunks import chunked
        result = {}
        for ids in chunked(photo_ids):
            marks = ",".join("?" for _ in ids)
            for row in self.conn.execute(
                f"SELECT photo_id, COUNT(*) FROM photo_versions WHERE photo_id IN ({marks}) "
                "AND json_extract(value_json, '$.kind') = 'copy' GROUP BY photo_id", ids,
            ):
                result[row[0]] = row[1]
        return result

    def put(self, photo_id: int, value: dict[str, Any]) -> dict[str, Any]:
        value = dict(value, revision=uuid.uuid4().hex)
        self.conn.execute(
            """INSERT INTO photo_versions (photo_id, id, value_json) VALUES (?, ?, ?)
               ON CONFLICT(photo_id, id) DO UPDATE SET value_json = excluded.value_json""",
            (photo_id, value["id"], json.dumps(value)),
        )
        return value

    def delete(self, photo_id: int, version_id: str) -> None:
        self.conn.execute("DELETE FROM photo_versions WHERE photo_id = ? AND id = ?", (photo_id, version_id))

    def create(self, photo_id: int, recipe: Any, name: Any = None, kind: str = "copy") -> dict[str, Any]:
        if kind not in ("copy", "snapshot"):
            raise ValueError("kind must be copy or snapshot")
        if name is None:
            prefix = "Copy" if kind == "copy" else "Snapshot"
            names = {v["name"] for v in self.list(photo_id)}
            index = 1
            while f"{prefix} {index}" in names:
                index += 1
            name = f"{prefix} {index}"
        now = datetime.now(UTC).isoformat()
        return self.put(photo_id, {
            "id": uuid.uuid4().hex, "name": self.validate_name(name), "kind": kind,
            "recipe": normalize_recipe(recipe), "created_at": now, "updated_at": now,
        })

    @staticmethod
    def validate_name(name: Any) -> str:
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 120:
            raise ValueError("version name must contain 1 to 120 characters")
        return name.strip()

    def history(self, photo_id: int, version_id: str, workspace_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """SELECT h.description, h.created_at, h.undone, i.new_value
               FROM edit_history h JOIN edit_history_items i ON i.edit_id = h.id
               WHERE h.action_type = 'photo_version' AND i.photo_id = ?
                 AND h.workspace_id = ? AND json_extract(i.new_value, '$.id') = ?
               ORDER BY h.id DESC LIMIT 100""", (photo_id, workspace_id, version_id),
        )
        result = []
        for row in rows:
            value = json.loads(row["new_value"]).get("value")
            if value is not None:
                result.append({"description": row["description"], "created_at": row["created_at"],
                               "undone": bool(row["undone"]), "new_recipe": value["recipe"]})
        return result

    def serialize(self, photo_id: int) -> str:
        return json.dumps({"schema": 1, "versions": self.list(photo_id)}, sort_keys=True)

    def refresh_pending(self, photo_id: int) -> None:
        """Every workspace must sync the same current, globally shared versions."""
        self.conn.execute(
            "UPDATE pending_changes SET value = ?, change_token = lower(hex(randomblob(16))) "
            "WHERE photo_id = ? AND change_type = 'photo_versions'",
            (self.serialize(photo_id), photo_id),
        )

    def local_recipe_values(self) -> list[str]:
        return [row[0] for row in self.conn.execute(
            "SELECT value_json FROM photo_versions WHERE value_json LIKE '%\"local\"%'"
        )]

    def import_document(self, photo_id: int, raw: str) -> None:
        """Recover versions from XMP without overwriting local or queued edits."""
        if self.list(photo_id) or self.conn.execute(
            "SELECT 1 FROM pending_changes WHERE photo_id = ? AND change_type = 'photo_versions' LIMIT 1",
            (photo_id,),
        ).fetchone():
            return
        document = json.loads(raw)
        if not isinstance(document, dict) or document.get("schema") != 1:
            raise ValueError("Unsupported photo versions schema")
        values = document.get("versions")
        if not isinstance(values, list) or len(values) > 1000:
            raise ValueError("Invalid photo versions list")
        normalized = []
        seen = set()
        for value in values:
            if (not isinstance(value, dict) or not isinstance(value.get("id"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", value["id"]) or value["id"] in seen
                    or value.get("kind") not in ("copy", "snapshot")):
                raise ValueError("Invalid photo version")
            seen.add(value["id"])
            for field in ("created_at", "updated_at"):
                if not isinstance(value.get(field), str):
                    raise ValueError("Invalid version timestamp")
                datetime.fromisoformat(value[field])
            normalized.append(dict(value, name=self.validate_name(value.get("name")),
                                   recipe=normalize_recipe(value.get("recipe"))))
        for value in normalized:
            self.put(photo_id, value)


def remap_photo_versions(conn: sqlite3.Connection, mapping: dict[int, int | None]) -> None:
    """Keep copies, snapshots and their undo records when catalog photos merge."""
    repo = PhotoVersionsRepository(conn)
    for source, target in mapping.items():
        if target is None or source == target:
            continue
        values = repo.list(source)
        if not values and not conn.execute(
            "SELECT 1 FROM edit_history_items i JOIN edit_history h ON h.id=i.edit_id "
            "WHERE i.photo_id=? AND h.action_type='photo_version' LIMIT 1", (source,),
        ).fetchone():
            continue
        for value in values:
            existing = repo.get(target, value["id"])
            if existing is None or value["updated_at"] > existing["updated_at"]:
                repo.put(target, value)
        conn.execute(
            """UPDATE edit_history_items SET photo_id = ? WHERE photo_id = ?
               AND edit_id IN (SELECT id FROM edit_history WHERE action_type = 'photo_version')""",
            (target, source),
        )
        # A caller may already have moved pending changes before deleting the source.
        conn.execute(
            "UPDATE pending_changes SET photo_id = ?, value = ?, change_token = lower(hex(randomblob(16))) "
            "WHERE photo_id IN (?, ?) AND change_type = 'photo_versions'",
            (target, repo.serialize(target), source, target),
        )
