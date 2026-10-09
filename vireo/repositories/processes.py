"""Persistence for saved processes (user-editable process presets).

Saved processes are global: the table is shared across workspaces, so the
repository takes no workspace id. ``delete`` also clears any workspace's
``pipeline.default_process_id`` override that pointed at the deleted row.

Callers reach it as ``db.processes`` (a fresh repository per access, see
``Database.processes``) for :meth:`list_all`, :meth:`get` and
:meth:`create`; there are no forwarding wrappers on ``Database``.
``Database.update_saved_process`` and ``Database.delete_saved_process`` stay
on the façade: each checks the row exists through :meth:`get` before calling
:meth:`update` / :meth:`delete`, so call those rather than the repository
methods.
"""

import json
import sqlite3
from typing import Any

from repositories import UNSET


class ProcessesRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    @staticmethod
    def _row_to_dict(row):
        return {
            "id": row["id"],
            "name": row["name"],
            "skip_classify": bool(row["skip_classify"]),
            "skip_extract_masks": bool(row["skip_extract_masks"]),
            "skip_eye_keypoints": bool(row["skip_eye_keypoints"]),
            "skip_regroup": bool(row["skip_regroup"]),
            "miss_enabled": bool(row["miss_enabled"]),
            "review_mode": row["review_mode"],
            "is_seed": bool(row["is_seed"]),
            "sort_order": row["sort_order"],
        }

    def list_all(self) -> list[dict[str, Any]]:
        """Return all saved processes ordered for display (sort_order, id)."""
        rows = self.conn.execute(
            "SELECT * FROM saved_processes ORDER BY sort_order, id"
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get(self, process_id: int) -> dict[str, Any] | None:
        """Return one saved process as a dict, or None if it doesn't exist."""
        row = self.conn.execute(
            "SELECT * FROM saved_processes WHERE id = ?", (process_id,)
        ).fetchone()
        return self._row_to_dict(row) if row else None

    @staticmethod
    def _normalize_fields(name, skip_classify, skip_extract_masks,
                          skip_eye_keypoints, skip_regroup,
                          miss_enabled, review_mode):
        """Validate + coerce process fields. Raises ValueError on bad input."""
        if not isinstance(name, str) or not name.strip():
            raise ValueError("process name is required")
        if review_mode is not None and review_mode != "species":
            raise ValueError("review_mode must be 'species' or null")
        return (
            name.strip(),
            int(bool(skip_classify)),
            int(bool(skip_extract_masks)),
            int(bool(skip_eye_keypoints)),
            int(bool(skip_regroup)),
            int(bool(miss_enabled)),
            review_mode,
        )

    def create(self, name: str, *, skip_classify: bool = False,
               skip_extract_masks: bool = False, skip_eye_keypoints: bool = False,
               skip_regroup: bool = False, miss_enabled: bool = True,
               review_mode: str | None = None) -> int:
        """Insert a saved process, commit, and return its id.

        Raises ValueError on a blank/duplicate name or a bad review_mode.
        """
        fields = self._normalize_fields(
            name, skip_classify, skip_extract_masks, skip_eye_keypoints,
            skip_regroup, miss_enabled, review_mode,
        )
        # New user processes sort after the seeds; ties break by id.
        next_order = self.conn.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM saved_processes"
        ).fetchone()[0]
        try:
            cur = self.conn.execute(
                "INSERT INTO saved_processes "
                "(name, skip_classify, skip_extract_masks, skip_eye_keypoints, "
                " skip_regroup, miss_enabled, review_mode, is_seed, sort_order) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
                (*fields, next_order),
            )
        except sqlite3.IntegrityError as e:
            raise ValueError(
                f"a process named {name.strip()!r} already exists"
            ) from e
        self.conn.commit()
        return cur.lastrowid

    def update(self, process_id: int, current: dict[str, Any], *,
               name: str | None = None, skip_classify: bool | None = None,
               skip_extract_masks: bool | None = None,
               skip_eye_keypoints: bool | None = None,
               skip_regroup: bool | None = None, miss_enabled: bool | None = None,
               review_mode: Any = UNSET) -> bool:
        """Merge the given fields over ``current`` (the existing row as a
        dict), write them, commit, and return True.

        Any field left at its sentinel default (None, or UNSET for
        review_mode which is legitimately None) keeps its ``current`` value.
        Raises ValueError on a blank/duplicate name or a bad review_mode.
        """
        merged = {
            "name": current["name"] if name is None else name,
            "skip_classify": current["skip_classify"] if skip_classify is None else skip_classify,
            "skip_extract_masks": current["skip_extract_masks"] if skip_extract_masks is None else skip_extract_masks,
            "skip_eye_keypoints": current["skip_eye_keypoints"] if skip_eye_keypoints is None else skip_eye_keypoints,
            "skip_regroup": current["skip_regroup"] if skip_regroup is None else skip_regroup,
            "miss_enabled": current["miss_enabled"] if miss_enabled is None else miss_enabled,
            "review_mode": current["review_mode"] if review_mode is UNSET else review_mode,
        }
        fields = self._normalize_fields(
            merged["name"], merged["skip_classify"], merged["skip_extract_masks"],
            merged["skip_eye_keypoints"], merged["skip_regroup"],
            merged["miss_enabled"], merged["review_mode"],
        )
        try:
            self.conn.execute(
                "UPDATE saved_processes SET name=?, skip_classify=?, "
                "skip_extract_masks=?, skip_eye_keypoints=?, skip_regroup=?, "
                "miss_enabled=?, review_mode=? WHERE id=?",
                (*fields, process_id),
            )
        except sqlite3.IntegrityError as e:
            raise ValueError(
                f"a process named {merged['name'].strip()!r} already exists"
            ) from e
        self.conn.commit()
        return True

    def delete(self, process_id: int) -> bool:
        """Delete a saved process, null every workspace default pointing at
        it, commit, and return True. The caller checks that it exists.
        """
        self.conn.execute(
            "DELETE FROM saved_processes WHERE id = ?", (process_id,)
        )
        # Null the per-workspace default pointer wherever it referenced this id.
        # Write an explicit ``None`` (not ``pop``) so the workspace's effective
        # config resolves to "import only" instead of silently inheriting a
        # different global ``pipeline.default_process_id`` via _deep_merge.
        ws_rows = self.conn.execute(
            "SELECT id, config_overrides FROM workspaces "
            "WHERE config_overrides IS NOT NULL"
        ).fetchall()
        for row in ws_rows:
            try:
                overrides = json.loads(row["config_overrides"])
            except (TypeError, ValueError):
                continue
            if not isinstance(overrides, dict):
                continue
            pipeline_ov = overrides.get("pipeline")
            if not isinstance(pipeline_ov, dict):
                continue
            if pipeline_ov.get("default_process_id") == process_id:
                pipeline_ov["default_process_id"] = None
                self.conn.execute(
                    "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
                    (json.dumps(overrides), row["id"]),
                )
        self.conn.commit()
        return True
