"""Durable selected-review refresh requests, independent of long regroup locks."""
from __future__ import annotations

import contextlib
import json
import os
import threading
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict

_PENDING_LOCK = threading.Lock()


def _path(results_dir: str, workspace_id: int) -> str:
    from pipeline import _results_cache_path
    return _results_cache_path(results_dir, workspace_id) + ".analysis-refresh-pending.json"


def _read(path: str) -> dict[str, str]:
    try:
        with open(path, encoding="utf-8") as file:
            pending = json.load(file)
    except FileNotFoundError:
        return {}
    if not isinstance(pending, dict) or any(
        not isinstance(key, str) or not key.isdecimal() or int(key) <= 0
        or not isinstance(value, str) or not value for key, value in pending.items()
    ):
        raise ValueError("Saved review refresh requests are unreadable")
    return pending


def queue_refresh(results_dir: str, workspace_id: int, photo_ids: list[int]) -> None:
    """Durably merge a new generation without waiting for a regroup operation."""
    from pipeline import _atomic_json_dump
    path = _path(results_dir, workspace_id)
    with _PENDING_LOCK:
        pending = _read(path)
        pending.update({str(pid): uuid.uuid4().hex for pid in photo_ids})
        _atomic_json_dump(pending, path)


def pending_refresh(results_dir: str, workspace_id: int) -> dict[str, str]:
    with _PENDING_LOCK:
        return _read(_path(results_dir, workspace_id))


def _clear_consumed(results_dir: str, workspace_id: int, consumed: Mapping[str, str]) -> None:
    from pipeline import _atomic_json_dump
    path = _path(results_dir, workspace_id)
    with _PENDING_LOCK:
        pending = _read(path)
        remaining = {pid: token for pid, token in pending.items() if consumed.get(pid) != token}
        if remaining:
            _atomic_json_dump(remaining, path)
        else:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(path)


def refresh_pending(db: Database, data: JsonDict | None, results_dir: str,
                    effective: JsonDict | None = None) -> list[int]:
    """Caller holds the regroup lock; clear only generations saved successfully."""
    import config as cfg
    from pipeline import save_results_raw
    from services.saved_review import refresh_selected_suggestions

    workspace_id = db.require_workspace_id()
    pending = pending_refresh(results_dir, workspace_id)
    if not pending:
        return []
    photo_ids = [int(pid) for pid in pending]
    if data is not None:
        effective = effective if effective is not None else db.get_effective_config(cfg.load())
        refresh_selected_suggestions(db, data, photo_ids, effective)
        save_results_raw(data, results_dir, workspace_id)
    _clear_consumed(results_dir, workspace_id, pending)
    return photo_ids
