"""A workspace's effective settings: the global settings with its overrides.

The data layer reads global settings through the ``settings_source`` port
(``config`` installs itself there on import) and never imports ``config``.
A workspace stores its overrides as JSON in ``workspaces.config_overrides``;
the effective config deep-merges them over the global settings, so a nested
override (``{"pipeline": {"w_focus": 0.5}}``) replaces only the named leaf.

``Database.get_effective_config`` / ``Database.effective_config`` call
``workspace_effective_config`` for the active workspace. Repositories that
need a setting (the detector floor, the subject types) call
``active_effective_config`` with their own connection and workspace
resolver instead of calling back into ``Database``.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from typing import Any

import config_schema
import settings_source
from repositories.keywords import KEYWORD_TYPES

# Default set of types that count as "identifying" a photo for queue
# membership / classifier skip purposes. Workspaces can override.
SUBJECT_TYPES_DEFAULT = frozenset({"taxonomy", "individual", "genre"})


def workspace_effective_config(
    conn: sqlite3.Connection,
    workspace_id: int | None,
    global_config: dict[str, Any],
) -> dict[str, Any]:
    """``global_config`` with ``workspace_id``'s overrides deep-merged on top.

    No workspace (``None``), an unknown id, no overrides, or overrides that
    are not a JSON object all return ``global_config`` itself. An override
    stored before the workspace routes validated values (``null``,
    ``"abc"``) falls back to the global value it shadows.
    """
    ws = conn.execute(
        "SELECT * FROM workspaces WHERE id = ?", (workspace_id,)
    ).fetchone()
    if not ws or not ws["config_overrides"]:
        return global_config
    try:
        overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
        if not isinstance(overrides, dict):
            return global_config
        effective = config_schema.deep_merge(global_config, overrides)
    except (json.JSONDecodeError, TypeError):
        return global_config
    return config_schema.repair_types(effective, global_config)


def active_effective_config(
    conn: sqlite3.Connection,
    resolve_workspace_id: Callable[[], int],
) -> dict[str, Any]:
    """The global settings with the active workspace's overrides applied.

    ``resolve_workspace_id`` is the repository's workspace resolver
    (``Database._ws_id``); with no workspace active it raises
    ``RuntimeError``, and the result is the global settings alone, exactly
    as ``Database.effective_config()`` reads it.
    """
    try:
        workspace_id: int | None = resolve_workspace_id()
    except RuntimeError:
        workspace_id = None
    return workspace_effective_config(conn, workspace_id, settings_source.load())


def subject_types(effective: dict[str, Any]) -> set[str]:
    """The keyword types that count as 'identified' under ``effective``.

    config_overrides is arbitrary JSON (api_update_workspace persists
    whatever the client sends), so subject_types entries may not be
    strings — guard the membership test against unhashable values
    (nested lists/objects) to avoid TypeError downstream in callers
    like collection filtering and the classify skip-gate.
    """
    raw = effective.get("subject_types", list(SUBJECT_TYPES_DEFAULT))
    if not isinstance(raw, list):
        return set(SUBJECT_TYPES_DEFAULT)
    return {t for t in raw if isinstance(t, str) and t in KEYWORD_TYPES}
