"""Per-workspace settings read and written through ``config_overrides``.

Browse's stack settings and the active label files are read off the active
workspace's effective config (or its raw overrides); the mask-storage floor
is the most permissive ``detector_confidence`` any workspace would apply.
``create_default_collections_for_all_workspaces`` seeds each workspace's
default collections at startup.
``Database.get_effective_config`` and ``get_subject_types`` stay on
``Database``: repositories bind them by name (``FACADE_METHODS`` and the
collections factory).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from config import _deep_merge
from db import normalize_browse_stack_config

if TYPE_CHECKING:
    from db import BrowseStackConfig, Database
    from services.service_types import JsonDict


def browse_stack_settings(db: Database, global_config: JsonDict | None = None) -> BrowseStackConfig:
    """Resolve Browse's stack settings for the active workspace.

    Reads through ``get_effective_config`` so a per-workspace override of
    ``browse_stack_time_gap`` / ``browse_stack_split_mode`` applies — a
    workspace of 20fps flight sequences and one of hand-held portraits
    want different gaps.
    """
    if global_config is None:
        return normalize_browse_stack_config(None)
    return normalize_browse_stack_config(
        db.get_effective_config(global_config)
    )


def min_detector_confidence_across_workspaces(db: Database, global_config: JsonDict) -> float:
    """Return the minimum effective ``detector_confidence`` across all
    workspaces.

    Used by global mask-storage endpoints (``/api/storage/masks`` and
    ``/api/storage/masks/delete-stale``). Stale-mask scoring depends
    on the floor under which a detection is treated as too noisy to
    re-extract from. Picking the *active* workspace's floor would
    mean switching workspaces changes the global deletion set —
    masks valid under another workspace's lower floor could be
    deleted just because the user happened to be in a stricter
    workspace at the moment. The minimum is the most permissive
    view: a mask is only considered globally stale when **no**
    workspace would still consider it fresh.
    """
    default = global_config.get("detector_confidence", 0.2)
    try:
        default = float(default)
    except (TypeError, ValueError):
        default = 0.2
    workspaces = db.workspaces.list_all()
    if not workspaces:
        return default
    values = []
    for ws in workspaces:
        overrides_raw = ws["config_overrides"]
        if not overrides_raw:
            values.append(default)
            continue
        try:
            overrides = (
                json.loads(overrides_raw)
                if isinstance(overrides_raw, str)
                else overrides_raw
            )
        except (json.JSONDecodeError, TypeError):
            values.append(default)
            continue
        if not isinstance(overrides, dict):
            values.append(default)
            continue
        merged = _deep_merge(global_config, overrides)
        v = merged.get("detector_confidence", default)
        try:
            values.append(float(v))
        except (TypeError, ValueError):
            values.append(default)
    return min(values) if values else default


def get_workspace_active_labels(db: Database) -> list[Any] | None:
    """Return the active_labels list from workspace config_overrides, or None."""
    ws = db.workspaces.get(db.require_workspace_id())
    if not ws or not ws["config_overrides"]:
        return None
    try:
        overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
        if not isinstance(overrides, dict):
            return None
        labels = overrides.get("active_labels")
        return labels if isinstance(labels, list) else None
    except (json.JSONDecodeError, TypeError):
        return None


def set_workspace_active_labels(db: Database, labels_files: list[str]) -> None:
    """Store active_labels in the workspace's config_overrides."""
    ws = db.workspaces.get(db.require_workspace_id())
    overrides: Any = {}
    if ws and ws["config_overrides"]:
        try:
            overrides = json.loads(ws["config_overrides"]) if isinstance(ws["config_overrides"], str) else ws["config_overrides"]
        except (json.JSONDecodeError, TypeError):
            overrides = {}
        if not isinstance(overrides, dict):
            overrides = {}
    overrides["active_labels"] = labels_files
    db.workspaces.update(db.require_workspace_id(), config_overrides=overrides)


def create_default_collections_for_all_workspaces(db: Database) -> None:
    """Create missing default smart collections in every workspace."""
    for ws in db.workspaces.list_all():
        db.collections.create_defaults(workspace_id=ws["id"])
