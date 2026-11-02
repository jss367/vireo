"""The Dashboard's scoped statistics: coverage, per-folder coverage, aggregates.

Each reader composes the workspace-effective config (``detector_confidence``,
``preview_max_size``), the Dashboard scope (``dashboard_scope_clause``: a
folder subtree, a collection's rules compiled into a subquery, and a date
range) and the ``StatsRepository`` read that runs the SQL. Every reader checks
for an active workspace first, so with none it raises ``RuntimeError`` before
it reads config or composes a scope.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import config as cfg
from config import config_value
from repositories.photo_sql import inclusive_date_to as _inclusive_date_to
from repositories.photos import append_location_status_filter

if TYPE_CHECKING:
    from db import Database
    from repositories.stats import StatsRepository
    from services.service_types import JsonDict


def _scoped_stats(db: Database) -> StatsRepository:
    """The stats repository, once a workspace is known to be active."""
    db.require_workspace_id()
    return db.stats


def dashboard_scope_clause(
    db: Database,
    folder_id: int | None = None,
    collection_id: int | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    table_alias: str = "p",
) -> tuple[str, list[Any]]:
    """Return an ``AND ...`` clause for Dashboard-scoped photo queries.

    Unlike ``StatsRepository.scope_clause``, this stays composable and does not
    materialize every matching photo id in Python.  That matters for a
    date-scoped dashboard over a million-photo workspace.  Collection
    rules are embedded as a subquery using the same rule compiler as
    Browse, so Dashboard and Browse agree about collection membership.
    """
    conditions: list[str] = []
    params: list[Any] = []

    if folder_id is not None:
        if not _scoped_stats(db).folder_linked(folder_id):
            raise ValueError("folder not found in active workspace")
        subtree = db.folders.subtree_ids(folder_id)
        placeholders = ",".join("?" for _ in subtree)
        conditions.append(f"{table_alias}.folder_id IN ({placeholders})")
        params.extend(subtree)

    if collection_id is not None:
        # Dashboard scope keeps offline photos in totals, so the collection
        # subquery must not filter them out via the Browse-oriented
        # ``f.status IN ('ok', 'partial')`` join. Callers that need the
        # accessible-only view apply that filter in their outer query
        # (e.g. get_coverage_stats), so it's fine to be permissive here.
        parts = db.collections._build_collection_query(
            collection_id, include_offline_folders=True,
        )
        if parts is None:
            raise ValueError("collection not found in active workspace")
        folder_join, join_clause, where, collection_params = parts
        collection_query = (
            "SELECT DISTINCT p.id FROM photos p "
            f"{folder_join} {join_clause} {where}"
        )
        conditions.append(f"{table_alias}.id IN ({collection_query})")
        params.extend(collection_params)

    if date_from is not None:
        conditions.append(f"{table_alias}.timestamp >= ?")
        params.append(date_from)
    if date_to is not None:
        conditions.append(f"{table_alias}.timestamp <= ?")
        params.append(_inclusive_date_to(date_to))

    if not conditions:
        return "", []
    return " AND " + " AND ".join(conditions), params


def get_coverage_stats(
    db: Database, folder_id: int | None = None, collection_id: int | None = None,
    date_from: str | None = None, date_to: str | None = None,
) -> JsonDict:
    """Return per-stage coverage counts for the active workspace.

    ``total`` is the number of photos in active (status ``'ok'`` or
    ``'partial'``) folders of the workspace. Each other key is the count of those photos for which
    the named pipeline stage has produced output. ``detected`` and
    ``classified`` are joined from the detections/predictions tables;
    everything else is a simple NOT NULL check on ``photos``.
    """
    repo = _scoped_stats(db)
    min_conf = config_value(db.get_effective_config(cfg.load()), "detector_confidence")
    scope_sql, scope_params = dashboard_scope_clause(
        db, folder_id, collection_id, date_from, date_to,
    )
    return repo.get_coverage(
        min_conf, scope_sql, scope_params, repo.coverage_select_fragment(),
    )


def get_folder_coverage_stats(
    db: Database, folder_id: int | None = None, collection_id: int | None = None,
    date_from: str | None = None, date_to: str | None = None,
) -> list[JsonDict]:
    """Return a list of per-folder coverage counts for the active workspace.

    One row per folder that is linked to the workspace and has
    ``status`` of ``'ok'`` or ``'partial'``. Each row carries ``folder_id``, ``path``, ``name``,
    ``total`` (photos in that folder only — descendants are NOT rolled
    in), and the same coverage keys as :meth:`get_coverage_stats`.
    Folders with zero photos are included so the dashboard can still
    show them as 0 / 0 if it chooses.
    """
    repo = _scoped_stats(db)
    min_conf = config_value(db.get_effective_config(cfg.load()), "detector_confidence")
    scope_sql, scope_params = dashboard_scope_clause(
        db, folder_id, collection_id, date_from, date_to,
    )
    # Date and collection constraints belong on the LEFT JOIN so folders
    # with zero matching photos remain visible as 0 / 0. Folder scope is
    # different: it controls which folder rows are enumerated, so keep it
    # as an outer WHERE condition on ``f.id``.
    photo_scope_sql, photo_scope_params = dashboard_scope_clause(
        db, None, collection_id, date_from, date_to,
    )
    subtree = None
    if folder_id is not None:
        if not repo.folder_linked(folder_id):
            raise ValueError("folder not found in active workspace")
        subtree = db.folders.subtree_ids(folder_id)
    return repo.get_folder_coverage(
        min_conf, scope_sql, scope_params, photo_scope_sql,
        photo_scope_params, subtree, repo.coverage_select_fragment(),
    )


def get_dashboard_stats(
    db: Database, folder_id: int | None = None, collection_id: int | None = None,
    date_from: str | None = None, date_to: str | None = None,
) -> JsonDict:
    """Return scoped aggregate statistics and actionable gaps.

    Dashboard scope is metadata-only: photos remain countable while their
    storage is offline.  Signals that require reading source files (such
    as preview generation) separately restrict themselves to accessible
    folders and expose that distinction through ``accessible_photos``.
    """
    repo = _scoped_stats(db)
    # Hoisted: multiple queries below need the workspace-effective
    # detector_confidence to keep classified_count / prediction_status /
    # detected_count in sync as the threshold moves.
    effective = db.get_effective_config(cfg.load())
    min_conf = config_value(effective, "detector_confidence")
    preview_size = config_value(effective, "preview_max_size")
    scope_sql, scope_params = dashboard_scope_clause(
        db, folder_id, collection_id, date_from, date_to,
    )
    location_conditions: list[str] = []
    append_location_status_filter(location_conditions, "none")
    return repo.get_dashboard(
        min_conf, preview_size, scope_sql, scope_params, location_conditions,
    )
