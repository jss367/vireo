"""Browse's reads that compose more than one repository call.

The summary panel's aggregates (with the workspace-effective detector floor
resolved lazily), whether a rule tree compiles at all, and the stack-projected
counts, positions and id lists Browse pages and selects with. The SQL lives in
``db.photos`` and ``db.collections``; ``rules_resolvable`` compiles through
``Database._build_query_from_rules``, the forwarder the predictions repository
binds by name, so a patch of it reaches this check too.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import config as cfg
from config import config_value

if TYPE_CHECKING:
    from collections.abc import Mapping

    from db import Database
    from services.service_types import JsonDict


def get_browse_summary(
    db: Database, folder_id: int | None = None, collection_id: int | None = None,
    rules: Any = None,
) -> JsonDict:
    """Return summary stats for the browse panel, scoped to active workspace and filters.

    ``rules`` (a universal-filter rule tree) restricts every aggregate to
    the matching photo set, exactly like the collection_id subquery path —
    the summary panel must describe the same photos the filtered grid
    shows. Raises ValueError on malformed rules.
    """
    def detector_confidence() -> float:
        return config_value(db.get_effective_config(cfg.load()), "detector_confidence")

    # Joined: the TEMP id table and every aggregate share one snapshot, and
    # a caller's open transaction is neither committed nor ended. With none
    # open, a deferred BEGIN (no lock until the first read) puts the TEMP
    # table's creation inside the transaction too, so a superseded search's
    # interrupt, which makes SQLite roll the transaction back itself, takes
    # the table with it.
    with db.transaction(join=True):
        db.hold_read_snapshot()
        return db.photos.get_browse_summary(
            folder_id=folder_id,
            collection_id=collection_id,
            rules=rules,
            detector_confidence=detector_confidence,
        )


def rules_resolvable(db: Database, rules: Any) -> bool:
    """Return True if a rule tree can be resolved to SQL clauses without
    executing them.

    Callers (notably ``/api/browse/init``) use this to flag degraded
    collections at first paint without running the full
    ``COUNT(DISTINCT p.id)`` per collection — that N+1 is what the
    Browse client's async ``loadCollectionCounts()`` was built to
    avoid. Only the query-building step is exercised.
    """
    try:
        db._build_query_from_rules(rules)
    except ValueError:
        return False
    return True


def count_browse_stacks(
    db: Database, rules: Any, collection_id: int | None = None, folder_id: int | None = None,
    include_offline_folders: bool = False, stack_config: Mapping[str, Any] | None = None,
) -> int:
    """Count logical Browse items after stack projection."""
    return db.collections.browse_stack_totals(
        rules, collection_id=collection_id, folder_id=folder_id,
        include_offline_folders=include_offline_folders,
        stack_config=stack_config,
    )["total"]


def query_browse_stack_position(
    db: Database, rules: Any, photo_id: int, sort: str = "date",
    collection_id: int | None = None, folder_id: int | None = None,
    include_offline_folders: bool = False, stack_config: Mapping[str, Any] | None = None,
) -> int | None:
    """Return the zero-based position of the Browse item *containing*
    ``photo_id`` once stacks are projected, or ``None`` when the photo
    does not match.

    Stacked Browse pages logical items, not photos, so a hidden burst
    frame has no page of its own — the page that shows it is the one its
    cover sits on. Ranking the covers and joining back through
    ``_stack_key`` answers for members and singles alike, which is what
    lets a re-sort keep a selected stack member without Browse having to
    silently turn Stacks off the way focused deep links do.

    ``stack_config`` must be the grouping the caller pages with
    (``query_browse_stacks``): stacks are a configured projection of the
    result set, so a position read under different settings describes a
    different grid.

    Raises ValueError on malformed rules.
    """
    found = db.collections.query_browse_stack_position_first(
        rules, [photo_id], sort=sort, collection_id=collection_id,
        folder_id=folder_id,
        include_offline_folders=include_offline_folders,
        stack_config=stack_config,
    )
    return found[1] if found is not None else None


def query_photo_position(
    db: Database, rules: Any, photo_id: int, sort: str = "date",
    collection_id: int | None = None, folder_id: int | None = None,
    include_offline_folders: bool = False,
) -> int | None:
    """Return a photo's zero-based position in a universal-filter result
    set, or ``None`` when it does not match — the rules analog of
    ``get_photo_position``. One-ID shorthand for
    ``query_photo_position_first``.
    """
    found = db.collections.query_photo_position_first(
        rules, [photo_id], sort=sort, collection_id=collection_id,
        folder_id=folder_id,
        include_offline_folders=include_offline_folders,
    )
    return found[1] if found is not None else None


def get_collection_photo_ids_stacked(
    db: Database, collection_id: int, sort: str = "date",
    stack_config: Mapping[str, Any] | None = None,
) -> list[int]:
    """Return every photo ID matching a collection, in stack-projected
    order. See ``_stacked_photo_ids``.

    Without this, Select-all-matching with Stacks enabled would seed
    Best Batch, Burst Review, and the export-preview filename from a
    hidden member whenever a stack's quality-ranked cover isn't its
    earliest member under the selected sort.
    """
    return db.collections._stacked_photo_ids(
        [], sort=sort, collection_id=collection_id,
        stack_config=stack_config,
    )


def query_photo_ids_stacked(
    db: Database, rules: Any, sort: str = "date", collection_id: int | None = None,
    folder_id: int | None = None, stack_config: Mapping[str, Any] | None = None,
) -> list[int]:
    """Return every photo ID matching a universal-filter rule tree, in
    stack-projected order — the rules analog of
    ``get_collection_photo_ids_stacked``.

    Without this, Select-all-matching with Stacks enabled in the
    workspace, folder, dashboard-collection, or unsaved-filter paths
    would seed Best Batch, Burst Review, and the export-preview filename
    from a hidden member whenever a stack's quality-ranked cover isn't
    its earliest member under the selected sort.
    """
    return db.collections._stacked_photo_ids(
        rules, sort=sort, collection_id=collection_id, folder_id=folder_id,
        stack_config=stack_config,
    )
