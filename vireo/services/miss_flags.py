"""The Misses page's workflows over the photos' miss flags.

``list_misses`` and ``bulk_reject_miss_category`` scope to the active
workspace and an optional photo scope (``StatsRepository.scope_clause``);
``list_misses`` also attaches each photo's primary and best raw detection
under the workspace-effective ``detector_confidence``. ``clear_miss_flag``
refuses a photo outside the active workspace.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import config as cfg
from config import config_value
from services.photo_review import PhotoReviewService

if TYPE_CHECKING:
    from collections.abc import Iterable

    from db import Database


def list_misses(
    db: Database, category: str | None = None, since: str | None = None,
    photo_ids: Iterable[int] | None = None,
) -> list[dict[str, Any]]:
    """Return photos flagged as misses in the active workspace.

    category: None | "no_subject" | "clipped" | "oof"
    since: optional ISO timestamp; if set, restricts to photos whose
        miss_computed_at >= since. Used by the pipeline-review step to
        scope results to the current run.
    photo_ids: optional iterable restricting results to an already-resolved
        collection or filter scope. An empty iterable matches no photos.

    Excludes photos already flagged as rejected. Scoped to folders
    linked to the active workspace. ``detection_box`` and
    ``detection_conf`` are sourced from the selected primary
    row in the ``detections`` table — the legacy ``photos`` columns are
    not populated by normal pipeline runs. ``raw_detection_conf`` exposes
    the best animal candidate even when it is below the effective detector
    floor so the Misses UI can explain the decision. Ordered by timestamp
    DESC.
    """
    ws_id = db.require_workspace_id()
    repo = db.detections
    where = repo.miss_where(category)
    scope_clause, scope_params = db.stats.scope_clause(photo_ids)
    photos = repo.list_miss_photos(ws_id, where, since, scope_clause, scope_params)
    if not photos:
        return photos

    min_conf = config_value(db.get_effective_config(cfg.load()), "detector_confidence")
    return repo.attach_miss_detections(photos, min_conf)


def clear_miss_flag(db: Database, photo_id: int, category: str) -> None:
    """Set the given miss column to 0 on the given photo.

    Raises ValueError if the photo is not in the active workspace, so
    that `/api/misses/<id>/unflag` can't touch another workspace's photos.
    """
    db._verify_photo_in_workspace(photo_id)
    db.detections.clear_miss_flag(photo_id, category)


def bulk_reject_miss_category(
    db: Database, category: str, since: str | None = None,
    photo_ids: Iterable[int] | None = None,
) -> list[dict[str, Any]]:
    """Set flag='rejected' on every photo flagged with that miss category
    in the active workspace and not already rejected.

    ``since`` mirrors the filter on ``list_misses``: when set, only
    photos whose ``miss_computed_at >= since`` are rejected. This
    keeps bulk reject scoped to the /misses view the user is looking
    at (e.g. the current pipeline run), so older misses not shown on
    screen aren't silently rejected.

    ``photo_ids`` further restricts the mutation to the collection and
    Browse-style filters currently visible on the Misses page.

    Returns a list of ``{"photo_id": int, "old_value": str}`` for each
    changed photo. Flags, current manual attribution, sidecar queue, and
    reversible history snapshots are written in one review transaction.
    """
    repo = db.detections
    col = repo.miss_column(category)
    ws_id = db.require_workspace_id()
    scope_clause, scope_params = db.stats.scope_clause(photo_ids)
    with db.photo_review.flag_write() as owns_transaction:
        affected = repo.select_misses_to_reject(col, ws_id, since, scope_clause, scope_params)
        PhotoReviewService(db).set_flags(
            [item["photo_id"] for item in affected], "rejected",
            description=f"Rejected {len(affected)} miss photos (category={category})",
        )
    if owns_transaction and affected:
        db.prune_edit_history()
    return affected
