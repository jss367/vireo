"""Saved processes: expanding one into stage flags, updating and deleting it.

Each function checks the row exists before acting, so a stale id reads as
"not found" (``ValueError`` from ``resolve_process``, ``False`` from the
update and delete) instead of failing deeper in the pipeline. Reads and
creates go through ``db.processes`` directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import process_strategies as ps
from repositories import UNSET

if TYPE_CHECKING:
    from db import Database


def resolve_process(db: Database, process_id: int) -> dict[str, Any]:
    """Expand a saved-process id into a full stage-flags dict over _BASE.

    Raises ValueError if the id doesn't exist, so callers surface a clean
    400/404 instead of an AttributeError deeper in the pipeline.
    """
    proc = db.processes.get(process_id)
    if proc is None:
        raise ValueError(f"unknown process id: {process_id!r}")
    return {**ps._BASE, **{k: proc[k] for k in ps.FLAG_FIELDS}}


def update_saved_process(
    db: Database, process_id: int, *, name: str | None = None,
    skip_classify: bool | None = None, skip_extract_masks: bool | None = None,
    skip_eye_keypoints: bool | None = None, skip_regroup: bool | None = None,
    miss_enabled: bool | None = None, review_mode: Any = UNSET,
) -> bool:
    """Update an existing saved process. Returns True if it existed.

    Any field left at its sentinel default (None, or _UNSET for
    review_mode which is legitimately None) is untouched. Raises
    ValueError on a blank/duplicate name or a bad review_mode.
    """
    repo = db.processes
    # The merge reads the row it overwrites, so read it under the lock.
    with db.transaction(immediate=True):
        current = repo.get(process_id)
        if current is None:
            return False
        return repo.update(
            process_id,
            current,
            name=name,
            skip_classify=skip_classify,
            skip_extract_masks=skip_extract_masks,
            skip_eye_keypoints=skip_eye_keypoints,
            skip_regroup=skip_regroup,
            miss_enabled=miss_enabled,
            review_mode=review_mode,
        )


def delete_saved_process(db: Database, process_id: int) -> bool:
    """Delete a saved process and null out every reference to it.

    Any workspace whose pipeline.default_process_id pointed here falls
    back to null ("import only"). Returns True if the process existed.
    The app-wide global default (config.json) is cleared by the caller,
    which owns config-file I/O. Does not run inside an outer transaction.
    """
    repo = db.processes
    with db.transaction(immediate=True):
        if repo.get(process_id) is None:
            return False
        return repo.delete(process_id)
