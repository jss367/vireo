"""Lifecycle of catalog-owned pipeline and culling result files.

These are disposable, workspace-owned snapshots, unlike original photos or
exports. Enumerate only exact Vireo filenames, never arbitrary JSON siblings.
"""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING

from catalog_result_files import result_files

if TYPE_CHECKING:
    from collections.abc import Callable, Container, Iterable

log = logging.getLogger(__name__)


def delete_workspace_results(
    catalog_dir: str, workspace_id: int | str, removed: list[str] | None = None,
) -> list[str]:
    """Remove a workspace's result files; return the errors.

    Paths actually removed are appended to ``removed`` when it is given, so
    a caller can report the work done, not only the failures.
    """
    errors = []
    workspace_id = int(workspace_id)
    for kind in ("pipeline", "culling"):
        path = os.path.join(catalog_dir, f"{kind}_results_ws{workspace_id}.json")
        try:
            os.unlink(path)
            if removed is not None:
                removed.append(path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            # IDs remain reserved even when an OS lock leaves files behind.
            log.warning("Could not remove workspace results %s", path, exc_info=True)
            errors.append(f"Could not remove {path}: {exc}")
    return errors


def prune_photo_results(catalog_dir: str, photo_ids: Iterable[int], workspace_ids: Iterable[int] = ()) -> None:
    """Prune global photo deletions from every workspace after SQL commits."""
    from pipeline import prune_results
    from pipeline_locks import acquire_workspace_regroup

    deleted = set(photo_ids)
    owners = set(workspace_ids) | {ws for _path, _kind, ws in result_files(catalog_dir)}
    for workspace_id in sorted(owners):
        # The catalog deletion has committed before taking this lock. Use
        # the writer's lock so an older pruned copy cannot replace a newly
        # grouped snapshot from another workspace.
        with acquire_workspace_regroup(workspace_id):
            # A first-time writer may publish while this sweep waits, so
            # inspect expected paths after acquiring its lock even when
            # no result file existed during the initial directory scan.
            for kind in ("pipeline", "culling"):
                path = os.path.join(catalog_dir, f"{kind}_results_ws{workspace_id}.json")
                if not os.path.islink(path):
                    _prune_file(path, kind, catalog_dir, workspace_id, deleted, prune_results)


def _prune_file(
    path: str, kind: str, catalog_dir: str, workspace_id: int, deleted: set[int],
    prune_results: Callable[[str, int, set[int]], object],
) -> None:
    try:
        if kind == "pipeline":
            prune_results(catalog_dir, workspace_id, deleted)
        else:
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            # Recompute counts/recommendations on the next analysis.
            # A culling snapshot combines interdependent keeper, rank
            # and grouping decisions; dropping the entire affected
            # snapshot is safer than keeping stale summary counts.
            if _contains_photo(data, deleted):
                os.unlink(path)
    except FileNotFoundError:
        pass
    except (OSError, ValueError, TypeError, KeyError):
        log.warning("Could not prune deleted photos from %s", path, exc_info=True)
        # Do not serve a known-stale snapshot after a failed rewrite.
        # A failed unlink is logged too and can be retried by cleanup.
        try:
            os.unlink(path)
        except OSError:
            log.warning("Could not remove stale results %s", path, exc_info=True)


def _contains_photo(value: object, photo_ids: Container[object]) -> bool:
    if isinstance(value, dict):
        if value.get("photo_id") in photo_ids:
            return True
        return any(_contains_photo(child, photo_ids) for child in value.values())
    if isinstance(value, list):
        return any(_contains_photo(child, photo_ids) for child in value)
    return False
