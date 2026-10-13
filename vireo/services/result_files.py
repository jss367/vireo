"""Lifecycle of catalog-owned pipeline and culling result files.

These are disposable, workspace-owned snapshots, unlike original photos or
exports. Enumerate only exact Vireo filenames, never arbitrary JSON siblings.
"""

import json
import logging
import os
import re

log = logging.getLogger(__name__)
RESULT_NAME = re.compile(r"^(pipeline|culling)_results_ws([1-9][0-9]*)\.json$")


def result_files(catalog_dir):
    try:
        with os.scandir(catalog_dir or os.curdir) as entries:
            return [
                (entry.path, match[1], int(match[2]))
                for entry in entries
                if (match := RESULT_NAME.fullmatch(entry.name)) and entry.is_file()
                and not entry.is_symlink()
                and int(match[2]) < 2**63 - 1
            ]
    except FileNotFoundError:
        return []


def highest_workspace_id(catalog_dir):
    return max((ws for _path, _kind, ws in result_files(catalog_dir)), default=0)


def delete_workspace_results(catalog_dir, workspace_id):
    errors = []
    workspace_id = int(workspace_id)
    for kind in ("pipeline", "culling"):
        path = os.path.join(catalog_dir, f"{kind}_results_ws{workspace_id}.json")
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            # IDs remain reserved even when an OS lock leaves files behind.
            log.warning("Could not remove workspace results %s", path, exc_info=True)
            errors.append(f"Could not remove {path}: {exc}")
    return errors


def prune_photo_results(catalog_dir, photo_ids, workspace_ids=()):
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


def _prune_file(path, kind, catalog_dir, workspace_id, deleted, prune_results):
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


def _contains_photo(value, photo_ids):
    if isinstance(value, dict):
        if value.get("photo_id") in photo_ids:
            return True
        return any(_contains_photo(child, photo_ids) for child in value.values())
    if isinstance(value, list):
        return any(_contains_photo(child, photo_ids) for child in value)
    return False
