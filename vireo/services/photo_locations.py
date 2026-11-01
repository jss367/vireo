"""Reading a photo's location, and queueing the location-write backfill.

A photo's location comes from two places: EXIF GPS on the photo row, and the
``type='location'`` keyword it is tagged with. ``get_assigned_photo_location``
reads the keyword's coordinates for one photo after checking the active
workspace may see it (or, for the sync engine, may write its sidecar);
``get_effective_photo_location`` prefers EXIF and falls back to the keyword.
``get_photo_location_keyword_ids`` and ``get_photo_location_paths`` name the
leaf keyword and its chain for the sidecar's location keywords.
``queue_location_changes_for_tagged_photos`` queues a ``location`` change for
every located photo in the workspace (the "write my existing locations to XMP"
backfill).

The SQL is ``LocationRepository``'s (``db.locations``). The location-keyword
writers (``set_photo_location``, ``upsert_place_chain``, ...) stay on
``Database`` with the keyword workflows, and so do the workspace guards
(``_verify_photo_in_workspace``, ``_verify_photo_syncable_in_workspace``).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from db import Database
    from services.service_types import JsonDict

log = logging.getLogger(__name__)


def get_assigned_photo_location(
        db: Database, photo_id: int, verify_workspace: bool = True,
        allow_sync_only: bool = False) -> JsonDict | None:
    """Return linked location-keyword coordinates for one visible photo.

    ``allow_sync_only`` accepts a ``workspace_sync_only_photos`` grant
    as sufficient authorization. The sync engine passes it because that
    grant exists precisely to let a workspace write the sidecar of a
    photo it does not carry in its library: without it a remapped
    sibling-workspace ``location`` change resolves its path through the
    grant and then fails this check, staying queued on every retry.
    Browse and library callers keep the strict membership test.
    """
    if verify_workspace:
        if allow_sync_only:
            db._verify_photo_syncable_in_workspace(photo_id)
        else:
            db._verify_photo_in_workspace(photo_id)

    return db.locations.get_assigned(photo_id)


def get_photo_location_keyword_ids(db: Database, photo_ids: Iterable[int]) -> dict[int, int]:
    """Return the keyword IDs owning each photo's exported location."""
    return {pid: row["id"] for pid, row in db.locations.get_photo_leaves(photo_ids).items()}


def get_photo_location_paths(db: Database, photo_ids: Iterable[int]) -> dict[int, list[str]]:
    """Return ``{photo_id: [broadest, ..., leaf]}`` location keyword names.

    The leaf is chosen exactly as :func:`get_assigned_photo_location`
    chooses it -- coordinate-bearing first, then deepest in the chain,
    then most recent id as the tie-break -- so the keywords written to
    a sidecar always describe the same place as the GPS written beside
    them. When a photo carries both a coordinate-bearing location and
    a coordinate-less one (the generic keyword-add endpoint attaches
    another ``type='location'`` keyword without replacing the
    current), the coord-bearing row wins here just as it does in
    :func:`get_assigned_photo_location`. Unlike that function this one
    does not *require* coordinates: a free-text location the user
    typed still has a name worth writing when there is no coord-bearing
    alternative.

    Photos with no linked location are absent from the result, which is
    how the sync engine tells "write these keywords" from "remove the ones
    we wrote".
    """
    leaves = db.locations.get_photo_leaves(photo_ids)
    return db.locations.get_photo_paths(leaves)


def queue_location_changes_for_tagged_photos(db: Database) -> dict[str, int]:
    """Queue a ``location`` change for every located photo in the workspace.

    This is the backfill behind "write my existing locations to XMP":
    assigning a place queues the change at the time of assignment, so
    photos located before location writes were switched on have nothing
    queued and would never be written. What the queued change actually
    does to a sidecar still depends on the two location settings at sync
    time -- it writes coordinates, keywords, both, or removes what Vireo
    previously wrote -- and the sync review shows that per photo before
    anything is written.

    Returns ``{"photos": n, "queued": k, "already_queued": n - k}``.
    ``queue_change`` skips a duplicate, so re-running this is idempotent.
    """
    with db.transaction():
        result = db.locations.queue_changes_for_tagged_photos(
            queue_change=db.queue_change,
        )
    log.info(
        "Queued %d location change(s) for %d located photo(s)",
        result["queued"], result["photos"],
    )
    return result


def get_effective_photo_location(
        db: Database, photo_id: int, verify_workspace: bool = True) -> dict[str, Any] | None:
    """Return the coordinates Vireo should use for a single photo.

    EXIF GPS is source metadata and wins when both axes are present. If
    EXIF GPS is absent or partial, fall back as a pair to the linked
    ``type='location'`` keyword coordinates. Returns ``None`` when neither
    source has a complete coordinate pair.
    """
    return db.locations.get_effective(
        [photo_id], verify_workspace=verify_workspace
    ).get(photo_id)
