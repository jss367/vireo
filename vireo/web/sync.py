"""Sync: the queue of pending XMP writes, its review, and discarding it.

The ``/api/sync/*`` routes back the sync panel: queue counts, the location
write backfill, the progressive preview that shows each pending change next
to the XMP value it will replace, and discarding reviewed changes. The
preview helpers mirror the write order and guards in ``sync.py`` so the
review never promises a write the sync will not make. Starting the sync
itself is a background job and lives with the other job routes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from collections import OrderedDict

from flask import Blueprint, jsonify, request
from keyword_normalization import keyword_match_key
from photo_payload import render_key_for_recipe
from xmp import location_keyword_entries, read_sync_preview_metadata

log = logging.getLogger(__name__)


def _sync_preview_coordinate_text(location):
    """Return a compact, truthful coordinate label for sync review."""
    if not location:
        return None
    latitude = location.get("latitude")
    longitude = location.get("longitude")
    if latitude is not None and longitude is not None:
        return f"{latitude:.5f}, {longitude:.5f}"
    raw_latitude = location.get("raw_latitude")
    raw_longitude = location.get("raw_longitude")
    if raw_latitude is None and raw_longitude is None:
        return None
    return ", ".join(
        str(value) for value in (raw_latitude, raw_longitude)
        if value is not None
    )


def _sync_preview_location_name(location):
    """Format a location keyword leaf and its parents from specific to broad."""
    if not location:
        return None
    names = [location.get("name")]
    names.extend(
        parent.get("name")
        for parent in reversed(location.get("parent_chain") or [])
    )
    result = []
    seen = set()
    for name in names:
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(name)
    return ", ".join(result) or None


def _sync_preview_absent_xmp_value(metadata, empty_value):
    if metadata.get("status") == "missing":
        return "No XMP sidecar"
    if metadata.get("status") == "unreadable":
        return "Unreadable XMP sidecar"
    return empty_value


_SYNC_PREVIEW_FIELD_LABELS = {
    "keyword_add": "Keyword",
    "keyword_remove": "Keyword",
    "keyword_remove_flat": "Keyword",
    "keyword_merge": "Keyword hierarchy",
    "rating": "Rating",
    "flag": "Flag",
    "location": "Location",
    "edit_recipe": "Photo edits",
}


def _sync_preview_folder_offline_presentation(change_type):
    """Presentation for a change whose photo folder isn't accessible.

    ``sync_to_xmp`` calls ``os.path.isdir(os.path.dirname(xmp_path))`` before
    every write; when it returns False the photo is recorded as
    ``folder not accessible`` and none of the writers run. Mirror that no-op
    in the review so a NAS-offline or unmounted-folder photo does not appear
    to promise XMP changes that will never happen.
    """
    field = _SYNC_PREVIEW_FIELD_LABELS.get(
        change_type,
        change_type.replace("_", " ").strip().title() or "Metadata",
    )
    placeholder = "Folder not accessible"
    return {
        "field": field,
        "action": "unchanged",
        "before": placeholder,
        "after": placeholder,
        "after_detail": (
            "Sync will skip this photo because its folder is offline; "
            "no XMP will be written"
        ),
    }


def _sync_preview_rating_label(value):
    if value in (None, "", "0", 0):
        return "Unrated"
    try:
        rating = int(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{rating} star" if rating == 1 else f"{rating} stars"


def _sync_preview_flag_label(value):
    return {
        None: "Unflagged",
        "": "Unflagged",
        "none": "Unflagged",
        "flagged": "Picked",
        "rejected": "Rejected",
    }.get(value, str(value))


def _discard_history_items(db, changes):
    """Build durable discard history without conflating keyword homonyms."""
    items = []
    for change in changes:
        discarded_keyword_id = ""
        if change["change_type"] == "keyword_add":
            associations = db.conn.execute(
                """SELECT pk.keyword_id, pk.source,
                          EXISTS (
                              SELECT 1
                              FROM edit_history_items item
                              JOIN edit_history edit ON edit.id = item.edit_id
                              WHERE item.photo_id = pk.photo_id
                                AND edit.action_type = 'keyword_add'
                                AND edit.undone = 0
                                AND item.new_value = CAST(pk.keyword_id AS TEXT)
                          ) AS has_exact_history
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   WHERE pk.photo_id = ?
                     AND k.name = ? COLLATE NOCASE""",
                (change["photo_id"], change["value"]),
            ).fetchall()
            evidenced = [
                row for row in associations
                if row["source"] == "manual" or row["has_exact_history"]
            ]
            if len(associations) == 1:
                discarded_keyword_id = str(associations[0]["keyword_id"])
            elif len(evidenced) == 1:
                discarded_keyword_id = str(evidenced[0]["keyword_id"])
        items.append({
            "photo_id": change["photo_id"],
            "old_value": f'{change["change_type"]}:{change["value"]}',
            "new_value": discarded_keyword_id,
        })
    return items


def _sync_preview_location_gps_presentation(
    metadata, assigned_location, write_locations,
):
    """Describe the GPS half of a queued ``location`` change."""
    current_coordinates = _sync_preview_coordinate_text(
        metadata.get("location")
    )
    before = current_coordinates or _sync_preview_absent_xmp_value(
        metadata, "No GPS in XMP",
    )
    location_coordinates = _sync_preview_coordinate_text(assigned_location)
    location_name = _sync_preview_location_name(assigned_location)

    if write_locations and location_coordinates:
        return {
            "field": "Location",
            "action": "updated",
            "before": before,
            "after": location_name or location_coordinates,
            "after_detail": (
                f"{location_coordinates} · from a location keyword"
            ),
        }

    if assigned_location:
        if location_coordinates:
            detail = (
                f"{location_name} is assigned in Vireo; writing its GPS "
                "to XMP is turned off"
            )
        else:
            detail = (
                f"{location_name or 'This location'} is assigned in Vireo; "
                "it has no GPS coordinates to write to XMP"
            )

        if metadata.get("location_source"):
            restored_coordinates = _sync_preview_coordinate_text(
                metadata.get("previous_location")
            )
            if restored_coordinates:
                detail += f"; XMP GPS returns to {restored_coordinates}"
            else:
                detail += "; previously Vireo-assigned GPS is removed from XMP"

        return {
            "field": "Location",
            "action": "added",
            "before": before,
            "after": location_name or "Assigned location",
            "after_detail": detail,
        }

    if metadata.get("location_source"):
        restored_coordinates = _sync_preview_coordinate_text(
            metadata.get("previous_location")
        )
        return {
            "field": "Location",
            "action": "cleared",
            "before": before,
            "after": restored_coordinates or "No GPS in XMP",
            "after_detail": (
                "Restores the GPS that existed before Vireo"
                if restored_coordinates
                else "Removes Vireo-assigned GPS"
            ),
        }

    return {
        "field": "XMP location",
        "action": "unchanged",
        "before": before,
        "after": before,
        "after_detail": "No Vireo-assigned GPS needs to be removed",
    }


def _sync_preview_merge_location_keywords(
    presentation, metadata, assigned_location, location_path,
    write_location_keywords,
):
    """Fold the keyword half of a ``location`` change into its presentation.

    One queued ``location`` row can write two different things -- GPS and
    keywords -- under two independent settings, and the review has one line
    per row to say what the sync will do. The GPS wording is left exactly as
    it was and the keyword outcome is appended, so a review of a catalog with
    location keywords turned off (the default) reads as it always has.

    "Already in XMP" is checked against the sidecar's actual entries, not just
    the marker Vireo stamped: a keyword deleted in Lightroom must read as a
    write, not as a no-op.
    """
    def in_sidecar(path):
        """Whether the sidecar still carries both entries for one place path."""
        leaf, hierarchical = location_keyword_entries(path)
        if not hierarchical:
            return False
        leaf_key = keyword_match_key(leaf)
        # Match ``set_location_keywords``'s ``existed_hier`` logic: it treats
        # a sidecar variant that differs only in case or normalized spacing
        # as already present and skips the write. An exact-string check here
        # would tell the reviewer "writes the keyword X" for a queued row
        # the sync will then leave untouched.
        path_keys = [keyword_match_key(part) for part in hierarchical.split("|")]
        return (
            any(
                [keyword_match_key(s) for s in entry.split("|")] == path_keys
                for entry in (metadata.get("hierarchical_keywords") or set())
            )
            and any(
                keyword_match_key(keyword) == leaf_key
                for keyword in (metadata.get("keywords") or set())
            )
        )

    hierarchy = "|".join(location_path or ())
    previously_written = metadata.get("location_keywords")

    if write_location_keywords and hierarchy:
        already_written = previously_written == hierarchy and in_sidecar(hierarchy)
        detail = (
            f"XMP already lists the keyword {hierarchy}"
            if already_written
            else f"writes the keyword {hierarchy}"
        )
        changes_keywords = not already_written
    elif previously_written:
        # The marker can outlive its entries -- someone deleted the keyword in
        # Lightroom. Clearing the marker is still a write, but promising to
        # remove a keyword that is already gone would not be true.
        detail = (
            f"removes the keyword {previously_written} Vireo wrote"
            if in_sidecar(previously_written)
            else "clears the location-keyword marker Vireo left in XMP"
        )
        if not write_location_keywords:
            detail += "; writing location keywords to XMP is turned off"
        changes_keywords = True
    else:
        return presentation

    merged = dict(presentation)
    merged["after_detail"] = " · ".join(
        part for part in (presentation.get("after_detail"), detail) if part
    )
    if changes_keywords:
        # The GPS half may have nothing to do -- the setting is off, or there
        # was never any Vireo GPS to remove -- while the keyword half still
        # rewrites the sidecar. Reporting that as "unchanged" would promise a
        # no-op and then write the file.
        merged["field"] = "Location"
        if merged["action"] == "unchanged":
            if hierarchy:
                merged["action"] = "updated"
                merged["after"] = (
                    _sync_preview_location_name(assigned_location)
                    or location_path[-1]
                )
            else:
                merged["action"] = "cleared"
                merged["after"] = "No Vireo location keywords in XMP"
    return merged


def _sync_preview_presentation(
    change, metadata, *, assigned_location=None, write_locations=False,
    location_path=None, write_location_keywords=False,
    sidecar_will_exist=False, sync_flags=False, paired_keyword_rename=False,
    paired_add_value=None, folder_offline=False,
):
    """Translate one internal pending row into user-facing XMP before/after data."""
    change_type = change["change_type"]
    value = change["value"]

    if folder_offline:
        return _sync_preview_folder_offline_presentation(change_type)

    if change_type == 'keyword_merge':
        merge = json.loads(value)
        target_path = change.get('merge_target_path')
        return {
            'field': 'Keyword hierarchy',
            'action': 'updated' if target_path else 'removed',
            'before': ' → '.join(merge['source_path']),
            'after': ' → '.join(target_path) if target_path else 'Removed',
            'after_detail': 'The merged keyword and its hierarchy sync together',
        }

    if change_type in {"keyword_add", "keyword_remove", "keyword_remove_flat"}:
        existing = next(
            (
                keyword for keyword in metadata.get("keywords", set())
                if keyword_match_key(keyword) == keyword_match_key(value)
            ),
            None,
        )
        # Solo keyword_remove routes through remove_keywords() with
        # hierarchical=True in sync.py, which drops every
        # lr:hierarchicalSubject whose segments match this key in addition
        # to the flat dc:subject entry. Collect every hierarchy sync would
        # touch so the review reflects the full deletion.
        matching_hierarchies = []
        if change_type == "keyword_remove":
            matching_hierarchies = sorted(
                keyword
                for keyword in metadata.get("hierarchical_keywords", set())
                if any(
                    keyword_match_key(segment) == keyword_match_key(value)
                    for segment in keyword.split("|")
                )
            )
        hierarchy_display = [
            keyword.replace("|", " › ") for keyword in matching_hierarchies
        ]
        if (
            change_type == "keyword_remove"
            and paired_keyword_rename
            and hierarchy_display
        ):
            hierarchy_text = "; ".join(hierarchy_display)
            return {
                "field": "Keyword hierarchy",
                "action": "unchanged",
                "before": hierarchy_text,
                "after": hierarchy_text,
                "after_detail": (
                    "The matching keyword addition replaces only the "
                    "flat spelling; this hierarchy stays in XMP"
                ),
            }
        # A paired flat-only rename (e.g. remove `‘apapane` + add `apapane`)
        # is dispatched through the flat-only remove path in sync.py, and
        # the paired write_sidecar then adds the clean spelling. Reporting
        # the removal as `Not in XMP` hides that the keyword survives with
        # a canonicalized flat spelling; show it as an unchanged keyword
        # whose flat entry is being rewritten by the paired addition.
        if (
            change_type == "keyword_remove"
            and paired_keyword_rename
            and existing
            and paired_add_value
            and not hierarchy_display
        ):
            return {
                "field": "Keyword",
                "action": "unchanged",
                "before": existing,
                "after": paired_add_value,
                "after_detail": (
                    "The matching keyword addition rewrites this flat "
                    "spelling; the keyword itself stays in XMP"
                ),
            }
        xmp_value = existing or _sync_preview_absent_xmp_value(
            metadata, "Not in XMP",
        )
        if change_type == "keyword_add":
            return {
                "field": "Keyword",
                "action": "added",
                "before": xmp_value,
                "after": value,
            }
        if change_type == "keyword_remove" and not paired_keyword_rename:
            removed_entries = []
            if existing:
                removed_entries.append(existing)
            removed_entries.extend(hierarchy_display)
            if removed_entries:
                return {
                    "field": "Keyword",
                    "action": "removed",
                    "before": "; ".join(removed_entries),
                    "after": "Not in XMP",
                }
        if existing is None:
            if metadata.get("status") == "unreadable":
                detail = (
                    f"{value} cannot be removed because the XMP sidecar "
                    "is unreadable"
                )
            elif metadata.get("status") == "missing":
                detail = f"No XMP sidecar contains {value} to remove"
            else:
                detail = f"{value} is not present in XMP"
            return {
                "field": "XMP keyword",
                "action": "unchanged",
                "before": xmp_value,
                "after": xmp_value,
                "after_detail": detail,
            }
        return {
            "field": "Keyword",
            "action": "removed",
            "before": xmp_value,
            "after": "Not in XMP",
        }

    if change_type == "rating":
        before = _sync_preview_rating_label(metadata.get("rating"))
        if not metadata.get("rating_writable") and not sidecar_will_exist:
            before = _sync_preview_absent_xmp_value(metadata, before)
            return {
                "field": "XMP rating",
                "action": "unchanged",
                "before": before,
                "after": before,
                "after_detail": (
                    f"{_sync_preview_rating_label(value)} stays in Vireo; "
                    "rating sync only updates an existing, readable XMP sidecar"
                ),
            }
        if not metadata.get("rating_writable"):
            before = _sync_preview_absent_xmp_value(metadata, before)
            return {
                "field": "Rating",
                "action": "updated",
                "before": before,
                "after": _sync_preview_rating_label(value),
                "after_detail": (
                    "Another selected change creates the XMP sidecar first"
                ),
            }
        return {
            "field": "Rating",
            "action": "updated",
            "before": before,
            "after": _sync_preview_rating_label(value),
        }

    if change_type == "flag":
        before = _sync_preview_flag_label(metadata.get("flag"))
        if metadata.get("status") != "ok":
            before = _sync_preview_absent_xmp_value(metadata, before)
        if not sync_flags:
            return {
                "field": "XMP flag",
                "action": "unchanged",
                "before": before,
                "after": before,
                "after_detail": (
                    f"{_sync_preview_flag_label(value)} stays in Vireo; "
                    "flag sync to XMP is turned off"
                ),
            }
        return {
            "field": "Flag",
            "action": "updated",
            "before": before,
            "after": _sync_preview_flag_label(value),
        }

    if change_type == "location":
        return _sync_preview_merge_location_keywords(
            _sync_preview_location_gps_presentation(
                metadata, assigned_location, write_locations,
            ),
            metadata,
            assigned_location,
            location_path,
            write_location_keywords,
        )

    if change_type == "edit_recipe":
        before = (
            "Existing Vireo edits"
            if metadata.get("edit_recipe")
            else _sync_preview_absent_xmp_value(metadata, "No Vireo edits")
        )
        if not value and not metadata.get("edit_recipe"):
            if metadata.get("status") == "unreadable":
                detail = (
                    "The Vireo edit marker cannot be cleared because the "
                    "XMP sidecar is unreadable"
                )
            elif metadata.get("status") == "missing":
                detail = "No XMP sidecar contains Vireo edits to clear"
            else:
                detail = "No Vireo edit marker exists in XMP to clear"
            return {
                "field": "XMP photo edits",
                "action": "unchanged",
                "before": before,
                "after": before,
                "after_detail": detail,
            }
        return {
            "field": "Photo edits",
            "action": "updated" if value else "cleared",
            "before": before,
            "after": "Updated Vireo edits" if value else "No Vireo edits",
        }

    field = change_type.replace("_", " ").strip().title() or "Metadata"
    return {
        "field": field,
        "action": "updated",
        "before": "Current XMP value",
        "after": value or "Cleared",
    }


# Server-side snapshot cache for /api/sync/preview. Progressive page loads
# request the same pending-changes revision many times in a row; without a
# cache each request re-scans the full queue and re-hashes every change,
# making preview preparation quadratic in the queue size. Keyed by
# (database path, workspace_id, revision) so separate catalogs whose default
# workspace and pending rows happen to match cannot reuse each other's photo
# paths. Only the newest revision for each catalog/workspace is retained, and
# the remaining entries are bounded by count with LRU eviction.
_SYNC_PREVIEW_SNAPSHOTS = OrderedDict()
_SYNC_PREVIEW_SNAPSHOTS_LOCK = threading.Lock()
_SYNC_PREVIEW_SNAPSHOTS_MAX = 8


def _sync_preview_pending_fingerprint(db, ws_id):
    """Return the per-workspace monotonic write generation for pending_changes.

    A dedicated ``db_meta`` counter — ``pending_changes_version:<ws>`` —
    is bumped by INSERT/UPDATE/DELETE triggers on ``pending_changes``
    (see ``db.py`` next to the ``folder_health_version`` triggers).
    Reading it is a single-row lookup and, crucially, it changes for
    every row write including a delete of the top id followed by an
    INSERT that reuses it — ``pending_changes.id`` is a plain
    ``INTEGER PRIMARY KEY`` without ``AUTOINCREMENT``, so SQLite will
    do exactly that on the next INSERT after the highest row is
    deleted. A cheaper COUNT/MAX/SUM aggregate would stay identical
    across such a replacement even though ``change_token``, ``value``,
    ``change_type``, or ``photo_id`` differ, and the cached snapshot
    would then be served to a client that had never seen the new row.
    """
    row = db.conn.execute(
        "SELECT value FROM db_meta WHERE key = ?",
        (f"pending_changes_version:{ws_id}",),
    ).fetchone()
    return int(row[0]) if row else 0


def _sync_preview_build_snapshot(db, ws_id):
    """Load pending changes, group by photo, and compute the revision hash."""
    # Read the version counter BEFORE the row scan. If a concurrent write
    # commits between the two reads, the stored fingerprint is one behind
    # the actual queue state, and the next cache lookup will see a
    # mismatched (larger) counter and rebuild. Reading the counter after
    # the rows would risk stamping stale rows with a post-write version
    # and serving them from cache.
    fingerprint = _sync_preview_pending_fingerprint(db, ws_id)
    changes = db.conn.execute(
        """
        SELECT pc.*, p.filename, p.folder_id, f.path AS folder_path
        FROM pending_changes pc
        JOIN photos p ON p.id = pc.photo_id
        LEFT JOIN folders f ON f.id = p.folder_id
        WHERE pc.workspace_id = ?
        ORDER BY pc.created_at, pc.id
        """,
        (ws_id,),
    ).fetchall()

    revision_hash = hashlib.sha256()
    change_type_counts = {}
    by_photo = {}
    for change in changes:
        revision_hash.update(
            (
                f"{change['id']}\0{change['change_token'] or ''}\0"
                f"{change['photo_id']}\0{change['change_type']}\0"
                f"{change['value'] or ''}\n"
            ).encode()
        )
        change_type = change["change_type"]
        change_type_counts[change_type] = (
            change_type_counts.get(change_type, 0) + 1
        )
        cid = change["id"]
        pid = change["photo_id"]
        photo = by_photo.get(pid)
        if photo is None:
            photo = {
                "photo_id": pid,
                "filename": change["filename"],
                "folder": change["folder_path"] or "",
                "changes": [],
            }
            by_photo[pid] = photo
        photo["changes"].append({
            "id": cid,
            "type": change_type,
            # ``_sync_preview_change_creates_sidecar`` and
            # ``_sync_preview_presentation`` both read ``change_type`` and
            # ``value`` off the row they're passed; storing them under both
            # names lets the enrichment loop use these dicts directly
            # without a per-request lookup back into the DB row.
            "change_type": change_type,
            "value": change["value"],
        })

    return {
        "revision": revision_hash.hexdigest()[:24],
        "all_photos": list(by_photo.values()),
        "total_changes": len(changes),
        "change_type_counts": change_type_counts,
        "fingerprint": fingerprint,
    }


def _sync_preview_get_snapshot(db, ws_id, requested_revision):
    """Return a cached or freshly computed snapshot of pending changes.

    Progressive page loads normally arrive back-to-back with the same
    ``requested_revision``; on cache hit a lightweight aggregate check
    verifies the pending queue hasn't shifted since the snapshot was
    stored. On miss (or when the queue has shifted) the full snapshot is
    rebuilt and cached under its computed revision.
    """
    database_key = os.path.abspath(db._db_path)
    if requested_revision:
        key = (database_key, ws_id, requested_revision)
        with _SYNC_PREVIEW_SNAPSHOTS_LOCK:
            snapshot = _SYNC_PREVIEW_SNAPSHOTS.get(key)
            if snapshot is not None:
                _SYNC_PREVIEW_SNAPSHOTS.move_to_end(key)
        if snapshot is not None:
            if _sync_preview_pending_fingerprint(db, ws_id) == snapshot["fingerprint"]:
                return snapshot
            with _SYNC_PREVIEW_SNAPSHOTS_LOCK:
                _SYNC_PREVIEW_SNAPSHOTS.pop(key, None)

    snapshot = _sync_preview_build_snapshot(db, ws_id)
    key = (database_key, ws_id, snapshot["revision"])
    with _SYNC_PREVIEW_SNAPSHOTS_LOCK:
        obsolete_keys = [
            cached_key
            for cached_key in _SYNC_PREVIEW_SNAPSHOTS
            if cached_key[:2] == (database_key, ws_id) and cached_key != key
        ]
        for obsolete_key in obsolete_keys:
            _SYNC_PREVIEW_SNAPSHOTS.pop(obsolete_key, None)
        _SYNC_PREVIEW_SNAPSHOTS[key] = snapshot
        _SYNC_PREVIEW_SNAPSHOTS.move_to_end(key)
        while len(_SYNC_PREVIEW_SNAPSHOTS) > _SYNC_PREVIEW_SNAPSHOTS_MAX:
            _SYNC_PREVIEW_SNAPSHOTS.popitem(last=False)
    return snapshot


def _sync_preview_change_creates_sidecar(
    change, *, sync_flags=False, write_locations=False, assigned_location=None,
    write_location_keywords=False, location_path=None,
):
    """Mirror sync operations that create a missing XMP sidecar before rating.

    Matches the write order in ``sync.py``: ``add_keywords`` (keyword_add),
    ``set_pick_flag`` when flag sync is enabled, ``set_gps_location``
    when location sync is enabled and the linked location has valid
    coordinates, ``set_location_keywords`` when location keyword sync is
    enabled and the photo has a place, and ``set_edit_recipe`` with a
    non-empty payload all
    create a missing sidecar through ``SidecarEditor``. ``set_rating``
    and the ``remove_*`` paths do not, so they are excluded.
    """
    change_type = change["change_type"]
    if change_type == 'keyword_merge':
        return bool(change.get('merge_target_path'))
    if change_type == "keyword_add":
        return True
    if change_type == "flag":
        return sync_flags
    if change_type == "location":
        # ``set_location_keywords`` creates a sidecar exactly like a keyword
        # add does, and it needs no coordinates to do it.
        if write_location_keywords and location_path:
            return True
        if not write_locations or not assigned_location:
            return False
        return (
            assigned_location.get("latitude") is not None
            and assigned_location.get("longitude") is not None
        )
    if change_type == "edit_recipe":
        return bool(change["value"])
    return False


def create_sync_blueprint(get_db, json_error, get_runner, *, walk_parent_chain):
    """Build the sync blueprint.

    ``get_runner`` returns the app's job runner, used to spot a sync job
    already running in the active workspace. ``walk_parent_chain`` resolves
    a location keyword's ancestors; the single-photo and keyword location
    serializers still in ``create_app`` share it, so it is injected rather
    than moved.
    """
    blueprint = Blueprint("sync", __name__)

    def _serialize_photo_locations(db, photo_ids):
        """Bulk form of :func:`_serialize_photo_location`.

        Sync review can contain thousands of location changes.  Looking up
        each leaf and then walking the same parent chain once per photo turns
        that review into tens of thousands of SQLite queries.  Fetch leaves
        in chunks and reuse each distinct hierarchy instead.
        """
        if not photo_ids:
            return {}

        leaves = {}
        # Keep the first linked location, matching the single-photo helper's
        # LIMIT 1 behavior without relying on a window-function result shape.
        for start in range(0, len(photo_ids), 400):
            chunk = photo_ids[start:start + 400]
            placeholders = ",".join("?" for _ in chunk)
            rows = db.conn.execute(
                f"""
                SELECT pk.photo_id, k.id, k.name, k.place_id, k.latitude,
                       k.longitude, k.parent_id
                FROM photo_keywords pk
                JOIN keywords k ON k.id = pk.keyword_id
                WHERE pk.photo_id IN ({placeholders})
                  AND k.type = 'location'
                ORDER BY pk.rowid
                """,
                chunk,
            ).fetchall()
            for row in rows:
                leaves.setdefault(row["photo_id"], row)

        chain_cache = {}
        result = {}
        for photo_id, leaf in leaves.items():
            parent_id = leaf["parent_id"]
            if parent_id not in chain_cache:
                chain_cache[parent_id] = walk_parent_chain(db, parent_id)
            result[photo_id] = {
                "keyword_id": leaf["id"],
                "name": leaf["name"],
                "place_id": leaf["place_id"],
                "latitude": leaf["latitude"],
                "longitude": leaf["longitude"],
                "parent_chain": chain_cache[parent_id],
            }
        return result

    def _active_workspace_sync(db):
        for job in get_runner().list_jobs():
            if (job.get("type") == "sync"
                    and job.get("workspace_id") == db._ws_id()
                    and job.get("status") in ("queued", "running", "pausing", "paused")):
                return {key: job[key] for key in ("id", "status", "progress")}
        return None

    @blueprint.route("/api/sync/status")
    def api_sync_status():
        db = get_db()
        # One statement keeps totals and per-type counts in the same snapshot,
        # without loading every queued row into Python on each progress poll.
        counts = db.conn.execute(
            """SELECT NULL AS change_type, COUNT(*) AS changes,
                      COUNT(DISTINCT photo_id) AS photos
               FROM pending_changes WHERE workspace_id = ?
               UNION ALL
               SELECT change_type, COUNT(*), 0
               FROM pending_changes WHERE workspace_id = ? GROUP BY change_type""",
            (db._ws_id(), db._ws_id()),
        ).fetchall()
        return jsonify({
            "pending_count": counts[0]["changes"],
            "pending_photo_count": counts[0]["photos"],
            "change_type_counts": {row["change_type"]: row["changes"] for row in counts[1:]},
            "active_job": _active_workspace_sync(db),
        })

    @blueprint.route("/api/sync/location-writes")
    def api_sync_location_writes_status():
        """Report what queueing location writes for this workspace would do.

        The counts are the honest ones for the button that follows: how many
        photos carry a place, and how many of those already have a ``location``
        change waiting in the sync queue. Whether a queued change writes
        coordinates, keywords, both, or removes what Vireo previously wrote is
        decided at sync time by the two settings reported here, and shown per
        photo in the review before anything is written.
        """
        db = get_db()
        import config as cfg

        effective_config = db.get_effective_config(cfg.load())
        photos = db.count_photos_with_location()
        queued = db.conn.execute(
            """SELECT COUNT(DISTINCT pc.photo_id)
               FROM pending_changes pc
               JOIN photo_keywords pk ON pk.photo_id = pc.photo_id
               JOIN keywords k ON k.id = pk.keyword_id
               WHERE pc.workspace_id = ? AND pc.change_type = 'location'
                 AND k.type = 'location'""",
            (db._ws_id(),),
        ).fetchone()[0]
        return jsonify({
            "photos_with_location": photos,
            "already_queued": queued,
            "location_sync_enabled": bool(
                effective_config.get("write_assigned_location_to_xmp", False)
            ),
            "location_keyword_sync_enabled": bool(
                effective_config.get("write_location_keywords_to_xmp", False)
            ),
        })

    @blueprint.route("/api/sync/location-writes", methods=["POST"])
    def api_sync_queue_location_writes():
        """Queue a location change for every located photo in the workspace.

        Assigning a place queues its sidecar write at assignment time, so
        photos located before a location setting was turned on have nothing
        queued. This is the backfill for that, and it stops at the sync queue:
        the user still reviews the changes and starts the sync themselves.
        """
        db = get_db()
        result = db.queue_location_changes_for_tagged_photos()
        return jsonify({"ok": True, **result})

    @blueprint.route("/api/sync/preview")
    def api_sync_preview():
        """Preview pending changes with the XMP values they will replace.

        Supplying ``limit`` enables progressive photo pagination.  Omitting
        it retains the original all-at-once response for compatibility with
        existing API callers.
        """
        db = get_db()
        if _active_workspace_sync(db):
            return json_error(
                "XMP sync is in progress. Review will load after it finishes.",
                409, code="sync_in_progress",
            )
        raw_limit = request.args.get("limit")
        raw_offset = request.args.get("offset", "0")
        if raw_limit is None:
            if raw_offset != "0":
                return json_error("offset requires limit")
            limit = None
            offset = 0
        else:
            try:
                limit = int(raw_limit)
                offset = int(raw_offset)
            except (TypeError, ValueError):
                return json_error("limit and offset must be integers")
            if limit < 1 or limit > 200:
                return json_error("limit must be between 1 and 200")
            if offset < 0:
                return json_error("offset must be non-negative")

        requested_revision = request.args.get("revision")
        snapshot = _sync_preview_get_snapshot(
            db, db._ws_id(), requested_revision,
        )
        revision = snapshot["revision"]
        if requested_revision and requested_revision != revision:
            return json_error(
                "pending changes changed while the review was loading",
                409,
                code="sync_preview_changed",
                message="Pending changes changed. Restarting the review.",
            )

        all_photos = snapshot["all_photos"]
        total_photos = len(all_photos)
        total_changes = snapshot["total_changes"]
        change_type_counts = snapshot["change_type_counts"]
        if total_changes == 0:
            return jsonify({
                "photos": [],
                "total_changes": 0,
                "total_photos": 0,
                "change_type_counts": {},
                "offset": offset,
                "next_offset": None,
                "has_more": False,
                "revision": revision,
            })

        # Enrichment below mutates the per-change dicts (``creates_xmp_sidecar``,
        # ``presentation``, …), so copy the slice before touching it — the
        # underlying photos/changes lists live inside the cached snapshot and
        # are re-served across page requests for the same revision.
        page_slice = (
            all_photos
            if limit is None
            else all_photos[offset:offset + limit]
        )
        page_photos = [
            {
                "photo_id": photo["photo_id"],
                "filename": photo["filename"],
                "folder": photo["folder"],
                "changes": [dict(change) for change in photo["changes"]],
            }
            for photo in page_slice
        ]

        import config as cfg

        effective_config = db.get_effective_config(cfg.load())
        write_locations = bool(
            effective_config.get("write_assigned_location_to_xmp", False)
        )
        write_location_keywords = bool(
            effective_config.get("write_location_keywords_to_xmp", False)
        )
        sync_flags = bool(effective_config.get("sync_flags_to_xmp", False))
        location_photo_ids = [
            photo["photo_id"]
            for photo in page_photos
            if any(change["type"] == "location" for change in photo["changes"])
        ]
        assigned_locations = _serialize_photo_locations(db, location_photo_ids)
        # Read the keyword chain from the same helper ``sync_to_xmp`` writes
        # from, so the review cannot name one place and the sync write another.
        location_paths = db.get_photo_location_paths(location_photo_ids)
        from keyword_identity import keyword_paths, resolve_merge_target
        merge_paths = keyword_paths(db.conn.execute(
            'SELECT id, name, parent_id FROM keywords'
        ).fetchall()) if any(
            change['type'] == 'keyword_merge'
            for photo in page_photos for change in photo['changes']
        ) else {}
        folder_accessibility = {}
        for photo in page_photos:
            merges = [c for c in photo['changes'] if c['type'] == 'keyword_merge']
            if merges:
                tagged_ids = {k['id'] for k in db.get_photo_keywords(photo['photo_id'])}
                for change in merges:
                    target_id = resolve_merge_target(db, json.loads(change['value']))
                    change['merge_target_path'] = merge_paths.get(target_id) if target_id in tagged_ids else None
            xmp_path = os.path.join(
                photo["folder"],
                os.path.splitext(photo["filename"])[0] + ".xmp",
            )
            # Mirror the folder-accessibility guard in ``sync_to_xmp``: if
            # the photo's folder is offline (a common NAS case), sync
            # records the photo as ``folder not accessible`` and never
            # runs any writer, so the review must not present writes
            # against it either.
            folder = photo["folder"]
            if folder not in folder_accessibility:
                folder_accessibility[folder] = (
                    not bool(folder) or os.path.isdir(folder)
                )
            folder_offline = not folder_accessibility[folder]
            photo["folder_offline"] = folder_offline
            if folder_offline:
                # Skip filesystem access entirely — an unreachable XMP path
                # would just re-derive the same "no sidecar" fallback.
                metadata = {
                    "status": "folder_offline",
                    "keywords": set(),
                    "hierarchical_keywords": set(),
                    "rating": None,
                    "rating_writable": False,
                    "flag": None,
                    "location": None,
                    "previous_location": None,
                    "location_source": None,
                    "location_keywords": None,
                    "edit_recipe": None,
                }
            else:
                metadata = read_sync_preview_metadata(xmp_path)
            assigned_location = None
            location_path = None
            if (
                not folder_offline
                and any(change["type"] == "location" for change in photo["changes"])
            ):
                assigned_location = assigned_locations.get(photo["photo_id"])
                location_path = location_paths.get(photo["photo_id"])
            # Map normalized-key -> original add value so a paired
            # keyword_remove can display the clean spelling the paired
            # ``write_sidecar`` will end up writing.
            keyword_add_values_by_key = {}
            for change in photo["changes"]:
                if change["type"] == "keyword_add" and change["value"]:
                    key = keyword_match_key(change["value"])
                    if key:
                        keyword_add_values_by_key.setdefault(
                            key, change["value"],
                        )
            keyword_add_keys = set(keyword_add_values_by_key.keys())
            for change in photo["changes"]:
                auto_includes_keyword_add = bool(
                    change["type"] in {"keyword_remove", "keyword_remove_flat"}
                    and keyword_match_key(change["value"]) in keyword_add_keys
                )
                change["auto_includes_keyword_add"] = (
                    auto_includes_keyword_add
                )
                change["paired_keyword_rename"] = bool(
                    change["type"] == "keyword_remove"
                    and auto_includes_keyword_add
                )
                # An offline folder never runs any writer, so nothing
                # creates a sidecar during that sync.
                change["creates_xmp_sidecar"] = (
                    not folder_offline
                    and (
                        auto_includes_keyword_add
                        or _sync_preview_change_creates_sidecar(
                            change,
                            sync_flags=sync_flags,
                            write_locations=write_locations,
                            assigned_location=assigned_location,
                            write_location_keywords=write_location_keywords,
                            location_path=location_path,
                        )
                    )
                )

            sidecar_will_exist = any(
                change["creates_xmp_sidecar"] for change in photo["changes"]
            )
            for change in photo["changes"]:
                paired_add_value = None
                if change["type"] == "keyword_remove" and change[
                    "paired_keyword_rename"
                ]:
                    paired_add_value = keyword_add_values_by_key.get(
                        keyword_match_key(change["value"])
                    )
                if (
                    change["change_type"] == "rating"
                    and not folder_offline
                    and not metadata.get("rating_writable")
                ):
                    change["rating_requires_sidecar"] = True
                    change["presentation_without_sidecar"] = (
                        _sync_preview_presentation(
                            change,
                            metadata,
                            assigned_location=assigned_location,
                            write_locations=write_locations,
                            location_path=location_path,
                            write_location_keywords=write_location_keywords,
                            sidecar_will_exist=False,
                            sync_flags=sync_flags,
                            paired_keyword_rename=change[
                                "paired_keyword_rename"
                            ],
                            paired_add_value=paired_add_value,
                        )
                    )
                    change["presentation_with_sidecar"] = (
                        _sync_preview_presentation(
                            change,
                            metadata,
                            assigned_location=assigned_location,
                            write_locations=write_locations,
                            location_path=location_path,
                            write_location_keywords=write_location_keywords,
                            sidecar_will_exist=True,
                            sync_flags=sync_flags,
                            paired_keyword_rename=change[
                                "paired_keyword_rename"
                            ],
                            paired_add_value=paired_add_value,
                        )
                    )
                    change["presentation"] = change[
                        "presentation_with_sidecar"
                        if sidecar_will_exist
                        else "presentation_without_sidecar"
                    ]
                    continue
                change["presentation"] = _sync_preview_presentation(
                    change,
                    metadata,
                    assigned_location=assigned_location,
                    write_locations=write_locations,
                    location_path=location_path,
                    write_location_keywords=write_location_keywords,
                    sync_flags=sync_flags,
                    paired_keyword_rename=change["paired_keyword_rename"],
                    paired_add_value=paired_add_value,
                    folder_offline=folder_offline,
                )

        page_end = offset + len(page_photos)
        has_more = page_end < total_photos
        result = {
            "photos": page_photos,
            "total_changes": total_changes,
            "total_photos": total_photos,
            "change_type_counts": change_type_counts,
            "offset": offset,
            "next_offset": page_end if has_more else None,
            "has_more": has_more,
            "revision": revision,
            "location_sync_enabled": write_locations,
            "location_keyword_sync_enabled": write_location_keywords,
        }
        # The sync dialog needs edit recipes for correctly versioned rendered
        # thumbnails, but not the species/life-list enrichment performed by
        # _attach_nested_edit_recipes.
        recipe_map = db.get_photo_edit_recipes(
            [photo["photo_id"] for photo in page_photos]
        )
        for photo in page_photos:
            photo["edit_recipe"] = recipe_map.get(photo["photo_id"])
            photo["render_key"] = render_key_for_recipe(photo["edit_recipe"])
        # A slow sidecar or network-folder read can leave the queue time to
        # change after the snapshot was validated above. Never mark the final
        # page complete from that stale snapshot: the client will restart the
        # progressive review on this existing conflict response.
        if (
            not has_more
            and _sync_preview_pending_fingerprint(db, db._ws_id())
            != snapshot["fingerprint"]
        ):
            return json_error(
                "pending changes changed while the review was loading",
                409,
                code="sync_preview_changed",
                message="Pending changes changed. Restarting the review.",
            )
        return jsonify(result)

    @blueprint.route("/api/sync/discard", methods=["POST"])
    def api_sync_discard():
        """Discard specific pending changes."""
        db = get_db()
        body = request.get_json(silent=True) or {}
        if body.get("discard_all") is True:
            revision = body.get("revision")
            if not isinstance(revision, str) or not revision:
                return json_error("revision required for discard_all")

            ws_id = db._ws_id()
            try:
                # Validate and delete under one write transaction. This keeps
                # another request from replacing or adding a pending row
                # between the revision check and the workspace-wide delete.
                db.conn.execute("BEGIN IMMEDIATE")
                snapshot = _sync_preview_get_snapshot(db, ws_id, revision)
                if snapshot["revision"] != revision:
                    db.conn.rollback()
                    return json_error(
                        "pending changes changed since they were reviewed",
                        409,
                        code="sync_preview_changed",
                        message=(
                            "Pending changes changed. Review them again before "
                            "discarding all."
                        ),
                    )
                changes = db.conn.execute(
                    "SELECT * FROM pending_changes WHERE workspace_id = ?",
                    (ws_id,),
                ).fetchall()
                db.conn.execute(
                    "DELETE FROM pending_changes WHERE workspace_id = ?",
                    (ws_id,),
                )
                db.clear_equivalent_flat_removals(changes, _commit=False)
                if changes:
                    items = _discard_history_items(db, changes)
                    db.record_edit(
                        "discard",
                        f"Discarded {len(changes)} pending changes",
                        "",
                        items,
                        is_batch=len(changes) > 1,
                        _commit=False,
                    )
                db.conn.commit()
            except Exception:
                db.conn.rollback()
                raise

            if changes:
                db._prune_edit_history()
            log.info("Discarded all %d pending changes", len(changes))
            return jsonify({"ok": True, "discarded": len(changes)})

        change_ids = body.get("change_ids", [])
        if not change_ids:
            return json_error("change_ids required")

        # Look up changes before deleting so we can record what was discarded.
        # Keep each IN clause below SQLite's bound-parameter limit, just as
        # clear_pending does for the subsequent delete.
        from db import _chunks  # noqa: PLC0415
        changes = []
        for chunk in _chunks(change_ids):
            placeholders = ",".join("?" for _ in chunk)
            changes.extend(db.conn.execute(
                f"SELECT * FROM pending_changes "
                f"WHERE id IN ({placeholders}) AND workspace_id = ?",
                list(chunk) + [db._ws_id()],
            ).fetchall())

        db.clear_pending(
            change_ids, clear_equivalent_flat_removals=True,
        )

        # Record discard in history (not undoable)
        if changes:
            items = _discard_history_items(db, changes)
            db.record_edit('discard',
                           f'Discarded {len(changes)} pending changes',
                           '', items, is_batch=len(changes) > 1)

        # Report what was actually deleted: clear_pending only removes rows
        # that exist in the active workspace, which is exactly the set the
        # SELECT above found.
        log.info("Discarded %d pending changes", len(changes))
        return jsonify({"ok": True, "discarded": len(changes)})

    return blueprint
