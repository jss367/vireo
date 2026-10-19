"""Importing sidecar and embedded keywords onto photos, and the embedded backfill."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from db import KEYWORD_SOURCE_UNKNOWN
from keyword_identity import (
    drop_stale_vireo_location_keywords,
    filter_removed_import_aliases,
    validate_import_locations,
)
from keyword_normalization import keyword_match_key
from metadata import embedded_keywords
from xmp import read_keyword_snapshot

if TYPE_CHECKING:
    from collections.abc import Collection

    from db import Database
    from scanner.scan_types import ExifMetadata

log = logging.getLogger(__name__)


def _import_keywords_for_photo(db: Database, photo_id: int, xmp_path_str: str) -> bool:
    """Import readable XMP keywords; False leaves the sidecar retryable."""
    snapshot = read_keyword_snapshot(xmp_path_str)
    if snapshot is None:
        return False
    flat_keywords, hier_keywords = snapshot[:2]
    # A sidecar whose location keywords Vireo wrote describes the place the
    # photo had at the last sync. If a newer place (or none) is already queued
    # in Vireo, importing those entries would re-attach the old one.
    flat_keywords, hier_keywords = drop_stale_vireo_location_keywords(
        db, photo_id, xmp_path_str, flat_keywords, hier_keywords,
        location_metadata=snapshot[2:],
    )
    _import_keyword_lists(db, photo_id, flat_keywords, hier_keywords)
    return True


def _import_embedded_keywords_for_photo(db: Database, photo_id: int, file_meta: ExifMetadata | None, *,
                                        raise_on_conflict: bool = False) -> bool:
    """Import the keywords stored inside the image file (see ``embedded_keywords``).

    Additive, like the sidecar import, and run alongside it: a file can carry
    both. Vireo never writes into image files, so none of these entries can be
    a stale Vireo-written location. Returns whether anything was offered for
    import.

    A conflict with the photo's linked place skips this photo's embedded
    keywords instead of failing the scan: the sidecar import raises there, but
    the embedded copy is a second, older source, and one stale JPEG must not
    leave every folder in scope half-scanned.
    Backfill uses ``raise_on_conflict`` to keep skipped work retryable.

    Durable suppression: the pending-removal filter in ``_import_keyword_lists``
    only hides a value until the next XMP sync clears the queue entry, but
    Vireo cannot remove the value from the image itself, so an image rewrite
    or full rescan would otherwise re-tag the photo with a keyword the user
    had already deleted. Every embedded entry offered to the photo is
    recorded in ``photo_embedded_keyword_offered``; a key already in that
    record is filtered out before the import -- a user removal stands, and
    only genuinely new embedded entries (added by a later Lightroom export)
    reach the tag writer. Flat entries record their normalized leaf key;
    hierarchies record the full normalized pipe-joined path (``birds|robin``,
    not ``robin``) so a genuinely new hierarchy such as ``People|Robin``
    isn't filtered out just because an earlier scan offered ``Birds|Robin``
    -- ``_import_keyword_lists`` creates distinct keyword rows for those
    paths (their parent IDs differ), so they are distinct tags.
    """
    flat_keywords, hier_keywords = embedded_keywords(file_meta or {})
    if not flat_keywords and not hier_keywords:
        return False

    offered = db.keywords.embedded_offered_keys(photo_id)
    # Candidate suppression keys: flat entries map to their own normalized
    # leaf key; a hierarchy maps to its full normalized pipe-joined path so
    # distinct hierarchies sharing a leaf (``Birds|Robin`` vs
    # ``People|Robin``) record separately and don't collide.
    #
    # A malformed hierarchy whose ancestor normalizes to empty (e.g.
    # ``"Birds||Hawk"``) is skipped by ``_import_keyword_lists`` and so must
    # not be recorded as offered either; otherwise, once Lightroom rewrites
    # the entry to ``"Birds|Hawk"``, the path would already be in the
    # suppression set and every rescan would filter it out before the tag
    # writer ever saw it.
    def _hier_path(parts: list[str]) -> str | None:
        if not parts or any(not keyword_match_key(part) for part in parts):
            return None
        return "|".join(keyword_match_key(part) for part in parts)

    candidate_keys = {
        key for name in flat_keywords
        if (key := keyword_match_key(name))
    }
    for hier in hier_keywords:
        if (path := _hier_path(hier.split("|"))) is not None:
            candidate_keys.add(path)

    fresh_flat = [
        name for name in flat_keywords
        if keyword_match_key(name) not in offered
    ]
    fresh_hier = [
        hier for hier in hier_keywords
        if (path := _hier_path(hier.split("|"))) is not None
        and path not in offered
    ]

    if fresh_flat or fresh_hier:
        try:
            _import_keyword_lists(db, photo_id, fresh_flat, fresh_hier)
        except ValueError as exc:
            log.warning(
                "Skipped keywords embedded in photo %s: %s", photo_id, exc,
            )
            if raise_on_conflict:
                raise
            # Deliberately do not record the suppression on ValueError: the
            # import never ran, so the user's view of these keys has not
            # changed. A follow-up scan after the conflict is resolved
            # (user clears or picks a compatible place) should still get
            # the chance to tag them.
            return False
    # Record every candidate seen, including the fresh ones that imported
    # just now: the goal is to make the next scan's filter think of them
    # as "already offered" so a later removal is not silently undone.
    new_to_record = candidate_keys - offered
    if new_to_record:
        db.keywords.record_embedded_offered_keys(photo_id, new_to_record)
    return bool(fresh_flat or fresh_hier)


def _import_keyword_lists(
    db: Database, photo_id: int, flat_keywords: Collection[str], hier_keywords: Collection[str],
) -> None:
    """Tag a photo with imported flat and hierarchical keywords (additive)."""
    pending_flat_removals = db.pending_changes.keyword_removal_keys(photo_id)
    pending_hierarchical_removals = db.pending_changes.keyword_removal_keys(
        photo_id, hierarchical=True,
    )
    flat_keywords, hier_keywords = filter_removed_import_aliases(
        db, photo_id, flat_keywords, hier_keywords, pending_flat_removals, pending_hierarchical_removals,
    )
    validate_import_locations(
        db, photo_id,
        [kw for kw in flat_keywords if keyword_match_key(kw) not in pending_flat_removals],
        [hier for hier in hier_keywords
         if not any(keyword_match_key(part) in pending_hierarchical_removals for part in hier.split('|'))],
    )

    # Build hierarchy from lr:hierarchicalSubject
    # e.g., 'Birds|Raptors|Black kite' creates Birds -> Raptors -> Black kite
    # Skip a hierarchical entry whose chain contains any segment that
    # normalizes to `""` (e.g. `"|Birds"` or `"Birds|'|Hawk"`). add_keyword()
    # raises ValueError on those, and letting it propagate would abort the
    # whole scan on a malformed sidecar entry instead of ignoring it.
    hierarchy_leaf_keys = set()
    for hier in hier_keywords:
        parts = hier.split("|")
        if any(not keyword_match_key(part) for part in parts):
            continue
        if any(
            keyword_match_key(part) in pending_hierarchical_removals
            for part in parts
        ):
            continue
        parent_id = None
        for index, part in enumerate(parts):
            kid = db.add_keyword(part, parent_id=parent_id, _resolve_alias=index == len(parts) - 1)
            parent_id = kid
        # Tag with the leaf keyword. A sidecar term is genuinely ambiguous —
        # the user may have typed it in Lightroom, or Vireo may have written
        # it out itself — so this is one of the few writers that declines to
        # claim manual authorship.
        db.tag_photo(photo_id, parent_id, source=KEYWORD_SOURCE_UNKNOWN)
        hierarchy_leaf_keys.add(keyword_match_key(parts[-1]))

    # Also add any flat keywords not already covered by hierarchy. Compare
    # via the normalized match key on both sides: DB names are stored in
    # their cleaned form (add_keyword normalizes on insert), so a raw
    # `dc:subject` value like `‘apapane` that matches an existing clean
    # `apapane` on this photo would otherwise fall through to add_keyword
    # and get tagged again as a redundant top-level row. Same empty-key
    # filter as above — a lone smart-quote entry would raise inside
    # add_keyword() and abort the scan.
    existing_keys = {
        keyword_match_key(k["name"]) for k in db.keywords.get_for_photo(photo_id)
    } | hierarchy_leaf_keys
    for kw in flat_keywords:
        key = keyword_match_key(kw)
        if not key:
            continue
        # A sidecar write can be intentionally deferred in the sync panel.
        # Do not resurrect the DB association from the still-stale sidecar
        # while its removal is pending.
        if key in pending_flat_removals:
            continue
        if key in existing_keys:
            continue
        kid = db.add_keyword(kw, _resolve_alias=True)
        # Same ambiguity as the hierarchical branch above.
        db.tag_photo(photo_id, kid, source=KEYWORD_SOURCE_UNKNOWN)
        existing_keys.add(key)


EMBEDDED_KEYWORDS_BACKFILL_KEY = "embedded_keywords_backfill_v1"


def backfill_embedded_keywords(db: Database) -> int:
    """One-shot import of embedded keywords for photos scanned before scans read them.

    Scans only read keywords from ``.xmp`` sidecars until
    ``_import_embedded_keywords_for_photo`` existed, and an unchanged file is
    never re-read, so the keywords Lightroom wrote into JPEGs and DNGs stayed
    out of the catalog. The scan stored ExifTool's full output in
    ``exif_data``, so this pass imports them from there without touching the
    originals. Completion is ``db_meta``-gated; conflicts leave the pass
    retryable, while durable offered keys protect prior imports and removals
    on that retry. Returns the number of photos that gained a keyword.
    """
    if db.meta.get(EMBEDDED_KEYWORDS_BACKFILL_KEY) == "1":
        return 0
    rows = db.scanning.embedded_keyword_rows()
    imported = 0
    complete = True
    for row in rows:
        try:
            file_meta = json.loads(row["exif_data"])
        except (TypeError, ValueError):
            continue
        if not isinstance(file_meta, dict):
            continue
        before = _photo_keyword_count(db, row["id"])
        try:
            offered = _import_embedded_keywords_for_photo(
                db, row["id"], file_meta, raise_on_conflict=True,
            )
        except ValueError:
            complete = False
            continue
        if offered and _photo_keyword_count(db, row["id"]) > before:
            imported += 1
    if complete:
        db.meta.set(EMBEDDED_KEYWORDS_BACKFILL_KEY, "1")
    return imported


def _photo_keyword_count(db: Database, photo_id: int) -> int:
    return db.scanning.keyword_count(photo_id)
