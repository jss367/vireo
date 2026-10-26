"""RAW+JPEG companion pairing: choosing pairs, and folding a JPEG row into its RAW.

The SQL lives in ``repositories/companion_pairing.py`` (``db.companion_pairing``).
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Any

from canonical_schema import EXIF_SUMMARY_COLUMNS
from preview_cache import (
    catalog_masks_directory,
    cleanup_cached_files_for_deleted_photos,
)
from repositories.identity_alternatives import IdentityMergeConflict
from scanner.cache_invalidation import _invalidate_derived_caches, _invalidate_raw_display_cache
from services.identity_alternatives import prepare_photo_merge

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable, Mapping
    from pathlib import Path

    from db import Database
    from repositories.photo_row_deletion import PhotoRowDeletion
    from scanner.scan_types import ColumnRow, PairingRow, PhotoIdentity, StrPath

log = logging.getLogger(__name__)


# RAW and camera JPEG of one exposure carry the same DateTimeOriginal; a
# couple of seconds absorbs sub-second/rounding differences between the two
# files' EXIF blocks without admitting a different frame.
COMPANION_TIMESTAMP_TOLERANCE_SECS = 2.0


def _companions_compatible(raw: Mapping[str, Any], jpeg: Mapping[str, Any]) -> bool:
    """True unless the two rows' capture metadata says they are different shots.

    Missing metadata on either side is not evidence against the pair (an
    unreadable RAW still pairs with its JPEG by name, as before); only a
    capture-time gap beyond the tolerance or a different camera refuses it.
    """
    raw_ts, jpeg_ts = raw.get("timestamp"), jpeg.get("timestamp")
    if raw_ts and jpeg_ts:
        try:
            gap = abs(
                (datetime.fromisoformat(str(raw_ts))
                 - datetime.fromisoformat(str(jpeg_ts))).total_seconds()
            )
        except (TypeError, ValueError):
            gap = None
        if gap is not None and gap > COMPANION_TIMESTAMP_TOLERANCE_SECS:
            return False
    for column in ("camera_make", "camera_model"):
        a, b = raw.get(column), jpeg.get(column)
        if a and b and str(a).strip().lower() != str(b).strip().lower():
            return False
    return True


_COMPANION_RAW_EXTENSIONS = frozenset(
    {".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"}
)


_COMPANION_JPEG_EXTENSIONS = frozenset({".jpg", ".jpeg"})


def _pairing_rows_for_stem(db: Database, folder_id: int, stem: str) -> list[PairingRow]:
    """The cataloged pairing candidates sharing ``stem`` in one folder.

    Same rows ``_group_photos_by_folder_and_stem`` would put in this
    group, read through the ``(folder_id, filename)`` index: every name
    whose stem is ``stem`` sorts in ``[stem + ".", stem + "/")``.
    """
    rows = db.companion_pairing.rows_for_stem(folder_id, stem)
    return [
        dict(row) for row in rows
        if os.path.splitext(row["filename"])[0] == stem
    ]


def _group_photos_by_folder_and_stem(db: Database) -> defaultdict[tuple[int, str], list[PairingRow]]:
    """Group pairing candidates by folder_id + base name (without extension).

    Candidates are every photo not yet paired, plus RAW rows that already
    carry a companion_path (so a JPEG photo named like a RAW's companion
    re-pairs with that RAW, never another same-stem one).
    """
    rows = db.companion_pairing.pairing_candidate_rows()

    # Group by folder_id + base name (without extension)
    groups = defaultdict(list)
    for row in rows:
        base = os.path.splitext(row["filename"])[0]
        groups[(row["folder_id"], base)].append(dict(row))
    return groups


def _max_raw_jpeg_matching(raws: list[PairingRow], jpegs: list[PairingRow]) -> list[tuple[PairingRow, PairingRow]]:
    """Return a maximum-cardinality set of ``(raw, jpeg)`` pairs where every
    pair satisfies ``_companions_compatible``.

    Standard augmenting-path bipartite matching: for each RAW, DFS a path
    through already-matched JPEGs to find an unmatched one; the groups
    here are a single stem's RAWs and JPEGs, so near always ≤2 of each
    and ``len(raws) * len(jpegs)`` is tiny.

    A first-fit greedy loop is wrong because a wildcard-compatible RAW
    (missing capture metadata) matches every JPEG, so taking the first
    free JPEG can leave a more constrained RAW with no option even when
    every disjoint pair would be compatible.
    """
    if not raws or not jpegs:
        return []
    match_r2j: dict[int, int] = {}

    def augment(ri: int, visited: set[int]) -> bool:
        for ji, jpeg in enumerate(jpegs):
            if ji in visited or not _companions_compatible(raws[ri], jpeg):
                continue
            visited.add(ji)
            prev = next((r for r, j in match_r2j.items() if j == ji), None)
            if prev is None or augment(prev, visited):
                match_r2j[ri] = ji
                return True
        return False

    for ri in range(len(raws)):
        augment(ri, set())
    return [(raws[ri], jpegs[ji]) for ri, ji in match_r2j.items()]


def _pick_compatible_raw_jpeg_pairs(
    members: list[PairingRow], log_conflicts: bool = True,
) -> list[tuple[PairingRow, PairingRow]]:
    """Return every ``(raw, jpeg)`` to merge from one same-stem group.

    A group needs at least one RAW and one JPEG, and each returned pair
    must not be contradicted by capture metadata (see
    ``_companions_compatible``).

    A RAW holds exactly one companion (``companion_path`` and its one
    ``companion_identities`` row), so a RAW that already has one re-pairs
    only with that same file. Trading it for another same-stem JPEG
    (``IMG.jpg`` next to ``IMG.jpeg``) would drop the first JPEG from the
    catalog: no row, no companion path, no import identity, so re-importing
    the card would bring it in again. An unpaired RAW never takes a JPEG
    another RAW already claims, and a JPEG belongs to at most one pair, so
    disjoint RAW/JPEG sets in the same stem group pair independently
    (``IMG.cr3``/``IMG.jpg`` and ``IMG.arw``/``IMG.jpeg`` both merge in one
    pass instead of the newer set being starved by the reinserted older
    companion).

    A member may be a JPEG file the scan has not cataloged yet (``id`` is
    None; see ``_CompanionGroups.resolve_companion_group``), so the scan decides
    before inserting it whether it becomes a photo or a companion.
    ``log_conflicts=False`` leaves the conflicting-metadata message to the
    end-of-scan pass, which sees the same group.
    """
    raw_exts = _COMPANION_RAW_EXTENSIONS
    jpeg_exts = _COMPANION_JPEG_EXTENSIONS

    if len(members) < 2:
        return []

    raws = [m for m in members if m["extension"] in raw_exts]
    jpegs = [m for m in members if m["extension"] in jpeg_exts]

    if not raws or not jpegs:
        return []

    # A shared stem alone does not make a pair: two bodies shooting
    # the same day with overlapping counters (or an import that
    # renamed one side of a collision) put unrelated IMG_0001.CR3 and
    # IMG_0001.JPG side by side. Pair RAW/JPEG sets whose capture
    # metadata does not contradict them; otherwise leave both as
    # separate photos.
    claimed = {raw["companion_path"] for raw in raws if raw["companion_path"]}
    jpegs_by_name = {j["filename"]: j for j in jpegs}

    pairs = []
    taken = set()

    # Each RAW with an existing companion re-pairs with that same filename
    # first (a rescan that found the companion changed offers its file
    # again), and the companion never drifts to another same-stem JPEG.
    for raw in raws:
        name = raw["companion_path"]
        if (
            name
            and name in jpegs_by_name
            and name not in taken
            and _companions_compatible(raw, jpegs_by_name[name])
        ):
            pairs.append((raw, jpegs_by_name[name]))
            taken.add(name)

    # Remaining unpaired RAWs then take disjoint, unclaimed JPEGs — so
    # ``IMG.arw``/``IMG.jpeg`` merges alongside an existing
    # ``IMG.cr3``/``IMG.jpg`` instead of being perpetually left split.
    # Maximum bipartite matching keeps a wildcard-compatible RAW (one
    # with no capture metadata, which pairs with any JPEG) from stealing
    # the one JPEG a more constrained RAW needs — a first-fit loop would
    # match ARW→IMG.jpeg first and leave CR3 unable to pair with IMG.jpg
    # even when every disjoint pair is otherwise compatible.
    unpaired_raws = [r for r in raws if r["companion_path"] is None]
    free_jpegs = [
        j for j in jpegs
        if j["filename"] not in claimed and j["filename"] not in taken
    ]
    for raw, jpeg in _max_raw_jpeg_matching(unpaired_raws, free_jpegs):
        pairs.append((raw, jpeg))
        taken.add(jpeg["filename"])

    if not pairs and log_conflicts:
        unmatched_raws = [r for r in raws if r["companion_path"] is None]
        unmatched_jpegs = [j for j in jpegs if j["filename"] not in claimed]
        if unmatched_raws and unmatched_jpegs:
            log.info(
                "Not pairing same-stem RAW and JPEG with conflicting "
                "capture metadata: %s",
                ", ".join(
                    m["filename"] for m in unmatched_raws + unmatched_jpegs
                ),
            )
    return pairs


def _read_metadata_transfer_rows(
    db: Database, primary: Mapping[str, Any], companion: Mapping[str, Any],
) -> tuple[sqlite3.Row | None, sqlite3.Row | None]:
    """Read the columns a merge may carry from companion to primary.

    Returns ``(primary_full, companion_full)``.
    """
    # The column list, and why it names the EXIF summary columns, is
    # ``repositories.companion_pairing._TRANSFER_COLUMNS``.
    primary_full = db.companion_pairing.metadata_transfer_row(primary["id"])
    companion_full = db.companion_pairing.metadata_transfer_row(companion["id"])
    return primary_full, companion_full


def _keep_companion_import_identity(db: Database, primary: Mapping[str, Any], companion: Mapping[str, Any]) -> None:
    """Attach the companion's import identity to the primary."""
    # Pairing hides the JPEG row, not the JPEG's import identity. Keep
    # it attached to the RAW so re-importing a card still skips both.
    # ``file_mtime`` lets a later scan see the companion unchanged without
    # reading it (see ``_KnownCompanions``).
    db.companion_pairing.keep_import_identity(primary["id"], companion["id"])


def _fill_primary_metadata_gaps(
    db: Database, primary: Mapping[str, Any], primary_full: ColumnRow, companion_full: ColumnRow,
) -> None:
    """Copy companion metadata onto the primary only where the primary lacks it."""
    updates = []
    params = []
    if not primary_full["timestamp"] and companion_full["timestamp"]:
        updates.append("timestamp = ?")
        params.append(companion_full["timestamp"])
    if primary_full["rating"] == 0 and companion_full["rating"] != 0:
        updates.append("rating = ?")
        params.append(companion_full["rating"])
    if (
        primary_full["flag"] == "none"
        and companion_full["flag"] not in ("none", "rejected")
    ):
        # Never copy 'rejected': the duplicate auto-resolver runs earlier
        # in the same scan and rejects companion JPEGs that lose to a
        # byte-identical twin elsewhere — stamping that onto the RAW
        # would silently hide a unique photo.
        updates.append("flag = ?")
        params.append(companion_full["flag"])
    if primary_full["latitude"] is None and companion_full["latitude"] is not None:
        updates.append("latitude = ?")
        params.append(companion_full["latitude"])
    if primary_full["longitude"] is None and companion_full["longitude"] is not None:
        updates.append("longitude = ?")
        params.append(companion_full["longitude"])
    if not primary_full["exif_data"] and companion_full["exif_data"]:
        updates.append("exif_data = ?")
        params.append(companion_full["exif_data"])
    # Fill any promoted EXIF summary column the RAW row is missing but
    # its JPEG companion has. ``EXIF_SUMMARY_COLUMNS`` already contains
    # ``focal_length`` alongside the camera/exposure fields, so a single
    # loop covers all promoted columns — a separate ``focal_length``
    # transfer would fire twice and SQLite would reject the UPDATE for
    # assigning the same column twice. Only writes when the primary is
    # NULL — a non-NULL primary already reflects a rescan (which clears
    # absent columns to NULL via ``EXIF_SUMMARY_COLUMNS``), so
    # overwriting it would trample fresh metadata.
    for column in EXIF_SUMMARY_COLUMNS:
        if primary_full[column] is None and companion_full[column] is not None:
            updates.append(f"{column} = ?")
            params.append(companion_full[column])
    if not primary_full["width"] and companion_full["width"]:
        updates.append("width = ?")
        params.append(companion_full["width"])
    if not primary_full["height"] and companion_full["height"]:
        updates.append("height = ?")
        params.append(companion_full["height"])
    if updates:
        db.scanning.update_photo(primary["id"], updates, params)


def _transfer_companion_keywords(db: Database, primary: Mapping[str, Any], companion: Mapping[str, Any]) -> None:
    """Transfer keywords from companion to primary, keeping the stronger provenance."""
    db.keyword_provenance.carry_companion_keywords(companion["id"], primary["id"])


def _defer_primary_display_cache_invalidation(
    post_commit_fs_actions: list[Callable[[], None]], primary: Mapping[str, Any],
    vireo_dir: StrPath, thumb_cache_dir: StrPath | None,
) -> None:
    """Queue removal of the primary's RAW display cache and JPEG thumbnail.

    An unedited RAW display cache may have been rendered before the
    camera JPEG was paired. File mtimes cannot tell which source
    produced that cache, so discard it when the companion changes.
    Deferred to post-commit: if this pair's DB changes roll back
    (because a later iteration or the final commit fails), the
    primary's companion_path stays NULL and there's no reason to
    have invalidated its display cache — the RAW render is still
    valid.
    """
    _primary_id = primary["id"]
    _thumb_dir = thumb_cache_dir or os.path.join(
        vireo_dir, "thumbnails",
    )
    _vireo_dir = vireo_dir

    def _invalidate_primary_variants(
        pid: int = _primary_id, td: StrPath = _thumb_dir, vd: StrPath = _vireo_dir,
    ) -> None:
        _invalidate_raw_display_cache(vd, pid)
        jpeg_variant = os.path.join(td, f"{pid}_jpeg.jpg")
        try:
            if os.path.exists(jpeg_variant):
                os.remove(jpeg_variant)
        except OSError:
            log.debug(
                "Could not delete stale companion thumbnail %s",
                jpeg_variant,
                exc_info=True,
            )

    post_commit_fs_actions.append(_invalidate_primary_variants)


def _carry_prediction_reviews_to_survivors(db: Database, old_id: int, new_id: int) -> None:
    """Move review state off predictions that collided during a re-key."""
    # If a companion prediction collided with an existing primary
    # prediction, its row stayed on the old detection id. Preserve any
    # workspace review state by moving it onto the surviving prediction
    # before the old detection delete cascades the duplicate away.
    db.companion_pairing.carry_prediction_reviews_to_survivors(old_id, new_id)


def _rekey_companion_detections_onto_primary(
    db: Database, primary: Mapping[str, Any], companion: Mapping[str, Any],
) -> None:
    """Transfer detections (and their cascaded predictions) from companion to primary.

    Detection IDs are content-addressed on (photo_id, detector_model, box,
    category) — see vireo/detection_id.py. A bare `UPDATE photo_id` would
    leave the row's `id` column stale (still hashed against the companion's
    photo_id); a later detector run on the primary that produced the same
    box would then either collide on the stale id (if SQLite reused the
    companion's rowid for a new photo) or, more commonly, never match and
    so the stale row would be reaped by the stale-cleanup DELETE in
    `db.detections.upsert_rows`, cascading away its predictions. Recompute
    the id, redirect predictions to the new id, then drop the stale row.
    """
    from detection_id import detection_id as _detection_id

    pairing = db.companion_pairing
    moving = pairing.detections_of(companion["id"])
    for det in moving:
        new_id = _detection_id(
            primary["id"], det["detector_model"],
            (det["box_x"], det["box_y"], det["box_w"], det["box_h"]),
            det["category"],
        )
        if new_id == det["id"]:
            # Cannot happen in practice (photo_id changed) but defensive.
            pairing.move_detection(det["id"], primary["id"])
            continue
        # Insert the row under the new id. If the primary already has a
        # detection for the same (model, box, category), the UPSERT no-ops
        # and we just discard the companion's row below.
        pairing.insert_rekeyed_detection(new_id, primary["id"], det)
        # Redirect predictions to the new detection id. ON CONFLICT in
        # predictions is unlikely (would require the primary already had
        # a prediction for this species against the same detection) but
        # IGNORE keeps us defensive.
        pairing.redirect_predictions(det["id"], new_id)
        _carry_prediction_reviews_to_survivors(db, det["id"], new_id)
        # Redirect classifier_runs too — they're the cache key the
        # non-reclassify gate consults. Without this, paired photos with
        # cached predictions would look unclassified to
        # `get_classifier_run_keys(new_id)` and rerun the classifier
        # after every pair-up. Same OR IGNORE pattern as predictions:
        # primary's own run rows win on (detection_id, classifier_model,
        # labels_fingerprint) conflicts.
        pairing.redirect_classifier_runs(det["id"], new_id)
        # Drop any predictions/classifier_runs that lost the UPDATE race
        # (duplicate-key) along with the now-orphan companion detection
        # row — CASCADE on both FKs cleans up any remaining rows tied to
        # the old id.
        pairing.delete_detection(det["id"])


def _transfer_companion_pending_changes(db: Database, primary: Mapping[str, Any], companion: Mapping[str, Any]) -> None:
    """Transfer pending_changes from companion to primary.

    No dedup needed here (unlike ``_transfer_companion_inat_submissions``):
    pending_changes has no UNIQUE constraint that would crash on
    collision, and duplicate rows from a raw+JPEG pairing are harmless and
    vanishingly unlikely.
    """
    db.companion_pairing.transfer_pending_changes(primary["id"], companion["id"])


def _transfer_companion_inat_submissions(
    db: Database, primary: Mapping[str, Any], companion: Mapping[str, Any],
) -> None:
    """Transfer iNaturalist submissions from companion to primary.

    Deduplicate on (photo_id, observation_id) before reassigning to avoid
    UNIQUE constraint violation.
    """
    db.companion_pairing.transfer_inat_submissions(primary["id"], companion["id"])


def _transfer_companion_edit_recipe(
    db: Database, primary: Mapping[str, Any], companion: Mapping[str, Any],
    vireo_dir: StrPath | None, thumb_cache_dir: StrPath | None,
    post_commit_fs_actions: list[Callable[[], None]],
) -> None:
    """Preserve non-destructive edits when a JPEG companion is folded into a RAW primary.

    If both already have recipes, the primary wins. When the companion's
    recipe is transferred, its edit-history items follow it. Referenced masks
    are copied before commit while originals remain usable on rollback.
    Derived-render invalidation and old-snapshot cleanup wait for commit.
    """
    if db.companion_pairing.copy_edit_recipe(primary["id"], companion["id"]):
        from local_masks import prepare_snapshot_transfer

        refs = prepare_snapshot_transfer(
            vireo_dir, companion["id"], primary["id"],
            db.companion_pairing.recipe_snapshot_values(companion["id"]),
        )
        db.companion_pairing.transfer_recipe_history(primary["id"], companion["id"])
        if vireo_dir:
            _defer_recipe_snapshot_cleanup(
                db, post_commit_fs_actions, primary, companion,
                vireo_dir, thumb_cache_dir, refs,
            )
        else:
            db.caches.clear_thumbnail_path(primary["id"])
            db.caches.delete_photo_previews(primary["id"])


def _defer_recipe_snapshot_cleanup(
    db: Database, post_commit_fs_actions: list[Callable[[], None]], primary: Mapping[str, Any],
    companion: Mapping[str, Any], vireo_dir: StrPath, thumb_cache_dir: StrPath | None,
    refs: set[str],
) -> None:
    """Queue old-snapshot cleanup and render invalidation after commit.

    Referenced snapshots have already been copied to the primary. Removing
    old copies is best-effort cleanup; a failure cannot disable the
    transferred recipe. Originals must stay in place until commit so a
    rollback leaves the companion's recipe usable.
    """
    _primary_id = primary["id"]
    _companion_id = companion["id"]
    _tcd = thumb_cache_dir
    _vd = vireo_dir

    def _apply_recipe_transfer_fs(
        pid: int = _primary_id, cid: int = _companion_id, tcd: StrPath | None = _tcd, vd: StrPath = _vd,
    ) -> None:
        from local_masks import cleanup_snapshot_transfer

        # A concurrent insert can reuse the deleted companion's id after
        # commit. Keep its new owner's snapshots and hold the writer lock
        # from this ownership check through cleanup.
        if not db.in_transaction:
            db.begin_immediate()
        if db.photos.get(cid) is None:
            cleanup_snapshot_transfer(vd, cid, pid, refs)
        _invalidate_derived_caches(
            db, vd, pid, thumb_cache_dir=tcd,
        )

    post_commit_fs_actions.append(_apply_recipe_transfer_fs)


def _delete_companion_row(
    db: Database, primary: Mapping[str, Any], companion: Mapping[str, Any],
    photo_rows: PhotoRowDeletion,
) -> None:
    """Move the companion's remaining per-photo state to the primary, then delete its row.

    ``photo_rows`` is the caller's ``PhotoRowDeletion``; the companion's
    collection entries move to the primary when the caller's block exits.
    """
    # Remove keyword associations then the duplicate JPEG record
    db.moves_merge.transfer_gps_review(companion["id"], primary["id"])
    db.companion_pairing.delete_keyword_associations(companion["id"])
    # Move the embedded-offered suppression onto the survivor so a
    # user removal survives this pairing. The FK on
    # ``photo_embedded_keyword_offered.photo_id`` is non-cascading, so
    # a leftover row would also block the ``DELETE FROM photos`` below.
    db.photo_keywords.transfer_embedded_offered_keys(
        companion["id"], primary["id"],
    )
    # Transfer effective visibility before cascade deletion removes the
    # companion's photo-only grants. This shares only the surviving ID.
    db.photo_visibility.remap({companion["id"]: primary["id"]})
    photo_rows.delete({companion["id"]: primary["id"]})


def _defer_companion_derivative_cleanup(
    post_commit_fs_actions: list[Callable[[], None]], companion: Mapping[str, Any],
    vireo_dir: StrPath, thumb_cache_dir: StrPath | None, masks_dir: str | None = None,
) -> None:
    """Queue unlinking the deleted companion's cached derivatives.

    The companion's rowid is now free for SQLite to hand to the next
    insert. Its derivatives must be unlinked so the next photo to
    inherit the id can't adopt the companion's thumbnail / working
    copy / masks (see ``purge_cached_files_for_recycled_id``).
    Deferred to post-commit: unlinking inline and then losing this
    pair's DELETE to a later rollback would leave a restored
    companion row pointing at gone-from-disk derivatives, and none
    of ``purge_cached_files_for_recycled_id``'s protections would
    help — the rowid is still occupied by that restored row, not
    available for reuse.
    """
    _companion_id = companion["id"]
    _thumb_dir_for_companion = thumb_cache_dir or os.path.join(
        vireo_dir, "thumbnails",
    )

    def _cleanup_companion(
        cid: int = _companion_id, td: StrPath = _thumb_dir_for_companion,
        vd: StrPath = vireo_dir, md: str | None = masks_dir,
    ) -> None:
        cleanup_cached_files_for_deleted_photos(
            td, [{"photo_id": cid}], vireo_dir=vd, masks_dir=md,
        )

    post_commit_fs_actions.append(_cleanup_companion)


def _merge_companion_into_primary(
    db: Database, primary: PairingRow, companion: PairingRow, vireo_dir: StrPath | None,
    thumb_cache_dir: StrPath | None, post_commit_fs_actions: list[Callable[[], None]],
    photo_rows: PhotoRowDeletion,
) -> bool:
    """Fold one JPEG companion row into its RAW primary inside the open transaction.

    Every DB write joins the caller's transaction. Referenced snapshots are
    copied without removing originals; destructive filesystem changes wait
    in ``post_commit_fs_actions`` until commit succeeds. The companion row
    is deleted through the caller's ``PhotoRowDeletion`` (``photo_rows``).
    """
    primary_full, companion_full = _read_metadata_transfer_rows(
        db, primary, companion,
    )
    # Candidates predate these reads; a concurrent deletion or merge can
    # remove either row. Skip before writes or deferred filesystem actions.
    if primary_full is None or companion_full is None:
        return False
    prepare_photo_merge(db, companion["id"], primary["id"])
    _keep_companion_import_identity(db, primary, companion)
    _fill_primary_metadata_gaps(
        db, primary, primary_full, companion_full,
    )
    _transfer_companion_keywords(db, primary, companion)

    db.companion_pairing.set_companion_path(primary["id"], companion["filename"])
    if vireo_dir:
        _defer_primary_display_cache_invalidation(
            post_commit_fs_actions, primary, vireo_dir, thumb_cache_dir,
        )

    _rekey_companion_detections_onto_primary(db, primary, companion)
    _transfer_companion_pending_changes(db, primary, companion)
    _transfer_companion_inat_submissions(db, primary, companion)
    _transfer_companion_edit_recipe(
        db, primary, companion, vireo_dir, thumb_cache_dir,
        post_commit_fs_actions,
    )
    _delete_companion_row(db, primary, companion, photo_rows)
    return True


def _run_post_commit_fs_actions(post_commit_fs_actions: Iterable[Callable[[], object]]) -> None:
    """Run the deferred filesystem changes once the pairing commit is durable.

    Any exception here is per-action so a single failing unlink doesn't
    skip the rest, and the recycled-id purge on the next insert would
    eventually recover anything we missed.
    """
    for action in post_commit_fs_actions:
        try:
            action()
        except Exception:  # broad: post-commit file cleanup is best-effort; the pairing already committed
            log.exception(
                "Deferred filesystem cleanup after raw+JPEG pairing "
                "failed; may leave stale derivative files that the "
                "recycled-id purge or Clear Cache will reclaim later",
            )


class _MergedPhotoIds(dict[int, int]):
    """Merged ids plus the identities (companion AND surviving RAW)
    that pairing captured at merge time, so publication can refuse to
    substitute when either id's current row is not the one pairing saw.
    """

    def __init__(self) -> None:
        super().__init__()
        # ``primary_id -> (folder_id, filename)`` of each surviving RAW.
        self.owners: dict[int, PhotoIdentity] = {}
        # ``companion_id -> (folder_id, filename)`` as pairing saw the
        # JPEG row before deleting it. The scanner compares this against
        # the identity it originally reported for that id: if the two
        # disagree, the id was reused after being reported and must not
        # be substituted with the pairing's RAW id (the two name
        # unrelated files).
        self.companions: dict[int, PhotoIdentity] = {}


def _try_automatic_companion_merge(
    db: Database, primary: PairingRow, companion: PairingRow, vireo_dir: StrPath | None,
    thumb_cache_dir: StrPath | None, post_commit_fs_actions: list[Callable[[], None]],
    photo_rows: PhotoRowDeletion,
) -> bool:
    """Keep a domain-conflicting pair separate without stopping unrelated work."""
    from local_masks import SnapshotTransferError

    pair_actions: list[Callable[[], None]] = []
    try:
        with photo_rows.atomic_merge(companion['id']):
            paired = _merge_companion_into_primary(
                db, primary, companion, vireo_dir, thumb_cache_dir, pair_actions, photo_rows,
            )
    except IdentityMergeConflict as exc:
        log.warning('Keeping RAW photo %s and JPEG photo %s separate: %s', primary['id'], companion['id'], exc)
        return False
    except SnapshotTransferError:
        log.warning(
            "Keeping RAW photo %s and JPEG photo %s separate because their edit masks "
            "could not be preserved; a later scan will retry pairing",
            primary['id'], companion['id'], exc_info=True,
        )
        return False
    if paired:
        post_commit_fs_actions.extend(pair_actions)
    return paired


def _pair_raw_jpeg_companions(
    db: Database, vireo_dir: StrPath | None = None, thumb_cache_dir: StrPath | None = None,
) -> _MergedPhotoIds:
    """Find raw+JPEG pairs in the same folder and merge them.

    When both IMG_001.cr3 and IMG_001.jpg exist in the same folder,
    keep the raw as the primary photo and set companion_path to the JPEG filename.
    Delete the duplicate JPEG-only photo record.

    Successful pairs share one transaction; conflicting identities or mask-copy
    failures leave that pair separate with a warning. Other errors abort the
    batch. Snapshot copies precede commit; destructive cleanup follows it.

    Returns ``{companion_id: primary_id}`` for the JPEG rows merged away.
    Callers that count indexed photos must discount these: both files were
    counted on the way in, but only the RAW row survives, so the scan would
    otherwise claim two photos where the catalog holds one. Callers that
    handed a merged id to someone (``scan``'s ``photo_callback``) must tell
    them which row absorbed it: the id no longer names a photo, and SQLite
    can give it to the next insert.

    Ids, not a bare count, because this query covers the *whole* photos
    table: it also merges pairs left pending anywhere else in the catalog
    (an interrupted earlier scan, an older build). A caller must intersect
    with the ids it actually counted, or a scoped scan would be debited
    for merges it had nothing to do with and could report a negative
    total.

    A scan pairs a JPEG file it has not cataloged yet before inserting it
    (``_CompanionGroups.resolve_companion_group``), so what reaches this pass is
    a JPEG that is already a photo of its own: one cataloged before its
    RAW arrived, or a pair left pending.
    """
    groups = _group_photos_by_folder_and_stem(db)

    # Filesystem changes are collected here and executed only after
    # commit_with_retry succeeds. All pairs share one transaction, so a
    # failure on any later iteration (or on the commit itself) rolls back
    # every companion row that was DELETEd earlier — but any files we'd
    # already unlinked inline are gone for good, leaving the restored
    # companion rows pointing at missing thumbnails, working copies,
    # masks, offline originals, and moved mask snapshots. Deferring gives
    # us "all DB changes durable, then destructive FS changes". Referenced
    # mask copies are prepared before commit without removing originals.
    post_commit_fs_actions = []
    # Companion rows merged away (``companion_id -> primary_id``), reported
    # to the caller so it can correct its indexed count and the ids it
    # handed out. Collected at the DELETE but only returned after the
    # commit below succeeds — the whole loop shares one transaction, so a
    # commit failure rolls every deletion back and none of them happened.
    merged_ids = _MergedPhotoIds()

    # The deletion chokepoint batches collection rewrites for all pairs;
    # retain each RAW identity for ownership-checked callback publication.
    with db.companion_pairing.row_deletion() as photo_rows:
        for (_folder_id, _base), members in groups.items():
            for primary, companion in _pick_compatible_raw_jpeg_pairs(members):
                if not _try_automatic_companion_merge(
                    db, primary, companion, vireo_dir, thumb_cache_dir,
                    post_commit_fs_actions, photo_rows,
                ):
                    continue
                merged_ids[companion["id"]] = primary["id"]
                merged_ids.owners[primary["id"]] = (
                    primary["folder_id"], primary["filename"],
                )
                merged_ids.companions[companion["id"]] = (
                    companion["folder_id"], companion["filename"],
                )
                if vireo_dir:
                    _defer_companion_derivative_cleanup(
                        post_commit_fs_actions, companion, vireo_dir,
                        thumb_cache_dir, masks_dir=catalog_masks_directory(db._db_path),
                    )

    db.commit_with_retry()
    # DB state is durable now. Run the collected filesystem operations.
    _run_post_commit_fs_actions(post_commit_fs_actions)
    # ``_invalidate_derived_caches`` inside a deferred action issues DB
    # updates (clears thumb_path / working_copy_path, drops preview_cache
    # rows). Those auto-opened a new transaction; commit it so the state
    # is durable and doesn't leak into the caller's next write.
    if db.in_transaction:
        db.commit_with_retry()
    return merged_ids


class _KnownCompanions:
    """JPEGs the catalog already holds as some RAW's companion.

    A paired JPEG has no ``photos`` row of its own: it is the RAW row's
    ``companion_path`` plus a ``companion_identities`` row. Without this
    lookup a rescan found no row for the JPEG, inserted it as a new photo,
    and the pairing pass merged it straight back into the RAW (re-keying
    its state, deleting the row, discarding the RAW's companion-derived
    display caches), once per companion per rescan.

    ``lookup`` matches the RAW row by ``folder_id`` and ``companion_path``,
    the same keys pairing writes. Each folder's companion names are read
    once per scan and only filter; a hit is re-read from the catalog, so a
    pairing or deletion that ran since the snapshot is honored.
    """

    def __init__(self, db: Database) -> None:
        self.db = db
        self._names_by_folder = {}
        self._folder_ids_by_path = {}

    def lookup(self, folder_id: int, filename: str) -> sqlite3.Row | None:
        """The owning RAW and its stored companion identity, or None."""
        names = self._names_by_folder.get(folder_id)
        if names is None:
            names = self.db.companion_pairing.companion_names(folder_id)
            self._names_by_folder[folder_id] = names
        if filename not in names:
            return None
        return self.db.companion_pairing.owner_of(folder_id, filename)

    def lookup_path(self, image_path: Path) -> sqlite3.Row | None:
        """``lookup`` by path, resolving the folder like ``_incremental_photo_index``."""
        parent = str(image_path.parent)
        folder_ids = self._folder_ids_by_path.get(parent)
        if folder_ids is None:
            folder_ids = self.db.scanning.folder_ids_at(parent)
            self._folder_ids_by_path[parent] = folder_ids
        for folder_id in folder_ids:
            known = self.lookup(folder_id, image_path.name)
            if known is not None:
                return known
        return None


def _companion_stat_unchanged(known: sqlite3.Row, stat: os.stat_result) -> bool:
    """True when size and mtime match the identity a hash vouched for."""
    return (
        not known["needs_sync"]
        and known["file_mtime"] is not None
        and known["file_mtime"] == stat.st_mtime
        and known["file_size"] == stat.st_size
        and (known["file_hash"] is not None or stat.st_size == 0)
    )


def _companion_bytes_unchanged(known: sqlite3.Row, file_size: int, file_hash: str | None) -> bool:
    """True when freshly hashed bytes are the ones the identity stores."""
    if known["needs_sync"]:
        # Recovery verifies bytes for duplicate preview, but has not
        # imported metadata/keywords or invalidated companion caches.
        # Re-pair it once (``_ScanRun._attach_companion``) to synchronize
        # them.
        return False
    if known["file_size"] is None or known["file_size"] != file_size:
        return False
    if file_size == 0:
        return True
    return file_hash is not None and file_hash == known["file_hash"]
