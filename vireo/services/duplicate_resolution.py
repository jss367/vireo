"""Exact-duplicate resolution: the auto-resolver and the user's overrides.

``check_and_resolve_duplicates_for_hash`` is the best-effort hook that runs
after a photo gains a ``file_hash`` (``photo_ingest.add_photo`` calls it, as do
scanner paths that set the hash later); ``apply_duplicate_resolution`` picks a
winner with ``duplicates.resolve_duplicates``; ``keep_duplicate_copy`` and
``bulk_resolve_by_folder`` apply the user's choice. Each plans through
``db.duplicates`` and applies the plan with ``Database._apply_winner_loser_merge``,
which stays on ``Database``: ``test_keyword_provenance_contract`` pins that
provenance-carrying writer (it re-tags the winner through ``tag_photo``) to
``db.py``.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Iterable

    from db import Database
    from services.service_types import JsonDict

log = logging.getLogger(__name__)


def check_and_resolve_duplicates_for_hash(db: Database, file_hash: str | None) -> JsonDict | None:
    """Look up non-rejected photos sharing this hash; if >=2, resolve.

    Returns the result dict from apply_duplicate_resolution when resolution
    ran, or None when no resolution was needed. Failures are logged and
    swallowed — this is a best-effort hook, not a correctness guarantee.
    """
    if not file_hash:
        return None
    try:
        dup_ids = db.duplicates.live_ids_for_hash(file_hash)
        if len(dup_ids) > 1:
            return apply_duplicate_resolution(db, dup_ids)
    except sqlite3.Error as e:
        log.warning(
            "Duplicate auto-resolve failed for hash %s: %s", file_hash, e,
        )
    return None


def apply_duplicate_resolution(db: Database, photo_ids: Iterable[int]) -> JsonDict:
    """Resolve a group of photos sharing a file_hash.

    Picks a winner using :func:`vireo.duplicates.resolve_duplicates`,
    merges metadata (rating/keywords) from losers onto the winner, and
    flags the losers as rejected. Runs in a single transaction.

    Photos whose ``flag`` is already ``'rejected'`` are filtered out
    before resolving — we never un-reject previously handled losers.

    Returns ``{"winner_id": int|None, "loser_ids": [int], "rejected": int}``.
    If fewer than 2 non-rejected candidates remain, returns the no-op
    shape with ``winner_id=None``. When at least one candidate lives
    on an offline volume, the resolver defers instead of picking a
    winner: the return dict adds ``"deferred": True`` so callers can
    tell deferrals apart from no-ops and keep the group visible.
    """
    from repositories.duplicates import DEFERRED_PLAN

    repo = db.duplicates
    # This entry point owns its writer transaction. An outer transaction
    # cannot safely run disk probes or be committed by the merge helper.
    deferred = {"winner_id": None, "loser_ids": [], "rejected": 0, "deferred": True}
    if db.in_transaction:
        return deferred
    ids = list(dict.fromkeys(photo_ids))
    preflight = repo.resolution_preflight(ids)
    db.begin_immediate()
    with db.transaction():
        plan = repo.resolution_plan(ids, preflight=preflight)
        if plan is None:
            db.rollback()
            return {"winner_id": None, "loser_ids": [], "rejected": 0}
        if plan is DEFERRED_PLAN:
            db.rollback()
            return deferred
        winner_id, loser_ids = cast("tuple[int, list[int]]", plan)
        db._apply_winner_loser_merge(winner_id, loser_ids)
    return {
        "winner_id": winner_id,
        "loser_ids": list(loser_ids),
        "rejected": len(loser_ids),
    }


def keep_duplicate_copy(db: Database, photo_id: int) -> JsonDict:
    """Make ``photo_id`` the one kept copy of its duplicate group.

    The user's override of a decision the resolver (or an earlier choice)
    made: the chosen copy is un-rejected if needed, every other kept copy
    of the same file is rejected, and their rating and keywords merge
    onto the chosen copy, all in one transaction. Copies already rejected
    stay rejected. Refuses a copy that is missing on disk or on an
    offline volume (see ``DuplicatesRepository.keep_copy_refusal``).

    The plan is read under SQLite's writer lock and applied in the same
    transaction, so two concurrent keeps in one group can't each reject
    the old keeper and leave both chosen copies kept; the later one sees
    the earlier one's keeper and rejects it. The disk checks run before
    the lock, since a stat on a network volume can be slow.

    Returns ``{"file_hash", "winner_id", "loser_ids", "rating", "flag"}``
    or ``{"error": reason}``.
    """
    repo = db.duplicates
    reason = repo.keep_copy_refusal(photo_id)
    if reason is not None:
        return {"error": reason}
    db.begin_immediate()
    try:
        found, reason = repo.keep_copy_plan(photo_id)
        if reason is not None:
            db.rollback()
            return {"error": reason}
        plan = cast("JsonDict", found)
        # Commits the transaction opened above.
        db._apply_winner_loser_merge(
            photo_id, plan["loser_ids"], winner_flag=plan["winner_flag"],
        )
    except BaseException:
        if db.in_transaction:
            db.rollback()
        raise
    return {
        "file_hash": plan["file_hash"],
        "winner_id": photo_id,
        "loser_ids": plan["loser_ids"],
        "rating": repo.photo_metadata(photo_id).rating,
        "flag": plan["winner_flag"],
    }


def bulk_resolve_by_folder(
    db: Database, file_hashes: Iterable[str], keep_folder: str | None,
) -> JsonDict:
    """Force-resolve many duplicate groups by keeping the photo whose
    folder matches ``keep_folder``. Companion to the bulk-decide UI.

    For each ``file_hash``: find non-rejected candidates, pick the one
    whose folder path equals ``keep_folder`` as winner, mark every
    other candidate as rejected, merge metadata. If multiple
    candidates live in ``keep_folder`` (rare — typically only happens
    when same-folder duplicates accumulate), runs the deterministic
    resolver against just those to pick a single winner.

    Skip reasons are surfaced rather than raised so a single bad hash
    doesn't poison a 1000-hash batch:
    - ``"no candidates"`` — file_hash has no DB rows (stale UI state)
    - ``"fewer than 2 candidates"`` — only one row left, nothing to do
    - ``"no candidate in keep_folder"`` — group exists but isn't
      actionable from this folder choice
    - ``"keep_folder candidate missing on disk"`` — the row(s) in
      keep_folder point at files that no longer exist; promoting them
      would reject the only surviving sibling (and, if the caller
      chains a delete, trash it).

    Returns ``{"resolved": [{"file_hash", "winner_id", "loser_ids"}],
    "skipped": [{"file_hash", "reason"}]}``.
    """
    # Normalize once. The bucket UI derives folder paths from
    # ``os.path.dirname(...)`` (never trailing-slashed), but
    # ``folders.path`` rows can carry a trailing separator from
    # manual relocation or legacy imports — a naive string compare
    # silently no-ops the action for those users.
    keep_folder_norm = os.path.normpath(keep_folder) if keep_folder else ""

    repo = db.duplicates
    resolved: list[JsonDict] = []
    skipped: list[JsonDict] = []
    for file_hash in file_hashes:
        preflight = repo.keep_folder_preflight(file_hash, keep_folder_norm)
        # Disk work precedes BEGIN; catalog revalidation and the merge share
        # the writer lock. A failed nested BEGIN leaves caller work intact.
        db.begin_immediate()
        with db.transaction():
            winner_id, loser_ids, reason = repo.keep_folder_plan(
                file_hash, keep_folder_norm, preflight=preflight,
            )
            if reason is not None:
                db.rollback()
                skipped.append({"file_hash": file_hash, "reason": reason})
                continue
            db._apply_winner_loser_merge(winner_id, loser_ids)
        resolved.append({
            "file_hash": file_hash,
            "winner_id": winner_id,
            "loser_ids": loser_ids,
        })

    return {"resolved": resolved, "skipped": skipped}
