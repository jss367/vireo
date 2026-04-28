"""Exact-duplicate resolution for photos sharing a file_hash.

Pure functions — no DB access. See docs/plans/2026-04-12-duplicate-detection-design.md.
"""
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DupCandidate:
    id: int
    path: str
    mtime: float
    # Whether the file is currently present on disk. Defaults to True so old
    # call sites and tests that don't care about existence keep working; the
    # scan/db layers populate it via os.path.exists().
    exists: bool = True


_DUP_SUFFIX_RES = [
    re.compile(r" \(\d+\)$", re.IGNORECASE),
    re.compile(r" copy( \d+)?$", re.IGNORECASE),
    re.compile(r"-\d+$"),
    re.compile(r"_\d+$"),
]


def _has_dup_suffix(path: str) -> bool:
    stem = Path(path).stem
    return any(r.search(stem) for r in _DUP_SUFFIX_RES)


def resolve_duplicates(candidates):
    """Return ``(winner_id, [(loser_id, reason), ...])`` for a duplicate group.

    The resolver itself is the single source of truth for *why* each candidate
    lost — callers (UI, DB) should never replay the rules to reconstruct a
    reason.

    Tiebreakers applied in order until decisive. Candidates eliminated at an
    earlier rule keep the earlier-rule reason; later rules operate only on the
    still-tied pool:

    0. Files that exist on disk beat files that don't. Missing-file losers get
       reason ``"file missing on disk"``. If all candidates are missing (or
       all exist), this rule is a no-op — when *all* are missing we still
       pick a winner via the remaining rules so the DB rows can be cleaned
       up; callers should warn the user since no on-disk file will survive.
    1. If at least one candidate has a clean filename, all dirty candidates
       lose with reason ``"filename has dup suffix"`` and tiebreaking
       continues among the clean ones. If all candidates are dirty (or all
       are clean), this rule is a no-op.
    2. Shorter path wins. Longer-path losers get reason ``"longer path"``.
    3. Older mtime wins. Later-mtime losers get reason ``"later mtime"``.
    4. Lower id wins. Higher-id losers get reason ``"higher id"``.
    """
    assert len(candidates) >= 2, "resolver called with <2 candidates"

    losers_with_reasons = []

    # Rule 0: existing files beat missing ones
    present = [c for c in candidates if c.exists]
    missing = [c for c in candidates if not c.exists]
    if present and missing:
        losers_with_reasons.extend(
            (c.id, "file missing on disk") for c in missing
        )
        pool = present
        if len(pool) == 1:
            return pool[0].id, losers_with_reasons
    else:
        pool = candidates

    # Rule 1: clean filename beats dirty (dup-suffix) filename
    clean = [c for c in pool if not _has_dup_suffix(c.path)]
    dirty = [c for c in pool if _has_dup_suffix(c.path)]
    if clean and dirty:
        losers_with_reasons.extend(
            (c.id, "filename has dup suffix") for c in dirty
        )
        pool = clean

    # Rule 2: shorter path wins
    min_len = min(len(c.path) for c in pool)
    shortest = [c for c in pool if len(c.path) == min_len]
    if len(shortest) == 1:
        winner = shortest[0]
        losers_with_reasons.extend(
            (c.id, "longer path") for c in pool if c.id != winner.id
        )
        return winner.id, losers_with_reasons

    # Rule 3: older mtime wins — operate on `shortest` (still tied at rule 2).
    # Record the longer-path candidates being dropped from the pool as losers
    # now; the later rules only see `shortest`, so without this the longer-path
    # rows would never appear in losers_with_reasons and stay unrejected.
    losers_with_reasons.extend(
        (c.id, "longer path") for c in pool if len(c.path) != min_len
    )
    pool = shortest
    min_mtime = min(c.mtime for c in pool)
    oldest = [c for c in pool if c.mtime == min_mtime]
    if len(oldest) == 1:
        winner = oldest[0]
        losers_with_reasons.extend(
            (c.id, "later mtime") for c in pool if c.id != winner.id
        )
        return winner.id, losers_with_reasons

    # Rule 4: lower id wins (deterministic) — operate on `oldest`.
    # Same as above: record the later-mtime candidates being dropped so the
    # rule-4 sub-pool isn't the only source of losers.
    losers_with_reasons.extend(
        (c.id, "later mtime") for c in pool if c.mtime != min_mtime
    )
    pool = oldest
    winner = min(pool, key=lambda c: c.id)
    losers_with_reasons.extend(
        (c.id, "higher id") for c in pool if c.id != winner.id
    )
    return winner.id, losers_with_reasons


@dataclass
class PhotoMetadata:
    id: int
    rating: int
    keyword_ids: set
    collection_ids: set
    has_pending_edit: bool


@dataclass
class MergeResult:
    winner_id: int
    new_rating: int
    keyword_ids_to_add: set
    collection_ids_to_add: set
    pending_from_loser_id: int | None
    loser_ids: list


def merge_metadata(winner: PhotoMetadata, losers: list) -> MergeResult:
    new_rating = max([winner.rating] + [l.rating for l in losers])

    all_loser_kws = set().union(*(l.keyword_ids for l in losers)) if losers else set()
    kws_to_add = all_loser_kws - winner.keyword_ids

    all_loser_cols = set().union(*(l.collection_ids for l in losers)) if losers else set()
    cols_to_add = all_loser_cols - winner.collection_ids

    pending_from = None
    if not winner.has_pending_edit:
        for l in losers:
            if l.has_pending_edit:
                pending_from = l.id
                break

    return MergeResult(
        winner_id=winner.id,
        new_rating=new_rating,
        keyword_ids_to_add=kws_to_add,
        collection_ids_to_add=cols_to_add,
        pending_from_loser_id=pending_from,
        loser_ids=[l.id for l in losers],
    )
