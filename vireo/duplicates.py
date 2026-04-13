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
    """Return (winner_id, [loser_ids]) for a group sharing a file_hash.

    Tiebreakers applied in order until decisive:
    1. If at least one candidate has a clean filename, all dirty candidates
       lose and tiebreaking continues among the clean ones. If all candidates
       are dirty (or all are clean), this rule is a no-op.
    2. Shorter path wins.
    3. Older mtime wins.
    4. Lower id wins.
    """
    assert len(candidates) >= 2, "resolver called with <2 candidates"

    clean = [c for c in candidates if not _has_dup_suffix(c.path)]
    dirty = [c for c in candidates if _has_dup_suffix(c.path)]
    if clean and dirty:
        # rule 1 eliminated dirty; now run rules 2-4 on clean only
        pool = clean
    else:
        pool = candidates

    # Rule 2: shorter path wins
    min_len = min(len(c.path) for c in pool)
    shortest = [c for c in pool if len(c.path) == min_len]
    if len(shortest) == 1:
        winner = shortest[0]
        losers = [c.id for c in candidates if c.id != winner.id]
        return winner.id, losers

    # Rule 3: older mtime wins
    min_mtime = min(c.mtime for c in pool)
    oldest = [c for c in pool if c.mtime == min_mtime]
    if len(oldest) == 1:
        winner = oldest[0]
        losers = [c.id for c in candidates if c.id != winner.id]
        return winner.id, losers

    # Rule 4: lower id wins (deterministic)
    winner = min(pool, key=lambda c: c.id)
    losers = [c.id for c in candidates if c.id != winner.id]
    return winner.id, losers


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
