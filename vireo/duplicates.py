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
    1. Prefer filenames without dup-ish suffixes (case-insensitive).
    2. (TODO) shorter path wins.
    3. (TODO) older mtime wins.
    4. (TODO) lower id wins.
    """
    assert len(candidates) >= 2, "resolver called with <2 candidates"

    clean = [c for c in candidates if not _has_dup_suffix(c.path)]
    dirty = [c for c in candidates if _has_dup_suffix(c.path)]
    if clean and dirty:
        # rule 1 decisive: all dirty are losers; among clean, arbitrary for now
        winner = clean[0]
        losers = [c for c in candidates if c.id != winner.id]
        return winner.id, [l.id for l in losers]

    # No decisive split yet — fall through to later rules (raise for now)
    raise NotImplementedError("later tiebreakers not yet implemented")
