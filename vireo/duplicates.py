"""Exact-duplicate resolution for photos sharing a file_hash.

Pure functions — no DB access. See docs/plans/2026-04-12-duplicate-detection-design.md.
"""
from dataclasses import dataclass


@dataclass
class DupCandidate:
    id: int
    path: str
    mtime: float


def resolve_duplicates(candidates):
    raise NotImplementedError
