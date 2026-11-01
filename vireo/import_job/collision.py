"""Read-only destination candidate traversal and recovery verification.

The importer owns claims, companion placement and bookkeeping. The preview
supplies batched directory metadata; both use these same names and decisions.
"""

from __future__ import annotations

import logging
import os
import stat
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from import_job.copy_verify import DestReadCancelled
from import_job.destination import _is_source_backed_path
from import_job.siblings import _slot_for
from scanner import EMPTY_FILE_SHA256

log = logging.getLogger(__name__)
type Adoption = tuple[str, str, str | None]


@dataclass(frozen=True)
class CollisionCandidate:
    """One primary or suffixed destination name, without filesystem I/O."""

    name: str
    path: str
    slot: int


def collision_candidates(
    source_file: Path, folder: str, anchor: int | None = None,
) -> Iterator[CollisionCandidate]:
    """Try the companion anchor first, then primary and increasing suffixes.

    Callers stop at the first usable free slot or verified adoption. Yielding
    names before probing lets the importer consult its unflushed rsync claims
    and lets the preview stop against its one-per-folder directory snapshot.
    """
    stem, suffix = os.path.splitext(source_file.name)
    counter = 0
    while True:
        slot = _slot_for(counter, anchor)
        name = source_file.name if slot == 0 else f"{stem}_{slot}{suffix}"
        yield CollisionCandidate(name, os.path.join(folder, name), slot)
        counter += 1


def candidate_adoption(
    source_file: Path, candidate_path: str, candidate_stat: os.stat_result, *,
    source_size: int, source_hash: Callable[[], str | None],
    path_under_any_source: Callable[[str | Path], bool],
    hash_candidate: Callable[[str], str | None],
) -> Adoption | None:
    """Verify a regular off-source candidate, using caller-supplied metadata.

    A failed stat is never a free slot or an empty file. The caller advances
    past it. Destination cancellation propagates before any source hash read;
    an ordinary unreadable destination is simply not adoptable.
    """
    if _is_source_backed_path(source_file, candidate_path, path_under_any_source):
        log.warning("Destination candidate %s resolves to source media; skipping it", candidate_path)
        return None
    if not stat.S_ISREG(candidate_stat.st_mode):
        return None
    candidate_size = candidate_stat.st_size
    if source_size == 0 and candidate_size == 0:
        # The source may have grown since its opening stat. This branch reads
        # no bytes, so recheck before attesting that an empty archive is enough.
        try:
            if source_file.stat().st_size != 0:
                return None
        except OSError:
            return None
        return candidate_path, EMPTY_FILE_SHA256, EMPTY_FILE_SHA256
    if candidate_size != source_size:
        return None
    try:
        candidate_hash = hash_candidate(candidate_path)
    except DestReadCancelled:
        raise
    except OSError:
        candidate_hash = None
    expected = source_hash()
    if candidate_hash is not None and expected is not None and candidate_hash == expected:
        return candidate_path, expected, expected
    return None
