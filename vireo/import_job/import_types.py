"""Type names shared by the import job's annotations.

Nothing here runs: the aliases are plain ``tuple``/``dict``/``Callable``
types and the protocols describe callables the job threads through its
phases (the progress emitter and ``scanner.scan``), so a phase module can
name them without importing the module that builds them.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, TypedDict

if TYPE_CHECKING:
    from db import Database
    from scanner import ScanControl, ScanHooks, ScanOptions

# A ``DuplicateChecker`` identity token: ``("hash", sha256)``, or
# ``("key", (casefolded filename, byte size, capture second))``.
DupToken = tuple[Literal["hash"], str] | tuple[Literal["key"], tuple[str, int, str]]

# ``(rel, files)``: card files that land in one destination folder, ``rel``
# being that folder relative to the destination ("." for its root).
ImportBatch = tuple[str, list[Path]]

# ``(card directory, casefolded stem)``: card files that must keep one
# destination stem (a RAW and its JPEG).
CompanionKey = tuple[str, str]

# A file queued for the batch rsync: ``(source_file, dest_basename,
# src_hash, src_size, src_mtime_ns)``. ``src_hash`` is None for a zero-byte
# source when duplicate skipping is on.
QueuedTransfer = tuple[Path, str, "str | None", int, int]

# The run's nonblocking Stop probe (``runner.cancellation_requested``).
StopCheck = Callable[[], bool]

# A landed file's recorded identity: ``[size, mtime_ns, verified_hash]``
# (``-1`` sizes when the stat failed); a parent's record may lack the hash.
LandedIdentity = list[int | str]

# ``dest_path -> (card-side path, size, mtime_ns)``: where the deferred
# working-copy pass may read a landed file's bytes instead of the archive.
WorkingCopySources = dict[str, tuple[str, "int | None", "int | None"]]


class UnsafeFile(TypedDict):
    """One ``unsafe_files`` line: what is still card-only, and why."""

    path: str
    reason: str


class SourceSnapshot(TypedDict):
    """A source's discovered file set, for a retry's drift check."""

    count: int
    signature: str


class Emit(Protocol):
    """The progress emitter ``progress._make_emitter`` builds for a run."""

    def __call__(
        self, phase: str, current: int, total: int, current_file: str = "", *,
        is_importing: bool = False,
    ) -> None: ...


class ScanFunction(Protocol):
    """``scanner.scan``, as the per-batch catalog scan calls it."""

    def __call__(
        self, root: str, db: Database, *, options: ScanOptions | None = None,
        hooks: ScanHooks | None = None, control: ScanControl | None = None,
    ) -> object: ...
