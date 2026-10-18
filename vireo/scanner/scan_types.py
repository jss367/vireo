"""Type names shared by the scanner package's annotations.

Nothing here runs: the aliases are plain ``dict``/``tuple``/``Callable``
types and the protocol only describes ``file_identity.catalog_folder_path``,
so the phases can name what they pass each other without importing the
modules that provide the values.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from db import Database

# A filesystem path as callers hand it to ``scan()``: a string or a
# ``pathlib.Path``.
StrPath = str | os.PathLike[str]

# One file's ExifTool output, keyed by group (``File``, ``EXIF``,
# ``Composite``, ...). Tag names and value types vary by camera and file
# format, so the values are ``Any``.
ExifMetadata = dict[str, Any]

# A row read by column name: a catalog ``sqlite3.Row``, or a dict shaped like
# one (a pairing member, the columns built from fresh metadata, or
# ``{"id": ...}`` / ``{"owner_id": ...}`` stand-ins for a row).
ColumnRow = sqlite3.Row | Mapping[str, Any]

# A pairing candidate: a ``photos`` row turned into a dict, or an uncataloged
# JPEG shaped like one with ``id`` None (see
# ``_CompanionGroups._uncataloged_member``).
PairingRow = dict[str, Any]

# ``(folder_id, filename)``: the catalog identity reported for a photo id.
PhotoIdentity = tuple[int, str]

# ``(owner folder_id, owner filename, companion filename)``: what must still
# hold for a RAW row to own a companion JPEG.
CompanionOwnership = tuple[int, str, str]

# ``catalog_folder_aliases(db)``: normalized, case-folded path -> the stored
# spellings of the cataloged folders it names.
FolderAliases = dict[str, list[str]]

# A working-copy pass's folder scope entry: a path (that folder and every
# descendant) or ``(path, "exact" | "subtree")``.
WorkingCopyScopeEntry = str | tuple[str, str]

# ``source_paths`` for working-copy extraction: cataloged path ->
# ``(card-side path, expected size, expected mtime_ns)``.
SourcePaths = Mapping[str, tuple[str, int | None, int | None]]

# The caller's probes return a truthy value to stop or pause (``ScanControl``
# documents each); only their truth is read.
CheckCallback = Callable[[], Any]

# ``progress(current, total)``.
ProgressCallback = Callable[[int, int], object]

# ``status(message)``, optionally also taking the keyword-only
# ``phase_current``, ``phase_total`` and ``phase_label``
# (``scanner.status`` checks which before calling it).
StatusCallback = Callable[..., object]

# ``photo(photo_id, path)``, after a photo is committed.
PhotoCallback = Callable[[int, str], object]

# ``photo_merged(old_id, new_id, path)``; ``new_id`` and ``path`` are None
# when the surviving RAW could not be confirmed.
PhotoMergedCallback = Callable[[int, "int | None", "str | None"], object]


class CatalogFolderPath(Protocol):
    """``file_identity.catalog_folder_path``."""

    def __call__(
        self, db: Database, path: StrPath, *, aliases: FolderAliases | None = None,
    ) -> str: ...
