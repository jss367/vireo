"""Photo ingest: the catalog insert and the duplicate hook behind it.

``add_photo`` inserts (or finds) a photo row through ``db.photos`` and, when
the caller already knows the file's hash, runs the duplicate auto-resolver
(``duplicate_resolution.check_and_resolve_duplicates_for_hash``) so a second
copy of a file is flagged rejected as it lands.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

from services import duplicate_resolution

if TYPE_CHECKING:
    from db import Database


@overload
def add_photo(
    db: Database,
    folder_id: int,
    filename: str,
    extension: str,
    file_size: int | None,
    file_mtime: float | None,
    timestamp: str | None = None,
    width: int | None = None,
    height: int | None = None,
    xmp_mtime: float | None = None,
    file_hash: str | None = None,
    return_inserted: Literal[False] = False,
) -> int: ...


@overload
def add_photo(
    db: Database,
    folder_id: int,
    filename: str,
    extension: str,
    file_size: int | None,
    file_mtime: float | None,
    timestamp: str | None = None,
    width: int | None = None,
    height: int | None = None,
    xmp_mtime: float | None = None,
    file_hash: str | None = None,
    *,
    return_inserted: Literal[True],
) -> tuple[int, bool]: ...


@overload
def add_photo(
    db: Database,
    folder_id: int,
    filename: str,
    extension: str,
    file_size: int | None,
    file_mtime: float | None,
    timestamp: str | None = None,
    width: int | None = None,
    height: int | None = None,
    xmp_mtime: float | None = None,
    file_hash: str | None = None,
    return_inserted: bool = False,
) -> int | tuple[int, bool]: ...


def add_photo(
    db: Database,
    folder_id: int,
    filename: str,
    extension: str,
    file_size: int | None,
    file_mtime: float | None,
    timestamp: str | None = None,
    width: int | None = None,
    height: int | None = None,
    xmp_mtime: float | None = None,
    file_hash: str | None = None,
    return_inserted: bool = False,
) -> int | tuple[int, bool]:
    """Insert a photo. Returns the photo id.

    If ``file_hash`` is provided and the insert creates a new row that
    collides with an existing non-rejected photo sharing the same hash,
    the duplicate auto-resolver runs and flags the loser(s) as rejected.
    The hook is wrapped in try/except so resolver bugs never break
    inserts.

    With ``return_inserted=True`` returns ``(photo_id, inserted)`` instead;
    ``inserted`` is False when another writer inserted the row first and
    the INSERT OR IGNORE here was a no-op. Callers that would purge
    state inherited from a recycled id must branch on this signal rather
    than on their own pre-check SELECT, which cannot see a concurrent
    insert.
    """
    # Its own unit, committed before the duplicate resolver below, which
    # refuses to run inside an open transaction.
    with db.transaction():
        photo_id, inserted = db.photos.add(
            folder_id,
            filename,
            extension,
            file_size,
            file_mtime,
            timestamp=timestamp,
            width=width,
            height=height,
            xmp_mtime=xmp_mtime,
            file_hash=file_hash,
        )

    # Auto-resolve duplicates when we have a file_hash and >1 non-rejected
    # rows share it. Most scanner-path callers leave file_hash=None here
    # and set it later via an UPDATE; those callers must invoke
    # duplicate_resolution.check_and_resolve_duplicates_for_hash themselves
    # after the UPDATE.
    if file_hash:
        duplicate_resolution.check_and_resolve_duplicates_for_hash(db, file_hash)

    if return_inserted:
        return photo_id, inserted
    return photo_id
