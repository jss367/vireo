"""Catalog reads and folder-status writes the import job runs (``import_job``).

The import job copies card files into the archive, catalogs each batch
through ``scanner.scan()`` and then cross-checks what landed against the
catalog. This repository holds the SQL of that cross-check and of the
bookkeeping around it: the landed-photo fingerprints a retry verifies, the
per-path photo and companion lookups behind hash stamping and cache
invalidation, a resume's recovery of its parent's landings, the duplicate
gate's twin rows, and the ``'missing'`` → ``'ok'`` promotion of a folder the
import just proved present. It also lists every cataloged file for the
local-processing import's archive preflight (``pipeline_stages.scanning``)
and holds the SQL of the duplicate gate itself (``import_dedup``: the
catalog identity index and the backfill of missing companion identities)
and of ``ingest``'s destination-folder identity read.

Everything here is catalog-wide (photos and folders are global), so nothing
resolves a workspace. The writes never commit: they run in the caller's
transaction, and ``import_job`` ends it with ``db.commit()`` or
``db.rollback()`` exactly where it always did. The one exception is
``record_recovered_companion_identities``, which writes inside its own
savepoint; releasing an outermost savepoint commits it, exactly as the
inline block in ``import_dedup`` did. Id lists arrive already chunked by the
caller.

Callers reach it as ``db.imports`` (a fresh repository per access, see
``Database.imports``); there are no forwarding wrappers on ``Database``.
"""

import sqlite3
from collections.abc import Callable, Iterable, Sequence
from typing import Any

# Paired companions whose import identity is missing from
# ``companion_identities``, in usable folders.
MISSING_COMPANION_IDENTITIES_SQL = (
    "SELECT p.id, p.companion_path, f.path FROM photos p "
    "JOIN folders f ON f.id=p.folder_id "
    "LEFT JOIN companion_identities c "
    "ON c.photo_id=p.id AND c.filename=p.companion_path "
    "WHERE p.companion_path IS NOT NULL AND c.photo_id IS NULL "
    "AND f.status IN ('ok', 'partial')"
)


class ImportRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # -- landed photos --------------------------------------------------------

    def landed_fingerprint_rows(self, photo_ids: Sequence[int]) -> list[sqlite3.Row]:
        """``id``, ``folder_path``, ``filename``, ``file_size`` and ``file_hash``
        of each photo in ``photo_ids`` (one chunk) that exists."""
        placeholders = ",".join("?" for _ in photo_ids)
        return self.conn.execute(
            f"""SELECT p.id AS id,
                       f.path AS folder_path,
                       p.filename AS filename,
                       p.file_size AS file_size,
                       p.file_hash AS file_hash
                FROM photos p
                JOIN folders f ON f.id = p.folder_id
                WHERE p.id IN ({placeholders})""",
            list(photo_ids),
        ).fetchall()

    def cataloged_file_rows(self) -> list[sqlite3.Row]:
        """``path`` (the folder's) and ``filename`` of every cataloged photo.

        The local-processing import's archive preflight indexes these to tell
        a cataloged file in the destination from unindexed copy debris.
        """
        return self.conn.execute(
            """SELECT f.path, p.filename
                 FROM photos p
                 JOIN folders f ON f.id = p.folder_id"""
        ).fetchall()

    def photo_hash_row(self, folder_path: str, filename: str) -> sqlite3.Row | None:
        """``id`` and ``file_hash`` of the photo stored at ``folder_path``/``filename``."""
        return self.conn.execute(
            """SELECT p.id, p.file_hash FROM photos p
               JOIN folders f ON f.id = p.folder_id
               WHERE f.path = ? AND p.filename = ?""",
            (folder_path, filename),
        ).fetchone()

    def photo_with_companion(self, folder_path: str,
                             companion_name: str) -> sqlite3.Row | None:
        """``id`` of a photo in ``folder_path`` whose ``companion_path`` is
        ``companion_name`` (a JPEG merged into its RAW's row), or None."""
        return self.conn.execute(
            """SELECT p.id FROM photos p
               JOIN folders f ON f.id = p.folder_id
               WHERE f.path = ? AND p.companion_path = ?""",
            (folder_path, companion_name),
        ).fetchone()

    def folder_photo_identities(self, folder_path: str) -> list[sqlite3.Row]:
        """``id``, ``filename``, ``companion_path`` and ``file_hash`` of every
        photo in the folder stored at ``folder_path``."""
        return self.conn.execute(
            """SELECT p.id, p.filename, p.companion_path, p.file_hash
               FROM photos p JOIN folders f ON f.id = p.folder_id
               WHERE f.path = ?""",
            (folder_path,),
        ).fetchall()

    def folders_lacking_working_copy(self, photo_ids: Sequence[int]) -> list[sqlite3.Row]:
        """Distinct folder ``path`` of the photos in ``photo_ids`` (one chunk)
        that have no working copy yet."""
        placeholders = ",".join("?" for _ in photo_ids)
        return self.conn.execute(
            f"""SELECT DISTINCT f.path AS path
                FROM photos p
                JOIN folders f ON f.id = p.folder_id
                WHERE p.id IN ({placeholders})
                  AND p.working_copy_path IS NULL""",
            list(photo_ids),
        ).fetchall()

    # -- duplicate twins ------------------------------------------------------

    def twin_rows_by_size(self, file_size: int) -> list[sqlite3.Row]:
        """Photos of ``file_size`` bytes with their folder path and status: the
        prefilter for a metadata-key twin, which the caller compares in Python."""
        return self.conn.execute(
            """SELECT p.id, p.filename, p.file_size, p.timestamp, p.file_hash,
                      f.path AS folder_path, f.status AS folder_status
               FROM photos p JOIN folders f ON f.id = p.folder_id
               WHERE p.file_size = ?""",
            (file_size,),
        ).fetchall()

    def twin_rows_by_hash(self, file_hash: str) -> list[sqlite3.Row]:
        """Photos whose stored ``file_hash`` is ``file_hash``, with their folder
        path and status."""
        return self.conn.execute(
            """SELECT p.id, p.filename, p.file_size, f.path AS folder_path,
                      f.status AS folder_status
               FROM photos p JOIN folders f ON f.id = p.folder_id
               WHERE p.file_hash = ?""",
            (file_hash,),
        ).fetchall()

    # -- duplicate gate (``import_dedup``, ``ingest``) ---------------------------

    def catalog_identity_rows(self) -> list[sqlite3.Row]:
        """``filename``, ``file_size``, ``timestamp`` and ``file_hash`` of every
        photo, then of every paired companion's recorded identity."""
        return self.conn.execute(
            "SELECT filename, file_size, timestamp, file_hash FROM photos "
            "UNION ALL SELECT c.filename, c.file_size, c.timestamp, c.file_hash "
            "FROM companion_identities c JOIN photos p ON p.id=c.photo_id "
            "AND p.companion_path=c.filename"
        ).fetchall()

    def missing_companion_identity_rows(self) -> list[sqlite3.Row]:
        """Every companion missing its identity (``id``, ``companion_path``,
        folder ``path``), ordered by photo id."""
        return self.conn.execute(
            MISSING_COMPANION_IDENTITIES_SQL + " ORDER BY p.id"
        ).fetchall()

    def missing_companion_identity_batch(self, after_id: int,
                                         limit: int) -> list[sqlite3.Row]:
        """The next ``limit`` companions missing their identity past photo
        ``after_id``, ordered by photo id."""
        return self.conn.execute(
            MISSING_COMPANION_IDENTITIES_SQL
            + " AND p.id > ? ORDER BY p.id LIMIT ?",
            (after_id, limit),
        ).fetchall()

    def record_recovered_companion_identities(
            self, recovered: Sequence[tuple]) -> None:
        """Write recovered ``(photo_id, filename, file_size, timestamp,
        file_hash)`` identities with a NULL ``file_mtime`` and ``needs_sync``.

        Runs in ``SAVEPOINT recover_companion_identities``: an exception rolls
        back to it and re-raises, and the savepoint is released either way
        (which commits when no outer transaction is open). See
        ``import_dedup._recover_companion_batch`` for why ``file_mtime`` stays
        NULL.
        """
        self.conn.execute("SAVEPOINT recover_companion_identities")
        try:
            self.conn.executemany(
                "INSERT OR REPLACE INTO companion_identities "
                "(photo_id, filename, file_size, timestamp, file_hash, file_mtime, needs_sync)"
                " VALUES (?, ?, ?, ?, ?, NULL, 1)",
                recovered,
            )
        except BaseException:
            self.conn.execute("ROLLBACK TO recover_companion_identities")
            raise
        finally:
            self.conn.execute("RELEASE recover_companion_identities")

    def destination_identity_rows(
            self, folder_scope: str, folder_params: Sequence[Any],
            sql_functions: Iterable[tuple[str, int, Callable[..., Any]]] = (),
    ) -> list[sqlite3.Row]:
        """Identity rows (``file_hash``, ``filename``, ``file_size``,
        ``timestamp``, ``folder_path``) of photos, then of paired companions,
        in usable folders matching ``folder_scope`` (SQL over folder alias
        ``f``, starting with ``AND``, or empty for every folder).

        ``sql_functions`` are ``(name, narg, func)`` registered on the
        connection first, for a scope that calls into Python.
        """
        for name, narg, func in sql_functions:
            self.conn.create_function(name, narg, func)
        folder_rows = []
        for identity, source in (
            ("p", "photos p"),
            ("c", "companion_identities c JOIN photos p ON p.id=c.photo_id AND p.companion_path=c.filename"),
        ):
            folder_rows.extend(self.conn.execute(
                f"""SELECT {identity}.file_hash, {identity}.filename,
                       {identity}.file_size, {identity}.timestamp,
                       f.path AS folder_path
                  FROM {source}
                  JOIN folders f ON p.folder_id = f.id
                 WHERE ({identity}.file_hash IS NOT NULL OR {identity}.timestamp IS NOT NULL)
                   AND f.status IN ('ok', 'partial') {folder_scope}""",
                folder_params,
            ).fetchall())
        return folder_rows

    # -- folder status --------------------------------------------------------

    def folder_status_row(self, path: str) -> sqlite3.Row | None:
        """``id`` and ``status`` of the folder stored at ``path``, or None."""
        return self.conn.execute(
            "SELECT id, status FROM folders WHERE path = ?",
            (path,),
        ).fetchone()

    def promote_missing_folder(self, folder_id: int) -> None:
        """Mark folder ``folder_id`` ``'ok'`` if it is ``'missing'``; any
        other status (``'partial'`` in particular) is left alone. Does not
        commit."""
        self.conn.execute(
            "UPDATE folders SET status = 'ok' WHERE id = ? "
            "AND status = 'missing'",
            (folder_id,),
        )

    def promote_missing_folder_at(self, path: str) -> None:
        """``promote_missing_folder`` for the folder stored at ``path``. Does
        not commit."""
        self.conn.execute(
            "UPDATE folders SET status = 'ok' "
            "WHERE path = ? AND status = 'missing'",
            (path,),
        )
