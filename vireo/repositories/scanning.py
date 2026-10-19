"""Persistence for ``scanner.scan``'s catalog pass.

What a scan reads and writes per file and per folder that has no other
domain: the incremental pre-pass's baseline rows, the per-file existence
check, the metadata and identity columns a scan refreshes on a ``photos``
row, the sidecar ``xmp_mtime``, the ``folders.status`` badge a failed scan
leaves (``'partial'``) and a clean one clears, and the embedded-keyword
backfill's candidate rows, and the photos whose broken metadata the
pipeline's scan stage sends back through a rescan. It also owns the connection-local machinery the
scan uses to notice collection edits between its inserts (TEMP triggers and
a Python counter on the connection; see ``install_collection_change_counter``).

Companion pairing lives in ``repositories/companion_pairing.py`` and the
working-copy pass in ``repositories/working_copies.py``.

Callers reach this module as ``db.scanning`` (a fresh repository per access,
see ``Database.scanning``); there are no forwarding wrappers on
``Database``. A scan catalogs photos globally, so the repository takes no
workspace id. Nothing here commits: the scan owns its commits (through
``Database.commit_with_retry``).
"""

import os
import sqlite3
from collections.abc import Collection, Sequence
from typing import Any


class ScanningRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # -- reads ---------------------------------------------------------------

    def incremental_baseline_rows(
        self, folder_filenames: Sequence[tuple[str, str]],
    ) -> sqlite3.Cursor:
        """Scan baselines for ``(folder_path, filename)`` pairs, across all workspaces.

        Iterate the returned cursor; ``folder_filenames`` must stay below
        SQLite's parameter limit (the caller batches it).
        """
        placeholders = ",".join("(?, ?)" for _ in folder_filenames)
        params = [part for pair in folder_filenames for part in pair]
        return self.conn.execute(
            f"WITH requested(folder_path, filename) AS (VALUES {placeholders}) "
            "SELECT f.path AS folder_path, p.id, p.folder_id, p.filename, "
            "p.extension, p.file_size, p.file_mtime, p.xmp_mtime, p.xmp_import_pending, p.timestamp, "
            "p.width, p.file_hash, p.exif_data IS NOT NULL AS exif_extracted, "
            "(p.exif_data IS NULL AND p.camera_make IS NULL "
            "AND p.camera_model IS NULL AND p.lens IS NULL "
            "AND p.aperture IS NULL AND p.shutter_speed IS NULL "
            "AND p.iso IS NULL) AS summary_needs_extract "
            # Preserve os.path.join's match for legacy folder paths ending
            # in a separator, while retaining indexed equality lookups.
            "FROM requested r JOIN folders f "
            "ON f.path IN (r.folder_path, r.folder_path || ?) "
            "JOIN photos p ON p.folder_id = f.id AND p.filename = r.filename",
            [*params, os.sep],
        )

    def folder_ids_at(self, folder_path: str) -> list[int]:
        """Folder rows stored as ``folder_path``, with or without a trailing separator."""
        return [
            row[0] for row in self.conn.execute(
                "SELECT id FROM folders WHERE path IN (?, ?)",
                (folder_path, folder_path + os.sep),
            )
        ]

    def content_baseline(self, folder_id: int, filename: str) -> sqlite3.Row | None:
        """``file_hash`` and ``flag`` of the cataloged row, or None if there is none."""
        return self.conn.execute(
            "SELECT file_hash, flag FROM photos WHERE folder_id = ? AND filename = ?",
            (folder_id, filename),
        ).fetchone()

    def embedded_keyword_rows(self) -> list[sqlite3.Row]:
        """``id, exif_data`` of photos whose stored ExifTool JSON names keywords."""
        # ``exif_data`` is ``json.dumps`` output, so the key spelling is fixed.
        # The closing quote keeps ``"SubjectDistance"`` and friends out.
        return self.conn.execute(
            """SELECT id, exif_data FROM photos
           WHERE exif_data LIKE '%"Subject": %'
              OR exif_data LIKE '%"HierarchicalSubject": %'
              OR exif_data LIKE '%"Keywords": %'"""
        ).fetchall()

    def broken_metadata_rows(
        self, photo_ids: Sequence[int], raw_extensions: Sequence[str],
    ) -> list[sqlite3.Row]:
        """``folder_path`` and ``filename`` of the photos in ``photo_ids`` (one
        chunk) whose metadata a rescan should repair, ordered by folder path
        then filename.

        A photo qualifies when it has no stored ExifTool JSON and either no
        timestamp or, for a RAW extension in ``raw_extensions``, a recorded
        width under 1000 px (the embedded-preview size a failed RAW read
        leaves behind).
        """
        raw_list = ",".join(f"'{e}'" for e in raw_extensions)
        placeholders = ",".join("?" * len(photo_ids))
        return self.conn.execute(
            f"""SELECT f.path AS folder_path, p.filename
                FROM photos p
                JOIN folders f ON p.folder_id = f.id
                WHERE p.id IN ({placeholders})
                  AND p.exif_data IS NULL
                  AND (p.timestamp IS NULL
                       OR (p.extension IN ({raw_list})
                           AND p.width IS NOT NULL AND p.width < 1000))
                ORDER BY f.path, p.filename""",
            tuple(photo_ids),
        ).fetchall()

    def keyword_count(self, photo_id: int) -> int:
        """How many keyword associations the photo has."""
        return self.conn.execute(
            "SELECT COUNT(*) FROM photo_keywords WHERE photo_id = ?", (photo_id,),
        ).fetchone()[0]

    # -- writes --------------------------------------------------------------

    def update_photo(
        self, photo_id: int, assignments: Sequence[str], params: Sequence[Any],
    ) -> None:
        """Run ``UPDATE photos SET <assignments> WHERE id=photo_id``.

        ``assignments`` are ``"<column>=?"`` (or literal) clauses for the
        columns the caller decided to write, from the scan's metadata refresh
        and the companion gap fill (and ``capture_time``'s re-read after it
        rewrites a photo's capture time); ``params`` binds their placeholders
        in order.
        """
        self.conn.execute(
            f"UPDATE photos SET {', '.join(assignments)} WHERE id=?",
            [*params, photo_id],
        )

    def set_xmp_mtime(self, photo_id: int, xmp_mtime: float | None) -> None:
        """Record the sidecar's mtime; None clears it (the sidecar is gone)."""
        if xmp_mtime is None:
            self.conn.execute(
                "UPDATE photos SET xmp_mtime = NULL, xmp_import_pending = 0 WHERE id = ?",
                (photo_id,),
            )
            return
        self.conn.execute(
            "UPDATE photos SET xmp_mtime = ?, xmp_import_pending = 0 WHERE id = ?",
            (xmp_mtime, photo_id),
        )

    def defer_xmp_import(self, photo_id: int, *, clear_mtime: bool = False) -> None:
        """Do not resolve this row's duplicate group before its keywords land."""
        stamp = ", xmp_mtime = NULL" if clear_mtime else ""
        self.conn.execute(f"UPDATE photos SET xmp_import_pending = 1{stamp} WHERE id = ?", (photo_id,))

    def set_folder_status(
        self, new_status: str, *, paths: Collection[str] | None = None,
        folder_ids: Collection[int] | None = None, only_from_partial: bool = False,
    ) -> None:
        """Stamp the folders at ``paths`` and with ``folder_ids`` with ``new_status``.

        ``only_from_partial`` restricts both UPDATEs to rows already
        ``'partial'``.
        """
        guard = " AND status = 'partial'" if only_from_partial else ""
        if paths:
            path_placeholders = ",".join("?" * len(paths))
            self.conn.execute(
                f"UPDATE folders SET status = ? "
                f"WHERE path IN ({path_placeholders}){guard}",
                (new_status, *paths),
            )
        if folder_ids:
            id_placeholders = ",".join("?" * len(folder_ids))
            self.conn.execute(
                f"UPDATE folders SET status = ? "
                f"WHERE id IN ({id_placeholders}){guard}",
                (new_status, *folder_ids),
            )

    # -- collection edits on this connection ---------------------------------

    def install_collection_change_counter(self) -> None:
        """Count this connection's ``collections`` writes, once per connection.

        TEMP triggers call a Python function that bumps
        ``conn._scan_collection_revision``. Unlike a SQL counter, a rollback
        cannot reuse a revision. The triggers and the function last only as
        long as the connection and never change the persistent schema.
        """
        conn = self.conn
        if not hasattr(conn, "_scan_collection_revision"):
            conn._scan_collection_revision = 0  # type: ignore[attr-defined]

            def collection_changed():
                conn._scan_collection_revision += 1  # type: ignore[attr-defined]
                return 0

            conn.create_function("_scan_collection_changed", 0, collection_changed)
            for operation in ("INSERT", "UPDATE", "DELETE"):
                # IF NOT EXISTS keeps this idempotent when a prior scan on
                # the same sqlite connection has already created the TEMP
                # triggers — e.g. a test that re-wraps the connection loses
                # the Python-side counter attribute but the triggers persist
                # for the lifetime of the connection.
                conn.execute(
                    f"CREATE TEMP TRIGGER IF NOT EXISTS "
                    f"_scan_collection_{operation.lower()} "
                    f"AFTER {operation} ON main.collections BEGIN "
                    "SELECT _scan_collection_changed(); END"
                )

    def data_version(self) -> int:
        """``PRAGMA data_version``: changes when another connection commits."""
        return self.conn.execute("PRAGMA data_version").fetchone()[0]

    def collection_revision(self) -> int:
        """This connection's collection-write counter (see ``install_collection_change_counter``)."""
        return self.conn._scan_collection_revision  # type: ignore[attr-defined]
