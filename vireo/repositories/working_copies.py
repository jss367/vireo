"""Persistence for the scanner's working-copy extraction pass.

The ``working_copy_*`` columns on ``photos`` record whether a JPEG working
copy exists for a photo, whether extraction failed (and against which
source mtime), and whether a quota pass evicted or deferred it.
``scanner._extract_working_copies`` snapshots its candidate rows up front
and then spends seconds per row decoding, so every write it makes for a
candidate carries the snapshot's full source identity (see
``ROW_IDENTITY_WHERE``): a row whose id was reused, or whose companion or
folder changed while extraction ran, is never stamped with this batch's
result.

The candidate predicate itself (``scanner._working_copy_candidate_predicate``)
stays in the scanner beside the retry window it encodes; the backfill summary
and the extractor's SELECT both receive it from there so the two stay in sync.

Callers reach this module as ``db.working_copies`` (a fresh repository per
access, see ``Database.working_copies``); there are no forwarding wrappers on
``Database``. The data is catalog-wide, so the repository takes no workspace
id. Nothing here commits: the extraction pass owns its commits (through
``Database.commit_with_retry``) except ``load_scope``, which commits only the
transaction it opened itself, exactly as the scanner did. The render paths'
RAW-decode failure marker (``mark_raw_decode_failed``) lives here too; they
commit it themselves.
"""

import sqlite3
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

# Every catalog write for a candidate row carries the snapshot's full source
# identity, so a row whose id was reused, or whose companion or folder changed
# while extraction ran, is never stamped with this batch's result. Parameters:
# id, then ``row_identity_params(row)``.
ROW_IDENTITY_WHERE = (
    " WHERE id=? AND folder_id=? AND filename=?"
    " AND companion_path IS ?"
    " AND file_size IS ? AND file_mtime IS ?"
    " AND working_copy_path IS NULL"
    " AND (SELECT path FROM folders"
    "        WHERE id = photos.folder_id) IS ?"
)


def row_identity_params(row: Mapping[str, Any]) -> tuple:
    """Parameters for ``ROW_IDENTITY_WHERE``."""
    return (
        row["id"], row["folder_id"], row["filename"],
        row["companion_path"],
        row["file_size"], row["file_mtime"],
        row["folder_path"],
    )


class WorkingCopyRepository:
    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        commit_with_retry: Callable[[sqlite3.Connection], None],
    ) -> None:
        self.conn = conn
        self.commit_with_retry = commit_with_retry

    # -- candidates ----------------------------------------------------------

    def candidate_count(self, where: str, params: Sequence[Any]) -> int:
        """How many photos match the candidate predicate ``where``."""
        return self.conn.execute(
            f"SELECT COUNT(*) FROM photos WHERE {where}", params
        ).fetchone()[0]

    def count_with_working_copy(self) -> int:
        """How many photos have a working copy recorded."""
        return self.conn.execute(
            "SELECT COUNT(*) FROM photos WHERE working_copy_path IS NOT NULL"
        ).fetchone()[0]

    def load_scope(self, scope_rows: Iterable[tuple]) -> str:
        """Load ``(path, mode, like_pattern)`` rows into a TEMP table; return the clause.

        The clause is ANDed onto a query that joins ``folders f``.
        """
        # A snapshot can span thousands of exact directories. Expanding one
        # OR term per directory hits SQLite's default MAX_EXPR_DEPTH=1000.
        # Keep the scope connection-local in a TEMP table instead; executemany
        # also avoids the bound-parameter limit. Commit only the transaction
        # opened here so the subsequent main-WAL read cannot retain a stale
        # snapshot if another job writes before this extractor does.
        started_in_transaction = self.conn.in_transaction
        try:
            self.conn.execute(
                """CREATE TEMP TABLE IF NOT EXISTS working_copy_scope (
                       path TEXT NOT NULL,
                       mode TEXT NOT NULL,
                       like_pattern TEXT,
                       PRIMARY KEY (path, mode)
                   )"""
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS working_copy_scope_mode "
                "ON working_copy_scope (mode)"
            )
            self.conn.execute("DELETE FROM working_copy_scope")
            self.conn.executemany(
                "INSERT OR IGNORE INTO working_copy_scope "
                "(path, mode, like_pattern) VALUES (?, ?, ?)",
                scope_rows,
            )
            if not started_in_transaction and self.conn.in_transaction:
                self.commit_with_retry(self.conn)
        except Exception:
            if not started_in_transaction and self.conn.in_transaction:
                self.conn.rollback()
            raise
        return """AND (
              EXISTS (
                  SELECT 1 FROM working_copy_scope wcs
                   WHERE wcs.path = f.path
              )
              OR EXISTS (
                  SELECT 1 FROM working_copy_scope wcs
                   WHERE wcs.mode = 'subtree'
                     AND f.path LIKE wcs.like_pattern ESCAPE '\\'
              )
           )"""

    def candidate_rows(
        self, candidate_where: str, scope_clause: str, params: Sequence[Any],
    ) -> list[sqlite3.Row]:
        """The rows matching ``candidate_where`` and ``scope_clause``, newest import first."""
        return self.conn.execute(
            f"""
        SELECT p.id, p.folder_id, p.filename, p.companion_path,
               p.working_copy_path,
               p.extension, p.width, p.height, p.exif_data, p.file_mtime,
               p.file_size,
               f.path AS folder_path
          FROM photos p
          JOIN folders f ON f.id = p.folder_id
         WHERE {candidate_where}
           {scope_clause}
         -- Newest import first. A library whose working copies do not all
         -- fit under the quota gets partial coverage no matter what, so the
         -- order decides *which* photos are covered — and unordered means
         -- rowid order, which walks the oldest imports first and leaves the
         -- shoot the user just brought in for last. Import recency (id) is
         -- the right proxy here rather than capture time: an archive of
         -- decade-old files imported today is what is being culled today.
         ORDER BY p.id DESC
        """,
            params,
        ).fetchall()

    def snapshot_row(self, photo_id: int) -> sqlite3.Row | None:
        """The columns a candidate snapshot is revalidated against, or None."""
        return self.conn.execute(
            """
        SELECT p.folder_id, p.filename, p.companion_path, p.file_size,
               p.file_mtime, p.working_copy_path, f.path AS folder_path
          FROM photos p
          JOIN folders f ON f.id = p.folder_id
         WHERE p.id = ?
        """,
            (photo_id,),
        ).fetchone()

    # -- per-row results -----------------------------------------------------

    def set_working_copy(
        self, row: Mapping[str, Any], wc_rel: str, *, raw_failed_then_companion: bool,
    ) -> sqlite3.Cursor:
        """Point the snapshot's row at ``wc_rel``; returns the UPDATE cursor."""
        if raw_failed_then_companion:
            # The companion-derived working copy is usable, but request
            # paths still need to know the RAW itself failed: edited RAW
            # render paths gate companion selection on a present "source"
            # failure marker.
            return self.conn.execute(
                "UPDATE photos SET working_copy_path=?,"
                " working_copy_evicted_mtime=NULL,"
                " working_copy_failed_at=datetime('now'),"
                " working_copy_failed_mtime=?,"
                " working_copy_failed_source='source'"
                + ROW_IDENTITY_WHERE,
                (wc_rel, row["file_mtime"], *row_identity_params(row)),
            )
        return self.conn.execute(
            "UPDATE photos SET working_copy_path=?,"
            " working_copy_evicted_mtime=NULL,"
            " working_copy_failed_at=NULL,"
            " working_copy_failed_mtime=NULL,"
            " working_copy_failed_source=NULL"
            + ROW_IDENTITY_WHERE,
            (wc_rel, *row_identity_params(row)),
        )

    def path_is_claimed(self, wc_rel: str) -> bool:
        """Whether any photo row records ``wc_rel`` as its working copy."""
        return self.conn.execute(
            "SELECT 1 FROM photos WHERE working_copy_path=?"
            " LIMIT 1",
            (wc_rel,),
        ).fetchone() is not None

    def mark_failed(self, row: Mapping[str, Any], failure_source: str) -> None:
        """Stamp the snapshot's row with a failure marker for its current mtime."""
        self.conn.execute(
            "UPDATE photos SET working_copy_failed_at=datetime('now'),"
            " working_copy_failed_mtime=?,"
            " working_copy_failed_source=?"
            + ROW_IDENTITY_WHERE,
            (
                row["file_mtime"], failure_source,
                *row_identity_params(row),
            ),
        )

    def mark_raw_decode_failed(self, photo_id: int, file_mtime: Any) -> None:
        """Record that the photo's RAW source failed to decode at ``file_mtime``.

        The render paths (the thumbnail fallbacks, ``render_source``) stamp it
        so the next request fails fast instead of repeating the decode until
        the source changes. The caller commits.
        """
        self.conn.execute(
            "UPDATE photos SET"
            " working_copy_failed_at=datetime('now'),"
            " working_copy_failed_mtime=?,"
            " working_copy_failed_source='source'"
            " WHERE id=?",
            (file_mtime, photo_id),
        )

    def reset(self, photo_id: int) -> None:
        """Forget the photo's working copy and any eviction or failure marker."""
        self.conn.execute(
            "UPDATE photos SET working_copy_path = NULL,"
            " working_copy_evicted_mtime = NULL,"
            " working_copy_failed_at = NULL,"
            " working_copy_failed_mtime = NULL,"
            " working_copy_failed_source = NULL"
            " WHERE id = ?",
            (photo_id,),
        )

    # -- capacity deferral ---------------------------------------------------

    def defer(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Mark the snapshots' rows capacity-deferred at their snapshot mtime."""
        self.conn.executemany(
            "UPDATE photos SET"
            " working_copy_evicted_mtime=COALESCE(?, -1)"
            + ROW_IDENTITY_WHERE,
            [(row["file_mtime"], *row_identity_params(row)) for row in rows],
        )

    def undo_deferral(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Clear the deferred markers ``defer`` wrote for these snapshots."""
        deferred_marker_rows = [
            (
                row["id"], row["folder_id"], row["filename"],
                row["companion_path"],
                row["file_size"], row["file_mtime"],
                row["file_mtime"],
                row["folder_path"],
            )
            for row in rows
        ]
        self.conn.executemany(
            "UPDATE photos SET working_copy_evicted_mtime=NULL "
            "WHERE id=? AND folder_id=? AND filename=?"
            " AND companion_path IS ?"
            " AND file_size IS ? AND file_mtime IS ?"
            " AND working_copy_path IS NULL"
            " AND working_copy_evicted_mtime IS COALESCE(?, -1)"
            " AND (SELECT path FROM folders"
            "        WHERE id = photos.folder_id) IS ?",
            deferred_marker_rows,
        )

    def defer_trimmed(
        self, trimmed: Iterable[tuple[int, Mapping[str, Any] | None]],
    ) -> None:
        """Clear trimmed rows' working copy and mark them deferred.

        ``trimmed`` holds ``(photo_id, snapshot)`` pairs; a pair without a
        snapshot or a usable id is skipped.
        """
        deferred_rows = [
            (
                snapshot["file_mtime"]
                if snapshot["file_mtime"] is not None
                else -1,
                photo_id,
                f"working/{photo_id}.jpg",
                snapshot["folder_id"],
                snapshot["filename"],
                snapshot["companion_path"],
                snapshot["file_size"],
                snapshot["file_mtime"],
                snapshot["folder_path"],
            )
            for photo_id, snapshot in trimmed
            if photo_id >= 0 and snapshot is not None
        ]
        if deferred_rows:
            self.conn.executemany(
                "UPDATE photos SET working_copy_path=NULL,"
                " working_copy_evicted_mtime=?"
                " WHERE id=? AND working_copy_path IS ?"
                " AND folder_id=? AND filename=?"
                " AND companion_path IS ?"
                " AND file_size IS ? AND file_mtime IS ?"
                " AND (SELECT path FROM folders"
                "        WHERE id = photos.folder_id) IS ?",
                deferred_rows,
            )
