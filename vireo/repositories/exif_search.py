"""Backfill of ``photo_exif_search_text``, metadata search's EXIF prefilter.

Triggers on ``photos`` (``metadata_search.exif_search_text_triggers``) keep a
row current for every EXIF write; this repository only fills in photos
written before the table existed or before its definition last changed.
Catalog-wide, so it takes no workspace id. ``Database`` keeps
``count_exif_search_unindexed`` / ``index_exif_search_batch`` as thin
wrappers over this class.
"""

from metadata_search import EXIF_SEARCH_TEXT_TABLE, exif_search_text

_UNINDEXED = (
    f"NOT EXISTS (SELECT 1 FROM {EXIF_SEARCH_TEXT_TABLE} search_text "
    "WHERE search_text.photo_id = p.id)"
)


class ExifSearchRepository:
    def __init__(self, conn, commit_with_retry):
        self.conn = conn
        self._commit_with_retry = commit_with_retry

    def count_unindexed(self):
        """Photos with no search text row yet."""
        return self.conn.execute(
            f"SELECT COUNT(*) FROM photos p WHERE {_UNINDEXED}"
        ).fetchone()[0]

    def index_batch(self, after_id, limit):
        """Index up to ``limit`` unindexed photos with ids above ``after_id``.

        Returns ``(last_id, indexed)``, or ``None`` once no photo is left.
        Each batch commits on its own so scans writing EXIF are never held
        behind the whole pass. A row a trigger wrote in the meantime is newer
        than this read, so it is kept.
        """
        ids = [row[0] for row in self.conn.execute(
            f"SELECT p.id FROM photos p WHERE p.id > ? AND {_UNINDEXED} "
            "ORDER BY p.id LIMIT ?",
            (after_id, limit),
        )]
        if not ids:
            return None
        cursor = self.conn.execute(
            f"INSERT OR IGNORE INTO {EXIF_SEARCH_TEXT_TABLE} (photo_id, value_text) "
            f"SELECT p.id, {exif_search_text('p.exif_data')} FROM photos p "
            "WHERE p.id BETWEEN ? AND ?",
            (ids[0], ids[-1]),
        )
        self._commit_with_retry(self.conn)
        return ids[-1], cursor.rowcount
