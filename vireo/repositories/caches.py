"""Persistence for the on-disk caches: the preview LRU and offline originals.

The caches are catalog-wide (keyed by photo id, not by workspace), so the
repository takes no workspace id. The lock-retry helpers
(``execute_with_retry`` / ``commit_with_retry``) live in ``db``; the façade
passes them in so this module imports no ``db`` code and a monkeypatch of
``db.commit_with_retry`` still reaches the offline-original writes.
"""


class CachesRepository:
    def __init__(self, conn, *, execute_with_retry, commit_with_retry):
        self.conn = conn
        self.execute_with_retry = execute_with_retry
        self.commit_with_retry = commit_with_retry

    # -- preview_cache LRU ---------------------------------------------------

    def preview_insert(self, photo_id, size, bytes_):
        """Insert or replace a preview_cache entry. last_access_at = now()."""
        import time
        self.conn.execute(
            "INSERT OR REPLACE INTO preview_cache "
            "(photo_id, size, bytes, last_access_at) VALUES (?, ?, ?, ?)",
            (photo_id, size, bytes_, time.time()),
        )
        self.conn.commit()

    def preview_touch(self, photo_id, size):
        """Update last_access_at for an existing entry. No-op if missing."""
        import time
        self.conn.execute(
            "UPDATE preview_cache SET last_access_at=? WHERE photo_id=? AND size=?",
            (time.time(), photo_id, size),
        )
        self.conn.commit()

    def preview_delete(self, photo_id, size):
        """Delete a preview_cache entry (caller removes the file)."""
        self.conn.execute(
            "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
            (photo_id, size),
        )
        self.conn.commit()

    def preview_total_bytes(self):
        """Return total bytes tracked across ordinary and paired previews."""
        row = self.conn.execute(
            "SELECT (SELECT COALESCE(SUM(bytes), 0) FROM preview_cache) + "
            "(SELECT COALESCE(SUM(bytes), 0) FROM paired_preview_cache) AS total"
        ).fetchone()
        return row["total"]

    def preview_oldest_first(self):
        """Return all rows ordered by last_access_at ascending (oldest first)."""
        return self.conn.execute(
            "SELECT photo_id, size, bytes, last_access_at FROM preview_cache "
            "ORDER BY last_access_at ASC"
        ).fetchall()

    def preview_get(self, photo_id, size):
        """Return the row for (photo_id, size), or None."""
        return self.conn.execute(
            "SELECT photo_id, size, bytes, last_access_at FROM preview_cache "
            "WHERE photo_id=? AND size=?",
            (photo_id, size),
        ).fetchone()

    def paired_preview_insert(self, photo_id, filename, bytes_):
        """Join the publisher's transaction; the filename includes source state."""
        import time
        self.conn.execute(
            "INSERT OR REPLACE INTO paired_preview_cache "
            "(filename, photo_id, bytes, last_access_at) VALUES (?, ?, ?, ?)",
            (filename, photo_id, bytes_, time.time()),
        )

    def paired_preview_get(self, filename):
        return self.conn.execute(
            "SELECT * FROM paired_preview_cache WHERE filename=?", (filename,),
        ).fetchone()

    def paired_preview_touch(self, filename):
        import time
        self.conn.execute(
            "UPDATE paired_preview_cache SET last_access_at=? WHERE filename=?",
            (time.time(), filename),
        )
        self.conn.commit()

    def paired_preview_oldest_first(self):
        return self.conn.execute(
            "SELECT * FROM paired_preview_cache ORDER BY last_access_at",
        ).fetchall()

    # -- offline original cache ----------------------------------------------

    def offline_original_upsert(
        self,
        photo_id,
        original_path,
        xmp_path,
        companion_path,
        bytes_,
        source_size,
        source_mtime,
        cached_at,
        status,
        error=None,
    ):
        self.execute_with_retry(
            self.conn,
            """INSERT OR REPLACE INTO offline_originals
               (photo_id, original_path, xmp_path, companion_path, bytes,
                source_size, source_mtime, cached_at, status, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                photo_id,
                original_path,
                xmp_path,
                companion_path,
                bytes_,
                source_size,
                source_mtime,
                cached_at,
                status,
                error,
            ),
        )
        self.commit_with_retry(self.conn)

    def offline_original_get(self, photo_id):
        return self.conn.execute(
            """SELECT photo_id, original_path, xmp_path, companion_path, bytes,
                      source_size, source_mtime, cached_at, status, error
               FROM offline_originals WHERE photo_id=?""",
            (photo_id,),
        ).fetchone()

    def offline_original_delete(self, photo_id):
        self.execute_with_retry(
            self.conn,
            "DELETE FROM offline_originals WHERE photo_id=?",
            (photo_id,),
        )
        self.commit_with_retry(self.conn)

    def offline_original_total_bytes(self):
        row = self.conn.execute(
            "SELECT COALESCE(SUM(bytes), 0) AS total FROM offline_originals "
            "WHERE status='cached'"
        ).fetchone()
        return row["total"]
