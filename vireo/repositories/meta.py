"""Persistence for the ``db_meta`` key/value table.

``db_meta`` holds catalog-wide markers (one-shot migration flags, version
stamps), so the repository takes no workspace id. ``Database`` keeps its
``get_meta`` / ``set_meta`` methods as thin wrappers over this class.
"""


class MetaRepository:
    def __init__(self, conn):
        self.conn = conn

    def get(self, key):
        """Return the db_meta value for `key`, or None if unset."""
        row = self.conn.execute(
            "SELECT value FROM db_meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set(self, key, value, _commit=True):
        """Upsert a db_meta row."""
        self.conn.execute(
            "INSERT INTO db_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )
        if _commit:
            self.conn.commit()
