"""Persistence for iNaturalist submissions.

Submissions are catalog-wide (keyed by photo, not workspace), so the
repository takes no workspace id. ``Database`` keeps its
``record_inat_submission`` / ``get_inat_submissions`` methods as thin
wrappers over this class.
"""


class InatRepository:
    def __init__(self, conn, *, chunk_size=800):
        self.conn = conn
        self.chunk_size = chunk_size

    def record_submission(self, photo_id, observation_id, observation_url):
        """Record a successful iNaturalist submission."""
        self.conn.execute(
            """INSERT OR IGNORE INTO inat_submissions
               (photo_id, observation_id, observation_url)
               VALUES (?, ?, ?)""",
            (photo_id, observation_id, observation_url),
        )
        self.conn.commit()

    def get_submissions(self, photo_ids):
        """Return {photo_id: {observation_id, observation_url, submitted_at}} for given IDs."""
        if not photo_ids:
            return {}
        result = {}
        for chunk in self._chunks(list(dict.fromkeys(photo_ids))):
            placeholders = ",".join("?" * len(chunk))
            rows = self.conn.execute(
                f"SELECT photo_id, observation_id, observation_url, submitted_at"
                f" FROM inat_submissions WHERE photo_id IN ({placeholders})"
                f" ORDER BY submitted_at DESC, id DESC",
                list(chunk),
            ).fetchall()
            # Rows arrive newest-first; keep the first seen per photo so each
            # photo maps to its most recent submission (a dict comprehension
            # here would let older rows overwrite newer ones).
            for r in rows:
                if r["photo_id"] not in result:
                    result[r["photo_id"]] = dict(r)
        return result

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
