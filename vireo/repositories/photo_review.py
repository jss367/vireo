"""Persistence for workspace-scoped photo ratings and flags.

Ratings (0-5) and flags (``'none'`` / ``'flagged'`` / ``'rejected'``) live on
``photos`` rows, but the workspace check runs against ``workspace_folders``
so a photo whose folder is not linked to the active workspace cannot be
rated or flagged from the façade's write paths. ``Database`` builds the
repository with ``self._active_workspace_id`` — not ``self._ws_id()`` — so
``_photo_review_repository()`` never raises off the active workspace; the
``verify_workspace=True`` writes still raise if the workspace is unset,
because ``_verify_photo`` demands one. ``Database`` keeps the wrappers
(``update_photo_rating``, ``batch_update_photo_rating``,
``update_photo_flag``, ``batch_update_photo_flag``) as one-line delegations
and calls in here for the SQL. ``_commit=False`` on ``set_flag`` is
carried through unchanged for callers that already hold ``BEGIN IMMEDIATE``
(the prediction-decision lock), so the writer lock is not released
mid-decision. The wildlife-exclusion toggle (``update_photo_wildlife_excluded``)
is a different column with its own workspace check and stays on
``Database``.
"""


class PhotoReviewRepository:
    def __init__(self, conn, workspace_id, *, chunk_size=800):
        self.conn = conn
        self.workspace_id = workspace_id
        self.chunk_size = chunk_size

    def set_rating(self, photo_id, rating, *, verify_workspace=True):
        if verify_workspace:
            self._verify_photo(photo_id)
        self.conn.execute(
            "UPDATE photos SET rating = ? WHERE id = ?", (rating, photo_id)
        )
        self.conn.commit()

    def set_ratings(self, photo_ids, rating, *, verify_workspace=True):
        self._set_many(
            photo_ids,
            "rating",
            rating,
            verify_workspace=verify_workspace,
        )

    def set_flag(self, photo_id, flag, *, verify_workspace=True, _commit=True):
        if verify_workspace:
            self._verify_photo(photo_id)
        self.conn.execute(
            "UPDATE photos SET flag = ? WHERE id = ?", (flag, photo_id)
        )
        if _commit:
            self.conn.commit()

    def set_flags(self, photo_ids, flag, *, verify_workspace=True):
        self._set_many(
            photo_ids,
            "flag",
            flag,
            verify_workspace=verify_workspace,
        )

    def _set_many(self, photo_ids, column, value, *, verify_workspace):
        if not photo_ids:
            return
        if verify_workspace:
            for photo_id in photo_ids:
                self._verify_photo(photo_id)
        try:
            for chunk in self._chunks(photo_ids):
                placeholders = ",".join("?" for _ in chunk)
                self.conn.execute(
                    f"UPDATE photos SET {column} = ? WHERE id IN ({placeholders})",
                    [value, *chunk],
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _verify_photo(self, photo_id):
        if self.workspace_id is None:
            raise RuntimeError("No active workspace set")
        row = self.conn.execute(
            "SELECT 1 FROM photos p "
            "JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
            "WHERE p.id = ? AND wf.workspace_id = ?",
            (photo_id, self.workspace_id),
        ).fetchone()
        if row is None:
            raise ValueError(
                f"Photo {photo_id} does not belong to the active workspace"
            )

    def _chunks(self, values):
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
