"""Persistence for workspace-scoped photo ratings and flags.

Ratings (0-5) and flags (``'none'`` / ``'flagged'`` / ``'rejected'``) live on
``photos`` rows, but the workspace check runs against
``photo_workspace_visibility`` so a photo the active workspace cannot see
cannot be rated or flagged through the ``verify_workspace=True`` writes.
``Database`` builds the repository with ``self._active_workspace_id`` — not
``self._ws_id()`` — so ``_photo_review_repository()`` (and the
``db.photo_review`` accessor) never raises off the active workspace; the
``verify_workspace=True`` writes and ``wildlife_excluded_states`` still raise
if the workspace is unset. ``_commit=False`` on ``set_flag`` is carried
through unchanged for callers that already hold ``BEGIN IMMEDIATE`` (the
prediction-decision lock), so the writer lock is not released mid-decision.

Callers reach it as ``db.photo_review`` (a fresh repository per access, see
``Database.photo_review``); there are no forwarding wrappers on ``Database``.
The wildlife-exclusion toggle (``Database.update_photo_wildlife_excluded``)
is a different column whose workspace check stays on ``Database``; its write
(``set_wildlife_excluded``, ``_commit=False`` for a batch that records one
edit and commits once) lives here but is reached only through that method.
"""

import sqlite3
from collections.abc import Collection, Iterable, Iterator


class PhotoReviewRepository:
    def __init__(
        self, conn: sqlite3.Connection, workspace_id: int | None, *, chunk_size: int = 800,
    ) -> None:
        self.conn = conn
        self.workspace_id = workspace_id
        self.chunk_size = chunk_size

    def set_rating(self, photo_id: int, rating: int, *, verify_workspace: bool = True) -> None:
        """Set a photo's rating (0-5) and commit.

        ``verify_workspace=True`` (the default) raises ``ValueError`` if the
        photo is not visible in the active workspace (``RuntimeError`` when
        none is set). Pass False from background jobs that already scope
        their photo lists, or from undo/redo where the edit history is
        already workspace-scoped.
        """
        if verify_workspace:
            self._verify_photo(photo_id)
        self.conn.execute(
            "UPDATE photos SET rating = ? WHERE id = ?", (rating, photo_id)
        )
        self.conn.commit()

    def set_ratings(
        self, photo_ids: Collection[int], rating: int, *, verify_workspace: bool = True,
    ) -> None:
        """Set the rating of several photos in one transaction.

        ``verify_workspace=True`` checks every photo first and raises
        ``ValueError`` before writing if any is outside the active workspace.
        """
        self._set_many(
            photo_ids,
            "rating",
            rating,
            verify_workspace=verify_workspace,
        )

    def set_flag(
        self, photo_id: int, flag: str, *, verify_workspace: bool = True, _commit: bool = True,
    ) -> None:
        """Set a photo's flag (``'none'``, ``'flagged'``, ``'rejected'``).

        ``verify_workspace`` works as in ``set_rating``. ``_commit=False``
        skips the commit (the caller owns the transaction). Callers that
        hold ``BEGIN IMMEDIATE`` — the prediction decision lock, for
        example — must pass False so the writer lock is not released
        mid-decision.
        """
        if verify_workspace:
            self._verify_photo(photo_id)
        self.conn.execute(
            "UPDATE photos SET flag = ? WHERE id = ?", (flag, photo_id)
        )
        if _commit:
            self.conn.commit()

    def set_wildlife_excluded(self, photo_id: int, excluded: bool, *, _commit: bool = True) -> None:
        self.conn.execute(
            "UPDATE photos SET wildlife_excluded = ? WHERE id = ?",
            (1 if excluded else 0, photo_id),
        )
        if _commit:
            self.conn.commit()

    def wildlife_excluded_states(self, photo_ids: Iterable[int]) -> dict[int, int]:
        """``{photo_id: 0 or 1}`` for the named photos the active workspace can see.

        Ids that don't exist or sit outside the workspace are absent. Raises
        ``RuntimeError`` when no workspace is active.
        """
        if self.workspace_id is None:
            raise RuntimeError("No active workspace set")
        states = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT p.id, COALESCE(p.wildlife_excluded, 0) AS excluded
                    FROM photos p
                    WHERE p.id IN ({placeholders})
                      AND EXISTS (
                          SELECT 1 FROM photo_workspace_visibility wf
                          WHERE wf.photo_id = p.id
                            AND wf.workspace_id = ?
                      )""",
                [*chunk, self.workspace_id],
            ).fetchall()
            for row in rows:
                states[row["id"]] = int(row["excluded"])
        return states

    def set_flags(
        self, photo_ids: Collection[int], flag: str, *, verify_workspace: bool = True,
    ) -> None:
        """Set the flag of several photos in one transaction; see ``set_ratings``."""
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
            "JOIN photo_workspace_visibility wf ON wf.photo_id = p.id "
            "WHERE p.id = ? AND wf.workspace_id = ?",
            (photo_id, self.workspace_id),
        ).fetchone()
        if row is None:
            raise ValueError(
                f"Photo {photo_id} does not belong to the active workspace"
            )

    def _chunks(self, values: Iterable[int]) -> Iterator[list[int]]:
        values = list(values)
        return (
            values[index:index + self.chunk_size]
            for index in range(0, len(values), self.chunk_size)
        )
