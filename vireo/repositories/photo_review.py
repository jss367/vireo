"""Persistence for workspace-scoped photo ratings and flags.

Ratings (0-5) and flags (``'none'`` / ``'flagged'`` / ``'rejected'``) live on
``photos`` rows, but the workspace check runs against
``photo_workspace_visibility`` so a photo the active workspace cannot see
cannot be rated or flagged through the ``verify_workspace=True`` writes.
``Database`` builds the repository with ``self._active_workspace_id`` — not
``self._ws_id()`` — so ``_photo_review_repository()`` (and the
``db.photo_review`` accessor) never raises off the active workspace; the
``verify_workspace=True`` writes and ``wildlife_excluded_states`` still raise
if the workspace is unset. No write commits: the route, job or service
that calls them owns the unit of work (``db.transaction()``).

Callers reach it as ``db.photo_review`` (a fresh repository per access, see
``Database.photo_review``); there are no forwarding wrappers on ``Database``.
The wildlife-exclusion toggle (``Database.update_photo_wildlife_excluded``)
is a different column whose workspace check stays on ``Database``; its write
(``set_wildlife_excluded``) lives here but is reached only through that method.
"""

import json
import sqlite3
from collections.abc import Collection, Iterable, Iterator


class PhotoReviewRepository:
    def __init__(
        self, conn: sqlite3.Connection, workspace_id: int | None, *, chunk_size: int = 800,
    ) -> None:
        self.conn = conn
        self.workspace_id = workspace_id
        self.chunk_size = chunk_size

    def set_rating(
        self, photo_id: int, rating: int, *, verify_workspace: bool = True,
    ) -> None:
        """Set a photo's rating (0-5); the caller's unit of work commits.

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
        self, photo_id: int, flag: str, *, verify_workspace: bool = True,
    ) -> None:
        """Set a photo's flag (``'none'``, ``'flagged'``, ``'rejected'``).

        ``verify_workspace`` works as in ``set_rating``. No commit: the
        caller's unit of work owns it.
        """
        if verify_workspace:
            self._verify_photo(photo_id)
        self.conn.execute(
            "UPDATE photos SET flag = ? WHERE id = ?", (flag, photo_id)
        )

    def set_wildlife_excluded(self, photo_id: int, excluded: bool) -> None:
        self.conn.execute(
            "UPDATE photos SET wildlife_excluded = ? WHERE id = ?",
            (1 if excluded else 0, photo_id),
        )

    def flag_ranked_candidate(self, photo_id: int) -> bool:
        """Record a provisional winner without overwriting any existing choice.

        The condition and write are one statement so a concurrent photographer
        decision cannot be replaced between a separate read and update. The
        ordinary invalidation trigger leaves this ranking write unattributed.
        """
        if self.workspace_id is None:
            raise RuntimeError("No active workspace set")
        changed = self.conn.execute(
            """UPDATE photos SET flag='flagged' WHERE id=? AND (flag='none' OR flag IS NULL)
               AND EXISTS (SELECT 1 FROM photo_workspace_visibility
                           WHERE photo_id=photos.id AND workspace_id=?)
               AND NOT EXISTS (SELECT 1 FROM current_flag_attribution a
                               WHERE a.photo_id=photos.id AND a.flag IS photos.flag
                                 AND a.source IS NULL AND a.reason IS NULL)""",
            (photo_id, self.workspace_id),
        ).rowcount
        return bool(changed)

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
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            self.conn.execute(
                f"UPDATE photos SET {column} = ? WHERE id IN ({placeholders})",
                [value, *chunk],
            )

    def set_current_flag_note(self, photo_ids, flag, description, *, source=None, reason=None):
        """Publish only inside the flag writer's unit of work, with no history dependency."""
        if self.workspace_id is None:
            raise RuntimeError("No active workspace set")
        if not self.conn.in_transaction:
            raise RuntimeError("Current flag attribution requires the flag write transaction")
        self.conn.executemany(
            """INSERT OR REPLACE INTO current_flag_attribution
               (photo_id, workspace_id, flag, source, reason, description)
               SELECT id, ?, flag, ?, ?, ? FROM photos WHERE id=? AND flag IS ?""",
            [(self.workspace_id, source, reason, description, pid, flag) for pid in photo_ids],
        )

    def current_flag_note(self, photo_id):
        """Current visible note; manual confirmations do not impersonate agent notes."""
        if self.workspace_id is None:
            raise RuntimeError("No active workspace set")
        row = self.conn.execute(
            """SELECT a.flag, a.source, a.reason, a.description, a.created_at AS at
               FROM current_flag_attribution a JOIN photos p ON p.id=a.photo_id
               WHERE a.photo_id=? AND a.workspace_id=? AND a.flag IS p.flag
                 AND (a.source IS NOT NULL OR a.reason IS NOT NULL)""",
            (photo_id, self.workspace_id),
        ).fetchone()
        return dict(row) if row else None

    def culling_flag_states(self, photo_ids):
        """Read flag and its current confirmation together, within visible scope.

        The attribution trigger invalidates every flag write. Only an explicit
        review write subsequently publishes attribution; a missing legacy row
        is unknown, and an attributed automated source is not a photographer.
        Flags are global, so a real manual decision remains valid when its photo
        is visible in another workspace. No catalog flag is changed here.
        """
        if self.workspace_id is None:
            raise RuntimeError("No active workspace set")
        result = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT p.id, p.flag,
                    CASE WHEN a.photo_id IS NOT NULL
                              AND a.source IS NULL AND a.reason IS NULL
                         THEN p.flag END AS confirmed_flag
                    FROM photos p
                    JOIN photo_workspace_visibility v ON v.photo_id=p.id AND v.workspace_id=?
                    LEFT JOIN current_flag_attribution a ON a.photo_id=p.id AND a.flag IS p.flag
                    WHERE p.id IN ({placeholders})""", [self.workspace_id, *chunk],
            )
            result.update({row["id"]: dict(row) for row in rows})
        return result

    def flag_attributions(self, photo_ids):
        """Capture exact current attribution before a reversible explicit write."""
        result = {}
        for chunk in self._chunks(photo_ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = self.conn.execute(
                f"""SELECT a.* FROM current_flag_attribution a
                    JOIN photos p ON p.id=a.photo_id AND p.flag IS a.flag
                    WHERE a.photo_id IN ({placeholders})""", chunk,
            )
            result.update({row["photo_id"]: dict(row) for row in rows})
        return result

    def confirm_duplicate_choices(self, winner_id, loser_ids):
        """Publish an explicit duplicate choice inside its metadata transaction.

        Keeping a copy preserves its flag; it does not turn an unflagged copy
        into a flagged keeper. An explicit clear is still a confirmed decision.
        Duplicate resolution has no flag-only Undo entry: its metadata merge
        is not reversible independently of these choices.
        """
        row = self.conn.execute("SELECT flag FROM photos WHERE id=?", (winner_id,)).fetchone()
        if row is None:
            raise ValueError("Duplicate winner no longer exists")
        flag = row[0] or "none"
        if row[0] is None:
            self.set_flag(winner_id, flag, verify_workspace=False)
        self.set_current_flag_note([winner_id], flag, "Kept this duplicate copy")
        self.set_current_flag_note(loser_ids, "rejected", "Rejected duplicate copies by user choice")

    def transfer_flag_for_merge(self, source_id, surviving_id, flag):
        """Carry only the winning queue photo's matching current attribution.

        Capture before the flag UPDATE invalidates attribution, including when
        the survivor itself wins. Unknown or stale evidence stays unknown.
        The merge owns the transaction and the losing photo's deletion.
        """
        state = self.flag_attributions([source_id]).get(source_id)
        self.conn.execute("UPDATE photos SET flag = ? WHERE id = ?", (flag, surviving_id))
        if state is not None and state["flag"] == flag:
            self.conn.execute(
                """INSERT INTO current_flag_attribution
                   (photo_id,workspace_id,flag,source,reason,description,created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (surviving_id, state["workspace_id"], flag, state["source"],
                 state["reason"], state["description"], state["created_at"]),
            )

    def record_flag_attribution_history(self, edit_id, photo_ids, before, after):
        """Keep replay metadata with its edit; history pruning cascades it away."""
        if not self.conn.in_transaction:
            raise RuntimeError("Flag replay metadata requires the flag write transaction")
        self.conn.executemany(
            "INSERT INTO flag_attribution_history VALUES(?,?,?,?)",
            [(edit_id, pid, json.dumps(before[pid]) if pid in before else None,
              json.dumps(after[pid]) if pid in after else None) for pid in photo_ids],
        )

    def restore_flag_attribution_history(self, edit_id, photo_id, *, undo):
        """Restore only captured metadata; legacy replay never manufactures it."""
        field = "old_state" if undo else "new_state"
        row = self.conn.execute(
            f"SELECT {field} FROM flag_attribution_history WHERE edit_id=? AND photo_id=?",
            (edit_id, photo_id),
        ).fetchone()
        if row is None or row[0] is None:
            return
        state = json.loads(row[0])
        if state["photo_id"] != photo_id:
            raise ValueError("Flag attribution belongs to a different photo")
        self.conn.execute(
            """INSERT INTO current_flag_attribution
               (photo_id,workspace_id,flag,source,reason,description,created_at)
               SELECT p.id,(SELECT id FROM workspaces WHERE id=?),p.flag,?,?,?,? FROM photos p
               WHERE p.id=? AND p.flag IS ?""",
            (state["workspace_id"], state["source"], state["reason"], state["description"],
             state["created_at"], photo_id, state["flag"]),
        )

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
