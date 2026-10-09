"""Persistence for pending NAS transfers (``pending_archives``).

A row is an import's retained local originals waiting for the user to send
them to the NAS (see the top-level ``pending_archives`` module for the
transfer itself). Rows belong to the workspace that imported them, so the
listing and the discard are scoped to the active workspace, which they
resolve through ``workspace_id_fn`` (``Database._ws_id``) before running any
SQL; building the repository never resolves it, and with no workspace active
both raise ``RuntimeError``. A transfer id is a globally unique text key, and
the send job flips its state by id alone, so ``set_state`` never resolves a
workspace and works with none active.

Callers reach it as ``db.pending_archives`` (a fresh repository per access,
see ``Database.pending_archives``); there are no forwarding wrappers on
``Database``. Registering a transfer and reading one by id still live in the
``pending_archives`` module.
"""

import sqlite3
from collections.abc import Callable


class PendingArchiveRepository:
    def __init__(self, conn: sqlite3.Connection,
                 workspace_id_fn: Callable[[], int] | None = None) -> None:
        self.conn = conn
        self.workspace_id_fn = workspace_id_fn

    def open_with_review_collection(self) -> list[sqlite3.Row]:
        """The workspace's transfers not yet ``complete``, oldest first.

        Each row is every ``pending_archives`` column plus
        ``review_collection_id`` and ``collection_name`` from the import's
        review collection, both NULL when that collection is gone or belongs
        to another workspace.
        """
        return self.conn.execute(
            "SELECT a.*, c.id AS review_collection_id, c.name AS collection_name FROM pending_archives a "
            "LEFT JOIN collections c ON c.id = a.collection_id AND c.workspace_id = a.workspace_id "
            "WHERE a.workspace_id = ? AND a.state != 'complete' ORDER BY a.created_at",
            (self.workspace_id_fn(),),
        ).fetchall()

    def delete(self, archive_id: str) -> None:
        """Forget one of the workspace's transfers (the row only) and commit."""
        self.conn.execute(
            "DELETE FROM pending_archives WHERE id = ? AND workspace_id = ?",
            (archive_id, self.workspace_id_fn()),
        )
        self.conn.commit()

    def set_state(self, archive_id: str, state: str, error: str = "") -> None:
        """Set a transfer's ``state`` and ``error`` by id, in any workspace, and commit."""
        self.conn.execute(
            "UPDATE pending_archives SET state = ?, error = ? WHERE id = ?",
            (state, error, archive_id),
        )
        self.conn.commit()

    def attach_collection(self, col_id: int | None, pending_archive_id: str) -> None:
        self.conn.execute(
            "UPDATE pending_archives SET collection_id = COALESCE(?, collection_id) WHERE id = ?",
            (col_id, pending_archive_id),
        )

    def completed_row(self, pending_archive_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT 1 FROM pending_archives WHERE id = ? AND state = 'complete'", (pending_archive_id,),
        ).fetchone()
