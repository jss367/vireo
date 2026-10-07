"""Persistence for pending NAS transfers (``pending_archives``).

A row is an import's retained local originals waiting for the user to send
them to the NAS (see the top-level ``pending_archives`` module for the
transfer itself). Rows belong to the workspace that imported them, so the
listing and the discard are scoped to ``self.workspace_id``, which the façade
resolves with ``Database._ws_id()`` when it builds the repository. A transfer
id is a globally unique text key, and the send job flips its state by id
alone, so ``set_state`` takes no workspace.

``Database`` keeps the wrappers (``get_open_pending_archives``,
``delete_pending_archive``, ``set_pending_archive_state``). Registering a
transfer and reading one by id still live in the ``pending_archives`` module.
"""


class PendingArchiveRepository:
    def __init__(self, conn, workspace_id=None):
        self.conn = conn
        self.workspace_id = workspace_id

    def open_with_review_collection(self):
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
            (self.workspace_id,),
        ).fetchall()

    def delete(self, archive_id):
        """Forget one of the workspace's transfers (the row only) and commit."""
        self.conn.execute(
            "DELETE FROM pending_archives WHERE id = ? AND workspace_id = ?",
            (archive_id, self.workspace_id),
        )
        self.conn.commit()

    def set_state(self, archive_id, state, error=""):
        """Set a transfer's ``state`` and ``error`` by id and commit."""
        self.conn.execute(
            "UPDATE pending_archives SET state = ?, error = ? WHERE id = ?",
            (state, error, archive_id),
        )
        self.conn.commit()
