"""The one place production code deletes ``photos`` rows.

Collections hold static membership as ``photo_ids`` lists inside their rules
JSON, so no foreign key drops a member when its photo goes away. ``photos``
has no AUTOINCREMENT either: SQLite gives the next insert the highest id in
the table plus one, so a freed id at the top is handed out again. An id left
behind in a collection would make whichever photo later takes it join that
collection. Every path that removes a ``photos`` row therefore deletes it
through ``photo_row_deletion``, which rewrites every workspace's collections
for the rows it removed before the block exits.
``test_every_photo_row_delete_goes_through_photo_row_deletion`` fails on a
``DELETE FROM photos`` anywhere else.

The caller still owns everything else a delete needs: moving or dropping the
row's non-cascading dependents first, the transaction, and the commit. A
caller that merges a row into a survivor maps it to the survivor so the
collections follow the photo; a plain delete maps it to None.
"""

import contextlib
import logging

from repositories.collections import remap_collection_photo_ids
from sql_chunks import chunked

log = logging.getLogger(__name__)


class PhotoRowDeletion:
    """Deletes ``photos`` rows and remembers where their collection entries go."""

    def __init__(self, conn):
        self.conn = conn
        self._collection_remap = {}

    def delete(self, mapping):
        """Delete each photo row ``mapping`` names, in the caller's transaction.

        ``mapping`` maps a photo id to the id that absorbed it, or to None
        when the photo is simply gone. The collections are rewritten once,
        when the ``photo_row_deletion`` block exits: the rewrite parses every
        collection's rules, so doing it per row would make a loop of N merges
        cost N passes over every collection.
        """
        ids = list(dict.fromkeys(mapping))
        for chunk in chunked(ids):
            marks = ",".join("?" for _ in chunk)
            self.conn.execute(f"DELETE FROM photos WHERE id IN ({marks})", chunk)
        self._collection_remap.update(mapping)

    def _rewrite_collections(self):
        remap, self._collection_remap = self._collection_remap, {}
        return remap_collection_photo_ids(self.conn, remap)


@contextlib.contextmanager
def photo_row_deletion(conn):
    """Yield a ``PhotoRowDeletion``; rewrite collections when the block exits.

    The rewrite joins the caller's transaction and never commits. It also
    runs when the block raises: a caller that rolls back undoes it with the
    deletes, and one that commits the rows it did delete must not leave them
    in collections. A failure of that rewrite is logged rather than raised,
    so it cannot hide the error that ended the block.
    """
    deletion = PhotoRowDeletion(conn)
    try:
        yield deletion
    except BaseException:
        try:
            deletion._rewrite_collections()
        except Exception:
            log.exception(
                "Could not remove deleted photos from collections after an error"
            )
        raise
    deletion._rewrite_collections()
