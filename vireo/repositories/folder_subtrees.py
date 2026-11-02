"""Folder subtrees as one workspace sees them.

``folder_subtree_ids`` is the read every folder-scoped query composes: the
Browse listings (``PhotoRepository``), the collection rules
(``CollectionRepository``), the map reads (``LocationRepository``), the
Highlights candidates (``SpeciesCurationRepository``) and
``FolderRepository.subtree_ids`` itself. It lives on its own so those
repositories import it without importing the folder repository.
"""

import sqlite3


def folder_subtree_ids(
    conn: sqlite3.Connection, workspace_id: int, folder_id: int,
) -> list[int]:
    """Return the seed plus visible descendants, skipping hidden ancestors.

    The root is always included as-is so callers' own workspace filter on
    photos still applies. The folder tree reparents a visible descendant to
    its nearest visible ancestor. Walk physical nodes first so selecting that
    displayed parent reaches the same descendants, without returning hidden
    folders. A seed outside ``workspace_id`` never expands, so a stale or
    crafted ``folder_id`` for a folder no longer in the workspace will not
    expand into its visible descendants.

    Repositories that scope a query to a folder subtree call this on their
    own connection.
    """
    rows = conn.execute(
        """WITH RECURSIVE tree(id) AS (
               SELECT ?
               UNION ALL
               SELECT f.id FROM folders f
               JOIN tree t ON f.parent_id = t.id
               WHERE EXISTS (
                   SELECT 1 FROM workspace_visible_folders seed
                   WHERE seed.folder_id = ? AND seed.workspace_id = ?
               )
           )
           SELECT t.id FROM tree t
           WHERE t.id = ? OR EXISTS (
               SELECT 1 FROM workspace_visible_folders wf
               WHERE wf.folder_id = t.id AND wf.workspace_id = ?
           )""",
        (folder_id, folder_id, workspace_id, folder_id, workspace_id),
    ).fetchall()
    return [r["id"] for r in rows]
