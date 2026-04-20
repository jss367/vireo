"""Regression scenario for #597: orphan folders stay visible on /browse.

A folder whose ``parent_id`` points at an unlinked (or ``status != 'ok'``)
parent used to disappear from the browse sidebar tree — the renderer groups
by ``parent_id`` and only descends from the ``'root'`` bucket, so any folder
whose parent wasn't in the returned set was stranded. ``get_folder_tree``
now rewrites ``parent_id`` to the nearest visible ancestor (or NULL).

This scenario uses ``orphan_folder_seed``: one folder (``2024``) is linked to
the workspace but its DB-level parent (``archive``) is unlinked. The child
must still appear in the browse sidebar.
"""


def run(session):
    session.goto("/browse")
    session.page.wait_for_selector("#folderTree .tree-item", state="visible", timeout=5000)
    session.screenshot("browse-folder-tree")

    tree = session.eval(
        """(() => {
            return Array.from(document.querySelectorAll('#folderTree .tree-item'))
                .map(el => ({
                    id: parseInt(el.dataset.folderId, 10),
                    name: (el.querySelector('span') || {}).textContent || '',
                }));
        })()"""
    )

    names = [t["name"] for t in tree]
    session.assert_that(
        "2024" in names,
        f"expected orphan-parent folder '2024' to appear in sidebar; got {names!r}",
    )
    session.assert_that(
        "inbox" in names,
        f"expected control folder 'inbox' in sidebar; got {names!r}",
    )
    # The unlinked parent must not leak into the sidebar.
    session.assert_that(
        "archive" not in names,
        f"unlinked parent 'archive' should not appear in sidebar; got {names!r}",
    )

    # Clicking the orphan folder should filter the grid to its one photo.
    orphan = next((t for t in tree if t["name"] == "2024"), None)
    if orphan is None:
        return

    session.page.click(f'#folderTree .tree-item[data-folder-id="{orphan["id"]}"]')
    session.page.wait_for_function(
        f'() => document.querySelectorAll(\'.grid-card[data-id]\').length >= 1'
        f' && document.querySelector(\'#folderTree .tree-item[data-folder-id="{orphan["id"]}"]\').classList.contains(\'active\')',
        timeout=5000,
    )
    session.screenshot("browse-folder-filtered")

    filtered_filenames = session.eval(
        """(() => Array.from(document.querySelectorAll('.grid-card[data-id]'))
            .map(c => c.dataset.filename || ''))()"""
    )
    session.assert_that(
        any("archive2024" in f for f in filtered_filenames),
        f"expected orphan folder's photo to appear when filtered; got {filtered_filenames!r}",
    )
