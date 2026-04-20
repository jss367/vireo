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
                .map(el => {
                    const nameEl = el.querySelector(
                        'span:not(.tree-indent):not(.tree-toggle):not(.count)'
                    );
                    return {
                        id: parseInt(el.dataset.folderId, 10),
                        name: (nameEl ? nameEl.textContent : '').trim(),
                    };
                });
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
    # Wait until the grid has narrowed to exactly the orphan's one photo. The
    # unfiltered grid contains both archive2024_01.jpg and inbox_01.jpg, so
    # asserting only that archive2024 is present would pass on a filter no-op.
    session.page.wait_for_function(
        f'() => {{'
        f'  const cards = Array.from(document.querySelectorAll(\'.grid-card[data-id]\'));'
        f'  const item = document.querySelector(\'#folderTree .tree-item[data-folder-id="{orphan["id"]}"]\');'
        f'  return item && item.classList.contains(\'active\')'
        f'    && cards.length === 1'
        f'    && (cards[0].dataset.filename || \'\').includes(\'archive2024\');'
        f'}}',
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
    # The control folder's photo must be filtered out — otherwise the click
    # was a no-op and the regression guard would miss the failure mode.
    session.assert_that(
        not any("inbox_" in f for f in filtered_filenames),
        f"expected control folder's photo to be excluded when filtering by orphan; got {filtered_filenames!r}",
    )
    session.assert_that(
        len(filtered_filenames) == 1,
        f"expected exactly one photo after filtering by orphan folder; got {filtered_filenames!r}",
    )
