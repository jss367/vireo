"""The selection panel's "Show N photos" button, driven as a user would.

A bulk selection's Predictions panel offers to accept a species on dozens
of photos at once. Two of those photos predicting something rare is either
a genuine find or a bad detection, and the counts in the row cannot say
which — only the frames can. This scenario multi-selects the photos, finds
the two-photo species row, clicks its Show button, and checks that the
lightbox opens on exactly those two photos while the selection the Accept
buttons act on is left alone.
"""

# Seeded by ``prediction_selection_seed``: the five photos browse_seed
# leaves untagged, all predicting one species, two of them also predicting
# a rare one.
TARGET_FILENAMES = [
    "sunset01.jpg",
    "mountain01.jpg",
    "hawk01.jpg",
    "finch01.jpg",
    "heron01.jpg",
]


def run(session):
    session.goto("/browse")
    session.page.wait_for_selector(".grid-card[data-id]", state="visible", timeout=5000)

    ids = session.eval(
        """(() => {
            const wanted = %s;
            return Array.from(document.querySelectorAll('.grid-card[data-id]'))
              .filter(c => wanted.includes(c.dataset.filename))
              .map(c => parseInt(c.dataset.id, 10));
        })()""" % TARGET_FILENAMES
    )
    session.assert_that(
        len(ids) == len(TARGET_FILENAMES),
        f"seeded predicted photos should all be in the grid; got {ids}",
    )
    if len(ids) != len(TARGET_FILENAMES):
        return

    # Meta rather than Control: macOS turns Ctrl+click into a right-click.
    session.page.click(f'.grid-card[data-id="{ids[0]}"]')
    for photo_id in ids[1:]:
        session.page.click(f'.grid-card[data-id="{photo_id}"]', modifiers=["Meta"])

    session.page.wait_for_selector(
        "#selectionPredictions .prediction-row", state="visible", timeout=5000
    )
    session.screenshot("selection-predictions")

    rows = session.eval(
        """(() => Array.from(
            document.querySelectorAll('#selectionPredictions .prediction-row')
        ).map(row => ({
            species: row.querySelector('.prediction-species').textContent,
            meta: row.querySelector('.prediction-meta').textContent,
            show: (row.querySelector('.prediction-show') || {}).textContent || null,
        })))()"""
    )
    rare = [r for r in rows if r["species"] == "Blue-breasted Quail"]
    session.assert_that(
        len(rare) == 1,
        f"the two-photo species should have its own prediction row; got {rows}",
    )
    if not rare:
        return

    # The button names the count it opens, and that count is the one the
    # row's own "Predicted on N of M" line already states.
    session.assert_that(
        rare[0]["show"] == "Show 2 photos",
        f"Show button should name the 2 photos it opens; got {rare[0]['show']!r}",
    )
    session.assert_that(
        "Predicted on 2 of 5" in rare[0]["meta"],
        f"row meta should agree with the Show count; got {rare[0]['meta']!r}",
    )

    session.page.click(
        "#selectionPredictions .prediction-row:has(.prediction-species:text-is("
        "'Blue-breasted Quail')) .prediction-show"
    )
    session.page.wait_for_selector("#lightboxOverlay.active", state="visible", timeout=5000)
    session.screenshot("lightbox-from-show-button")

    opened = session.eval(
        "(window._lightboxPhotoList || []).map(p => p.id)"
    )
    session.assert_that(
        len(opened) == 2,
        f"lightbox should hold exactly the 2 predicted photos; got {opened}",
    )
    session.assert_that(
        set(opened) <= set(ids),
        f"lightbox photos should come from the selection; got {opened} vs {ids}",
    )

    # The whole point of not narrowing the selection: the Accept buttons for
    # all five photos are still there when the user closes the lightbox.
    still_selected = session.eval("selectedPhotos.size")
    session.assert_that(
        still_selected == len(ids),
        f"looking must not change the selection; got size={still_selected}",
    )

    session.page.keyboard.press("Escape")
    session.page.wait_for_selector("#lightboxOverlay.active", state="hidden", timeout=5000)
    count_text = session.eval(
        "document.getElementById('selectionCount').textContent"
    )
    session.assert_that(
        count_text.startswith(str(len(ids))),
        f"selection panel should still read {len(ids)} photos; got {count_text!r}",
    )
