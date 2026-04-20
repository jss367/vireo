"""Regression scenario for #598: lightbox arrows work when opened from /browse.

Before the fix, ``openLightbox()`` was called without the ``photoList``
argument from browse.html, so ``_lightboxPhotoList`` stayed empty and the
on-screen Next/Prev arrows silently no-op'd. Double-clicking a grid card,
then clicking Next, should advance to a different photo.
"""


def run(session):
    session.goto("/browse")

    session.page.wait_for_selector(".grid-card[data-id]", state="visible", timeout=5000)
    session.screenshot("browse-loaded")

    first = session.eval(
        """(() => {
            const card = document.querySelector('.grid-card[data-id]');
            return card ? {id: card.dataset.id, filename: card.dataset.filename || ''} : null;
        })()"""
    )
    session.assert_that(first is not None, "expected at least one grid card")
    if first is None:
        return

    session.page.dblclick(f'.grid-card[data-id="{first["id"]}"]')
    session.page.wait_for_selector("#lightboxOverlay.active", timeout=5000)
    session.screenshot("lightbox-open")

    shown_before = session.eval(
        "(document.getElementById('lightboxFilename') || {}).textContent || ''"
    )
    session.assert_that(
        shown_before == first["filename"],
        f"lightbox should show clicked photo first; expected {first['filename']!r}, got {shown_before!r}",
    )

    counter_before = session.eval(
        "(document.getElementById('lightboxCounter') || {}).textContent || ''"
    )
    session.assert_that(
        "1 /" in counter_before,
        f"counter should start at '1 / N'; got {counter_before!r}",
    )

    session.page.click("[title='Next (\u2192)']")
    # Give the filename label a beat to update before reading it.
    session.page.wait_for_function(
        f"() => (document.getElementById('lightboxFilename') || {{}}).textContent !== {first['filename']!r}",
        timeout=3000,
    )
    session.screenshot("lightbox-after-next")

    shown_after_next = session.eval(
        "(document.getElementById('lightboxFilename') || {}).textContent || ''"
    )
    session.assert_that(
        shown_after_next != first["filename"],
        "Next arrow should advance to a different photo "
        f"(still showing {shown_after_next!r})",
    )
    counter_after_next = session.eval(
        "(document.getElementById('lightboxCounter') || {}).textContent || ''"
    )
    session.assert_that(
        "2 /" in counter_after_next,
        f"counter should read '2 / N' after Next; got {counter_after_next!r}",
    )

    session.page.click("[title='Previous (\u2190)']")
    session.page.wait_for_function(
        f"() => (document.getElementById('lightboxFilename') || {{}}).textContent === {first['filename']!r}",
        timeout=3000,
    )
    shown_after_prev = session.eval(
        "(document.getElementById('lightboxFilename') || {}).textContent || ''"
    )
    session.assert_that(
        shown_after_prev == first["filename"],
        f"Previous arrow should return to the first photo; got {shown_after_prev!r}",
    )
