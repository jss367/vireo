"""E2E tests for the lightbox right-click context menu.

These tests exercise the shared `openContextMenu` handler wired to the
lightbox `<img id="lightboxImg">`. The E2E seed fixture creates photo rows
but the underlying files don't exist on disk, so `/photos/<id>/full` returns
500 and the image element never gets a non-zero size. We dispatch the
`contextmenu` event directly rather than using Playwright's
`click(button="right")` which requires visibility/stability.
"""
from playwright.sync_api import expect


def _open_lightbox(page, url):
    """Navigate to browse and open the lightbox on the first grid card."""
    page.goto(f"{url}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.dblclick()
    # Overlay becomes active synchronously once openLightbox() runs.
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    # Wait until `_lightboxCurrentId` is populated (openLightbox assigns it
    # synchronously, but the dblclick → handler chain is async from pytest's
    # point of view).
    page.wait_for_function(
        "typeof _lightboxCurrentId !== 'undefined' && _lightboxCurrentId !== null",
        timeout=3000,
    )


def _fire_contextmenu_on_lightbox(page):
    """Dispatch a contextmenu event on the lightbox image.

    Using dispatch_event bypasses visibility checks (the underlying image
    never loads in the test harness because the photo file doesn't exist
    on disk). The event still reaches the handler, which is what matters.
    """
    page.evaluate(
        """
        const img = document.getElementById('lightboxImg');
        const evt = new MouseEvent('contextmenu', {
            bubbles: true, cancelable: true, clientX: 400, clientY: 300,
            button: 2,
        });
        img.dispatchEvent(evt);
        """
    )


def test_lightbox_right_click_opens_menu(live_server, page):
    """Right-clicking the lightbox image opens the shared context menu."""
    url = live_server["url"]
    _open_lightbox(page, url)

    _fire_contextmenu_on_lightbox(page)
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    # A handful of the lightbox-specific menu items.
    expect(
        menu.locator(".vireo-ctx-item", has_text="Reveal in")
    ).to_be_visible()
    expect(
        menu.locator(".vireo-ctx-item", has_text="Copy Path")
    ).to_be_visible()
    expect(
        menu.locator(".vireo-ctx-item", has_text="Close Lightbox")
    ).to_be_visible()
    # Rating / color / flag chip rows are present (14 chips total).
    assert menu.locator(".vireo-ctx-chip").count() > 5


def test_lightbox_right_click_does_not_toggle_zoom(live_server, page):
    """Right-click must not trip the click-to-zoom / pan handlers.

    The lightbox exposes the current zoom level via `_lbZoom`. A contextmenu
    event must not change zoom state.
    """
    url = live_server["url"]
    _open_lightbox(page, url)

    before = page.evaluate(
        "typeof _lbZoom !== 'undefined' ? _lbZoom : null"
    )
    _fire_contextmenu_on_lightbox(page)
    expect(page.locator(".vireo-ctx-menu")).to_be_visible()
    after = page.evaluate(
        "typeof _lbZoom !== 'undefined' ? _lbZoom : null"
    )
    assert before == after


def test_lightbox_close_menu_item_closes_overlay(live_server, page):
    """The 'Close Lightbox' menu item dismisses the overlay."""
    url = live_server["url"]
    _open_lightbox(page, url)

    _fire_contextmenu_on_lightbox(page)
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(".vireo-ctx-item", has_text="Close Lightbox").click()
    expect(menu).to_be_hidden()
    page.wait_for_function(
        "!document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=2000,
    )


def test_lightbox_rating_chip_applies(live_server, page):
    """Clicking a rating chip in the lightbox menu rates the current photo."""
    url = live_server["url"]
    _open_lightbox(page, url)

    pid = page.evaluate("_lightboxCurrentId")
    assert pid is not None

    _fire_contextmenu_on_lightbox(page)
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    # First chip row is ratings (0..5). Click the "2" chip.
    menu.locator(".vireo-ctx-chip", has_text="2").first.click()
    expect(menu).to_be_hidden()

    # setRatingFor is defined on browse.html; it updates the server and the
    # local photos[] list. Poll for the rating change.
    page.wait_for_function(
        f"(photos.find(p => p.id === {pid}) || {{}}).rating === 2",
        timeout=3000,
    )


def test_outside_click_dismiss_swallows_next_click(live_server, page):
    """Outside-click dismissal of the context menu must swallow the click.

    If the click propagates after `_outside` tears the menu down, any
    underlying handler (e.g. the lightbox overlay's `onclick=closeLightbox`)
    would fire unexpectedly. We install a body-level click listener,
    dismiss the menu by clicking outside it, and assert the listener never
    saw the click.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.evaluate(
        """() => {
            window.__outside_click_count = 0;
            document.body.addEventListener('click', () => {
                window.__outside_click_count++;
            }, false);
            openContextMenu({clientX: 100, clientY: 100},
                [{label: 'X', onClick: () => {}}]);
        }"""
    )
    expect(page.locator('.vireo-ctx-menu')).to_be_visible()
    page.mouse.click(500, 500)
    expect(page.locator('.vireo-ctx-menu')).to_be_hidden()
    # The outside click should have been swallowed before reaching body.
    count = page.evaluate("window.__outside_click_count")
    assert count == 0, f"outside-click-to-dismiss reached body (count={count})"
