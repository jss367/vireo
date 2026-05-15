from playwright.sync_api import expect


def test_browse_lightbox_arrows_navigate(live_server, page):
    """On-screen ◄/► arrows in the lightbox navigate between photos opened from /browse.

    Regression: previously browse.html called openLightbox() without the photo-list
    argument, so _lightboxPhotoList stayed empty and lightboxNav() silently no-op'd.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_filename = first_card.get_attribute("data-filename")

    first_card.dblclick()

    overlay = page.locator("#lightboxOverlay")
    expect(overlay).to_have_class("lightbox-overlay active")

    filename_display = page.locator("#lightboxFilename")
    expect(filename_display).to_have_text(first_filename)

    counter = page.locator("#lightboxCounter")
    expect(counter).to_be_visible()
    expect(counter).to_contain_text("1 /")

    page.locator("[title='Next (→)']").click()

    expect(filename_display).not_to_have_text(first_filename)
    expect(counter).to_contain_text("2 /")

    page.locator("[title='Previous (←)']").click()
    expect(filename_display).to_have_text(first_filename)
    expect(counter).to_contain_text("1 /")


def test_browse_lightbox_arrows_preserve_one_to_one_zoom(live_server, page):
    """Navigating from a 1:1 lightbox view keeps the next photo at 1:1."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_card.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")

    page.evaluate(
        """() => {
            window._lbNativeZoom = 2;
            window._lbZoom = 2;
            window._lbPending1To1 = false;
        }"""
    )

    page.locator("[title='Next (→)']").click()
    expect(page.locator("#lightboxCounter")).to_contain_text("2 /")
    assert page.evaluate("window._lbPending1To1") is True

    restored = page.evaluate(
        """() => {
            window._lbNativeZoom = 2.5;
            window._lbApplyPendingOneToOneZoom();
            return Math.abs(window._lbZoom - window._lbNativeZoom) <= Math.max(0.01, window._lbNativeZoom * 0.01);
        }"""
    )
    assert restored
    assert page.evaluate("window._lbZoom") > 1.001
