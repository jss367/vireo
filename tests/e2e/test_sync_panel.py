"""End-to-end tests for the pending-changes sync overlay."""

from playwright.sync_api import expect


def test_escape_closes_pending_changes_overlay(live_server, page):
    """Escape dismisses the Review Pending Changes overlay without leaking."""
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        """() => {
            window.__syncEscapeLeaked = 0;
            document.body.addEventListener('keydown', (event) => {
                if (event.key === 'Escape') window.__syncEscapeLeaked += 1;
            });
            openSyncPreview();
        }"""
    )

    overlay = page.locator("#syncPreviewOverlay")
    expect(overlay).to_be_visible()

    page.keyboard.press("Escape")

    expect(overlay).to_be_hidden()
    assert page.evaluate("window._syncModalOpen") is False
    assert page.evaluate("window.__syncEscapeLeaked") == 0


def test_escape_closes_lightbox_before_pending_changes_overlay(live_server, page):
    """Stacked popups unwind one at a time, with the newest closing first."""
    photo_id = live_server["data"]["photos"][0]
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        """(photoId) => {
            openSyncPreview();
            openLightbox(photoId, 'hawk1.jpg', [
                {id: photoId, filename: 'hawk1.jpg'},
            ]);
        }""",
        photo_id,
    )

    sync_overlay = page.locator("#syncPreviewOverlay")
    lightbox = page.locator("#lightboxOverlay")
    expect(sync_overlay).to_be_visible()
    expect(lightbox).to_have_class("lightbox-overlay active")

    page.keyboard.press("Escape")

    expect(lightbox).not_to_have_class("lightbox-overlay active")
    expect(sync_overlay).to_be_visible()

    page.keyboard.press("Escape")

    expect(sync_overlay).to_be_hidden()
