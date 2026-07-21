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


def test_location_changes_are_grouped_with_plain_language_delta(live_server, page):
    """Identical location writes render once with thumbnails, not raw tokens."""
    import config as cfg

    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:2]
    config = cfg.load()
    config["write_assigned_location_to_xmp"] = True
    cfg.save(config)

    florida_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES ('Florida', 'location')"
    ).lastrowid
    tallahassee_id = db.conn.execute(
        "INSERT INTO keywords "
        "(name, parent_id, type, latitude, longitude) "
        "VALUES ('Tallahassee', ?, 'location', 30.4383, -84.2807)",
        (florida_id,),
    ).lastrowid
    db.conn.commit()
    for photo_id in photo_ids:
        db.set_photo_location(photo_id, tallahassee_id)
        db.queue_change(photo_id, "location", "effective")

    page.goto(f"{live_server['url']}/browse")
    page.evaluate("openSyncPreview()")

    overlay = page.locator("#syncPreviewOverlay")
    expect(overlay).to_be_visible()
    expect(overlay.locator(".sync-review-group")).to_have_count(1)
    expect(overlay.locator(".sync-review-group-title")).to_have_text(
        "Location updated on 2 photos"
    )
    expect(overlay.locator(".sync-review-delta")).to_contain_text(
        "No XMP sidecar"
    )
    expect(overlay.locator(".sync-review-delta")).to_contain_text(
        "Tallahassee, Florida"
    )
    expect(overlay.locator(".sync-review-note")).to_contain_text(
        "written to XMP as GPS metadata"
    )
    expect(overlay.locator(".sync-preview-thumb")).to_have_count(2)
    expect(overlay).not_to_contain_text("effective")

    # The Dashboard's compact pending-change detail consumes the same API;
    # keep the internal token out of that secondary review surface too.
    page.goto(f"{live_server['url']}/dashboard")
    pending_card = page.locator("#pendingCard")
    expect(pending_card).to_be_visible()
    pending_card.click()
    pending_detail = page.locator("#pendingDetail")
    expect(pending_detail).to_contain_text(
        "Location: No XMP sidecar → Tallahassee, Florida"
    )
    expect(pending_detail).not_to_contain_text("effective")
