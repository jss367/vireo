"""Review builds its grid in batches and adds cards as the reader scrolls."""

import re

from labels_fingerprint import TOL_SENTINEL
from playwright.sync_api import expect
from services import photo_ingest

BULK = 130


def _add_predictions(db, folder_id, count):
    """One pending prediction on its own photo per species, so each is a card."""
    for index in range(count):
        photo_id = photo_ingest.add_photo(
            db, folder_id=folder_id, filename=f"bulk{index:03d}.jpg", extension=".jpg",
            file_size=5000 + index, file_mtime=1.0, timestamp="2024-07-01T09:00:00",
        )
        detection_ids = db.detections.save(photo_id, [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
             "confidence": 0.95, "category": "animal"},
        ], detector_model="test-detector")
        db.predictions.add(
            detection_id=detection_ids[0], species=f"Bulk species {index:03d}",
            confidence=0.8, model="BioCLIP-2", labels_fingerprint=TOL_SENTINEL,
        )
    db.commit()


def _scroll_to_end(page):
    page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight)")


def test_grid_builds_a_batch_and_scrolling_adds_the_rest(page, live_server):
    db = live_server["db"]
    _add_predictions(db, live_server["data"]["folders"][0], BULK)
    total = BULK + len(live_server["data"]["photos"])

    page.goto(live_server["url"] + "/review")
    cards = page.locator("#grid .card")
    expect(cards).to_have_count(60)
    expect(page.locator('#tabs [data-tab="all"] .count')).to_have_text(str(total))
    expect(page.locator("#gridMoreStatus")).to_have_text(
        f"Showing 60 of {total} cards. More appear as you scroll."
    )

    # The lightbox walks every photo of the view, including cards not built yet.
    cards.first.locator("img[data-photo-id]").click()
    expect(page.locator("#lightboxCounter")).to_contain_text(f"1 / {total}")
    page.keyboard.press("Escape")
    expect(page.locator(".lightbox-overlay.active")).to_have_count(0)

    _scroll_to_end(page)
    expect(cards).to_have_count(120)
    _scroll_to_end(page)
    expect(cards).to_have_count(total)
    expect(page.locator("#gridMore")).to_be_hidden()


def test_a_decision_keeps_the_cards_already_built(page, live_server):
    db = live_server["db"]
    _add_predictions(db, live_server["data"]["folders"][0], BULK)

    page.goto(live_server["url"] + "/review")
    cards = page.locator("#grid .card")
    expect(cards).to_have_count(60)
    _scroll_to_end(page)
    expect(cards).to_have_count(120)

    # A card past the first batch: the grid is rebuilt after the decision, and
    # the card must still be there, where the reader is looking.
    late = cards.nth(100)
    prediction_id = late.get_attribute("data-pred-id")
    late.scroll_into_view_if_needed()
    with page.expect_response(
        lambda r: r.url.endswith("/accept") and r.request.method == "POST"
    ) as response:
        late.locator(".btn-accept").click()
    assert response.value.status == 200
    expect(page.locator(f'#grid .card[data-pred-id="{prediction_id}"]')).to_have_class("card accepted")
    assert cards.count() >= 120
    assert page.evaluate("window.scrollY") > 0


def test_first_page_opens_individual_photos_while_bulk_decisions_wait(page, live_server):
    _add_predictions(live_server["db"], live_server["data"]["folders"][0], 65)
    later_page = re.compile(r"/api/predictions\?.*after_photo=[1-9]")
    held = []
    page.route(later_page, lambda route: held.append(route))

    page.goto(live_server["url"] + "/review", wait_until="domcontentloaded")
    link = page.locator("#loadingPhotos a").first
    expect(link).to_be_visible()
    expect(link).to_have_attribute("href", re.compile(r"/review\?photo_id=\d+"))
    expect(page.locator("#acceptAllBtn")).to_be_disabled()
    assert held
    held[0].continue_()
    expect(page.locator("#loadingPhotos")).to_be_hidden()
    total = 65 + len(live_server["data"]["photos"])
    expect(page.locator('#tabs [data-tab="all"] .count')).to_have_text(str(total))
