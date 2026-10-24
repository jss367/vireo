"""Highlights with a photo that holds two species (issue #1298).

The photo is tagged Red-tailed Hawk and its second subject reads as a
Cooper's Hawk. It shows in both buckets; confirming it from the Cooper's Hawk
bucket adds that species and keeps the Red-tailed Hawk tag; picking it
updates both cards.
"""
import re
import time

import pytest
from playwright.sync_api import expect

from e2e.module_pages import expose_page_modules
from e2e.viewport_test_support import install_viewport_test_helpers


@pytest.fixture(autouse=True)
def _viewport_geometry_helpers(page):
    install_viewport_test_helpers(page)


def _goto_highlights(page, base_url):
    page.goto(f"{base_url}/highlights", timeout=10_000)
    expose_page_modules(page, "highlights", "highlightsState")


def _seed_two_species_photo(db, data):
    pid = data["photos"][0]
    hawk_kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("Red-tailed Hawk",)
    ).fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.9 WHERE id = ?", (pid,),
    )
    db.conn.execute(
        "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, hawk_kid),
    )
    db.conn.commit()
    hawk_det, coopers_det = db.detections.save(
        pid,
        [{"box": {"x": x, "y": 0.1, "w": 0.3, "h": 0.3}, "confidence": 0.9,
          "category": "animal"} for x in (0.1, 0.6)],
        detector_model="MDV6",
    )
    db.predictions.add(hawk_det, "Red-tailed Hawk", 0.92, "m")
    db.predictions.add(coopers_det, "Cooper's Hawk", 0.86, "m")
    return pid


def _species_on(db, pid):
    return {
        row["name"] for row in db.conn.execute(
            "SELECT k.name FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id WHERE pk.photo_id = ?",
            (pid,),
        )
    }


def _wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def test_two_species_photo_confirms_from_its_second_bucket(live_server, page):
    db = live_server["db"]
    pid = _seed_two_species_photo(db, live_server["data"])

    _goto_highlights(page, live_server["url"])
    card_selector = f'.highlights-card[data-photo-id="{pid}"]'
    coopers = page.locator("section.bucket").filter(has_text="Cooper's Hawk")
    coopers_card = coopers.locator(card_selector)
    expect(coopers_card).to_be_visible(timeout=5000)
    hawk = page.locator("section.bucket").filter(has_text="Red-tailed Hawk")
    expect(hawk.locator(card_selector)).to_be_visible()
    # The card says the photo carries another species.
    expect(coopers_card.locator(".card-chip", has_text="+1 species")).to_be_visible()

    coopers_card.click()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    panel = page.locator("#highlightLightboxPanel")
    expect(panel).to_contain_text("Predicted: Cooper's Hawk")
    expect(panel).to_contain_text("also tagged Red-tailed Hawk")

    panel.locator('button[data-lb-action="confirm"]').click()

    assert _wait_for(lambda: "Cooper's Hawk" in _species_on(db, pid)), (
        _species_on(db, pid)
    )
    assert "Red-tailed Hawk" in _species_on(db, pid)
    # After the reload the entry the lightbox shows is the confirmed one.
    expect(panel).to_contain_text("Keyword confirmed: Cooper's Hawk", timeout=5000)


def test_picking_a_two_species_photo_updates_both_cards(live_server, page):
    db = live_server["db"]
    pid = _seed_two_species_photo(db, live_server["data"])

    _goto_highlights(page, live_server["url"])
    cards = page.locator(f'.highlights-card[data-photo-id="{pid}"]')
    expect(cards).to_have_count(2, timeout=5000)

    page.locator("section.bucket").filter(has_text="Cooper's Hawk").locator(
        f'.highlights-card[data-photo-id="{pid}"]'
    ).click()
    page.wait_for_function(
        "pid => vireoLightboxSession.requestedPhotoId() === pid",
        arg=pid,
        timeout=3000,
    )
    page.keyboard.press("p")

    assert _wait_for(lambda: db.photos.get(pid)["flag"] == "flagged")
    for i in range(2):
        expect(cards.nth(i)).to_have_class(
            re.compile(r"\bpick-flag-card\b"), timeout=5000,
        )
