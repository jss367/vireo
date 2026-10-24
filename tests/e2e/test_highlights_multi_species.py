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
from services import photo_ingest

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


def test_picking_promotes_photo_into_unloaded_second_bucket(live_server, page):
    """Pick must refresh every confirmed-species bucket the photo belongs to,
    including one whose current loaded window hasn't reached the photo yet.

    Regression for Codex feedback on PR #2291 (flag-changes.js:68): a photo
    tagged with two species that sits past the 20-photo window of its second
    bucket is only visible in the first bucket. ``picked_first`` on the server
    promotes the photo to the top of the second bucket after a pick, but the
    client used to refetch only the loaded hit — leaving the second bucket's
    loaded prefix stale (and the Save-as-Collection payload for it short the
    newly promoted photo) until a full reload.
    """
    db = live_server["db"]
    data = live_server["data"]

    # Tag the target photo with BOTH keywords: this photo belongs to the
    # Red-tailed Hawk AND American Robin buckets. Its quality score (0.65) is
    # higher than the fillers below so it stays in the Hawk bucket's loaded
    # window, but lower than the robin fillers so it falls past the Robin
    # bucket's 20-photo prefix.
    pid = data["photos"][0]
    hawk_kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("Red-tailed Hawk",)
    ).fetchone()["id"]
    robin_kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("American Robin",)
    ).fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.65 WHERE id = ?", (pid,),
    )
    db.conn.execute(
        "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, robin_kid),
    )

    folder_id = data["folders"][1]
    for i in range(21):
        filler = photo_ingest.add_photo(
            db, folder_id=folder_id,
            filename=f"filler-robin-{i}.jpg",
            extension=".jpg",
            file_size=1000,
            file_mtime=1.0,
            timestamp=f"2024-06-15T11:{i:02d}:00",
        )
        db.conn.execute(
            "UPDATE photos SET quality_score = ? WHERE id = ?",
            (0.95 - i * 0.001, filler),
        )
        db.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (filler, robin_kid),
        )
    db.conn.commit()

    _goto_highlights(page, live_server["url"])

    hawk_section = page.locator("section.bucket").filter(has_text="Red-tailed Hawk")
    expect(hawk_section.locator(f'.highlights-card[data-photo-id="{pid}"]')).to_be_visible(
        timeout=5000,
    )

    def _robin_bucket_has_pid():
        return page.evaluate(
            "pid => {"
            "  const b = highlightsState.currentData.buckets.find("
            "    x => x.species === 'American Robin');"
            "  return !!(b && b.photos.some(p => p.id === pid));"
            "}",
            pid,
        )

    # The Robin bucket is loaded (21 fillers outrank pid), but pid is past
    # the 20-photo loaded window — the client cannot see it yet.
    assert _robin_bucket_has_pid() is False, (
        "setup: Robin bucket should NOT contain pid in its loaded prefix "
        "(the 21 fillers push it out)"
    )

    hawk_section.locator(f'.highlights-card[data-photo-id="{pid}"]').click()
    page.wait_for_function(
        "pid => vireoLightboxSession.requestedPhotoId() === pid",
        arg=pid,
        timeout=3000,
    )
    page.keyboard.press("p")

    assert _wait_for(lambda: db.photos.get(pid)["flag"] == "flagged"), (
        db.photos.get(pid)
    )

    # The fix: picked_first promotes pid to the top of the Robin bucket, and
    # the pick handler refetches that bucket (using confirmed_species_list)
    # even though pid wasn't in the loaded hits.
    page.wait_for_function(
        "pid => {"
        "  const b = highlightsState.currentData.buckets.find("
        "    x => x.species === 'American Robin');"
        "  return !!(b && b.photos.some(p => p.id === pid && p.flag === 'flagged'));"
        "}",
        arg=pid,
        timeout=5000,
    )
