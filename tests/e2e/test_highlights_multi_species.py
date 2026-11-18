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
        # About the size of the seeded hawk region (0.5 x 0.5), so both
        # birds are subjects of the photo and a pick counts for each.
        [{"box": {"x": x, "y": 0.1, "w": 0.4, "h": 0.4}, "confidence": 0.9,
          "category": "animal"} for x in (0.1, 0.6)],
        detector_model="MDV6",
    )
    db.predictions.add(hawk_det, "Red-tailed Hawk", 0.92, "m")
    db.predictions.add(coopers_det, "Cooper's Hawk", 0.86, "m")
    db.commit()
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


@pytest.mark.parametrize("robin_by", ["keyword", "prediction"])
def test_picking_promotes_photo_into_unloaded_second_bucket(live_server, page, robin_by):
    """Pick must refresh every bucket the photo belongs to, confirmed or
    predicted, including one whose loaded window hasn't reached the photo yet.

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
    # Tagged Red-tailed Hawk, with a second subject that reads as a robin
    # and is about the size of the hawk, so the photo is a picture of both
    # and a pick counts in each bucket. The robin is a second tag
    # ("keyword") or an unconfirmed candidate ("prediction").
    db.conn.execute(
        "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, hawk_kid),
    )
    if robin_by == "keyword":
        db.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, robin_kid),
        )
    hawk_det, robin_det = db.detections.save(
        pid,
        [{"box": {"x": x, "y": 0.1, "w": 0.4, "h": 0.4}, "confidence": 0.9,
          "category": "animal"} for x in (0.1, 0.6)],
        detector_model="MDV6",
    )
    db.predictions.add(hawk_det, "Red-tailed Hawk", 0.92, "m")
    db.predictions.add(robin_det, "American Robin", 0.9, "m")
    db.commit()

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
    # the pick handler refetches that bucket (listed in the entry's
    # bucket_species_list) even though pid wasn't in the loaded hits.
    page.wait_for_function(
        "pid => {"
        "  const b = highlightsState.currentData.buckets.find("
        "    x => x.species === 'American Robin');"
        "  return !!(b && b.photos.some(p => p.id === pid && p.flag === 'flagged'));"
        "}",
        arg=pid,
        timeout=5000,
    )


def test_a_pick_is_labeled_for_the_species_the_photo_is_of(live_server, page):
    """A picked hawk photo with a small robin behind it leads the hawk row
    as a pick. In the robin row it sits below the robin's own photo, under
    a divider, and its badge says the pick is for the hawk."""
    db = live_server["db"]
    data = live_server["data"]
    flock, portrait = data["photos"][0], data["photos"][3]
    kids = {
        name: db.conn.execute(
            "SELECT id FROM keywords WHERE name = ?", (name,)
        ).fetchone()["id"]
        for name in ("Red-tailed Hawk", "American Robin")
    }
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.9, flag = 'flagged' WHERE id = ?",
        (flock,),
    )
    db.conn.execute(
        "UPDATE photos SET quality_score = 0.4 WHERE id = ?", (portrait,),
    )
    for pid, name in (
        (flock, "Red-tailed Hawk"), (flock, "American Robin"),
        (portrait, "American Robin"),
    ):
        db.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, kids[name]),
        )
    db.conn.commit()
    hawk_det, robin_det = db.detections.save(
        flock,
        [{"box": {"x": 0.3, "y": 0.3, "w": 0.4, "h": 0.4}, "confidence": 0.9,
          "category": "animal"},
         {"box": {"x": 0.8, "y": 0.1, "w": 0.1, "h": 0.1}, "confidence": 0.6,
          "category": "animal"}],
        detector_model="MDV6",
    )
    db.predictions.add(hawk_det, "Red-tailed Hawk", 0.92, "m")
    db.predictions.add(robin_det, "American Robin", 0.9, "m")
    db.commit()

    _goto_highlights(page, live_server["url"])

    flock_card = f'.highlights-card[data-photo-id="{flock}"]'
    hawk = page.locator("section.bucket").filter(
        has=page.locator(".bucket-title", has_text="Red-tailed Hawk"),
    )
    expect(hawk.locator(flock_card)).to_have_class(
        re.compile(r"\bpick-flag-card\b"), timeout=5000,
    )
    expect(hawk.locator(flock_card).locator(".pick-flag-badge")).to_have_text(
        "Pick", ignore_case=True,
    )
    expect(hawk.locator(".bucket-decision")).to_contain_text("a pick")

    robin = page.locator("section.bucket").filter(
        has=page.locator(".bucket-title", has_text="American Robin"),
    )
    # The row: the robin's own photos (the scored one, then the seeded robin
    # photo nothing has analyzed), then the hawk photo under its divider.
    unscored = data["photos"][4]
    strip = robin.locator(".bucket-strip > *")
    expect(strip).to_have_count(5)
    expect(strip.nth(0)).to_have_attribute("data-photo-id", str(portrait))
    expect(strip.nth(1)).to_contain_text("Not yet analyzed", ignore_case=True)
    expect(strip.nth(2)).to_have_attribute("data-photo-id", str(unscored))
    expect(strip.nth(3)).to_have_class(re.compile(r"\bother-subject-divider\b"))
    expect(strip.nth(3)).to_contain_text(
        "Another bird is the subject", ignore_case=True,
    )
    expect(strip.nth(4)).to_have_attribute("data-photo-id", str(flock))
    behind = robin.locator(flock_card)
    expect(behind).not_to_have_class(re.compile(r"\bpick-flag-card\b"))
    expect(behind.locator(".pick-flag-badge.other-subject")).to_have_text(
        "Pick for Red-tailed Hawk", ignore_case=True,
    )
    expect(behind.locator(".card-chip.presence")).to_have_text("Not the subject")
    expect(robin.locator(".bucket-decision")).to_contain_text("ranked by Vireo's score")
