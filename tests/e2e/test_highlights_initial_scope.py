"""Regression test for the /highlights initial-load scope alignment.

Codex P1 on PR 617: on first load, the page's fetch was sent before the
folder dropdown was populated, so no scope/folder_id was passed and the
backend defaulted to a single (most recent) folder. The dropdown was then
populated with ``All folders in this workspace`` as the default-selected
option, so the UI claimed workspace-wide results while the data was in fact
single-folder. This test locks in the fix by asserting that on first render,
the photos shown reflect workspace scope (blending every folder with quality
data) and the dropdown selection matches.
"""
import json
import re
import time
from urllib.parse import quote
from urllib.request import urlopen

from playwright.sync_api import expect


def _seed_quality_scores_and_species(db, data):
    """Give every seeded photo a quality_score and a species keyword.

    The default seeder tags only one photo per folder with a species; we tag
    all of them here so the species label on the rendered card uniquely
    identifies the source folder (hawks -> park, robins -> yard). That gives
    the test a clean signal for which folders actually contributed photos.
    """
    hawk_kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("Red-tailed Hawk",)
    ).fetchone()["id"]
    robin_kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("American Robin",)
    ).fetchone()["id"]
    # Seed order: park photos (hawks) at indices 0-2, yard (robins) at 3-4.
    species_map = {0: hawk_kid, 1: hawk_kid, 2: hawk_kid, 3: robin_kid, 4: robin_kid}
    for i, pid in enumerate(data["photos"]):
        db.conn.execute(
            "UPDATE photos SET quality_score = ? WHERE id = ?",
            (0.9 - i * 0.05, pid),
        )
        db.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, species_map[i]),
        )
    db.conn.commit()


def _wait_for_flag(db, photo_id, expected, timeout=3.0):
    deadline = time.time() + timeout
    flag = None
    while time.time() < deadline:
        photo = db.get_photo(photo_id)
        flag = photo["flag"] if photo else None
        if flag == expected:
            return flag
        time.sleep(0.05)
    return flag


def test_initial_load_matches_default_workspace_scope(live_server, page):
    """First fetch must use workspace scope, matching the default selection.

    Seeded data has two folders: ``park`` (3 hawks, 2024-03) and ``yard``
    (2 robins, 2024-06). ``yard`` is the most-recent folder, so without the
    fix the initial fetch — which sends no scope/folder_id — returns only
    robins (2 photos). With the fix, the initial fetch explicitly requests
    ``scope=workspace`` and returns photos from both folders.
    """
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    url = live_server["url"]
    page.goto(f"{url}/highlights", timeout=5000)

    # Wait for the grid to populate (async fetch → render).
    cards = page.locator(".highlights-card")
    expect(cards.first).to_be_visible(timeout=5000)

    # The default-selected dropdown option must be the workspace sentinel,
    # matching the scope the page actually fetched.
    folder_select = page.locator("#folderSelect")
    expect(folder_select).to_have_value("__workspace__")

    # If the initial fetch used workspace scope, both species (Hawk + Robin)
    # are represented. If it only fetched the most-recent folder, we'd only
    # see ``American Robin`` (from ``yard``, the newest folder).
    # The redesigned template buckets cards by species, with the species name
    # rendered in the per-bucket header (.bucket-title); cards no longer carry
    # a per-card .card-species label.
    bucket_titles = page.locator(".bucket-title").all_inner_texts()
    species_text = {t.strip() for title in bucket_titles for t in [title]}
    # bucket-title text may include a trailing badge (e.g. "Confirmed"); match
    # by substring so we tolerate either "Red-tailed Hawk" or
    # "Red-tailed Hawk Confirmed".
    has_hawk = any("Red-tailed Hawk" in t for t in species_text)
    has_robin = any("American Robin" in t for t in species_text)
    assert has_hawk and has_robin, (
        f"Expected both species on initial load (workspace scope), "
        f"got bucket titles {species_text!r}. This likely means the first "
        f"fetch used folder scope and the UI/data are out of sync."
    )


def test_highlights_ranks_species_by_rich_subject_score(live_server, page):
    """Best image should use persisted subject quality, not only legacy score.

    The first hawk has a low legacy ``quality_score`` but strong subject
    metrics; the second has a high legacy score but soft/clipped/incomplete
    subject metrics. Highlights should put the real photographic keeper first.
    """
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    hawk_good, hawk_bad = data["photos"][0], data["photos"][1]
    db.conn.execute(
        """UPDATE photos
           SET quality_score = 0.20,
               subject_tenengrad = 900,
               bg_tenengrad = 20,
               crop_complete = 0.98,
               bg_separation = 10,
               subject_clip_high = 0.0,
               subject_clip_low = 0.0,
               subject_y_median = 115,
               noise_estimate = 4,
               subject_size = 0.10
           WHERE id = ?""",
        (hawk_good,),
    )
    db.conn.execute(
        """UPDATE photos
           SET quality_score = 0.95,
               subject_tenengrad = 20,
               bg_tenengrad = 400,
               crop_complete = 0.35,
               bg_separation = 200,
               subject_clip_high = 0.65,
               subject_clip_low = 0.0,
               subject_y_median = 245,
               noise_estimate = 90,
               subject_size = 0.01
           WHERE id = ?""",
        (hawk_bad,),
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/highlights", timeout=5000)
    hawk_section = page.locator("section.bucket").filter(has_text="Red-tailed Hawk")
    expect(hawk_section.locator(".highlights-card").first).to_be_visible(timeout=5000)
    expect(hawk_section.locator(".highlights-card img").first).to_have_attribute(
        "alt", "hawk1.jpg"
    )


def test_highlights_best_ui_is_advanced_only(live_server, page):
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    page.add_init_script(
        """() => {
            localStorage.setItem('vireo_advanced_mode', 'false');
            localStorage.setItem('vireo_dev_mode', 'false');
        }"""
    )
    page.goto(f"{live_server['url']}/highlights", timeout=5000)
    expect(page.locator(".highlights-card").first).to_be_visible(timeout=5000)

    expect(page.locator(".best-ribbon", has_text="Best")).to_have_count(0)
    expect(page.locator(".highlights-card .card-chip.score")).to_have_count(0)
    expect(page.locator(".highlights-card .card-chip.reason")).to_have_count(0)
    expect(page.locator("#sortSelect option[value='best']")).to_have_text(
        "Recommended first"
    )
    assert page.locator("#sortSelect option[value='worst']").evaluate(
        "el => el.hidden && el.disabled"
    )

    page.evaluate(
        """() => {
            document.documentElement.setAttribute('data-advanced-mode', 'true');
            document.documentElement.setAttribute('data-dev-mode', 'true');
            localStorage.setItem('vireo_advanced_mode', 'true');
            localStorage.setItem('vireo_dev_mode', 'true');
            window.dispatchEvent(new Event('advancedmodechange'));
        }"""
    )

    expect(page.locator(".best-ribbon", has_text="Best").first).to_be_visible()
    expect(page.locator(".highlights-card .card-chip.score").first).to_be_visible()
    expect(page.locator("#sortSelect option[value='best']")).to_have_text(
        "Best photo first"
    )
    assert page.locator("#sortSelect option[value='worst']").evaluate(
        "el => !el.hidden && !el.disabled"
    )


def test_highlights_picked_photos_show_flag_marker(live_server, page):
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)
    picked_id = data["photos"][0]
    db.update_photo_flag(picked_id, "flagged")

    page.goto(f"{live_server['url']}/highlights", timeout=5000)

    card = page.locator(f'.highlights-card[data-photo-id="{picked_id}"]')
    expect(card).to_be_visible(timeout=5000)
    expect(card).to_have_class(re.compile(r"\bpick-flag-card\b"))
    expect(card.locator(".pick-flag-badge")).to_be_visible()
    expect(card.locator(".pick-flag-badge")).to_have_text("Pick")


def test_highlights_lightbox_pick_updates_card_without_reload(live_server, page):
    """Picking/unpicking from the lightbox must refresh the card DOM in place.

    Regression for Codex feedback on PR #1176: the highlights
    ``lightbox:flagchanged`` handler only reacted to ``rejected`` (and
    previously-rejected) transitions, so the new Pick badge/outline never
    appeared or disappeared until a full reload.
    """
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    page.goto(f"{live_server['url']}/highlights", timeout=5000)
    hawk_section = page.locator("section.bucket").filter(has_text="Red-tailed Hawk")
    first_card = hawk_section.locator(".highlights-card").nth(0)
    expect(first_card).to_be_visible(timeout=5000)
    first_pid = int(first_card.get_attribute("data-photo-id"))

    # Nothing picked yet — the badge/class must be absent.
    expect(first_card).not_to_have_class(re.compile(r"\bpick-flag-card\b"))
    expect(
        page.locator(f'.highlights-card[data-photo-id="{first_pid}"] .pick-flag-badge')
    ).to_have_count(0)

    first_card.click()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    page.wait_for_function(
        "pid => _lightboxCurrentId === pid",
        arg=first_pid,
        timeout=3000,
    )

    page.keyboard.press("p")

    assert _wait_for_flag(db, first_pid, "flagged") == "flagged"
    refreshed = page.locator(f'.highlights-card[data-photo-id="{first_pid}"]')
    expect(refreshed).to_have_class(re.compile(r"\bpick-flag-card\b"), timeout=5000)
    expect(refreshed.locator(".pick-flag-badge")).to_be_visible()

    # Clearing the pick from the lightbox must also refresh the card DOM.
    page.keyboard.press("u")

    assert _wait_for_flag(db, first_pid, "none") == "none"
    cleared = page.locator(f'.highlights-card[data-photo-id="{first_pid}"]')
    expect(cleared).not_to_have_class(re.compile(r"\bpick-flag-card\b"), timeout=5000)
    expect(cleared.locator(".pick-flag-badge")).to_have_count(0)


def test_highlights_lightbox_pick_hidden_photo_promotes_to_visible(live_server, page):
    """Picking a preloaded-but-hidden photo from the lightbox must promote it
    into the visible slice, not leave it hidden until a reload.

    Regression for Codex feedback on PR #1176: the ``lightbox:flagchanged``
    handler only mutated ``pickedPhoto.flag`` and rerendered the existing
    array order. So a photo picked from beyond the ``perRow`` slice (the
    backend preloads up to 20 per bucket but the grid shows 5 by default)
    kept its new Pick badge invisible until a full refetch, even though the
    server's ``picked_first`` sort would have promoted it.
    """
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    hawk_kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("Red-tailed Hawk",)
    ).fetchone()["id"]
    folder_id = data["folders"][0]
    extras = []
    for i in range(8):
        pid = db.add_photo(
            folder_id=folder_id,
            filename=f"extra-hawk-{i}.jpg",
            extension=".jpg",
            file_size=1000,
            file_mtime=1.0,
            timestamp=f"2024-03-11T08:{i:02d}:00",
        )
        db.conn.execute(
            "UPDATE photos SET quality_score = ? WHERE id = ?",
            (0.5 - i * 0.01, pid),
        )
        db.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, hawk_kid),
        )
        extras.append(pid)
    db.conn.commit()

    page.goto(f"{live_server['url']}/highlights", timeout=5000)
    hawk_section = page.locator("section.bucket").filter(has_text="Red-tailed Hawk")
    expect(hawk_section.locator(".highlights-card").first).to_be_visible(timeout=5000)

    visible_ids = hawk_section.locator(".highlights-card").evaluate_all(
        "cards => cards.map(c => Number(c.getAttribute('data-photo-id')))"
    )
    # perRow default is 5; extra hawks (8) plus seeded hawks (3) = 11 in the
    # bucket, so at least one preloaded photo is hidden past the slice.
    assert len(visible_ids) == 5
    hidden_pid = next(pid for pid in extras if pid not in set(visible_ids))

    # Open the lightbox on the first visible card so `_lightboxPhotoList` is
    # populated with the full bucket, then jump to the hidden photo.
    hawk_section.locator(".highlights-card").nth(0).click()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    page.evaluate(
        "pid => openLightbox(pid, '', _lightboxPhotoList)",
        hidden_pid,
    )
    page.wait_for_function(
        "pid => _lightboxCurrentId === pid",
        arg=hidden_pid,
        timeout=3000,
    )

    page.keyboard.press("p")

    assert _wait_for_flag(db, hidden_pid, "flagged") == "flagged"
    promoted = page.locator(f'.highlights-card[data-photo-id="{hidden_pid}"]')
    expect(promoted).to_be_visible(timeout=5000)
    expect(promoted).to_have_class(re.compile(r"\bpick-flag-card\b"))
    expect(promoted.locator(".pick-flag-badge")).to_be_visible()


def test_highlights_species_search_filters_buckets(live_server, page):
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    page.goto(f"{live_server['url']}/highlights", timeout=5000)
    expect(page.locator(".highlights-card").first).to_be_visible(timeout=5000)

    with page.expect_response(
        lambda r: "/api/highlights?" in r.url and "species=robin" in r.url.lower()
    ):
        page.locator("#speciesSearch").fill("robin")
    expect(page.locator(".bucket-title")).to_have_count(1)
    expect(page.locator(".bucket-title").first).to_contain_text("American Robin")
    assert not any(
        "Red-tailed Hawk" in title
        for title in page.locator(".bucket-title").all_inner_texts()
    )


def test_highlights_general_search_filters_by_filename_folder_and_keyword(live_server, page):
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    perch_kid = db.add_keyword("Perched portrait")
    db.tag_photo(data["photos"][1], perch_kid)
    db.conn.commit()

    page.goto(f"{live_server['url']}/highlights", timeout=5000)
    expect(page.locator(".highlights-card").first).to_be_visible(timeout=5000)

    search = page.locator("#highlightSearch")
    with page.expect_response(
        lambda r: "/api/highlights?" in r.url and "q=hawk1" in r.url.lower()
    ):
        search.fill("hawk1")
    expect(page.locator(".highlights-card")).to_have_count(1)
    expect(page.locator(".highlights-card img")).to_have_attribute("alt", "hawk1.jpg")

    with page.expect_response(
        lambda r: "/api/highlights?" in r.url and "q=yard" in r.url.lower()
    ):
        search.fill("yard")
    expect(page.locator(".bucket-title")).to_have_count(1)
    expect(page.locator(".bucket-title").first).to_contain_text("American Robin")
    expect(page.locator(".highlights-card")).to_have_count(2)

    with page.expect_response(
        lambda r: "/api/highlights?" in r.url and "q=perched" in r.url.lower()
    ):
        search.fill("perched")
    expect(page.locator(".highlights-card")).to_have_count(1)
    expect(page.locator(".highlights-card img")).to_have_attribute("alt", "hawk2.jpg")


def test_highlights_unidentified_search_includes_low_confidence_predictions(live_server):
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    pid = db.add_photo(
        folder_id=data["folders"][0],
        filename="low-conf-bird.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=1.0,
        timestamp="2024-03-10T08:03:00",
    )
    db.conn.execute("UPDATE photos SET quality_score = 0.65 WHERE id = ?", (pid,))
    det_id = db.save_detections(
        pid,
        [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
            "confidence": 0.95,
            "category": "animal",
        }],
        detector_model="test-detector",
    )[0]
    db.add_prediction(
        detection_id=det_id,
        species="Low-confidence Sparrow",
        confidence=0.55,
        model="BioCLIP-2",
    )
    db.conn.commit()

    base = live_server["url"]
    with urlopen(f"{base}/api/highlights?scope=workspace&q=unidentified") as resp:
        payload = json.load(resp)

    filenames = {p["filename"] for p in payload["unidentified"]["photos"]}
    assert "low-conf-bird.jpg" in filenames


def test_ordered_highlight_updates_top_photo_timestamp(live_server):
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    highlighted = data["photos"][2]
    db.add_species_highlight("Red-tailed Hawk", highlighted)

    base = live_server["url"]
    with urlopen(f"{base}/api/highlights?scope=workspace") as resp:
        payload = json.load(resp)

    hawk = next(b for b in payload["buckets"] if b["species"] == "Red-tailed Hawk")
    assert hawk["photos"][0]["id"] == highlighted
    assert hawk["photos"][0]["is_highlighted"] is True
    assert hawk["best_timestamp"] == "2024-03-10T08:02:00"


def test_highlights_search_recomputes_bucket_accepted_status(live_server):
    """Filtering a mixed bucket down to only confirmed photos must flip
    ``is_accepted`` to True so the bucket loses the candidate badge and the
    Confirmed-first sort places it above unconfirmed rows."""
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    # Add a prediction-only photo that lands in the same "Red-tailed Hawk"
    # bucket (predicted confidence above the default 0.70 threshold) so the
    # bucket is a mix of confirmed (keyword-tagged) and unconfirmed photos.
    pid = db.add_photo(
        folder_id=data["folders"][0],
        filename="predicted-hawk.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=1.0,
        timestamp="2024-03-10T08:05:00",
    )
    db.conn.execute("UPDATE photos SET quality_score = 0.7 WHERE id = ?", (pid,))
    det_id = db.save_detections(
        pid,
        [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
            "confidence": 0.95,
            "category": "animal",
        }],
        detector_model="test-detector",
    )[0]
    db.add_prediction(
        detection_id=det_id,
        species="Red-tailed Hawk",
        confidence=0.9,
        model="BioCLIP-2",
    )
    db.conn.commit()

    base = live_server["url"]

    # Without a filter, the mixed bucket is unconfirmed at the bucket level.
    with urlopen(f"{base}/api/highlights?scope=workspace") as resp:
        payload = json.load(resp)
    hawk = next(b for b in payload["buckets"] if b["species"] == "Red-tailed Hawk")
    assert hawk["is_accepted"] is False
    assert hawk["certainty"] != "confirmed"

    # Filtering by filename to only include a confirmed (keyword-tagged) photo
    # must recompute ``is_accepted`` from the filtered photos.
    with urlopen(f"{base}/api/highlights?scope=workspace&q=hawk1") as resp:
        payload = json.load(resp)
    hawk = next(b for b in payload["buckets"] if b["species"] == "Red-tailed Hawk")
    assert hawk["photo_count"] == 1
    assert hawk["is_accepted"] is True
    assert hawk["certainty"] == "confirmed"


def test_highlights_lightbox_reject_advances_and_can_restore(live_server, page):
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    page.goto(f"{live_server['url']}/highlights", timeout=5000)
    hawk_section = page.locator("section.bucket").filter(has_text="Red-tailed Hawk")
    first_card = hawk_section.locator(".highlights-card").nth(0)
    second_card = hawk_section.locator(".highlights-card").nth(1)
    expect(first_card).to_be_visible(timeout=5000)
    expect(second_card).to_be_visible(timeout=5000)
    first_pid = int(first_card.get_attribute("data-photo-id"))
    second_pid = int(second_card.get_attribute("data-photo-id"))

    first_card.click()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    page.wait_for_function(
        "pid => _lightboxCurrentId === pid",
        arg=first_pid,
        timeout=3000,
    )

    page.keyboard.press("x")

    assert _wait_for_flag(db, first_pid, "rejected") == "rejected"
    page.wait_for_function(
        "pid => _lightboxCurrentId === pid",
        arg=second_pid,
        timeout=3000,
    )
    expect(page.locator(f'.highlights-card[data-photo-id="{first_pid}"]')).to_have_count(0)
    expect(page.locator("#highlightUndo")).to_have_class(re.compile(r"\bopen\b"))

    page.locator('#highlightLightboxPanel button[data-lb-action="undo-reject"]').click()

    assert _wait_for_flag(db, first_pid, "none") == "none"
    expect(page.locator(f'.highlights-card[data-photo-id="{first_pid}"]')).to_have_count(
        1,
        timeout=5000,
    )
    expect(page.locator("#highlightUndo")).not_to_have_class(re.compile(r"\bopen\b"))


def test_highlights_lightbox_next_preserves_pending_one_to_one_zoom(live_server, page):
    """Highlights lightbox navigation must carry a pending 1:1 zoom intent."""
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    page.goto(f"{live_server['url']}/highlights", timeout=5000)
    hawk_section = page.locator("section.bucket").filter(has_text="Red-tailed Hawk")
    expect(hawk_section.locator(".highlights-card").first).to_be_visible(timeout=5000)
    hawk_section.locator(".highlights-card").first.click()
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    expect(page.locator("#lightboxCounter")).to_contain_text("1 /")

    # Leave the first photo at a true 1:1 zoom, and pre-seed the *next* photo's
    # cached viewport as un-zoomed (fit). Navigation must override that cache and
    # carry the 1:1 intent forward, not honor the stale fit state.
    #
    # The 1:1 handoff is established synchronously by lightboxNav -> openLightbox;
    # only the later async /api/photos + image-load callbacks settle it (and, for
    # a real high-res photo, resolve the pending flag into an actual 1:1 zoom).
    # The seeded photos have no on-disk file or dimensions, so that settling is
    # degenerate and timing-dependent. Trigger the nav and read the handoff state
    # in the same synchronous tick — before any async callback runs — so we test
    # exactly the guarantee (intent carried across navigation) deterministically.
    handoff = page.evaluate(
        """() => {
            const next = window._lightboxPhotoList[1];
            window._lbNativeZoom = 2;
            window._lbZoom = 2;
            window._lbPending1To1 = false;
            window._lbViewportByPhotoId[String(next.id)] = {
                zoom: 1,
                centerX: 0.5,
                centerY: 0.5,
                oneToOne: false,
                pending1To1: false,
            };
            lightboxNav(1);
            return {
                counter: document.getElementById('lightboxCounter').textContent,
                pending1To1: window._lbPending1To1,
                zoom: window._lbZoom,
                srcKey: window._lbCurrentSrcKey,
            };
        }"""
    )

    assert "2 /" in handoff["counter"]
    assert handoff["pending1To1"] is True
    assert handoff["zoom"] > 1.001
    assert handoff["srcKey"] == "original"


def test_highlights_api_limits_initial_bucket_and_loads_more(live_server):
    db = live_server["db"]
    data = live_server["data"]
    _seed_quality_scores_and_species(db, data)

    hawk_kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("Red-tailed Hawk",)
    ).fetchone()["id"]
    folder_id = data["folders"][0]
    for i in range(25):
        pid = db.add_photo(
            folder_id=folder_id,
            filename=f"extra-hawk-{i}.jpg",
            extension=".jpg",
            file_size=1000,
            file_mtime=1.0,
            timestamp=f"2024-03-11T08:{i:02d}:00",
        )
        db.conn.execute(
            "UPDATE photos SET quality_score = ? WHERE id = ?",
            (0.7 - i * 0.001, pid),
        )
        db.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, hawk_kid),
        )
    db.conn.commit()

    base = live_server["url"]
    with urlopen(f"{base}/api/highlights?scope=workspace&limit_per_bucket=5") as resp:
        payload = json.load(resp)
    hawk = next(b for b in payload["buckets"] if b["species"] == "Red-tailed Hawk")
    assert hawk["photo_count"] == 28
    assert hawk["loaded_count"] == 5
    assert hawk["has_more"] is True
    assert len(hawk["photos"]) == 5

    species = quote("Red-tailed Hawk")
    with urlopen(
        f"{base}/api/highlights/bucket?scope=workspace&species={species}&offset=5&limit=10"
    ) as resp:
        chunk = json.load(resp)
    assert chunk["photo_count"] == 28
    assert chunk["loaded_count"] == 15
    assert chunk["has_more"] is True
    assert len(chunk["photos"]) == 10
