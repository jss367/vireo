"""Highlights says why each species' first photo leads.

A species row carries one line under its title: the photo is the top
highlight there (chosen by whom, why), several highlights were never compared,
or nothing is highlighted and Vireo ordered the row. Making a photo the top
highlight from the page settles a tie, and with one folder selected it claims
only that folder.
"""
import re

import pytest
from playwright.sync_api import expect
from services.species_choices import choose_best

from e2e.module_pages import expose_page_modules

NAVIGATION_TIMEOUT_MS = 10_000


def _seed_hawks(db, data):
    """Score the three park hawks; returns their ids, best score first."""
    hawk_kid = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("Red-tailed Hawk",)
    ).fetchone()["id"]
    hawks = data["photos"][:3]
    for i, pid in enumerate(hawks):
        db.conn.execute(
            "UPDATE photos SET quality_score = ? WHERE id = ?", (0.9 - i * 0.1, pid),
        )
        db.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, hawk_kid),
        )
    db.conn.commit()
    return hawks


def _goto_highlights(page, base_url):
    page.goto(f"{base_url}/highlights", timeout=NAVIGATION_TIMEOUT_MS)
    expose_page_modules(page, "highlights", "highlightsState")


def _hawk_row(page):
    return page.locator("section.bucket").filter(has_text="Red-tailed Hawk")


def test_row_says_vireo_ordered_it_until_someone_chooses(live_server, page):
    db = live_server["db"]
    hawks = _seed_hawks(db, live_server["data"])

    _goto_highlights(page, live_server["url"])

    line = _hawk_row(page).locator(".bucket-decision")
    expect(line).to_have_text(
        "First photo: ranked by Vireo's score; nothing is highlighted here",
        timeout=5000,
    )

    with db.transaction():
        choose_best(db, "Red-tailed Hawk", hawks[2], over=[], source="claude",
                    reason="wings fully spread")
    page.reload()
    expose_page_modules(page, "highlights", "highlightsState")

    expect(_hawk_row(page).locator(".bucket-decision")).to_have_text(
        "First photo: the top highlight here, chosen by claude (wings fully spread)",
        timeout=5000,
    )
    first = _hawk_row(page).locator(".highlights-card").first
    assert int(first.get_attribute("data-photo-id")) == hawks[2]


def test_making_a_top_highlight_settles_a_tie_within_the_folder(live_server, page):
    db = live_server["db"]
    hawks = _seed_hawks(db, live_server["data"])
    # Two hawks highlighted on their own, never against each other.
    with db.transaction():
        choose_best(db, "Red-tailed Hawk", hawks[0], over=[])
        choose_best(db, "Red-tailed Hawk", hawks[1], over=[])
    park_id = db.conn.execute(
        "SELECT folder_id FROM photos WHERE id = ?", (hawks[0],)
    ).fetchone()["folder_id"]

    _goto_highlights(page, live_server["url"])
    line = _hawk_row(page).locator(".bucket-decision")
    expect(line).to_contain_text(
        "the newest of 2 highlights here that were never compared", timeout=5000,
    )
    expect(line).to_have_class(re.compile(r"\btie\b"))
    first = _hawk_row(page).locator(".highlights-card").first
    assert int(first.get_attribute("data-photo-id")) == hawks[1]

    page.select_option("#folderSelect", str(park_id))
    expect(_hawk_row(page).locator(".bucket-decision")).to_contain_text(
        "never compared", timeout=5000,
    )
    _hawk_row(page).locator(f'.highlights-card[data-photo-id="{hawks[0]}"]').click()
    page.wait_for_function(
        "pid => vireoLightboxSession.requestedPhotoId() === pid", arg=hawks[0], timeout=3000,
    )
    button = page.locator('[data-lb-action="make-top-highlight"]')
    expect(button).to_have_text("Make Top Highlight")
    button.click()

    expect(page.locator("#toastContainer")).to_contain_text(
        "Top highlight set, ahead of 1 other highlight in this folder",
        timeout=5000,
    )
    expect(page.locator('[data-lb-action="make-top-highlight"]')).to_have_text(
        "Top Highlight", timeout=5000,
    )
    page.keyboard.press("Escape")
    expect(_hawk_row(page).locator(".bucket-decision")).to_have_text(
        "First photo: the top highlight here", timeout=5000,
    )
    first = _hawk_row(page).locator(".highlights-card").first
    assert int(first.get_attribute("data-photo-id")) == hawks[0]
    comparisons = [(c.better, c.worse) for c in db.species_choices.comparisons()]
    assert comparisons == [(hawks[0], hawks[1])]


def test_decision_is_below_title_and_metadata(live_server, page):
    _seed_hawks(live_server["db"], live_server["data"])
    page.set_viewport_size({"width": 1440, "height": 900})
    _goto_highlights(page, live_server["url"])
    row = _hawk_row(page)
    expect(row.locator(".bucket-decision")).to_be_visible()
    for width in (1440, 768):
        page.set_viewport_size({"width": width, "height": 900})
        decision = row.locator(".bucket-decision").bounding_box()
        for selector in (".bucket-title", ".bucket-meta"):
            preceding = row.locator(selector).bounding_box()
            assert decision["y"] >= preceding["y"] + preceding["height"]


def test_shared_unscoped_top_highlight_button_is_hidden(live_server, page):
    hawks = _seed_hawks(live_server["db"], live_server["data"])
    _goto_highlights(page, live_server["url"])
    _hawk_row(page).locator(f'.highlights-card[data-photo-id="{hawks[0]}"]').click()
    expect(page.locator('[data-lb-action="make-top-highlight"]')).to_be_visible()
    # Wait for the async photo detail, which creates the shared panel later.
    page.wait_for_function(
        "document.querySelector('#lifeListLightboxPanel .lifelist-lb-row') !== null",
    )
    expect(page.locator("#lifeListLightboxPanel")).to_be_hidden()


def test_rejecting_nonleading_tied_highlight_refreshes_decision(live_server, page):
    db = live_server["db"]
    hawks = _seed_hawks(db, live_server["data"])
    with db.transaction():
        choose_best(db, "Red-tailed Hawk", hawks[0], over=[])
        choose_best(db, "Red-tailed Hawk", hawks[1], over=[])
    _goto_highlights(page, live_server["url"])
    page.select_option("#highlightSelectionFilter", "all")
    row = _hawk_row(page)
    expect(row.locator(".bucket-decision")).to_contain_text("newest of 2")
    row.locator(f'.highlights-card[data-photo-id="{hawks[0]}"]').click()
    page.wait_for_function(
        "pid => vireoLightboxSession.requestedPhotoId() === pid", arg=hawks[0],
    )
    page.keyboard.press("x")
    page.keyboard.press("Escape")
    expect(row.locator(f'.highlights-card[data-photo-id="{hawks[0]}"]')).to_have_count(0)
    expect(row.locator(".bucket-decision.tie")).to_have_count(0)
    assert int(row.locator(".highlights-card").first.get_attribute("data-photo-id")) == hawks[1]


@pytest.mark.parametrize("opening", ["right-click", "keyboard"])
def test_folder_highlights_menu_omits_unscoped_top_highlight(live_server, page, opening):
    db = live_server["db"]
    hawks = _seed_hawks(db, live_server["data"])
    park_id = db.conn.execute(
        "SELECT folder_id FROM photos WHERE id = ?", (hawks[0],)
    ).fetchone()["folder_id"]
    page.route(
        "**/photos/*/full**",
        lambda route: route.fulfill(
            body='<svg xmlns="http://www.w3.org/2000/svg" width="200" height="200"/>',
            content_type="image/svg+xml",
        ),
    )
    _goto_highlights(page, live_server["url"])
    page.select_option("#folderSelect", str(park_id))
    _hawk_row(page).locator(f'.highlights-card[data-photo-id="{hawks[0]}"]').click()
    page.wait_for_function(
        """pid => {
            const photo = window._lbPhotoDataByPhoto?.[String(pid)];
            return photo?.life_list?.length > 0;
        }""", arg=hawks[0],
    )
    image = page.locator("#lightboxImg")
    expect(image).to_be_visible()
    if opening == "right-click":
        image.click(button="right")
    else:
        image.focus()
        page.keyboard.press("Shift+F10")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Make Top Highlight")).to_have_count(0)
    expect(menu.locator(".vireo-ctx-item", has_text="Close Lightbox")).to_be_visible()
    page.keyboard.press("Escape")
    expect(page.locator('[data-lb-action="make-top-highlight"]')).to_be_visible()
