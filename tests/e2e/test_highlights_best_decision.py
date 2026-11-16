"""Highlights says why each species' first photo leads.

A species row carries one line under its title: the photo was chosen (by
whom, why), several chosen photos were never compared, or nobody chose and
Vireo ordered the row. Setting a representative from the page settles a tie,
and with one folder selected it claims only that folder.
"""
import re

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
        "First photo: ranked by Vireo's score; nobody has chosen one here",
        timeout=5000,
    )

    with db.transaction():
        choose_best(db, "Red-tailed Hawk", hawks[2], over=[], source="claude",
                    reason="wings fully spread")
    page.reload()
    expose_page_modules(page, "highlights", "highlightsState")

    expect(_hawk_row(page).locator(".bucket-decision")).to_have_text(
        "First photo: chosen as representative by claude (wings fully spread)",
        timeout=5000,
    )
    first = _hawk_row(page).locator(".highlights-card").first
    assert int(first.get_attribute("data-photo-id")) == hawks[2]


def test_setting_a_representative_settles_a_tie_within_the_folder(live_server, page):
    db = live_server["db"]
    hawks = _seed_hawks(db, live_server["data"])
    # Two hawks chosen on their own, never against each other.
    with db.transaction():
        choose_best(db, "Red-tailed Hawk", hawks[0], over=[])
        choose_best(db, "Red-tailed Hawk", hawks[1], over=[])
    park_id = db.conn.execute(
        "SELECT folder_id FROM photos WHERE id = ?", (hawks[0],)
    ).fetchone()["folder_id"]

    _goto_highlights(page, live_server["url"])
    line = _hawk_row(page).locator(".bucket-decision")
    expect(line).to_contain_text(
        "the newest of 2 representatives here that were never compared", timeout=5000,
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
    page.locator('[data-lb-action="set-representative"]').click()

    expect(page.locator("#toastContainer")).to_contain_text(
        "Representative set, chosen over 1 other representative in this folder",
        timeout=5000,
    )
    page.keyboard.press("Escape")
    expect(_hawk_row(page).locator(".bucket-decision")).to_have_text(
        "First photo: chosen as representative", timeout=5000,
    )
    first = _hawk_row(page).locator(".highlights-card").first
    assert int(first.get_attribute("data-photo-id")) == hawks[0]
    comparisons = [(c.better, c.worse) for c in db.species_choices.comparisons()]
    assert comparisons == [(hawks[0], hawks[1])]
