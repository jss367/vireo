"""The Browse detail panel says who set a photo's flag and why.

A reviewer that records a flag with ``source`` and ``reason`` (an agent
adjudicating a burst) leaves a note the user can read under the flag
buttons; the user's own click carries no provenance and clears it.
"""
from playwright.sync_api import expect


def test_detail_panel_shows_the_reviewer_note_until_the_user_reflags(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    from services.photo_review import PhotoReviewService

    service = PhotoReviewService(db)
    for pid in live_server["data"]["photos"]:
        service.set_flag(
            pid, "rejected", source="claude-fable-5-1", reason="eye soft at 1:1, focus on branch",
        )

    page.goto(f"{url}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    note = page.locator("#detailFlagNote")
    expect(note).to_be_visible()
    expect(note).to_contain_text("Rejected — claude-fable-5-1: eye soft at 1:1, focus on branch")
    expect(page.locator(".detail-flag-btn").nth(2)).to_have_class("detail-flag-btn active-reject")

    # The user's own flag has no source or reason, so the note goes away.
    page.locator(".detail-flag-btn", has_text="Flag (P)").click()
    expect(note).to_be_hidden()
    expect(page.locator(".detail-flag-btn").nth(1)).to_have_class("detail-flag-btn active-flag")
    filename = page.locator("#detailFilename").text_content()
    pid = next(p["id"] for p in db.photos.list_page() if p["filename"] == filename)
    assert db.photos.get(pid)["flag"] == "flagged"
