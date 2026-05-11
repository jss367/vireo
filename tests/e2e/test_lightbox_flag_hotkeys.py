"""E2E tests for the lightbox flag/reject/unflag keyboard shortcuts.

The lightbox lives in `_navbar.html` and is shared across pages. Pressing
`p` / `x` / `u` while the lightbox is open should flag / reject / unflag the
displayed photo, regardless of whether the host page defines `setFlagFor`
(browse), `setReviewFlag` (review), or neither (misses, pipeline-review).
"""
import time


def _open_lightbox_on_browse(page, url):
    page.goto(f"{url}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.dblclick()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    page.wait_for_function(
        "typeof _lightboxCurrentId !== 'undefined' && _lightboxCurrentId !== null",
        timeout=3000,
    )


def _current_lightbox_id(page):
    return page.evaluate("_lightboxCurrentId")


def _wait_for_flag(db, photo_id, expected, timeout=3.0):
    """Poll the DB until the photo's flag matches `expected`. The keydown
    handler dispatches the flag write fire-and-forget, so the round-trip is
    not synchronous from Playwright's perspective."""
    deadline = time.time() + timeout
    flag = None
    while time.time() < deadline:
        photo = db.get_photo(photo_id)
        flag = photo["flag"] if photo else None
        if flag == expected:
            return flag
        time.sleep(0.05)
    return flag


def test_lightbox_x_rejects_photo(live_server, page):
    """Pressing `x` in the lightbox sets flag=rejected on the displayed photo."""
    url = live_server["url"]
    db = live_server["db"]
    _open_lightbox_on_browse(page, url)
    pid = _current_lightbox_id(page)

    page.keyboard.press("x")

    flag = _wait_for_flag(db, pid, "rejected")
    assert flag == "rejected", f"expected 'rejected', got {flag!r}"


def test_lightbox_p_flags_photo(live_server, page):
    """Pressing `p` in the lightbox sets flag=flagged on the displayed photo."""
    url = live_server["url"]
    db = live_server["db"]
    _open_lightbox_on_browse(page, url)
    pid = _current_lightbox_id(page)

    page.keyboard.press("p")

    flag = _wait_for_flag(db, pid, "flagged")
    assert flag == "flagged", f"expected 'flagged', got {flag!r}"


def test_lightbox_u_unflags_photo(live_server, page):
    """Pressing `u` in the lightbox clears the flag on the displayed photo."""
    url = live_server["url"]
    db = live_server["db"]
    _open_lightbox_on_browse(page, url)
    pid = _current_lightbox_id(page)

    # Pre-flag the photo so we can verify that `u` clears it.
    db.update_photo_flag(pid, "flagged")
    assert db.get_photo(pid)["flag"] == "flagged"

    page.keyboard.press("u")

    flag = _wait_for_flag(db, pid, "none")
    assert flag == "none", f"expected 'none', got {flag!r}"
