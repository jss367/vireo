"""E2E tests for per-card flag/reject keyboard shortcuts on /misses.

The misses page already supports `j`/`k` to move focus and `u` to unflag the
focused card. This adds `p` (flag as pick) and `x` (reject) on the focused
card, mirroring the bulk-reject button at a single-photo granularity.
"""
import time


def _seed_miss(db, photo_id, category="no_subject"):
    """Mark a photo as a miss in the given category so /misses returns it."""
    col = {
        "no_subject": "miss_no_subject",
        "clipped": "miss_clipped",
        "oof": "miss_oof",
    }[category]
    db.conn.execute(
        f"UPDATE photos SET {col}=1, miss_computed_at='2026-04-22' WHERE id=?",
        (photo_id,),
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


def test_misses_x_rejects_focused_card(live_server, page):
    """Pressing `x` with a focused miss card sets flag=rejected on that photo."""
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_miss(db, pid, "no_subject")

    page.goto(f"{url}/misses")
    # Wait until the focusable card for this photo renders.
    card = page.locator(f"[data-testid='miss-card-no_subject-{pid}']")
    card.wait_for(state="visible", timeout=3000)

    # `j` moves focus from "none" to the first card.
    page.keyboard.press("j")
    page.wait_for_function(
        f"document.querySelector('[data-testid=\"miss-card-no_subject-{pid}\"]')"
        ".classList.contains('focused')",
        timeout=2000,
    )

    page.keyboard.press("x")

    flag = _wait_for_flag(db, pid, "rejected")
    assert flag == "rejected", f"expected 'rejected', got {flag!r}"


def test_misses_p_flags_focused_card(live_server, page):
    """Pressing `p` with a focused miss card sets flag=flagged on that photo."""
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_miss(db, pid, "clipped")

    page.goto(f"{url}/misses")
    card = page.locator(f"[data-testid='miss-card-clipped-{pid}']")
    card.wait_for(state="visible", timeout=3000)

    page.keyboard.press("j")
    page.wait_for_function(
        f"document.querySelector('[data-testid=\"miss-card-clipped-{pid}\"]')"
        ".classList.contains('focused')",
        timeout=2000,
    )

    page.keyboard.press("p")

    flag = _wait_for_flag(db, pid, "flagged")
    assert flag == "flagged", f"expected 'flagged', got {flag!r}"


def test_misses_x_no_focus_does_nothing(live_server, page):
    """`x` with no focused card must not flag anything."""
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_miss(db, pid, "oof")

    page.goto(f"{url}/misses")
    page.locator(f"[data-testid='miss-card-oof-{pid}']").wait_for(
        state="visible", timeout=3000,
    )

    # No `j` press → no focused card.
    page.keyboard.press("x")
    # Give the handler a moment to (not) fire.
    time.sleep(0.3)

    photo = db.get_photo(pid)
    flag = photo["flag"] if photo else None
    # Flag should remain unset (None or 'none').
    assert flag in (None, "none"), f"expected unflagged, got {flag!r}"


def test_misses_honors_modifier_rebind(live_server, page):
    """A modifier-based rebind of `browse.reject` (e.g. alt+x) must trigger
    on /misses. Previously the misses keydown handler returned early on any
    modifier press, so a rebind to alt+x would never fire there even though
    Browse honored it."""
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_miss(db, pid, "no_subject")

    page.goto(f"{url}/misses")
    card = page.locator(f"[data-testid='miss-card-no_subject-{pid}']")
    card.wait_for(state="visible", timeout=3000)

    page.wait_for_function(
        "window._vireoShortcuts && window._vireoShortcuts.browse",
        timeout=3000,
    )
    page.evaluate(
        "window._vireoShortcuts.browse = window._vireoShortcuts.browse || {};"
        "window._vireoShortcuts.browse.reject = 'alt+x';"
    )

    page.keyboard.press("j")
    page.wait_for_function(
        f"document.querySelector('[data-testid=\"miss-card-no_subject-{pid}\"]')"
        ".classList.contains('focused')",
        timeout=2000,
    )

    # Bare 'x' should now be a no-op (modifier mismatch).
    page.keyboard.press("x")
    time.sleep(0.3)
    assert db.get_photo(pid)["flag"] in (None, "none")

    # Alt+X should reject the focused card.
    page.keyboard.press("Alt+x")
    flag = _wait_for_flag(db, pid, "rejected")
    assert flag == "rejected", f"expected 'rejected' after Alt+X, got {flag!r}"
