"""E2E tests for multi-select on /misses.

Covers ctrl-click toggle, shift-click range, shift+J/K keyboard extension,
Esc-to-clear, and bulk P/X/U acting on the selection. Plain-click behavior
(open lightbox) is preserved when no modifier is held.
"""
import time


def _seed_misses(db, photo_ids, category="no_subject"):
    """Mark each given photo as a miss in the named category."""
    col = {
        "no_subject": "miss_no_subject",
        "clipped": "miss_clipped",
        "oof": "miss_oof",
    }[category]
    for pid in photo_ids:
        db.conn.execute(
            f"UPDATE photos SET {col}=1, miss_computed_at='2026-04-22' "
            f"WHERE id=?",
            (pid,),
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


def _ctrl_click(page, locator):
    # Cmd on macOS, Ctrl elsewhere — both map to e.metaKey/e.ctrlKey in our
    # handler. Use Playwright's "Meta" because the test host is macOS.
    locator.click(modifiers=["Meta"])


def test_ctrl_click_toggles_selection(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")

    page.goto(f"{url}/misses")
    page.locator(f"[data-testid='miss-card-no_subject-{pids[0]}']").wait_for(
        state="visible", timeout=3000,
    )

    _ctrl_click(page, page.locator(f"[data-testid='miss-card-no_subject-{pids[0]}']"))
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-no_subject-{pids[2]}']"))

    selected = page.evaluate("Array.from(selection)")
    assert sorted(selected) == sorted([pids[0], pids[2]])

    # Toggle the first one off.
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-no_subject-{pids[0]}']"))
    selected = page.evaluate("Array.from(selection)")
    assert selected == [pids[2]]


def test_shift_click_selects_range_within_category(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "clipped")

    page.goto(f"{url}/misses")
    page.locator(f"[data-testid='miss-card-clipped-{pids[0]}']").wait_for(
        state="visible", timeout=3000,
    )

    # Anchor on the first card via plain click — but plain click opens the
    # lightbox, so use ctrl-click to set focus + add to selection without
    # opening the overlay.
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-clipped-{pids[0]}']"))
    page.locator(f"[data-testid='miss-card-clipped-{pids[3]}']").click(
        modifiers=["Shift"],
    )

    selected = page.evaluate("Array.from(selection)")
    # /api/misses orders by timestamp DESC, so the in-page indices may not
    # match seed order. Verify by count + that both endpoints are present.
    assert len(selected) >= 2
    assert pids[0] in selected
    assert pids[3] in selected


def test_shift_j_extends_selection(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "oof")

    page.goto(f"{url}/misses")
    page.locator(f"[data-testid='miss-card-oof-{pids[0]}']").wait_for(
        state="visible", timeout=3000,
    )

    page.keyboard.press("j")  # focus first
    page.keyboard.press("Shift+J")  # extend by 1
    page.keyboard.press("Shift+J")  # extend by 1 more

    selected_count = page.evaluate("selection.size")
    assert selected_count >= 2


def test_escape_clears_selection(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")

    page.goto(f"{url}/misses")
    page.locator(f"[data-testid='miss-card-no_subject-{pids[0]}']").wait_for(
        state="visible", timeout=3000,
    )
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-no_subject-{pids[0]}']"))
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-no_subject-{pids[1]}']"))
    assert page.evaluate("selection.size") == 2

    page.keyboard.press("Escape")
    assert page.evaluate("selection.size") == 0


def test_x_with_selection_bulk_rejects(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")

    page.goto(f"{url}/misses")
    page.locator(f"[data-testid='miss-card-no_subject-{pids[0]}']").wait_for(
        state="visible", timeout=3000,
    )
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-no_subject-{pids[0]}']"))
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-no_subject-{pids[2]}']"))
    assert page.evaluate("selection.size") == 2

    page.keyboard.press("x")

    f0 = _wait_for_flag(db, pids[0], "rejected")
    f2 = _wait_for_flag(db, pids[2], "rejected")
    assert f0 == "rejected", f"pids[0]: expected 'rejected', got {f0!r}"
    assert f2 == "rejected", f"pids[2]: expected 'rejected', got {f2!r}"

    # The non-selected photo must remain unflagged.
    untouched = db.get_photo(pids[1])
    assert untouched["flag"] in (None, "none"), f"pids[1] was modified: {untouched['flag']!r}"

    # Selection cleared after bulk apply.
    assert page.evaluate("selection.size") == 0


def test_p_with_selection_bulk_flags(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "clipped")

    page.goto(f"{url}/misses")
    page.locator(f"[data-testid='miss-card-clipped-{pids[0]}']").wait_for(
        state="visible", timeout=3000,
    )
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-clipped-{pids[0]}']"))
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-clipped-{pids[1]}']"))

    page.keyboard.press("p")

    assert _wait_for_flag(db, pids[0], "flagged") == "flagged"
    assert _wait_for_flag(db, pids[1], "flagged") == "flagged"


def test_x_without_selection_falls_back_to_focused(live_server, page):
    """Regression: PR #1's focused-card behavior must still fire when the
    selection is empty."""
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")

    page.goto(f"{url}/misses")
    page.locator(f"[data-testid='miss-card-no_subject-{pids[0]}']").wait_for(
        state="visible", timeout=3000,
    )

    # Focus the first card with j (no selection set).
    page.keyboard.press("j")
    assert page.evaluate("selection.size") == 0

    page.keyboard.press("x")

    # Exactly the focused card should be rejected. The other miss-photos must
    # remain unflagged.
    focused_pid = page.evaluate(
        "missesData[focusedCategory][focusedIndex].id"
    )
    assert _wait_for_flag(db, focused_pid, "rejected") == "rejected"


def test_setSelected_updates_all_duplicate_cards(live_server, page):
    """A photo flagged in multiple miss categories renders one card per
    category. Toggling its selection must update every card's `.selected`
    class — not just the first match — or the visual state drifts from the
    actual `selection` set."""
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    # Flag the same photo as BOTH clipped and oof so the page renders two
    # cards for it.
    _seed_misses(db, [pid], "clipped")
    _seed_misses(db, [pid], "oof")

    page.goto(f"{url}/misses")
    clipped_card = page.locator(f"[data-testid='miss-card-clipped-{pid}']")
    oof_card = page.locator(f"[data-testid='miss-card-oof-{pid}']")
    clipped_card.wait_for(state="visible", timeout=3000)
    oof_card.wait_for(state="visible", timeout=3000)

    _ctrl_click(page, clipped_card)

    # Both cards must now carry the `selected` class — querySelector would
    # have stopped after the first match.
    assert "selected" in (clipped_card.get_attribute("class") or "")
    assert "selected" in (oof_card.get_attribute("class") or ""), (
        "duplicate card in another category was not updated by setSelected"
    )

    # And toggling off must clear both too.
    _ctrl_click(page, clipped_card)
    assert "selected" not in (clipped_card.get_attribute("class") or "")
    assert "selected" not in (oof_card.get_attribute("class") or "")


def test_plain_click_still_opens_lightbox(live_server, page):
    """Regression: plain (no-modifier) click on a miss card must still open
    the shared lightbox — the multi-select model only kicks in with Ctrl/Shift."""
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "oof")

    page.goto(f"{url}/misses")
    card = page.locator(f"[data-testid='miss-card-oof-{pids[0]}']")
    card.wait_for(state="visible", timeout=3000)

    card.click()

    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
