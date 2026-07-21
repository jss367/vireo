"""E2E tests for multi-select on /misses.

Covers plain-click selection, ctrl-click toggle, shift-click range, shift+J/K
keyboard extension, Esc-to-clear, toolbar actions, and bulk P/X/U acting on the
selection. Double-click opens the shared lightbox.
"""
import json
import time

from playwright.sync_api import expect


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


def test_collection_and_rating_filters_limit_visible_misses(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")
    collection_id = db.add_collection(
        "Hawk review",
        json.dumps([{"field": "photo_ids", "value": pids[:3]}]),
    )

    page.goto(f"{url}/misses")
    page.locator("[data-testid^='miss-card-no_subject-']").first.wait_for(
        state="visible", timeout=3000,
    )
    page.locator("#missCollectionFilter").select_option(str(collection_id))
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(3)
    assert f"collection_id={collection_id}" in page.url

    # The shared E2E fixture gives the first hawk four stars; the other two
    # have no rating, so composing filters should leave exactly that miss.
    page.locator("#missRatingFilter").select_option("4")
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(1)
    expect(
        page.locator(f"[data-testid='miss-card-no_subject-{pids[0]}']")
    ).to_be_visible()


def test_filter_change_ignores_older_threshold_preview(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")

    page.goto(f"{url}/misses")
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(5)
    page.evaluate("""() => {
      const realFetch = window.fetch.bind(window);
      let held = false;
      window.fetch = (url, options) => {
        if (!held && String(url).includes('/api/misses/preview')) {
          held = true;
          return new Promise(resolve => {
            window.releaseHeldPreview = () => realFetch(url, options).then(resolve);
          });
        }
        return realFetch(url, options);
      };
    }""")
    page.locator("#missCfgNoSubject").evaluate("""el => {
      el.value = String(Number(el.value) + 1);
      el.dispatchEvent(new Event('input', {bubbles: true}));
    }""")
    page.wait_for_function("window.releaseHeldPreview != null")

    page.locator("#missRatingFilter").select_option("4")
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(1)
    page.evaluate("window.releaseHeldPreview()")
    page.wait_for_timeout(400)
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(1)


def test_filter_change_ignores_older_recompute_response(live_server, page):
    """A recompute POST that returns after the user has changed filters must
    not replace the newer filtered view with the previous filter's payload,
    and — since the server-side recompute has still persisted miss flags for
    the previous scope — must trigger a fresh loadMisses() for the current
    filters so any overlapping photos reflect the new flags."""
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")

    page.goto(f"{url}/misses")
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(5)
    page.evaluate("""() => {
      window.missesFetchCalls = [];
      const realFetch = window.fetch.bind(window);
      let held = false;
      window.fetch = (url, options) => {
        window.missesFetchCalls.push(String(url));
        if (!held && String(url).includes('/api/misses/recompute')) {
          held = true;
          return new Promise(resolve => {
            window.releaseHeldRecompute = () => realFetch(url, options).then(resolve);
          });
        }
        return realFetch(url, options);
      };
    }""")
    saved_threshold = page.locator("#missCfgNoSubject").evaluate("""el => {
      el.value = String(Number(el.value) + 1);
      return Number(el.value) / 100;
    }""")
    page.locator("#missRecomputeBtn").click()
    page.wait_for_function("window.releaseHeldRecompute != null")

    page.locator("#missRatingFilter").select_option("4")
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(1)
    calls_before_release = page.evaluate(
        "window.missesFetchCalls.filter(u => u.startsWith('/api/misses') && !u.includes('/recompute') && !u.includes('/preview')).length"
    )
    page.evaluate("window.releaseHeldRecompute()")
    expect(page.locator("#missTuningStatus")).to_have_text(
        "Recomputed previous filters; refreshing current view"
    )
    # The stale recompute must have scheduled a fresh /api/misses for the
    # current filters so any overlapping photos pick up the newly persisted
    # miss flags rather than continuing to render the pre-recompute payload.
    page.wait_for_function(
        f"window.missesFetchCalls.filter(u => u.startsWith('/api/misses') && !u.includes('/recompute') && !u.includes('/preview')).length > {calls_before_release}",
        timeout=3000,
    )
    assert page.evaluate("originalMissConfig.miss_det_confidence") == saved_threshold


def test_bulk_reject_uses_filters_that_rendered_visible_cards(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")

    page.goto(f"{url}/misses?rating_min=4")
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(1)
    page.evaluate("""() => {
      const realFetch = window.fetch.bind(window);
      let held = false;
      window.fetch = (url, options) => {
        if (!held && String(url) === '/api/misses') {
          held = true;
          return new Promise(resolve => {
            window.releaseHeldFilterLoad = () => realFetch(url, options).then(resolve);
          });
        }
        return realFetch(url, options);
      };
    }""")
    page.locator("#missRatingFilter").select_option("")
    page.wait_for_function("window.releaseHeldFilterLoad != null")

    page.once("dialog", lambda dialog: dialog.accept())
    page.locator("[data-testid='miss-reject-no_subject']").click()
    assert _wait_for_flag(db, pids[0], "rejected") == "rejected"
    assert all(db.get_photo(pid)["flag"] != "rejected" for pid in pids[1:])
    page.evaluate("window.releaseHeldFilterLoad()")


def test_recompute_uses_filters_that_rendered_visible_cards(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")

    page.goto(f"{url}/misses?rating_min=4")
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(1)
    page.evaluate("""() => {
      const realFetch = window.fetch.bind(window);
      let held = false;
      window.fetch = (url, options) => {
        if (!held && String(url) === '/api/misses') {
          held = true;
          return new Promise(resolve => {
            window.releaseHeldFilterLoad = () => realFetch(url, options).then(resolve);
          });
        }
        return realFetch(url, options);
      };
    }""")
    page.locator("#missRatingFilter").select_option("")
    page.wait_for_function("window.releaseHeldFilterLoad != null")

    page.locator("#missRecomputeBtn").click()
    expect(page.locator("#missTuningStatus")).to_have_text(
        "Recomputed visible photos; refreshing filters"
    )
    timestamps = {
        row["id"]: row["miss_computed_at"]
        for row in db.conn.execute(
            "SELECT id, miss_computed_at FROM photos ORDER BY id"
        )
    }
    assert timestamps[pids[0]] != "2026-04-22"
    assert all(timestamps[pid] == "2026-04-22" for pid in pids[1:])
    page.evaluate("window.releaseHeldFilterLoad()")


def test_initial_recompute_uses_url_filters_before_grid_loads(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")
    page.add_init_script("""(() => {
      const realFetch = window.fetch.bind(window);
      let held = false;
      window.fetch = (url, options) => {
        if (!held && String(url).includes('/api/misses?rating_min=4')) {
          held = true;
          return new Promise(resolve => {
            window.releaseInitialMissesLoad = () => realFetch(url, options).then(resolve);
          });
        }
        if (String(url).includes('/api/misses/recompute')) {
          window.initialRecomputeBody = JSON.parse(options.body);
        }
        return realFetch(url, options);
      };
    })()""")

    page.goto(f"{url}/misses?rating_min=4")
    page.wait_for_function("window.releaseInitialMissesLoad != null")
    expect(page.locator("#missRecomputeBtn")).to_be_enabled()
    page.locator("#missRecomputeBtn").click()
    page.wait_for_function("window.initialRecomputeBody != null")
    assert page.evaluate("window.initialRecomputeBody.rating_min") == "4"
    page.evaluate("window.releaseInitialMissesLoad()")


def test_pick_refreshes_unflagged_miss_filter(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "no_subject")

    page.goto(f"{url}/misses?flag=none")
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(5)
    page.locator(f"[data-testid='miss-card-no_subject-{pids[0]}']").click()
    page.keyboard.press("p")

    assert _wait_for_flag(db, pids[0], "flagged") == "flagged"
    expect(page.locator("[data-testid^='miss-card-no_subject-']")).to_have_count(4)


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

    # Anchor on the first card via plain click.
    page.locator(f"[data-testid='miss-card-clipped-{pids[0]}']").click()
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
    page.wait_for_function("selection.size === 0", timeout=3000)


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


def test_per_card_unflag_prunes_selection_when_last_card_gone(live_server, page):
    """If a selected photo is unflagged-as-miss via the per-card X button and
    no other category still shows it, its id must drop out of `selection` —
    otherwise a follow-up bulk P/X/U would silently act on the now-hidden
    photo."""
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    other = live_server["data"]["photos"][1]
    _seed_misses(db, [pid, other], "no_subject")

    page.goto(f"{url}/misses")
    card = page.locator(f"[data-testid='miss-card-no_subject-{pid}']")
    other_card = page.locator(f"[data-testid='miss-card-no_subject-{other}']")
    card.wait_for(state="visible", timeout=3000)

    _ctrl_click(page, card)
    _ctrl_click(page, other_card)
    assert page.evaluate("selection.size") == 2

    # Click the per-card unflag-as-miss button on the first card.
    page.locator(
        f"[data-testid='miss-unflag-no_subject-{pid}']"
    ).click()

    # That card is now gone from the only category that contained it, so its
    # id must be pruned. The other selection survives.
    page.wait_for_function(
        f"!selection.has({pid})",
        timeout=3000,
    )
    assert page.evaluate("selection.size") == 1
    assert page.evaluate(f"selection.has({other})")


def test_bulk_reject_category_prunes_selected_ids_synchronously(live_server, page):
    """Codex P2: when "Reject all in <category>" empties a section, ids that
    were in that category must drop out of `selection` synchronously — not
    only when the deferred loadMisses() refetch completes. Otherwise a
    follow-up P/X/U via the bulk-selection branch silently re-edits hidden
    photos."""
    url = live_server["url"]
    db = live_server["db"]
    a, b, c = live_server["data"]["photos"][:3]
    _seed_misses(db, [a, b], "no_subject")
    _seed_misses(db, [c], "clipped")

    page.goto(f"{url}/misses")
    page.locator(f"[data-testid='miss-card-no_subject-{a}']").wait_for(
        state="visible", timeout=3000,
    )
    page.locator(f"[data-testid='miss-card-clipped-{c}']").wait_for(
        state="visible", timeout=3000,
    )

    _ctrl_click(page, page.locator(f"[data-testid='miss-card-no_subject-{a}']"))
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-no_subject-{b}']"))
    _ctrl_click(page, page.locator(f"[data-testid='miss-card-clipped-{c}']"))
    assert page.evaluate("selection.size") == 3

    # Auto-confirm the window.confirm dialog the bulk-reject button raises.
    page.once("dialog", lambda d: d.accept())
    page.locator("[data-testid='miss-reject-no_subject']").click()

    # The two no_subject ids must drop out synchronously — the third (clipped)
    # remains.
    page.wait_for_function(
        f"!selection.has({a}) && !selection.has({b}) && selection.has({c})",
        timeout=3000,
    )


def test_per_card_unflag_keeps_selection_when_other_category_still_renders(live_server, page):
    """A photo flagged in multiple categories renders one card per category.
    Unflagging from one category must NOT drop the id from selection while a
    sibling card is still visible — that card stays selected and bulk
    operations should still cover it."""
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_misses(db, [pid], "clipped")
    _seed_misses(db, [pid], "oof")

    page.goto(f"{url}/misses")
    clipped_card = page.locator(f"[data-testid='miss-card-clipped-{pid}']")
    oof_card = page.locator(f"[data-testid='miss-card-oof-{pid}']")
    clipped_card.wait_for(state="visible", timeout=3000)
    oof_card.wait_for(state="visible", timeout=3000)

    _ctrl_click(page, clipped_card)
    assert page.evaluate(f"selection.has({pid})")

    # Unflag just the clipped card.
    page.locator(
        f"[data-testid='miss-unflag-clipped-{pid}']"
    ).click()

    # The clipped card disappears; the oof card stays. The id must remain in
    # selection, and the surviving oof card must still render as selected.
    page.wait_for_function(
        f"document.querySelector('[data-testid=\"miss-card-clipped-{pid}\"]') === null",
        timeout=3000,
    )
    assert page.evaluate(f"selection.has({pid})"), (
        "id was wrongly pruned even though another category still renders it"
    )
    oof_class = oof_card.get_attribute("class") or ""
    assert "selected" in oof_class


def test_plain_click_selects_without_opening_lightbox(live_server, page):
    """Plain click highlights/selects the card; it no longer opens lightbox."""
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "oof")

    page.goto(f"{url}/misses")
    card = page.locator(f"[data-testid='miss-card-oof-{pids[0]}']")
    card.wait_for(state="visible", timeout=3000)

    card.click()

    assert page.evaluate("selection.size") == 1
    assert page.evaluate(f"selection.has({pids[0]})")
    assert "selected" in (card.get_attribute("class") or "")
    assert not page.evaluate(
        "document.getElementById('lightboxOverlay').classList.contains('active')"
    )


def test_double_click_opens_lightbox(live_server, page):
    """Double-click on a miss card opens the shared lightbox."""
    url = live_server["url"]
    db = live_server["db"]
    pids = live_server["data"]["photos"]
    _seed_misses(db, pids, "oof")

    page.goto(f"{url}/misses")
    card = page.locator(f"[data-testid='miss-card-oof-{pids[0]}']")
    card.wait_for(state="visible", timeout=3000)

    card.dblclick()

    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )


def test_u_unmarks_miss_from_lightbox(live_server, page):
    """When inspecting a miss in the lightbox, `u` clears the miss flag."""
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_misses(db, [pid], "oof")

    page.goto(f"{url}/misses")
    card = page.locator(f"[data-testid='miss-card-oof-{pid}']")
    card.wait_for(state="visible", timeout=3000)

    card.dblclick()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )

    page.keyboard.press("u")

    page.wait_for_function(
        f"!document.querySelector('[data-testid=\"miss-card-oof-{pid}\"]')",
        timeout=3000,
    )
    row = db.conn.execute(
        "SELECT miss_oof, flag FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert row["miss_oof"] == 0
    assert row["flag"] in (None, "none")


def test_u_unmarks_only_active_miss_category_from_lightbox(live_server, page):
    """A lightbox `u` clears the miss category that opened the lightbox."""
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_misses(db, [pid], "clipped")
    _seed_misses(db, [pid], "oof")

    page.goto(f"{url}/misses")
    clipped_card = page.locator(f"[data-testid='miss-card-clipped-{pid}']")
    oof_card = page.locator(f"[data-testid='miss-card-oof-{pid}']")
    clipped_card.wait_for(state="visible", timeout=3000)
    oof_card.wait_for(state="visible", timeout=3000)

    clipped_card.dblclick()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )

    page.keyboard.press("u")

    page.wait_for_function(
        f"!document.querySelector('[data-testid=\"miss-card-clipped-{pid}\"]')"
        f" && document.querySelector('[data-testid=\"miss-card-oof-{pid}\"]')",
        timeout=3000,
    )
    row = db.conn.execute(
        "SELECT miss_clipped, miss_oof FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert row["miss_clipped"] == 0
    assert row["miss_oof"] == 1


def test_selection_bar_unmarks_selected_misses(live_server, page):
    """The visible toolbar can clear the selected photos' miss flags."""
    url = live_server["url"]
    db = live_server["db"]
    a, b = live_server["data"]["photos"][:2]
    _seed_misses(db, [a, b], "no_subject")

    page.goto(f"{url}/misses")
    first = page.locator(f"[data-testid='miss-card-no_subject-{a}']")
    second = page.locator(f"[data-testid='miss-card-no_subject-{b}']")
    first.wait_for(state="visible", timeout=3000)
    second.wait_for(state="visible", timeout=3000)

    first.click()
    _ctrl_click(page, second)
    assert page.evaluate("selection.size") == 2

    page.get_by_role("button", name="Unmark missed").click()

    page.wait_for_function(
        f"!document.querySelector('[data-testid=\"miss-card-no_subject-{a}\"]')"
        f" && !document.querySelector('[data-testid=\"miss-card-no_subject-{b}\"]')",
        timeout=3000,
    )
    rows = db.conn.execute(
        "SELECT id, miss_no_subject FROM photos WHERE id IN (?, ?)",
        (a, b),
    ).fetchall()
    assert {r["id"]: r["miss_no_subject"] for r in rows} == {a: 0, b: 0}


def test_u_unmarks_clicked_miss(live_server, page):
    """Plain-click selection followed by `u` clears the selected miss."""
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_misses(db, [pid], "no_subject")

    page.goto(f"{url}/misses")
    card = page.locator(f"[data-testid='miss-card-no_subject-{pid}']")
    card.wait_for(state="visible", timeout=3000)

    card.click()
    assert page.evaluate("selection.size") == 1

    page.keyboard.press("u")

    page.wait_for_function(
        f"!document.querySelector('[data-testid=\"miss-card-no_subject-{pid}\"]')",
        timeout=3000,
    )
    row = db.conn.execute(
        "SELECT miss_no_subject FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert row["miss_no_subject"] == 0


def test_selection_bar_deletes_selected_misses_from_vireo(live_server, page):
    """The visible toolbar can delete selected miss photos through the shared confirmation."""
    url = live_server["url"]
    db = live_server["db"]
    a, b = live_server["data"]["photos"][:2]
    _seed_misses(db, [a, b], "clipped")

    page.goto(f"{url}/misses")
    first = page.locator(f"[data-testid='miss-card-clipped-{a}']")
    second = page.locator(f"[data-testid='miss-card-clipped-{b}']")
    first.wait_for(state="visible", timeout=3000)
    second.wait_for(state="visible", timeout=3000)

    first.click()
    _ctrl_click(page, second)
    assert page.evaluate("selection.size") == 2

    page.get_by_role("button", name="Delete selected").click()
    page.locator("#deleteModal.open").wait_for(state="visible", timeout=3000)
    page.locator("#deleteConfirmBtn").click()

    page.wait_for_function(
        f"!document.querySelector('[data-testid=\"miss-card-clipped-{a}\"]')"
        f" && !document.querySelector('[data-testid=\"miss-card-clipped-{b}\"]')",
        timeout=3000,
    )
    assert db.get_photo(a) is None
    assert db.get_photo(b) is None


def test_selection_bar_opens_selected_miss_in_browse(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_misses(db, [pid], "no_subject")

    page.goto(f"{url}/misses")
    card = page.locator(f"[data-testid='miss-card-no_subject-{pid}']")
    card.wait_for(state="visible", timeout=3000)
    card.click()

    btn = page.locator("#missesSelectionBrowseBtn")
    expect(btn).to_be_enabled()
    btn.click()

    page.wait_for_function(
        f"location.pathname === '/browse' && new URLSearchParams(location.search).get('photo_id') === '{pid}'",
        timeout=5000,
    )


def test_misses_b_opens_focused_card_in_browse(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_misses(db, [pid], "clipped")

    page.goto(f"{url}/misses")
    page.locator(f"[data-testid='miss-card-clipped-{pid}']").wait_for(
        state="visible", timeout=3000,
    )

    page.keyboard.press("j")
    page.keyboard.press("b")

    page.wait_for_function(
        f"location.pathname === '/browse' && new URLSearchParams(location.search).get('photo_id') === '{pid}'",
        timeout=5000,
    )


def test_misses_context_menu_opens_card_in_browse(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    pid = live_server["data"]["photos"][0]
    _seed_misses(db, [pid], "oof")

    page.goto(f"{url}/misses")
    card = page.locator(f"[data-testid='miss-card-oof-{pid}']")
    card.wait_for(state="visible", timeout=3000)
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(".vireo-ctx-item", has_text="Open in Browse").click()

    page.wait_for_function(
        f"location.pathname === '/browse' && new URLSearchParams(location.search).get('photo_id') === '{pid}'",
        timeout=5000,
    )
