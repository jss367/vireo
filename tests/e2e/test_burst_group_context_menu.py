import pytest
from playwright.sync_api import expect


def _seed_burst_group(db, group_id="grp-test-1", model="BioCLIP-2"):
    """Promote the fixture's three hawk predictions into a single burst group.

    ``seed_e2e_data`` in conftest.py creates three hawk photos with one
    prediction each (species=Red-tailed Hawk, classifier_model=BioCLIP-2).
    Group state (``group_id``, ``vote_count``, ``total_votes``) lives in the
    workspace-scoped ``prediction_review`` table, so we upsert one row per
    prediction in the active workspace. Setting a non-null ``quality_score``
    on each photo is what makes the /review page render a group card whose
    button (``button[data-group-id]``) opens the burst modal via
    ``openGroupReview``.

    Returns the number of predictions that joined the group (usually 3).
    """
    rows = db.conn.execute(
        """SELECT pr.id, d.photo_id
             FROM predictions pr
             JOIN detections d ON d.id = pr.detection_id
            WHERE pr.species = 'Red-tailed Hawk'
              AND pr.classifier_model = ?""",
        (model,),
    ).fetchall()
    if not rows:
        return 0
    total = len(rows)
    ws_id = db._active_workspace_id
    for i, row in enumerate(rows):
        db.conn.execute(
            """INSERT INTO prediction_review
                 (prediction_id, workspace_id, status,
                  group_id, vote_count, total_votes)
               VALUES (?, ?, 'pending', ?, ?, ?)
               ON CONFLICT(prediction_id, workspace_id)
               DO UPDATE SET group_id    = excluded.group_id,
                             vote_count  = excluded.vote_count,
                             total_votes = excluded.total_votes""",
            (row["id"], ws_id, group_id, total, total),
        )
        # Give each photo a different quality score so the modal has a
        # deterministic AI-best pick.
        db.conn.execute(
            "UPDATE photos SET quality_score = ?, subject_sharpness = ? WHERE id = ?",
            (0.5 + 0.1 * i, 100 + 10 * i, row["photo_id"]),
        )
    db.conn.commit()
    return total


def _open_burst_modal(page):
    """Click the group card button and wait for #grmOverlay to become visible."""
    trigger = page.locator("button[data-group-id]").first
    expect(trigger).to_be_visible()
    trigger.click()
    # Wait for the modal's open state by polling for a rendered card.
    page.locator("#grmOverlay .grm-card[data-photo-id]").first.wait_for(
        state="visible", timeout=2000
    )


def _dispatch_contextmenu(locator):
    locator.evaluate(
        "el => el.dispatchEvent(new MouseEvent('contextmenu', "
        "{clientX: 100, clientY: 100, bubbles: true, cancelable: true}))"
    )


def test_burst_card_right_click_opens_menu(live_server, page):
    """Right-clicking a burst-modal card opens the context menu."""
    n = _seed_burst_group(live_server["db"])
    if n < 1:
        pytest.skip("could not seed burst group")

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    card = page.locator("#grmOverlay .grm-card[data-photo-id]").first
    card.wait_for(state="visible")
    _dispatch_contextmenu(card)

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in (
        "Move to Picks",
        "Move to Rejects",
        "Move to Candidates",
        "Open in Lightbox",
        "Open in Browse Mode",
        "Reveal in",
        "Copy Path",
        "Remove from Group",
    ):
        expect(menu.locator(".vireo-ctx-item", has_text=label)).to_be_visible()


def test_burst_right_click_force_selects_card(live_server, page):
    """Right-click must set grmState.selected to the clicked card's photo_id.

    Otherwise the move/remove actions (which operate on grmState.selected)
    would act on the wrong card when the user right-clicks a non-selected
    card.
    """
    n = _seed_burst_group(live_server["db"])
    if n < 2:
        pytest.skip("need at least 2 burst cards for this test")

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    cards = page.locator("#grmOverlay .grm-card[data-photo-id]")
    assert cards.count() >= 2

    # Click card 0 to seed the selection, then right-click a DIFFERENT card
    # and confirm grmState.selected flipped to that card's photo_id.
    first_pid = cards.nth(0).get_attribute("data-photo-id")
    cards.nth(0).click()
    # Find a card whose data-photo-id differs from the initial selection.
    target_card = None
    target_pid = None
    for i in range(cards.count()):
        pid = cards.nth(i).get_attribute("data-photo-id")
        if pid != first_pid:
            target_card = cards.nth(i)
            target_pid = pid
            break
    assert target_card is not None
    _dispatch_contextmenu(target_card)

    selected = page.evaluate("grmState.selected")
    assert str(selected) == target_pid


def test_burst_native_open_browse_uses_selected_card(live_server, page):
    """Native Photo > Open in Browse should understand burst modal selection."""
    n = _seed_burst_group(live_server["db"])
    if n < 1:
        pytest.skip("could not seed burst group")

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    page.locator("#grmOverlay .grm-card[data-photo-id]").first.wait_for(
        state="visible", timeout=2000,
    )
    pid = page.evaluate("String(grmState.selected)")
    assert pid and pid != "null"

    page.evaluate("window.handleNativeMenuCommand('photo_open_browse')")

    page.wait_for_function(
        "expectedPid => location.pathname === '/browse'"
        " && new URLSearchParams(location.search).get('photo_id') === expectedPid",
        arg=pid,
        timeout=5000,
    )


def test_burst_menu_has_chip_rows(live_server, page):
    """Burst menu includes rating chips (0-5) and flag chips (3)."""
    n = _seed_burst_group(live_server["db"])
    if n < 1:
        pytest.skip("could not seed burst group")

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    card = page.locator("#grmOverlay .grm-card[data-photo-id]").first
    _dispatch_contextmenu(card)

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    # 6 rating chips + 3 flag chips = 9.
    assert menu.locator(".vireo-ctx-chip").count() >= 9


def test_burst_move_to_picks_updates_state(live_server, page):
    """Clicking 'Move to Picks' on a right-clicked card adds it to grmState.picks."""
    n = _seed_burst_group(live_server["db"])
    if n < 2:
        pytest.skip("need at least 2 burst cards")

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    cards = page.locator("#grmOverlay .grm-card[data-photo-id]")
    # Pick a card that is NOT already the auto-selected AI-best to guarantee
    # a state change when we click 'Move to Picks'.
    initially_selected = page.evaluate("grmState.selected")
    target_card = None
    target_pid = None
    for i in range(cards.count()):
        pid = cards.nth(i).get_attribute("data-photo-id")
        if str(initially_selected) != pid:
            target_card = cards.nth(i)
            target_pid = pid
            break
    assert target_card is not None

    _dispatch_contextmenu(target_card)
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(".vireo-ctx-item", has_text="Move to Picks").click()

    in_picks = page.evaluate(
        f"Array.from(grmState.picks).includes({target_pid})"
    )
    assert in_picks is True


def test_burst_multi_selected_cards_drag_together(live_server, page):
    """Ctrl/Cmd-style multi-selection should pan selected burst cards together."""
    n = _seed_burst_group(live_server["db"])
    if n < 2:
        pytest.skip("need at least 2 burst cards")

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    cards = page.locator("#grmOverlay .grm-card[data-photo-id]")
    assert cards.count() >= 2
    first_pid = cards.nth(0).get_attribute("data-photo-id")
    second_pid = cards.nth(1).get_attribute("data-photo-id")
    assert first_pid and second_pid

    # Start from an empty selection so this exercises normal click + additive
    # Cmd-style additive click rather than depending on the modal's
    # auto-selected AI best.
    page.evaluate(
        """() => {
          grmState.selected = null;
          grmState.selectedIds.clear();
          grmState.selectionAnchor = null;
          renderGroupModal();
        }"""
    )

    first = page.locator(f'#grmOverlay .grm-card[data-photo-id="{first_pid}"]')
    second = page.locator(f'#grmOverlay .grm-card[data-photo-id="{second_pid}"]')
    first.click()
    second.click(modifiers=["Meta"])

    selected = page.evaluate("Array.from(grmState.selectedIds).map(String).sort()")
    assert selected == sorted([first_pid, second_pid])

    bbox = first.bounding_box()
    assert bbox is not None
    x = bbox["x"] + bbox["width"] / 2
    y = bbox["y"] + bbox["height"] / 2
    page.mouse.move(x, y)
    page.mouse.down()
    page.mouse.move(x + 30, y + 12)
    page.mouse.up()

    offsets = page.evaluate(
        """([a, b]) => ({
          a: _grmOffsets[a],
          b: _grmOffsets[b],
        })""",
        [first_pid, second_pid],
    )
    assert offsets["a"] and offsets["b"]
    assert abs(offsets["a"]["tx"] - offsets["b"]["tx"]) < 0.01
    assert abs(offsets["a"]["ty"] - offsets["b"]["ty"]) < 0.01
    assert abs(offsets["a"]["tx"]) > 0
    assert abs(offsets["a"]["ty"]) > 0


def test_burst_loupe_drag_offsets_selected_cards_only(live_server, page):
    """Dragging the right preview should nudge only the selected burst cards."""
    n = _seed_burst_group(live_server["db"])
    if n < 2:
        pytest.skip("need at least 2 burst cards")

    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")
    _open_burst_modal(page)

    cards = page.locator("#grmOverlay .grm-card[data-photo-id]")
    assert cards.count() >= 2
    first_pid = cards.nth(0).get_attribute("data-photo-id")
    second_pid = cards.nth(1).get_attribute("data-photo-id")
    third_pid = cards.nth(2).get_attribute("data-photo-id") if cards.count() >= 3 else None
    assert first_pid and second_pid

    page.evaluate(
        """() => {
          grmState.selected = null;
          grmState.selectedIds.clear();
          grmState.selectionAnchor = null;
          renderGroupModal();
        }"""
    )

    page.locator(f'#grmOverlay .grm-card[data-photo-id="{first_pid}"]').click()
    selected_pids = [first_pid]
    untouched_pid = second_pid
    if third_pid:
        page.locator(f'#grmOverlay .grm-card[data-photo-id="{second_pid}"]').click(modifiers=["Meta"])
        selected_pids.append(second_pid)
        untouched_pid = third_pid
    selected = page.evaluate("Array.from(grmState.selectedIds).map(String).sort()")
    assert selected == sorted(selected_pids)

    loupe = page.locator("#grmLoupeImg")
    bbox = loupe.bounding_box()
    assert bbox is not None
    x = bbox["x"] + bbox["width"] / 2
    y = bbox["y"] + bbox["height"] / 2
    page.mouse.move(x, y)
    page.mouse.down()
    drag_started = page.evaluate("_grmLoupeAlignDragging ? _grmLoupeAlignDragging.targets.length : 0")
    assert drag_started == len(selected_pids)
    page.mouse.move(x + 28, y + 14)
    page.mouse.up()

    offsets = page.evaluate(
        """([selected, untouched]) => ({
          selected: selected.map((pid) => _grmOffsets[pid]),
          untouched: _grmOffsets[untouched],
          locked: _grmLoupeLocked,
        })""",
        [selected_pids, untouched_pid],
    )
    assert all(offsets["selected"])
    assert offsets["untouched"] is None
    assert offsets["locked"] is False
    first_offset = offsets["selected"][0]
    for offset in offsets["selected"][1:]:
        assert abs(first_offset["tx"] - offset["tx"]) < 0.01
        assert abs(first_offset["ty"] - offset["ty"]) < 0.01
    assert abs(first_offset["tx"]) > 0
    assert abs(first_offset["ty"]) > 0


def test_review_card_menu_has_no_burst_items(live_server, page):
    """The ordinary review-card menu must not expose burst-only actions.

    Guards against event-listener collision: the burst contextmenu handler
    keys on ``.grm-card[data-photo-id]`` and must NOT fire for regular
    ``.card[data-pred-id]`` elements.
    """
    url = live_server["url"]
    page.goto(f"{url}/review")
    page.wait_for_load_state("networkidle")

    card = page.locator(".card[data-pred-id]").first
    if card.count() == 0:
        pytest.skip("no review cards seeded")
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    # Move to Picks is unique to the burst menu; it must not be here.
    assert menu.locator(".vireo-ctx-item", has_text="Move to Picks").count() == 0
    assert menu.locator(".vireo-ctx-item", has_text="Remove from Group").count() == 0
