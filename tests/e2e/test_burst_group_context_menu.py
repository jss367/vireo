import pytest
from playwright.sync_api import expect


def _seed_burst_group(db, group_id="grp-test-1", model="test-classifier"):
    """Promote the fixture's three hawk predictions into a single burst group.

    ``seed_e2e_data`` in conftest.py creates three hawk photos with one
    prediction each (species=Red-tailed Hawk, classifier_model=test-classifier).
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
