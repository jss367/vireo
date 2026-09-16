"""The photo the user clicked stays on screen when the window is resized."""

from e2e.stack_seed import seed_browse_stack

CARD_VISIBILITY = """
() => {
  const card = document.querySelector('.grid-card.selected');
  if (!card) return null;
  const view = document.getElementById('gridContainer').getBoundingClientRect();
  const rect = card.getBoundingClientRect();
  return {
    fullyVisible: rect.top >= view.top - 1 && rect.bottom <= view.bottom + 1,
    card: {top: rect.top, bottom: rect.bottom},
    view: {top: view.top, bottom: view.bottom},
  };
}
"""

# Playwright returns from clicks and viewport changes before the next paint.
# Selecting a photo shows the batch bar, which resizes the grid too. Let its
# observer finish before a deliberate scroll, or that pending resize can
# undo the scroll using the visibility sampled before the click.
SETTLE = "() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))"

# Scrolling the selection out of view is only established once the grid's own
# scroll handler has sampled it. The event is dispatched asynchronously, so
# without this wait the sample can land after the next interaction and record
# that interaction's card instead.
SCROLL_SAMPLED = "() => focusedCardWasOnScreen === false"


def _seed_extra_photos(live_server, count):
    """Enough photos that a column-count change cannot fit the grid on one screen."""
    import os

    from PIL import Image

    db = live_server["db"]
    thumb_dir = live_server["app"].config["THUMB_CACHE_DIR"]
    folder_id = live_server["data"]["folders"][0]
    for i in range(count):
        pid = db.add_photo(
            folder_id=folder_id, filename=f"extra{i}.jpg", extension=".jpg",
            file_size=1000, file_mtime=1.0,
            timestamp=f"2024-03-11T{8 + i // 60:02d}:{i % 60:02d}:00",
        )
        Image.new("RGB", (100, 100), color="blue").save(
            os.path.join(thumb_dir, f"{pid}.jpg")
        )

def test_browse_keeps_clicked_photo_visible_when_window_resizes(live_server, page):
    # The grid is an `auto-fill` grid, so narrowing the window drops the column
    # count and reflows every card onto a different row. Without a correction
    # the photo the user clicked scrolls out from under them.
    page.set_viewport_size({"width": 1400, "height": 900})
    page.goto(live_server["url"] + "/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    cards.last.click()
    assert page.evaluate("selectedPhotoId") is not None
    assert page.evaluate(CARD_VISIBILITY)["fullyVisible"] is True

    wide_columns = page.evaluate("() => gridContainer.clientWidth")
    page.set_viewport_size({"width": 780, "height": 900})
    page.wait_for_function(
        "width => gridContainer.clientWidth < width", arg=wide_columns
    )
    page.evaluate(SETTLE)

    visibility = page.evaluate(CARD_VISIBILITY)
    assert visibility["fullyVisible"] is True, (
        f"the clicked photo left the grid viewport after the resize: {visibility}"
    )


def test_browse_resize_keeps_the_most_recently_clicked_card_visible(live_server, page):
    # A cmd-click extends the selection but leaves selectedPhotoId on the
    # earlier detail focus. The card the user just clicked is the one they are
    # looking at, so that is the one the resize has to keep on screen.
    page.set_viewport_size({"width": 1400, "height": 900})
    page.goto(live_server["url"] + "/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    cards.first.click()
    cards.last.click(modifiers=["ControlOrMeta"])
    last_id = page.evaluate("lastClickedPhotoId")
    assert page.evaluate("selectedPhotoId") != last_id

    wide = page.evaluate("() => gridContainer.clientWidth")
    page.set_viewport_size({"width": 780, "height": 900})
    page.wait_for_function("width => gridContainer.clientWidth < width", arg=wide)
    page.evaluate(SETTLE)

    visibility = page.evaluate(
        """id => {
          const card = document.querySelector(`.grid-card[data-id="${id}"]`);
          const view = document.getElementById('gridContainer').getBoundingClientRect();
          const rect = card.getBoundingClientRect();
          return {fullyVisible: rect.top >= view.top - 1 && rect.bottom <= view.bottom + 1,
                  card: {top: rect.top, bottom: rect.bottom},
                  view: {top: view.top, bottom: view.bottom}};
        }""",
        last_id,
    )
    assert visibility["fullyVisible"] is True, (
        f"the cmd-clicked photo left the grid viewport after the resize: {visibility}"
    )


def test_browse_resize_leaves_a_deliberately_scrolled_away_selection_alone(live_server, page):
    # Selecting a photo and then scrolling somewhere else is a deliberate move.
    # A later resize must not haul the grid back to that selection — the reflow
    # is not what put the card off screen.
    page.set_viewport_size({"width": 780, "height": 900})
    page.goto(live_server["url"] + "/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    cards.first.click()

    page.evaluate(SETTLE)
    page.evaluate("() => { gridContainer.scrollTop = gridContainer.scrollHeight; }")
    page.wait_for_function(SCROLL_SAMPLED)
    scrolled_away = page.evaluate("() => gridContainer.scrollTop")
    assert scrolled_away > 0
    assert page.evaluate(CARD_VISIBILITY)["fullyVisible"] is False

    tall = page.evaluate("() => gridContainer.clientHeight")
    page.set_viewport_size({"width": 780, "height": 700})
    page.wait_for_function("h => gridContainer.clientHeight < h", arg=tall)
    page.evaluate(SETTLE)

    after = page.evaluate("() => gridContainer.scrollTop")
    assert after == scrolled_away, (
        f"the resize scrolled back to the selection ({scrolled_away} → {after})"
    )


def test_browse_resize_falls_back_when_the_last_clicked_card_is_not_rendered(live_server, page):
    # Collapsing a stack after a stack-wide "Select all" leaves the last-clicked
    # member selected but unrendered, with selectedPhotoId pinned to the visible
    # cover. Preferring the hidden member would find no element and disable the
    # correction for the card the user can actually see.
    page.set_viewport_size({"width": 1400, "height": 900})
    page.goto(live_server["url"] + "/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    cards.last.click()
    visible_id = page.evaluate("selectedPhotoId")

    # Stand in for the collapsed stack member: selected, last clicked, no card.
    page.evaluate(
        """() => {
          lastClickedPhotoId = 999999;
          selectedPhotos.add(999999);
          noteFocusedCardVisibility();
        }"""
    )
    assert page.evaluate("focusedBrowsePhotoId()") == visible_id

    wide = page.evaluate("() => gridContainer.clientWidth")
    page.set_viewport_size({"width": 780, "height": 900})
    page.wait_for_function("width => gridContainer.clientWidth < width", arg=wide)
    page.evaluate(SETTLE)

    visibility = page.evaluate(CARD_VISIBILITY)
    assert visibility["fullyVisible"] is True, (
        f"an unrendered last-clicked photo disabled the correction: {visibility}"
    )


def test_browse_resize_keeps_a_right_clicked_photo_visible(live_server, page):
    # Right-clicking an unselected card coerces the selection Finder-style,
    # outside selectPhoto. That card is on screen and is the one the user is
    # pointing at, so a following resize has to keep it visible even though the
    # previous selection had been scrolled away from.
    _seed_extra_photos(live_server, 40)

    page.set_viewport_size({"width": 780, "height": 900})
    page.goto(live_server["url"] + "/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    cards.first.click()

    page.evaluate(SETTLE)
    # Scroll well away from the selection, then right-click a card that is on
    # screen down here. Not the last card: a reflow that shortens the grid
    # pins the scroll to the bottom, which would keep the final card visible
    # for reasons that have nothing to do with the correction under test.
    page.evaluate("() => { gridContainer.scrollTop = Math.round(gridContainer.scrollHeight * 0.6); }")
    page.wait_for_function(SCROLL_SAMPLED)
    assert page.evaluate(CARD_VISIBILITY)["fullyVisible"] is False
    on_screen_id = page.evaluate(
        """() => {
          const view = gridContainer.getBoundingClientRect();
          const card = Array.from(document.querySelectorAll('.grid-card')).find(c => {
            const r = c.getBoundingClientRect();
            return r.top >= view.top && r.bottom <= view.bottom;
          });
          return card ? Number(card.dataset.id) : null;
        }"""
    )
    assert on_screen_id is not None
    page.locator(f'.grid-card[data-id="{on_screen_id}"]').click(button="right")
    page.keyboard.press("Escape")
    assert page.evaluate("selectedPhotoId") == on_screen_id
    # The coercion has to register as a click on a visible card, or the resize
    # is judged against where the *previous* selection was scrolled to.
    assert page.evaluate("lastClickedPhotoId") == on_screen_id
    assert page.evaluate("focusedCardWasOnScreen") is True

    narrow = page.evaluate("() => gridContainer.clientWidth")
    page.set_viewport_size({"width": 1400, "height": 900})
    page.wait_for_function("width => gridContainer.clientWidth > width", arg=narrow)
    page.evaluate(SETTLE)
    assert page.evaluate("() => gridContainer.scrollTop") > 0, (
        "the reflow clamped the scroll to the top, so this asserts nothing"
    )

    visibility = page.evaluate(CARD_VISIBILITY)
    assert visibility["fullyVisible"] is True, (
        f"the right-clicked photo left the grid viewport after the resize: {visibility}"
    )


def test_browse_resize_keeps_a_collapsed_stack_cover_visible(live_server, page):
    # Collapsing a stack while two of its members are selected deliberately
    # leaves the focus on a hidden member — it is still an active batch target —
    # and moves only the caret to the visible cover. The correction has to
    # resolve that hidden focus to the cover, the way grid navigation does,
    # instead of finding no card and giving up on the cover the user can see.
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    _seed_extra_photos(live_server, 40)

    page.set_viewport_size({"width": 1400, "height": 900})
    page.goto(live_server["url"] + "/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(".grid-card.has-browse-stack").first
    cover.wait_for(state="visible")
    cover_id = int(cover.get_attribute("data-id"))

    cover.locator(".browse-stack-badge").click()
    members = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{cover_id}"] .browse-stack-member'
    )
    members.first.wait_for(state="visible")
    hidden = [
        int(members.nth(i).get_attribute("data-id"))
        for i in range(members.count())
        if int(members.nth(i).get_attribute("data-id")) != cover_id
    ]
    page.locator(f'.browse-stack-member[data-id="{hidden[0]}"]').click()
    page.locator(f'.browse-stack-member[data-id="{hidden[1]}"]').click(
        modifiers=["ControlOrMeta"]
    )
    page.locator(f'.grid-card[data-id="{cover_id}"] .browse-stack-badge').click()

    # Both the focus and the last click are now on unrendered members.
    assert page.evaluate("selectedPhotoId") in hidden
    assert page.evaluate("lastClickedPhotoId") in hidden
    assert page.evaluate("() => !!renderedCardForFocus(focusedBrowsePhotoId())") is True

    page.evaluate("() => noteFocusedCardVisibility()")
    wide = page.evaluate("() => gridContainer.clientWidth")
    page.set_viewport_size({"width": 780, "height": 900})
    page.wait_for_function("width => gridContainer.clientWidth < width", arg=wide)
    page.evaluate(SETTLE)

    visibility = page.evaluate(
        """id => {
          const card = document.querySelector(`.grid-card[data-id="${id}"]`);
          const view = document.getElementById('gridContainer').getBoundingClientRect();
          const rect = card.getBoundingClientRect();
          return {fullyVisible: rect.top >= view.top - 1 && rect.bottom <= view.bottom + 1,
                  card: {top: rect.top, bottom: rect.bottom},
                  view: {top: view.top, bottom: view.bottom}};
        }""",
        cover_id,
    )
    assert visibility["fullyVisible"] is True, (
        f"the collapsed stack cover left the grid viewport after the resize: {visibility}"
    )


def test_browse_thumbnail_size_change_resamples_focus_visibility(live_server, page):
    # The thumbnail-size slider reflows the grid without changing the viewport's
    # box. At the top of the grid it does not fire a scroll event either —
    # there is nothing above the viewport for the browser to anchor — so a
    # reflow that pulls the selection back into view goes unnoticed and the
    # sampled visibility stays stale. A later resize would then refuse to keep
    # a card the user is looking at on screen.
    page.set_viewport_size({"width": 1400, "height": 900})
    page.goto(live_server["url"] + "/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    slider = page.locator("#thumbSizeSlider")
    slider.fill("400")  # one column: the last card sits below the fold
    page.evaluate(SETTLE)
    cards = page.locator(".grid-card")
    cards.last.click()
    page.evaluate(SETTLE)
    page.evaluate("() => { gridContainer.scrollTop = 0; }")
    page.wait_for_function(SCROLL_SAMPLED)
    assert page.evaluate(CARD_VISIBILITY)["fullyVisible"] is False

    slider.fill("120")  # six columns: every card fits in the first rows
    page.evaluate(SETTLE)
    assert page.evaluate("() => gridContainer.scrollTop") == 0
    assert page.evaluate(CARD_VISIBILITY)["fullyVisible"] is True
    assert page.evaluate("focusedCardWasOnScreen") is True, (
        "a grid-only reflow left the sampled visibility stale"
    )
