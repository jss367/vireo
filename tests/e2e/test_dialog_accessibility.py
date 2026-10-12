"""Keyboard and focus contracts for the shared lifecycle and Browse dialogs."""

from pathlib import Path

import pytest
from playwright.sync_api import expect

STATIC = Path(__file__).resolve().parents[2] / "vireo" / "static"


def mount_dialogs(page):
    page.set_content("""
        <style>.modal-overlay {display:none} .modal-overlay.open {display:block}</style>
        <div id="content">
          <div id="background"><button id="opener" onclick="first.open(this)">Open</button></div>
          <div id="first" class="modal-overlay" role="dialog" aria-label="First">
            <input id="firstInput"><button id="firstLast">Last</button>
          </div>
        </div>
        <div id="second" class="modal-overlay" role="dialog" aria-label="Second">
          <button id="secondButton">Second button</button>
        </div>
        <div id="alreadyInert" inert><button>Unavailable</button></div>
    """)
    page.add_script_tag(path=str(STATIC / "keymap.js"))
    page.add_script_tag(path=str(STATIC / "vireo-dialog.js"))
    page.evaluate("""() => {
      window.dismissals = 0;
      window.escapedToPage = 0;
      window.first = VireoDialog.create(document.getElementById('first'), {
        initialFocus: document.getElementById('firstInput'),
        onDismiss() { dismissals++; first.close(); },
      });
      window.second = VireoDialog.create(document.getElementById('second'));
      document.addEventListener('keydown', event => {
        if (event.key === 'Escape') escapedToPage++;
      });
    }""")
    page.locator("#opener").click()


def test_shared_dialog_contains_tab_and_recomputes_changed_controls(page):
    mount_dialogs(page)
    expect(page.locator("#firstInput")).to_be_focused()
    page.keyboard.press("Shift+Tab")
    expect(page.locator("#firstLast")).to_be_focused()
    page.keyboard.press("Tab")
    expect(page.locator("#firstInput")).to_be_focused()
    page.evaluate("""() => {
      document.getElementById('firstLast').disabled = true;
      document.getElementById('first').insertAdjacentHTML('beforeend',
        '<button hidden>Hidden</button><button style="visibility:hidden">Invisible</button>' +
        '<button id="dynamicLast">New last control</button>');
    }""")
    page.keyboard.press("Tab")
    expect(page.locator("#dynamicLast")).to_be_focused()
    page.keyboard.press("Tab")
    expect(page.locator("#firstInput")).to_be_focused()


def test_shared_dialog_tab_scrolls_focused_control_into_view_inside_dialog(page):
    # Codex P2: a dialog's own scrollable list (e.g. the batch
    # Add-to-Collection chooser's max-height:200px overflow-y:auto area)
    # would otherwise hide the newly focused button when Tab lands below
    # the fold, since the Tab-trap focus() call used preventScroll: true.
    # scrollIntoView('nearest') on the newly focused element must now scroll
    # the dialog's own list, without disturbing already-visible controls.
    mount_dialogs(page)
    page.evaluate("""() => {
      const dialog = document.getElementById('first');
      const list = document.createElement('div');
      list.id = 'tallList';
      list.style.maxHeight = '80px';
      list.style.overflowY = 'auto';
      for (let i = 0; i < 20; i++) {
        const b = document.createElement('button');
        b.id = 'row' + i;
        b.textContent = 'Row ' + i;
        b.style.display = 'block';
        b.style.height = '32px';
        list.appendChild(b);
      }
      dialog.appendChild(list);
      list.scrollTop = 0;
    }""")
    expect(page.locator("#firstInput")).to_be_focused()
    # Tab path: firstInput -> firstLast -> row0 -> row1 -> ... -> row8 is
    # well below the 80px fold, so if the Tab-trap did not scroll it the
    # user would lose the focus indicator inside the dialog.
    for _ in range(10):
        page.keyboard.press("Tab")
    expect(page.locator("#row8")).to_be_focused()
    assert page.evaluate("document.getElementById('tallList').scrollTop") > 0


def test_shared_dialog_inerts_background_and_restores_existing_state(page):
    mount_dialogs(page)
    assert page.evaluate("document.getElementById('background').inert")
    assert not page.evaluate("document.getElementById('content').inert")
    page.evaluate("document.getElementById('opener').focus()")
    expect(page.locator("#firstInput")).to_be_focused()
    page.keyboard.press("Escape")
    expect(page.locator("#first")).to_be_hidden()
    expect(page.locator("#opener")).to_be_focused()
    assert page.evaluate("[dismissals, escapedToPage]") == [1, 0]
    assert not page.evaluate("document.getElementById('background').inert")
    assert page.evaluate("document.getElementById('alreadyInert').inert")
    page.keyboard.press("Escape")
    assert page.evaluate("escapedToPage") == 1


def test_shared_dialog_repeated_open_and_close_do_not_leak_locks_or_escape(page):
    mount_dialogs(page)
    page.evaluate("first.open(); first.open(); first.close(); first.close()")
    expect(page.locator("#opener")).to_be_focused()
    assert page.evaluate("document.body.style.overflow") == ""
    page.keyboard.press("Escape")
    assert page.evaluate("[dismissals, escapedToPage]") == [0, 1]
    page.locator("#opener").click()
    page.keyboard.press("Escape")
    assert page.evaluate("[dismissals, escapedToPage]") == [1, 1]


def test_shared_dialog_stacking_closes_only_top_and_keeps_lower_modal(page):
    mount_dialogs(page)
    page.locator("#firstLast").focus()
    page.evaluate("second.open()")
    expect(page.locator("#secondButton")).to_be_focused()
    assert page.evaluate("document.getElementById('content').inert")
    page.keyboard.press("Escape")
    expect(page.locator("#second")).to_be_hidden()
    expect(page.locator("#firstLast")).to_be_focused()
    assert page.evaluate("document.body.style.overflow") == "hidden"
    assert page.evaluate("document.getElementById('background').inert")
    page.keyboard.press("Escape")
    expect(page.locator("#opener")).to_be_focused()
    assert page.evaluate("[dismissals, escapedToPage]") == [1, 0]


def test_shared_dialog_lower_close_transfers_return_focus_to_original_opener(page):
    mount_dialogs(page)
    page.locator("#firstLast").focus()
    page.evaluate("second.open(); first.close()")
    expect(page.locator("#secondButton")).to_be_focused()
    page.keyboard.press("Escape")
    expect(page.locator("#opener")).to_be_focused()
    assert not page.evaluate("document.getElementById('background').inert")
    assert page.evaluate("document.body.style.overflow") == ""


def test_shared_dialog_visual_stacking_follows_open_order(page):
    mount_dialogs(page)
    page.evaluate("""() => {
      first.close();
      for (const dialog of document.querySelectorAll('.modal-overlay')) {
        Object.assign(dialog.style, {position: 'fixed', inset: '0', zIndex: '500'});
      }
      second.open(document.getElementById('opener'));
      first.open(document.getElementById('secondButton'));
    }""")
    # The first dialog is earlier in the DOM but opened last: it must be on top.
    assert page.locator("#firstInput").evaluate("""el => {
      const rect = el.getBoundingClientRect();
      return document.elementFromPoint(rect.x + rect.width / 2, rect.y + rect.height / 2) === el;
    }""")
    page.keyboard.press("Escape")
    expect(page.locator("#secondButton")).to_be_focused()
    assert page.locator("#first").evaluate("el => el.style.zIndex") == "500"
    page.keyboard.press("Escape")
    expect(page.locator("#opener")).to_be_focused()


def test_shared_dialog_without_enabled_controls_keeps_focus_on_dialog(page):
    mount_dialogs(page)
    page.evaluate("""() => {
      first.close();
      document.getElementById('firstInput').disabled = true;
      document.getElementById('firstLast').hidden = true;
      first.open();
    }""")
    expect(page.locator("#first")).to_be_focused()
    page.keyboard.press("Tab")
    page.keyboard.press("Shift+Tab")
    expect(page.locator("#first")).to_be_focused()
    page.keyboard.press("Escape")
    expect(page.locator("#opener")).to_be_focused()


def open_browse(page, live_server):
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.click()


@pytest.mark.parametrize("dismiss", ["escape", "cancel"])
def test_collection_dialog_keyboard_and_cleanup(live_server, page, dismiss):
    open_browse(page, live_server)
    selection = page.evaluate("getActiveSelection()")
    opener = page.get_by_role("button", name="+ New Collection", exact=True)
    opener.click()
    dialog = page.get_by_role("dialog", name="New Smart Collection", exact=True)
    name = dialog.get_by_role("textbox", name="Collection name", exact=True)
    expect(name).to_be_focused()
    # Fetching options inserts rule controls into the already-open dialog.
    expect(dialog.locator("#ruleRows select").first).to_be_visible()
    cancel = dialog.get_by_role("button", name="Cancel", exact=True)
    name.press("Shift+Tab")
    expect(cancel).to_be_focused()
    cancel.press("Tab")
    expect(name).to_be_focused()
    if dismiss == "escape":
        page.keyboard.press("Escape")
    else:
        cancel.click()
    expect(dialog).to_be_hidden()
    expect(opener).to_be_focused()
    assert page.evaluate("getActiveSelection()") == selection
    assert page.evaluate("[collectionEditingId, _previewTimer]") == [None, None]
    assert not page.locator(".main-layout").evaluate("el => el.inert")


@pytest.mark.parametrize("dismiss", ["escape", "cancel"])
def test_batch_keyword_dialog_returns_focus_without_clearing_selection(live_server, page, dismiss):
    open_browse(page, live_server)
    selection = page.evaluate("getActiveSelection()")
    opener = page.locator("#batchBar").get_by_role("button", name="+ Keyword", exact=True)
    opener.click()
    dialog = page.get_by_role("dialog", name="Add keyword to 1 photos", exact=True)
    keyword = dialog.get_by_role("textbox", name="Keyword name", exact=True)
    expect(keyword).to_be_focused()
    keyword.press("Shift+Tab")
    cancel = dialog.get_by_role("button", name="Cancel", exact=True)
    expect(cancel).to_be_focused()
    cancel.press("Tab")
    expect(keyword).to_be_focused()
    if dismiss == "escape":
        keyword.press("Escape")
    else:
        cancel.click()
    expect(dialog).to_be_hidden()
    expect(opener).to_be_focused()
    expect(page.locator("#batchKeywordSuggestions")).not_to_have_class("keyword-suggestion-list open")
    assert page.evaluate("getActiveSelection()") == selection


def test_batch_collection_choices_work_with_keyboard_and_restore_opener(live_server, page):
    collection_id = live_server["db"].add_collection("Keyboard picks", "[]")
    open_browse(page, live_server)
    opener = page.locator("#batchBar").get_by_role("button", name="+ Collection", exact=True)
    opener.click()
    dialog = page.get_by_role("dialog", name="Add 1 photo(s) to collection", exact=True)
    name = dialog.get_by_role("textbox", name="New collection name", exact=True)
    expect(name).to_be_focused()
    name.press("Shift+Tab")
    choice = dialog.get_by_role("button", name="Keyboard picks", exact=True)
    expect(choice).to_be_focused()
    choice.press("Space")
    expect(choice).to_have_attribute("aria-pressed", "true")
    assert page.evaluate("_batchCollectionSelectedId") == collection_id
    choice.press("Escape")
    expect(dialog).to_be_hidden()
    expect(opener).to_be_focused()


def test_edit_collection_escape_clears_edit_state(live_server, page):
    open_browse(page, live_server)
    opener = page.get_by_role("button", name="+ New Collection", exact=True)
    opener.focus()
    page.evaluate("showCollectionModal({id: 123, name: 'Existing collection', rules: []})")
    dialog = page.get_by_role("dialog", name="Edit Smart Collection", exact=True)
    expect(dialog.get_by_role("textbox", name="Collection name")).to_have_value("Existing collection")
    page.keyboard.press("Escape")
    expect(opener).to_be_focused()
    assert page.evaluate("collectionEditingId") is None


def test_edit_rules_context_menu_restores_focus_to_collection_row(live_server, page):
    # The context menu removes the clicked item before firing its onClick, and
    # the collection row is tabindex=-1 (not in Tab order). Codex P2: without
    # a captured return target the editor falls back to document.body, so Esc
    # cannot restore focus to the row that launched it.
    collection_id = live_server["db"].add_collection("Right-click target", "[]")
    open_browse(page, live_server)
    row = page.locator(
        f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
    )
    row.click(button="right")
    page.get_by_text("Edit Rules", exact=True).click()
    dialog = page.get_by_role("dialog", name="Edit Smart Collection", exact=True)
    expect(dialog.get_by_role("textbox", name="Collection name")).to_have_value("Right-click target")
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    expect(row).to_be_focused()


def test_confirm_batch_collection_moves_focus_to_grid_not_hidden_opener(live_server, page):
    # confirmBatchCollection calls clearSelection() after the dialog closes;
    # that hides the batch bar with display:none, leaving the + Collection
    # opener an invisible focus target. Codex P2: focus must land on a stable
    # page control instead.
    collection_id = live_server["db"].add_collection("Pick target", "[]")
    open_browse(page, live_server)
    opener = page.locator("#batchBar").get_by_role("button", name="+ Collection", exact=True)
    opener.click()
    dialog = page.get_by_role("dialog", name="Add 1 photo(s) to collection", exact=True)
    choice = dialog.get_by_role("button", name="Pick target", exact=True)
    choice.click()
    assert page.evaluate("_batchCollectionSelectedId") == collection_id
    dialog.get_by_role("button", name="Add", exact=True).click()
    expect(dialog).to_be_hidden()
    # The batch bar hides once the selection is cleared, so the opener is no
    # longer visible and would be an invalid focus target.
    expect(page.locator("#batchBar")).to_be_hidden()
    expect(page.locator("#grid")).to_be_focused()


def test_confirm_batch_collection_preserves_focus_moved_during_pending_add(live_server, page):
    # Codex P2: confirmBatchCollection awaits the add-photos request between
    # dialog close and the grid fallback. If the user moved focus to another
    # stable control while that request was pending, the fallback used to
    # steal focus unconditionally. It must now only run when focus landed in
    # the batch bar (invalidated by display:none) or on document.body.
    live_server["db"].add_collection("Pick target", "[]")
    open_browse(page, live_server)
    # Install a JS-level fetch interceptor that stalls the add-photos
    # response until we flip _releasePending. page.route would block the
    # sync Playwright worker (sleep inside the handler stalls the main
    # loop), so stall inside JS instead. Replace window.safeFetch directly:
    # vireo-api.js captures the native fetch and the browser wrapper in
    # closure before exposing them, so patching Vireo.api.fetch/window.fetch
    # does not reach what batch-actions.js actually calls (safeFetch →
    # Vireo.api.json → closed-over browserFetch).
    page.evaluate("""() => {
      const savedSafeFetch = window.safeFetch;
      window._pendingSignalled = false;
      window._releasePending = false;
      window.safeFetch = async function(url, opts, options) {
        if (typeof url === 'string' && url.indexOf('/add-photos') !== -1) {
          window._pendingSignalled = true;
          while (!window._releasePending) {
            await new Promise(r => setTimeout(r, 20));
          }
        }
        return savedSafeFetch(url, opts, options);
      };
    }""")
    page.locator("#batchBar").get_by_role("button", name="+ Collection", exact=True).click()
    dialog = page.get_by_role("dialog", name="Add 1 photo(s) to collection", exact=True)
    dialog.get_by_role("button", name="Pick target", exact=True).click()
    dialog.get_by_role("button", name="Add", exact=True).click()
    expect(dialog).to_be_hidden()
    page.wait_for_function("window._pendingSignalled === true")
    # The user moves focus to another stable, visible control that lives
    # outside #batchBar while the request is pending. A standalone test
    # anchor is injected so the assertion does not depend on the live
    # browse chrome staying stable.
    page.evaluate("""() => {
      const anchor = document.createElement('button');
      anchor.id = 'userChosenAnchor';
      anchor.type = 'button';
      anchor.textContent = 'User moved here';
      document.body.appendChild(anchor);
      anchor.focus({preventScroll: true});
    }""")
    expect(page.locator("#userChosenAnchor")).to_be_focused()
    # Release the stalled request. confirmBatchCollection resolves, runs
    # clearSelection(), and reaches the focus fallback.
    page.evaluate("window._releasePending = true")
    page.wait_for_function("window.getActiveSelection && getActiveSelection().length === 0")
    # Focus must stay on the user's deliberate choice, not snap to #grid.
    expect(page.locator("#userChosenAnchor")).to_be_focused()


def test_f1_help_is_noop_while_a_managed_dialog_is_open(live_server, page):
    # updateBackground() inerts every body-level sibling along the active
    # dialog's branch, including #helpModal. Codex P2: opening help on top
    # of a managed dialog would present an inert overlay whose search input
    # and controls cannot receive focus. F1 (and the help icon) must be a
    # no-op in that state so the user keeps interacting with the dialog
    # until they dismiss it; help opens normally afterward.
    open_browse(page, live_server)
    opener = page.get_by_role("button", name="+ New Collection", exact=True)
    opener.click()
    dialog = page.get_by_role("dialog", name="New Smart Collection", exact=True)
    expect(dialog).to_be_visible()
    page.keyboard.press("F1")
    # Help stays closed; the dialog is still the top overlay and keeps focus.
    expect(page.locator("#helpModal")).not_to_have_class("help-overlay active")
    expect(dialog).to_be_visible()
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    # With no managed dialog on the stack, F1 opens help as usual.
    page.keyboard.press("F1")
    expect(page.locator("#helpModal")).to_have_class("help-overlay active")


def test_cmd_k_command_palette_is_noop_while_a_managed_dialog_is_open(live_server, page):
    # updateBackground() inerts every body-level sibling along the active
    # dialog's branch, including #commandPalette. Codex P2: pressing
    # Ctrl/Cmd+K while a managed dialog is open used to reveal the palette
    # after its asynchronous tab-state refresh and focus its input, but the
    # inert attribute made every control unfocusable and the dialog's focus
    # containment pulled focus back out; the first Escape was consumed by
    # the dialog's Keymap handler, leaving the palette visible until a
    # second Escape. openCommandPalette (and its tab-refresh wrapper) must
    # be a no-op while VireoDialog.hasActive() is true, mirroring the help
    # overlay guard; dismissing the dialog clears the inert state and the
    # palette opens normally afterwards.
    open_browse(page, live_server)
    opener = page.get_by_role("button", name="+ New Collection", exact=True)
    opener.click()
    dialog = page.get_by_role("dialog", name="New Smart Collection", exact=True)
    expect(dialog).to_be_visible()
    page.keyboard.press("ControlOrMeta+K")
    # The palette stays hidden; the dialog is still the top overlay.
    expect(page.locator("#commandPalette")).to_be_hidden()
    expect(dialog).to_be_visible()
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    # With no managed dialog on the stack, Ctrl/Cmd+K opens the palette as
    # usual. openCommandPalette refreshes tab state asynchronously before
    # revealing #commandPalette, so wait for the overlay to lose [hidden].
    page.keyboard.press("ControlOrMeta+K")
    page.wait_for_selector("#commandPalette:not([hidden])", timeout=3000)
    expect(page.locator("#cmdPaletteInput")).to_be_focused()


def test_context_menu_add_keyword_restores_focus_to_right_clicked_card(live_server, page):
    # buildPhotoContextMenu's Add Keyword item used to call batchAddKeyword()
    # without a return-focus argument. closeContextMenu removes the clicked
    # (non-focusable) menu item before firing the callback, so
    # document.activeElement collapses to body and the batch keyword dialog
    # cannot restore focus to the card that launched it. Codex P2: capture
    # the right-clicked card and pass it through so Escape/Cancel return to
    # that card (now tabindex=-1, so .focus() lands).
    open_browse(page, live_server)
    card = page.locator(".grid-card").first
    card.click(button="right")
    page.get_by_text("Add Keyword…", exact=True).click()
    dialog = page.get_by_role("dialog", name="Add keyword to 1 photos", exact=True)
    expect(dialog).to_be_visible()
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    expect(card).to_be_focused()


def test_batch_more_add_to_collection_restores_focus_to_more_button(live_server, page):
    # The batch-bar More button opens the same menu as a card right-click.
    # Codex P2: openBatchMoreMenu passes the button itself as the return
    # target so Add to Collection → Cancel/Escape lands back on More instead
    # of document.body (WebKit does not necessarily focus a button on
    # pointer click, and closeContextMenu has removed the menu item by the
    # time the callback runs).
    open_browse(page, live_server)
    more = page.locator("#batchMoreBtn")
    more.click()
    page.get_by_text("Add to Collection…", exact=True).click()
    dialog = page.get_by_role("dialog", name="Add 1 photo(s) to collection", exact=True)
    expect(dialog).to_be_visible()
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    expect(more).to_be_focused()
