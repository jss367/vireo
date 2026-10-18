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
    selection = page.evaluate("pageModules.selection.getActiveSelection()")
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
    assert page.evaluate("pageModules.selection.getActiveSelection()") == selection
    assert page.evaluate("[pageModules.collectionEditor.collectionEditingId, pageModules.collectionEditor._previewTimer]") == [None, None]
    assert not page.locator(".main-layout").evaluate("el => el.inert")


@pytest.mark.parametrize("dismiss", ["escape", "cancel"])
def test_batch_keyword_dialog_returns_focus_without_clearing_selection(live_server, page, dismiss):
    open_browse(page, live_server)
    selection = page.evaluate("pageModules.selection.getActiveSelection()")
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
    assert page.evaluate("pageModules.selection.getActiveSelection()") == selection


def test_batch_collection_choices_work_with_keyboard_and_restore_opener(live_server, page):
    collection_id = live_server["db"].collections.add("Keyboard picks", "[]")
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
    assert page.evaluate("pageModules.batchActions._batchCollectionSelectedId") == collection_id
    choice.press("Escape")
    expect(dialog).to_be_hidden()
    expect(opener).to_be_focused()


def test_edit_collection_escape_clears_edit_state(live_server, page):
    open_browse(page, live_server)
    opener = page.get_by_role("button", name="+ New Collection", exact=True)
    opener.focus()
    page.evaluate("pageModules.collectionEditor.showCollectionModal({id: 123, name: 'Existing collection', rules: []})")
    dialog = page.get_by_role("dialog", name="Edit Smart Collection", exact=True)
    expect(dialog.get_by_role("textbox", name="Collection name")).to_have_value("Existing collection")
    page.keyboard.press("Escape")
    expect(opener).to_be_focused()
    assert page.evaluate("pageModules.collectionEditor.collectionEditingId") is None


def test_edit_rules_context_menu_restores_focus_to_collection_row(live_server, page):
    # The context menu removes the clicked item before firing its onClick, and
    # the collection row is tabindex=-1 (not in Tab order). Codex P2: without
    # a captured return target the editor falls back to document.body, so Esc
    # cannot restore focus to the row that launched it.
    collection_id = live_server["db"].collections.add("Right-click target", "[]")
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
    collection_id = live_server["db"].collections.add("Pick target", "[]")
    open_browse(page, live_server)
    opener = page.locator("#batchBar").get_by_role("button", name="+ Collection", exact=True)
    opener.click()
    dialog = page.get_by_role("dialog", name="Add 1 photo(s) to collection", exact=True)
    choice = dialog.get_by_role("button", name="Pick target", exact=True)
    choice.click()
    assert page.evaluate("pageModules.batchActions._batchCollectionSelectedId") == collection_id
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
    live_server["db"].collections.add("Pick target", "[]")
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
    page.wait_for_function("window.pageModules && pageModules.selection.getActiveSelection && pageModules.selection.getActiveSelection().length === 0")
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


def test_add_to_collection_dismisses_palette_opened_during_pending_fetch(live_server, page):
    # Codex P2: addToCollection awaits /api/collections before calling
    # batchCollectionDialog.open(), so during that window Ctrl/Cmd+K's
    # hasActive() guard sees no managed dialog and reveals the command
    # palette. Once the fetch resolves and the dialog becomes the top of
    # the stack, updateBackground() marks the palette inert but it stays
    # visible at its higher z-index covering the dialog; Escape would then
    # be consumed by the dialog's Keymap handler, leaving the palette
    # visible until a second Escape. addToCollection must dismiss the
    # palette before opening the dialog so the dialog is the only visible
    # overlay once the fetch resolves.
    live_server["db"].collections.add("Pick target", "[]")
    open_browse(page, live_server)
    # Stall /api/collections at the safeFetch layer until released, mirroring
    # the pending-add helper above. Only the collections GET from
    # addToCollection is intercepted; sidebar refreshes and confirm POST paths
    # go through unmodified.
    page.evaluate("""() => {
      const savedSafeFetch = window.safeFetch;
      window._collectionsFetchSignalled = false;
      window._releaseCollectionsFetch = false;
      window.safeFetch = async function(url, opts, options) {
        if (typeof url === 'string' && url === '/api/collections'
            && (!opts || !opts.method || opts.method === 'GET')) {
          window._collectionsFetchSignalled = true;
          while (!window._releaseCollectionsFetch) {
            await new Promise(r => setTimeout(r, 20));
          }
        }
        return savedSafeFetch(url, opts, options);
      };
    }""")
    opener = page.locator("#batchBar").get_by_role("button", name="+ Collection", exact=True)
    opener.click()
    page.wait_for_function("window._collectionsFetchSignalled === true")
    # The dialog has not opened yet, so hasActive() is false and Ctrl/Cmd+K
    # opens the command palette.
    page.keyboard.press("ControlOrMeta+K")
    expect(page.locator("#commandPalette")).not_to_have_attribute("hidden", "")
    # Release the stalled fetch. addToCollection must close the palette
    # before opening the dialog so no opener-guarded sibling is left
    # visible-but-inert when the dialog becomes the top overlay.
    page.evaluate("window._releaseCollectionsFetch = true")
    dialog = page.get_by_role("dialog", name="Add 1 photo(s) to collection", exact=True)
    expect(dialog).to_be_visible()
    expect(page.locator("#commandPalette")).to_be_hidden()
    # One Escape closes the dialog cleanly; focus returns to the opener.
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    expect(page.locator("#commandPalette")).to_be_hidden()
    expect(opener).to_be_focused()


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


def test_add_to_collection_dismisses_help_opened_during_pending_fetch(live_server, page):
    # Codex P2: addToCollection awaits /api/collections before calling
    # batchCollectionDialog.open(), so during that window F1's hasActive()
    # guard sees no managed dialog and openHelpModal reveals #helpModal.
    # Once the fetch resolves and the dialog becomes the top of the stack,
    # updateBackground() marks the help overlay inert but it stays visible
    # at its higher z-index (10000 vs. the dialog's 500) covering the
    # dialog; Escape would then be consumed by the dialog's Keymap handler,
    # leaving help visible until a second Escape. addToCollection must
    # dismiss the help overlay before opening the dialog, mirroring the
    # command-palette dismissal below, so the dialog is the only visible
    # overlay once the fetch resolves.
    live_server["db"].collections.add("Pick target", "[]")
    open_browse(page, live_server)
    page.evaluate("""() => {
      const savedSafeFetch = window.safeFetch;
      window._collectionsFetchSignalled = false;
      window._releaseCollectionsFetch = false;
      window.safeFetch = async function(url, opts, options) {
        if (typeof url === 'string' && url === '/api/collections'
            && (!opts || !opts.method || opts.method === 'GET')) {
          window._collectionsFetchSignalled = true;
          while (!window._releaseCollectionsFetch) {
            await new Promise(r => setTimeout(r, 20));
          }
        }
        return savedSafeFetch(url, opts, options);
      };
    }""")
    opener = page.locator("#batchBar").get_by_role("button", name="+ Collection", exact=True)
    opener.click()
    page.wait_for_function("window._collectionsFetchSignalled === true")
    # The dialog has not opened yet, so hasActive() is false and F1 opens
    # the help overlay.
    page.keyboard.press("F1")
    expect(page.locator("#helpModal")).to_have_class("help-overlay active")
    # Release the stalled fetch. addToCollection must close the help overlay
    # before opening the dialog so no opener-guarded sibling is left
    # visible-but-inert when the dialog becomes the top overlay.
    page.evaluate("window._releaseCollectionsFetch = true")
    dialog = page.get_by_role("dialog", name="Add 1 photo(s) to collection", exact=True)
    expect(dialog).to_be_visible()
    expect(page.locator("#helpModal")).not_to_have_class("help-overlay active")
    # One Escape closes the dialog cleanly; focus returns to the opener.
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    expect(page.locator("#helpModal")).not_to_have_class("help-overlay active")
    expect(opener).to_be_focused()


def test_add_to_collection_dismisses_report_opened_during_pending_fetch(live_server, page):
    # Codex P2: addToCollection awaits /api/collections before calling
    # batchCollectionDialog.open(), so during that window the navbar's
    # Report Issue icon (⚠) still fires openReportModal — openReportModal
    # has no managed-dialog guard. Once the fetch resolves and the dialog
    # becomes the top of the stack, updateBackground() marks the report
    # overlay inert but it stays visible at z-index 10000 (vs. the
    # dialog's 500) covering the dialog; Escape would then be consumed by
    # the dialog's Keymap handler, leaving the report form visible until
    # a second Escape. addToCollection must dismiss the report overlay
    # before opening the dialog, mirroring the help/palette/lightbox
    # dismissals, so the dialog is the only visible overlay once the
    # fetch resolves.
    live_server["db"].collections.add("Pick target", "[]")
    open_browse(page, live_server)
    page.evaluate("""() => {
      const savedSafeFetch = window.safeFetch;
      window._collectionsFetchSignalled = false;
      window._releaseCollectionsFetch = false;
      window.safeFetch = async function(url, opts, options) {
        if (typeof url === 'string' && url === '/api/collections'
            && (!opts || !opts.method || opts.method === 'GET')) {
          window._collectionsFetchSignalled = true;
          while (!window._releaseCollectionsFetch) {
            await new Promise(r => setTimeout(r, 20));
          }
        }
        return savedSafeFetch(url, opts, options);
      };
    }""")
    opener = page.locator("#batchBar").get_by_role("button", name="+ Collection", exact=True)
    opener.click()
    page.wait_for_function("window._collectionsFetchSignalled === true")
    # The dialog has not opened yet, so hasActive() is false and the
    # navbar's Report Issue click reveals the report overlay. The icon's
    # focus handoff doesn't matter here, so trigger openReportModal
    # directly to keep the assertion independent of pointer timing.
    page.evaluate("openReportModal()")
    expect(page.locator("#reportModal")).to_have_class("report-overlay active")
    # Release the stalled fetch. addToCollection must close the report
    # overlay before opening the dialog so no opener-guarded sibling is
    # left visible-but-inert when the dialog becomes the top overlay.
    page.evaluate("window._releaseCollectionsFetch = true")
    dialog = page.get_by_role("dialog", name="Add 1 photo(s) to collection", exact=True)
    expect(dialog).to_be_visible()
    expect(page.locator("#reportModal")).not_to_have_class("report-overlay active")
    # One Escape closes the dialog cleanly; focus returns to the opener.
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    expect(page.locator("#reportModal")).not_to_have_class("report-overlay active")
    expect(opener).to_be_focused()


def test_save_collection_refocuses_replacement_row_after_refresh(live_server, page):
    # Codex P2: hideCollectionModal() restores focus to the pre-refresh row
    # (the context-menu opener), but saveCollection() then calls
    # loadCollections() → renderCollectionList(), which replaces every row
    # via innerHTML; the focused opener is detached and activeElement
    # collapses to body. After the refresh completes, focus must land on
    # the replacement row that renders for the same collection id so
    # keyboard users don't lose their place after every successful edit.
    collection_id = live_server["db"].collections.add("Edit focus target", "[]")
    open_browse(page, live_server)
    row = page.locator(
        f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
    )
    row.click(button="right")
    page.get_by_text("Edit Rules", exact=True).click()
    dialog = page.get_by_role("dialog", name="Edit Smart Collection", exact=True)
    name = dialog.get_by_role("textbox", name="Collection name", exact=True)
    expect(name).to_have_value("Edit focus target")
    name.fill("Edit focus target renamed")
    dialog.get_by_role("button", name="Update", exact=True).click()
    expect(dialog).to_be_hidden()
    # The server-side rename replaces every row, so the pre-refresh node is
    # detached. Wait for the new row to render and assert focus lands there.
    replacement = page.locator(
        f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
    )
    expect(replacement.get_by_text("Edit focus target renamed", exact=True)).to_be_visible()
    expect(replacement).to_be_focused()


def test_save_collection_preserves_focus_moved_during_pending_refresh(live_server, page):
    # Companion to the row-refocus test: when the user has moved focus to
    # another stable, visible control while saveCollection()'s refresh is
    # pending, the row refocus must not steal that choice back. It only
    # runs when activeElement has been orphaned to body or a detached node.
    collection_id = live_server["db"].collections.add("Keep user focus", "[]")
    open_browse(page, live_server)
    page.evaluate("""() => {
      const savedSafeFetch = window.safeFetch;
      window._listFetchSignalled = false;
      window._releaseListFetch = false;
      window.safeFetch = async function(url, opts, options) {
        if (typeof url === 'string' && url === '/api/collections'
            && (!opts || !opts.method || opts.method === 'GET')) {
          window._listFetchSignalled = true;
          while (!window._releaseListFetch) {
            await new Promise(r => setTimeout(r, 20));
          }
        }
        return savedSafeFetch(url, opts, options);
      };
    }""")
    row = page.locator(
        f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
    )
    row.click(button="right")
    page.get_by_text("Edit Rules", exact=True).click()
    dialog = page.get_by_role("dialog", name="Edit Smart Collection", exact=True)
    name = dialog.get_by_role("textbox", name="Collection name", exact=True)
    expect(name).to_have_value("Keep user focus")
    name.fill("Keep user focus renamed")
    dialog.get_by_role("button", name="Update", exact=True).click()
    expect(dialog).to_be_hidden()
    page.wait_for_function("window._listFetchSignalled === true")
    # The user moves focus to another visible control while the refresh is
    # pending. A standalone anchor is injected so the assertion does not
    # depend on the live browse chrome staying stable.
    page.evaluate("""() => {
      const anchor = document.createElement('button');
      anchor.id = 'userChosenAnchor';
      anchor.type = 'button';
      anchor.textContent = 'User moved here';
      document.body.appendChild(anchor);
      anchor.focus({preventScroll: true});
    }""")
    expect(page.locator("#userChosenAnchor")).to_be_focused()
    # Release the stalled refresh. The row-refocus guard must leave the
    # user's choice alone because activeElement is still a connected,
    # visible control.
    page.evaluate("window._releaseListFetch = true")
    replacement = page.locator(
        f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
    )
    expect(replacement.get_by_text("Keep user focus renamed", exact=True)).to_be_visible()
    expect(page.locator("#userChosenAnchor")).to_be_focused()


def test_add_to_collection_dismisses_lightbox_opened_during_pending_fetch(live_server, page):
    # Codex P2: addToCollection awaits /api/collections before calling
    # batchCollectionDialog.open(), so during that window a grid dblclick
    # can still reveal the shared lightbox — openLightbox has no managed-
    # dialog guard. Once the fetch resolves and the dialog becomes the top
    # of the stack, updateBackground() marks the lightbox overlay inert
    # but it stays visible at z-index 9999 (vs. the dialog's 500) covering
    # the dialog; Escape would then be consumed by the dialog's Keymap
    # handler, leaving the lightbox visible until a second Escape.
    # addToCollection must dismiss the lightbox before opening the dialog
    # so the dialog is the only visible overlay once the fetch resolves.
    live_server["db"].collections.add("Pick target", "[]")
    open_browse(page, live_server)
    # Stall /api/collections at the safeFetch layer until released,
    # mirroring the pending-help and pending-palette helpers above.
    page.evaluate("""() => {
      const savedSafeFetch = window.safeFetch;
      window._collectionsFetchSignalled = false;
      window._releaseCollectionsFetch = false;
      window.safeFetch = async function(url, opts, options) {
        if (typeof url === 'string' && url === '/api/collections'
            && (!opts || !opts.method || opts.method === 'GET')) {
          window._collectionsFetchSignalled = true;
          while (!window._releaseCollectionsFetch) {
            await new Promise(r => setTimeout(r, 20));
          }
        }
        return savedSafeFetch(url, opts, options);
      };
    }""")
    opener = page.locator("#batchBar").get_by_role("button", name="+ Collection", exact=True)
    opener.click()
    page.wait_for_function("window._collectionsFetchSignalled === true")
    # The dialog has not opened yet, so hasActive() is false and a grid
    # dblclick reveals the shared lightbox. Trigger openLightbox directly
    # with a known photo id — a page dblclick would also work, but the
    # scripted call keeps the assertion independent of pointer timing.
    page.evaluate("""() => {
      const card = document.querySelector('.grid-card');
      const id = parseInt(card.dataset.id, 10);
      openLightbox(id, card.dataset.filename || '', pageModules.lightbox.availableBrowsePhotos());
    }""")
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    # Release the stalled fetch. addToCollection must close the lightbox
    # before opening the dialog so no opener-guarded sibling is left
    # visible-but-inert when the dialog becomes the top overlay.
    page.evaluate("window._releaseCollectionsFetch = true")
    dialog = page.get_by_role("dialog", name="Add 1 photo(s) to collection", exact=True)
    expect(dialog).to_be_visible()
    expect(page.locator("#lightboxOverlay")).not_to_have_class("lightbox-overlay active")
    # One Escape closes the dialog cleanly; focus returns to the opener.
    page.keyboard.press("Escape")
    expect(dialog).to_be_hidden()
    expect(page.locator("#lightboxOverlay")).not_to_have_class("lightbox-overlay active")
    expect(opener).to_be_focused()


def test_save_collection_restores_focus_after_superseding_render(live_server, page):
    # Codex P2: if another loadCollections() (e.g. a concurrent folder-
    # health refresh) is still pending when saveCollection()'s own refresh
    # resolves, reconcileCollectionLoadRenders() defers rendering until
    # the newer generation settles — the pre-save row stays connected
    # past the await, orphaned reads false, and the later superseding
    # render replaces the list and drops focus to body with no restore
    # path. The row-refocus must watch for subsequent child-list mutations
    # on #collectionList and run again once the newer render arrives.
    collection_id = live_server["db"].collections.add("Supersede target", "[]")
    open_browse(page, live_server)
    # Install a safeFetch wrapper that lets the first /api/collections GET
    # (the one saveCollection will fire) run normally, but holds a second
    # concurrent GET pending until released — simulating a folder-health
    # refresh that started slightly later and resolves after our save's
    # own refresh. The initial browse-load GET runs before this wrapper
    # is installed, so it is not affected.
    page.evaluate("""() => {
      const savedSafeFetch = window.safeFetch;
      window._supersedingGetReleased = false;
      window._supersedingGetSignalled = false;
      window._savesFetchSeen = 0;
      window.safeFetch = async function(url, opts, options) {
        if (typeof url === 'string' && url === '/api/collections'
            && (!opts || !opts.method || opts.method === 'GET')) {
          window._savesFetchSeen += 1;
          if (window._savesFetchSeen === 2) {
            window._supersedingGetSignalled = true;
            while (!window._supersedingGetReleased) {
              await new Promise(r => setTimeout(r, 20));
            }
          }
        }
        return savedSafeFetch(url, opts, options);
      };
    }""")
    row = page.locator(
        f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
    )
    row.click(button="right")
    page.get_by_text("Edit Rules", exact=True).click()
    dialog = page.get_by_role("dialog", name="Edit Smart Collection", exact=True)
    name = dialog.get_by_role("textbox", name="Collection name", exact=True)
    expect(name).to_have_value("Supersede target")
    name.fill("Supersede target renamed")
    # Kick off a concurrent loadCollections() that will bump the gen
    # counter past saveCollection's own gen. This matches the folder-
    # health refresh race the Codex finding describes.
    page.evaluate("pageModules.sidebarCollections.loadCollections()")
    dialog.get_by_role("button", name="Update", exact=True).click()
    expect(dialog).to_be_hidden()
    # Wait until the second (superseding) GET is signalled stalled; by
    # then saveCollection's own refresh has resolved and its await has
    # fallen through, but reconcileCollectionLoadRenders is still waiting
    # on the pending newer gen, so the pre-save row stays rendered.
    page.wait_for_function("window._supersedingGetSignalled === true")
    # Release the superseding GET. Its render replaces the row list and
    # detaches the pre-save opener; the MutationObserver installed by
    # saveCollection must catch that render and refocus the replacement.
    page.evaluate("window._supersedingGetReleased = true")
    replacement = page.locator(
        f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
    )
    expect(replacement.get_by_text("Supersede target renamed", exact=True)).to_be_visible()
    expect(replacement).to_be_focused()


def test_save_collection_preserves_focus_moved_to_sibling_row_during_pending_refresh(live_server, page):
    # Codex P2: when the user tabs to another collection row inside
    # #collectionList while a superseding loadCollections() is pending,
    # the subsequent innerHTML render detaches that sibling row too and
    # drops activeElement to body. The row-refocus must not misread the
    # body-focus as the opener being orphaned and jerk focus back to the
    # edited row — a focusin tracker captures the user's intervening move.
    first_id = live_server["db"].collections.add("Sibling source", "[]")
    second_id = live_server["db"].collections.add("Sibling target", "[]")
    open_browse(page, live_server)
    page.evaluate("""() => {
      const savedSafeFetch = window.safeFetch;
      window._supersedingGetReleased = false;
      window._supersedingGetSignalled = false;
      window._savesFetchSeen = 0;
      window.safeFetch = async function(url, opts, options) {
        if (typeof url === 'string' && url === '/api/collections'
            && (!opts || !opts.method || opts.method === 'GET')) {
          window._savesFetchSeen += 1;
          if (window._savesFetchSeen === 2) {
            window._supersedingGetSignalled = true;
            while (!window._supersedingGetReleased) {
              await new Promise(r => setTimeout(r, 20));
            }
          }
        }
        return savedSafeFetch(url, opts, options);
      };
    }""")
    row = page.locator(
        f'#collectionList .tree-item[data-collection-id="{first_id}"]'
    )
    row.click(button="right")
    page.get_by_text("Edit Rules", exact=True).click()
    dialog = page.get_by_role("dialog", name="Edit Smart Collection", exact=True)
    name = dialog.get_by_role("textbox", name="Collection name", exact=True)
    expect(name).to_have_value("Sibling source")
    name.fill("Sibling source renamed")
    page.evaluate("pageModules.sidebarCollections.loadCollections()")
    dialog.get_by_role("button", name="Update", exact=True).click()
    expect(dialog).to_be_hidden()
    # Before releasing the superseding render, the user tabs to the
    # sibling row inside #collectionList.
    page.evaluate(f"""() => {{
      const sibling = document.querySelector(
        '#collectionList .tree-item[data-collection-id="{second_id}"]');
      sibling.focus({{preventScroll: true}});
    }}""")
    sibling = page.locator(
        f'#collectionList .tree-item[data-collection-id="{second_id}"]'
    )
    expect(sibling).to_be_focused()
    page.wait_for_function("window._supersedingGetSignalled === true")
    page.evaluate("window._supersedingGetReleased = true")
    replacement_first = page.locator(
        f'#collectionList .tree-item[data-collection-id="{first_id}"]'
    )
    expect(replacement_first.get_by_text("Sibling source renamed", exact=True)).to_be_visible()
    # The edited row should NOT grab focus back from the user's sibling
    # choice. activeElement went to body when the sibling row was
    # detached, but the focusin tracker remembers the user's intentional
    # move, so the restore guard leaves it alone.
    expect(replacement_first).not_to_be_focused()


def test_save_collection_restores_focus_after_slow_superseding_render(live_server, page):
    # Codex P2: a superseding /api/collections GET that takes longer than
    # the previous fixed 5s observer timeout must still trigger focus
    # restoration. The observer's lifetime is anchored to
    # collectionLoadGen > collectionRenderDecisionGen (not wall-clock),
    # so it survives slow fetches up to a 30s hard safety cap.
    collection_id = live_server["db"].collections.add("Slow supersede", "[]")
    open_browse(page, live_server)
    page.evaluate("""() => {
      const savedSafeFetch = window.safeFetch;
      window._supersedingGetReleased = false;
      window._supersedingGetSignalled = false;
      window._savesFetchSeen = 0;
      window.safeFetch = async function(url, opts, options) {
        if (typeof url === 'string' && url === '/api/collections'
            && (!opts || !opts.method || opts.method === 'GET')) {
          window._savesFetchSeen += 1;
          if (window._savesFetchSeen === 2) {
            window._supersedingGetSignalled = true;
            while (!window._supersedingGetReleased) {
              await new Promise(r => setTimeout(r, 20));
            }
          }
        }
        return savedSafeFetch(url, opts, options);
      };
    }""")
    row = page.locator(
        f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
    )
    row.click(button="right")
    page.get_by_text("Edit Rules", exact=True).click()
    dialog = page.get_by_role("dialog", name="Edit Smart Collection", exact=True)
    name = dialog.get_by_role("textbox", name="Collection name", exact=True)
    expect(name).to_have_value("Slow supersede")
    name.fill("Slow supersede renamed")
    page.evaluate("pageModules.sidebarCollections.loadCollections()")
    dialog.get_by_role("button", name="Update", exact=True).click()
    expect(dialog).to_be_hidden()
    page.wait_for_function("window._supersedingGetSignalled === true")
    # Hold the superseding GET pending past the old 5s cap, then release.
    page.wait_for_timeout(6000)
    page.evaluate("window._supersedingGetReleased = true")
    replacement = page.locator(
        f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
    )
    expect(replacement.get_by_text("Slow supersede renamed", exact=True)).to_be_visible()
    expect(replacement).to_be_focused()
