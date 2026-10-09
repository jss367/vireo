"""Page search through both keyboard and desktop menu entry points."""

import re
from pathlib import Path

import pytest
from playwright.sync_api import expect

from e2e.test_life_list_explorer import _seed_hummingbird_tree


def test_native_find_shortcut_is_reserved_for_text_search():
    menu = (Path(__file__).parents[2] / "src-tauri/src/menu.rs").read_text()
    accelerator_items = re.findall(
        r'MenuItemBuilder::with_id\(ids::(\w+),[^)]*\)\s*'
        r'\.accelerator\("CmdOrCtrl\+F"\)',
        menu,
    )
    assert accelerator_items == ["EDIT_FIND"]
    assert 'ids::EDIT_FIND => Some("find")' in menu
    assert 'ids::PHOTO_FIND_SIMILAR => Some("photo_find_similar")' in menu


@pytest.mark.parametrize("entry", ["Control+F", "Meta+F", "native"])
def test_find_searches_life_list_explorer(live_server, page, entry):
    _seed_hummingbird_tree(live_server["db"])
    page.goto(f"{live_server['url']}/life-list?view=explorer")
    card = page.locator(".ll-card", has_text="Swifts and Hummingbirds")
    expect(card).to_be_visible()
    if entry == "native":
        page.evaluate("handleNativeMenuCommand('find')")
    else:
        page.keyboard.press(entry)

    field = page.get_by_role("searchbox", name="Find in page", exact=True)
    expect(field).to_be_focused()
    field.fill("hummingbirds")
    expect(card.locator("mark.page-find-mark.active")).to_have_text("Hummingbirds")
    expect(page.locator("#pageFindStatus")).to_have_text("1 of 1")
    expect(page.locator("#toastContainer")).not_to_contain_text("Find Similar")

    # Search markers preserve the taxon card's click handler. A drill-down
    # replaces the chart and cards; the open search must update with them.
    card.locator("mark").click()
    expect(page.locator("#explorerSunburstCenter")).to_have_attribute(
        "data-name", "Swifts and Hummingbirds"
    )
    expect(page.locator("#pageFindStatus")).to_have_text("1 of 2")

    # The center label truncates the taxon name; search its visible text.
    field.fill("Swifts")
    expect(page.locator("tspan.page-find-mark")).to_have_count(1)
    expect(page.locator("#pageFindStatus")).to_have_text("1 of 2")

    field.focus()
    page.keyboard.press("Enter")
    expect(page.locator("#pageFindStatus")).to_have_text("2 of 2")
    page.keyboard.press("Shift+Enter")
    expect(page.locator("#pageFindStatus")).to_have_text("1 of 2")
    page.keyboard.press("Shift+Enter")
    expect(page.locator("#pageFindStatus")).to_have_text("2 of 2")
    page.keyboard.press("Escape")
    expect(page.locator("#pageFindPanel")).to_be_hidden()
    expect(page.locator(".page-find-mark")).to_have_count(0)
    expect(page.locator("#explorerSunburstCenter")).to_have_attribute(
        "data-name", "Swifts and Hummingbirds"
    )


def test_find_ignores_hidden_text_and_clears_old_matches(live_server, page):
    page.goto(f"{live_server['url']}/life-list")
    page.evaluate("""() => {
        const section = document.createElement('div');
        section.innerHTML = '<p>Needle needle</p><p hidden>needle</p>' +
            '<p style="display:none">needle</p><p aria-hidden="true">needle</p>';
        document.body.appendChild(section);
    }""")
    page.keyboard.press("Control+F")
    field = page.locator("#pageFindInput")
    field.fill("needle")
    expect(page.locator("#pageFindStatus")).to_have_text("1 of 2")
    field.fill("absent-search-term")
    expect(page.locator("#pageFindStatus")).to_have_text("0 results")
    expect(page.locator(".page-find-mark")).to_have_count(0)
    expect(page.locator("#pageFindNext")).to_be_disabled()
    page.locator("#pageFindClose").click()
    expect(page.locator("#pageFindPanel")).to_be_hidden()


@pytest.mark.parametrize("path", ["/browse", "/settings"])
def test_native_find_uses_existing_page_search(live_server, page, path):
    page.goto(f"{live_server['url']}{path}")
    selector = ".vf-search input" if path == "/browse" else "#settingsFindInput"
    if path == "/browse":
        expect(page.locator(selector)).to_be_visible()
    page.evaluate("handleNativeMenuCommand('find')")
    expect(page.locator(selector)).to_be_focused()
    expect(page.locator("#pageFindPanel")).to_be_hidden()


@pytest.mark.parametrize("entry", ["Control+F", "Meta+F", "native"])
def test_find_does_not_interrupt_shortcut_capture(live_server, page, entry):
    page.goto(f"{live_server['url']}/shortcuts")
    button = page.locator(".shortcut-key-btn[onclick*=\"'navigation', 'browse'\"]")
    expect(button).to_be_visible()
    button.click()
    expect(button).to_have_class(re.compile("capturing"))
    # A native accelerator may consume the keystroke without delivering a
    # webview keydown, so test the native command without a second keypress.
    if entry == "native":
        page.evaluate("handleNativeMenuCommand('find')")
    else:
        page.keyboard.press(entry)
    expect(page.locator("#pageFindPanel")).to_be_hidden()
    expect(page.locator(".shortcut-key-btn.capturing")).to_have_count(0)
    assert page.evaluate("currentShortcuts.navigation.browse") == "ctrl+f"


def test_find_updates_when_details_open_or_close(live_server, page):
    page.goto(f"{live_server['url']}/life-list")
    page.evaluate("""() => {
        const details = document.createElement('details');
        details.id = 'findTestDetails';
        details.innerHTML = '<summary>Photo details</summary><p>CollapsedNeedle</p>';
        document.body.appendChild(details);
    }""")
    page.keyboard.press("Control+F")
    page.locator("#pageFindInput").fill("Photo details")
    expect(page.locator("#pageFindStatus")).to_have_text("1 of 1")
    page.locator("#pageFindInput").fill("CollapsedNeedle")
    expect(page.locator("#pageFindStatus")).to_have_text("0 results")
    page.locator("#findTestDetails summary").click()
    expect(page.locator("#pageFindStatus")).to_have_text("1 of 1")
    page.locator("#findTestDetails summary").click()
    expect(page.locator("#pageFindStatus")).to_have_text("0 results")


@pytest.mark.parametrize("entry", ["Control+F", "native"])
def test_find_works_on_standalone_setup_page(live_server, page, entry):
    page.route("**/api/models/status", lambda route: route.fulfill(
        json={"available_models": [], "classification": {"labels_ready": False}}
    ))
    page.goto(f"{live_server['url']}/welcome?force=1")
    if entry == "native":
        page.evaluate("handleNativeMenuCommand('find')")
    else:
        page.keyboard.press(entry)
    field = page.locator("#pageFindInput")
    expect(field).to_be_focused()
    field.fill("wildlife")
    expect(page.locator("#pageFindStatus")).not_to_have_text("0 results")
    expect(page.locator(".page-find-mark").first).to_be_visible()
    page.keyboard.press("Escape")
    expect(page.locator("#pageFindPanel")).to_be_hidden()


def test_find_preserves_unicode_offsets_and_literal_queries(live_server, page):
    page.goto(f"{live_server['url']}/life-list")
    page.evaluate("""() => {
        const paragraph = document.createElement('p');
        paragraph.id = 'findUnicodeText';
        paragraph.textContent = 'İ Needle needle 🐦 [bird].* [bird].*';
        document.body.appendChild(paragraph);
    }""")
    page.keyboard.press("Control+F")
    field = page.locator("#pageFindInput")
    field.fill("needle")
    assert page.locator("#findUnicodeText .page-find-mark").all_text_contents() == [
        "Needle", "needle"
    ]
    field.fill("[bird].*")
    expect(page.locator("#pageFindStatus")).to_have_text("1 of 2")
    assert page.locator("#findUnicodeText .page-find-mark").all_text_contents() == [
        "[bird].*", "[bird].*"
    ]
    page.keyboard.press("Escape")
    expect(page.locator("#findUnicodeText")).to_have_text(
        "İ Needle needle 🐦 [bird].* [bird].*"
    )


def test_find_matches_across_inline_markup(live_server, page):
    page.goto(f"{live_server['url']}/life-list")
    page.evaluate("""() => {
        const section = document.createElement('div');
        section.id = 'findInlineText';
        section.innerHTML = '<p>Click <b>Scan for duplicate files</b> to find photos.</p>' +
            '<p>wild<span>life</span></p><p>separate</p><p>blocks</p>' +
            '<p>Line<br>break</p><div>empty<div></div>boundary</div>';
        document.body.appendChild(section);
    }""")
    page.keyboard.press("Control+F")
    field = page.locator("#pageFindInput")
    for query in ("Click Scan", "files to find", "wildlife", "Line break"):
        field.fill(query)
        expect(page.locator("#pageFindStatus")).to_have_text("1 of 1")
        expect(page.locator("#findInlineText .page-find-mark.active")).to_have_count(2)
    field.fill("separate blocks")
    expect(page.locator("#pageFindStatus")).to_have_text("0 results")
    field.fill("emptyboundary")
    expect(page.locator("#pageFindStatus")).to_have_text("0 results")
    field.fill("Scan for duplicate")
    expect(page.locator("#findInlineText b .page-find-mark.active")).to_have_text(
        "Scan for duplicate"
    )
    field.fill("Click Scan")
    page.evaluate("""() => {
        const p = document.createElement('p');
        p.innerHTML = 'Click <b>Scan</b>';
        document.getElementById('findInlineText').appendChild(p);
    }""")
    expect(page.locator("#pageFindStatus")).to_have_text("1 of 2")
    page.keyboard.press("Enter")
    expect(page.locator("#pageFindStatus")).to_have_text("2 of 2")
    expect(page.locator("#findInlineText > p:last-child .page-find-mark.active")).to_have_count(2)
    page.keyboard.press("Escape")
    expect(page.locator("#findInlineText b").first).to_have_text("Scan for duplicate files")
    expect(page.locator("#findInlineText .page-find-mark")).to_have_count(0)
