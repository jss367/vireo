"""The questions and notices the app shows in the page.

``Vireo.confirm``, ``Vireo.prompt`` and ``Vireo.alert`` (vireo-utils.js)
stand in for the native dialogs. The desktop app's webview swaps
``window.confirm`` for an async function, so its result is a promise: always
truthy, and no dialog is shown; its ``alert`` shows nothing and its ``prompt``
returns null. The tests install those replacements and check that the
question is still asked, and that nothing is deleted before it is answered.
"""
import json

from playwright.sync_api import expect

from e2e.browser_doubles import fulfill_sse, record_toasts, toasts
from e2e.in_page_dialogs import ACCEPT, CANCEL, DESKTOP_NATIVE_DIALOGS, DIALOG, INPUT


def _ask(page):
    """Open a question; its answer lands in ``window.__answer``."""
    page.evaluate("""() => {
      window.__answer = 'pending';
      Vireo.confirm({
        title: 'Delete it?', message: 'First paragraph.\\n\\nSecond paragraph.',
        details: ['/photos/a.jpg'], confirmLabel: 'Delete permanently', danger: true,
      }).then(answer => { window.__answer = answer; });
    }""")
    expect(page.locator(DIALOG)).to_be_visible()


def _prompt(page, options):
    """Open a prompt; its answer lands in ``window.__answer``."""
    page.evaluate("""options => {
      window.__answer = 'pending';
      Vireo.prompt(options).then(answer => { window.__answer = answer; });
    }""", options)
    expect(page.locator(DIALOG)).to_be_visible()


def _answer(page):
    return page.evaluate("window.__answer")


def _watch_page_keys(page):
    """Record every key the page behind a dialog would act on, and every
    Esc the page's own Esc stack receives."""
    page.evaluate("""() => {
      window.__keys = [];
      window.__escaped = 0;
      document.addEventListener('keydown', event => { window.__keys.push(event.key); }, true);
      Keymap.pushEsc(() => { window.__escaped++; });
    }""")


def test_only_the_confirm_button_answers_yes(live_server, page):
    page.goto(f"{live_server['url']}/browse")

    _ask(page)
    expect(page.locator(f"{DIALOG} h3")).to_have_text("Delete it?")
    expect(page.locator(f"{DIALOG} .vireo-confirm-text")).to_have_count(2)
    expect(page.locator(f"{DIALOG} .vireo-confirm-details li")).to_have_text(["/photos/a.jpg"])
    assert _answer(page) == "pending"
    # Cancel holds the focus, so Enter declines.
    expect(page.locator(CANCEL)).to_be_focused()
    page.keyboard.press("Enter")
    expect(page.locator(DIALOG)).to_have_count(0)
    assert _answer(page) is False

    _ask(page)
    page.keyboard.press("Tab")
    expect(page.locator(ACCEPT)).to_be_focused()
    page.keyboard.press("Escape")
    expect(page.locator(DIALOG)).to_have_count(0)
    assert _answer(page) is False

    _ask(page)
    page.locator(ACCEPT).click()
    expect(page.locator(DIALOG)).to_have_count(0)
    assert _answer(page) is True


def test_a_second_question_is_declined_while_one_is_open(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _ask(page)

    assert page.evaluate("Vireo.confirm({title: 'Another?'})") is False
    assert page.evaluate("Vireo.prompt({title: 'A name?'})") is None

    expect(page.locator(DIALOG)).to_have_count(1)
    assert _answer(page) == "pending"


def test_keys_do_not_reach_the_page_behind_the_question(live_server, page):
    """A shortcut pressed while the question is open must not act on the
    photo behind it, and Esc must close only the question."""
    page.goto(f"{live_server['url']}/browse")
    _watch_page_keys(page)

    _ask(page)
    for key in ("x", "ArrowRight", "Delete", "Escape"):
        page.keyboard.press(key)

    expect(page.locator(DIALOG)).to_have_count(0)
    assert _answer(page) is False
    assert page.evaluate("window.__keys") == []
    assert page.evaluate("window.__escaped") == 0

    # With the question gone the page owns Esc again.
    page.keyboard.press("Escape")
    assert page.evaluate("window.__escaped") == 1


def test_prompt_takes_typing_and_enter_accepts(live_server, page):
    """Typing goes to the prompt's field and nowhere else: "x" and "p" are
    Browse's reject and pick shortcuts."""
    page.goto(f"{live_server['url']}/browse")
    _watch_page_keys(page)

    _prompt(page, {"title": "Rename collection", "label": "Collection name", "value": "Old name"})
    field = page.locator(INPUT)
    expect(field).to_be_focused()
    expect(field).to_have_value("Old name")
    expect(page.locator(f"{DIALOG} label")).to_have_text("Collection name")
    # The starting text is selected, so typing replaces it.
    page.keyboard.type("xp picks")
    expect(field).to_have_value("xp picks")
    assert _answer(page) == "pending"
    page.keyboard.press("Enter")

    expect(page.locator(DIALOG)).to_have_count(0)
    assert _answer(page) == "xp picks"
    assert page.evaluate("window.__keys") == []


def test_prompt_cancel_and_escape_answer_null(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _watch_page_keys(page)

    _prompt(page, {"title": "Name this snapshot"})
    page.keyboard.type("half typed")
    page.keyboard.press("Escape")
    expect(page.locator(DIALOG)).to_have_count(0)
    assert _answer(page) is None
    assert page.evaluate("window.__escaped") == 0

    _prompt(page, {"title": "Name this snapshot"})
    # Tab walks the field, Cancel, then the confirm button.
    page.keyboard.press("Tab")
    expect(page.locator(CANCEL)).to_be_focused()
    page.keyboard.press("Tab")
    expect(page.locator(ACCEPT)).to_be_focused()
    page.keyboard.press("Shift+Tab")
    page.keyboard.press("Enter")
    expect(page.locator(DIALOG)).to_have_count(0)
    assert _answer(page) is None

    # An empty field accepted is an empty answer, not a cancel.
    _prompt(page, {"title": "Name this snapshot"})
    page.locator(ACCEPT).click()
    assert _answer(page) == ""


def test_prompt_with_choices_answers_one_of_them(live_server, page):
    page.goto(f"{live_server['url']}/browse")

    _prompt(page, {
        "title": "Add as a highlight for which species?",
        "choices": ["Mallard", "Wood Duck"], "value": "Mallard",
    })
    field = page.locator(INPUT)
    expect(field).to_have_value("Mallard")
    assert field.evaluate("node => node.tagName") == "SELECT"
    field.select_option("Wood Duck")
    page.locator(ACCEPT).click()

    expect(page.locator(DIALOG)).to_have_count(0)
    assert _answer(page) == "Wood Duck"


def test_alert_is_shown_until_closed_and_waits_for_an_open_dialog(live_server, page):
    """A notice asked while a question is open is shown once the question
    closes, not dropped."""
    page.goto(f"{live_server['url']}/browse")
    _watch_page_keys(page)

    _ask(page)
    page.evaluate("""() => {
      window.__closed = 0;
      Vireo.alert({title: 'Settings were not imported', message: 'Unknown key.', details: ['theme: bad value']})
        .then(() => { window.__closed++; });
    }""")
    expect(page.locator(f"{DIALOG} h3")).to_have_text("Delete it?")
    page.locator(CANCEL).click()

    expect(page.locator(f"{DIALOG} h3")).to_have_text("Settings were not imported")
    expect(page.locator(f"{DIALOG} .vireo-confirm-details li")).to_have_text(["theme: bad value"])
    # A notice has one button, and no way to answer no.
    expect(page.locator(CANCEL)).to_have_count(0)
    expect(page.locator(ACCEPT)).to_be_focused()
    assert page.evaluate("window.__closed") == 0
    # A question asked while the notice is open is declined.
    assert page.evaluate("Vireo.confirm({title: 'Another?'})") is False
    page.keyboard.press("x")
    page.keyboard.press("Enter")

    expect(page.locator(DIALOG)).to_have_count(0)
    assert page.evaluate("window.__closed") == 1
    assert page.evaluate("window.__keys") == []


class TrashFailure:
    """A delete job whose Trash step failed for one photo, and the
    permanent-delete requests the page sends afterwards."""

    def __init__(self, page, photo_id):
        self.permanent_requests = []
        result = {
            "deleted": 0, "trashed": 0, "failed_photo_ids": [photo_id],
            "trash_failed": [{
                "path": "/photos/park/hawk1.jpg", "error": "Trash unavailable",
                "photo_id": photo_id,
            }],
        }
        page.route("**/api/jobs/batch-delete", lambda route: route.fulfill(
            status=200, content_type="application/json",
            body=json.dumps({"job_id": "delete-ui-test"}),
        ))
        page.route("**/api/jobs/delete-ui-test/stream", lambda route: fulfill_sse(
            route, "complete",
            {"status": "failed", "errors": ["Trash unavailable"], "result": result},
        ))
        page.route("**/api/batch/delete", self._permanent)

    def _permanent(self, route):
        self.permanent_requests.append(json.loads(route.request.post_data))
        route.fulfill(status=200, content_type="application/json", body=json.dumps({
            "ok": True, "deleted": 1, "trashed": 1, "trash_failed": [],
            "failed_photo_ids": [],
        }))


def _delete_to_trash(page, photo_id):
    """Run the delete dialog in Trash mode for one photo."""
    page.evaluate("id => showDeleteDialog([id], 0, null)", photo_id)
    page.locator('#deleteModal input[name="deleteMode"][value="disk"]').check()
    page.locator("#deleteConfirmBtn").click()


def test_trash_failure_asks_before_deleting_permanently(live_server, page):
    """In the desktop app a native confirm() is always "yes", so a failed
    Trash would go straight on to a permanent delete."""
    page.add_init_script(DESKTOP_NATIVE_DIALOGS)
    page.goto(f"{live_server['url']}/browse")
    card = page.locator(".grid-card").first
    card.wait_for(state="visible")
    photo_id = int(card.get_attribute("data-id"))
    server = TrashFailure(page, photo_id)
    record_toasts(page)

    _delete_to_trash(page, photo_id)

    expect(page.locator(DIALOG)).to_be_visible()
    expect(page.locator(f"{DIALOG} h3")).to_have_text("Delete permanently instead?")
    expect(page.locator(f"{DIALOG} .vireo-confirm-details li")).to_have_text(
        ["/photos/park/hawk1.jpg — Trash unavailable"]
    )
    # Nothing is deleted while the question is open...
    assert server.permanent_requests == []
    page.locator(CANCEL).click()
    expect(page.locator(DIALOG)).to_have_count(0)
    # ...or after it is declined.
    page.wait_for_function("window.__toasts.length > 0")
    assert server.permanent_requests == []
    assert toasts(page)[-1] == {
        "message": "0 photos moved to Trash; 1 retained after file errors", "type": "error",
    }

    _delete_to_trash(page, photo_id)
    page.locator(ACCEPT).click()
    page.wait_for_function("window.__toasts.length > 1")
    assert server.permanent_requests == [{
        "photo_ids": [photo_id], "mode": "disk_permanent",
        "confirm_permanent": True, "include_companions": False,
    }]
    assert toasts(page)[-1] == {
        "message": "0 photos moved to Trash; 1 deleted permanently", "type": "success",
    }
