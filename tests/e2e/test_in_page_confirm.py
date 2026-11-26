"""The in-page question that stands in front of a permanent delete.

``Vireo.confirm`` (vireo-utils.js) replaces ``window.confirm()`` where an
answer gates the removal of files. The desktop app's webview swaps the native
function for an async one, so its result is a promise: always truthy, and no
dialog is shown. The tests install that same replacement and check that the
question is still asked, and that nothing is deleted before it is answered.
"""
import json

from playwright.sync_api import expect

from e2e.browser_doubles import fulfill_sse, record_toasts, toasts

# What tauri-plugin-dialog's init script does to window.confirm.
DESKTOP_CONFIRM = "window.confirm = async function() { return true; };"

DIALOG = "#vireoConfirmDialog"
CANCEL = f"{DIALOG} [data-vireo-confirm=cancel]"
ACCEPT = f"{DIALOG} [data-vireo-confirm=accept]"


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


def _answer(page):
    return page.evaluate("window.__answer")


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

    second = page.evaluate("Vireo.confirm({title: 'Another?'})")

    assert second is False
    expect(page.locator(DIALOG)).to_have_count(1)
    assert _answer(page) == "pending"


def test_keys_do_not_reach_the_page_behind_the_question(live_server, page):
    """A shortcut pressed while the question is open must not act on the
    photo behind it, and Esc must close only the question."""
    page.goto(f"{live_server['url']}/browse")
    page.evaluate("""() => {
      window.__keys = [];
      window.__escaped = 0;
      document.addEventListener('keydown', event => { window.__keys.push(event.key); }, true);
      Keymap.pushEsc(() => { window.__escaped++; });
    }""")

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
    """In the desktop app the old native confirm() was always "yes", so a
    failed Trash went straight on to a permanent delete."""
    page.add_init_script(DESKTOP_CONFIRM)
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
        ["/photos/park/hawk1.jpg \u2014 Trash unavailable"]
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
