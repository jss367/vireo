"""Review photo actions report real outcomes and remain usable after failure."""

import time

import pytest
from playwright.sync_api import expect
from testing.waits import synchronization_timeout

from e2e.test_burst_group_context_menu import _dispatch_contextmenu, _open_burst_modal, _seed_burst_group


def wait_for_held(page, held, count):
    # request events may precede dispatch of Python route callbacks.
    deadline = time.monotonic() + synchronization_timeout(10)
    while len(held) < count and time.monotonic() < deadline:
        page.wait_for_timeout(20)
    assert len(held) == count


def mount_review(live_server, page, surface="grid"):
    if surface == "burst":
        assert _seed_burst_group(live_server["db"]) > 0
    page.goto(live_server["url"] + "/review")
    card = page.locator(".card[data-pred-id]").first
    expect(card).to_be_visible()
    if surface == "burst":
        _open_burst_modal(page)
        target = page.locator("#grmOverlay .grm-card[data-photo-id]").first
        photo_id = int(target.get_attribute("data-photo-id"))
    else:
        target = card
        photo_id = int(card.locator("img[data-photo-id]").first.get_attribute("data-photo-id"))
    if surface == "lightbox":
        card.click(button="right")
        page.locator(".vireo-ctx-item", has_text="Open in Lightbox").click()
        expect(page.locator("#lightboxOverlay")).to_be_visible()
        page.wait_for_function("!_lbVisualTransitionPending && vireoLightboxSession.displayedPhotoId() !== null")
        target = page.locator("#lightboxImg")
    page.evaluate("document.getElementById('toastContainer').replaceChildren()")
    return target, photo_id


def action_chip(page, target, field):
    _dispatch_contextmenu(target)
    title = "Rate 3" if field == "rating" else "Reject"
    return page.locator(f'.vireo-ctx-chip[title="{title}"]')


@pytest.mark.parametrize("surface", ["grid", "burst", "lightbox"])
@pytest.mark.parametrize("field", ["rating", "flag"])
@pytest.mark.parametrize("failure", ["network", "http"])
def test_save_failure_is_visible_and_the_same_action_can_retry(live_server, page, surface, field, failure):
    target, photo_id = mount_review(live_server, page, surface)
    requests = []

    def intercept(route):
        requests.append(route.request.post_data_json)
        if len(requests) == 1:
            if failure == "network":
                route.abort("failed")
            else:
                route.fulfill(status=503, json={"error": "<img src=x onerror=alert(1)> temporarily unavailable"})
        else:
            route.continue_()

    page.route(f"**/api/batch/{field}", intercept)
    original = live_server["db"].photos.get(photo_id)[field]
    action_chip(page, target, field).click()
    error = page.locator('#toastContainer > [data-type="error"]')
    expect(error).to_have_count(1)
    expect(error).to_contain_text(f"Could not save {field}")
    expect(error.locator("img")).to_have_count(0)
    expect(page.locator('#toastContainer > [data-type="success"]')).to_have_count(0)
    assert live_server["db"].photos.get(photo_id)[field] == original
    if surface == "lightbox" and field == "flag":
        page.wait_for_function("_lbFlagPendingWrites === 0")
        expect(page.locator("#lightboxFlagStatus")).to_contain_text("No flag")
    action_chip(page, target, field).click()
    expect(page.locator('#toastContainer > [data-type="success"]')).to_have_count(1)
    assert live_server["db"].photos.get(photo_id)[field] == (3 if field == "rating" else "rejected")
    assert requests == [{"photo_ids": [photo_id], field: 3 if field == "rating" else "rejected"}] * 2


@pytest.mark.parametrize("surface", ["grid", "burst"])
@pytest.mark.parametrize("failure", ["lookup", "missing-path", "denied"])
def test_copy_path_reports_failure_and_confirms_only_after_clipboard_write(live_server, page, surface, failure):
    target, photo_id = mount_review(live_server, page, surface)
    page.evaluate("""() => {
      window.copyAttempts = [];
      Object.defineProperty(navigator, 'clipboard', {configurable: true, value: {
        writeText: path => { copyAttempts.push(path); return Promise.reject(new Error('Permission denied')); }
      }});
    }""")
    pattern = f"**/api/photos/{photo_id}"
    page.route(pattern, lambda route: route.fulfill(
        status=503 if failure == "lookup" else 200,
        json={"error": "Photo unavailable"} if failure == "lookup" else {} if failure == "missing-path" else {"path": "/photos/hawk.jpg"},
    ))
    _dispatch_contextmenu(target)
    page.locator(".vireo-ctx-item", has_text="Copy Path").click()
    error = page.locator('#toastContainer > [data-type="error"]')
    expect(error).to_have_count(1)
    expect(error).to_contain_text("Could not copy path")
    expect(page.locator('#toastContainer > [data-type="success"]')).to_have_count(0)
    page.unroute(pattern)
    page.route(pattern, lambda route: route.fulfill(json={"path": "/photos/hawk.jpg"}))
    page.evaluate("""() => {
      navigator.clipboard.writeText = path => new Promise(resolve => {
        copyAttempts.push(path); window.finishCopy = resolve;
      });
    }""")
    _dispatch_contextmenu(target)
    page.locator(".vireo-ctx-item", has_text="Copy Path").click()
    page.wait_for_function("typeof finishCopy === 'function'")
    expect(page.locator('#toastContainer > [data-type="success"]')).to_have_count(0)
    page.evaluate("finishCopy()")
    expect(page.locator('#toastContainer > [data-type="success"]')).to_have_text("Path copied")
    assert page.evaluate("copyAttempts.at(-1)") == "/photos/hawk.jpg"


@pytest.mark.parametrize("field", ["rating", "flag"])
def test_rapid_choices_are_serialized_and_duplicate_pending_clicks_share_one_write(live_server, page, field):
    mount_review(live_server, page)
    photo_id = live_server["data"]["photos"][0]
    held = []
    page.route(f"**/api/batch/{field}", lambda route: held.append(route))
    with page.expect_request(f"**/api/batch/{field}"):
        page.evaluate("""({field, photoId}) => {
          const set = field === 'rating' ? setReviewRating : setReviewFlag;
          window.firstSave = set(photoId, field === 'rating' ? 1 : 'rejected');
          window.lastSave = set(photoId, field === 'rating' ? 5 : 'none');
          window.duplicateSave = set(photoId, field === 'rating' ? 5 : 'none');
        }""", {"field": field, "photoId": photo_id})
    wait_for_held(page, held, 1)
    expect(page.locator('#toastContainer > [data-type="success"]')).to_have_count(0)
    with page.expect_request(f"**/api/batch/{field}"):
        held[0].continue_()
    page.evaluate("async () => { await firstSave; }")
    assert page.evaluate("lastSave === duplicateSave") is True
    wait_for_held(page, held, 2)
    held[1].continue_()
    assert page.evaluate("async () => await lastSave") is True
    assert live_server["db"].photos.get(photo_id)[field] == (5 if field == "rating" else "none")
    assert len(held) == 2
    expect(page.locator('#toastContainer > [data-type="success"]')).to_have_count(2)


@pytest.mark.parametrize("surface", ["burst", "lightbox"])
@pytest.mark.parametrize("field", ["rating", "flag"])
def test_native_review_actions_never_announce_success_before_a_save(live_server, page, surface, field):
    mount_review(live_server, page, surface)
    held = []
    page.route(f"**/api/batch/{field}", lambda route: held.append(route))
    command = "nativeMenuSetRating" if field == "rating" else "nativeMenuSetFlag"
    value = 4 if field == "rating" else "flagged"
    with page.expect_request(f"**/api/batch/{field}"):
        page.evaluate("""({command, value}) => {
          window.nativeSave = window[command](value);
        }""", {"command": command, "value": value})
    expect(page.locator("#toastContainer > div")).to_have_count(0)
    wait_for_held(page, held, 1)
    held[0].fulfill(status=503, json={"error": "temporarily unavailable"})
    expect(page.locator('#toastContainer > [data-type="error"]')).to_have_count(1)
    expect(page.locator('#toastContainer > [data-type="success"]')).to_have_count(0)
    expect(page.locator('#toastContainer > [data-type="info"]')).to_have_count(0)
    with page.expect_request(f"**/api/batch/{field}"):
        page.evaluate("""({command, value}) => {
          window.nativeSave = window[command](value);
        }""", {"command": command, "value": value})
    wait_for_held(page, held, 2)
    held[1].continue_()
    expect(page.locator('#toastContainer > [data-type="success"]')).to_have_count(1)
    expect(page.locator('#toastContainer > [data-type="info"]')).to_have_count(0)


def test_lightbox_rapid_flag_failure_restores_the_last_confirmed_flag(live_server, page):
    _, photo_id = mount_review(live_server, page, "lightbox")
    held = []
    page.route("**/api/batch/flag", lambda route: held.append(route))
    with page.expect_request("**/api/batch/flag"):
        page.keyboard.press("x")
    wait_for_held(page, held, 1)
    page.keyboard.press("p")
    assert len(held) == 1
    with page.expect_request("**/api/batch/flag"):
        held[0].continue_()
    wait_for_held(page, held, 2)
    held[1].fulfill(status=503, json={"error": "temporarily unavailable"})
    page.wait_for_function("_lbFlagPendingWrites === 0")
    expect(page.locator("#lightboxFlagStatus")).to_contain_text("Rejected")
    expect(page.locator('#toastContainer > [data-type="error"]')).to_have_count(1)
    assert live_server["db"].photos.get(photo_id)["flag"] == "rejected"
