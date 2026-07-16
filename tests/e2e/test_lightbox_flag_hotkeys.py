"""E2E tests for the lightbox flag/reject/unflag keyboard shortcuts.

The lightbox lives in `_navbar.html` and is shared across pages. Pressing
`p` / `x` / `u` while the lightbox is open should flag / reject / unflag the
displayed photo, regardless of whether the host page defines `setFlagFor`
(browse), `setReviewFlag` (review), or neither (misses, pipeline-review).
"""
import time


def _open_lightbox_on_browse(page, url):
    page.goto(f"{url}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.dblclick()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    page.wait_for_function(
        "typeof _lightboxCurrentId !== 'undefined' && _lightboxCurrentId !== null",
        timeout=3000,
    )


def _current_lightbox_id(page):
    return page.evaluate("_lightboxCurrentId")


def _wait_for_flag(db, photo_id, expected, timeout=3.0):
    """Poll the DB until the photo's flag matches `expected`. The keydown
    handler dispatches the flag write fire-and-forget, so the round-trip is
    not synchronous from Playwright's perspective."""
    deadline = time.time() + timeout
    flag = None
    while time.time() < deadline:
        photo = db.get_photo(photo_id)
        flag = photo["flag"] if photo else None
        if flag == expected:
            return flag
        time.sleep(0.05)
    return flag


def test_lightbox_x_rejects_photo(live_server, page):
    """Pressing `x` in the lightbox sets flag=rejected on the displayed photo."""
    url = live_server["url"]
    db = live_server["db"]
    _open_lightbox_on_browse(page, url)
    pid = _current_lightbox_id(page)

    page.keyboard.press("x")

    flag = _wait_for_flag(db, pid, "rejected")
    assert flag == "rejected", f"expected 'rejected', got {flag!r}"
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')"
        " && document.getElementById('lightboxFlagStatus').textContent.trim() === 'Rejected'"
        " && document.getElementById('lightboxFlagStatus').classList.contains('rejected')",
        timeout=3000,
    )


def test_lightbox_x_not_overwritten_by_slow_initial_metadata(live_server, page):
    """A slow /api/photos/<id> response from lightbox open must not clobber
    the immediate flag feedback from a hotkey pressed while it is in flight."""
    url = live_server["url"]
    db = live_server["db"]
    page.goto(f"{url}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    pid = int(first.get_attribute("data-id"))

    page.evaluate(
        """pid => {
          const originalFetch = window.fetch.bind(window);
          window.__heldPhotoFetch = { captured: false, released: false };
          window.fetch = function(input, init) {
            const rawUrl = typeof input === 'string' ? input : (input && input.url) || '';
            let path = rawUrl;
            try { path = new URL(rawUrl, window.location.href).pathname; } catch (e) {}
            if (!window.__heldPhotoFetch.captured && path === '/api/photos/' + pid) {
              return originalFetch(input, init).then(response => {
                const status = response.status;
                const statusText = response.statusText;
                const headers = {};
                response.headers.forEach((value, key) => { headers[key] = value; });
                return response.text().then(body => {
                  window.__heldPhotoFetch.captured = true;
                  return new Promise(resolve => {
                    window.__releaseHeldPhotoFetch = function() {
                      window.__heldPhotoFetch.released = true;
                      window.fetch = originalFetch;
                      resolve(new Response(body, { status, statusText, headers }));
                    };
                  });
                });
              });
            }
            return originalFetch(input, init);
          };
        }""",
        pid,
    )

    page.evaluate(
        "pid => { const p = photos.find(x => x.id === pid);"
        " openLightbox(pid, p ? p.filename : '', photos); }",
        pid,
    )
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    page.wait_for_function(
        "typeof _lightboxCurrentId !== 'undefined' && _lightboxCurrentId !== null",
        timeout=3000,
    )
    page.wait_for_function("window.__heldPhotoFetch && window.__heldPhotoFetch.captured")

    page.keyboard.press("x")
    flag = _wait_for_flag(db, pid, "rejected")
    assert flag == "rejected", f"expected 'rejected', got {flag!r}"
    page.wait_for_function(
        "document.getElementById('lightboxFlagStatus').textContent.trim() === 'Rejected'",
        timeout=3000,
    )

    page.evaluate("window.__releaseHeldPhotoFetch()")
    page.wait_for_timeout(100)
    assert page.locator("#lightboxFlagStatus").inner_text().strip() == "Rejected"


def test_lightbox_reopen_ignores_stale_metadata_from_previous_open(live_server, page):
    """A delayed metadata response from an earlier open of the same photo
    should not apply after the lightbox is closed and reopened."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    pid = int(first.get_attribute("data-id"))
    base_photo = {
        "id": pid,
        "filename": first.get_attribute("data-filename") or "photo.jpg",
        "width": 100,
        "height": 100,
        "flag": "none",
        "metadata": None,
        "keywords": [],
        "location": None,
        "xmp_exists": False,
        "xmp_keywords": [],
        "path": "",
    }

    held = {}
    count = {"value": 0}

    def hold_first_photo_fetch(route):
        count["value"] += 1
        if count["value"] == 1:
            data = dict(base_photo)
            data["flag"] = "rejected"
            held["json"] = data
            held["route"] = route
            return
        route.fulfill(json=base_photo)

    page.route(f"**/api/photos/{pid}", hold_first_photo_fetch)

    page.evaluate(
        "pid => { const p = photos.find(x => x.id === pid);"
        " openLightbox(pid, p ? p.filename : '', photos); }",
        pid,
    )
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    deadline = time.time() + 3
    while "route" not in held and time.time() < deadline:
        time.sleep(0.05)
    assert "route" in held, "expected first lightbox metadata fetch to be held"

    page.evaluate("closeLightbox()")
    page.evaluate(
        "pid => { const p = photos.find(x => x.id === pid);"
        " openLightbox(pid, p ? p.filename : '', photos); }",
        pid,
    )
    page.wait_for_function(
        "document.getElementById('lightboxFlagStatus').textContent.trim() === 'No flag'",
        timeout=3000,
    )

    held["route"].fulfill(json=held["json"])
    page.wait_for_timeout(100)
    assert page.locator("#lightboxFlagStatus").inner_text().strip() == "No flag"


def test_lightbox_x_reverts_feedback_when_flag_write_fails(live_server, page):
    """A failed flag write should not leave a false rejected confirmation."""
    url = live_server["url"]
    db = live_server["db"]
    _open_lightbox_on_browse(page, url)
    pid = _current_lightbox_id(page)

    page.route(
        "**/api/batch/flag",
        lambda route: route.fulfill(
            status=500,
            content_type="application/json",
            body='{"error":"forced failure"}',
        ),
    )

    page.keyboard.press("x")

    page.wait_for_function(
        "document.getElementById('lightboxFlagStatus').textContent.trim() === 'No flag'",
        timeout=3000,
    )
    assert db.get_photo(pid)["flag"] in (None, "none")


def test_lightbox_newer_failed_write_falls_back_to_older_success(live_server, page):
    """If a newer flag write fails while an older write later succeeds, the
    lightbox should settle on the older confirmed flag rather than stale state."""
    url = live_server["url"]
    _open_lightbox_on_browse(page, url)

    held = {}
    count = {"value": 0}

    def handle_flag_write(route):
        count["value"] += 1
        if count["value"] == 1:
            held["route"] = route
            return
        route.fulfill(
            status=500,
            content_type="application/json",
            body='{"error":"forced failure"}',
        )

    page.route("**/api/batch/flag", handle_flag_write)

    page.keyboard.press("x")
    # The flag write is dispatched asynchronously after the keypress; under CI
    # CPU contention it can take well over 3s to reach the route handler, so use
    # generous headroom before asserting it was held (the loop exits as soon as
    # the request lands, so the ceiling only matters on the failure path).
    deadline = time.time() + 10
    while "route" not in held and time.time() < deadline:
        time.sleep(0.05)
    assert "route" in held, "expected first flag write to be held"

    page.keyboard.press("p")
    page.wait_for_function(
        "document.getElementById('lightboxFlagStatus').textContent.trim() === 'No flag'",
        timeout=3000,
    )

    held["route"].fulfill(status=200, content_type="application/json", body="{}")
    page.wait_for_function(
        "document.getElementById('lightboxFlagStatus').textContent.trim() === 'Rejected'",
        timeout=3000,
    )


def test_lightbox_p_flags_photo(live_server, page):
    """Pressing `p` in the lightbox sets flag=flagged on the displayed photo."""
    url = live_server["url"]
    db = live_server["db"]
    _open_lightbox_on_browse(page, url)
    pid = _current_lightbox_id(page)

    page.keyboard.press("p")

    flag = _wait_for_flag(db, pid, "flagged")
    assert flag == "flagged", f"expected 'flagged', got {flag!r}"
    page.wait_for_function(
        "document.getElementById('lightboxFlagStatus').textContent.trim() === 'Flagged'"
        " && document.getElementById('lightboxFlagStatus').classList.contains('flagged')",
        timeout=3000,
    )


def test_lightbox_u_unflags_photo(live_server, page):
    """Pressing `u` in the lightbox clears the flag on the displayed photo."""
    url = live_server["url"]
    db = live_server["db"]
    _open_lightbox_on_browse(page, url)
    pid = _current_lightbox_id(page)

    # Pre-flag the photo so we can verify that `u` clears it.
    db.update_photo_flag(pid, "flagged")
    assert db.get_photo(pid)["flag"] == "flagged"

    page.keyboard.press("u")

    flag = _wait_for_flag(db, pid, "none")
    assert flag == "none", f"expected 'none', got {flag!r}"
    page.wait_for_function(
        "document.getElementById('lightboxFlagStatus').textContent.trim() === 'No flag'"
        " && document.getElementById('lightboxFlagStatus').classList.contains('visible')",
        timeout=3000,
    )


def test_lightbox_honors_modifier_rebind(live_server, page):
    """A modifier-based rebind of `browse.flag` (e.g. ctrl+p) must trigger
    in the lightbox. Previously the lightbox keydown block lived inside a
    `!ctrlKey && !metaKey && !altKey` guard, so any modifier-combo rebind
    would silently fail even though Browse honored it."""
    url = live_server["url"]
    db = live_server["db"]
    _open_lightbox_on_browse(page, url)
    pid = _current_lightbox_id(page)

    # Rebind browse.flag to ctrl+p after the page loads its config. Browse
    # caches the value in a local `_shortcuts` (filled at page load from
    # window._vireoShortcuts.browse), so the test must update both — same
    # thing a real settings rebind achieves via the reload that follows.
    page.wait_for_function(
        "window._vireoShortcuts && window._vireoShortcuts.browse"
        " && typeof _shortcuts !== 'undefined' && _shortcuts !== null",
        timeout=3000,
    )
    page.evaluate(
        "window._vireoShortcuts.browse.flag = 'ctrl+p';"
        "_shortcuts.flag = 'ctrl+p';"
    )

    # Bare 'p' should now be a no-op (modifier mismatch).
    page.keyboard.press("p")
    time.sleep(0.3)
    assert db.get_photo(pid)["flag"] in (None, "none")

    # Ctrl+P should flag.
    page.keyboard.press("Control+p")
    flag = _wait_for_flag(db, pid, "flagged")
    assert flag == "flagged", f"expected 'flagged' after Ctrl+P, got {flag!r}"
