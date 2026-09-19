import json
import re

from playwright.sync_api import expect


def test_settings_links_to_storage_page(live_server, page):
    """Settings makes the dedicated storage page discoverable."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)

    storage_link = page.get_by_role("link", name="Open Storage")
    expect(storage_link).to_be_visible()
    expect(storage_link).to_have_attribute("href", "/storage")


def test_settings_system_info_renders(live_server, page):
    """Settings page loads and displays system info section with real data."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)

    # The system info section should be visible
    system_section = page.locator(".section-title", has_text="System")
    expect(system_section).to_be_visible()

    # /api/system/info populates these fields asynchronously via JS.
    # Allow a generous timeout for the async fetch to complete.
    api_timeout = 30_000

    # Compute device should be populated
    device_name = page.locator("#deviceName")
    expect(device_name).not_to_have_text("-", timeout=api_timeout)


def test_settings_cmd_f_opens_page_text_search(live_server, page):
    """Settings captures find shortcut and highlights page text matches."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)

    page.keyboard.press("Control+F")

    find_panel = page.locator("#settingsFindPanel")
    find_input = page.locator("#settingsFindInput")
    expect(find_panel).to_be_visible()
    expect(find_input).to_be_focused()

    find_input.fill("api")
    expect(page.locator(".settings-find-mark").first).to_be_visible()
    expect(page.locator(".settings-find-mark.active")).to_have_count(1)
    expect(page.locator("#settingsFindStatus")).to_contain_text("of")


def test_settings_text_search_ignores_hidden_update_section(live_server, page):
    """Settings find should not count permanently hidden page text."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)

    page.keyboard.press("Control+F")
    page.locator("#settingsFindInput").fill("Check for Updates")

    expect(page.locator(".settings-find-mark")).to_have_count(0)
    expect(page.locator("#settingsFindStatus")).to_have_text("0 results")


def _wait_for_settings_idle(page):
    """Let the initial config load (and any migration autosave) settle.

    Waits for the explicit ready marker the page flips on after both
    /api/config and /api/workspaces/active/config have been fetched and
    applied, otherwise an interaction can beat loadConfig() to a field
    (and see its value overwritten) or trigger saveWsConfig() before
    _wsOverridesLoaded flips on (a silent no-op that skips the POST the
    test is asserting against). The autosave-pill idle check comes after
    that, since it has no state at all before loads complete and would
    pass immediately on a slow server.
    """
    status = page.locator("#settingsSaveStatus")
    expect(page.locator("#cfgKeywordCase")).to_be_visible()
    expect(page.locator("body")).to_have_attribute(
        "data-settings-ready", "true", timeout=10_000
    )
    expect(status).not_to_have_attribute("data-state", "saving", timeout=10_000)
    return status


def test_remote_target_folder_browse_saves_and_cancel_preserves(live_server, page, tmp_path):
    """Both local fields browse real folders and persist to the right target."""
    mount = tmp_path / "Mounted Photos"
    archive = tmp_path / "Local Archive"
    mount.mkdir()
    archive.mkdir()
    targets = [
        {"id": "nas-a", "name": "First NAS", "host": "nas-a", "user": "me",
         "remote_path": "/photos", "mount_path": str(tmp_path)},
        {"id": "nas-b", "name": "Second NAS", "host": "nas-b", "user": "me",
         "remote_path": "/photos", "mount_path": str(tmp_path),
         "local_archive_root": str(tmp_path)},
    ]
    page.request.post(live_server["url"] + "/api/config", data={"remote_targets": targets})
    page.goto(live_server["url"] + "/settings")
    _wait_for_settings_idle(page)
    for label, folder in (("Local mount path", mount),
                          ("Local archive root (chained moves)", archive)):
        page.get_by_role("button", name="Browse for " + label, exact=True).nth(1).click()
        browser = page.locator("#folderBrowser")
        expect(browser).to_be_visible()
        browser.locator(".folder-browser-item", has_text=folder.name).click()
        expect(page.locator("#folderBrowserPath")).to_have_text(str(folder))
        with page.expect_response(lambda r: "/api/config" in r.url and r.request.method == "POST"):
            page.get_by_role("button", name="Select This Folder", exact=True).click()
        expect(page.get_by_role("textbox", name=label, exact=True).nth(1)).to_have_value(str(folder))

    page.get_by_role("button", name="Browse for Local mount path", exact=True).nth(1).click()
    page.locator("#folderBrowser").get_by_role("button", name="Cancel", exact=True).click()
    page.reload()
    _wait_for_settings_idle(page)
    mounts = page.get_by_role("textbox", name="Local mount path", exact=True)
    expect(mounts.nth(0)).to_have_value(str(tmp_path))
    expect(mounts.nth(1)).to_have_value(str(mount))
    expect(page.get_by_role("textbox", name="Local archive root (chained moves)", exact=True).nth(1)).to_have_value(str(archive))


def test_remote_target_native_picker_cancel_and_selection(live_server, page):
    page.goto(live_server["url"] + "/settings")
    _wait_for_settings_idle(page)
    page.evaluate("""() => {
      _remoteTargetsState = [{id: 'nas', host: 'nas', user: 'me', remote_path: '/photos',
                             mount_path: '/original'}];
      renderRemoteTargets();
      window.isTauri = () => true;
      window.pickDirectory = async () => null;
    }""")
    button = page.get_by_role("button", name="Browse for Local mount path", exact=True)
    button.click()
    expect(button).to_be_enabled()
    expect(page.locator("#folderBrowser")).not_to_be_visible()
    field = page.get_by_role("textbox", name="Local mount path", exact=True)
    expect(field).to_have_value("/original")
    page.evaluate("window.pickDirectory = async () => '/chosen folder'")
    with page.expect_response(lambda r: "/api/config" in r.url and r.request.method == "POST"):
        button.click()
    expect(field).to_have_value("/chosen folder")


def test_remote_target_rsync_hint_copies_install_command(live_server, page):
    page.route("**/api/remote-targets/test", lambda route: route.fulfill(json={
        "ok": False, "message": "Install GNU rsync with Homebrew: brew install rsync.",
        "rsync_install_commands": ["brew install rsync"],
    }))
    page.goto(live_server["url"] + "/settings")
    _wait_for_settings_idle(page)
    page.evaluate("""() => {
      _remoteTargetsState = [{id: 'nas', host: 'nas', user: 'me', remote_path: '/photos'}];
      renderRemoteTargets();
      Object.defineProperty(navigator, 'clipboard', {value: {
        writeText: async text => { window.copiedCommand = text; }
      }, configurable: true});
    }""")
    page.get_by_role("button", name="Test connection", exact=True).click()
    copy = page.get_by_role("button", name="Copy brew install rsync", exact=True)
    expect(copy).to_be_visible()
    copy.click()
    expect(copy).to_have_text("Copied")
    assert page.evaluate("window.copiedCommand") == "brew install rsync"


def test_settings_autosave_shows_saved_confirmation(live_server, page):
    """Changing a curated field reports Saving… and then a persistent Saved ✓ time."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    status = _wait_for_settings_idle(page)

    with page.expect_request(
        lambda r: r.url.endswith("/api/config") and r.method == "POST"
    ):
        page.select_option("#cfgKeywordCase", "title")
        # The write is debounced 500 ms; the pill must already say so.
        expect(status).to_have_attribute("data-state", "saving")
        expect(status).to_have_text("Saving…")

    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    expect(status).to_contain_text("Saved ✓")
    # A wall-clock time so the confirmation still means something after the
    # transient flash is gone.
    expect(status).to_have_text(re.compile(r"Saved ✓ \d{1,2}:\d{2}:\d{2}"))

    # The page must agree with what is actually on disk.
    cfg = page.request.get(f"{url}/api/config").json()
    assert cfg["keyword_case"] == "title"


def test_settings_autosave_failure_is_visible(live_server, page):
    """A rejected save is reported in the pill instead of looking like success."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    status = _wait_for_settings_idle(page)

    def _fail_post(route, request):
        if request.method == "POST":
            route.fulfill(status=500, content_type="application/json",
                          body='{"error": "disk full"}')
        else:
            route.continue_()

    page.route("**/api/config", _fail_post)
    page.select_option("#cfgKeywordCase", "lower")
    expect(status).to_have_attribute("data-state", "error", timeout=10_000)
    expect(status).to_contain_text("Save failed")

    # The next successful write clears the failure.
    page.unroute("**/api/config", _fail_post)
    page.select_option("#cfgKeywordCase", "title")
    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    expect(status).to_contain_text("Saved ✓")
    cfg = page.request.get(f"{url}/api/config").json()
    assert cfg["keyword_case"] == "title"


def test_settings_workspace_override_autosave_shows_saved(live_server, page):
    """The workspace-overrides form reports through the same pill."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    status = _wait_for_settings_idle(page)

    checkbox = page.locator("#wsOverride_classification_threshold")
    expect(checkbox).to_be_visible()
    with page.expect_request(
        lambda r: r.url.endswith("/api/workspaces/active/config") and r.method == "POST"
    ):
        checkbox.check()
        expect(status).to_have_attribute("data-state", "saving")

    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    expect(status).to_contain_text("Saved ✓")
    overrides = page.request.get(f"{url}/api/workspaces/active/config").json()
    assert "classification_threshold" in overrides


def test_settings_cleared_override_is_not_reported_as_saved(live_server, page):
    """A blank override is dropped from the save; the pill must not claim it saved."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    status = _wait_for_settings_idle(page)

    checkbox = page.locator("#wsOverride_grouping_window_seconds")
    checkbox.check()
    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    value = page.locator("#wsVal_grouping_window_seconds")
    expect(value).to_have_value("10")

    # Hold the resync GET so we can observe that "Saved" waits for it.
    held = []

    def _hold_get(route, request):
        if request.method == "GET":
            held.append(route)
        else:
            route.continue_()

    page.route("**/api/workspaces/active/config", _hold_get)
    with page.expect_response(
        lambda r: r.url.endswith("/api/workspaces/active/config")
        and r.request.method == "POST"
    ):
        value.fill("")
        value.dispatch_event("change")
        expect(status).to_have_attribute("data-state", "saving")
    # The POST has completed but the resync GET is held: still not "Saved".
    for _ in range(40):
        if held:
            break
        page.wait_for_timeout(50)
    assert held, "resync GET was never requested"
    expect(status).to_have_attribute("data-state", "saving")

    for route in held:
        route.continue_()
    page.unroute("**/api/workspaces/active/config", _hold_get)
    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    # The field shows the value actually in effect, not the blank entry.
    expect(value).to_have_value("10")
    overrides = page.request.get(f"{url}/api/workspaces/active/config").json()
    assert overrides["grouping_window_seconds"] == 10


def test_settings_saves_for_same_path_are_serialized(live_server, page):
    """A second edit waits for the in-flight POST so snapshots reach the server in order."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    status = _wait_for_settings_idle(page)

    held = []

    def _hold_first_post(route, request):
        if request.method == "POST" and not held:
            held.append(route)
        else:
            route.continue_()

    page.route("**/api/config", _hold_first_post)
    page.select_option("#cfgKeywordCase", "title")
    for _ in range(60):
        if held:
            break
        page.wait_for_timeout(50)
    assert held, "first POST was never requested"

    # Second edit while the first POST is stalled: its POST must not be sent
    # until the first one settles.
    posts_seen = []
    page.on(
        "request",
        lambda r: posts_seen.append(r)
        if r.url.endswith("/api/config") and r.method == "POST"
        else None,
    )
    page.select_option("#cfgKeywordCase", "lower")
    page.wait_for_timeout(1200)  # well past the 500 ms debounce
    assert not posts_seen, "second POST was sent while the first was in flight"
    expect(status).to_have_attribute("data-state", "saving")

    with page.expect_request(
        lambda r: r.url.endswith("/api/config") and r.method == "POST"
    ):
        held[0].continue_()
    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    cfg = page.request.get(f"{url}/api/config").json()
    assert cfg["keyword_case"] == "lower"


def test_settings_import_drops_queued_autosave(live_server, page):
    """A save queued behind an in-flight POST must not overwrite an import."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    status = _wait_for_settings_idle(page)
    page.on("dialog", lambda d: d.accept())

    held = []
    config_posts = []

    def _hold_first_post(route, request):
        if request.method == "POST":
            config_posts.append(request)
            if len(config_posts) == 1:
                held.append(route)
                return
        route.continue_()

    page.route("**/api/config", _hold_first_post)
    page.select_option("#cfgKeywordCase", "title")
    for _ in range(60):
        if held:
            break
        page.wait_for_timeout(50)
    assert held, "first POST was never requested"

    # Second edit queues behind the stalled POST.
    page.select_option("#cfgKeywordCase", "lower")
    page.wait_for_timeout(700)
    assert len(config_posts) == 1

    import_posts = []
    page.on(
        "request",
        lambda r: import_posts.append(r)
        if r.url.endswith("/api/settings/import")
        else None,
    )
    payload = page.request.get(f"{url}/api/settings/export").json()
    payload["keyword_case"] = "auto"
    page.set_input_files(
        "#settingsImportInput",
        {
            "name": "backup.json",
            "mimeType": "application/json",
            "buffer": json.dumps(payload).encode(),
        },
    )
    # Import waits for the in-flight save to settle before replacing config.
    page.wait_for_timeout(500)
    assert not import_posts, "import POSTed while a save was still in flight"

    with page.expect_response("**/api/settings/import"):
        held[0].continue_()
    expect(page.locator("#cfgKeywordCase")).to_have_value("auto", timeout=10_000)
    page.wait_for_timeout(700)
    # The queued 'lower' snapshot was dropped: only the first POST ever went out.
    assert len(config_posts) == 1
    expect(status).not_to_have_attribute("data-state", "saving")
    cfg = page.request.get(f"{url}/api/config").json()
    assert cfg["keyword_case"] == "auto"


def test_settings_failed_import_requeues_dropped_autosave(live_server, page):
    """If the import is rejected, the edit dropped for it is saved after all."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    status = _wait_for_settings_idle(page)
    page.on("dialog", lambda d: d.accept())

    held = []
    config_posts = []

    def _hold_first_post(route, request):
        if request.method == "POST":
            config_posts.append(request)
            if len(config_posts) == 1:
                held.append(route)
                return
        route.continue_()

    page.route("**/api/config", _hold_first_post)
    page.select_option("#cfgKeywordCase", "title")
    for _ in range(60):
        if held:
            break
        page.wait_for_timeout(50)
    assert held, "first POST was never requested"
    page.select_option("#cfgKeywordCase", "lower")
    page.wait_for_timeout(700)
    assert len(config_posts) == 1

    # Invalid payload: the server rejects it and leaves config untouched.
    page.set_input_files(
        "#settingsImportInput",
        {
            "name": "bad.json",
            "mimeType": "application/json",
            "buffer": json.dumps({"classification_threshold": "not a number"}).encode(),
        },
    )
    page.wait_for_timeout(300)
    with page.expect_response(
        lambda r: r.url.endswith("/api/settings/import") and r.status == 400
    ):
        held[0].continue_()

    # The dropped 'lower' edit is re-queued and persists.
    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    assert len(config_posts) >= 2
    expect(page.locator("#cfgKeywordCase")).to_have_value("lower")
    cfg = page.request.get(f"{url}/api/config").json()
    assert cfg["keyword_case"] == "lower"


def test_settings_import_preserves_queued_workspace_override_save(live_server, page):
    """A queued workspace-override save must not be canceled by an import.

    The import only replaces global config and preserves workspace overrides;
    a queued /api/workspaces/active/config POST behind an in-flight one
    should still fire and land server-side, otherwise the UI would keep
    showing the newer value while the server retained the older one.
    """
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    _wait_for_settings_idle(page)
    page.on("dialog", lambda d: d.accept())

    checkbox = page.locator("#wsOverride_grouping_window_seconds")
    value_input = page.locator("#wsVal_grouping_window_seconds")

    # Track every workspace-override POST via a global request listener so
    # that unrouting later doesn't stop counting the queued follow-up.
    ws_posts = []
    page.on(
        "request",
        lambda r: ws_posts.append(r)
        if r.url.endswith("/api/workspaces/active/config") and r.method == "POST"
        else None,
    )

    # Hold the first workspace-override POST so we can stack a second edit
    # behind it in the workspace save chain.
    held_ws = []

    def _hold_first_ws_post(route, request):
        if request.method == "POST" and not held_ws:
            held_ws.append(route)
            return
        route.continue_()

    page.route("**/api/workspaces/active/config", _hold_first_ws_post)

    checkbox.check()
    for _ in range(60):
        if held_ws:
            break
        page.wait_for_timeout(50)
    assert held_ws, "first workspace override POST was never requested"

    # Second override edit is queued behind the stalled POST via the
    # workspace save chain.
    value_input.fill("55")
    value_input.dispatch_event("change")
    page.wait_for_timeout(700)  # past the 500 ms debounce
    assert len(ws_posts) == 1, (
        f"second POST fired before the first settled: {len(ws_posts)}"
    )

    # Kick off an import. It must NOT cancel the queued workspace save.
    payload = page.request.get(f"{url}/api/settings/export").json()
    page.set_input_files(
        "#settingsImportInput",
        {
            "name": "backup.json",
            "mimeType": "application/json",
            "buffer": json.dumps(payload).encode(),
        },
    )
    # Give the import a chance to start and reach _cancelQueuedSaves.
    page.wait_for_timeout(300)

    # Release the first workspace POST — the queued 55 snapshot must fire
    # (the import must not have discarded it).
    with page.expect_response(
        lambda r: r.url.endswith("/api/workspaces/active/config")
        and r.request.method == "POST"
    ):
        held_ws[0].continue_()

    page.unroute("**/api/workspaces/active/config", _hold_first_ws_post)
    overrides = None
    for _ in range(80):
        overrides = page.request.get(f"{url}/api/workspaces/active/config").json()
        if overrides.get("grouping_window_seconds") == 55:
            break
        page.wait_for_timeout(50)
    assert overrides and overrides.get("grouping_window_seconds") == 55, (
        f"queued workspace override was dropped by the settings import: {overrides}"
    )
    assert len(ws_posts) == 2, (
        f"expected 2 workspace POSTs (initial + queued), got {len(ws_posts)}"
    )


def test_settings_edit_during_import_suspension_is_saved_on_failure(live_server, page):
    """An edit made while autosave is suspended for the import must not be lost."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    status = _wait_for_settings_idle(page)
    page.on("dialog", lambda d: d.accept())

    import_routes = []
    config_posts = []

    def _hold_import(route, request):
        if request.method == "POST":
            import_routes.append(route)
        else:
            route.continue_()

    page.on(
        "request",
        lambda r: config_posts.append(r)
        if r.url.endswith("/api/config") and r.method == "POST"
        else None,
    )
    page.route("**/api/settings/import", _hold_import)

    page.set_input_files(
        "#settingsImportInput",
        {
            "name": "bad.json",
            "mimeType": "application/json",
            "buffer": json.dumps({"classification_threshold": "not a number"}).encode(),
        },
    )
    for _ in range(60):
        if import_routes:
            break
        page.wait_for_timeout(50)
    assert import_routes, "import POST was never sent"

    # Edit a curated field while the import is stalled: saveConfig() bails
    # out because _autosaveSuspended is true, but the edit must be
    # remembered so it is written after the import fails.
    page.select_option("#cfgKeywordCase", "title")
    page.wait_for_timeout(700)
    assert not config_posts, "config POST fired while autosave was suspended"

    import_routes[0].fulfill(
        status=400,
        content_type="application/json",
        body='{"error": "bad"}',
    )

    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    assert config_posts, "suspended edit was never saved after the import failed"
    cfg = page.request.get(f"{url}/api/config").json()
    assert cfg["keyword_case"] == "title"


def test_settings_successful_import_clears_prior_save_error(live_server, page):
    """A prior autosave error must not linger on the pill after a successful import."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    status = _wait_for_settings_idle(page)
    page.on("dialog", lambda d: d.accept())

    def _fail_post(route, request):
        if request.method == "POST":
            route.fulfill(
                status=500,
                content_type="application/json",
                body='{"error": "disk full"}',
            )
        else:
            route.continue_()

    page.route("**/api/config", _fail_post)
    page.select_option("#cfgKeywordCase", "lower")
    expect(status).to_have_attribute("data-state", "error", timeout=10_000)
    page.unroute("**/api/config", _fail_post)

    # Import a valid backup: the pill must move out of the error state
    # (the import replaced the whole config on the server, so the earlier
    # unsaved autosave no longer describes what is on disk).
    payload = page.request.get(f"{url}/api/settings/export").json()
    payload["keyword_case"] = "auto"
    with page.expect_response("**/api/settings/import"):
        page.set_input_files(
            "#settingsImportInput",
            {
                "name": "backup.json",
                "mimeType": "application/json",
                "buffer": json.dumps(payload).encode(),
            },
        )
    expect(page.locator("#cfgKeywordCase")).to_have_value("auto", timeout=10_000)
    expect(status).not_to_have_attribute("data-state", "error", timeout=10_000)


def test_settings_override_edit_during_resync_survives(live_server, page):
    """An override edit made while the post-invalid resync GET is in flight is not overwritten."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    status = _wait_for_settings_idle(page)

    # Enable the grouping override so blanking it triggers the resync branch.
    checkbox = page.locator("#wsOverride_grouping_window_seconds")
    checkbox.check()
    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    value = page.locator("#wsVal_grouping_window_seconds")
    expect(value).to_have_value("10")

    # Hold the resync GET so we can edit a DIFFERENT field while the reload
    # is in flight. A prior implementation used a full loadWsOverrides()
    # here, which iterated every override and reset the newly checked one
    # from the server response (still empty), silently losing the edit.
    held = []

    def _hold_get(route, request):
        if request.method == "GET":
            held.append(route)
        else:
            route.continue_()

    page.route("**/api/workspaces/active/config", _hold_get)
    with page.expect_response(
        lambda r: r.url.endswith("/api/workspaces/active/config")
        and r.request.method == "POST"
    ):
        value.fill("")
        value.dispatch_event("change")
        expect(status).to_have_attribute("data-state", "saving")

    for _ in range(40):
        if held:
            break
        page.wait_for_timeout(50)
    assert held, "resync GET was never requested"

    other_checkbox = page.locator("#wsOverride_similarity_threshold")
    other_checkbox.check()
    other_value = page.locator("#wsVal_similarity_threshold")
    other_value.fill("77")
    other_value.dispatch_event("change")

    for route in held:
        route.continue_()
    page.unroute("**/api/workspaces/active/config", _hold_get)

    # The queued save for the new edit fires after the resync settles;
    # wait for it so any resync-induced reset would have had time to hit.
    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    expect(other_checkbox).to_be_checked()
    expect(other_value).to_have_value("77")
    overrides = page.request.get(f"{url}/api/workspaces/active/config").json()
    assert "similarity_threshold" in overrides


def test_settings_import_reloads_page_when_config_refetch_fails(live_server, page):
    """If /api/config GET fails after a committed import, the page reloads.

    loadConfig() swallows fetch errors and falls back to placeholders so the
    page can still paint on first load. The import path used to trust that a
    successful await meant the form was fresh, so a failed post-import GET
    left autosave suspended cleared and the next curated edit POSTed the
    stale pre-import snapshot over the just-imported config. The current
    safeguard: loadConfig() returns false on failure, the import treats that
    as "form not reloaded" and the finally block reloads the page from the
    imported config on disk.
    """
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)
    _wait_for_settings_idle(page)
    dialogs = []
    page.on("dialog", lambda d: (dialogs.append(d.message), d.accept()))

    # Set the pre-import form to a value that must not be persisted after
    # the import (proves autosave stays off / the page truly reloads from
    # disk instead of the stale form).
    with page.expect_response(
        lambda r: r.url.endswith("/api/config") and r.request.method == "POST"
    ):
        page.select_option("#cfgKeywordCase", "lower")

    # Fail the /api/config GET that loadConfig() issues after the import
    # commits, but let the POST for /api/settings/import (the actual import)
    # and any config POSTs through. Stop failing once the page reloads: the
    # framenavigated event is delivered before the reloaded document's
    # subresource requests, so flipping the flag there is race-free. The
    # route stays registered for the rest of the test on purpose. Unrouting
    # while the reload is in flight changes the browser's interception
    # patterns mid-navigation, which intermittently wedged one of the
    # reloaded page's blocking script requests and left the document
    # half-parsed (navbar present, settings form never rendered).
    reloaded = {"seen": False}

    def _note_reload(frame):
        if frame == page.main_frame:
            reloaded["seen"] = True

    page.on("framenavigated", _note_reload)

    def _fail_config_get_until_reload(route, request):
        if request.method == "GET" and not reloaded["seen"]:
            route.fulfill(status=500, content_type="application/json",
                          body='{"error": "reload failed"}')
        else:
            route.continue_()

    page.route("**/api/config", _fail_config_get_until_reload)

    payload = page.request.get(f"{url}/api/settings/export").json()
    payload["keyword_case"] = "title"
    with page.expect_event("framenavigated", timeout=10_000):
        page.set_input_files(
            "#settingsImportInput",
            {
                "name": "backup.json",
                "mimeType": "application/json",
                "buffer": json.dumps(payload).encode(),
            },
        )

    # The refresh-failure dialog fired before the page reload.
    assert any("could not refresh" in m for m in dialogs), (
        "expected the refresh-failure alert before location.reload(); "
        f"saw {dialogs!r}"
    )

    # After the reload, the form is rebuilt from disk (the import
    # committed), not from any stale in-memory snapshot.
    _wait_for_settings_idle(page)
    expect(page.locator("#cfgKeywordCase")).to_have_value("title")
    cfg = page.request.get(f"{url}/api/config").json()
    assert cfg["keyword_case"] == "title"
