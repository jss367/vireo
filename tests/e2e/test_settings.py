import json
import re
from pathlib import Path

import pytest
from playwright.sync_api import expect

from e2e.in_page_dialogs import answer_dialogs, dialogs


def test_corrupt_config_warns_once_after_settings_are_saved(live_server, page):
    import config as cfg

    Path(cfg.CONFIG_PATH).write_text("{broken")
    cfg.set("classification_threshold", 0.31)
    page.goto(live_server["url"] + "/welcome?force=1")
    expect(page.locator("#configRecoveryNotice")).to_contain_text(
        "Vireo could not read config.json"
    )
    page.goto(live_server["url"] + "/settings")
    warning = page.locator(".notification-toast").filter(has_text="default settings")
    expect(warning).to_contain_text(cfg.CONFIG_PATH + ".corrupt")
    warning.get_by_role("button", name="Dismiss").click()
    with page.expect_response(lambda response: response.url.endswith("/api/config")):
        page.reload()
    page.locator("#notificationsToggle").click()
    history = page.locator("#notificationList li").filter(has_text="default settings")
    expect(history).to_have_count(1)
    expect(warning).to_have_count(0)


@pytest.mark.parametrize("state", ["ok", "unverified"])
def test_models_card_names_a_failed_common_name_repair(live_server, page, state):
    """A timm model missing label_descriptions.json gets Repair. When Vireo's
    own attempt to fetch the file failed, the card says why instead of
    leaving that to the log."""
    model = {
        "id": "timm-inat21-eva02-l", "name": "iNat21 (EVA-02 Large)",
        "model_str": "hf-hub:timm/eva02_large_patch14_clip_336.merged2b_ft_inat21",
        "model_type": "timm", "source": "timm", "downloaded": True, "state": state,
        "weights_path": "/models/timm", "missing_optional_files": ["label_descriptions.json"],
        "label_descriptions_error": "could not fetch the model's upstream config: offline",
    }
    page.route("**/api/models", lambda route: route.fulfill(
        json={"models": [model], "active_id": model["id"]},
    ))
    page.goto(live_server["url"] + "/settings?category=identification#manage-models")
    expect(page.locator("#speciesModelStatus").get_by_role("button", name="Repair")).to_be_visible()
    card = page.locator("#modelsContent")
    expect(card).to_contain_text(
        "Vireo's last attempt to fetch label_descriptions.json failed: could "
        "not fetch the model's upstream config: offline."
    )
    expect(card.get_by_role("button", name="Repair")).to_be_visible()


def test_eye_keypoint_threshold_uses_raw_score_units(live_server, page):
    url = live_server["url"]
    page.goto(url + "/settings?category=processing")
    _wait_for_settings_idle(page)
    page.locator(".collapsible-header").filter(has_text="Pipeline Tuning").click()
    field = page.locator("#cfgEyeDetectionConfGate")
    expect(field).to_have_value("0.5")
    expect(page.locator("#cfgEyeDetectionConfGateVal")).to_have_text("0.5")
    assert field.get_attribute("max") is None
    with page.expect_response(lambda r: r.url.endswith("/api/config") and r.request.method == "POST") as saved:
        field.fill("1.05")
        field.press("Tab")
    assert saved.value.ok
    assert page.request.get(url + "/api/config").json()["pipeline"]["eye_detection_conf_gate"] == 1.05
    page.reload()
    _wait_for_settings_idle(page)
    page.locator(".collapsible-header").filter(has_text="Pipeline Tuning").click()
    expect(field).to_have_value("1.05")
    expect(page.locator("#cfgEyeDetectionConfGateVal")).to_have_text("1.05")
    with page.expect_response(lambda r: r.url.endswith("/api/config") and r.request.method == "POST") as reset:
        page.get_by_role("button", name="Reset to defaults", exact=True).click()
    assert reset.value.ok
    expect(field).to_have_value("0.5")
    expect(page.locator("#cfgEyeDetectionConfGateVal")).to_have_text("0.5")
    assert page.request.get(url + "/api/config").json()["pipeline"]["eye_detection_conf_gate"] == 0.5


def test_time_format_saves_and_applies_to_photo_cards(live_server, page):
    url = live_server["url"]
    page.request.post(url + "/api/config", data={"browse_card_fields": ["filename", "capture_date"]})
    photo_id = live_server["data"]["photos"][0]

    for value, expected in [("12h", "2024-03-10 08:00 AM"), ("24h", "2024-03-10 08:00")]:
        page.goto(url + "/settings")
        clock = page.get_by_label("Time format", exact=True)
        expect(clock).to_be_enabled()
        # Wait for the asynchronous form population before making an edit.
        expect(page.locator("body")).to_have_attribute("data-settings-ready", "true")
        with page.expect_response(lambda response: response.url.endswith("/api/config")
                                 and response.request.method == "POST") as saved:
            clock.select_option(value)
        assert saved.value.ok
        expect(page.locator("#settingsSaveStatus")).to_contain_text("Saved")
        page.reload()
        expect(page.get_by_label("Time format", exact=True)).to_have_value(value)
        page.goto(url + "/browse")
        expect(page.locator(f'.grid-card[data-id="{photo_id}"] .grid-card-info')).to_contain_text(expected)

    # Reset through the schema editor, then verify the curated control follows it.
    response = page.request.delete(url + "/api/settings/global/time_format")
    assert response.ok
    page.goto(url + "/settings")
    expect(page.get_by_label("Time format", exact=True)).to_have_value("system")


def test_settings_links_to_storage_page(live_server, page):
    """Settings makes the dedicated storage page discoverable."""
    url = live_server["url"]
    page.goto(f"{url}/settings?category=storage", timeout=5000)

    storage_link = page.get_by_role("link", name="Open Storage")
    expect(storage_link).to_be_visible()
    expect(storage_link).to_have_attribute("href", "/storage")


def test_settings_system_info_renders(live_server, page):
    """Settings page loads and displays system info section with real data."""
    url = live_server["url"]
    page.goto(f"{url}/settings?category=advanced", timeout=5000)

    # The system info section should be visible
    system_section = page.locator(".section-title", has_text="System")
    expect(system_section).to_be_visible()

    # /api/system/info populates these fields asynchronously via JS.
    # Allow a generous timeout for the async fetch to complete.
    api_timeout = 30_000

    # Compute device should be populated
    device_name = page.locator("#deviceName")
    expect(device_name).not_to_have_text("-", timeout=api_timeout)


@pytest.mark.parametrize(
    ("tier", "long_paths", "expected"),
    [
        ("supported", True, "Supported"),
        ("supported", False, "Action needed"),
        ("unsupported", True, "Unsupported Windows version"),
    ],
)
def test_settings_windows_support_status(live_server, page, tier, long_paths, expected):
    def system_info(route):
        response = route.fetch()
        info = response.json()
        info["platform_support"] = {
            "platform": "win32",
            "support_tier": tier,
            "windows_release": "11" if tier == "supported" else "10",
            "architecture": "AMD64",
            "long_paths": {"enabled": long_paths},
        }
        route.fulfill(response=response, json=info)

    page.route("**/api/system/info", system_info)
    page.goto(f"{live_server['url']}/settings?category=advanced")
    expect(page.locator("#windowsSupportRow")).to_be_visible()
    expect(page.locator("#windowsSupportStatus")).to_have_text(expected)


def test_settings_cmd_f_opens_page_text_search(live_server, page):
    """Settings captures find shortcut and highlights page text matches."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)

    page.keyboard.press("Control+F")

    find_panel = page.locator("#settingsFindPanel")
    find_input = page.locator("#settingsFindInput")
    expect(find_panel).to_be_visible()
    expect(find_input).to_be_focused()

    find_input.fill("top_k_predictions")
    page.locator("#settingsResults button").first.click()
    expect(page.locator("#setting-top_k_predictions")).to_be_visible()
    page.go_back()
    expect(page.locator("#cfgPhotosPerPage")).to_be_visible()


def test_settings_text_search_ignores_hidden_update_section(live_server, page):
    """Settings find should not count permanently hidden page text."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)

    page.keyboard.press("Control+F")
    page.locator("#settingsFindInput").fill("Check for Updates")

    expect(page.locator("#settingsResults")).to_contain_text("No matches")


def test_desktop_update_banner_links_to_available_release(live_server, page):
    page.goto(live_server["url"] + "/settings")
    page.evaluate("""async () => {
        window.__TAURI_INTERNALS__ = {invoke: async () => ({available: true, version: '1.2.3'})};
        try {
            await startupUpdateCheck();
        } finally {
            delete window.__TAURI_INTERNALS__;
        }
    }""")
    link = page.locator("#updateBanner").get_by_role("link", name="View release")
    expect(link).to_have_attribute("href", "https://github.com/jss367/vireo/releases/latest")


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
    page.goto(live_server["url"] + "/settings?category=storage")
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
    page.goto(live_server["url"] + "/settings?category=storage")
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
    page.goto(live_server["url"] + "/settings?category=storage")
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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
    status = _wait_for_settings_idle(page)

    checkbox = page.locator("#wsOverride_grouping_window_seconds")
    checkbox.check()
    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)
    value = page.locator("#wsVal_grouping_window_seconds")
    expect(value).to_have_value("10")

    assert page.request.post(f"{url}/api/workspaces/active/config", data={"grouping_window_seconds": 20}).ok
    # Hold the resync GET so we can observe that "Saved" waits for it.
    held = []

    def _hold_get(route, request):
        if request.method == "GET":
            held.append(route)
        else:
            route.continue_()

    page.route("**/api/workspaces/active/config", _hold_get)
    value.fill("")
    value.blur()
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
    # The restored server value becomes the baseline for the next edit.
    expect(value).to_have_value("20")
    with page.expect_response(lambda r: r.url.endswith("/api/workspaces/active/config") and r.request.method == "POST"):
        value.fill("10")
        value.blur()
    overrides = page.request.get(f"{url}/api/workspaces/active/config").json()
    assert overrides["grouping_window_seconds"] == 10


def test_settings_saves_for_same_path_are_serialized(live_server, page):
    """A second edit waits for the in-flight POST so snapshots reach the server in order."""
    url = live_server["url"]
    page.goto(f"{url}/settings?category=identification", timeout=5000)
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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
    status = _wait_for_settings_idle(page)
    answer_dialogs(page)

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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
    status = _wait_for_settings_idle(page)
    answer_dialogs(page)

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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
    _wait_for_settings_idle(page)
    answer_dialogs(page)

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
    # workspace save chain. Leaving the field commits it, as it does for a
    # user who goes on to click Import; an edit still focused would commit
    # again when the import's confirmation dialog takes the focus.
    value_input.fill("55")
    value_input.blur()
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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
    status = _wait_for_settings_idle(page)
    answer_dialogs(page)

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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
    status = _wait_for_settings_idle(page)
    answer_dialogs(page)

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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
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
    page.goto(f"{url}/settings?category=identification", timeout=5000)
    _wait_for_settings_idle(page)
    answer_dialogs(page)

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

    # The refresh-failure notice was shown before the page reload.
    said = dialogs(page)
    assert any("could not refresh" in m for m in said), (
        "expected the refresh-failure notice before location.reload(); "
        f"saw {said!r}"
    )

    # After the reload, the form is rebuilt from disk (the import
    # committed), not from any stale in-memory snapshot.
    _wait_for_settings_idle(page)
    expect(page.locator("#cfgKeywordCase")).to_have_value("title")
    cfg = page.request.get(f"{url}/api/config").json()
    assert cfg["keyword_case"] == "title"


def test_all_settings_text_field_saves_on_commit_not_per_keystroke(live_server, page):
    """All settings typed fields save the finished value, and keep focus.

    The row used to autosave 300 ms after each keystroke and then replace
    itself, so a pause mid-path wrote the partial path to config and the
    next keystrokes went nowhere.
    """
    url = live_server["url"]
    page.goto(f"{url}/settings?category=advanced", timeout=5000)
    _wait_for_settings_idle(page)

    patches = []
    page.on(
        "request",
        lambda r: patches.append(r.post_data_json)
        if r.method == "PATCH" and "/api/settings/global" in r.url
        else None,
    )

    field = page.locator(
        '#allSettingsCategories input[data-input-key="darktable_style"]'
    )
    expect(field).to_be_visible(timeout=10_000)
    field.click()
    field.fill("")
    field.press_sequentially("half")
    # Longer than the old 300 ms debounce: nothing may be written yet.
    page.wait_for_timeout(700)
    assert patches == []
    field.press_sequentially("-typed")

    row = page.locator(
        '#allSettingsCategories .setting-row-card[data-key="darktable_style"]'
    )
    with page.expect_request(
        lambda r: r.method == "PATCH" and "/api/settings/global" in r.url
    ):
        field.press("Enter")
    expect(row.locator(".setting-row-status")).to_have_text("✓ Saved", timeout=10_000)
    assert [p["value"] for p in patches] == ["half-typed"]

    # The row was re-rendered with the saved value and focus stayed in it.
    refreshed = page.locator(
        '#allSettingsCategories input[data-input-key="darktable_style"]'
    )
    expect(refreshed).to_have_value("half-typed")
    expect(refreshed).to_be_focused()
    values = page.request.get(f"{url}/api/settings/values").json()
    assert values["global"]["darktable_style"] == "half-typed"


@pytest.mark.parametrize("scope", ["global", "workspace"])
def test_failed_location_keyword_cleanup_is_shown_after_save(
    live_server, page, monkeypatch, scope,
):
    """Turning off location keywords says so when their removal can't queue.

    The save stands, but the keywords Vireo wrote stay in the sidecars, so
    the row must not report a plain "Saved": it keeps the server's warning
    on screen, and a warning toast points at it.
    """
    import sqlite3

    from services import photo_locations

    def _locked(_db):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(
        photo_locations, "queue_location_changes_for_tagged_photos", _locked,
    )
    url = live_server["url"]
    key = "write_location_keywords_to_xmp"
    turn_on = page.request.patch(
        f"{url}/api/settings/{scope}", data={"key": key, "value": True},
    )
    assert turn_on.ok

    page.goto(f"{url}/settings?category=advanced", timeout=5000)
    _wait_for_settings_idle(page)
    if scope == "workspace":
        page.locator('.scope-tab[data-scope="workspace"]').click()

    row = page.locator(f'#allSettingsCategories .setting-row-card[data-key="{key}"]')
    checkbox = row.locator(f'input[data-input-key="{key}"]')
    expect(checkbox).to_be_checked(timeout=10_000)
    with page.expect_response(
        lambda r: r.request.method == "PATCH" and f"/api/settings/{scope}" in r.url
    ) as saved:
        checkbox.uncheck()
    assert saved.value.json()["ok"] is True

    notice = row.locator(".setting-row-warning")
    expect(notice).to_be_visible(timeout=10_000)
    expect(notice).to_contain_text(
        "could not queue removal of the location keywords it wrote to XMP sidecars"
    )
    expect(notice).to_contain_text('"Default"')
    expect(notice).to_contain_text("Queue location writes")
    expect(row.locator(".setting-row-status")).to_have_text("Saved with a warning")
    toast = page.locator('#toastContainer [data-type="warning"]')
    expect(toast).to_contain_text("Those keywords stay in the sidecars")

    values = page.request.get(f"{url}/api/settings/values").json()
    assert values["effective"][key] is False


def test_category_search_navigation_and_workspace_link(live_server, page):
    url = live_server["url"]
    page.goto(url + "/settings")
    _wait_for_settings_idle(page)
    expect(page.locator("#cfgBrowseThumbDefault")).to_be_in_viewport()
    search = page.get_by_role("searchbox", name="Search settings")
    search.fill("Storage & Maintenance")
    search.press("Enter")
    expect(page.get_by_role("heading", name="Storage & Maintenance")).to_be_focused()
    search.fill("thumbnail")
    result = page.locator("#settingsResults button").filter(has_text="Default browse thumbnail")
    expect(result).to_have_count(1)
    result.click()
    expect(page.locator("#cfgBrowseThumbDefault")).to_be_focused()

    search.fill("Top-K predictions")
    page.locator("#settingsResults button").first.click()
    expect(page.locator("#cfgTopK")).to_be_focused()
    row = page.locator(".setting-row").filter(has=page.locator("#cfgTopK"))
    row.get_by_role("link", name=re.compile("Override for this workspace")).click()
    expect(page.locator('.scope-tab[data-scope="workspace"]')).to_have_class(re.compile("active"))
    expect(page.locator("#setting-top_k_predictions")).to_be_in_viewport()
    page.go_back()
    expect(page.locator("#cfgTopK")).to_be_visible()
    page.set_viewport_size({"width": 640, "height": 800})
    page.get_by_label("Category", exact=True).select_option("browsing")
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")


def test_curated_save_preserves_stored_precision(live_server, page):
    url = live_server["url"]
    assert page.request.post(url + "/api/config", data={"detector_confidence": 0.405}).ok
    page.goto(url + "/settings")
    _wait_for_settings_idle(page)
    field = page.locator("#cfgPhotosPerPage")
    with page.expect_request(lambda r: r.url.endswith("/api/config") and r.method == "POST") as sent:
        field.fill("80")
        field.blur()
    expect(page.locator("#settingsSaveStatus")).to_contain_text("Saved")
    assert sent.value.post_data_json == {"photos_per_page": 80}
    assert page.request.get(url + "/api/config").json()["detector_confidence"] == 0.405


def test_global_load_can_recover_while_workspace_remains_editable(live_server, page):
    url = live_server["url"]
    page.route("**/api/config", lambda route: route.fulfill(status=503, json={"error": "offline"}))
    page.goto(url + "/settings?category=identification")
    retry = page.get_by_role("button", name="Reload global settings")
    reset_pipeline = page.locator("#resetPipeline")
    expect(retry).to_be_visible()
    expect(reset_pipeline).to_be_disabled()
    assert page.locator("#cfgThreshold").evaluate("el => !!el.closest('[inert]')")
    page.locator("#wsOverride_classification_threshold").check()
    expect(page.locator("#settingsSaveStatus")).to_contain_text("Saved")
    retry.click()
    expect(retry).to_be_enabled()
    page.unroute("**/api/config")
    held = []
    page.route("**/api/config", lambda route: held.append(route))
    with page.expect_request(lambda request: request.url.endswith("/api/config")):
        retry.click()
    page.evaluate("renderSettingsIssues()")
    expect(retry).to_be_disabled()
    expect(reset_pipeline).to_be_disabled()
    assert len(held) == 1
    held[0].continue_()
    page.unroute("**/api/config")
    expect(retry).to_have_count(0)
    expect(reset_pipeline).to_be_enabled()
    page.select_option("#cfgKeywordCase", "title")
    expect(page.locator("#settingsSaveStatus")).to_contain_text("Saved")
    assert page.request.get(url + "/api/config").json()["keyword_case"] == "title"


def test_retry_only_saves_its_scope(live_server, page):
    url = live_server["url"]
    page.goto(url + "/settings?category=identification")
    _wait_for_settings_idle(page)

    def fail_writes(route):
        if route.request.method == "POST":
            route.fulfill(status=503, json={"error": "temporarily offline"})
        else:
            route.continue_()

    for endpoint in ["**/api/config", "**/api/workspaces/active/config"]:
        page.route(endpoint, fail_writes)
    page.select_option("#cfgKeywordCase", "title")
    page.locator("#wsOverride_classification_threshold").check()
    issues = page.locator("#settingsSaveIssues")
    global_retry = issues.get_by_role("button", name="Retry Global changes")
    ws_retry = issues.get_by_role("button", name=re.compile("Retry (?!Global)"))
    expect(global_retry).to_be_visible()
    expect(ws_retry).to_be_visible()
    page.unroute("**/api/config", fail_writes)
    global_retry.click()
    expect(global_retry).to_have_count(0)
    expect(ws_retry).to_be_enabled()
    assert page.request.get(url + "/api/config").json()["keyword_case"] == "title"
    assert "classification_threshold" not in page.request.get(url + "/api/workspaces/active/config").json()
    page.unroute("**/api/workspaces/active/config", fail_writes)
    ws_retry.click()
    expect(ws_retry).to_have_count(0)
    assert "classification_threshold" in page.request.get(url + "/api/workspaces/active/config").json()


def test_settings_search_covers_schema_and_curated_controls(live_server, page):
    page.goto(live_server["url"] + "/settings")
    page.wait_for_selector("#allSettingsCategories [data-key]", state="attached")
    _wait_for_settings_idle(page)
    problems = page.evaluate("""() => {
        const errors = [];
        for (const key of Object.keys(ALL_SETTINGS_CACHE.schema)) {
            const hits = settingsMatches(key);
            if (hits.length !== 1 || hits[0].target.dataset.key !== key) errors.push(key);
        }
        const claimed = new Set();
        for (const control of document.querySelectorAll('.setting-row input[id^="cfg"], .setting-row select[id^="cfg"]')) {
            const cardFields = control.closest('#cfgCardFieldsContainer');
            if (cardFields) {
                const label = control.closest('label').querySelector('span').textContent.trim();
                const hits = settingsMatches(label.toLowerCase()).filter(hit => hit.target === cardFields);
                if (hits.length !== 1 || cardFields.dataset.settingKeys !== 'browse_card_fields') errors.push(label);
                continue;
            }
            const rowLabel = control.closest('.setting-row').querySelector('.setting-label');
            const label = control.title || (rowLabel.querySelector('label') || rowLabel.firstChild).textContent.trim();
            const hits = settingsMatches(label.toLowerCase()).filter(hit => hit.target === control);
            if (hits.length !== 1 || !control.dataset.settingKeys) errors.push(control.id);
            for (const key of (control.dataset.settingKeys || '').split(' ')) {
                if (claimed.has(key)) errors.push('Duplicate: ' + key);
                claimed.add(key);
            }
        }
        return errors;
    }""")
    assert problems == [], problems


def test_curated_credential_updates_advanced_value_and_reset(live_server, page):
    page.goto(live_server["url"] + "/settings?category=integrations")
    _wait_for_settings_idle(page)
    credential = page.locator("#cfgHfToken")
    advanced = page.locator("#setting-hf_token")
    for value in ("hf_test_settings_sync", ""):
        with page.expect_response(lambda r: r.url.endswith("/api/config") and r.request.method == "POST"):
            credential.fill(value)
            credential.blur()
        page.locator('[data-category-link="advanced"]').click()
        expect(advanced.locator('input[type="password"]')).to_have_value(value)
        expect(advanced.get_by_role("button", name="Reset", include_hidden=True)).to_have_count(int(bool(value)))
        page.locator('[data-category-link="integrations"]').click()


def test_reverting_after_lost_save_response_restores_server_value(live_server, page):
    url = live_server["url"]
    page.goto(url + "/settings")
    _wait_for_settings_idle(page)
    field = page.locator("#cfgPhotosPerPage")
    original = field.input_value()

    def lose_response(route):
        assert route.fetch().ok
        route.abort()

    page.route("**/api/config", lose_response)
    field.fill("80" if original != "80" else "90")
    field.blur()
    expect(page.locator("#settingsSaveIssues")).to_contain_text("could not be saved")
    page.unroute("**/api/config", lose_response)
    with page.expect_response(lambda r: r.url.endswith("/api/config") and r.request.method == "POST"):
        field.fill(original)
        field.blur()
    expect(page.locator("#settingsSaveIssues")).to_be_empty()
    assert page.request.get(url + "/api/config").json()["photos_per_page"] == int(original)
