import json
import re

from playwright.sync_api import expect


def test_import_source_browse_button_adds_source_folder(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => ['/tmp/card-a', '/tmp/card-b']")

    browse_btn = page.locator("[data-testid='import-source-browse-btn']")
    expect(browse_btn).to_be_visible()
    browse_btn.click()

    source_list = page.locator("#sourceList")
    expect(source_list).to_contain_text("/tmp/card-a")
    expect(source_list).to_contain_text("/tmp/card-b")


def test_import_source_browse_button_shows_quick_photo_count(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                total_count: 42,
                total_size: 0,
                type_breakdown: {'.jpg': 42},
                duplicate_count: 0,
                files: [],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
          window.pickDirectory = async () => ['/tmp/card-a'];
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()

    source_list = page.locator("#sourceList")
    expect(source_list).to_contain_text("/tmp/card-a")
    expect(source_list.locator(".source-meta")).to_have_text("42 photos")


def test_import_destination_browse_button_sets_destination(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => '/tmp/archive'")
    page.locator("#modeCopy").check()

    browse_btn = page.locator("[data-testid='import-destination-browse-btn']")
    expect(browse_btn).to_be_visible()
    browse_btn.click()

    expect(page.locator("#destInput")).to_have_value("/tmp/archive")


def test_import_custom_extensions_feed_preview(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__previewBody = null;
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              window.__previewBody = JSON.parse(init.body);
              return Promise.resolve(new Response(JSON.stringify({
                total_count: 0,
                total_size: 0,
                type_breakdown: {},
                duplicate_count: 0,
                files: [],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#fileTypePreset").select_option("custom")
    page.evaluate(
        """
        () => {
          document.querySelectorAll('.file-ext').forEach(el => { el.checked = false; });
          document.querySelector('.file-ext[value=".jpg"]').checked = true;
          document.querySelector('.file-ext[value=".nef"]').checked = true;
        }
        """
    )
    page.locator("#btnPreview").click()
    page.wait_for_function("window.__previewBody !== null")

    body = page.evaluate("window.__previewBody")
    assert body["folders"] == ["/tmp/card-a"]
    assert body["file_types"] == [".jpg", ".nef"]


def test_import_preview_passes_verify_by_hash_to_duplicate_check(live_server, page):
    """The preview and the actual import must use the same duplicate mode so
    the counts don't disagree for renamed / metadata-colliding files."""
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__dupBody = null;
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                total_count: 1,
                total_size: 0,
                type_breakdown: {'.jpg': 1},
                duplicate_count: 0,
                files: [{path: '/tmp/card-a/IMG_0001.jpg'}],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            if (target && target.indexOf('/api/import/check-duplicates') === 0) {
              window.__dupBody = JSON.parse(init.body);
              const frame = 'data: ' + JSON.stringify({
                done: true, duplicate_count: 0, checked: 1, total: 1,
              }) + '\\n\\n';
              return Promise.resolve(new Response(frame, {
                status: 200,
                headers: {'Content-Type': 'text/event-stream'},
              }));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#chkSkipDuplicates").check()
    page.locator("#chkVerifyByHash").check()
    page.locator("#btnPreview").click()
    page.wait_for_function("window.__dupBody !== null")

    body = page.evaluate("window.__dupBody")
    assert body["paths"] == ["/tmp/card-a/IMG_0001.jpg"]
    assert body["verify_by_hash"] is True


def test_import_preview_shows_destination_folder_structure(live_server, page):
    """Copy-mode preview surfaces the destination folder structure (new vs
    existing folders) and a managed-archive callout, wired to
    /api/import/destination-preview. Skipped duplicates are excluded so the
    folder counts match the files that will actually land."""
    url = live_server["url"]
    captured = {}

    def folder_preview(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "files": [
                    {"path": "/tmp/card-a/IMG_0001.jpg"},
                    {"path": "/tmp/card-a/IMG_0002.jpg"},
                ],
            }),
        )

    def check_duplicates(route):
        frame = (
            "data: " + json.dumps({
                "duplicates": ["/tmp/card-a/IMG_0002.jpg"],
                "checked": 2, "total": 2,
            }) + "\n\n"
            + "data: " + json.dumps({
                "done": True, "duplicate_count": 1, "checked": 2, "total": 2,
            }) + "\n\n"
        )
        route.fulfill(
            status=200, content_type="text/event-stream", body=frame,
        )

    def destination_preview(route):
        captured["body"] = json.loads(route.request.post_data or "{}")
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "folders": [
                    {"path": "2026/2026-07-01",
                     "full_path": "/archive/2026/2026-07-01",
                     "count": 1, "exists": False},
                    {"path": "2026/2026-07-02",
                     "full_path": "/archive/2026/2026-07-02",
                     "count": 1, "exists": True},
                ],
                "total_photos": 2,
                "total_folders": 2,
                "new_folders": 1,
                "existing_folders": 1,
                "managed_archive": {"path": "/archive", "photo_count": 1284},
            }),
        )

    page.route("**/api/import/folder-preview", folder_preview)
    page.route("**/api/import/check-duplicates", check_duplicates)
    page.route("**/api/import/destination-preview", destination_preview)
    page.goto(f"{url}/import")

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/archive")
    page.locator("#btnPreview").click()

    structure = page.locator("#destStructure")
    expect(structure).to_be_visible()
    expect(structure).to_contain_text(
        "2 photos → 2 folders (1 new, 1 existing)"
    )
    expect(structure).to_contain_text("Merging into a managed archive at")
    expect(structure).to_contain_text("/archive")
    expect(structure).to_contain_text("1284 photos already cataloged")
    expect(structure).to_contain_text("2026/2026-07-01")
    expect(structure).to_contain_text("new")
    expect(structure).to_contain_text("existing")

    # The structure block is only made visible after destination-preview
    # resolves, so captured["body"] is populated by the time we get here.
    # The skipped duplicate is excluded from the structure preview so the
    # new/existing folder counts reflect the copy set, not every file found.
    assert captured["body"]["exclude_paths"] == ["/tmp/card-a/IMG_0002.jpg"]
    assert captured["body"]["destination"] == "/archive"
    assert captured["body"]["file_types"] == "both"


def test_import_destination_structure_ignores_stale_response(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__resolveDestStructure = null;
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                files: [{path: '/tmp/card-a/IMG_0001.jpg'}],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            if (target && target.indexOf('/api/import/check-duplicates') === 0) {
              const frame = 'data: ' + JSON.stringify({
                done: true, duplicate_count: 0, checked: 1, total: 1,
              }) + '\\n\\n';
              return Promise.resolve(new Response(frame, {
                status: 200,
                headers: {'Content-Type': 'text/event-stream'},
              }));
            }
            if (target && target.indexOf('/api/import/destination-preview') === 0) {
              return new Promise((resolve) => {
                window.__resolveDestStructure = () => resolve(new Response(JSON.stringify({
                  folders: [{
                    path: '2026/07/11',
                    full_path: '/archive/2026/07/11',
                    count: 1,
                    exists: false,
                  }],
                  total_photos: 1,
                  total_folders: 1,
                  new_folders: 1,
                  existing_folders: 0,
                  managed_archive: null,
                }), {status: 200, headers: {'Content-Type': 'application/json'}}));
              });
            }
            return originalFetch(input, init);
          };
          addSourcePath('/tmp/card-a');
        }
        """
    )

    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/archive")
    page.locator("#btnPreview").click()
    page.wait_for_function("window.__resolveDestStructure !== null")
    page.locator("#destInput").fill("/new-archive")
    page.evaluate("() => window.__resolveDestStructure()")

    expect(page.locator("#destStructure")).to_be_hidden()


def test_import_destination_structure_hides_on_duplicate_control_toggle(
    live_server, page
):
    url = live_server["url"]

    def folder_preview(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"files": [{"path": "/tmp/card-a/IMG_0001.jpg"}]}),
        )

    def check_duplicates(route):
        frame = (
            "data: " + json.dumps({
                "done": True, "duplicate_count": 0, "checked": 1, "total": 1,
            }) + "\n\n"
        )
        route.fulfill(status=200, content_type="text/event-stream", body=frame)

    def destination_preview(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "folders": [{
                    "path": "2026/07/11",
                    "full_path": "/archive/2026/07/11",
                    "count": 1,
                    "exists": False,
                }],
                "total_photos": 1,
                "total_folders": 1,
                "new_folders": 1,
                "existing_folders": 0,
                "managed_archive": None,
            }),
        )

    page.route("**/api/import/folder-preview", folder_preview)
    page.route("**/api/import/check-duplicates", check_duplicates)
    page.route("**/api/import/destination-preview", destination_preview)
    page.goto(f"{url}/import")

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/archive")
    page.locator("#btnPreview").click()
    expect(page.locator("#destStructure")).to_be_visible()

    page.locator("#chkSkipDuplicates").uncheck()
    expect(page.locator("#destStructure")).to_be_hidden()

    page.locator("#btnPreview").click()
    expect(page.locator("#destStructure")).to_be_visible()

    page.locator("#chkVerifyByHash").check()
    expect(page.locator("#destStructure")).to_be_hidden()

    page.locator("#btnPreview").click()
    expect(page.locator("#destStructure")).to_be_visible()

    page.evaluate("() => addSourcePath('/tmp/card-b')")
    expect(page.locator("#destStructure")).to_be_hidden()

    page.locator("#btnPreview").click()
    expect(page.locator("#destStructure")).to_be_visible()

    page.locator("#sourceList .source-item button").first.click()
    expect(page.locator("#destStructure")).to_be_hidden()


def test_import_duplicate_stream_result_ignored_after_controls_change(
    live_server, page
):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__resolveDuplicates = null;
          window.__destinationPreviewCalled = false;
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                files: [
                  {path: '/tmp/card-a/IMG_0001.jpg'},
                  {path: '/tmp/card-a/IMG_0002.jpg'},
                ],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            if (target && target.indexOf('/api/import/check-duplicates') === 0) {
              return new Promise((resolve) => {
                window.__resolveDuplicates = () => {
                  const frame = 'data: ' + JSON.stringify({
                    duplicates: ['/tmp/card-a/IMG_0002.jpg'],
                    checked: 2,
                    total: 2,
                  }) + '\\n\\n' + 'data: ' + JSON.stringify({
                    done: true,
                    duplicate_count: 1,
                    checked: 2,
                    total: 2,
                  }) + '\\n\\n';
                  resolve(new Response(frame, {
                    status: 200,
                    headers: {'Content-Type': 'text/event-stream'},
                  }));
                };
              });
            }
            if (target && target.indexOf('/api/import/destination-preview') === 0) {
              window.__destinationPreviewCalled = true;
              return Promise.resolve(new Response(JSON.stringify({folders: []}), {
                status: 200,
                headers: {'Content-Type': 'application/json'},
              }));
            }
            return originalFetch(input, init);
          };
          addSourcePath('/tmp/card-a');
        }
        """
    )

    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/archive")
    page.locator("#btnPreview").click()
    page.wait_for_function("window.__resolveDuplicates !== null")
    page.locator("#fileTypePreset").select_option("custom")
    page.locator("#chkVerifyByHash").check()
    page.evaluate("() => window.__resolveDuplicates()")
    page.wait_for_timeout(100)

    assert page.evaluate("window.__destinationPreviewCalled") is False
    expect(page.locator("#destStructure")).to_be_hidden()


def test_import_copy_start_sends_restored_options(live_server, page):
    url = live_server["url"]
    captured = {}

    def remote_targets(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "rsync_available": True,
                "targets": [{
                    "id": "nas1",
                    "name": "Photo NAS",
                    "user": "photo",
                    "host": "nas.local",
                    "remote_path": "/srv/photos",
                    "mount_path": "/Volumes/photos",
                }],
            }),
        )

    def start_import(route):
        captured["body"] = json.loads(route.request.post_data or "{}")
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "job_id": "import-test",
                "workspace": {"id": 22, "name": "Kenya 2026"},
            }),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.route("**/api/jobs/import-photos", start_import)
    page.goto(f"{url}/import")

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#workspaceNew").check()
    page.locator("#newWorkspaceName").fill("Kenya 2026")
    page.locator("#destMode").select_option("remote:nas1")
    page.locator("#remoteSubpath").fill("2026/kenya")
    page.locator("#fileTypePreset").select_option("custom")
    page.evaluate(
        """
        () => {
          document.querySelectorAll('.file-ext').forEach(el => { el.checked = false; });
          document.querySelector('.file-ext[value=".jpg"]').checked = true;
          document.querySelector('.file-ext[value=".nef"]').checked = true;
        }
        """
    )
    page.locator("#chkSkipDuplicates").uncheck()
    page.locator("#chkVerifyByHash").check()

    page.locator("#btnStart").click()
    expect(page.locator("#progressCard")).to_be_visible()

    body = captured["body"]
    assert body["sources"] == ["/tmp/card-a"]
    assert body["new_workspace_name"] == "Kenya 2026"
    assert body["remote_target_id"] == "nas1"
    assert body["remote_subpath"] == "2026/kenya"
    assert "destination" not in body
    assert body["file_types"] == [".jpg", ".nef"]
    assert body["skip_duplicates"] is False
    assert body["verify_by_hash"] is True
    # The After Import dropdown was untouched, and this is a new-workspace
    # import — the client must omit after_import so the server resolves the
    # default against the newly-created workspace instead of leaking the
    # previously-active workspace's pipeline.default_strategy.
    assert "after_import" not in body


def test_import_new_workspace_forwards_explicit_after_import(live_server, page):
    """When the user actively picks a strategy for a new-workspace import,
    the client must forward that pick — only the untouched-dropdown case is
    omitted so the server can apply the new workspace's default."""
    url = live_server["url"]
    captured = {}

    def start_import(route):
        captured["body"] = json.loads(route.request.post_data or "{}")
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "job_id": "import-test",
                "workspace": {"id": 23, "name": "Serengeti"},
            }),
        )

    page.route("**/api/jobs/import-photos", start_import)
    page.goto(f"{url}/import")

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#workspaceNew").check()
    page.locator("#newWorkspaceName").fill("Serengeti")
    page.locator("#destInput").fill("/tmp/archive")
    page.locator("#afterImportSelect").select_option("identify")

    page.locator("#btnStart").click()
    expect(page.locator("#progressCard")).to_be_visible()

    body = captured["body"]
    assert body["new_workspace_name"] == "Serengeti"
    assert body["after_import"] == "identify"


def test_import_new_workspace_shows_target_default_in_after_import_display(
    live_server, page
):
    """When 'New workspace' is picked and the After Import dropdown is
    untouched, the visible label must describe what the server will
    actually do — the global default that the freshly-created workspace
    inherits — rather than leaking the currently-active workspace's
    (possibly-overridden) prefilled selection."""
    url = live_server["url"]

    def config_route(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "pipeline": {"default_strategy": "identify"},
            }),
        )

    def active_workspace_route(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "id": 1,
                "name": "Existing",
                "config_overrides": {
                    "pipeline": {"default_strategy": "quick_look"},
                },
            }),
        )

    page.route("**/api/config", config_route)
    page.route("**/api/workspaces/active", active_workspace_route)
    page.goto(f"{url}/import")

    # Before touching workspaceNew, the dropdown reflects the CURRENT
    # workspace's default (quick_look) — the source of the misleading
    # signal that this fix addresses.
    expect(page.locator("#afterImportSelect")).to_have_value("quick_look")

    page.locator("#workspaceNew").check()

    # After switching to new-workspace mode, the visible selection swaps
    # to a placeholder that names the GLOBAL default (identify) — matching
    # what the server will actually apply to the freshly-created workspace.
    expect(page.locator("#afterImportSelect")).to_have_value("__hidden_default__")
    placeholder = page.locator("#afterImportHiddenDefault")
    expect(placeholder).to_contain_text("New workspace default")
    expect(placeholder).to_contain_text("Identify birds")

    # Switching back to current-workspace mode without touching the
    # dropdown restores the current workspace's default rather than
    # sticking on the placeholder.
    page.locator("#workspaceCurrent").check()
    expect(page.locator("#afterImportSelect")).to_have_value("quick_look")


def test_import_browse_button_opens_folder_browser_fallback(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")

    page.locator("[data-testid='import-source-browse-btn']").click()

    browser = page.locator("[data-testid='import-folder-browser']")
    expect(browser).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#folderBrowserTitle")).to_have_text("Select Source Folders")
    expect(page.locator(".folder-browser-panel")).to_have_attribute("role", "dialog")
    expect(page.locator(".folder-browser-panel")).to_have_attribute("aria-modal", "true")
    expect(page.locator(".folder-browser-panel")).to_have_attribute(
        "aria-labelledby", "folderBrowserTitle")


def test_import_folder_browser_selects_multiple_source_folders(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/browse') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                path: '/tmp',
                dirs: [
                  {name: 'card-a', path: '/tmp/card-a'},
                  {name: 'card-b', path: '/tmp/card-b'},
                  {name: 'card-c', path: '/tmp/card-c'},
                ],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()
    items = page.locator("#folderBrowserList .folder-browser-item[data-folder-path]")
    expect(items).to_have_count(3)

    items.nth(0).click()
    items.nth(2).click(modifiers=["Shift"])

    expect(page.locator("#folderBrowserSelectBtn")).to_have_text("Add 3 Folders")
    page.locator("#folderBrowserSelectBtn").click()

    source_list = page.locator("#sourceList")
    expect(source_list).to_contain_text("/tmp/card-a")
    expect(source_list).to_contain_text("/tmp/card-b")
    expect(source_list).to_contain_text("/tmp/card-c")


def test_import_folder_browser_toggles_discontiguous_source_folders(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/browse') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                path: '/tmp',
                dirs: [
                  {name: 'card-a', path: '/tmp/card-a'},
                  {name: 'card-b', path: '/tmp/card-b'},
                  {name: 'card-c', path: '/tmp/card-c'},
                ],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()
    items = page.locator("#folderBrowserList .folder-browser-item[data-folder-path]")
    expect(items).to_have_count(3)

    items.nth(0).click()
    items.nth(2).evaluate(
        """el => el.dispatchEvent(new MouseEvent('click', {
          bubbles: true,
          ctrlKey: true,
        }))"""
    )

    expect(page.locator("#folderBrowserSelectBtn")).to_have_text("Add 2 Folders")
    page.locator("#folderBrowserSelectBtn").click()

    source_list = page.locator("#sourceList")
    expect(source_list).to_contain_text("/tmp/card-a")
    expect(source_list).not_to_contain_text("/tmp/card-b")
    expect(source_list).to_contain_text("/tmp/card-c")


def test_import_folder_browser_selects_volumes_from_synthetic_root(live_server, page):
    # The Volumes shortcut renders /api/volumes as a synthetic root with
    # browserPath = ''. Volume rows are selectable, so the Add button must
    # enable once at least one is picked and must submit the selected drives
    # instead of the (empty) synthetic root.
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")
    page.evaluate(
        """
        () => {
          Object.defineProperty(navigator, 'userAgent', {
            value: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
            configurable: true,
          });
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/volumes') >= 0) {
              return Promise.resolve(new Response(JSON.stringify([
                {name: 'Volume A', path: '/Volumes/A'},
                {name: 'Volume B', path: '/Volumes/B'},
              ]), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()
    # Non-Mac branch fetches /api/volumes (stubbed) and renders the drives
    # as selectable rows in a synthetic root with browserPath = ''.
    page.evaluate("async () => { await browseImportFolderTo('__volumes__'); }")

    items = page.locator("#folderBrowserList .folder-browser-item[data-folder-path]")
    expect(items).to_have_count(2)

    select_btn = page.locator("#folderBrowserSelectBtn")
    expect(select_btn).to_be_disabled()

    items.nth(0).evaluate(
        """el => el.dispatchEvent(new MouseEvent('click', {
          bubbles: true,
          ctrlKey: true,
        }))"""
    )
    items.nth(1).evaluate(
        """el => el.dispatchEvent(new MouseEvent('click', {
          bubbles: true,
          ctrlKey: true,
        }))"""
    )

    expect(select_btn).to_be_enabled()
    expect(select_btn).to_have_text("Add 2 Folders")
    select_btn.click()

    source_list = page.locator("#sourceList")
    expect(source_list).to_contain_text("/Volumes/A")
    expect(source_list).to_contain_text("/Volumes/B")


def test_import_folder_browser_disables_select_while_pending(live_server, page):
    # A stale browserPath from a prior fetch used to remain selectable during
    # the next navigation. If the fetch stalled or failed, clicking "Select
    # This Folder" would submit the previous folder. Guard: the button must
    # be disabled while /api/browse is in flight and only re-enable once a
    # real path resolves.
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__releaseBrowse = null;
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/browse') === 0) {
              return new Promise((resolve) => {
                window.__releaseBrowse = () => resolve(new Response(
                  JSON.stringify({path: '/tmp/target', dirs: []}),
                  {status: 200, headers: {'Content-Type': 'application/json'}}
                ));
              });
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()

    select_btn = page.locator("#folderBrowserSelectBtn")
    expect(select_btn).to_be_disabled()

    page.evaluate("() => window.__releaseBrowse && window.__releaseBrowse()")
    expect(select_btn).to_be_enabled()


def test_use_staging_as_import_source_forces_copy_mode(live_server, page):
    # Orphaned-staging recovery must copy the staging tree into the archive.
    # In-place would catalog paths that vanish when staging is cleaned up,
    # so useStagingAsImportSource() has to flip the mode to Copy regardless
    # of the page default.
    url = live_server["url"]
    page.goto(f"{url}/import")

    # Default is in_place — sanity check before invoking the recovery helper.
    expect(page.locator("#modeInPlace")).to_be_checked()

    page.evaluate(
        """
        () => useStagingAsImportSource({
          source_root: '/tmp/staging-src',
          inferred_destination: '/tmp/archive-dest',
        })
        """
    )

    expect(page.locator("#modeCopy")).to_be_checked()
    expect(page.locator("#modeInPlace")).not_to_be_checked()
    expect(page.locator("#destInput")).to_have_value("/tmp/archive-dest")
    expect(page.locator("#sourceList")).to_contain_text("/tmp/staging-src")
    # updateImportMode() must have run so the destination card is visible again.
    expect(page.locator("#destCard")).to_be_visible()


def test_import_menu_deep_link_opens_copy_mode_with_source_picker(live_server, page):
    # File > Import Folder... in the native menu routes to
    # /import?mode=copy&pick=source so the two File-menu import commands stay
    # distinct actions. In browser mode pickDirectory() returns null, so the
    # in-page folder browser must open instead of the native dialog.
    url = live_server["url"]
    page.goto(f"{url}/import?mode=copy&pick=source")

    expect(page.locator("#modeCopy")).to_be_checked()
    expect(page.locator("#destCard")).to_be_visible()
    expect(page.locator("[data-testid='import-folder-browser']")).to_have_class(
        re.compile(r"\bopen\b"))
    expect(page.locator("#folderBrowserTitle")).to_have_text("Select Source Folders")
    # pick is a one-shot trigger: it must be stripped from the URL so a manual
    # reload doesn't reopen the picker, while the mode param survives.
    page.wait_for_url(f"{url}/import?mode=copy")


def test_import_folder_browser_escape_closes_modal(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")

    page.locator("[data-testid='import-source-browse-btn']").click()
    expect(page.locator("[data-testid='import-folder-browser']")).to_have_class(
        re.compile(r"\bopen\b"))
    expect(page.locator(".folder-browser-close")).to_be_focused()

    page.keyboard.press("Escape")

    expect(page.locator("[data-testid='import-folder-browser']")).not_to_have_class(
        re.compile(r"\bopen\b"))
