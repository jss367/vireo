"""Behavioral coverage for browser and Tauri external-navigation handoffs."""

from playwright.sync_api import expect

from e2e.browser_doubles import fulfill_sse

INAT_UPLOAD_URL = "https://www.inaturalist.org/observations/upload"
INAT_URL = (
    INAT_UPLOAD_URL
    + "?taxon_name=Corvus%20corax&observed_on=2026-07-12&lat=47.61&lng=-122.33"
)


def _item(photo_id=1, *, url=INAT_UPLOAD_URL, duplicate=False):
    return {
        "photo_id": photo_id,
        "filename": f"photo-{photo_id}.jpg",
        "upload_url": url,
        "taxon_name": "Corvus corax",
        "observed_on": "2026-07-12",
        "latitude": 47.61,
        "longitude": -122.33,
        "already_submitted": duplicate,
        "existing_url": (
            "https://www.inaturalist.org/observations/123" if duplicate else None
        ),
    }


def _mock_tauri(page, *, fail_open=False, fail_copy=False):
    page.evaluate(
        """
        options => {
          window.__externalTest = { invokes: [], windowOpenCalls: [] };
          window.__TAURI_INTERNALS__ = {
            invoke: (command, args) => {
              window.__externalTest.invokes.push({ command, args });
              if (options.failOpen && command === 'open_external_url') {
                return Promise.reject(new Error('browser launch failed'));
              }
              if (options.failCopy && command === 'plugin:clipboard-manager|write_text') {
                return Promise.reject(new Error('clipboard write failed'));
              }
              return Promise.resolve(null);
            }
          };
          window.open = (...args) => {
            window.__externalTest.windowOpenCalls.push(args);
            return null;
          };
        }
        """,
        {"failOpen": fail_open, "failCopy": fail_copy},
    )


def _open_commands(page):
    return page.evaluate(
        "window.__externalTest.invokes.filter(call => "
        "call.command === 'open_external_url')"
    )


def _defer_token_validation(page):
    """Hold the token check until _resolve_token_validation answers it."""
    held = []
    page.route("**/api/inat/token", lambda route: held.append(route))
    return held


def _resolve_token_validation(page, held):
    page.wait_for_timeout(0)
    assert held, "expected the token check to be pending"
    held.pop(0).fulfill(json={"ok": True, "login": "birder42"})
    page.wait_for_timeout(50)


def test_native_quick_open_reviews_checked_metadata_before_opening(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    original_url = page.url
    _mock_tauri(page)

    page.evaluate("item => openInatQuickModal([item], [])", _item())

    assert _open_commands(page) == []
    expect(page.locator("#inatModal")).to_have_class("modal-overlay open")
    expect(page.locator("#inatIncludeTaxon0")).to_be_checked()
    expect(page.locator("#inatIncludeDate0")).to_be_checked()
    expect(page.locator("#inatIncludeLocation0")).to_be_checked()

    page.locator("#inatCards a", has_text="Open Upload Page").click()
    page.wait_for_function(
        "window.__externalTest.invokes.some(call => "
        "call.command === 'open_external_url')"
    )

    commands = _open_commands(page)
    assert commands == [{"command": "open_external_url", "args": {"url": INAT_URL}}]
    assert page.evaluate("window.__externalTest.windowOpenCalls") == []
    assert page.url == original_url
    expect(page.locator("#toastContainer")).to_contain_text(
        "Opened iNaturalist in your browser."
    )
    expect(page.locator("#inatModal")).to_have_class("modal-overlay open")


def test_missing_token_disables_direct_send_and_offers_inline_setup_and_export(
    live_server, page,
):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)

    page.evaluate("item => openInatQuickModal([item], [])", _item())

    expect(page.locator("#inatTokenSetup")).to_contain_text(
        "Direct submission needs an iNaturalist token"
    )
    expect(page.locator("#inatQuickToken")).to_be_visible()
    expect(page.locator("#inatQuickTokenBtn")).to_have_text("Validate & Save")
    expect(page.locator("#inatSubmitBtn")).to_be_disabled()
    expect(page.locator("#inatQuickExportBtn")).to_be_enabled()


def test_valid_inline_token_switches_modal_to_direct_submission(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)
    page.route(
        "**/api/inat/token",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body='{"ok":true,"login":"birder42"}',
        ),
    )
    page.evaluate("item => openInatQuickModal([item], [])", _item())

    page.locator("#inatIncludeTaxon0").uncheck()
    page.locator("#inatIncludeDate0").uncheck()
    page.locator("#inatIncludeLocation0").uncheck()
    page.locator("#inatIncludeDescription0").check()
    page.locator("#inatQuickDescription0").fill("Seen beside the trail")
    page.locator("#inatQuickToken").fill("valid-token")
    page.locator("#inatQuickTokenBtn").click()

    expect(page.locator("#inatTaxon0")).to_have_value("")
    expect(page.locator("#inatDate0")).to_have_value("")
    expect(page.locator("#inatLat0")).to_have_value("")
    expect(page.locator("#inatLng0")).to_have_value("")
    expect(page.locator("#inatDesc0")).to_have_value("Seen beside the trail")
    expect(page.locator("#inatSubmitBtn")).to_be_enabled()
    expect(page.locator("#inatSubmitBtn")).to_have_text("Send to iNaturalist")


def test_stale_token_validation_does_not_reopen_closed_modal(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)
    held_token_checks = _defer_token_validation(page)
    page.evaluate("item => openInatQuickModal([item], [])", _item())

    page.locator("#inatQuickToken").fill("valid-token")
    page.locator("#inatQuickTokenBtn").click()
    expect(page.locator("#inatQuickTokenBtn")).to_have_text("Validating…")
    page.evaluate("closeInatModal()")
    _resolve_token_validation(page, held_token_checks)

    expect(page.locator("#inatModal")).to_have_class("modal-overlay")
    assert page.evaluate("inatQueue.length") == 0


def test_stale_token_validation_does_not_replace_new_quick_modal(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)
    held_token_checks = _defer_token_validation(page)
    page.evaluate("item => openInatQuickModal([item], [])", _item())

    page.locator("#inatQuickToken").fill("valid-token")
    page.locator("#inatQuickTokenBtn").click()
    expect(page.locator("#inatQuickTokenBtn")).to_have_text("Validating…")
    page.evaluate("item => openInatQuickModal([item], [])", _item(2))
    _resolve_token_validation(page, held_token_checks)

    expect(page.locator("#inatQuickToken")).to_be_visible()
    expect(page.locator("#inatCards")).to_contain_text("photo-2.jpg")
    expect(page.locator("#inatSubmitBtn")).to_be_disabled()
    assert page.evaluate("inatQueue.map(item => item.photo_id)") == [2]


def test_token_free_export_uses_checked_metadata(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)
    page.evaluate(
        """
        () => {
          window.__TAURI_INTERNALS__.invoke = (command, args) => {
            window.__externalTest.invokes.push({ command, args });
            if (command === 'plugin:dialog|open') {
              return Promise.resolve('/tmp/inaturalist-exports');
            }
            return Promise.resolve(null);
          };
        }
        """
    )
    captured = {}

    def handle_export(route):
        captured.update(route.request.post_data_json)
        route.fulfill(
            status=200,
            content_type="application/json",
            body='{"job_id":"inat-export-1"}',
        )

    page.route("**/api/inat/export", handle_export)
    streams = []

    def complete_export(route):
        streams.append(route.request.url)
        fulfill_sse(route, "complete", {
            "status": "completed",
            "result": {
                "exported": [{"photo_id": 1, "filename": "photo-1-iNaturalist.jpg"}],
                "errors": [],
                "revealed": True,
            },
        })

    page.route("**/api/jobs/inat-export-1/stream", complete_export)
    page.evaluate("item => openInatQuickModal([item], [])", _item())
    page.locator("#inatIncludeLocation0").uncheck()
    page.locator("#inatIncludeDescription0").check()
    page.locator("#inatQuickDescription0").fill("Seen beside the trail")

    page.locator("#inatQuickExportBtn").click()
    expect(page.locator("#inatQuickExportStatus")).to_contain_text("Exported 1 JPEG")

    assert captured["destination"] == "/tmp/inaturalist-exports"
    assert captured["reveal"] is True
    assert [url.split("/api/", 1)[1] for url in streams] == ["jobs/inat-export-1/stream"]
    assert captured["submissions"] == [{
        "photo_id": 1,
        "taxon_name": "Corvus corax",
        "observed_on": "2026-07-12",
        "latitude": 47.61,
        "longitude": -122.33,
        "include_taxon": True,
        "include_date": True,
        "include_location": False,
        "include_description": True,
        "description": "Seen beside the trail",
    }]


def test_folder_picker_result_does_not_export_replacement_modal(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)
    page.evaluate(
        """
        () => {
          window.__TAURI_INTERNALS__.invoke = (command, args) => {
            window.__externalTest.invokes.push({ command, args });
            if (command === 'plugin:dialog|open') {
              return new Promise(resolve => {
                window.__externalTest.resolveExportDirectory = resolve;
              });
            }
            return Promise.resolve(null);
          };
        }
        """
    )
    captured = []

    def handle_export(route):
        captured.append(route.request.post_data_json)
        route.fulfill(
            status=200,
            content_type="application/json",
            body='{"job_id":"unexpected-export"}',
        )

    page.route("**/api/inat/export", handle_export)
    page.evaluate("item => openInatQuickModal([item], [])", _item())
    page.locator("#inatQuickExportBtn").click()
    page.wait_for_function(
        "typeof window.__externalTest.resolveExportDirectory === 'function'"
    )

    page.evaluate("item => openInatQuickModal([item], [])", _item(2))
    page.evaluate(
        """
        async () => {
          window.__externalTest.resolveExportDirectory('/tmp/old-export');
          await new Promise(resolve => setTimeout(resolve, 0));
        }
        """
    )

    assert captured == []
    expect(page.locator("#inatCards")).to_contain_text("photo-2.jpg")
    expect(page.locator("#inatQuickExportBtn")).to_be_enabled()


def test_export_button_guards_pending_folder_picker(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)
    page.evaluate(
        """
        () => {
          window.__TAURI_INTERNALS__.invoke = (command, args) => {
            window.__externalTest.invokes.push({ command, args });
            if (command === 'plugin:dialog|open') {
              return new Promise(resolve => {
                window.__externalTest.resolveExportDirectory = resolve;
              });
            }
            return Promise.resolve(null);
          };
        }
        """
    )
    page.evaluate("item => openInatQuickModal([item], [])", _item())

    page.evaluate("() => { exportInatQuickPhotos(); exportInatQuickPhotos(); }")
    page.wait_for_function(
        "typeof window.__externalTest.resolveExportDirectory === 'function'"
    )

    assert len([
        call for call in page.evaluate("window.__externalTest.invokes")
        if call["command"] == "plugin:dialog|open"
    ]) == 1
    expect(page.locator("#inatQuickExportBtn")).to_be_disabled()
    expect(page.locator("#inatQuickExportBtn")).to_have_text("Choosing folder…")

    page.evaluate("window.__externalTest.resolveExportDirectory(null)")
    expect(page.locator("#inatQuickExportBtn")).to_be_enabled()
    expect(page.locator("#inatQuickExportBtn")).to_have_text("Export JPEG…")


def test_closing_quick_modal_closes_export_stream(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)
    page.evaluate(
        """
        () => {
          window.__TAURI_INTERNALS__.invoke = (command, args) => {
            window.__externalTest.invokes.push({ command, args });
            if (command === 'plugin:dialog|open') {
              return Promise.resolve('/tmp/inaturalist-exports');
            }
            return Promise.resolve(null);
          };
        }
        """
    )
    page.route(
        "**/api/inat/export",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body='{"job_id":"inat-export-open"}',
        ),
    )
    # The export job's progress stream stays open until the modal closes it.
    page.route("**/api/jobs/inat-export-open/stream", lambda route: None)
    page.evaluate("item => openInatQuickModal([item], [])", _item())

    page.locator("#inatQuickExportBtn").click()
    page.wait_for_function("window._inatExportStream !== null")
    page.evaluate("window.__exportStream = window._inatExportStream; closeInatModal()")

    assert page.evaluate("window.__exportStream.readyState === EventSource.CLOSED")
    assert page.evaluate("window._inatExportStream") is None
    expect(page.locator("#inatModal")).to_have_class("modal-overlay")


def test_quick_open_can_omit_all_metadata_for_generic_upload(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)
    page.evaluate("item => openInatQuickModal([item], [])", _item())

    page.locator("#inatIncludeTaxon0").uncheck()
    page.locator("#inatIncludeDate0").uncheck()
    page.locator("#inatIncludeLocation0").uncheck()
    page.locator("#inatCards a", has_text="Open Upload Page").click()
    page.wait_for_function(
        "window.__externalTest.invokes.some(call => "
        "call.command === 'open_external_url')"
    )

    assert _open_commands(page) == [
        {"command": "open_external_url", "args": {"url": INAT_UPLOAD_URL}}
    ]


def test_native_open_failure_stays_put_and_offers_retry_and_copy(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    original_url = page.url
    _mock_tauri(page, fail_open=True)

    page.evaluate("item => openInatQuickModal([item], [])", _item())
    page.locator("#inatCards a", has_text="Open Upload Page").click()

    assert len(_open_commands(page)) == 1
    assert page.evaluate("window.__externalTest.windowOpenCalls") == []
    assert page.url == original_url
    modal = page.locator("#externalOpenModal")
    expect(modal).to_be_visible()
    expect(modal.locator("#externalOpenModalUrl")).to_have_value(INAT_URL)
    expect(modal.locator("#externalOpenRetryBtn")).to_be_visible()
    expect(modal.locator("#externalOpenCopyBtn")).to_be_visible()
    expect(modal.locator("#externalOpenCloseBtn")).to_be_visible()


def test_browser_popup_block_never_replaces_vireo(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    original_url = page.url
    page.evaluate(
        """
        () => {
          delete window.__TAURI_INTERNALS__;
          window.__externalTest = { windowOpenCalls: [] };
          window.open = (...args) => {
            window.__externalTest.windowOpenCalls.push(args);
            return null;
          };
        }
        """
    )

    page.evaluate("url => openExternalWithRecovery(url)", INAT_URL)

    assert page.url == original_url
    assert page.evaluate("window.__externalTest.windowOpenCalls") == [
        ["about:blank", "_blank"]
    ]
    expect(page.locator("#externalOpenModal")).to_be_visible()
    expect(page.locator("#externalOpenModalUrl")).to_have_value(INAT_URL)


def test_quick_upload_uses_real_link_when_tauri_bridge_is_missing(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    page.evaluate("delete window.__TAURI_INTERNALS__")
    fallback_base = f"{live_server['url']}/fake-inaturalist-upload"
    page.evaluate(
        "item => openInatQuickModal([item], [])",
        _item(url=fallback_base),
    )

    link = page.locator("#inatCards a", has_text="Open Upload Page")
    with page.expect_popup() as popup_info:
        link.click()

    popup = popup_info.value
    popup.wait_for_url(fallback_base + "?**")
    assert popup.url == (
        fallback_base
        + "?taxon_name=Corvus%20corax&observed_on=2026-07-12&lat=47.61&lng=-122.33"
    )
    expect(page.locator("#externalOpenModal")).to_have_count(0)


def test_delegated_handler_covers_unannotated_external_links(live_server, page):
    page.goto(f"{live_server['url']}/settings")
    original_url = page.url
    _mock_tauri(page)

    # This link deliberately has no target=_blank or inline external handler.
    page.locator('a[href="https://www.darktable.org/"]').click()
    page.wait_for_function(
        "window.__externalTest.invokes.some(call => "
        "call.command === 'open_external_url')"
    )

    commands = _open_commands(page)
    assert commands == [
        {"command": "open_external_url", "args": {"url": "https://www.darktable.org/"}}
    ]
    assert page.url == original_url


def test_internal_links_are_not_claimed_by_external_handler(live_server, page):
    page.goto(f"{live_server['url']}/settings")
    _mock_tauri(page)

    assert page.evaluate("_isExternalHttpUrl('/browse')") is False
    assert page.evaluate("_isExternalHttpUrl('https://example.com')") is True
    assert _open_commands(page) == []


def test_batch_quick_uploads_require_explicit_per_item_open(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page, fail_copy=True)
    items = [
        _item(1),
        _item(
            2,
        ),
    ]

    page.evaluate("items => openInatQuickModal(items, [])", items)

    assert _open_commands(page) == []
    expect(page.locator("#inatModal")).to_have_class("modal-overlay open")
    expect(page.locator("#inatCards a", has_text="Open Upload")).to_have_count(2)
    expect(page.locator("#inatCards button", has_text="Copy URL")).to_have_count(2)

    # Copy still has a DOM fallback when clipboard permission is unavailable;
    # batch recovery must not depend on the separate failure modal existing.
    page.evaluate(
        """
        () => {
          Object.defineProperty(navigator, 'clipboard', {
            configurable: true,
            value: { writeText: () => Promise.reject(new Error('denied')) }
          });
          window.__externalTest.execCopyCalls = 0;
          document.execCommand = command => {
            if (command === 'copy') window.__externalTest.execCopyCalls += 1;
            return true;
          };
        }
        """
    )
    page.locator("#inatCards button", has_text="Copy URL").first.click()
    page.wait_for_function("window.__externalTest.execCopyCalls === 1")


def test_native_copy_url_uses_system_clipboard_plugin(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)
    page.evaluate("item => openInatQuickModal([item], [])", _item())
    page.evaluate(
        """
        () => {
          Object.defineProperty(navigator, 'clipboard', {
            configurable: true,
            value: { writeText: () => Promise.reject(new Error('should not run')) }
          });
          window.__externalTest.execCopyCalls = 0;
          document.execCommand = () => {
            window.__externalTest.execCopyCalls += 1;
            return true;
          };
        }
        """
    )

    page.locator("#inatCards button", has_text="Copy URL").click()
    page.wait_for_function(
        "window.__externalTest.invokes.some(call => "
        "call.command === 'plugin:clipboard-manager|write_text')"
    )

    copy_commands = page.evaluate(
        "window.__externalTest.invokes.filter(call => "
        "call.command === 'plugin:clipboard-manager|write_text')"
    )
    assert copy_commands == [
        {
            "command": "plugin:clipboard-manager|write_text",
            "args": {"text": INAT_URL},
        }
    ]
    assert page.evaluate("window.__externalTest.execCopyCalls") == 0
    expect(page.locator("#toastContainer")).to_contain_text("URL copied.")


def test_batch_quick_open_preserves_preparation_failures(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)

    failures = [
        {"photo_id": 42, "error": "Missing GPS coordinates"},
        {"photo_id": 43, "error": "No identifiable taxon"},
    ]
    page.evaluate(
        "args => openInatQuickModal([args.item], args.failures)",
        {"item": _item(1), "failures": failures},
    )

    # The review modal must render both the prepared upload and the failure
    # summary — otherwise those dropped photos are silently lost.
    assert _open_commands(page) == []
    expect(page.locator("#inatModal")).to_have_class("modal-overlay open")
    expect(page.locator("#inatCards", has_text="Open Upload Page")).to_be_visible()
    expect(page.locator("#inatCards")).to_contain_text(
        "2 photos could not be prepared."
    )


def test_copy_url_fallback_does_not_reuse_stale_failure_modal_value(live_server, page):
    """Codex P2 regression: after the external-open failure modal has been shown
    once with URL A, a subsequent copyExternalUrl(URL_B) call whose clipboard
    write rejects must copy URL_B — not the stale value still sitting in
    #externalOpenModalUrl."""
    page.goto(f"{live_server['url']}/browse")
    stale_url = "https://www.inaturalist.org/observations/upload?taxon_name=Stale"
    fresh_url = "https://www.inaturalist.org/observations/upload?taxon_name=Fresh"

    # Prime the failure modal so #externalOpenModalUrl exists with the stale URL.
    page.evaluate("url => showExternalOpenFailure(url)", stale_url)
    expect(page.locator("#externalOpenModalUrl")).to_have_value(stale_url)
    page.evaluate("() => closeExternalOpenFailure()")

    # Force the clipboard fallback and capture what document.execCommand would copy.
    page.evaluate(
        """
        () => {
          Object.defineProperty(navigator, 'clipboard', {
            configurable: true,
            value: { writeText: () => Promise.reject(new Error('denied')) }
          });
          window.__copyProbe = { selection: null, executed: false };
          document.execCommand = command => {
            if (command === 'copy') {
              window.__copyProbe.selection = String(document.getSelection() || '');
              window.__copyProbe.executed = true;
            }
            return true;
          };
        }
        """
    )

    page.evaluate("url => copyExternalUrl(url)", fresh_url)
    page.wait_for_function("window.__copyProbe.executed === true")

    probe = page.evaluate("window.__copyProbe")
    assert probe["selection"] == fresh_url, (
        f"Expected fresh URL to be copied, got {probe['selection']!r}"
    )
    # Stale modal input must be left untouched — no one should be reading from it.
    expect(page.locator("#externalOpenModalUrl")).to_have_value(stale_url)


def test_duplicate_quick_upload_does_not_auto_open(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)

    page.evaluate("item => openInatQuickModal([item], [])", _item(1, duplicate=True))

    assert _open_commands(page) == []
    expect(page.locator("#inatModal")).to_have_class("modal-overlay open")
    expect(page.locator("#inatCards")).to_contain_text("Already submitted")
    expect(page.locator("#inatCards", has_text="Open Upload Page")).to_be_visible()
    expect(page.locator("#inatCards button", has_text="Copy URL")).to_be_visible()
