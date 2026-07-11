"""Behavioral coverage for browser and Tauri external-navigation handoffs."""

from playwright.sync_api import expect

INAT_URL = "https://www.inaturalist.org/observations/upload?taxon_name=Corvus"


def _item(photo_id=1, *, url=INAT_URL, duplicate=False):
    return {
        "photo_id": photo_id,
        "filename": f"photo-{photo_id}.jpg",
        "upload_url": url,
        "already_submitted": duplicate,
        "existing_url": (
            "https://www.inaturalist.org/observations/123" if duplicate else None
        ),
    }


def _mock_tauri(page, *, fail_open=False):
    page.evaluate(
        """
        failOpen => {
          window.__externalTest = { invokes: [], windowOpenCalls: [] };
          window.__TAURI_INTERNALS__ = {
            invoke: (command, args) => {
              window.__externalTest.invokes.push({ command, args });
              if (failOpen && command === 'open_external_url') {
                return Promise.reject(new Error('browser launch failed'));
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
        fail_open,
    )


def _open_commands(page):
    return page.evaluate(
        "window.__externalTest.invokes.filter(call => "
        "call.command === 'open_external_url')"
    )


def test_native_quick_open_uses_one_command_and_only_a_toast(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    original_url = page.url
    _mock_tauri(page)

    page.evaluate("item => openInatQuickModal([item], [])", _item())

    commands = _open_commands(page)
    assert commands == [{"command": "open_external_url", "args": {"url": INAT_URL}}]
    assert page.evaluate("window.__externalTest.windowOpenCalls") == []
    assert page.url == original_url
    expect(page.locator("#toastContainer")).to_contain_text(
        "Opened iNaturalist in your browser."
    )
    expect(page.locator("#inatModal")).not_to_have_class("open")


def test_native_open_failure_stays_put_and_offers_retry_and_copy(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    original_url = page.url
    _mock_tauri(page, fail_open=True)

    page.evaluate("item => openInatQuickModal([item], [])", _item())

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
    _mock_tauri(page)
    items = [
        _item(1),
        _item(
            2,
            url="https://www.inaturalist.org/observations/upload?taxon_name=Pica",
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

    # The batch had preparation failures, so the auto-open shortcut must be
    # skipped and the modal must render both the prepared upload and the
    # failure summary — otherwise those dropped photos are silently lost.
    assert _open_commands(page) == []
    expect(page.locator("#inatModal")).to_have_class("modal-overlay open")
    expect(page.locator("#inatCards", has_text="Open Upload Page")).to_be_visible()
    expect(page.locator("#inatCards")).to_contain_text(
        "2 photos could not be prepared."
    )


def test_duplicate_quick_upload_does_not_auto_open(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _mock_tauri(page)

    page.evaluate("item => openInatQuickModal([item], [])", _item(1, duplicate=True))

    assert _open_commands(page) == []
    expect(page.locator("#inatModal")).to_have_class("modal-overlay open")
    expect(page.locator("#inatCards")).to_contain_text("Already submitted")
    expect(page.locator("#inatCards", has_text="Open Upload Page")).to_be_visible()
    expect(page.locator("#inatCards button", has_text="Copy URL")).to_be_visible()
