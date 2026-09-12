import json
import re

import pytest
from playwright.sync_api import expect

# The preview endpoint streams SSE frames and ends with a `done` frame that
# carries the old synchronous folder-preview payload. These helpers let
# stubs speak that protocol: __previewDoneResponse wraps a payload in a
# single done frame; __sseStream hands tests a push()/close() handle for
# driving scan-progress frames one at a time.
SSE_PREVIEW_HELPERS = """
window.__previewDoneResponse = (payload) => new Response(
  'data: ' + JSON.stringify(Object.assign({type: 'done'}, payload)) + '\\n\\n',
  {status: 200, headers: {'Content-Type': 'text/event-stream'}});
window.__sseStream = () => {
  let controller;
  const encoder = new TextEncoder();
  const stream = new ReadableStream({start(c) { controller = c; }});
  return {
    response: new Response(stream,
      {status: 200, headers: {'Content-Type': 'text/event-stream'}}),
    push: (frame) => controller.enqueue(encoder.encode(
      'data: ' + JSON.stringify(frame) + '\\n\\n')),
    close: () => { try { controller.close(); } catch (e) {} },
  };
};
"""


@pytest.fixture(autouse=True)
def _sse_preview_helpers(page):
    page.add_init_script(SSE_PREVIEW_HELPERS)
    yield


def _fulfill_preview_stream(route, payload):
    route.fulfill(
        status=200,
        content_type="text/event-stream",
        body="data: " + json.dumps({"type": "done", **payload}) + "\n\n",
    )



def _suppress_auto_preview(page):
    """Stop the 350ms debounced auto-preview from firing.

    For tests that only care about the Start payload. Their source folders
    don't exist, so a debounced preview that does land completes with zero
    files -- a real, completed preview -- and updateStartGate() correctly
    disables Start with "No files to import". Whether it lands at all is a
    race against how fast Playwright drives the form, so suppress it rather
    than let the machine's speed decide.

    This hooks window.scheduleImportPreview, so it stops working silently if
    a caller is ever changed to reach previewImport() directly. Assert
    #btnStart is enabled immediately before clicking it in tests that rely on
    this, so the failure surfaces as an assertion rather than as a 30s
    actionability timeout with no explanation.
    """
    page.evaluate("() => { window.scheduleImportPreview = () => {}; }")


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
            if (target && target.indexOf('/api/import/folder-preview-stream') === 0) {
              return Promise.resolve(window.__previewDoneResponse({
                total_count: 42,
                total_size: 0,
                type_breakdown: {'.jpg': 42},
                duplicate_count: 0,
                files: [],
                source_counts: {'/tmp/card-a': 42},
              }));
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


# Stubs the preview stream with a hand-driven SSE stream. Tests push
# policy/folder frames via window.__streams[N].push(...) to walk the UI
# through scan states; a final done frame completes the preview. Aborting a
# superseded request closes its stream (mirroring a real disconnected
# response body) and counts it in __streamAborts.
CONTROLLED_STREAM_STUB = """
    () => {
      window.__streams = [];
      window.__streamAborts = 0;
      const originalFetch = window.fetch.bind(window);
      window.fetch = (input, init) => {
        const target = typeof input === 'string' ? input : input.url;
        if (target && target.indexOf('/api/import/folder-preview-stream') === 0) {
          const stream = window.__sseStream();
          stream.folders = JSON.parse(init.body).folders;
          if (init.signal) {
            init.signal.addEventListener('abort', () => {
              window.__streamAborts += 1;
              stream.close();
            }, {once: true});
          }
          window.__streams.push(stream);
          return Promise.resolve(stream.response);
        }
        return originalFetch(input, init);
      };
      window.pickDirectory = async () => ['/tmp/card-a', '/tmp/card-b'];
    }
"""


def _push_frame(page, stream_index, frame_js):
    page.evaluate(
        "() => window.__streams[" + str(stream_index) + "].push(" +
        frame_js + ")")


def test_import_source_counts_explain_queue_progress(live_server, page):
    """Multiple folders show which scan is active and which are waiting."""
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(CONTROLLED_STREAM_STUB)

    page.locator("[data-testid='import-source-browse-btn']").click()
    page.wait_for_function("() => window.__streams.length === 1")

    _push_frame(page, 0, """{type: 'policy', sources: [
      {path: '/tmp/card-a', storage: 'network', position: 1, total: 2},
      {path: '/tmp/card-b', storage: 'network', position: 2, total: 2},
    ]}""")
    metas = page.locator("#sourceList .source-meta")
    expect(metas.nth(0)).to_have_text("Waiting · 1 of 2")
    expect(metas.nth(1)).to_have_text("Waiting · 2 of 2")

    _push_frame(
        page, 0,
        "{type: 'folder_started', path: '/tmp/card-a', storage: 'network'}")
    expect(metas.nth(0)).to_contain_text("Scanning")
    expect(metas.nth(1)).to_have_text("Waiting · 2 of 2")
    progress = page.locator("#sourceCountProgress")
    expect(progress).to_contain_text("Scanning folder 1 of 2")
    expect(progress).to_contain_text("network storage")

    _push_frame(page, 0, """{type: 'folder_progress', path: '/tmp/card-a',
      stage: 'walk', checked: 1200, found: 3}""")
    expect(metas.nth(0)).to_contain_text("1,200 checked")
    expect(metas.nth(0)).to_contain_text("(3 photos)")

    _push_frame(
        page, 0,
        "{type: 'folder_done', path: '/tmp/card-a', count: 3, error: false}")
    expect(metas.nth(0)).to_have_text("3 photos")
    _push_frame(
        page, 0,
        "{type: 'folder_started', path: '/tmp/card-b', storage: 'network'}")
    expect(metas.nth(1)).to_contain_text("Scanning")
    expect(progress).to_contain_text("1 folder counted (3 photos)")

    _push_frame(
        page, 0,
        "{type: 'folder_done', path: '/tmp/card-b', count: 4, error: false}")
    _push_frame(page, 0, """{type: 'done', total_count: 7, total_size: 0,
      type_breakdown: {'.jpg': 7}, duplicate_count: 0, files: [],
      source_counts: {'/tmp/card-a': 3, '/tmp/card-b': 4}}""")
    page.evaluate("() => window.__streams[0].close()")
    expect(metas.nth(1)).to_have_text("4 photos")
    expect(progress).to_have_text("2 folders counted · 7 photos found")


def test_import_source_counts_overlap_on_known_local_storage(live_server, page):
    """A fast local volume scans several folders at once."""
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(CONTROLLED_STREAM_STUB)

    page.locator("[data-testid='import-source-browse-btn']").click()
    page.wait_for_function("() => window.__streams.length === 1")

    _push_frame(page, 0, """{type: 'policy', sources: [
      {path: '/tmp/card-a', storage: 'local', position: 1, total: 2},
      {path: '/tmp/card-b', storage: 'local', position: 2, total: 2},
    ]}""")
    _push_frame(
        page, 0,
        "{type: 'folder_started', path: '/tmp/card-a', storage: 'local'}")
    _push_frame(
        page, 0,
        "{type: 'folder_started', path: '/tmp/card-b', storage: 'local'}")

    metas = page.locator("#sourceList .source-meta")
    expect(metas.nth(0)).to_contain_text("Scanning")
    expect(metas.nth(1)).to_contain_text("Scanning")
    progress = page.locator("#sourceCountProgress")
    expect(progress).to_contain_text("Scanning folders 1 and 2 of 2")
    expect(progress).to_contain_text("local storage")

    _push_frame(
        page, 0,
        "{type: 'folder_done', path: '/tmp/card-a', count: 3, error: false}")
    _push_frame(
        page, 0,
        "{type: 'folder_done', path: '/tmp/card-b', count: 4, error: false}")
    _push_frame(page, 0, """{type: 'done', total_count: 7, total_size: 0,
      type_breakdown: {'.jpg': 7}, duplicate_count: 0, files: [],
      source_counts: {'/tmp/card-a': 3, '/tmp/card-b': 4}}""")
    page.evaluate("() => window.__streams[0].close()")
    expect(progress).to_have_text("2 folders counted · 7 photos found")


def test_preview_done_payload_settles_source_rows(live_server, page):
    """A stream delivering only the final payload still fills every row."""
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview-stream') === 0) {
              return Promise.resolve(window.__previewDoneResponse({
                total_count: 2,
                total_size: 2,
                type_breakdown: {'.jpg': 2},
                duplicate_count: 0,
                files: [],
                source_counts: {'/tmp/card-a': 1, '/tmp/card-b': 1},
              }));
            }
            return originalFetch(input, init);
          };
          window.pickDirectory = async () => ['/tmp/card-a', '/tmp/card-b'];
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()

    expect(page.locator("#sourceList .source-meta")).to_have_text([
        "1 photo", "1 photo",
    ])
    expect(page.locator("#sourceCountProgress")).to_have_text(
        "2 folders counted · 2 photos found")


def test_superseded_preview_stream_is_aborted(live_server, page):
    """Changing the source list cancels the previous walk outright.

    The client holds one stream per preview; aborting it is what lets the
    server stop walking (test_source_discovery covers the server half).
    """
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(CONTROLLED_STREAM_STUB)

    page.evaluate("() => addSourcePath('/tmp/card-a')")
    page.wait_for_function("() => window.__streams.length === 1")
    _push_frame(page, 0, """{type: 'policy', sources: [
      {path: '/tmp/card-a', storage: 'network', position: 1, total: 1},
    ]}""")
    _push_frame(
        page, 0,
        "{type: 'folder_started', path: '/tmp/card-a', storage: 'network'}")
    metas = page.locator("#sourceList .source-meta")
    expect(metas.nth(0)).to_contain_text("Scanning")

    page.evaluate("() => addSourcePath('/tmp/card-b')")
    page.wait_for_function("() => window.__streams.length === 2")
    assert page.evaluate("() => window.__streamAborts") == 1
    # The successor walks the full selection in one traversal.
    assert page.evaluate("() => window.__streams[1].folders") == [
        "/tmp/card-a", "/tmp/card-b",
    ]


def test_no_file_types_settles_rows_from_aborted_preview(live_server, page):
    """Validation after abort must not leave the old scan appearing active."""
    page.goto(f"{live_server['url']}/import")
    page.wait_for_load_state("networkidle")
    _suppress_auto_preview(page)
    page.evaluate(
        """() => {
          sources = ['/tmp/card-a'];
          document.getElementById('modeCopy').checked = true;
          sourceCounts['/tmp/card-a'] = {
            status: 'loading', position: 1, total: 1,
            storage: 'network', startedAt: Date.now(),
            lastEventAt: Date.now(),
          };
          ensureSourceCountClock();
          renderSources();
          window.__previewAborted = false;
          importPreviewAbort = new AbortController();
          importPreviewAbort.signal.addEventListener(
            'abort', () => { window.__previewAborted = true; }, {once: true});
        }"""
    )
    meta = page.locator("#sourceList .source-meta")
    expect(meta).to_contain_text("Scanning")

    page.evaluate(
        """async () => {
          document.getElementById('fileTypePreset').value = 'custom';
          document.querySelectorAll('.file-ext').forEach(
            (el) => { el.checked = false; });
          await previewImport();
        }"""
    )

    assert page.evaluate("() => window.__previewAborted") is True
    expect(meta).to_have_text("Waiting to scan")
    expect(page.locator("#sourceCountProgress")).not_to_have_class(
        re.compile(r"\bvisible\b"))
    assert page.evaluate("() => sourceCountTickTimer") is None


def test_no_file_types_clears_completed_counts_from_old_filter(
        live_server, page):
    """A validation abort must not retain a count from the old file filter."""
    page.goto(f"{live_server['url']}/import")
    page.wait_for_load_state("networkidle")
    _suppress_auto_preview(page)
    page.evaluate(
        """() => {
          sources = ['/tmp/card-a'];
          document.getElementById('modeCopy').checked = true;
          sourceCounts['/tmp/card-a'] = {
            status: 'loaded', count: 42, text: '42 photos',
          };
          renderSources();
        }"""
    )
    meta = page.locator("#sourceList .source-meta")
    expect(meta).to_have_text("42 photos")

    page.evaluate(
        """async () => {
          document.getElementById('fileTypePreset').value = 'custom';
          document.querySelectorAll('.file-ext').forEach(
            (el) => { el.checked = false; });
          await previewImport();
        }"""
    )

    expect(meta).to_have_text("Waiting to scan")
    expect(page.locator("#sourceCountProgress")).not_to_have_class(
        re.compile(r"\bvisible\b"))


def test_stalled_network_scan_shows_no_response_hint(live_server, page):
    """A quiet network walk surfaces a disconnected-storage hint."""
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(CONTROLLED_STREAM_STUB)
    # Shrink the stall threshold so the hint appears within one clock tick.
    page.evaluate("() => { SOURCE_SCAN_STALL_MS = 300; }")

    page.evaluate("() => addSourcePath('/tmp/card-a')")
    page.wait_for_function("() => window.__streams.length === 1")
    _push_frame(page, 0, """{type: 'policy', sources: [
      {path: '/tmp/card-a', storage: 'network', position: 1, total: 1},
    ]}""")
    _push_frame(
        page, 0,
        "{type: 'folder_started', path: '/tmp/card-a', storage: 'network'}")

    # No further frames arrive: the walk is blocked on the mount.
    metas = page.locator("#sourceList .source-meta")
    expect(metas.nth(0)).to_contain_text("no response")
    expect(page.locator("#sourceCountProgress")).to_contain_text(
        "not responding — the storage may be disconnected or slow")

    # A late frame clears the hint: the walk was slow, not dead.
    _push_frame(page, 0, """{type: 'folder_progress', path: '/tmp/card-a',
      stage: 'walk', checked: 900, found: 2}""")
    expect(metas.nth(0)).not_to_contain_text("no response")


# Stubs a two-file copy-mode preview where IMG_0002.jpg comes back flagged as
# a duplicate. Shared by the tests that exercise the preview grid.
TWO_FILE_PREVIEW_STUB = """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__fullPreviewCalls = 0;
          window.__dupCalls = 0;
          window.__destCalls = 0;
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              const body = JSON.parse(init.body || '{}');
              window.__fullPreviewCalls += 1;
              // Tests flip __emptyPreview to simulate a source that comes
              // back with zero importable files (e.g. an empty card or a
              // filter that excludes everything).
              if (window.__emptyPreview) {
                return Promise.resolve(window.__previewDoneResponse({
                  total_count: 0,
                  total_size: 0,
                  type_breakdown: {},
                  duplicate_count: 0,
                  files: [],
                }));
              }
              return Promise.resolve(window.__previewDoneResponse({
                total_count: 2,
                total_size: 2468,
                type_breakdown: {'.jpg': 2},
                duplicate_count: 0,
                files: [
                  {
                    path: '/tmp/card-a/IMG_0001.jpg',
                    filename: 'IMG_0001.jpg',
                    subfolder: 'card-a',
                    size: 1234,
                    extension: '.jpg',
                    thumb_url: 'data:image/gif;base64,R0lGODlhAQABAAAAACw=',
                  },
                  {
                    path: '/tmp/card-a/IMG_0002.jpg',
                    filename: 'IMG_0002.jpg',
                    subfolder: 'card-a',
                    size: 1234,
                    extension: '.jpg',
                    thumb_url: 'data:image/gif;base64,R0lGODlhAQABAAAAACw=',
                  },
                ],
              }));
            }
            if (target && target.indexOf('/api/import/check-duplicates') === 0) {
              window.__dupCalls += 1;
              // Tests flip __noDupes to simulate the next card coming back
              // clean, which is how the preview reaches a completed check
              // with zero duplicates.
              const dupes = window.__noDupes
                ? [] : ['/tmp/card-a/IMG_0002.jpg'];
              const frame = 'data: ' + JSON.stringify({
                duplicates: dupes,
                checked: 2,
                total: 2,
              }) + '\\n\\n' + 'data: ' + JSON.stringify({
                done: true,
                duplicate_count: dupes.length,
                checked: 2,
                total: 2,
              }) + '\\n\\n';
              return Promise.resolve(new Response(frame, {
                status: 200,
                headers: {'Content-Type': 'text/event-stream'},
              }));
            }
            if (target && target.indexOf('/api/import/destination-preview') === 0) {
              window.__destCalls += 1;
              return Promise.resolve(new Response(JSON.stringify({
                folders: [{
                  path: '2026/2026-07-11',
                  full_path: '/archive/2026/2026-07-11',
                  count: 1,
                  exists: false,
                }],
                total_photos: 1,
                total_folders: 1,
                new_folders: 1,
                existing_folders: 0,
                managed_archive: null,
                files: [{
                  path: '/tmp/card-a/IMG_0001.jpg',
                  folder: '2026/2026-07-11',
                  full_folder: '/archive/2026/2026-07-11',
                }],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """


def run_two_file_preview(live_server, page):
    """Load /import with the stub above and wait for the preview to settle."""
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(TWO_FILE_PREVIEW_STUB)

    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/archive")
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()

    page.wait_for_function(
        "window.__fullPreviewCalls >= 1 && window.__dupCalls >= 1 && window.__destCalls >= 1"
    )


# Stubs a four-file copy-mode preview split across two subfolders:
# card-a (both files are duplicates) and card-b (one duplicate, one new).
# The mixed layout is what exercises the "one folder is entirely duplicates
# while another still has content" case for the hide-duplicates filter.
MULTI_FOLDER_MIXED_PREVIEW_STUB = """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__fullPreviewCalls = 0;
          window.__dupCalls = 0;
          window.__destCalls = 0;
          const THUMB = 'data:image/gif;base64,R0lGODlhAQABAAAAACw=';
          const FILES = [
            {path: '/tmp/cards/card-a/A1.jpg', filename: 'A1.jpg', subfolder: 'card-a', size: 1234, extension: '.jpg', thumb_url: THUMB},
            {path: '/tmp/cards/card-a/A2.jpg', filename: 'A2.jpg', subfolder: 'card-a', size: 1234, extension: '.jpg', thumb_url: THUMB},
            {path: '/tmp/cards/card-b/B1.jpg', filename: 'B1.jpg', subfolder: 'card-b', size: 1234, extension: '.jpg', thumb_url: THUMB},
            {path: '/tmp/cards/card-b/B2.jpg', filename: 'B2.jpg', subfolder: 'card-b', size: 1234, extension: '.jpg', thumb_url: THUMB},
          ];
          const DUPES = [
            '/tmp/cards/card-a/A1.jpg',
            '/tmp/cards/card-a/A2.jpg',
            '/tmp/cards/card-b/B2.jpg',
          ];
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              const body = JSON.parse(init.body || '{}');
              window.__fullPreviewCalls += 1;
              return Promise.resolve(window.__previewDoneResponse({
                total_count: 4,
                total_size: 4936,
                type_breakdown: {'.jpg': 4},
                duplicate_count: 0,
                files: FILES,
              }));
            }
            if (target && target.indexOf('/api/import/check-duplicates') === 0) {
              window.__dupCalls += 1;
              const frame = 'data: ' + JSON.stringify({
                duplicates: DUPES, checked: FILES.length, total: FILES.length,
              }) + '\\n\\n' + 'data: ' + JSON.stringify({
                done: true, duplicate_count: DUPES.length, checked: FILES.length, total: FILES.length,
              }) + '\\n\\n';
              return Promise.resolve(new Response(frame, {
                status: 200,
                headers: {'Content-Type': 'text/event-stream'},
              }));
            }
            if (target && target.indexOf('/api/import/destination-preview') === 0) {
              window.__destCalls += 1;
              return Promise.resolve(new Response(JSON.stringify({
                folders: [{path: '2026/2026-07-11', full_path: '/archive/2026/2026-07-11', count: 1, exists: false}],
                total_photos: 1, total_folders: 1, new_folders: 1, existing_folders: 0, managed_archive: null,
                files: [{path: '/tmp/cards/card-b/B1.jpg', folder: '2026/2026-07-11', full_folder: '/archive/2026/2026-07-11'}],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """


def run_multi_folder_mixed_preview(live_server, page):
    """Load /import with the multi-folder stub and wait for it to settle."""
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(MULTI_FOLDER_MIXED_PREVIEW_STUB)

    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/archive")
    page.locator("#sourceInput").fill("/tmp/cards")
    page.locator("#btnAddSource").click()

    page.wait_for_function(
        "window.__fullPreviewCalls >= 1 && window.__dupCalls >= 1 && window.__destCalls >= 1"
    )


def test_import_preview_runs_automatically_after_source_selection(live_server, page):
    run_two_file_preview(live_server, page)

    expect(page.locator("#previewSummary")).to_contain_text("1 already in your library")
    grid = page.locator("#importPreviewGrid")
    expect(grid).to_be_visible()
    expect(grid).to_contain_text("IMG_0001.jpg")
    # Duplicate filtering is opt-in, so the automatic preview initially
    # shows both files and marks the duplicate. The dedicated filter test
    # below covers the collapsed state after the checkbox is enabled.
    expect(grid).to_contain_text("IMG_0002.jpg")
    expect(grid).to_contain_text("Duplicate")
    expect(grid).to_contain_text("To: 2026/2026-07-11")


def test_import_preview_hide_duplicates_checkbox_filters_grid(live_server, page):
    run_two_file_preview(live_server, page)

    grid = page.locator("#importPreviewGrid")
    checkbox = page.locator("#chkHideDuplicates")
    # Defaults to off: every discovered file is visible until the user opts in.
    expect(page.locator("#hideDuplicatesRow")).to_be_visible()
    expect(checkbox).not_to_be_checked()
    expect(page.locator("#hideDuplicatesLabel")).to_have_text("Hide duplicates (1)")
    expect(grid).to_contain_text("IMG_0002.jpg")

    calls_before = page.evaluate(
        "[window.__fullPreviewCalls, window.__dupCalls, window.__destCalls]",
    )

    checkbox.check()
    expect(grid).to_contain_text("IMG_0001.jpg")
    expect(grid).not_to_contain_text("IMG_0002.jpg")
    expect(grid).to_contain_text("1 duplicate hidden")
    # The summary still reports the full picture — hiding is a view filter,
    # not a change to what the import will do.
    expect(page.locator("#previewSummary")).to_contain_text("1 already in your library")

    checkbox.uncheck()
    expect(grid).to_contain_text("IMG_0002.jpg")
    expect(grid).to_contain_text("Duplicate")

    # Toggling re-renders from the last preview result and must never
    # re-run discovery, the duplicate check, or the destination preview —
    # on a real card those are the slow calls the filter exists to avoid
    # repeating.
    assert page.evaluate(
        "[window.__fullPreviewCalls, window.__dupCalls, window.__destCalls]",
    ) == calls_before


def test_hide_duplicates_all_duplicate_banner_has_no_phantom_tiles(
    live_server, page,
):
    """When filtering leaves zero pending files, the collapsed banner must
    not claim that zero files are shown below when no tiles follow it."""
    run_two_file_preview(live_server, page)
    page.evaluate(
        """
        () => {
          const file = {
            path: '/tmp/card-a/IMG_0002.jpg',
            filename: 'IMG_0002.jpg',
            subfolder: 'card-a',
          };
          renderImportPreviewGrid(
            [file],
            [file.path],
            [],
          );
        }
        """
    )

    page.locator("#chkHideDuplicates").check()
    banner = page.locator("[data-testid='collapsed-duplicate-preview']")
    expect(banner).to_contain_text("1 duplicate hidden")
    expect(banner).to_contain_text(
        "Nothing else will be imported from this selection."
    )
    expect(banner).not_to_contain_text("0 files to import")
    expect(page.locator("#importPreviewGrid .import-preview-thumb")).to_have_count(0)


def test_hide_duplicates_resets_once_a_check_finds_no_duplicates(
    live_server, page,
):
    """A completed check that finds nothing to hide retires the opt-in.

    Otherwise the checkbox stays silently checked while its row is hidden,
    and the next card that does have duplicates gets filtered without the
    user asking for it this time.
    """
    run_two_file_preview(live_server, page)
    page.locator("#chkHideDuplicates").check()
    expect(page.locator("#importPreviewGrid")).not_to_contain_text(
        "IMG_0002.jpg",
    )

    # Next preview comes back clean, so the control has nothing to offer.
    page.evaluate("window.__noDupes = true")
    page.locator("#btnPreview").click()
    expect(page.locator("#hideDuplicatesRow")).to_be_hidden()
    expect(page.locator("#chkHideDuplicates")).not_to_be_checked()

    # ...so a later duplicate-bearing card starts unfiltered again.
    page.evaluate("window.__noDupes = false")
    page.locator("#btnPreview").click()
    expect(page.locator("#hideDuplicatesRow")).to_be_visible()
    expect(page.locator("#chkHideDuplicates")).not_to_be_checked()
    expect(page.locator("#importPreviewGrid")).to_contain_text("IMG_0002.jpg")


def test_hide_duplicates_resets_when_dedup_is_turned_off(live_server, page):
    """Turning off "Skip duplicates" settles the picture too: nothing will
    be skipped, so the filter has nothing to hide and the opt-in retires
    rather than lying in wait for the next dedup-enabled preview."""
    run_two_file_preview(live_server, page)
    page.locator("#chkHideDuplicates").check()

    page.locator("#chkSkipDuplicates").uncheck()
    expect(page.locator("#previewSummary")).to_contain_text(
        "duplicates will be copied",
    )
    expect(page.locator("#hideDuplicatesRow")).to_be_hidden()
    expect(page.locator("#chkHideDuplicates")).not_to_be_checked()

    dup_calls = page.evaluate("window.__dupCalls")
    page.locator("#chkSkipDuplicates").check()
    page.wait_for_function(f"window.__dupCalls > {dup_calls}")
    expect(page.locator("#chkHideDuplicates")).not_to_be_checked()
    expect(page.locator("#importPreviewGrid")).to_contain_text("IMG_0002.jpg")


def test_hide_duplicates_resets_when_the_last_source_is_removed(
    live_server, page,
):
    """Emptying the source list ends this card's session.

    The row is hidden at that point, so a filter left checked is invisible
    — and the next card added would be filtered on arrival without the
    user opting in for it.
    """
    run_two_file_preview(live_server, page)
    page.locator("#chkHideDuplicates").check()

    page.locator("#sourceList button", has_text="×").first.click()
    expect(page.locator("#hideDuplicatesRow")).to_be_hidden()
    expect(page.locator("#chkHideDuplicates")).not_to_be_checked()

    # A different card arrives and starts unfiltered.
    dup_calls = page.evaluate("window.__dupCalls")
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.wait_for_function(f"window.__dupCalls > {dup_calls}")
    expect(page.locator("#chkHideDuplicates")).not_to_be_checked()
    expect(page.locator("#importPreviewGrid")).to_contain_text("IMG_0002.jpg")


def test_hide_duplicates_resets_when_a_preview_settles_with_no_files(
    live_server, page,
):
    """A completed preview that returns zero files settles the picture too.

    The `!files.length` branch hides the row without ever running the
    duplicate check, so a filter left checked is invisible there — and
    the next card that does surface duplicates would be filtered on
    arrival without the user opting in for it.
    """
    run_two_file_preview(live_server, page)
    page.locator("#chkHideDuplicates").check()

    # Next preview finds nothing to import (e.g. an empty source card).
    preview_calls = page.evaluate("window.__fullPreviewCalls")
    page.evaluate("window.__emptyPreview = true")
    page.locator("#btnPreview").click()
    page.wait_for_function(f"window.__fullPreviewCalls > {preview_calls}")

    expect(page.locator("#previewSummary")).to_contain_text(
        "No importable files found.",
    )
    expect(page.locator("#hideDuplicatesRow")).to_be_hidden()
    expect(page.locator("#chkHideDuplicates")).not_to_be_checked()

    # ...so a later duplicate-bearing card starts unfiltered again.
    page.evaluate("window.__emptyPreview = false")
    dup_calls = page.evaluate("window.__dupCalls")
    page.locator("#btnPreview").click()
    page.wait_for_function(f"window.__dupCalls > {dup_calls}")
    expect(page.locator("#hideDuplicatesRow")).to_be_visible()
    expect(page.locator("#chkHideDuplicates")).not_to_be_checked()
    expect(page.locator("#importPreviewGrid")).to_contain_text("IMG_0002.jpg")


def test_hide_duplicates_resets_when_the_preview_cannot_run(live_server, page):
    """A preview that stops on a validation error is settled as well.

    Clearing every file extension aborts before discovery, so nothing can
    be hidden, and the opt-in must not survive behind the hidden row.
    """
    run_two_file_preview(live_server, page)
    page.locator("#chkHideDuplicates").check()

    page.locator("#fileTypePreset").select_option("custom")
    exts = page.locator(".file-ext")
    for i in range(exts.count()):
        exts.nth(i).uncheck()

    expect(page.locator("#importError")).to_contain_text(
        "Choose at least one file extension.",
    )
    expect(page.locator("#hideDuplicatesRow")).to_be_hidden()
    expect(page.locator("#chkHideDuplicates")).not_to_be_checked()

    # Leave a valid selection behind. This test is the only one that puts
    # the page in an unpreviewable state, and the preset is the kind of
    # option later tests assume is sane.
    page.locator("#fileTypePreset").select_option("both")


def test_hide_duplicates_preserves_first_occurrence_from_overlapping_sources(
    live_server, page,
):
    """Overlapping sources yield the same path twice in the preview.

    The `/api/import/check-duplicates` stream flags only the *later*
    occurrence as an intra-import duplicate — the first is the tile that
    will actually be copied and land in the archive. Hiding by path-set
    membership would drop both tiles, silently claiming the to-copy file
    is a duplicate too. Match the summary: hide one tile, keep the other.
    """
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__fullPreviewCalls = 0;
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              const body = JSON.parse(init.body || '{}');
              window.__fullPreviewCalls += 1;
              // Same path arrives twice — mimics scanning /card and
              // /card/DCIM together, the exact overlap that surfaces this
              // per-occurrence bug.
              return Promise.resolve(window.__previewDoneResponse({
                total_count: 2,
                total_size: 2468,
                type_breakdown: {'.jpg': 2},
                duplicate_count: 0,
                files: [
                  {
                    path: '/tmp/card/DCIM/IMG_0001.jpg',
                    filename: 'IMG_0001.jpg',
                    subfolder: 'card/DCIM',
                    size: 1234,
                    extension: '.jpg',
                    thumb_url: 'data:image/gif;base64,R0lGODlhAQABAAAAACw=',
                  },
                  {
                    path: '/tmp/card/DCIM/IMG_0001.jpg',
                    filename: 'IMG_0001.jpg',
                    subfolder: 'card/DCIM',
                    size: 1234,
                    extension: '.jpg',
                    thumb_url: 'data:image/gif;base64,R0lGODlhAQABAAAAACw=',
                  },
                ],
              }));
            }
            if (target && target.indexOf('/api/import/check-duplicates') === 0) {
              // The real checker only flags the second occurrence as an
              // intra-import duplicate, so the returned list carries the
              // path exactly once.
              const frame = 'data: ' + JSON.stringify({
                duplicates: ['/tmp/card/DCIM/IMG_0001.jpg'],
                checked: 2,
                total: 2,
              }) + '\\n\\n' + 'data: ' + JSON.stringify({
                done: true,
                duplicate_count: 1,
                checked: 2,
                total: 2,
              }) + '\\n\\n';
              return Promise.resolve(new Response(frame, {
                status: 200,
                headers: {'Content-Type': 'text/event-stream'},
              }));
            }
            if (target && target.indexOf('/api/import/destination-preview') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                folders: [{
                  path: '2026/2026-07-11',
                  full_path: '/archive/2026/2026-07-11',
                  count: 1,
                  exists: false,
                }],
                total_photos: 1,
                total_folders: 1,
                new_folders: 1,
                existing_folders: 0,
                managed_archive: null,
                files: [{
                  path: '/tmp/card/DCIM/IMG_0001.jpg',
                  folder: '2026/2026-07-11',
                  full_folder: '/archive/2026/2026-07-11',
                }],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/archive")
    page.locator("#sourceInput").fill("/tmp/card")
    page.locator("#btnAddSource").click()
    page.wait_for_function("window.__fullPreviewCalls >= 1")

    grid = page.locator("#importPreviewGrid")
    # Off by default: both occurrences render, one badged Duplicate.
    expect(page.locator("#previewSummary")).to_contain_text(
        "1 already in your library"
    )
    expect(page.locator("#hideDuplicatesLabel")).to_have_text(
        "Hide duplicates (1)"
    )
    expect(grid.locator(".import-preview-thumb")).to_have_count(2)
    expect(grid.locator(".import-preview-thumb.duplicate")).to_have_count(1)

    # Turn on the filter: the second occurrence (the tile the server
    # actually flagged) hides; the first stays, matching what the import
    # will copy. Set-based hiding would erroneously drop both.
    page.locator("#chkHideDuplicates").check()
    expect(grid.locator(".import-preview-thumb")).to_have_count(1)
    expect(grid.locator(".import-preview-thumb.duplicate")).to_have_count(0)
    expect(grid).to_contain_text("1 duplicate hidden")
    # Filter never claims "all X are duplicates" when a to-copy tile
    # survives — that phrasing would contradict the summary line.
    expect(grid).not_to_contain_text("are duplicates")


def test_hide_duplicates_survives_a_repreview_that_still_has_duplicates(
    live_server, page,
):
    """Re-previewing must not silently drop the filter.

    Any import-option change schedules a fresh preview, and each preview
    renders the grid with an empty duplicate list before its check
    finishes. Clearing the opt-in on every zero-count render would flip
    the filter off mid-session and flood the grid back.
    """
    run_two_file_preview(live_server, page)
    page.locator("#chkHideDuplicates").check()

    dup_calls = page.evaluate("window.__dupCalls")
    page.locator("#btnPreview").click()
    page.wait_for_function(f"window.__dupCalls > {dup_calls}")

    expect(page.locator("#chkHideDuplicates")).to_be_checked()
    expect(page.locator("#importPreviewGrid")).not_to_contain_text(
        "IMG_0002.jpg",
    )


def test_hide_duplicates_keeps_all_duplicate_folder_headers_visible(
    live_server, page,
):
    """When one folder of a multi-folder preview is made entirely of
    duplicates, enabling the filter must not silently drop that folder.

    Otherwise the surviving folders' headers own their hidden files, but
    the all-duplicate folder disappears without a trace — the user is
    given no evidence the folder or its duplicate count ever existed. The
    header stays visible as "folder (0) · N duplicates hidden" so the
    hidden count is always accounted for somewhere the user can see.
    """
    run_multi_folder_mixed_preview(live_server, page)

    grid = page.locator("#importPreviewGrid")
    # Off (default): both headers with full counts and all four tiles.
    expect(grid).to_contain_text("card-a (2)")
    expect(grid).to_contain_text("card-b (2)")
    expect(grid).to_contain_text("A1.jpg")
    expect(grid).to_contain_text("A2.jpg")
    expect(grid).to_contain_text("B1.jpg")
    expect(grid).to_contain_text("B2.jpg")

    page.locator("#chkHideDuplicates").check()

    # card-a is entirely duplicates — the header must survive with (0) and
    # the hidden count so the folder never silently vanishes.
    expect(grid).to_contain_text("card-a (0) · 2 duplicates hidden")
    # card-b keeps only its non-duplicate tile plus its hidden count.
    expect(grid).to_contain_text("card-b (1) · 1 duplicate hidden")
    expect(grid).to_contain_text("B1.jpg")
    expect(grid).not_to_contain_text("A1.jpg")
    expect(grid).not_to_contain_text("A2.jpg")
    expect(grid).not_to_contain_text("B2.jpg")

    # The global "all N are duplicates" empty state is reserved for the
    # case where every folder went to zero — a partially filtered preview
    # like this one still has visible tiles, so it must not fire.
    expect(grid).not_to_contain_text("All 4 files are duplicates")

    # Unchecking restores the full multi-folder grid unchanged.
    page.locator("#chkHideDuplicates").uncheck()
    expect(grid).to_contain_text("card-a (2)")
    expect(grid).to_contain_text("card-b (2)")
    expect(grid).to_contain_text("A1.jpg")
    expect(grid).to_contain_text("B2.jpg")


def test_import_auto_preview_clears_grid_when_selection_becomes_invalid(
    live_server, page
):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__fullPreviewCalls = 0;
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              const body = JSON.parse(init.body || '{}');
              window.__fullPreviewCalls += 1;
              return Promise.resolve(window.__previewDoneResponse({
                total_count: 1,
                total_size: 1234,
                type_breakdown: {'.jpg': 1},
                duplicate_count: 0,
                files: [{
                  path: '/tmp/card-a/IMG_0001.jpg',
                  filename: 'IMG_0001.jpg',
                  subfolder: 'card-a',
                  size: 1234,
                  extension: '.jpg',
                  thumb_url: 'data:image/gif;base64,R0lGODlhAQABAAAAACw=',
                }],
              }));
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
              return Promise.resolve(new Response(JSON.stringify({
                folders: [],
                files: [],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/archive")
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.wait_for_function("window.__fullPreviewCalls >= 1")
    expect(page.locator("#importPreviewGrid")).to_be_visible()

    page.locator("#fileTypePreset").select_option("custom")
    page.evaluate(
        """
        () => {
          document.querySelectorAll('.file-ext').forEach(el => { el.checked = false; });
          document.querySelector('.file-ext').dispatchEvent(
            new Event('change', { bubbles: true }));
        }
        """
    )

    expect(page.locator("#importError")).to_contain_text(
        "Choose at least one file extension."
    )
    expect(page.locator("#importPreviewGrid")).to_be_hidden()


def test_import_destination_browse_button_sets_destination(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => '/tmp/archive'")
    page.locator("#modeCopy").check()

    browse_btn = page.locator("[data-testid='import-destination-browse-btn']")
    expect(browse_btn).to_be_visible()
    browse_btn.click()

    expect(page.locator("#destInput")).to_have_value("/tmp/archive")


def test_import_recent_destination_button_selects_saved_path(live_server, page):
    """Saved import destinations remain visible as one-click choices."""
    import config as cfg

    config = cfg.load()
    config["ingest"]["recent_destinations"] = [
        "/Volumes/Photos/Archive",
        "/Volumes/Photos/Trips",
    ]
    cfg.save(config)

    page.goto(f"{live_server['url']}/import")
    page.locator("#modeCopy").check()

    choices = page.locator("[data-testid='recent-destinations']")
    expect(choices).to_be_visible()
    expect(choices).to_contain_text("Archive")
    expect(choices).to_contain_text("Trips")

    page.get_by_role(
        "button", name="Use /Volumes/Photos/Trips"
    ).click()
    expect(page.locator("#destInput")).to_have_value("/Volumes/Photos/Trips")


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
              return Promise.resolve(window.__previewDoneResponse({
                total_count: 0,
                total_size: 0,
                type_breakdown: {},
                duplicate_count: 0,
                files: [],
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
              return Promise.resolve(window.__previewDoneResponse({
                total_count: 1,
                total_size: 0,
                type_breakdown: {'.jpg': 1},
                duplicate_count: 0,
                files: [{path: '/tmp/card-a/IMG_0001.jpg'}],
              }));
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
    """Copy-mode preview surfaces exact destination folder paths and file
    counts beside the folder template, plus a managed-archive callout, wired to
    /api/import/destination-preview. Skipped duplicates are excluded so the
    folder counts match the files that will actually land."""
    url = live_server["url"]
    captured = {}

    def folder_preview(route):
        _fulfill_preview_stream(route, {
            "files": [
                {"path": "/tmp/card-a/IMG_0001.jpg"},
                {"path": "/tmp/card-a/IMG_0002.jpg"},
            ],
        })

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

    page.route("**/api/import/folder-preview-stream", folder_preview)
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
        "Resulting folders: 2 files split into 2 folders (1 new, 1 existing)"
    )
    expect(page.locator("#destCard #destStructure")).to_be_visible()
    expect(structure.locator("th")).to_have_text(["Exact folder", "Files", "Status"])
    rows = structure.locator("tr")
    expect(rows.nth(1).locator("td")).to_have_text(
        ["/archive/2026/2026-07-01", "1", "new"]
    )
    expect(rows.nth(2).locator("td")).to_have_text(
        ["/archive/2026/2026-07-02", "1", "existing"]
    )
    expect(structure).to_contain_text(
        "Files imported here land inside the managed archive rooted at"
    )
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
              return Promise.resolve(window.__previewDoneResponse({
                files: [{path: '/tmp/card-a/IMG_0001.jpg'}],
              }));
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
        _fulfill_preview_stream(
            route, {"files": [{"path": "/tmp/card-a/IMG_0001.jpg"}]})

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

    page.route("**/api/import/folder-preview-stream", folder_preview)
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
              return Promise.resolve(window.__previewDoneResponse({
                files: [
                  {path: '/tmp/card-a/IMG_0001.jpg'},
                  {path: '/tmp/card-a/IMG_0002.jpg'},
                ],
              }));
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
    # The control changes schedule a legitimate automatic refresh after the
    # debounce interval. Cancel that future run so this assertion stays scoped
    # to the stale duplicate stream we are releasing, then synchronize on the
    # stale preview's own finally block instead of an arbitrary timeout.
    page.evaluate(
        """() => {
          clearScheduledImportPreview();
          window.__resolveDuplicates();
        }"""
    )
    expect(page.locator("#btnPreview")).to_be_enabled()

    assert page.evaluate("window.__destinationPreviewCalled") is False
    expect(page.locator("#destStructure")).to_be_hidden()


def test_new_import_preview_aborts_superseded_duplicate_stream(
    live_server, page
):
    """Starting a new preview must stop the old hash-heavy stream.

    Sequence guards keep stale results off screen, but without transport
    cancellation the server keeps reading every source file anyway.
    """
    page.goto(f"{live_server['url']}/import")
    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/archive")
    page.evaluate(
        """
        () => {
          clearScheduledImportPreview();
          const originalFetch = window.fetch.bind(window);
          window.__duplicateFetchCount = 0;
          window.__firstDuplicateAborted = false;
          window.fetch = (input, init = {}) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(window.__previewDoneResponse({
                files: [{path: '/tmp/card-a/IMG_0001.jpg'}],
              }));
            }
            if (target && target.indexOf('/api/import/check-duplicates') === 0) {
              window.__duplicateFetchCount += 1;
              if (window.__duplicateFetchCount === 1) {
                return new Promise((_resolve, reject) => {
                  init.signal.addEventListener('abort', () => {
                    window.__firstDuplicateAborted = true;
                    reject(new DOMException('Aborted', 'AbortError'));
                  }, {once: true});
                });
              }
              const frame = 'data: ' + JSON.stringify({
                duplicates: [], recovered: [], checked: 1, total: 1,
              }) + '\\n\\n' + 'data: ' + JSON.stringify({
                done: true, duplicate_count: 0, recovered_count: 0,
                checked: 1, total: 1,
              }) + '\\n\\n';
              return Promise.resolve(new Response(frame, {
                status: 200,
                headers: {'Content-Type': 'text/event-stream'},
              }));
            }
            if (target && target.indexOf('/api/import/destination-preview') === 0) {
              return Promise.resolve(new Response(JSON.stringify({folders: []}), {
                status: 200,
                headers: {'Content-Type': 'application/json'},
              }));
            }
            return originalFetch(input, init);
          };
          addSourcePath('/tmp/card-a');
          clearScheduledImportPreview();
        }
        """
    )

    page.locator("#btnPreview").click()
    page.wait_for_function("window.__duplicateFetchCount === 1")
    page.evaluate("() => { void previewImport(); }")

    page.wait_for_function("window.__firstDuplicateAborted === true")
    page.wait_for_function("window.__duplicateFetchCount === 2")
    expect(page.locator("#btnPreview")).to_be_enabled()
    expect(page.locator("#importError")).to_have_text("")


def test_removing_last_source_aborts_in_flight_duplicate_stream(
    live_server, page
):
    """Removing the last source while a preview is running must stop the
    server-side hash/EXIF work.

    ``scheduleImportPreview()`` short-circuits on an empty source list, so
    no successor ``previewImport()`` fires — and its own abort (which
    lives at the top of ``previewImport``) never runs. Before the fix, the
    UI cleared but the request kept scanning the removed card. The
    scheduler now aborts the in-flight controller from the short-circuit
    path itself.
    """
    page.goto(f"{live_server['url']}/import")
    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/archive")
    page.evaluate(
        """
        () => {
          clearScheduledImportPreview();
          const originalFetch = window.fetch.bind(window);
          window.__duplicateAborted = false;
          window.__duplicateStarted = false;
          window.fetch = (input, init = {}) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(window.__previewDoneResponse({
                files: [{path: '/tmp/card-a/IMG_0001.jpg'}],
              }));
            }
            if (target && target.indexOf('/api/import/check-duplicates') === 0) {
              window.__duplicateStarted = true;
              return new Promise((_resolve, reject) => {
                init.signal.addEventListener('abort', () => {
                  window.__duplicateAborted = true;
                  reject(new DOMException('Aborted', 'AbortError'));
                }, {once: true});
              });
            }
            if (target && target.indexOf('/api/import/destination-preview') === 0) {
              return Promise.resolve(new Response(JSON.stringify({folders: []}), {
                status: 200,
                headers: {'Content-Type': 'application/json'},
              }));
            }
            return originalFetch(input, init);
          };
          addSourcePath('/tmp/card-a');
          clearScheduledImportPreview();
        }
        """
    )

    page.locator("#btnPreview").click()
    page.wait_for_function("window.__duplicateStarted === true")
    # Drop the last source (mirrors the user clicking × on the only card
    # while the duplicate stream is still running). Then invoke the same
    # scheduler the click handler does — that is the code path the
    # short-circuit lives in.
    page.evaluate(
        """
        () => {
          sources.splice(0, sources.length);
          scheduleImportPreview();
        }
        """
    )
    page.wait_for_function("window.__duplicateAborted === true")


def test_re_adding_the_last_source_after_abort_does_not_reuse_stale_paths(
    live_server, page
):
    """Empty-source abort must retire the captured preview too, not just
    the flight flags.

    Removing the last source mid-stream aborts the in-flight request and
    clears importPreviewInFlight / importDupStreamPending / importPreviewSeq.
    Without this fix it left importPreviewCapturedSignature and
    importPreviewedPaths behind: re-adding the same source inside the 350ms
    debounce restored a matching signature over an empty grid, so
    updateStartGate() found no reason to hold Start and lit it up as
    "Start import (0 files)". Clicking that button then posted the
    previously discovered paths as include_paths, silently importing every
    file behind an empty screen.
    """
    page.goto(f"{live_server['url']}/import")
    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/archive")
    page.evaluate(
        """
        () => {
          clearScheduledImportPreview();
          const originalFetch = window.fetch.bind(window);
          window.__duplicateStarted = false;
          window.fetch = (input, init = {}) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(window.__previewDoneResponse({
                files: [{path: '/tmp/card-a/IMG_0001.jpg'},
                        {path: '/tmp/card-a/IMG_0002.jpg'}],
              }));
            }
            if (target && target.indexOf('/api/import/check-duplicates') === 0) {
              window.__duplicateStarted = true;
              return new Promise((_resolve, reject) => {
                init.signal.addEventListener('abort', () => {
                  reject(new DOMException('Aborted', 'AbortError'));
                }, {once: true});
              });
            }
            if (target && target.indexOf('/api/import/destination-preview') === 0) {
              return Promise.resolve(new Response(JSON.stringify({folders: []}), {
                status: 200,
                headers: {'Content-Type': 'application/json'},
              }));
            }
            return originalFetch(input, init);
          };
          addSourcePath('/tmp/card-a');
          clearScheduledImportPreview();
        }
        """
    )

    page.locator("#btnPreview").click()
    page.wait_for_function("window.__duplicateStarted === true")
    # previewImport() captured the signature and previewed paths before
    # the duplicate stream started. The bug hinges on those still being
    # set at this point.
    page.wait_for_function(
        "() => importPreviewCapturedSignature !== null "
        "&& importPreviewedPaths.length > 0"
    )
    # Remove the last source mid-stream. This is the empty-source abort
    # branch inside scheduleImportPreview().
    page.evaluate(
        """
        () => {
          sources.splice(0, sources.length);
          scheduleImportPreview();
        }
        """
    )
    # The fix retires the captured preview alongside the flight flags.
    page.wait_for_function(
        "() => importPreviewCapturedSignature === null "
        "&& importPreviewedPaths.length === 0"
    )
    # Re-add the same source inside the 350ms debounce (no timer fired
    # yet — clearScheduledImportPreview keeps it that way). Before the
    # fix, updateStartGate() would light up Start with "0 files" here.
    page.evaluate(
        """
        () => {
          addSourcePath('/tmp/card-a');
          clearScheduledImportPreview();
        }
        """
    )
    # Start must NOT read "Start import (0 files)" — the previously
    # captured paths must not be resurrected against an empty grid.
    expect(page.locator("#btnStart")).not_to_have_text("Start import (0 files)")


def test_import_preview_older_response_does_not_clobber_newer_summary(
    live_server, page
):
    # If an older previewImport() response arrives after a newer one has
    # already written the summary text, the older run's stale-signature
    # branch used to clear #previewSummary — erasing the newer run's
    # results. The importPreviewSeq guard makes the older run bail
    # without touching summary.
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          // First call to folder-preview stalls forever until we resolve
          // it manually; subsequent calls resolve immediately.
          window.__resolveOldPreview = null;
          window.__folderPreviewCallCount = 0;
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              window.__folderPreviewCallCount += 1;
              const payload = window.__previewDoneResponse({
                files: [{path: '/tmp/card-a/IMG_0001.jpg'}],
              });
              if (window.__folderPreviewCallCount === 1) {
                return new Promise((resolve) => {
                  window.__resolveOldPreview = () => resolve(payload);
                });
              }
              return Promise.resolve(payload);
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
              return Promise.resolve(new Response(JSON.stringify({
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
            }
            return originalFetch(input, init);
          };
          addSourcePath('/tmp/card-a');
        }
        """
    )

    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/archive")
    # Fire preview #1 — its folder-preview fetch will hang.
    page.locator("#btnPreview").click()
    page.wait_for_function("window.__resolveOldPreview !== null")
    # Change a control that would flip the stale-signature branch, then
    # fire preview #2 — it resolves immediately and renders results.
    page.locator("#chkVerifyByHash").check()
    page.locator("#btnPreview").click()
    expect(page.locator("#previewSummary")).to_contain_text("1 to copy")
    # Now let the older, in-flight preview response come back. Its
    # signature no longer matches; the OLD code would clear the summary
    # here, wiping preview #2's rendered result.
    page.evaluate("() => window.__resolveOldPreview()")
    page.wait_for_timeout(100)
    expect(page.locator("#previewSummary")).to_contain_text("1 to copy")


def test_import_dest_structure_invalidation_survives_slow_startup(live_server, page):
    # initImportPage() awaits /api/volumes, /api/config, and
    # /api/workspaces/active before the rest of setup runs. If the
    # destination-structure invalidation listeners are wired after those
    # awaits, a user can render a structure preview and edit destInput
    # while startup is still blocked, leaving the stale structure
    # visible. Wiring the listeners synchronously (before any await)
    # closes that gap.
    url = live_server["url"]
    # Stall /api/volumes forever so initImportPage() never gets past its
    # first await. The listeners must already be installed by the time
    # the page becomes interactive.
    def volumes(route):
        # Never fulfill — playwright's route will hang the request.
        pass

    page.route("**/api/volumes", volumes)
    page.goto(f"{url}/import", wait_until="domcontentloaded")
    # Wait until initImportPage() has at least started so its synchronous
    # prefix (including wireDestStructureInvalidation) has run.
    page.wait_for_function(
        "() => typeof wireDestStructureInvalidation === 'function'"
    )
    # Fake a rendered destination structure — bypass the full preview
    # flow, since /api/volumes is stalled and the button pipeline would
    # await other startup state too. The invalidation contract is:
    # editing any of the wired controls hides #destStructure.
    page.evaluate(
        """
        () => {
          const el = document.getElementById('destStructure');
          el.innerHTML = '<div>fake structure</div>';
          el.style.display = '';
        }
        """
    )
    expect(page.locator("#destStructure")).to_be_visible()
    # Edit destInput — this is one of the controls wireDestStructureInvalidation
    # listens on. If the listener wasn't installed yet, the structure
    # would stay visible.
    page.locator("#destInput").fill("/archive")
    expect(page.locator("#destStructure")).to_be_hidden()


def test_import_remote_targets_load_while_readiness_is_slow(live_server, page):
    """A catalog-wide metadata check must not hide configured SSH targets.

    Import readiness can take minutes for a large library.  The destination
    picker is independent of that check, so populate it while readiness is
    still in flight instead of leaving only the static local-path option.
    """
    url = live_server["url"]

    def readiness(route):
        # Never fulfill: this models the slow catalog scan without blocking
        # Playwright's event loop, so independent requests can still finish.
        pass

    def remote_targets(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "rsync_available": True,
                "ssh_available": True,
                "targets": [{
                    "id": "nas1",
                    "name": "Photo NAS",
                    "user": "photo",
                    "host": "nas.local",
                    "remote_path": "/volume1/Photography",
                    "mount_path": "/Volumes/Photography",
                }],
            }),
        )

    page.route("**/api/import/readiness", readiness)
    page.route("**/api/remote-targets", remote_targets)
    with page.expect_request("**/api/import/readiness") as readiness_request:
        page.goto(f"{url}/import", wait_until="domcontentloaded")

    assert readiness_request.value.url.endswith("/api/import/readiness")
    expect(page.locator("#destMode option")).to_have_count(2)
    expect(page.locator("#destMode option").nth(1)).to_have_text(
        "Photo NAS — direct transfer over SSH"
    )


def test_import_remote_destination_requires_visible_folder_inside_nas(
    live_server, page,
):
    """The remote root alone is not a complete destination. Explain the
    missing folder beside the field and keep Start unavailable until it is
    valid, instead of hiding a tiny error below the thumbnail grid."""
    url = live_server["url"]

    def remote_targets(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "rsync_available": True,
                "ssh_available": True,
                "targets": [{
                    "id": "nas1",
                    "name": "Photo NAS",
                    "user": "photo",
                    "host": "nas.local",
                    "remote_path": "/volume1/Photography",
                    "mount_path": "/Volumes/Photography",
                }],
            }),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destMode").select_option("remote:nas1")

    inline_error = page.locator("#remoteSubpathError")
    expect(inline_error).to_be_visible()
    expect(inline_error).to_contain_text(
        "Enter the folder inside /volume1/Photography"
    )
    expect(page.locator("#btnStart")).to_be_disabled()
    assert page.evaluate("resolvedCopyDestination()") == ""

    page.locator("#remoteSubpath").fill("/Raw Files/USA")
    expect(inline_error).to_contain_text("Subpath must be relative")
    expect(page.locator("#btnStart")).to_be_disabled()

    page.locator("#remoteSubpath").fill("Raw Files/USA")
    expect(inline_error).to_be_hidden()
    assert page.evaluate("resolvedCopyDestination()") == (
        "/Volumes/Photography/Raw Files/USA"
    )


def test_failed_import_result_offers_preconfigured_retry(live_server, page):
    """A mixed import can retry from its result without rebuilding the form;
    the parent's duplicate-skip setting is preserved, and the parent's
    already-imported photo IDs plus its own job ID are carried on the
    retry request so the after-import chain covers the complete original
    scope."""
    url = live_server["url"]
    captured = {}

    def start_import(route):
        captured["body"] = json.loads(route.request.post_data or "{}")
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"job_id": "import-retry"}),
        )

    page.route("**/api/jobs/import-photos", start_import)
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          lastFinishedImportJob = {
            id: 'import-original',
            type: 'import',
            status: 'failed',
            config: {
              sources: ['/Volumes/CARD/DCIM'],
              destination: '/Volumes/Photography/Raw Files/USA',
              recursive: true,
              folder_template: '%Y/%Y-%m-%d',
              file_types: 'both',
              skip_duplicates: false,
              verify_by_hash: false,
              trust_likely_duplicates: true,
              after_import: 1,
              tags: ['trip'],
              location_from_gps: true,
              allow_missing_exiftool: false,
              remote_target_id: null,
              remote_subpath: null,
            },
            result: {
              photo_ids: [101, 102, 103],
              failed: 1,
            },
          };
          renderResult({
            discovered: 985,
            copied: 984,
            verified: 984,
            skipped_duplicate: 0,
            failed: 1,
            safe_to_format: false,
            unsafe_files: [{
              path: '/Volumes/CARD/DCIM/DSC_7172.NEF',
              reason: 'copy verification failed',
            }],
            folders: {},
            errors: ['copy verification failed'],
          }, 'failed');
        }
        """
    )

    retry = page.locator("#btnRetryImport")
    expect(retry).to_be_visible()
    expect(retry).to_have_text("Retry 1 failed file")
    retry.click()
    expect(page.locator("#progressCard")).to_be_visible()

    body = captured["body"]
    assert body["sources"] == ["/Volumes/CARD/DCIM"]
    assert body["destination"] == "/Volumes/Photography/Raw Files/USA"
    assert body["folder_template"] == "%Y/%Y-%m-%d"
    # retryBodyFromFinishedJob() preserves the parent's skip_duplicates
    # setting rather than forcing it on, so a parent configured with
    # dedup OFF retries with dedup OFF — otherwise the very file that
    # failed could be silently skipped if it happens to match some
    # unrelated catalog entry. The parent above seeds `false`, so the
    # retry must send `false`.
    assert body["skip_duplicates"] is False
    assert body["after_import"] == 1
    assert body["tags"] == ["trip"]
    # The original run's successful photos must be carried into the retry
    # so the after-import chain covers the complete import scope, not
    # only the newly-recovered file. Without this, the failed original
    # (which skipped chaining because it wasn't OK) plus the retry
    # (which chains on only 1 new photo) silently leave 984 photos
    # unprocessed.
    assert body["carry_photo_ids"] == [101, 102, 103]
    # And it must bind the carry list to the parent so the server can
    # verify each carry ID actually came from that parent — without
    # this binding an API caller could inject arbitrary IDs into the
    # after-import chain scope.
    assert body["parent_import_job_id"] == "import-original"
    # A whole-folder parent has no ``include_paths`` on its config, so
    # the retry body must not fabricate one either — the endpoint's
    # selection gate is conjunctive and would 400 on a stray
    # ``include_paths`` sent without both counts. Same for the two
    # counts.
    assert "include_paths" not in body
    assert "previewed_count" not in body
    assert "checked_count" not in body


def test_failed_import_retry_preserves_parent_include_paths(live_server, page):
    """When the failed parent import was a per-file selection, the retry
    request must echo the parent's ``include_paths`` plus its two count
    fields. Without this the retry either fails the source-drift
    check or, past that, silently re-imports the files the user
    deliberately deselected on the parent run — the "Retry failed files"
    button then quietly widens scope."""
    url = live_server["url"]
    captured = {}

    def start_import(route):
        captured["body"] = json.loads(route.request.post_data or "{}")
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"job_id": "import-retry"}),
        )

    page.route("**/api/jobs/import-photos", start_import)
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          lastFinishedImportJob = {
            id: 'import-original',
            type: 'import',
            status: 'failed',
            config: {
              sources: ['/Volumes/CARD/DCIM'],
              destination: '/Volumes/Photography/Raw Files/USA',
              recursive: true,
              folder_template: '%Y/%Y-%m-%d',
              file_types: 'both',
              skip_duplicates: true,
              verify_by_hash: false,
              trust_likely_duplicates: false,
              after_import: null,
              tags: [],
              location_from_gps: false,
              allow_missing_exiftool: false,
              remote_target_id: null,
              remote_subpath: null,
              include_paths: [
                '/Volumes/CARD/DCIM/DSC_0001.NEF',
                '/Volumes/CARD/DCIM/DSC_0002.NEF',
              ],
              previewed_count: 5,
              checked_count: 2,
            },
            result: {
              photo_ids: [201],
              failed: 1,
            },
          };
          renderResult({
            discovered: 5,
            copied: 1,
            verified: 1,
            skipped_duplicate: 0,
            failed: 1,
            safe_to_format: false,
            unsafe_files: [{
              path: '/Volumes/CARD/DCIM/DSC_0002.NEF',
              reason: 'copy verification failed',
            }],
            folders: {},
            errors: ['copy verification failed'],
          }, 'failed');
        }
        """
    )

    page.locator("#btnRetryImport").click()
    expect(page.locator("#progressCard")).to_be_visible()

    body = captured["body"]
    # The three selection fields must travel together — the server 400s
    # on a partial set — and the retry must carry the parent's exact
    # selection so it stays scoped to the same files.
    assert body["include_paths"] == [
        "/Volumes/CARD/DCIM/DSC_0001.NEF",
        "/Volumes/CARD/DCIM/DSC_0002.NEF",
    ]
    assert body["previewed_count"] == 5
    assert body["checked_count"] == 2


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
    _suppress_auto_preview(page)

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

    # Guards _suppress_auto_preview(): if the debounce ever escapes it, a
    # landed zero-file preview disables Start, and this reports that
    # directly instead of as a bare actionability timeout.
    expect(page.locator("#btnStart")).to_be_enabled()
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
    # previously-active workspace's pipeline.default_process_id.
    assert "after_import" not in body


def test_import_start_sends_common_tags_and_gps_location_option(
    live_server, page,
):
    url = live_server["url"]
    captured = {}

    def config_route(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "google_maps_api_key": "configured-for-test",
                "pipeline": {"default_process_id": None},
            }),
        )

    def start_import(route):
        captured["body"] = json.loads(route.request.post_data or "{}")
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"job_id": "import-tags-test"}),
        )

    page.route("**/api/config", config_route)
    page.route("**/api/jobs/import-in-place", start_import)
    page.goto(f"{url}/import")

    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#importTagInput").fill("Kenya trip")
    page.locator("#importTagInput").press("Enter")
    page.locator("#importTagInput").fill("Portfolio")
    page.locator("#btnAddImportTag").click()
    expect(page.locator("#importTagList .import-tag-chip")).to_have_count(2)
    page.locator("#chkLocationFromGps").check()

    page.locator("#btnStart").click()
    expect(page.locator("#progressCard")).to_be_visible()

    assert captured["body"]["tags"] == ["Kenya trip", "Portfolio"]
    assert captured["body"]["location_from_gps"] is True


def test_import_gps_location_option_explains_missing_api_key(live_server, page):
    url = live_server["url"]

    def config_route(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "google_maps_api_key": "",
                "pipeline": {"default_process_id": None},
            }),
        )

    page.route("**/api/config", config_route)
    page.goto(f"{url}/import")

    expect(page.locator("#chkLocationFromGps")).to_be_disabled()
    expect(page.locator("#locationGpsHint")).to_contain_text(
        "Add a Google Maps API key in Settings"
    )


def test_import_new_workspace_forwards_explicit_after_import(live_server, page):
    """When the user actively picks a saved process for a new-workspace
    import, the client must forward that pick as a process id — only the
    untouched-dropdown case is omitted so the server can apply the new
    workspace's default."""
    url = live_server["url"]
    db = live_server["db"]
    identify_id = next(
        p["id"] for p in db.get_saved_processes() if p["name"] == "Identify birds"
    )
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
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#workspaceNew").check()
    page.locator("#newWorkspaceName").fill("Serengeti")
    page.locator("#destInput").fill("/tmp/archive")
    page.locator("#afterImportSelect").select_option(str(identify_id))

    # Guards _suppress_auto_preview(): if the debounce ever escapes it, a
    # landed zero-file preview disables Start, and this reports that
    # directly instead of as a bare actionability timeout.
    expect(page.locator("#btnStart")).to_be_enabled()
    page.locator("#btnStart").click()
    expect(page.locator("#progressCard")).to_be_visible()

    body = captured["body"]
    assert body["new_workspace_name"] == "Serengeti"
    assert body["after_import"] == identify_id


def test_mounted_nas_offers_managed_local_processing(live_server, page):
    identify_id = next(p["id"] for p in live_server["db"].get_saved_processes()
                       if p["name"] == "Identify birds")
    page.route("**/api/remote-targets", lambda route: route.fulfill(
        status=200, content_type="application/json", body=json.dumps({
            "rsync_available": False, "ssh_available": False,
            "targets": [{"id": "nas1", "name": "Synology NAS", "host": "nas.local",
                         "user": "photo", "remote_path": "/volume1/Photography",
                         "mount_path": "/Volumes/Photography", "local_archive_root": ""}],
        }),
    ))
    page.goto(f"{live_server['url']}/import")
    _suppress_auto_preview(page)
    page.locator("#modeCopy").check()
    page.locator("#destInput").fill("/Volumes/Photography/Raw Files/USA/2026")
    page.locator("#afterImportSelect").select_option(str(identify_id))
    expect(page.locator("#localProcessingRow")).to_be_visible()
    expect(page.locator("#chkLocalProcessing")).to_be_checked()
    expect(page.locator("#afterMoveUnavailable")).to_be_hidden()
    expect(page.locator("#afterMoveRow")).to_be_hidden()
    expect(page.locator("#destModeHint")).to_contain_text("final NAS destination")
    assert page.evaluate("localProcessingRequest()") is True
    expect(page.locator("#chkDeferNasTransfer")).to_be_checked()
    assert page.evaluate("deferNasTransferRequest()") is True
    expect(page.locator("#destModeHint")).to_contain_text("until you choose Send to NAS")
    page.locator("#chkDeferNasTransfer").uncheck()
    assert page.evaluate("deferNasTransferRequest()") is False
    expect(page.locator("#destModeHint")).to_contain_text("then transferred here")
    page.locator("#chkDeferNasTransfer").check()
    assert page.evaluate("afterMoveRequest()") is None
    page.locator("#chkLocalProcessing").uncheck()
    assert page.evaluate("localProcessingRequest()") is False
    assert page.evaluate("deferNasTransferRequest()") is False
    expect(page.locator("#destModeHint")).to_contain_text("mounted volume")
    page.locator("#chkLocalProcessing").check()
    page.locator("#afterImportSelect").select_option("__none__")
    expect(page.locator("#localProcessingRow")).to_be_hidden()
    assert page.evaluate("localProcessingRequest()") is False
    page.locator("#afterImportSelect").select_option(str(identify_id))
    page.locator("#destInput").fill("/Users/me/Pictures")
    expect(page.locator("#localProcessingRow")).to_be_hidden()
    assert page.evaluate("localProcessingRequest()") is False


@pytest.mark.parametrize("path", ["/browse", "/import", "/jobs"])
def test_pending_archive_panel_sends_remembered_destination(live_server, page, path):
    state = {"sent": False}

    def pending(route):
        route.fulfill(status=200, content_type="application/json", body=json.dumps({
            "items": [] if state["sent"] else [{
                "id": "import-test", "name": "Photos from the coast", "collection_id": 123,
                "destination": "/Volumes/Photography/Coast", "state": "ready", "error": "",
            }],
        }))

    def send(route):
        assert route.request.method == "POST"
        state["sent"] = True
        route.fulfill(status=200, content_type="application/json", body='{"job_id":"send-to-nas-test"}')

    page.route("**/api/import/pending-archives", pending)
    page.route("**/api/import/pending-archives/import-test/send", send)
    page.goto(live_server["url"] + path)
    panel = page.locator("#pendingArchives")
    expect(panel).to_be_visible()
    expect(panel).to_contain_text("Photos from the coast")
    expect(panel).to_contain_text("/Volumes/Photography/Coast")
    expect(panel.get_by_role("link", name="Review photos")).to_have_attribute("href", "/browse?collection_id=123")
    page.reload()
    expect(panel).to_be_visible()
    panel.get_by_role("button", name="Send to NAS", exact=True).click()
    expect(panel).to_be_hidden()
    assert state["sent"]


def test_import_after_move_hint_blames_the_right_field(live_server, page):
    """Issue #1377: with a typo'd archive root (Vireo_Archive vs the real
    "Vireo Archive"), the old hint said the *destination* was outside every
    archive root, so the user re-typed a destination that was never the
    problem. The hint must name the configured root and say which of the
    three situations applies."""
    url = live_server["url"]
    db = live_server["db"]
    identify_id = next(
        p["id"] for p in db.get_saved_processes() if p["name"] == "Identify birds"
    )

    def remote_targets(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "rsync_available": True,
                "ssh_available": True,
                "targets": [{
                    "id": "nas1",
                    "name": "Photo NAS",
                    "user": "photo",
                    "host": "nas.local",
                    "remote_path": "/volume1/Photography",
                    "mount_path": "/Volumes/Photography",
                    "local_archive_root": "/Users/me/Pictures/Vireo_Archive",
                    "local_archive_root_present": False,
                }],
            }),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/Users/me/Pictures/Vireo Archive/2026")
    page.locator("#afterImportSelect").select_option(str(identify_id))

    hint = page.locator("#afterMoveUnavailable")
    row = page.locator("#afterMoveRow")

    # Typo'd root: the destination is lexically outside it, so this is the
    # "outside" case, but the missing root is annotated inline so the
    # underscore-vs-space mismatch is visible in one line.
    expect(hint).to_be_visible()
    expect(hint).to_contain_text("the destination is outside the local archive root of")
    expect(hint).to_contain_text(
        "Photo NAS (/Users/me/Pictures/Vireo_Archive — does not exist on this machine)"
    )
    expect(hint).not_to_contain_text("destination is not inside")
    expect(row).to_be_hidden()

    # Root exists but the destination genuinely is outside it: no annotation.
    page.evaluate(
        """
        () => {
          importRemoteTargets[0].local_archive_root_present = true;
          updateAfterMoveUI();
        }
        """
    )
    expect(hint).to_contain_text("the destination is outside the local archive root of")
    expect(hint).to_contain_text("Photo NAS (/Users/me/Pictures/Vireo_Archive)")
    expect(hint).not_to_contain_text("does not exist")

    # A second target with a missing root that does not contain the
    # destination is listed, but the user is not told to create it.
    page.evaluate(
        """
        () => {
          importRemoteTargets.push({
            id: "nas2", name: "Backup NAS", user: "photo", host: "backup.local",
            remote_path: "/volume1/Backup", mount_path: "/Volumes/Backup",
            local_archive_root: "/archive-b", local_archive_root_present: false,
          });
          updateAfterMoveUI();
        }
        """
    )
    expect(hint).to_contain_text("the destination is outside the local archive root of")
    expect(hint).to_contain_text("Backup NAS (/archive-b — does not exist on this machine)")
    expect(hint).not_to_contain_text("Create it")
    page.evaluate("() => { importRemoteTargets.pop(); }")

    # No target has a root at all.
    page.evaluate(
        """
        () => {
          importRemoteTargets[0].local_archive_root = "";
          updateAfterMoveUI();
        }
        """
    )
    expect(hint).to_contain_text("no remote target has a local archive root configured")

    # Destination inside a root that does not exist yet: creating the root
    # is the fix, and the prefix match alone must not show the move row.
    page.evaluate(
        """
        () => {
          importRemoteTargets[0].local_archive_root = "/Users/me/Pictures/Vireo Archive";
          importRemoteTargets[0].local_archive_root_present = false;
          updateAfterMoveUI();
        }
        """
    )
    expect(hint).to_be_visible()
    expect(hint).to_contain_text(
        "the local archive root for Photo NAS (/Users/me/Pictures/Vireo Archive "
        "— does not exist on this machine). Create it"
    )
    expect(row).to_be_hidden()

    # Once the root exists, the same destination makes the row appear.
    page.evaluate(
        """
        () => {
          importRemoteTargets[0].local_archive_root_present = true;
          updateAfterMoveUI();
        }
        """
    )
    expect(hint).to_be_hidden()
    expect(row).to_be_visible()

    # The root's volume dropped (external drive / share): the server could
    # not probe it, so the gate says so instead of guessing.
    page.evaluate(
        """
        () => {
          importRemoteTargets[0].local_archive_root_present = null;
          importRemoteTargets[0].local_archive_root_volume_offline = true;
          updateAfterMoveUI();
        }
        """
    )
    expect(hint).to_contain_text(
        "the volume holding the local archive root for Photo NAS "
        "(/Users/me/Pictures/Vireo Archive — volume not reachable right now)"
    )
    expect(row).to_be_hidden()


def test_import_after_move_recovers_when_archive_root_is_created(
    live_server, page,
):
    """Archive-root presence is fetched with the target list; if the user
    creates the folder mid-session (Finder, the Settings "Create folder"
    button), the gate must re-check instead of insisting the root is
    missing until a full reload."""
    url = live_server["url"]
    db = live_server["db"]
    identify_id = next(
        p["id"] for p in db.get_saved_processes() if p["name"] == "Identify birds"
    )
    state = {"present": False, "fetches": 0}

    def remote_targets(route):
        state["fetches"] += 1
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "rsync_available": True,
                "ssh_available": True,
                "targets": [{
                    "id": "nas1",
                    "name": "Photo NAS",
                    "user": "photo",
                    "host": "nas.local",
                    "remote_path": "/volume1/Photography",
                    "mount_path": "/Volumes/Photography",
                    "local_archive_root": "/Users/me/Pictures/Vireo Archive",
                    "local_archive_root_present": state["present"],
                }],
            }),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/Users/me/Pictures/Vireo Archive/2026")
    page.locator("#afterImportSelect").select_option(str(identify_id))

    hint = page.locator("#afterMoveUnavailable")
    row = page.locator("#afterMoveRow")
    expect(hint).to_contain_text("does not exist on this machine")
    expect(row).to_be_hidden()

    # The folder now exists on disk. Touch the destination once, like the
    # native picker does — a single input event, no reload. This lands
    # inside the refresh throttle window that started at page load, which
    # is exactly the case where the one trigger must not be dropped: the
    # refresh is deferred to the end of the window, not skipped.
    state["present"] = True
    page.locator("#destInput").dispatch_event("input")

    expect(row).to_be_visible(timeout=10000)
    expect(hint).to_be_hidden()
    assert state["fetches"] >= 2


def test_import_after_move_recovers_when_archive_root_is_corrected(
    live_server, page,
):
    """The hint sends the user to Settings to fix a typo'd root. Doing so
    in another tab must be picked up by the still-open Import page when
    the user comes back to it — even for a legacy target whose id changes
    when Settings re-saves it, and without touching the form again."""
    url = live_server["url"]
    db = live_server["db"]
    identify_id = next(
        p["id"] for p in db.get_saved_processes() if p["name"] == "Identify birds"
    )
    state = {
        "id": "photo@nas.local:/volume1/Photography",
        "root": "/Users/me/Pictures/Vireo_Archive",
        "present": False,
    }

    def remote_targets(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "rsync_available": True,
                "ssh_available": True,
                "targets": [{
                    "id": state["id"],
                    "name": "Photo NAS",
                    "user": "photo",
                    "host": "nas.local",
                    "remote_path": "/volume1/Photography",
                    "mount_path": "/Volumes/Photography",
                    "local_archive_root": state["root"],
                    "local_archive_root_present": state["present"],
                }],
            }),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/Users/me/Pictures/Vireo Archive/2026")
    page.locator("#afterImportSelect").select_option(str(identify_id))

    hint = page.locator("#afterMoveUnavailable")
    row = page.locator("#afterMoveRow")
    expect(hint).to_contain_text("Vireo_Archive — does not exist on this machine")
    expect(row).to_be_hidden()

    # Let the refresh that the hint triggered settle on the unchanged list,
    # so what follows is not riding on that first fetch.
    page.wait_for_function("() => _afterMoveTargetsCheckedAt > 0")

    # The user fixes the underscore in Settings (another tab). Settings
    # re-saves the legacy entry under a generated id. They come back to
    # this tab without touching the form.
    state["id"] = "rt-9f3a"
    state["root"] = "/Users/me/Pictures/Vireo Archive"
    state["present"] = True
    page.evaluate("() => document.dispatchEvent(new Event('visibilitychange'))")

    expect(row).to_be_visible(timeout=10000)
    expect(hint).to_be_hidden()
    # The dropdown was rebuilt with the new id and the selection kept.
    expect(page.locator("#destMode option")).to_have_count(2)
    assert page.locator("#destMode").input_value() == "local"


def test_import_after_move_refresh_recovers_failed_initial_target_load(
    live_server, page,
):
    """If the initial /api/remote-targets request fails but a later
    background refresh succeeds, that success is a full recovery: the
    SSH destinations come back, the rsync/ssh availability flags are
    applied (so picking one is not falsely blocked), and the load-error
    banner clears."""
    url = live_server["url"]
    db = live_server["db"]
    identify_id = next(
        p["id"] for p in db.get_saved_processes() if p["name"] == "Identify birds"
    )
    state = {"calls": 0}

    def remote_targets(route):
        state["calls"] += 1
        if state["calls"] == 1:
            route.fulfill(status=500, content_type="text/plain", body="boom")
            return
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "rsync_available": True,
                "ssh_available": True,
                "targets": [{
                    "id": "nas1",
                    "name": "Photo NAS",
                    "user": "photo",
                    "host": "nas.local",
                    "remote_path": "/volume1/Photography",
                    "mount_path": "/Volumes/Photography",
                    "local_archive_root": "/Users/me/Pictures/Vireo Archive",
                    "local_archive_root_present": True,
                }],
            }),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    banner = page.locator("#remoteTargetsError")
    expect(banner).to_be_visible()
    expect(page.locator("#destMode option")).to_have_count(1)

    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/Users/me/Pictures/Vireo Archive/2026")
    page.locator("#afterImportSelect").select_option(str(identify_id))

    # The hint renders (no targets known), which triggers the background
    # refresh; the deferred refresh succeeds and recovers everything.
    expect(page.locator("#afterMoveRow")).to_be_visible(timeout=10000)
    expect(banner).to_be_hidden()
    expect(page.locator("#destMode option")).to_have_count(2)
    assert page.evaluate("importRsyncAvailable && importSshAvailable")


def test_import_after_move_drops_eligibility_when_root_changes_elsewhere(
    live_server, page,
):
    """A target eligible at page load must stop being offered when its
    archive root is changed in Settings (another tab) and the user comes
    back — otherwise Start posts an id the server rejects against the new
    root. The tab-return refresh applies even when the snapshot looks
    eligible."""
    url = live_server["url"]
    db = live_server["db"]
    identify_id = next(
        p["id"] for p in db.get_saved_processes() if p["name"] == "Identify birds"
    )
    state = {"root": "/Users/me/Pictures/Vireo Archive"}

    def remote_targets(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "rsync_available": True,
                "ssh_available": True,
                "targets": [{
                    "id": "nas1",
                    "name": "Photo NAS",
                    "user": "photo",
                    "host": "nas.local",
                    "remote_path": "/volume1/Photography",
                    "mount_path": "/Volumes/Photography",
                    "local_archive_root": state["root"],
                    "local_archive_root_present": True,
                }],
            }),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/Users/me/Pictures/Vireo Archive/2026")
    page.locator("#afterImportSelect").select_option(str(identify_id))

    hint = page.locator("#afterMoveUnavailable")
    row = page.locator("#afterMoveRow")
    expect(row).to_be_visible()
    expect(hint).to_be_hidden()

    # Root moved in Settings; user returns to this tab without editing.
    state["root"] = "/Users/me/Pictures/Staging"
    page.evaluate("() => document.dispatchEvent(new Event('visibilitychange'))")

    expect(row).to_be_hidden(timeout=10000)
    expect(hint).to_contain_text(
        "the destination is outside the local archive root of "
        "Photo NAS (/Users/me/Pictures/Staging)"
    )


def test_import_remote_selection_survives_target_refresh(live_server, page):
    """A background refresh replaces the target list. A selected SSH
    destination must follow its target across a legacy id change (matched
    by user/host/remote_path), transfer-defining fields must be refreshed
    even when the after-move gate fields did not change, and a target that
    disappeared must fall back to Local *visibly*."""
    url = live_server["url"]
    state = {
        "id": "photo@nas.local:/volume1/Photography",
        "remote_path": "/volume1/Photography",
        "targets": True,
    }

    def remote_targets(route):
        targets = [] if not state["targets"] else [{
            "id": state["id"],
            "name": "Photo NAS",
            "user": "photo",
            "host": "nas.local",
            "remote_path": state["remote_path"],
            "mount_path": "/Volumes/Photography",
            "local_archive_root": "",
            "local_archive_root_present": None,
        }]
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "rsync_available": True,
                "ssh_available": True,
                "targets": targets,
            }),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destMode").select_option("remote:" + state["id"])
    page.locator("#remoteSubpath").fill("2026/kenya")
    expect(page.locator("#remoteSubpathError")).to_be_hidden()

    # Settings re-saved the legacy entry under a generated id; user, host
    # and remote path are unchanged, so the match is unambiguous.
    state["id"] = "rt-9f3a"
    page.evaluate("() => document.dispatchEvent(new Event('visibilitychange'))")

    expect(page.locator("#destMode")).to_have_value("remote:rt-9f3a", timeout=10000)
    assert page.evaluate("resolvedCopyDestination()") == (
        "/Volumes/Photography/2026/kenya"
    )
    expect(page.locator("#remoteTargetsError")).to_be_hidden()

    # A field the after-move gate never reads changes under the same id:
    # the page's copy must still be refreshed (Start posts only the id and
    # the server will use the new value).
    state["remote_path"] = "/volume1/Photos"
    page.evaluate("() => { _afterMoveTargetsCheckedAt = 0; }")
    page.evaluate("() => document.dispatchEvent(new Event('visibilitychange'))")
    page.wait_for_function(
        "() => importRemoteTargets[0].remote_path === '/volume1/Photos'",
        timeout=10000,
    )
    expect(page.locator("#destMode")).to_have_value("remote:rt-9f3a")

    # The selected target is replaced by a *different* one on the same
    # host (deleted + re-added with another remote path): that is not a
    # migration. Fall back to Local, and say so.
    state["id"] = "rt-0000"
    state["remote_path"] = "/volume1/Other"
    page.evaluate("() => { _afterMoveTargetsCheckedAt = 0; }")
    page.evaluate("() => document.dispatchEvent(new Event('visibilitychange'))")
    expect(page.locator("#destMode")).to_have_value("local", timeout=10000)
    banner = page.locator("#remoteTargetsError")
    expect(banner).to_be_visible()
    expect(banner).to_contain_text('"Photo NAS" is no longer configured')


def test_import_after_move_target_pick_survives_refresh_or_unchecks(
    live_server, page,
):
    """With several eligible targets and "Then move to NAS" checked, a
    refresh that re-saves the picked target under a new id must keep the
    pick (unambiguous match), and one that removes it must uncheck the box
    with a note rather than silently move the post-processing move to a
    different NAS."""
    url = live_server["url"]
    db = live_server["db"]
    identify_id = next(
        p["id"] for p in db.get_saved_processes() if p["name"] == "Identify birds"
    )
    state = {"nas2_id": "photo@backup.local:/volume1/Backup", "nas2": True}

    def target(tid, name, host, remote_path, root):
        return {
            "id": tid, "name": name, "user": "photo", "host": host,
            "remote_path": remote_path, "mount_path": "/Volumes/" + name,
            "local_archive_root": root, "local_archive_root_present": True,
        }

    def remote_targets(route):
        targets = [target("nas1", "Photo NAS", "nas.local",
                          "/volume1/Photography", "/Users/me/Pictures")]
        if state["nas2"]:
            targets.append(target(state["nas2_id"], "Backup NAS", "backup.local",
                                  "/volume1/Backup",
                                  "/Users/me/Pictures/Vireo Archive"))
        route.fulfill(
            status=200, content_type="application/json",
            body=json.dumps({"rsync_available": True, "ssh_available": True,
                             "targets": targets}),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/Users/me/Pictures/Vireo Archive/2026")
    page.locator("#afterImportSelect").select_option(str(identify_id))

    sel = page.locator("#afterMoveTarget")
    chk = page.locator("#chkAfterMove")
    expect(sel.locator("option")).to_have_count(2)
    sel.select_option(state["nas2_id"])
    chk.check()
    expect(page.locator("#afterMovePreview")).to_contain_text("/volume1/Backup")

    # Legacy id migration for the picked target: pick follows it.
    state["nas2_id"] = "rt-b4ck"
    page.evaluate("() => { _afterMoveTargetsCheckedAt = 0; }")
    page.evaluate("() => document.dispatchEvent(new Event('visibilitychange'))")
    expect(sel).to_have_value("rt-b4ck", timeout=10000)
    expect(chk).to_be_checked()
    expect(page.locator("#afterMovePreview")).to_contain_text("/volume1/Backup")

    # The picked target is deleted: another eligible target remains, but
    # the move must not silently retarget to it.
    state["nas2"] = False
    page.evaluate("() => { _afterMoveTargetsCheckedAt = 0; }")
    page.evaluate("() => document.dispatchEvent(new Event('visibilitychange'))")
    expect(chk).not_to_be_checked(timeout=10000)
    note = page.locator("#afterMoveUnavailable")
    # Visible, not merely present: the refresh re-renders twice and the
    # second pass must not hide the only explanation of the state change.
    expect(note).to_be_visible()
    expect(note).to_contain_text(
        '"Then move to NAS" was unchecked: Backup NAS is no longer available'
    )
    expect(page.locator("#afterMovePreview")).to_be_hidden()
    assert page.evaluate("afterMoveRequest()") is None
    # The note survives an unrelated re-render, and clears once the user
    # re-checks the box (acknowledging it) — the remaining target is then
    # offered normally.
    page.locator("#destInput").dispatch_event("input")
    expect(note).to_be_visible()
    chk.check()
    expect(note).to_be_hidden()
    expect(sel).to_have_value("nas1")
    expect(page.locator("#afterMovePreview")).to_contain_text("/volume1/Photography")


def test_import_remote_selection_does_not_migrate_to_preexisting_twin(
    live_server, page,
):
    """Two saved targets can share user/host/remote_path (different port or
    key). Deleting the selected one must not hand the selection to its
    pre-existing twin as if it were an id migration; a migration is a
    *new* id replacing the old one."""
    url = live_server["url"]
    state = {"keep_a": True}

    def target(tid, port):
        return {
            "id": tid, "name": "NAS " + tid, "user": "photo", "host": "nas.local",
            "port": port, "remote_path": "/volume1/Photography",
            "mount_path": "/Volumes/Photography",
            "local_archive_root": "", "local_archive_root_present": None,
        }

    def remote_targets(route):
        targets = ([target("a", 22)] if state["keep_a"] else []) + [target("b", 2222)]
        route.fulfill(
            status=200, content_type="application/json",
            body=json.dumps({"rsync_available": True, "ssh_available": True,
                             "targets": targets}),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destMode").select_option("remote:a")
    page.locator("#remoteSubpath").fill("2026/kenya")

    state["keep_a"] = False
    page.evaluate("() => document.dispatchEvent(new Event('visibilitychange'))")

    expect(page.locator("#destMode")).to_have_value("local", timeout=10000)
    banner = page.locator("#remoteTargetsError")
    expect(banner).to_be_visible()
    expect(banner).to_contain_text('"NAS a" is no longer configured')


def test_import_slow_initial_target_load_does_not_overwrite_newer_refresh(
    live_server, page,
):
    """The initial /api/remote-targets request and a background refresh
    can be in flight together. If the target changed in between and the
    older (initial) response lands last, it must be discarded rather than
    overwrite the newer snapshot the page already applied."""
    url = live_server["url"]
    db = live_server["db"]
    identify_id = next(
        p["id"] for p in db.get_saved_processes() if p["name"] == "Identify birds"
    )
    state = {"calls": 0, "held": None}

    def body(root):
        return json.dumps({
            "rsync_available": True,
            "ssh_available": True,
            "targets": [{
                "id": "nas1", "name": "Photo NAS", "user": "photo",
                "host": "nas.local", "remote_path": "/volume1/Photography",
                "mount_path": "/Volumes/Photography",
                "local_archive_root": root, "local_archive_root_present": True,
            }],
        })

    def remote_targets(route):
        state["calls"] += 1
        if state["calls"] == 1:
            # Hold the initial load; it will be answered (stale) later.
            state["held"] = route
            return
        route.fulfill(status=200, content_type="application/json",
                      body=body("/Users/me/Pictures/Vireo Archive"))

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/Users/me/Pictures/Vireo Archive/2026")
    page.locator("#afterImportSelect").select_option(str(identify_id))

    # The hint (no targets known yet) triggers the refresh, which completes
    # first with the current configuration.
    expect(page.locator("#afterMoveRow")).to_be_visible(timeout=10000)
    assert state["held"] is not None

    # Now the delayed initial response arrives with the OLD root.
    state["held"].fulfill(status=200, content_type="application/json",
                          body=body("/Users/me/Pictures/Vireo_Archive"))
    page.wait_for_timeout(700)

    assert page.evaluate("importRemoteTargets[0].local_archive_root") == (
        "/Users/me/Pictures/Vireo Archive"
    )
    expect(page.locator("#afterMoveRow")).to_be_visible()
    expect(page.locator("#afterMoveUnavailable")).to_be_hidden()


def test_import_after_move_destination_change_unchecks_instead_of_retargeting(
    live_server, page,
):
    """No refresh at all: two targets share user/host/remote_path with
    nested archive roots. The picked (inner) target stops being eligible
    when the destination moves up a level. The pick must not slide to the
    outer twin (same tuple, different port) — that is not a migration —
    the box unchecks with a note."""
    url = live_server["url"]
    db = live_server["db"]
    identify_id = next(
        p["id"] for p in db.get_saved_processes() if p["name"] == "Identify birds"
    )

    def target(tid, port, root):
        return {
            "id": tid, "name": "NAS " + tid, "user": "photo", "host": "nas.local",
            "port": port, "remote_path": "/volume1/Photography",
            "mount_path": "/Volumes/Photography",
            "local_archive_root": root, "local_archive_root_present": True,
        }

    def remote_targets(route):
        route.fulfill(
            status=200, content_type="application/json",
            body=json.dumps({"rsync_available": True, "ssh_available": True,
                             "targets": [
                                 target("outer", 22, "/Users/me/Pictures"),
                                 target("inner", 2222, "/Users/me/Pictures/Vireo Archive"),
                             ]}),
        )

    page.route("**/api/remote-targets", remote_targets)
    page.goto(f"{url}/import")
    _suppress_auto_preview(page)

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/Users/me/Pictures/Vireo Archive/2026")
    page.locator("#afterImportSelect").select_option(str(identify_id))

    sel = page.locator("#afterMoveTarget")
    chk = page.locator("#chkAfterMove")
    expect(sel.locator("option")).to_have_count(2)
    sel.select_option("inner")
    chk.check()

    # Destination moves out of the inner root; only the outer twin remains.
    page.locator("#destInput").fill("/Users/me/Pictures/2026")
    expect(chk).not_to_be_checked()
    note = page.locator("#afterMoveUnavailable")
    expect(note).to_be_visible()
    expect(note).to_contain_text(
        '"Then move to NAS" was unchecked: NAS inner is no longer available'
    )
    assert page.evaluate("afterMoveRequest()") is None


def test_import_new_workspace_shows_target_default_in_after_import_display(
    live_server, page
):
    """When 'New workspace' is picked and the After Import dropdown is
    untouched, the visible label must describe what the server will
    actually do — the global default that the freshly-created workspace
    inherits — rather than leaking the currently-active workspace's
    (possibly-overridden) prefilled selection."""
    url = live_server["url"]
    db = live_server["db"]
    procs_by_name = {p["name"]: p["id"] for p in db.get_saved_processes()}
    identify_id = procs_by_name["Identify birds"]
    quick_look_id = procs_by_name["Quick look"]

    def config_route(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "pipeline": {"default_process_id": identify_id},
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
                    "pipeline": {"default_process_id": quick_look_id},
                },
            }),
        )

    page.route("**/api/config", config_route)
    page.route("**/api/workspaces/active", active_workspace_route)
    page.goto(f"{url}/import")

    # Before touching workspaceNew, the dropdown reflects the CURRENT
    # workspace's default (Quick look) — the source of the misleading
    # signal that this fix addresses.
    expect(page.locator("#afterImportSelect")).to_have_value(str(quick_look_id))

    page.locator("#workspaceNew").check()

    # After switching to new-workspace mode, the visible selection swaps
    # to a placeholder that names the GLOBAL default (Identify birds) —
    # matching what the server will actually apply to the freshly-created
    # workspace.
    expect(page.locator("#afterImportSelect")).to_have_value("__hidden_default__")
    placeholder = page.locator("#afterImportHiddenDefault")
    expect(placeholder).to_contain_text("New workspace default")
    expect(placeholder).to_contain_text("Identify birds")

    # Switching back to current-workspace mode without touching the
    # dropdown restores the current workspace's default rather than
    # sticking on the placeholder.
    page.locator("#workspaceCurrent").check()
    expect(page.locator("#afterImportSelect")).to_have_value(str(quick_look_id))


def test_import_browse_button_opens_folder_browser_fallback(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")

    page.locator("[data-testid='import-source-browse-btn']").click()

    browser = page.locator("[data-testid='folder-browser']")
    expect(browser).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#folderBrowserTitle")).to_have_text("Select Source Folders")
    panel = browser.locator(".folder-browser-panel")
    expect(panel).to_have_attribute("role", "dialog")
    expect(panel).to_have_attribute("aria-modal", "true")
    expect(panel).to_have_attribute(
        "aria-labelledby", "folderBrowserTitle")


def test_import_folder_browser_shows_recursive_photo_counts(live_server, page):
    """Source picker rows show the recursive count returned for each folder."""
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target === '/api/browse/photo-counts') {
              const body = JSON.parse(init.body || '{}');
              window.__folderCountRequest = body;
              return Promise.resolve(new Response(JSON.stringify({
                counts: {
                  '/tmp/card-a': 1,
                  '/tmp/card-b': 1234,
                  '/tmp/empty': 0,
                },
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            if (target && target.indexOf('/api/browse') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                path: '/tmp',
                dirs: [
                  {name: 'card-a', path: '/tmp/card-a'},
                  {name: 'card-b', path: '/tmp/card-b'},
                  {name: 'empty', path: '/tmp/empty'},
                ],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()

    rows = page.locator("#folderBrowserList .folder-browser-item[data-folder-path]")
    expect(rows).to_have_count(3)
    expect(rows.nth(0).locator(".folder-browser-count")).to_have_text("1 photo")
    expect(rows.nth(1).locator(".folder-browser-count")).to_have_text("1,234 photos")
    expect(rows.nth(2).locator(".folder-browser-count")).to_be_empty()
    request = page.evaluate("window.__folderCountRequest")
    assert request["paths"] == ["/tmp/card-a", "/tmp/card-b", "/tmp/empty"]
    assert request["file_types"] == "both"


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
    expect(page.locator("[data-testid='folder-browser']")).to_have_class(
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
    browser = page.locator("[data-testid='folder-browser']")
    expect(browser).to_have_class(re.compile(r"\bopen\b"))
    expect(browser.locator(".folder-browser-close")).to_be_focused()

    page.keyboard.press("Escape")

    expect(browser).not_to_have_class(re.compile(r"\bopen\b"))


def test_import_destination_structure_hides_when_folder_browser_picks_destination(
    live_server, page
):
    # Selecting a destination via the folder browser assigns destInput.value
    # programmatically — input/change never fire — so the rendered structure
    # preview must be invalidated by the code path itself. Complements the
    # existing DOM-event and addSourcePath coverage with the fallback picker.
    url = live_server["url"]

    def folder_preview(route):
        _fulfill_preview_stream(
            route, {"files": [{"path": "/tmp/card-a/IMG_0001.jpg"}]})

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

    def browse(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "path": "/new-archive",
                "dirs": [{"name": "child", "path": "/new-archive/child"}],
            }),
        )

    page.route("**/api/import/folder-preview-stream", folder_preview)
    page.route("**/api/import/check-duplicates", check_duplicates)
    page.route("**/api/import/destination-preview", destination_preview)
    page.route("**/api/browse**", browse)
    page.goto(f"{url}/import")
    # Force the folder-browser fallback (no native picker) so the Select
    # button assigns destInput.value directly.
    page.evaluate("window.pickDirectory = async () => null")

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/archive")
    page.locator("#btnPreview").click()
    expect(page.locator("#destStructure")).to_be_visible()

    page.locator("[data-testid='import-destination-browse-btn']").click()
    expect(page.locator("[data-testid='folder-browser']")).to_have_class(
        re.compile(r"\bopen\b"))
    page.locator("#folderBrowserSelectBtn").click()

    expect(page.locator("#destInput")).to_have_value("/new-archive")
    expect(page.locator("#destStructure")).to_be_hidden()


def test_import_folder_template_examples_retire_with_their_source(
    live_server, page,
):
    """Removing the source must clear the folder-template examples.

    Each preset is labelled with an example folder name taken from the files
    being imported. Once those files are gone the example describes nothing —
    and a label that reads "this is what my folder will be called" must never
    outlive the data behind it. Removing the last source hides the structure
    table without re-running the destination preview, so the reset has to live
    on the shared invalidation path, not in the render.
    """
    url = live_server["url"]

    def folder_preview(route):
        _fulfill_preview_stream(
            route, {"files": [{"path": "/tmp/card-a/IMG_0001.jpg"}]})

    def check_duplicates(route):
        route.fulfill(
            status=200,
            content_type="text/event-stream",
            body="data: " + json.dumps({
                "done": True, "duplicate_count": 0, "checked": 1, "total": 1,
            }) + "\n\n",
        )

    def destination_preview(route):
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "folders": [
                    {"path": "2026/2026-07-01",
                     "full_path": "/archive/2026/2026-07-01",
                     "count": 1, "exists": False},
                ],
                "total_photos": 1,
                "total_folders": 1,
                "new_folders": 1,
                "existing_folders": 0,
                "managed_archive": None,
                "template_samples": {
                    "samples": {
                        "%Y/%Y-%m-%d": "2026/2026-07-01",
                        "%Y/%m/%d": "2026/07/01",
                        "%Y/%m": "2026/07",
                        "%Y": "2026",
                        "%Y-%m-%d": "2026-07-01",
                    },
                    "sample_date": "2026-07-01T09:00:00",
                    "dated_count": 1,
                },
            }),
        )

    page.route("**/api/import/folder-preview-stream", folder_preview)
    page.route("**/api/import/check-duplicates", check_duplicates)
    page.route("**/api/import/destination-preview", destination_preview)
    page.goto(f"{url}/import")

    preset = page.locator("#folderTemplatePreset")
    # Ships with no example: nothing is known about the files yet.
    expect(preset.locator("option[value='%Y/%Y-%m-%d']")).to_have_text(
        "%Y/%Y-%m-%d")

    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/archive")
    page.locator("#btnPreview").click()

    expect(page.locator("#destStructure")).to_be_visible()
    # The example is a folder this import really creates — the first row.
    expect(preset.locator("option[value='%Y/%Y-%m-%d']")).to_have_text(
        "%Y/%Y-%m-%d — 2026/2026-07-01")
    expect(preset.locator("option[value='%Y-%m-%d']")).to_have_text(
        "%Y-%m-%d — 2026-07-01")

    # Remove the source the way a user does: the × on the source row.
    page.locator("#sourceList button").last.click()

    expect(page.locator("#destStructure")).to_be_hidden()
    expect(preset.locator("option[value='%Y/%Y-%m-%d']")).to_have_text(
        "%Y/%Y-%m-%d")
    expect(preset.locator("option[value='%Y-%m-%d']")).to_have_text("%Y-%m-%d")


def _stub_preview(page, files, duplicates=None):
    """Stub folder-preview + check-duplicates, and put the page in COPY mode.

    Two things here are load-bearing:
      - #modeInPlace is `checked` by default (import.html:232). Without
        selecting copy mode, previewImport() returns early at the
        `if (!copyMode)` branch, no duplicate stream runs, and per Task 11
        the checkboxes are hidden entirely — every selection test would
        fail for the wrong reason.
      - check-duplicates is SSE, not newline-JSON. The client parses
        `buffer.split('\n\n')` + /^data: (.+)$/m. Frames must be
        `data: {...}\n\n`. Copied from the existing stub above.
    """
    page.locator("#modeCopy").check()
    page.evaluate(
        """
        ([files, dupes]) => {
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const t = typeof input === 'string' ? input : input.url;
            if (t && t.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(window.__previewDoneResponse({
                total_count: files.length, total_size: 0,
                type_breakdown: {'.jpg': files.length},
                duplicate_count: 0, files: files,
              }));
            }
            if (t && t.indexOf('/api/import/check-duplicates') === 0) {
              const frame = 'data: ' + JSON.stringify({
                duplicates: dupes, checked: files.length, total: files.length,
              }) + '\\n\\n' + 'data: ' + JSON.stringify({
                done: true, duplicate_count: dupes.length,
                checked: files.length, total: files.length,
              }) + '\\n\\n';
              return Promise.resolve(new Response(frame, {
                status: 200,
                headers: {'Content-Type': 'text/event-stream'},
              }));
            }
            return originalFetch(input, init);
          };
          window.pickDirectory = async () => ['/tmp/card'];
        }
        """,
        [files, duplicates or []],
    )


def _preview(page):
    """Add the stubbed source and wait for the grid to settle."""
    page.locator("[data-testid='import-source-browse-btn']").click()
    page.locator("#btnPreview").click()
    expect(page.locator("#importPreviewGrid")).to_be_visible()


def _set_destination(page, path="/tmp/archive"):
    """Give the form a destination so importFormProblem() stops holding Start.

    Only needed by the handful of tests that assert Start is ENABLED. #1387
    added a form-validity gate (updateStartGate() calls importFormProblem())
    that disables Start with "Choose a destination folder." until the copy
    destination is filled in, which is independent of everything the
    selection tests are about. Tests that only assert Start is DISABLED, or
    that only read its label, don't need this.
    """
    page.locator("#destInput").fill(path)


def _files(n, prefix='/tmp/card/DSC_'):
    return [{"path": f"{prefix}{i:04d}.jpg", "filename": f"DSC_{i:04d}.jpg",
             "subfolder": "card", "size": 100, "extension": ".jpg",
             "mtime": 0, "thumb_url": ""} for i in range(n)]


def test_import_preview_files_are_checked_by_default(live_server, page):
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)

    boxes = page.locator(".import-preview-thumb .thumb-check")
    expect(boxes).to_have_count(3)
    for i in range(3):
        expect(boxes.nth(i)).to_be_checked()


def test_import_preview_duplicates_are_unchecked_and_disabled(
        live_server, page):
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)

    dupe = page.locator(
        f".import-preview-thumb[data-path='{files[1]['path']}'] .thumb-check")
    expect(dupe).not_to_be_checked()
    expect(dupe).to_be_disabled()


def test_import_preview_deselection_survives_a_later_render(live_server, page):
    """A hand-picked deselection must outlive the next render pass.

    renderImportPreviewGrid runs up to three times per preview (files only,
    then with duplicate verdicts, then with destination data). Checkbox state
    is DERIVED from importDeselected on every pass rather than seeded, so a
    late-arriving pass re-computes the same answer instead of stomping the
    user's click. This drives the third pass the way import.html itself does.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)

    def box(path):
        return page.locator(
            f".import-preview-thumb[data-path='{path}'] .thumb-check")

    box(files[0]["path"]).uncheck()
    expect(box(files[0]["path"])).not_to_be_checked()

    # The duplicate verdict is an eligibility overlay, not user intent: it
    # must never be written into importDeselected.
    assert page.evaluate("() => Array.from(importDeselected)") == [
        files[0]["path"]]

    # Re-render with the same inputs, as the destination-data pass does.
    page.evaluate(
        "([files, dupes]) => renderImportPreviewGrid(files, dupes, null)",
        [files, [files[1]["path"]]],
    )

    expect(box(files[0]["path"])).not_to_be_checked()
    expect(box(files[0]["path"])).to_be_enabled()
    expect(box(files[1]["path"])).not_to_be_checked()
    expect(box(files[1]["path"])).to_be_disabled()
    expect(box(files[2]["path"])).to_be_checked()

    # Black-box proof of the same separation: drop the eligibility overlay
    # and the duplicate comes back checked. If the verdict had been written
    # into importDeselected, it would survive as intent and stay unchecked.
    # (Set the control directly: .uncheck() fires change, which reruns the
    # whole preview and would rebuild state from scratch.)
    page.evaluate(
        "() => { document.getElementById('chkSkipDuplicates').checked ="
        " false; }")
    page.evaluate(
        "([files, dupes]) => renderImportPreviewGrid(files, dupes, null)",
        [files, [files[1]["path"]]],
    )
    expect(box(files[1]["path"])).to_be_checked()
    expect(box(files[1]["path"])).to_be_enabled()
    # ...and the hand-picked deselection is still intent, so it survives.
    expect(box(files[0]["path"])).not_to_be_checked()


def _box(page, path):
    return page.locator(
        f".import-preview-thumb[data-path='{path}'] .thumb-check")


def _drop_skip_duplicates_and_rerender(page, files, dupes):
    """Turn the duplicate overlay off and re-render, without a re-preview.

    Reveals whether a bulk operation wrote duplicate verdicts into
    importDeselected: once the overlay is gone, anything still unchecked is
    intent. Uses a direct property write rather than .uncheck() so no change
    event fires and previewImport() doesn't rebuild the state under us.
    """
    page.evaluate(
        "() => { document.getElementById('chkSkipDuplicates').checked ="
        " false; }")
    page.evaluate(
        "([files, dupes]) => renderImportPreviewGrid(files, dupes, null)",
        [files, dupes],
    )


def test_shift_click_selects_a_contiguous_range(live_server, page):
    page.goto(f"{live_server['url']}/import")
    files = _files(5)
    _stub_preview(page, files)
    _preview(page)

    boxes = page.locator(".import-preview-thumb .thumb-check")
    boxes.nth(1).click()                          # uncheck index 1
    boxes.nth(3).click(modifiers=["Shift"])       # range 1..3 unchecked
    for i, want in enumerate([True, False, False, False, True]):
        if want:
            expect(boxes.nth(i)).to_be_checked()
        else:
            expect(boxes.nth(i)).not_to_be_checked()


def test_shift_click_range_runs_backwards_too(live_server, page):
    """Anchor after target. The range is min..max, not anchor..target."""
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(5))
    _preview(page)

    boxes = page.locator(".import-preview-thumb .thumb-check")
    boxes.nth(3).click()                          # anchor at index 3
    boxes.nth(1).click(modifiers=["Shift"])       # range 1..3 unchecked
    for i, want in enumerate([True, False, False, False, True]):
        if want:
            expect(boxes.nth(i)).to_be_checked()
        else:
            expect(boxes.nth(i)).not_to_be_checked()


def test_shift_click_moves_the_anchor(live_server, page):
    """A shift-click re-anchors, so the next range starts where it ended."""
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(5))
    _preview(page)

    boxes = page.locator(".import-preview-thumb .thumb-check")
    boxes.nth(0).click()                          # anchor 0, uncheck 0
    boxes.nth(4).click(modifiers=["Shift"])       # 0..4 unchecked, anchor 4
    for i in range(5):
        expect(boxes.nth(i)).not_to_be_checked()

    boxes.nth(2).click(modifiers=["Shift"])       # re-check 2..4, not 0..2
    for i, want in enumerate([False, False, True, True, True]):
        if want:
            expect(boxes.nth(i)).to_be_checked()
        else:
            expect(boxes.nth(i)).not_to_be_checked()


def test_shift_click_range_follows_render_order_not_api_order(
        live_server, page):
    """The range is over what the user dragged across, not over the payload.

    renderImportPreviewGrid groups by subfolder and sorts the group keys, so
    render order diverges from the API's file order whenever subfolders don't
    arrive alphabetically. Here the API returns b/ first; the grid shows a/
    first. A range computed from importPreviewedPaths would toggle b/0001 --
    a card the user never dragged across -- and leave a/0001 alone.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(2, prefix='/tmp/card/b/DSC_') + _files(
        2, prefix='/tmp/card/a/DSC_')
    for f in files[:2]:
        f["subfolder"] = "b"
    for f in files[2:]:
        f["subfolder"] = "a"
    _stub_preview(page, files)
    _preview(page)

    boxes = page.locator(".import-preview-thumb .thumb-check")
    # Rendered: a/0000, a/0001, b/0000, b/0001.
    assert page.evaluate(
        "() => Array.from(document.querySelectorAll("
        "'#importPreviewGrid .import-preview-thumb')).map(e => e.dataset.path)"
    ) == [files[2]["path"], files[3]["path"],
          files[0]["path"], files[1]["path"]]

    boxes.nth(0).click()
    boxes.nth(2).click(modifiers=["Shift"])   # a/0000 .. b/0000
    for i, want in enumerate([False, False, False, True]):
        if want:
            expect(boxes.nth(i)).to_be_checked()
        else:
            expect(boxes.nth(i)).not_to_be_checked()


def test_shift_range_does_not_deselect_ineligible_duplicates(
        live_server, page):
    """A range dragged across a skipped duplicate must not record intent.

    While Skip duplicates is on the card is disabled either way, so the DOM
    can't tell the two designs apart — turn the overlay off and look again.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(4)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)

    boxes = page.locator(".import-preview-thumb .thumb-check")
    boxes.nth(0).click()
    boxes.nth(2).click(modifiers=["Shift"])       # range spans the duplicate

    _drop_skip_duplicates_and_rerender(page, files, [files[1]["path"]])
    expect(_box(page, files[1]["path"])).to_be_checked()
    expect(_box(page, files[0]["path"])).not_to_be_checked()
    expect(_box(page, files[2]["path"])).not_to_be_checked()


def test_folder_header_checkbox_toggles_its_subfolder(live_server, page):
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)

    page.locator(".import-preview-folder-header .folder-check").first.click()
    boxes = page.locator(".import-preview-thumb .thumb-check")
    for i in range(3):
        expect(boxes.nth(i)).not_to_be_checked()


def test_folder_header_only_toggles_its_own_subfolder(live_server, page):
    page.goto(f"{live_server['url']}/import")
    files = (_files(2, prefix='/tmp/card/a/DSC_')
             + _files(2, prefix='/tmp/card/b/DSC_'))
    for f in files[:2]:
        f["subfolder"] = "a"
    for f in files[2:]:
        f["subfolder"] = "b"
    _stub_preview(page, files)
    _preview(page)

    headers = page.locator(".import-preview-folder-header .folder-check")
    expect(headers).to_have_count(2)
    headers.nth(0).click()
    for f in files[:2]:
        expect(_box(page, f["path"])).not_to_be_checked()
    for f in files[2:]:
        expect(_box(page, f["path"])).to_be_checked()


def test_folder_header_is_indeterminate_on_a_partial_selection(
        live_server, page):
    """The tri-state is the honest readout: some != all, and != none.

    "All" semantics, matching chkSelectAll on pipeline.html: a partial
    selection is dashed and NOT ticked.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)

    header = page.locator(".import-preview-folder-header .folder-check").first
    expect(header).to_be_checked()
    expect(header).to_have_js_property("indeterminate", False)

    boxes = page.locator(".import-preview-thumb .thumb-check")
    boxes.nth(0).click()
    expect(header).not_to_be_checked()
    expect(header).to_have_js_property("indeterminate", True)

    boxes.nth(1).click()
    boxes.nth(2).click()
    expect(header).not_to_be_checked()
    expect(header).to_have_js_property("indeterminate", False)


def test_folder_header_ignores_skipped_duplicates_in_its_tally(
        live_server, page):
    """An ineligible duplicate is not an unselected file.

    Two eligible files both checked plus one skipped duplicate must read as
    "all", not as a partial selection...
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)

    header = page.locator(".import-preview-folder-header .folder-check").first
    expect(header).to_be_checked()
    expect(header).to_have_js_property("indeterminate", False)

    # ...and with both eligible files deselected it must read as "none". This
    # is the case that actually pins the tally's duplicate filter: with the
    # duplicate counted as a selected file the header would claim a partial
    # selection the user cannot act on.
    _box(page, files[0]["path"]).click()
    _box(page, files[2]["path"]).click()
    expect(header).not_to_be_checked()
    expect(header).to_have_js_property("indeterminate", False)


def test_folder_header_does_not_deselect_ineligible_duplicates(
        live_server, page):
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)

    page.locator(".import-preview-folder-header .folder-check").first.click()

    _drop_skip_duplicates_and_rerender(page, files, [files[1]["path"]])
    expect(_box(page, files[0]["path"])).not_to_be_checked()
    expect(_box(page, files[1]["path"])).to_be_checked()
    expect(_box(page, files[2]["path"])).not_to_be_checked()


def test_select_all_toggles_every_file_and_reports_the_count(
        live_server, page):
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)

    expect(page.locator("#selectAllRow")).to_be_visible()
    expect(page.locator("#previewSelectedCount")).to_have_text(
        "3 of 3 selected")

    page.locator("#chkSelectAllImport").uncheck()
    boxes = page.locator(".import-preview-thumb .thumb-check")
    for i in range(3):
        expect(boxes.nth(i)).not_to_be_checked()
    expect(page.locator("#previewSelectedCount")).to_have_text(
        "0 of 3 selected")

    page.locator("#chkSelectAllImport").check()
    for i in range(3):
        expect(boxes.nth(i)).to_be_checked()
    expect(page.locator("#previewSelectedCount")).to_have_text(
        "3 of 3 selected")


def test_select_all_box_reflects_a_partial_selection(live_server, page):
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)

    master = page.locator("#chkSelectAllImport")
    expect(master).to_be_checked()
    expect(master).to_have_js_property("indeterminate", False)

    page.locator(".import-preview-thumb .thumb-check").nth(0).click()
    expect(master).not_to_be_checked()
    expect(master).to_have_js_property("indeterminate", True)


def test_clicking_a_dashed_select_all_selects_the_rest(live_server, page):
    """Tri-state direction, pinned deliberately.

    "All" semantics (chkSelectAll on pipeline.html:1955 uses the same rule),
    so a dashed master is unticked and clicking it completes the selection
    rather than discarding the partial one.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)

    boxes = page.locator(".import-preview-thumb .thumb-check")
    boxes.nth(0).click()
    master = page.locator("#chkSelectAllImport")
    expect(master).to_have_js_property("indeterminate", True)

    master.click()
    for i in range(3):
        expect(boxes.nth(i)).to_be_checked()
    expect(master).to_be_checked()
    expect(master).to_have_js_property("indeterminate", False)


def test_select_all_counts_against_the_eligible_files_not_the_total(
        live_server, page):
    """Skipped duplicates are not unselected files.

    Every selectable file on: the master must read as full, not dashed. Its
    denominator is the eligible count, while the readout beside it stays
    "N of <discovered>" to match the summary line above.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)

    master = page.locator("#chkSelectAllImport")
    expect(master).to_be_checked()
    expect(master).to_have_js_property("indeterminate", False)
    expect(page.locator("#previewSelectedCount")).to_have_text(
        "2 of 3 selected")


def test_bulk_checkboxes_are_disabled_when_nothing_is_selectable(
        live_server, page):
    """All duplicates: a live control that does nothing is a black box."""
    page.goto(f"{live_server['url']}/import")
    files = _files(2)
    _stub_preview(page, files, duplicates=[f["path"] for f in files])
    _preview(page)

    master = page.locator("#chkSelectAllImport")
    header = page.locator(".import-preview-folder-header .folder-check").first
    expect(master).to_be_disabled()
    expect(master).not_to_be_checked()
    expect(header).to_be_disabled()
    expect(header).not_to_be_checked()
    expect(page.locator("#previewSelectedCount")).to_have_text(
        "0 of 2 selected")


def test_select_all_does_not_deselect_ineligible_duplicates(
        live_server, page):
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)

    page.locator("#chkSelectAllImport").uncheck()

    _drop_skip_duplicates_and_rerender(page, files, [files[1]["path"]])
    expect(_box(page, files[0]["path"])).not_to_be_checked()
    expect(_box(page, files[1]["path"])).to_be_checked()
    expect(_box(page, files[2]["path"])).not_to_be_checked()


def test_a_stale_duplicate_verdict_cannot_disable_a_fresh_card(
        live_server, page):
    """A retained duplicate verdict outlives the render it came from.

    Verdicts only exist once the whole check-duplicates stream drains, so a
    re-preview's FIRST render pass draws every card enabled while the
    previous run's verdicts are the only ones anything could have kept.
    Anything that re-derives checkbox state from a retained set instead of
    from the card turns an unrelated click into a disabled, unchecked box
    with no badge explaining why -- and over SMB with verify_by_hash the
    window between the first pass and the new verdicts is minutes.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)
    expect(_box(page, files[1]["path"])).to_be_disabled()

    # The re-preview's first pass: same files, no verdicts yet.
    page.evaluate("(f) => renderImportPreviewGrid(f, [], null)", files)
    for f in files:
        expect(_box(page, f["path"])).to_be_enabled()
        expect(_box(page, f["path"])).to_be_checked()

    # A click anywhere refreshes every box. The stale verdict must not
    # resurrect and disable a card that carries no badge.
    _box(page, files[0]["path"]).click()
    expect(_box(page, files[1]["path"])).to_be_enabled()
    expect(_box(page, files[1]["path"])).to_be_checked()
    expect(_box(page, files[2]["path"])).to_be_checked()
    expect(page.locator(".import-preview-badge")).to_have_count(0)
    header = page.locator(".import-preview-folder-header .folder-check").first
    expect(header).to_be_enabled()
    expect(header).to_have_js_property("indeterminate", True)


def test_a_stale_duplicate_verdict_cannot_shrink_a_bulk_toggle(
        live_server, page):
    """Same stale window, the bulk paths.

    A bulk toggle that reads a retained verdict set would quietly skip a card
    the renderer drew as an ordinary selectable file: the user hits
    "deselect all" and one box stays ticked with nothing to explain it.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)

    page.evaluate("(f) => renderImportPreviewGrid(f, [], null)", files)

    page.locator("#chkSelectAllImport").uncheck()
    for f in files:
        expect(_box(page, f["path"])).not_to_be_checked()

    page.locator(".import-preview-folder-header .folder-check").first.check()
    for f in files:
        expect(_box(page, f["path"])).to_be_checked()


def test_a_stale_duplicate_verdict_cannot_kill_the_master_checkbox(
        live_server, page):
    """The counters are the third way a stale verdict reaches the screen.

    An all-duplicate previous run leaves verdicts covering every path.
    Counters reading a retained set report zero eligible over a grid of
    ticked, enabled, badge-free boxes -- which prints "0 of 2 selected" and,
    since fix 6 disables the master at zero eligible, hands the user a dead
    control captioned "every discovered file is a duplicate" while two
    perfectly selectable files sit under it.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(2)
    _stub_preview(page, files, duplicates=[f["path"] for f in files])
    _preview(page)
    expect(page.locator("#chkSelectAllImport")).to_be_disabled()

    # The re-preview's first pass: same files, no verdicts yet.
    page.evaluate("(f) => renderImportPreviewGrid(f, [], null)", files)
    page.evaluate("() => updateImportSelectionUI()")

    master = page.locator("#chkSelectAllImport")
    expect(master).to_be_enabled()
    expect(master).to_be_checked()
    expect(master).to_have_js_property("indeterminate", False)
    expect(page.locator("#previewSelectedCount")).to_have_text(
        "2 of 2 selected")
    expect(page.locator(".import-preview-badge")).to_have_count(0)


def test_folder_checkbox_tooltip_names_its_folder(live_server, page):
    """Two headers, two tooltips -- "this folder" tells the user nothing."""
    page.goto(f"{live_server['url']}/import")
    files = (_files(1, prefix='/tmp/card/a/DSC_')
             + _files(1, prefix='/tmp/card/b/DSC_'))
    files[0]["subfolder"] = "a"
    files[1]["subfolder"] = "b"
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)

    headers = page.locator(".import-preview-folder-header .folder-check")
    expect(headers.nth(0)).to_have_attribute(
        "title", "Select or deselect every file in a")
    expect(headers.nth(1)).to_have_attribute(
        "title", "Every file in b is a duplicate that will be skipped")


def test_a_single_render_pass_produces_a_correct_folder_header(
        live_server, page):
    """One render must be enough.

    #importPreviewGrid ships display:none and previewImport() re-hides it
    before every run, so anything that reads layout to build the header
    tally has to run after the grid is shown again. A later pass would
    paper over a mistake here, but a later pass is not guaranteed: with
    Skip duplicates OFF, the only re-render is the one gated on
    `destData && destData.files`, and renderDestStructure() returns null
    before it even fetches whenever the destination doesn't resolve. That
    leaves the files-only pass as the only one there is, and the user with
    a dead, unticked header over a fully selected folder.

    (With Skip duplicates ON there are always at least two passes -- the
    stream-drained render is unconditional -- so that is NOT the case this
    guards.)

    Copy mode explicitly, since the header checkbox only exists there, and
    the renderer is driven directly so the assertion lands on exactly one
    pass rather than on whichever one previewImport() happens to end with.
    """
    page.goto(f"{live_server['url']}/import")
    # page.goto() returns on `load`, but initImportPage() is still awaiting
    # /api/volumes, /api/config and /api/workspaces/active at that point, and
    # its trailing updateImportMode() calls clearImportPreviewGrid() ->
    # grid.innerHTML = ''. This test drives the renderer directly, so it has
    # no network round trip of its own to hide behind and the wipe lands
    # between the render and the assertions -- the header vanishes and the
    # last expect() fails with "element(s) not found". Measured on this
    # machine: 16/30 without this line, 30/30 with it.
    page.wait_for_load_state("networkidle")
    _suppress_auto_preview(page)
    page.locator("#modeCopy").check()
    expect(page.locator("#importPreviewGrid")).to_be_hidden()
    page.evaluate("(f) => renderImportPreviewGrid(f, [], null)", _files(3))

    header = page.locator(".import-preview-folder-header .folder-check").first
    expect(header).to_be_enabled()
    expect(header).to_be_checked()
    expect(header).to_have_js_property("indeterminate", False)


def test_shift_range_does_not_reach_through_a_hidden_card(live_server, page):
    """A range is what the user dragged across on screen, so a card that is
    not on screen must not be swept up by it.

    The hide here is synthetic, and stays synthetic on purpose. It was
    written expecting #1382's hide-duplicates filter to hide cards with CSS;
    that filter landed omitting them from the render instead, so nothing in
    the product currently produces a display:none card and this is the only
    thing exercising importVisibleCards()'s offsetParent test. See
    test_shift_range_over_the_real_hide_duplicates_filter for the same
    property under the filter as it actually shipped.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(4)
    _stub_preview(page, files)
    _preview(page)

    hide = ("(p) => { document.querySelector("
            "`.import-preview-thumb[data-path='${p}']`).style.display = 'X'; }")
    page.evaluate(hide.replace("'X'", "'none'"), files[1]["path"])

    _box(page, files[0]["path"]).click()
    _box(page, files[2]["path"]).click(modifiers=["Shift"])

    page.evaluate(hide.replace("'X'", "''"), files[1]["path"])
    expect(_box(page, files[0]["path"])).not_to_be_checked()
    expect(_box(page, files[1]["path"])).to_be_checked()
    expect(_box(page, files[2]["path"])).not_to_be_checked()
    expect(_box(page, files[3]["path"])).to_be_checked()


def _hide_duplicates_preview(page, n, dup_idx, prefixes=None):
    """Preview `n` files with `dup_idx` flagged, then switch the filter on."""
    if prefixes is None:
        files = _files(n)
    else:
        files = []
        for prefix, count in prefixes:
            base = len(files)
            for f in _files(count, prefix=f'/tmp/card/{prefix}/DSC_'):
                f["subfolder"] = prefix
                f["path"] = f'/tmp/card/{prefix}/DSC_{base:04d}.jpg'
                f["filename"] = f'DSC_{base:04d}.jpg'
                base += 1
                files.append(f)
    dupes = [files[i]["path"] for i in dup_idx]
    _stub_preview(page, files, duplicates=dupes)
    _preview(page)
    expect(page.locator("#hideDuplicatesRow")).to_be_visible()
    page.locator("#chkHideDuplicates").check()
    return files, dupes


def test_shift_range_over_the_real_hide_duplicates_filter(live_server, page):
    """The same property as the synthetic-hide test, under #1382 as shipped.

    The filter omits flagged cards from the render rather than hiding them,
    so a range that runs over the surviving cards must close over the gap
    the removed one left -- and must not record a deselection for it, which
    would outlive the filter and reappear the moment it is switched off.
    """
    page.goto(f"{live_server['url']}/import")
    files, _ = _hide_duplicates_preview(page, 5, (2,))

    expect(page.locator(".import-preview-thumb")).to_have_count(4)
    _box(page, files[1]["path"]).click()
    _box(page, files[3]["path"]).click(modifiers=["Shift"])

    expect(_box(page, files[0]["path"])).to_be_checked()
    expect(_box(page, files[1]["path"])).not_to_be_checked()
    expect(_box(page, files[3]["path"])).not_to_be_checked()
    expect(_box(page, files[4]["path"])).to_be_checked()
    # The card the filter removed was never in scope, so it collected no
    # deselection to carry back when the filter comes off.
    assert page.evaluate("() => Array.from(importDeselected)") == [
        files[1]["path"], files[3]["path"],
    ]


def test_hide_duplicates_keeps_the_master_and_its_headers_in_agreement(
        live_server, page):
    """Ticking every folder header must fill the master, not dash it.

    chkSelectAllImport counts importGridCards() while a folder header counts
    importVisibleCards(). A card that one scope sees and the other does not
    would leave the master permanently indeterminate with no header left to
    tick. #1382 is the only thing that removes cards from a rendered
    preview, so it is the thing that could break that -- it doesn't, because
    it omits them from the render rather than hiding them, which takes them
    out of both scopes at once.
    """
    page.goto(f"{live_server['url']}/import")
    _hide_duplicates_preview(
        page, 6, (1, 4), prefixes=[("a", 3), ("b", 3)])

    headers = page.locator(".import-preview-folder-header .folder-check")
    expect(headers).to_have_count(2)
    for i in range(2):
        if not headers.nth(i).is_checked():
            headers.nth(i).click()
        expect(headers.nth(i)).to_be_checked()

    master = page.locator("#chkSelectAllImport")
    expect(master).to_be_checked()
    expect(master).to_have_js_property("indeterminate", False)


def test_a_fully_filtered_folder_header_still_names_its_folder(
        live_server, page):
    """The tooltip must name the folder, not the whole header line.

    #1382 renders an all-duplicate folder as "a (0) · 2 duplicates hidden",
    and the folder name used to be recovered by stripping a trailing " (N)"
    off that text -- which this suffix defeats, leaving the checkbox
    claiming "Every file in a (0) · 2 duplicates hidden is a duplicate…".
    """
    page.goto(f"{live_server['url']}/import")
    _hide_duplicates_preview(
        page, 4, (0, 1, 2), prefixes=[("a", 2), ("b", 2)])

    headers = page.locator(".import-preview-folder-header")
    expect(headers.nth(0)).to_have_text("a (0) · 2 duplicates hidden")
    expect(headers.nth(1)).to_have_text("b (1) · 1 duplicate hidden")
    expect(headers.nth(0).locator(".folder-check")).to_have_attribute(
        "title", "Every file in a is a duplicate that will be skipped")
    expect(headers.nth(1).locator(".folder-check")).to_have_attribute(
        "title", "Select or deselect every file in b")


def test_hide_duplicates_does_not_re_request_the_thumbnails_per_click(
        live_server, page):
    """A selection click refreshes the boxes, it does not rebuild the grid.

    rerenderImportPreviewGridSafe() used to fall back to a targeted DOM
    refresh only because #1382's rerenderImportPreviewGrid() had not merged.
    It has, and calling it here would work -- but a full re-render drops
    every <img> and re-runs the thumbnail scheduler, once per checkbox
    click, over a grid #1382 sized for 985 files.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(6)
    _stub_preview(page, files)
    _preview(page)

    # _preview() returns as soon as the grid is visible, while the thumbnail
    # scheduler may still be draining its initial queue.  Start measuring only
    # after those requests have settled so they cannot be mistaken for work
    # caused by the selection clicks below.
    expect(page.locator(".import-preview-thumb.skeleton")).to_have_count(0)
    hits = []
    page.on("request", lambda r: hits.append(r.url)
            if "folder-preview/thumbnail" in r.url else None)
    _box(page, files[0]["path"]).click()
    _box(page, files[2]["path"]).click()
    page.wait_for_timeout(300)
    assert hits == [], f"selection clicks re-requested thumbnails: {hits}"


def test_duplicate_badge_and_checkbox_do_not_overlap(live_server, page):
    """Both must stay legible: the box says it's off, the badge says why."""
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    _preview(page)

    card = page.locator(
        f".import-preview-thumb[data-path='{files[1]['path']}']")
    check = card.locator(".thumb-check")
    badge = card.locator(".import-preview-badge")
    expect(check).to_be_visible()
    expect(badge).to_be_visible()
    expect(badge).to_have_text("Duplicate")

    cb = check.bounding_box()
    bb = badge.bounding_box()
    assert cb["width"] > 0 and bb["width"] > 0
    horizontal_gap = (cb["x"] + cb["width"] <= bb["x"]
                      or bb["x"] + bb["width"] <= cb["x"])
    vertical_gap = (cb["y"] + cb["height"] <= bb["y"]
                    or bb["y"] + bb["height"] <= cb["y"])
    assert horizontal_gap or vertical_gap, (
        f"checkbox {cb} overlaps duplicate badge {bb}")


# --- Task 10: Start gating and the preview lifecycle -----------------------


def test_start_is_disabled_when_all_importable_files_are_unchecked(
        live_server, page):
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)

    page.locator("#chkSelectAllImport").click()   # select none
    # Assert the LABEL as well as `disabled`. Start is transiently disabled
    # while a preview runs and while the duplicate stream drains, so
    # `to_be_disabled()` on its own can pass on a state that has nothing to do
    # with the selection and then settle to enabled.
    expect(page.locator("#btnStart")).to_have_text("No files selected")
    expect(page.locator("#btnStart")).to_be_disabled()


def test_start_stays_enabled_when_every_file_is_a_duplicate(live_server, page):
    """Zero checked, zero eligible — the user must still be able to run the
    import to get the safe-to-format verdict on an already-archived card."""
    page.goto(f"{live_server['url']}/import")
    files = _files(2)
    _stub_preview(page, files, duplicates=[f["path"] for f in files])
    _set_destination(page)
    _preview(page)

    # The count settles the timing: _preview() returns on the FIRST render
    # pass, before any verdict has landed, and at that moment both files look
    # eligible. Waiting for "0 files" means the assertion below is made
    # against the all-duplicate state and not the pre-verdict one.
    expect(page.locator("#btnStart")).to_have_text("Start import (0 files)")
    expect(page.locator("#btnStart")).to_be_enabled()


def test_changing_a_source_after_selecting_disables_start(live_server, page):
    """Toggling a signature input must gate Start immediately.

    #chkRecursive is wired into wireDestStructureInvalidation, which fires
    scheduleImportPreview() on a 350ms debounce. Suppress that entirely: if the
    auto-preview starts, updateStartGate checks importPreviewInFlight BEFORE
    staleness and the label reads "Previewing…" instead, making the assertion
    timing-dependent.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)
    page.locator(".import-preview-thumb .thumb-check").first.click()

    page.evaluate("() => { window.scheduleImportPreview = () => {}; }")
    page.locator("#chkRecursive").click()   # invalidates the signature
    # Label first, like every other gating assertion here: `disabled` alone
    # matches several unrelated states this test doesn't mean.
    expect(page.locator("#btnStart")).to_have_text(
        "Preview again before importing")
    expect(page.locator("#btnStart")).to_be_disabled()


def test_start_is_disabled_while_a_preview_is_in_flight(live_server, page):
    """The window between clearImportPreviewGrid() and the render is the
    5,000-file hazard: state must not reset to 'no preview run' there."""
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)
    page.locator(".import-preview-thumb .thumb-check").first.click()

    page.evaluate(
        """() => {
          const f = window.fetch.bind(window);
          window.__release = null;
          window.fetch = (input, init) => {
            const t = typeof input === 'string' ? input : input.url;
            if (t && t.indexOf('/api/import/folder-preview') === 0) {
              return new Promise((res) => { window.__release = res; });
            }
            return f(input, init);
          };
        }"""
    )
    page.locator("#btnPreview").click()
    expect(page.locator("#btnStart")).to_have_text("Previewing…")
    expect(page.locator("#btnStart")).to_be_disabled()
    # The prior selection survives the in-flight window.
    assert page.evaluate("() => importDeselected.size") == 1


def test_start_is_disabled_while_the_duplicate_stream_is_draining(
        live_server, page):
    """Checkbox eligibility is not final until verdicts land, so submitting
    mid-stream would send an include_paths that doesn't match the screen."""
    page.goto(f"{live_server['url']}/import")
    page.locator("#modeCopy").check()
    page.evaluate(
        """(files) => {
          const f = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const t = typeof input === 'string' ? input : input.url;
            if (t && t.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(window.__previewDoneResponse({
                total_count: files.length, total_size: 0,
                type_breakdown: {'.jpg': files.length},
                duplicate_count: 0, files: files,
              }));
            }
            if (t && t.indexOf('/api/import/check-duplicates') === 0) {
              return new Promise(() => {});   // stream never drains
            }
            return f(input, init);
          };
          window.pickDirectory = async () => ['/tmp/card'];
        }""",
        _files(3),
    )
    # Not _preview(): that waits for #importPreviewGrid, and since #1387 the
    # copy path with Skip duplicates ON no longer renders a pre-verdict pass
    # -- the grid appears only once the duplicate stream has drained, which
    # this stub deliberately never lets happen. The summary line is the
    # signal that discovery finished and the stream leg has begun.
    page.locator("[data-testid='import-source-browse-btn']").click()
    page.locator("#btnPreview").click()
    expect(page.locator("#previewSummary")).to_have_text(
        "3 files found — checking for duplicates…")
    expect(page.locator("#btnStart")).to_have_text("Checking duplicates…")
    expect(page.locator("#btnStart")).to_be_disabled()


def test_zero_file_preview_disables_start(live_server, page):
    """A completed preview that found nothing is NOT 'no preview run' —
    otherwise later arrivals would be imported unseen.

    _preview() can't be used here: it waits for the grid to become visible
    and a zero-file preview never renders one.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, [])
    page.locator("[data-testid='import-source-browse-btn']").click()
    page.locator("#btnPreview").click()
    expect(page.locator("#previewSummary")).to_have_text(
        "No importable files found.")
    expect(page.locator("#btnStart")).to_have_text("No files to import")
    expect(page.locator("#btnStart")).to_be_disabled()


def test_with_skip_duplicates_off_every_card_is_checked_and_enabled(
        live_server, page):
    """The duplicate stream returns early in this mode, so there are no
    verdicts and the derived-checked rule yields all-on."""
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[1]["path"]])
    page.locator("#chkSkipDuplicates").uncheck()
    _preview(page)

    boxes = page.locator(".import-preview-thumb .thumb-check")
    for i in range(3):
        expect(boxes.nth(i)).to_be_checked()
        expect(boxes.nth(i)).to_be_enabled()


def test_selected_count_readout(live_server, page):
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(4))
    _preview(page)
    page.locator(".import-preview-thumb .thumb-check").first.click()

    expect(page.locator("#previewSelectedCount")).to_have_text("3 of 4 selected")


def test_a_failed_preview_disables_start(live_server, page):
    """previewImport() clears the grid before it fetches, so a preview that
    throws leaves an EMPTY screen behind a signature that still matches.

    Without an explicit failure state the gate reads "not stale, nothing
    eligible" and re-enables Start over a grid the user can no longer see.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)

    page.evaluate(
        """() => {
          const f = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const t = typeof input === 'string' ? input : input.url;
            if (t && t.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(new Response(
                JSON.stringify({error: 'disk went away'}),
                {status: 500, headers: {'Content-Type': 'application/json'}}));
            }
            return f(input, init);
          };
        }"""
    )
    page.locator("#btnPreview").click()
    expect(page.locator("#importPreviewGrid")).not_to_be_visible()
    # The LABEL, not just `disabled`: previewImport() disables Start the
    # moment it starts, so asserting `disabled` alone passes on the in-flight
    # state and never observes what the failure settles to.
    expect(page.locator("#btnStart")).to_have_text(
        "Preview again before importing")
    expect(page.locator("#btnStart")).to_be_disabled()


def test_a_superseded_preview_does_not_reopen_the_gate(live_server, page):
    """An older run finishing must not clear the newer run's in-flight flag.

    Both runs share the module-level lifecycle flags, so the older run's
    `finally` has to check that it still owns them — otherwise a slow first
    walk completing hands the user an enabled Start while the second walk is
    still running, and the screen is blank.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)

    page.evaluate(
        """() => {
          const f = window.fetch.bind(window);
          window.__rejects = [];
          window.fetch = (input, init) => {
            const t = typeof input === 'string' ? input : input.url;
            if (t && t.indexOf('/api/import/folder-preview') === 0) {
              return new Promise((res, rej) => { window.__rejects.push(rej); });
            }
            return f(input, init);
          };
          previewImport();
          previewImport();
        }"""
    )
    # Run 1 fails; run 2 is still walking the disk and owns the gate.
    page.evaluate("() => window.__rejects[0](new Error('card ejected'))")
    expect(page.locator("#btnStart")).to_have_text("Previewing…")
    expect(page.locator("#btnStart")).to_be_disabled()


def test_start_is_disabled_while_an_import_is_running(live_server, page):
    """No second submission while one import is in flight.

    Two distinct states have to close the button: the POST is in flight and
    activeJobId is still null, and then the job id has arrived. Both read
    "Importing…" to the user; only the gate distinguishes them.
    """
    page.goto(f"{live_server['url']}/import")
    page.evaluate(
        """() => {
          const f = window.fetch.bind(window);
          window.__resolveStart = null;
          window.fetch = (input, init) => {
            const t = typeof input === 'string' ? input : input.url;
            if (t && t.indexOf('/api/jobs/import-in-place') === 0) {
              return new Promise((res) => { window.__resolveStart = res; });
            }
            return f(input, init);
          };
          // Keep the job alive: a real stream would complete and re-enable.
          window.EventSource = function () {
            this.close = () => {};
            this.addEventListener = () => {};
          };
          window.scheduleImportPreview = () => {};
        }"""
    )
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#btnStart").click()

    # The POST has not answered yet, so activeJobId is still null.
    expect(page.locator("#btnStart")).to_have_text("Importing…")
    expect(page.locator("#btnStart")).to_be_disabled()

    page.evaluate(
        """() => window.__resolveStart(new Response(
             JSON.stringify({job_id: 'gate-test'}),
             {status: 200, headers: {'Content-Type': 'application/json'}}))"""
    )
    # #progressCard is shown in the same synchronous block that sets
    # activeJobId, so waiting on it means the assertions below observe the
    # job-id state rather than the still-true pending flag.
    expect(page.locator("#progressCard")).to_be_visible()
    expect(page.locator("#btnStart")).to_have_text("Importing…")
    expect(page.locator("#btnStart")).to_be_disabled()


def test_start_is_re_enabled_when_the_import_finishes(live_server, page):
    """finishJob() owns the end of the import.

    The gate reads activeJobId, so finishJob has to drop it — otherwise the
    button that used to be re-enabled by hand stays shut for the rest of the
    page's life and the user has to reload to import again.
    """
    page.goto(f"{live_server['url']}/import")
    page.evaluate(
        """() => {
          const f = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const t = typeof input === 'string' ? input : input.url;
            if (t && t.indexOf('/api/jobs/import-in-place') === 0) {
              return Promise.resolve(new Response(
                JSON.stringify({job_id: 'gate-done'}),
                {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            if (t && t.indexOf('/api/jobs/gate-done') === 0) {
              return Promise.resolve(new Response(
                JSON.stringify({status: 'completed'}),
                {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return f(input, init);
          };
          window.EventSource = function () {
            this.close = () => {};
            this.addEventListener = (name, fn) => {
              if (name === 'complete') setTimeout(() => fn({}), 0);
            };
          };
          window.scheduleImportPreview = () => {};
        }"""
    )
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#btnStart").click()

    # The job really started: #progressCard is shown in the same synchronous
    # block that sets activeJobId. Without this the assertions below could
    # pass on the never-clicked initial state.
    expect(page.locator("#progressCard")).to_be_visible()
    # ...and finishJob() dropped it again. (The error banner would be the
    # more direct proof that finishJob ran, but initImportPage() calls
    # updateImportMode() -> showError('') after several awaits, so on a busy
    # machine page startup can wipe the banner after the fact.)
    expect(page.locator("#btnStart")).to_have_text("Start import")
    expect(page.locator("#btnStart")).to_be_enabled()


def test_a_completed_preview_replaces_the_previous_selection(live_server, page):
    """A finished preview OWNS the selection.

    Deselections are recorded against the paths a particular preview
    discovered. Carrying them into the next preview would silently exclude
    files from a run the user never made that choice for -- and the boxes on
    screen would agree, because they're derived from the same set.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)
    page.locator(".import-preview-thumb .thumb-check").first.click()
    expect(page.locator("#previewSelectedCount")).to_have_text("2 of 3 selected")

    page.locator("#btnPreview").click()
    expect(page.locator("#previewSelectedCount")).to_have_text("3 of 3 selected")
    boxes = page.locator(".import-preview-thumb .thumb-check")
    for i in range(3):
        expect(boxes.nth(i)).to_be_checked()


def test_removing_the_last_source_gates_start_immediately(live_server, page):
    """Not after the 350ms re-preview debounce.

    Dropping a source changes the preview signature, so the grid that is
    still on screen no longer describes the import. Nothing re-previews here
    (there are no sources left), so if the gate waited for the debounced
    preview to run it would never fire at all.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)
    expect(page.locator("#btnStart")).to_have_text("Start import (3 files)")

    page.locator("#sourceList .source-item button[title='Remove']").click()
    expect(page.locator("#btnStart")).to_have_text(
        "Preview again before importing")
    expect(page.locator("#btnStart")).to_be_disabled()


def test_start_is_blocked_when_the_captured_image_list_fails_to_load(
        live_server, page):
    """Snapshot mode: Start stays shut when there is no list behind it.

    The new-images flow imports exactly one frozen list of paths. If that
    list can't be loaded there is nothing to import, and a live Start would
    post a snapshot id the server has already rejected.
    """
    page.goto(f"{live_server['url']}/import?new_images=not-a-snapshot-id")
    expect(page.locator("#newImagesImportSource")).to_contain_text(
        "could not be loaded")
    # newImagesStartBlocked contributes no label, so the default caption is
    # what proves the block came from that flag and not from a stray reason.
    expect(page.locator("#btnStart")).to_have_text("Start import")
    expect(page.locator("#btnStart")).to_be_disabled()


def test_a_mode_round_trip_does_not_leave_start_live_over_an_empty_grid(
        live_server, page):
    """updateImportMode() throws the grid away, so it owes the gate a reset.

    copy -> in place -> copy inside the 350ms re-preview debounce restores
    the exact signature the completed preview captured, so the staleness
    check reports "current" over a grid that no longer exists -- a live
    Start, an invisible grid and a select-all still reading "3 of 3
    selected", all at once. The debounce is stubbed out here so the
    assertion pins the reset to updateImportMode() rather than to the
    re-preview that happens to follow it.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)
    expect(page.locator("#btnStart")).to_have_text("Start import (3 files)")

    _suppress_auto_preview(page)
    page.locator("#modeInPlace").check()
    page.locator("#modeCopy").check()

    expect(page.locator("#importPreviewGrid")).not_to_be_visible()
    # Back to "no preview run": Start is live, but it now means "import
    # everything", which is what the empty screen beside it says.
    expect(page.locator("#btnStart")).to_have_text("Start import")
    expect(page.locator("#previewSelectedCount")).to_have_text(
        "0 of 0 selected")


def test_the_start_label_pluralises_a_single_file(live_server, page):
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(2))
    _preview(page)

    page.locator(".import-preview-thumb .thumb-check").first.click()
    expect(page.locator("#btnStart")).to_have_text("Start import (1 file)")


# --- Task 10: the gate has to re-OPEN, not just close ---------------------


def test_start_re_opens_after_a_failed_preview_is_retried(live_server, page):
    """importPreviewFailed has exactly one reset, at the top of previewImport.

    Lose it and a single failed preview disables Start for the life of the
    page: the user previews again, gets a complete and current grid, and
    stares at a dead button captioned "Preview again before importing".
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _set_destination(page)
    page.evaluate(
        """() => {
          window.__failPreview = false;
          const f = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const t = typeof input === 'string' ? input : input.url;
            if (window.__failPreview && t
                && t.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(new Response(
                JSON.stringify({error: 'disk went away'}),
                {status: 500, headers: {'Content-Type': 'application/json'}}));
            }
            return f(input, init);
          };
        }"""
    )
    _preview(page)

    page.evaluate("() => { window.__failPreview = true; }")
    page.locator("#btnPreview").click()
    expect(page.locator("#btnStart")).to_have_text(
        "Preview again before importing")

    page.evaluate("() => { window.__failPreview = false; }")
    page.locator("#btnPreview").click()
    expect(page.locator("#importPreviewGrid")).to_be_visible()
    expect(page.locator("#btnStart")).to_have_text("Start import (3 files)")
    expect(page.locator("#btnStart")).to_be_enabled()


def test_start_re_opens_when_the_import_fails_to_start(live_server, page):
    """A rejected POST has to clear importStartPending.

    Otherwise Start stays disabled reading "Importing…" next to an error
    banner saying the import never started, and only a reload recovers.
    """
    page.goto(f"{live_server['url']}/import")
    page.evaluate(
        """() => {
          const f = window.fetch.bind(window);
          window.__rejectStart = null;
          window.fetch = (input, init) => {
            const t = typeof input === 'string' ? input : input.url;
            if (t && t.indexOf('/api/jobs/import-in-place') === 0) {
              return new Promise((res, rej) => { window.__rejectStart = rej; });
            }
            return f(input, init);
          };
          window.scheduleImportPreview = () => {};
        }"""
    )
    page.locator("#sourceInput").fill("/tmp/card-a")
    page.locator("#btnAddSource").click()
    page.locator("#btnStart").click()
    expect(page.locator("#btnStart")).to_have_text("Importing…")

    page.evaluate(
        "() => window.__rejectStart(new Error('archive is offline'))")
    expect(page.locator("#btnStart")).to_have_text("Start import")
    expect(page.locator("#btnStart")).to_be_enabled()


def test_previewing_with_no_sources_does_not_latch_start_shut(
        live_server, page):
    """The validation early returns run AFTER in-flight is set.

    previewImport() is awaited here so the assertion lands on the settled
    state rather than on the moment before the early return.
    """
    page.goto(f"{live_server['url']}/import")
    page.evaluate("async () => { await previewImport(); }")

    assert page.evaluate("() => importPreviewInFlight") is False
    # The LABEL is what pins this: a latched lifecycle gate would caption the
    # button "Previewing…". Start itself is still disabled, but by the form
    # gate #1387 added (title, not label) -- with no sources there is nothing
    # to import either way, and that gate is not what this test is about.
    expect(page.locator("#btnStart")).to_have_text("Start import")
    expect(page.locator("#btnStart")).to_have_attribute(
        "title", "Add at least one source folder.")


def test_previewing_with_no_file_types_does_not_latch_start_shut(
        live_server, page):
    """The second validation early return, same hazard.

    Reachable in copy mode by unchecking the last extension of a custom
    preset, which debounces straight into previewImport().
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)

    # Set without dispatching change: no debounced re-preview to race with.
    page.evaluate(
        """() => {
          document.getElementById('fileTypePreset').value = 'custom';
          document.querySelectorAll('.file-ext').forEach(
            (el) => { el.checked = false; });
        }"""
    )
    page.evaluate("async () => { await previewImport(); }")

    assert page.evaluate("() => importPreviewInFlight") is False
    # Stale, not "Previewing…": the file types no longer match the grid.
    expect(page.locator("#btnStart")).to_have_text(
        "Preview again before importing")
    expect(page.locator("#btnStart")).to_be_disabled()


# --- Task 11: selection is copy-mode only, and the absence is explained ----


def test_in_place_mode_hides_selection_and_explains_why(live_server, page):
    """Hiding the controls is only half the requirement.

    In-place import runs through do_scan(restrict_files=...), which
    vireo/scanner.py only honours alongside restrict_dirs, so per-file
    selection is out of scope for this mode. Omitting the checkboxes
    silently would leave the user unable to tell whether selection is
    missing, broken, or gated behind a setting they haven't found -- the
    same unexplained-control failure the disabled-state work already had to
    fix twice.

    BOTH halves of the note are pinned. The descriptive half alone leaves
    the user knowing selection doesn't apply but not where it does, which
    is the half this whole task exists for; and the pointer has to quote
    the radio's own label, since a user scanning for the note's words has
    to be able to find the control it names.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    page.locator("#modeInPlace").click()
    _preview(page)

    expect(page.locator(".import-preview-thumb .thumb-check")).to_have_count(0)
    note = page.locator("#selectionUnavailableNote")
    expect(note).to_be_visible()
    expect(note).to_contain_text("Add in place catalogs every file")
    expect(note).to_contain_text("File selection is available in Copy to"
                                 " archive mode.")
    # Not a paraphrase of the radio: the note has to name the control the
    # user will go looking for, exactly as the control names itself.
    expect(page.locator("label", has=page.locator("#modeCopy"))
           ).to_contain_text("Copy to archive")
    expect(page.locator("label", has=page.locator("#modeInPlace"))
           ).to_contain_text("Add in place")
    # The other two selection controls go with them: a folder header that
    # bulk-toggles nothing, and a master checkbox over no boxes.
    expect(page.locator(".import-preview-folder-header .folder-check")
           ).to_have_count(0)
    expect(page.locator("#selectAllRow")).not_to_be_visible()
    # The cards themselves stay -- the preview is still the honest list of
    # what will be catalogued.
    expect(page.locator(".import-preview-thumb")).to_have_count(3)


def test_the_folder_header_still_names_its_folder_without_a_checkbox(
        live_server, page):
    """Dropping the box must not leave an empty or half-empty header.

    The header is a flex row with a 6px gap, so leaving a hidden or
    zero-width checkbox in place would indent the folder name away from
    the grid it labels. The name itself still has to be there: without it
    the group separator would be a blank line.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    page.locator("#modeInPlace").click()
    _preview(page)

    header = page.locator(".import-preview-folder-header")
    expect(header).to_have_text("card (3)")
    expect(page.locator(".import-preview-folder-header > *")).to_have_count(1)


def test_switching_back_to_copy_restores_every_selection_control(
        live_server, page):
    """The re-open direction, which is where these flags usually rot.

    in place -> copy has to bring back the per-file boxes, the folder
    header, and the select-all row, AND retire the note -- a note still
    reading "selection is available when copying files" beside a live set of
    checkboxes is its own black box.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _preview(page)
    expect(page.locator(".import-preview-thumb .thumb-check")).to_have_count(3)
    expect(page.locator("#selectionUnavailableNote")).not_to_be_visible()

    page.locator("#modeInPlace").click()
    _preview(page)
    expect(page.locator(".import-preview-thumb .thumb-check")).to_have_count(0)
    expect(page.locator("#selectionUnavailableNote")).to_be_visible()

    page.locator("#modeCopy").click()
    _preview(page)
    expect(page.locator(".import-preview-thumb .thumb-check")).to_have_count(3)
    expect(page.locator(".import-preview-folder-header .folder-check")
           ).to_have_count(1)
    expect(page.locator("#selectAllRow")).to_be_visible()
    expect(page.locator("#selectionUnavailableNote")).not_to_be_visible()


def test_the_note_retires_on_a_mode_switch_before_any_preview(
        live_server, page):
    """The note is owned by the mode, not by the rendered grid.

    updateImportMode() throws the preview away, so if the note only ever
    updated inside the renderer it would survive a switch to copy mode with
    no grid on screen and contradict the controls the user is about to see.
    """
    page.goto(f"{live_server['url']}/import")
    _suppress_auto_preview(page)
    expect(page.locator("#selectionUnavailableNote")).to_be_visible()

    page.locator("#modeCopy").click()
    expect(page.locator("#selectionUnavailableNote")).not_to_be_visible()

    page.locator("#modeInPlace").click()
    expect(page.locator("#selectionUnavailableNote")).to_be_visible()


def test_in_place_start_label_carries_no_file_count(live_server, page):
    """The label's count is guarded by copyMode and must stay that way.

    In-place mode sends no include_paths, so "Start import (3 files)" would
    be a promise the request doesn't make: the scan catalogues whatever is
    on disk when it runs, not the three cards on screen.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    page.locator("#modeInPlace").click()
    _preview(page)

    expect(page.locator("#btnStart")).to_have_text("Start import")
    expect(page.locator("#btnStart")).to_be_enabled()


def _stub_snapshot_import(page, files):
    """Drive the new-images (snapshot) flow, the THIRD selection state.

    Routed at the network layer rather than by patching window.fetch:
    initNewImagesImport() runs from the page's own load handler, so a
    fetch stub installed after goto() would arrive too late.
    """
    # initImportPage() waits for volume suggestions before it follows the
    # new_images deep link.  The real endpoint probes the host and can take
    # longer than Playwright's five-second assertion timeout, even though
    # volumes have nothing to do with snapshot imports.  Keep this helper
    # isolated from that machine-dependent prerequisite.
    page.route(
        "**/api/volumes",
        lambda route: route.fulfill(
            status=200, content_type="application/json", body="[]"),
    )
    page.route(
        "**/api/workspaces/active/new-images/snapshot/42",
        lambda route: route.fulfill(
            status=200, content_type="application/json",
            body=json.dumps({
                "snapshot_id": 42, "file_count": len(files),
                "folder_paths": ["/tmp/card"],
            })),
    )
    page.route(
        "**/api/import/new-images-preview",
        lambda route: route.fulfill(
            status=200, content_type="application/json",
            body=json.dumps({
                "total_count": len(files), "unavailable_count": 0,
                "files": files,
            })),
    )


def test_snapshot_mode_hides_selection_and_says_what_it_will_import(
        live_server, page):
    """Snapshot mode is not in-place mode, and the note must not claim it is.

    Both mode radios are DISABLED here, so pointing at Copy to archive
    would name a control the user cannot reach, and "every file it finds in
    your source folders" is wrong twice over: this import posts a
    snapshot_id and adds exactly the frozen list that was captured, which
    is a subset of what those folders hold.
    """
    page.goto(f"{live_server['url']}/import")  # warm the app before routing
    _stub_snapshot_import(page, _files(3))
    page.goto(f"{live_server['url']}/import?new_images=42")
    expect(page.locator("#importPreviewGrid")).to_be_visible()

    expect(page.locator(".import-preview-thumb")).to_have_count(3)
    expect(page.locator(".import-preview-thumb .thumb-check")).to_have_count(0)
    expect(page.locator("#selectAllRow")).not_to_be_visible()
    note = page.locator("#selectionUnavailableNote")
    expect(note).to_be_visible()
    expect(note).to_contain_text("adds exactly the captured list")
    # Both halves again: "what it adds" and "what happens to the files".
    # The summary line above happens to repeat the second one today, but a
    # note that only half-explains itself shouldn't depend on that.
    expect(note).to_contain_text("leaving the originals where they are")
    expect(note).not_to_contain_text("Copy to archive")
    expect(note).not_to_contain_text("your source folders")
    # The disabled radios are what make naming Copy to archive wrong here.
    expect(page.locator("#modeCopy")).to_be_disabled()
    expect(page.locator("#modeInPlace")).to_be_disabled()


def test_snapshot_mode_suppresses_folder_scan_totals(live_server, page):
    """Snapshot mode never runs folder-count scans, so the source-count
    summary must stay silent instead of announcing "0 photos found"
    underneath the snapshot's own file-count message. activateNewImagesImport
    marks each source `loaded` with no `count`; a summary that would sum
    those into 0 is a lie about work that never happened.
    """
    page.goto(f"{live_server['url']}/import")  # warm the app before routing
    _stub_snapshot_import(page, _files(3))
    page.goto(f"{live_server['url']}/import?new_images=42")
    expect(page.locator("#importPreviewGrid")).to_be_visible()

    expect(page.locator("#newImagesImportSource")).to_contain_text(
        "3 newly detected images",
    )
    # The summary strip is either empty or hidden; either way it must not
    # claim "0 photos found" or announce a scan count.
    progress = page.locator("#sourceCountProgress")
    expect(progress).not_to_contain_text("0 photos")
    expect(progress).not_to_contain_text("folders counted")
    expect(progress).not_to_contain_text("photos found")


def test_snapshot_mode_loads_while_import_readiness_is_slow(
        live_server, page):
    """A metadata-repair count must not hold a captured import hostage.

    Snapshot activation only needs the frozen list and its preview.  Import
    readiness is independent advisory work and can take minutes on a large
    catalog, so the captured count and controls must render while that request
    is still in flight.
    """
    page.goto(f"{live_server['url']}/import")  # warm the app before routing
    _stub_snapshot_import(page, _files(1))

    def readiness(route):
        # Never fulfill: model a catalog-wide metadata check that is still
        # running while the snapshot endpoints answer normally.
        pass

    page.route("**/api/import/readiness", readiness)
    page.goto(
        f"{live_server['url']}/import?new_images=42",
        wait_until="domcontentloaded",
    )

    expect(page.locator("#newImagesImportSource")).to_contain_text(
        "1 newly detected image",
    )
    expect(page.locator("#modeInPlace")).to_be_disabled()
    expect(page.locator("#importPreviewGrid")).to_be_visible()


def test_snapshot_mode_withholds_selection_even_if_the_mode_radio_flips(
        live_server, page):
    """importSelectionEnabled()'s snapshot clause is not decoration.

    Today it is belt-and-braces: activateNewImagesImport() ticks in-place
    and DISABLES both radios, so "copy is checked" should be unreachable,
    and updateImportMode() re-ticks in-place on every call besides. But the
    snapshot import posts a snapshot_id against a frozen server-side list
    and carries no include_paths at all, so if the radio ever comes back the
    controls must not follow it -- checkboxes over a list the request cannot
    narrow are a lie. Driven through the renderer directly because
    updateImportMode() would undo the radio before the guard was reached.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_snapshot_import(page, files)
    page.goto(f"{live_server['url']}/import?new_images=42")
    expect(page.locator("#importPreviewGrid")).to_be_visible()

    page.evaluate(
        """(files) => {
          document.getElementById('modeCopy').disabled = false;
          document.getElementById('modeCopy').checked = true;
          renderImportPreviewGrid(files, [], null);
          updateImportSelectionUI();
        }""",
        files,
    )

    expect(page.locator(".import-preview-thumb")).to_have_count(3)
    expect(page.locator(".import-preview-thumb .thumb-check")).to_have_count(0)
    expect(page.locator(".import-preview-folder-header .folder-check")
           ).to_have_count(0)
    expect(page.locator("#selectAllRow")).not_to_be_visible()
    expect(page.locator("#selectionUnavailableNote")).to_be_visible()


def _capture_start(page):
    """Intercept the import job POST and stash its parsed body.

    Matches both job routes, so a test can assert on what in-place mode
    sends as well as what copy mode does. EventSource is stubbed out
    because watchJob() opens one against the fake job id the moment the
    POST resolves.
    """
    page.evaluate(
        """() => {
          const f = window.fetch;
          window.__body = null;
          window.__url = null;
          window.fetch = (input, init) => {
            const t = typeof input === 'string' ? input : input.url;
            if (t && t.indexOf('/api/jobs/import-') === 0) {
              window.__url = t;
              window.__body = JSON.parse(init.body);
              return Promise.resolve(new Response(
                JSON.stringify({job_id: 'import-x'}), {status: 200,
                headers: {'Content-Type': 'application/json'}}));
            }
            return f(input, init);
          };
          window.EventSource = function () {
            this.close = () => {};
            this.addEventListener = () => {};
          };
        }"""
    )


def _await_duplicate_verdicts(page, count):
    """Block until the check-duplicates stream has actually landed.

    _preview() returns on the files-only render; the duplicate stream is
    still draining behind it. Every assertion about include_paths turns on
    whether a card carries the duplicate class by the time Start is clicked,
    so waiting for the badge -- not for _preview() -- is what makes these
    tests deterministic.
    """
    expect(page.locator(".import-preview-thumb.duplicate")).to_have_count(count)


def test_start_import_sends_include_paths_with_duplicates_retained(
        live_server, page):
    """include_paths keeps a file the UI shows as an unchecked duplicate --
    the job needs it to count the duplicate and keep the ledger balanced."""
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[2]["path"]])
    _capture_start(page)
    # Fill the destination BEFORE previewing. #destInput is wired into
    # wireDestStructureInvalidation, so filling it after selecting schedules a
    # re-preview whose success path resets importDeselected -- the selection
    # assertions would then flake.
    page.locator("#destInput").fill("/tmp/archive")
    _preview(page)
    _await_duplicate_verdicts(page, 1)
    page.locator(".import-preview-thumb .thumb-check").first.click()
    page.locator("#btnStart").click()

    body = page.evaluate("() => window.__body")
    assert set(body["include_paths"]) == {files[1]["path"], files[2]["path"]}
    assert body["previewed_count"] == 3
    assert body["checked_count"] == 1


def test_a_duplicate_unchecked_before_verdicts_still_reaches_the_job(
        live_server, page):
    """At first render verdicts have not arrived, so duplicate cards are still
    enabled and clickable. A click there writes the path into importDeselected
    and nothing removes it -- the eligibleDeselections filter is what stops
    that path being dropped from include_paths, where it would land in no
    ledger bucket and falsely report a fully-archived card as unsafe to
    format.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[files[2]["path"]])
    page.locator("#destInput").fill("/tmp/archive")   # before preview -- see above
    _preview(page)
    _await_duplicate_verdicts(page, 1)
    _capture_start(page)
    # Simulate the pre-verdict click: the path IS a duplicate, but the user
    # unchecked it before the stream landed, so it sits in importDeselected.
    page.evaluate("(p) => importDeselected.add(p)", files[2]["path"])
    page.locator("#btnStart").click()

    body = page.evaluate("() => window.__body")
    assert files[2]["path"] in body["include_paths"]
    # And it is not silently counted as a file the user chose to copy.
    assert body["checked_count"] == 2


def test_in_place_start_sends_no_selection_fields(live_server, page):
    """The in-place route ignores these fields, but sending them is still a
    lie: do_scan() catalogues whatever is on disk when it runs, so a request
    carrying a file list promises a narrowing that never happens.

    The `importPreviewCapturedSignature !== null` guard is NOT what protects
    this. previewImport() captures the signature before its `if (!copyMode)`
    return, so an in-place preview leaves it set; only the copy-mode branch
    keeps the fields out. Hence the wait for the in-place summary below --
    without it the assertion would pass against a null signature and prove
    nothing.
    """
    page.goto(f"{live_server['url']}/import")
    _stub_preview(page, _files(3))
    _capture_start(page)
    page.locator("#destInput").fill("/tmp/archive")
    _preview(page)
    expect(page.locator("#btnStart")).to_have_text("Start import (3 files)")

    page.locator("#modeInPlace").check()
    # The debounced in-place preview has completed and captured a signature.
    expect(page.locator("#previewSummary")).to_have_text(
        "3 files found · originals will stay in place")
    page.locator("#btnStart").click()

    assert page.evaluate("() => window.__url").endswith("/import-in-place")
    body = page.evaluate("() => window.__body")
    assert "include_paths" not in body
    assert "previewed_count" not in body
    assert "checked_count" not in body


def test_the_selection_payload_follows_a_re_preview(live_server, page):
    """A completed preview owns the selection, and the payload has to hear
    about it in both directions: a deselection made before the re-preview is
    gone, and one made after it counts."""
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files)
    _capture_start(page)
    page.locator("#destInput").fill("/tmp/archive")
    _preview(page)
    page.locator(".import-preview-thumb .thumb-check").nth(1).click()
    expect(page.locator("#previewSelectedCount")).to_have_text("2 of 3 selected")

    page.locator("#btnPreview").click()
    expect(page.locator("#previewSelectedCount")).to_have_text("3 of 3 selected")
    page.locator(".import-preview-thumb .thumb-check").nth(0).click()
    expect(page.locator("#previewSelectedCount")).to_have_text("2 of 3 selected")
    page.locator("#btnStart").click()

    body = page.evaluate("() => window.__body")
    assert set(body["include_paths"]) == {files[1]["path"], files[2]["path"]}
    assert body["previewed_count"] == 3
    assert body["checked_count"] == 2


def test_a_fully_duplicate_card_sends_every_path_and_zero_checked(
        live_server, page):
    """Nothing is copied, so checked_count is 0 -- but every path still has to
    reach the job, because it is the copy loop that counts a skipped duplicate
    and only a balanced ledger yields "safe to format"."""
    page.goto(f"{live_server['url']}/import")
    files = _files(3)
    _stub_preview(page, files, duplicates=[f["path"] for f in files])
    _capture_start(page)
    page.locator("#destInput").fill("/tmp/archive")
    _preview(page)
    _await_duplicate_verdicts(page, 3)
    expect(page.locator("#btnStart")).to_have_text("Start import (0 files)")
    page.locator("#btnStart").click()

    body = page.evaluate("() => window.__body")
    assert set(body["include_paths"]) == {f["path"] for f in files}
    assert body["previewed_count"] == 3
    assert body["checked_count"] == 0


def test_a_file_discovered_under_two_sources_is_counted_once(
        live_server, page):
    """/api/import/folder-preview appends per source with no cross-source
    dedup, so nested sources (/card and /card/DCIM) emit the same file twice
    and the grid draws two cards for it. Both counts must be counts of FILES,
    not of cards: the route rejects checked_count > len(set(include_paths)),
    and one file the user ticked once cannot be two files selected.
    """
    page.goto(f"{live_server['url']}/import")
    files = _files(2)
    twin = dict(files[1], subfolder="DCIM")
    _stub_preview(page, files + [twin])
    _capture_start(page)
    page.locator("#destInput").fill("/tmp/archive")
    _preview(page)
    expect(page.locator(".import-preview-thumb")).to_have_count(3)
    # Both readouts are counts of files too. "3 of 3 selected" over an import
    # that copies two files is the same lie as the payload's, and the button
    # is the last thing the user reads before committing to the run.
    expect(page.locator("#previewSelectedCount")).to_have_text("2 of 2 selected")
    expect(page.locator("#btnStart")).to_have_text("Start import (2 files)")
    # And the master checkbox, which compares the two counts rather than
    # printing them. Card-counted eligibility (3) against a file-counted
    # checked (2) leaves it DASHED over a fully-selected preview -- a partial
    # selection the user would go hunting for and never find.
    master = page.locator("#chkSelectAllImport")
    expect(master).to_be_checked()
    expect(master).to_have_js_property("indeterminate", False)
    page.locator("#btnStart").click()

    body = page.evaluate("() => window.__body")
    assert set(body["include_paths"]) == {f["path"] for f in files}
    assert body["previewed_count"] == 2
    assert body["checked_count"] == 2
    # The route's own invariant, asserted the way it is enforced.
    assert body["checked_count"] <= len(set(body["include_paths"]))


def test_copy_mode_with_no_preview_run_sends_no_selection_fields(
        live_server, page):
    """The first of updateStartGate()'s four preview states, and it is
    reachable: a mode switch leaves importPreviewCapturedSignature null and
    Start live, so a click before the 350ms debounce lands here. There is no
    file list to send -- the import copies everything, which is what the
    screen says -- and sending one anyway means include_paths: [], which the
    route rejects outright ("include_paths must be a non-empty list").
    """
    page.goto(f"{live_server['url']}/import")
    _suppress_auto_preview(page)
    _capture_start(page)
    page.locator("#modeCopy").check()
    page.locator("#sourceInput").fill("/tmp/card")
    page.locator("#btnAddSource").click()
    page.locator("#destInput").fill("/tmp/archive")
    # No preview has run, so no count belongs on the button either.
    expect(page.locator("#btnStart")).to_have_text("Start import")
    expect(page.locator("#btnStart")).to_be_enabled()
    page.locator("#btnStart").click()

    # Really the copy route -- otherwise this would pass for the in-place
    # reason and prove nothing about the guard.
    assert page.evaluate("() => window.__url").endswith("/import-photos")
    body = page.evaluate("() => window.__body")
    assert "include_paths" not in body
    assert "previewed_count" not in body
    assert "checked_count" not in body


def test_snapshot_start_sends_no_selection_fields(live_server, page):
    """The snapshot import posts a snapshot_id against a frozen server-side
    list; there is nothing for include_paths to narrow, and the page draws no
    checkboxes to build one from.

    Asserted directly rather than left to ride on the in-place test: the two
    share only the `copyMode` predicate, and a change that split them would
    take this case with it silently. The signature assertion is the point --
    new-images-preview captures one, so `!== null` is true here and the
    copy-mode placement is the only thing keeping the fields out.
    """
    page.goto(f"{live_server['url']}/import")  # warm the app before routing
    _stub_snapshot_import(page, _files(3))
    page.goto(f"{live_server['url']}/import?new_images=42")
    expect(page.locator("#importPreviewGrid")).to_be_visible()
    _capture_start(page)
    assert page.evaluate("() => importPreviewCapturedSignature !== null")

    expect(page.locator("#btnStart")).to_be_enabled()
    page.locator("#btnStart").click()

    assert page.evaluate("() => window.__url").endswith("/import-in-place")
    body = page.evaluate("() => window.__body")
    assert body["source_snapshot_id"] == 42
    assert "include_paths" not in body
    assert "previewed_count" not in body
    assert "checked_count" not in body
