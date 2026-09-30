"""Exercise complete controller modules with explicit dependencies and real DOMs."""
from pathlib import Path

from playwright.sync_api import expect

STATIC = Path(__file__).resolve().parents[2] / "vireo" / "static"


def mount_compare(page):
    page.set_content('''
        <div id="browseCompareOverlay"></div>
        <span id="browseCompareCount"></span>
        <button id="browseComparePrev"></button><button id="browseCompareNext"></button>
    ''' + ''.join(f'''
        <div id="browseCompareWrap{pane}" class="browse-compare-image-wrap" data-pane="{pane}">
          <img id="browseCompareImg{pane}"><span id="browseCompareZoom{pane}"></span>
        </div>
        <span id="browseCompareName{pane}"></span><span id="browseCompareMeta{pane}"></span>
    ''' for pane in "AB"))
    page.add_script_tag(path=str(STATIC / "vireo-browse-compare.js"))
    page.evaluate('''() => {
        window.requests = [];
        window.probes = [];
        window.locks = 0;
        window.esc = new Set();
        window.controller = VireoBrowseCompare.create({
            window: {
                document,
                Image: class { constructor() { probes.push(this); } },
                addEventListener: window.addEventListener.bind(window),
                removeEventListener: window.removeEventListener.bind(window),
            },
            keymap: {
                pushEsc(fn) { esc.add(fn); return fn; },
                popEsc(fn) { esc.delete(fn); },
                lockBodyScroll() { locks++; },
                unlockBodyScroll() { locks--; },
            },
            findPhoto: () => null,
            fetch: url => new Promise(resolve => requests.push({url, resolve})),
            showToast() {},
        });
    }''')


def test_compare_owns_selection_and_rejects_old_pair_responses(page):
    mount_compare(page)
    page.evaluate('''() => {
        const ids = [1, 2, 3];
        window.first = controller.open(ids);
        ids.splice(0, ids.length, 8, 9);
        window.second = controller.step(1);
    }''')
    assert page.evaluate("requests.map(r => r.url)") == [
        "/api/photos/1", "/api/photos/2", "/api/photos/2", "/api/photos/3",
    ]
    page.evaluate('''async () => {
        requests[2].resolve({id: 2, filename: 'second.jpg'});
        requests[3].resolve({id: 3, filename: 'third.jpg'});
        await second;
        requests[0].resolve({id: 1, filename: 'stale.jpg'});
        requests[1].resolve({id: 2, filename: 'stale.jpg'});
        await first;
    }''')
    expect(page.locator("#browseCompareNameA")).to_have_text("second.jpg")
    expect(page.locator("#browseCompareNameB")).to_have_text("third.jpg")
    expect(page.locator("#browseCompareCount")).to_have_text("2-3 of 3")
    expect(page.locator("#browseCompareNext")).to_be_disabled()
    assert page.evaluate("[locks, esc.size]") == [1, 1]
    page.evaluate("controller.close(); controller.close()")
    assert page.evaluate("[locks, esc.size]") == [0, 0]
    assert page.evaluate("typeof browseCompareIds") == "undefined"


def test_compare_close_invalidates_pending_load_and_original_probe(page):
    mount_compare(page)
    page.evaluate('''async () => {
        window.pending = controller.open([1, 2]);
        controller.close();
        requests[0].resolve({id: 1, filename: 'closed.jpg'});
        requests[1].resolve({id: 2, filename: 'closed.jpg'});
        await pending;
    }''')
    expect(page.locator("#browseCompareNameA")).to_have_text("Loading...")
    page.evaluate('''async () => {
        const pending = controller.open([1, 2]);
        requests[2].resolve({id: 1, filename: 'current.jpg'});
        requests[3].resolve({id: 2, filename: 'other.jpg'});
        await pending;
    }''')
    page.locator("#browseCompareWrapA").dispatch_event("dblclick")
    assert page.evaluate("probes.length") == 1
    page.evaluate('''async () => {
        window.staleCallback = probes[0].onload;
        controller.destroy();
        const pending = controller.open([1, 2]);
        requests[4].resolve({id: 1, filename: 'reopened.jpg'});
        requests[5].resolve({id: 2, filename: 'other.jpg'});
        await pending;
        staleCallback();
    }''')
    expect(page.locator("#browseCompareImgA")).to_have_attribute("src", "/photos/1/full")
    expect(page.locator("#browseCompareZoomA")).to_have_text("Fit")
    # Closing and reopening must not duplicate the zoom listeners.
    page.locator("#browseCompareWrapA").dispatch_event("dblclick")
    expect(page.locator("#browseCompareZoomA")).to_have_text("200%")
    assert page.evaluate("[locks, esc.size]") == [1, 1]


def test_compare_replacing_pane_invalidates_probe_started_during_load(page):
    mount_compare(page)
    page.evaluate('''async () => {
        const first = controller.open([1, 2]);
        requests[0].resolve({id: 1});
        requests[1].resolve({id: 2});
        await first;
        window.reopened = controller.open([1, 2]);
    }''')
    # The outgoing bitmap is still visible while replacement metadata loads.
    page.locator("#browseCompareWrapA").dispatch_event("dblclick")
    page.evaluate('''async () => {
        const stale = probes[0].onload;
        requests[2].resolve({id: 1});
        requests[3].resolve({id: 2});
        await reopened;
        stale();
    }''')
    expect(page.locator("#browseCompareImgA")).to_have_attribute("src", "/photos/1/full")
    expect(page.locator("#browseCompareImgA")).to_have_attribute("data-original-loaded", "false")
    assert page.evaluate("[locks, esc.size]") == [1, 1]


def mount_workspace_switcher(page):
    page.set_content('''
        <div id="wsDropdown"><div id="wsMenu"><div id="wsMenuList"></div></div></div>
        <span id="wsCurrentName"></span>
        <div id="createWsModal"><input id="newWsName"><div id="newWsError"></div></div>
    ''')
    page.add_script_tag(path=str(STATIC / "vireo-workspace-switcher.js"))
    page.evaluate('''() => {
        window.requests = [];
        window.controller = VireoWorkspaceSwitcher.create({
            fetch: url => new Promise(resolve => requests.push({url, resolve})),
            navigate() {},
            clearWorkspaceCursors() {},
        });
    }''')


def test_workspace_menu_discards_responses_after_close_and_reopen(page):
    mount_workspace_switcher(page)
    page.evaluate('''async () => {
        window.first = controller.toggle();
        controller.close();
        window.second = controller.toggle();
        requests[3].resolve([{id: 2, name: 'Current workspace'}]);
        await second;
        requests[2].resolve({id: 2});
    }''')
    expect(page.locator(".ws-menu-item.active")).to_have_text("Current workspace✓☆")
    page.evaluate('''async () => {
        requests[0].resolve({id: 1});
        requests[1].resolve([{id: 1, name: 'Stale workspace'}]);
        await first;
    }''')
    expect(page.locator("#wsMenuList")).not_to_contain_text("Stale")
    expect(page.locator(".ws-menu-item.active")).to_contain_text("Current workspace")
    # An outside click synchronizes state as well as the DOM, so one toggle opens it.
    page.locator("#wsCurrentName").dispatch_event("click")
    expect(page.locator("#wsMenu")).not_to_have_class("open")
    page.evaluate("() => { controller.toggle(); }")
    expect(page.locator("#wsMenu")).to_have_class("open")
    assert page.evaluate("typeof _wsDropdownOpen") == "undefined"


def test_workspace_modal_closes_menu_and_cancels_deferred_listener(page):
    mount_workspace_switcher(page)
    page.evaluate('''() => {
        window.added = 0;
        window.removed = 0;
        const add = document.addEventListener.bind(document);
        const remove = document.removeEventListener.bind(document);
        document.addEventListener = (type, ...args) => {
            if (type === 'click') added++;
            add(type, ...args);
        };
        document.removeEventListener = (type, ...args) => {
            if (type === 'click') removed++;
            remove(type, ...args);
        };
        controller.toggle();
        controller.showCreate();
    }''')
    page.evaluate("() => new Promise(resolve => setTimeout(resolve, 0))")
    expect(page.locator("#wsMenu")).not_to_have_class("open")
    expect(page.locator("#createWsModal")).to_have_class("open")
    expect(page.locator("#newWsName")).to_be_focused()
    assert page.evaluate("added") == 0
    page.evaluate("controller.hideCreate(); controller.createWorkspace()")
    expect(page.locator("#newWsError")).to_have_text("Name is required")
    assert page.evaluate("requests.length") == 2  # Empty names never reach the API.
    page.evaluate("() => { controller.toggle(); }")
    page.evaluate("() => new Promise(resolve => setTimeout(resolve, 0))")
    assert page.evaluate("added") == 1
    page.evaluate("controller.showCreate()")
    assert page.evaluate("removed") >= 2
    expect(page.locator("#wsMenu")).not_to_have_class("open")


def test_workspace_create_entry_points_and_navigation(live_server, page):
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(f"{live_server['url']}/workspace")
    page.locator('.content button[onclick*="vireoWorkspaceSwitcher.showCreate"]').click()
    expect(page.locator("#createWsModal")).to_have_class("modal-overlay open")
    page.locator("#createWsModal").get_by_role("button", name="Cancel").click()
    page.evaluate("handleNativeMenuCommand('new_workspace')")
    expect(page.locator("#newWsName")).to_be_focused()
    page.locator("#newWsName").fill("Controller workspace")
    page.locator("#createWsModal").get_by_role("button", name="Create", exact=True).click()
    expect(page).to_have_url(f"{live_server['url']}/pipeline")
    expect(page.locator("#wsCurrentName")).to_have_text("Controller workspace")
    assert errors == []


def test_workspace_pin_and_rescan_use_controller(live_server, page):
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(f"{live_server['url']}/browse")
    page.evaluate("handleNativeMenuCommand('open_workspace')")
    field = page.locator(".ws-menu-item", has_text="Field Work")
    field.get_by_title("Pin workspace", exact=True).click()
    expect(field.get_by_title("Unpin workspace", exact=True)).to_be_visible()
    expect(page.locator("#wsCurrentName")).to_have_text("Default")
    page.locator("#wsMenu").get_by_role("button", name="Rescan folders", exact=False).click()
    expect(page.locator("#rescanModal")).to_have_class("modal-overlay open")
    expect(page.locator("#wsMenu")).not_to_have_class("ws-menu open")
    page.locator("#rescanModal").get_by_role("button", name="Cancel").click()
    page.locator("#wsCurrentBtn").click()
    expect(field).to_be_visible()
    assert errors == []


def test_lightbox_reopen_rejects_old_metadata_and_image_callbacks(live_server, page):
    """Same-photo reopen must not revive callbacks from the previous session."""
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.route("**/photos/*/full*", lambda route: route.fulfill(
        body='<svg xmlns="http://www.w3.org/2000/svg" width="800" height="400"></svg>',
        content_type="image/svg+xml",
    ))
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate('''() => {
        const photo = photos[0];
        const realFetch = window.fetch;
        window.metadataRequests = [];
        window.fetch = (url, ...args) => {
            if (url !== '/api/photos/' + photo.id) return realFetch(url, ...args);
            return new Promise(resolve => metadataRequests.push(data => resolve(
                new Response(JSON.stringify({...photo, ...data}), {
                    headers: {'Content-Type': 'application/json'}
                })
            )));
        };
        openLightbox(photo.id, photo.filename, [photo]);
        const image = document.getElementById('lightboxImg');
        const staleImageLoad = image.onload;
        closeLightbox();
        openLightbox(photo.id, photo.filename, [photo]);
        const currentImageLoad = image.onload;
        staleImageLoad();
        if (image.onload !== currentImageLoad) throw new Error('Stale callback replaced the current loader');
        metadataRequests[1]({width: 800, height: 400, keywords: [{name: 'Current metadata'}]});
    }''')
    expect(page.locator("#lightboxKeywords")).to_contain_text("Current metadata")
    page.wait_for_function("!_lbInitialDecodePending && !_lbVisualTransitionPending")
    page.evaluate('''async () => {
        metadataRequests[0]({width: 7777, height: 8888, keywords: [{name: 'Stale metadata'}]});
        await new Promise(resolve => setTimeout(resolve, 0));
    }''')
    expect(page.locator("#lightboxKeywords")).not_to_contain_text("Stale metadata")
    assert page.evaluate("_lbPhotoW") == 800
    assert page.evaluate("typeof _lightboxCurrentId") == "undefined"
    assert page.evaluate("typeof _lbAdjacentPreloads") == "undefined"
    page.evaluate("closeLightbox(); closeLightbox()")
    assert page.evaluate("vireoLightboxSession.requestedPhotoId()") is None
    assert errors == []
