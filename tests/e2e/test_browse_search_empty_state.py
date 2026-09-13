import json
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen

from playwright.sync_api import expect


def test_browse_init_reads_folder_health_from_single_snapshot(
    live_server, monkeypatch
):
    """Every health-sensitive init read must share one SQLite snapshot."""
    from db import Database

    reached_photo_query = threading.Event()
    release_photo_query = threading.Event()
    original_get_photos = Database.get_photos

    def _hold_after_photos_read(self, *args, **kwargs):
        rows = original_get_photos(self, *args, **kwargs)
        reached_photo_query.set()
        assert release_photo_query.wait(timeout=5)
        return rows

    monkeypatch.setattr(Database, "get_photos", _hold_after_photos_read)

    def _request_init():
        with urlopen(
            f"{live_server['url']}/api/browse/init?per_page=50", timeout=10
        ) as response:
            return json.loads(response.read())

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_request_init)
        assert reached_photo_query.wait(timeout=5), "init never read its photo page"

        # Commit a health transition after the missing-id/photo reads but
        # before the endpoint continues to get_folder_tree(). Without the
        # explicit read transaction the response mixes five old-snapshot
        # photos and [] missing IDs with a post-transition one-folder tree.
        missing_id = live_server["data"]["folders"][0]
        live_server["db"].conn.execute(
            "UPDATE folders SET status = 'missing' WHERE id = ?", (missing_id,)
        )
        live_server["db"].conn.commit()
        release_photo_query.set()
        payload = future.result(timeout=10)

    assert payload["missing_folder_ids"] == []
    assert len(payload["photos"]) == 5
    assert {row["id"] for row in payload["folders"]} == set(
        live_server["data"]["folders"]
    )


def test_folder_health_refresh_preserves_active_collection(live_server, page, tmp_path):
    """A folder-health refresh must not kick the user out of a collection scope.

    Regression: ``resetAndLoad()``'s default clears ``activeCollectionId``
    for non-dashboard scopes, and the previous refresh path also bumped
    ``browseScopeGen``. Both would combine to drop the user back to the
    unscoped workspace grid when a drive or share reconnected while they
    were viewing a normal collection — either during the brief
    bootstrap window before ``filterByCollection`` swallows the id into
    filter chips, or via the chip path being wiped by the reset
    (CodeRabbit review r3684913393, Codex review r3684907518).
    """
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    # park is repointed to a real path so the health check confirms it's
    # ok (no status change); yard is marked missing so the check flips it
    # back to ok and broadcasts vireo:folder-health-changed exactly once.
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(park), folder_ids[0]),
    )
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'missing' WHERE id = ?",
        (str(yard), folder_ids[1]),
    )
    db.conn.commit()

    rules = json.dumps([{"field": "extension", "op": "is", "value": ".jpg"}])
    collection_id = db.add_collection("All JPGs", rules)

    # Deep-link into the collection so bootstrap scopes the first paint
    # through ``activeCollectionId`` and then loads the saved expression
    # into the filter bar. yard is still missing, so only park's 3 hawks
    # show.
    page.goto(f"{live_server['url']}/browse?collection_id={collection_id}")
    page.wait_for_function("window.VireoFilter && VireoFilter.isReady()")
    page.wait_for_function(
        "document.querySelector('.vf-chips') && "
        "document.querySelector('.vf-chips').textContent.includes('File extension')",
        timeout=4000,
    )
    expect(page.locator(".grid-card")).to_have_count(3)

    # Health-refresh path must call resetAndLoad with preserveCollection so
    # the option's short-circuit is exercised directly. If a future refactor
    # drops the flag, this asserts the option is still honored.
    result = page.evaluate(
        f"(async () => {{"
        f"  activeCollectionId = {collection_id};"
        f"  await resetAndLoad({{ preserveCollection: true }});"
        f"  return activeCollectionId;"
        f"}})()"
    )
    assert result == collection_id

    with page.expect_response(
        lambda response: response.url.endswith("/api/folders/check-health")
    ) as health_response:
        page.evaluate("openMissingFoldersModal()")
    assert health_response.value.json()["changed"] == 1

    # The collection scope must survive the refresh: the collection's
    # rules stay in the filter bar and the grid now includes yard's
    # photos because the collection matches every .jpg.
    expect(page.locator(".grid-card")).to_have_count(5)
    expect(page.locator(".vf-chips")).to_contain_text("File extension")


def test_reconnected_folders_refresh_empty_browse(live_server, page, tmp_path):
    """A health check that restores folders must repopulate an open Browse."""
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'missing' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse")
    expect(page.locator("#welcomeState")).to_be_visible()
    expect(page.locator(".grid-card")).to_have_count(0)

    with page.expect_response(
        lambda response: response.url.endswith("/api/folders/check-health")
    ) as health_response:
        page.evaluate("openMissingFoldersModal()")

    assert health_response.value.json()["changed"] == 2
    expect(page.locator(".grid-card")).to_have_count(5)
    expect(page.locator("#welcomeState")).to_be_hidden()
    expect(page.locator("#filterSummary")).to_contain_text("of 5")


def test_background_health_recovery_refreshes_browse(live_server, page, tmp_path):
    """The 10-minute /api/folders/missing poll must refresh Browse when a
    server-side background reconnect flips a folder missing→ok.

    Regression: the folder-health event only fired from the modal's
    /api/folders/check-health POST. The server's own _folder_health_loop
    runs independently every 10 minutes and can restore a folder with no
    client involvement; without a diff in the periodic
    ``checkMissingFolders()`` poll a long-lived Browse view kept its
    pre-flip empty grid until the user reopened the modal or reloaded
    (Codex review r3685083009).
    """
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'missing' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse")
    expect(page.locator("#welcomeState")).to_be_visible()
    expect(page.locator(".grid-card")).to_have_count(0)

    # Wait for the initial poll snapshot to be recorded — otherwise the
    # next checkMissingFolders() would see _missingFoldersLastIds === null
    # and treat the first-since-load state as "no change".
    page.wait_for_function(
        "typeof _missingFoldersLastIds !== 'undefined' && "
        "_missingFoldersLastIds !== null && _missingFoldersLastIds.length === 2"
    )

    # Simulate the server-side _folder_health_loop restoring both folders.
    # This mutates DB state without ever touching /api/folders/check-health.
    db.conn.execute(
        "UPDATE folders SET status = 'ok' WHERE id IN (?, ?)",
        (folder_ids[0], folder_ids[1]),
    )
    db.conn.commit()

    # Fire the periodic poll manually (the real code runs it on a 10-minute
    # interval). It must diff the missing set, detect the missing→ok
    # transition, and dispatch vireo:folder-health-changed.
    page.evaluate("checkMissingFolders()")

    expect(page.locator(".grid-card")).to_have_count(5)
    expect(page.locator("#welcomeState")).to_be_hidden()
    expect(page.locator("#filterSummary")).to_contain_text("of 5")


def test_relocation_refreshes_browse_when_check_health_reports_no_change(
    live_server, page, tmp_path
):
    """Relocating a missing folder via the modal must refresh Browse even
    though ``/api/folders/check-health`` reports ``changed=0``.

    ``/api/folders/<id>/relocate`` flips the folder's status to ``ok``
    before ``loadMissingFolders()`` runs its check-health POST, so the
    server sees no transition and ``data.changed`` is zero. The
    workspace's missing set still shrank, and Browse must repopulate —
    otherwise the just-relocated folder's photos stay hidden until the
    next 10-minute poll or a manual reload (Codex review r3685295405).
    """
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    # park is missing (pointed at a non-existent path); yard is ok and
    # anchors its 2 photos in the initial grid so the reappearance of
    # park's 3 photos is visible in the count.
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'missing' WHERE id = ?",
        (str(tmp_path / "gone-park"), folder_ids[0]),
    )
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(yard), folder_ids[1]),
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse")
    # Wait for the initial /api/folders/missing poll so
    # _missingFoldersLastIds has park's id as its baseline; otherwise the
    # workspace-scoped ID diff has nothing to compare against and the
    # post-relocate check-health response would be treated as first-load
    # (no dispatch).
    page.wait_for_function(
        f"typeof _missingFoldersLastIds !== 'undefined' && "
        f"_missingFoldersLastIds !== null && "
        f"_missingFoldersLastIds.length === 1 && "
        f"_missingFoldersLastIds[0] === {folder_ids[0]}"
    )
    expect(page.locator(".grid-card")).to_have_count(2)

    # Simulate what /api/folders/<id>/relocate does server-side: flip
    # park's status back to ``ok``. The real endpoint uses ``let``-scoped
    # module state that page.evaluate cannot reach; the post-relocate
    # UI path (confirmRelocate → loadMissingFolders) is what matters
    # for this regression, so re-enter it directly below.
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(park), folder_ids[0]),
    )
    db.conn.commit()

    with page.expect_response(
        lambda response: response.url.endswith("/api/folders/check-health")
    ) as health_response:
        page.evaluate("loadMissingFolders()")

    # check-health sees no transition — park is already ``ok`` from the
    # (simulated) relocate. Without the workspace-scoped ID-diff dispatch
    # this would leave Browse showing only yard's 2 photos.
    assert health_response.value.json()["changed"] == 0
    expect(page.locator(".grid-card")).to_have_count(5)
    expect(page.locator("#filterSummary")).to_contain_text("of 5")


def test_check_health_dispatch_ignores_cross_workspace_flips(
    live_server, page, tmp_path
):
    """A folder flip in another workspace must not reset the active Browse.

    ``db.check_folder_health()`` scans every folder in the database and
    returns a global change count, while ``data.missing`` and Browse are
    scoped to the active workspace. Dispatching on ``data.changed`` would
    then blow away the active selection whenever a folder in an
    unrelated workspace transitioned (Codex review r3685295409).
    """
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(park), folder_ids[0]),
    )
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(yard), folder_ids[1]),
    )

    # Add a folder linked only to Field Work whose disk path is already
    # gone — the next check_folder_health() will flip its status to
    # ``missing`` and bump the global change count without affecting the
    # active Default workspace's missing set.
    field_ws = db.conn.execute(
        "SELECT id FROM workspaces WHERE name = 'Field Work'"
    ).fetchone()["id"]
    other_folder_id = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(tmp_path / "gone-field-folder"), "field"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id, is_root) "
        "VALUES (?, ?, 1)",
        (field_ws, other_folder_id),
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function(
        "typeof _missingFoldersLastIds !== 'undefined' && "
        "_missingFoldersLastIds !== null && _missingFoldersLastIds.length === 0"
    )
    expect(page.locator(".grid-card")).to_have_count(5)

    # Instrument the dispatch: the event must NOT fire, since only the
    # Field Work folder transitioned and the active workspace's missing
    # set is still empty.
    page.evaluate(
        "window._folderHealthEvents = 0;"
        "document.addEventListener('vireo:folder-health-changed', "
        "  function() { window._folderHealthEvents++; });"
    )

    with page.expect_response(
        lambda response: response.url.endswith("/api/folders/check-health")
    ) as health_response:
        page.evaluate("openMissingFoldersModal()")

    assert health_response.value.json()["changed"] == 1
    assert page.evaluate("window._folderHealthEvents") == 0
    # And the grid stays exactly as it was — no reset, no scope loss.
    expect(page.locator(".grid-card")).to_have_count(5)


def test_bootstrap_defers_load_lock_release_during_health_refresh(
    live_server, page, tmp_path
):
    """Bootstrap must not release the load lock owned by a concurrent
    health refresh.

    Regression (Codex review r3685627307): if a folder-health event
    fires while ``/api/browse/init`` is in flight, the health-refresh
    handler runs ``resetAndLoad()`` → ``loadPhotos()`` which owns the
    ``loading`` mutex. When init later returns, bootstrap already
    guarded the render block on ``healthChangedDuringInit`` — but its
    finally-region still unconditionally cleared ``loading`` and rearmed
    the intersection observer. That released the mutex the concurrent
    ``loadPhotos`` still held, letting the observer fire a duplicate
    page-1 request against the same ``loadEpoch`` and appending the same
    photos twice.
    """
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    # Both folders missing initially so bootstrap's /api/browse/init
    # returns an empty grid; the simulated health event then restores
    # them and triggers the refresh path.
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'missing' WHERE id = ?",
        [
            (str(tmp_path / "gone-park"), folder_ids[0]),
            (str(tmp_path / "gone-yard"), folder_ids[1]),
        ],
    )
    db.conn.commit()

    # Hold /api/browse/init so the health event fires strictly during
    # its in-flight window. Only intercept the FIRST init request —
    # bootstrap re-issues nothing, but we don't want to catch anything
    # else if the router matches loosely.
    held_init = []

    def _hold_first_init(route):
        if not held_init:
            held_init.append(route)
        else:
            route.continue_()

    page.route("**/api/browse/init**", _hold_first_init)

    page.goto(f"{live_server['url']}/browse")
    for _ in range(50):
        if held_init:
            break
        page.wait_for_timeout(100)
    assert held_init, "init request was never issued"

    # Restore the folders in the DB, then dispatch the event exactly the
    # way loadMissingFolders/checkMissingFolders would. The refresh
    # handler will reload folders/keywords/collections and then call
    # resetAndLoad → loadPhotos.
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', "
        "{detail: {restored: [], wentMissing: [], source: 'test'}}))"
    )

    # Wait for the health-refresh chain's loadPhotos to populate the
    # grid. The 5 photos come from the two restored folders.
    page.wait_for_function("photos.length === 5", timeout=15000)

    # Now release init. Bootstrap sees healthChangedDuringInit=true and
    # must skip both the render block AND the load-lock release; the
    # health refresh's own loadPhotos finally will clear ``loading``.
    held_init[0].continue_()

    # Give bootstrap's remaining code (VireoFilter.init promise chain +
    # finally region) a chance to run so any load-lock release would land
    # before we assert.
    page.wait_for_timeout(400)

    # The grid must show exactly 5 photos with no duplicates. If bootstrap
    # had released the mutex, the observer could have double-fetched page 1
    # and produced 10 (or 5 with duplicates in ``photos``).
    expect(page.locator(".grid-card")).to_have_count(5)
    unique_photos = page.evaluate("new Set(photos.map(p => p.id)).size")
    assert unique_photos == page.evaluate("photos.length"), (
        "duplicate photos in grid — bootstrap released the load lock and "
        "the intersection observer double-fetched page 1"
    )


def test_unrelated_health_event_initializes_pending_folder_bootstrap(
    live_server, page, tmp_path
):
    """An unrelated health transition must not preserve an unpainted grid."""
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()

    held_init = []

    def _hold_first_init(route):
        if not held_init:
            held_init.append(route)
        else:
            route.continue_()

    page.route("**/api/browse/init**", _hold_first_init)
    page.goto(f"{live_server['url']}/browse?folder_id={folder_ids[0]}")
    for _ in range(50):
        if held_init:
            break
        page.wait_for_timeout(100)
    assert held_init, "folder-scoped init request was never issued"
    assert page.evaluate("browseDatasetReady") is False

    # Yard changes while park's first paint is still held. The park scope is
    # unaffected, but there is no initialized grid to preserve: the health
    # refresh must take over the initial park-scoped load.
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?",
        (folder_ids[1],),
    )
    db.conn.commit()
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', "
        f"{{detail: {{restored: [], wentMissing: [{folder_ids[1]}], "
        "source: 'test'}}))"
    )
    page.evaluate("() => window._activeFolderHealthRefresh")

    held_init[0].continue_()
    page.wait_for_function(
        "browseDatasetReady && !loading && photos.length === 3",
        timeout=10000,
    )
    assert page.evaluate("activeFolderId") == folder_ids[0]
    expect(page.locator(".grid-card")).to_have_count(3)


def test_bootstrap_seeds_missing_snapshot_from_init_response(
    live_server, page, tmp_path
):
    """/api/browse/init must seed ``_missingFoldersLastIds`` so the first
    /api/folders/missing observation has a baseline to diff against.

    Regression (Codex review r3686191141): if the background
    ``_folder_health_loop`` flips folders in the active workspace
    between ``/api/browse/init`` and the first ``/api/folders/missing``
    poll, the poll used to return with ``_missingFoldersLastIds ===
    null`` and its null-baseline early-return silently swallowed the
    transition. Later polls then saw the post-flip IDs equal to the
    baseline set by that first poll and never dispatched, leaving the
    just-rendered Browse grid stuck showing pre-flip state until another
    transition or a reload. Seeding the baseline from init closes the
    window.
    """
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    # park is missing at init time; the init response's
    # ``missing_folder_ids`` must include park's id so the client seeds
    # its snapshot with [park].
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'missing' WHERE id = ?",
        (str(tmp_path / "gone-park"), folder_ids[0]),
    )
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(yard), folder_ids[1]),
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse")

    # After bootstrap settles, the snapshot must equal init's
    # workspace-scoped missing set — [park] — regardless of whether the
    # navbar's own initial poll landed first or the init response did.
    page.wait_for_function(
        f"typeof _missingFoldersLastIds !== 'undefined' && "
        f"_missingFoldersLastIds !== null && "
        f"_missingFoldersLastIds.length === 1 && "
        f"_missingFoldersLastIds[0] === {folder_ids[0]}"
    )

    # Instrument dispatches so we can verify the next poll transitions
    # the seeded [park] → [] state into a folder-health-changed event.
    page.evaluate(
        "window._folderHealthEvents = [];"
        "document.addEventListener('vireo:folder-health-changed', "
        "  function(e) { window._folderHealthEvents.push(e.detail.source); });"
    )

    # Server-side background loop restores park (no client involvement,
    # no /api/folders/check-health POST). The next poll must diff the
    # seeded [park] against the newly-observed [] and dispatch, so
    # Browse repopulates with park's photos.
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(park), folder_ids[0]),
    )
    db.conn.commit()

    page.evaluate("checkMissingFolders()")

    page.wait_for_function(
        "window._folderHealthEvents.length > 0", timeout=5000
    )
    assert page.evaluate("_missingFoldersLastIds") == []
    expect(page.locator(".grid-card")).to_have_count(5)


def test_bootstrap_adopts_init_when_poll_baseline_is_known_older(
    live_server, page, tmp_path
):
    """A poll completed before init starts must not override the newer init."""
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'missing' WHERE id = ?",
        (str(tmp_path / "gone-park"), folder_ids[0]),
    )
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(yard), folder_ids[1]),
    )
    db.conn.commit()

    # Hold the config promise so the navbar's initial missing-folder poll can
    # finish before bootstrap issues /api/browse/init.
    held_cfg = []

    def _hold_all_cfg(route):
        if route.request.method == "GET":
            held_cfg.append(route)
        else:
            route.continue_()

    page.route("**/api/config", _hold_all_cfg)
    page.goto(f"{live_server['url']}/browse")
    for _ in range(50):
        if held_cfg:
            break
        page.wait_for_timeout(100)
    assert held_cfg, "/api/config was never requested"
    page.wait_for_function(
        f"_missingFoldersLastIds !== null && "
        f"_missingFoldersLastIds.length === 1 && "
        f"_missingFoldersLastIds[0] === {folder_ids[0]}"
    )
    poll_version = page.evaluate("_missingFoldersSnapshotVersion")
    page.evaluate(
        "window._folderHealthEvents = [];"
        "document.addEventListener('vireo:folder-health-changed', "
        "function(e) { window._folderHealthEvents.push(e.detail.source); });"
    )

    # Restore park after the poll observation but before init starts. Init is
    # now the known-newer snapshot and should be adopted directly, not treated
    # as stale and reconciled backwards to the old poll baseline.
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(park), folder_ids[0]),
    )
    db.conn.commit()
    page.unroute("**/api/config", _hold_all_cfg)

    page.wait_for_function("browseDatasetReady", timeout=10000)
    state = page.evaluate(
        "({photoCount: photos.length, missingIds: _missingFoldersLastIds, "
        "events: window._folderHealthEvents, "
        "snapshotVersion: _missingFoldersSnapshotVersion})"
    )
    assert state["photoCount"] == 5, state
    assert state["missingIds"] == [], state
    assert page.evaluate("_missingFoldersSnapshotVersion") > poll_version
    assert state["events"] == []
    assert page.locator("#missingFoldersBanner").evaluate(
        "(el) => getComputedStyle(el).display"
    ) == "none"
    expect(page.locator(".grid-card")).to_have_count(5)


def test_bootstrap_snapshot_invalidates_older_inflight_poll(
    live_server, page, tmp_path
):
    """A poll response read before init must not overwrite init's baseline."""
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()

    # Hold config so the navbar poll can read the old [] state before init
    # starts. route.fetch() captures that response now; delaying fulfill
    # simulates a slow delivery after the newer init snapshot is applied.
    held_cfg = []
    held_missing = []

    def _hold_all_cfg(route):
        held_cfg.append(route)

    def _capture_then_hold_missing(route):
        held_missing.append((route, route.fetch()))

    page.route("**/api/config", _hold_all_cfg)
    page.route("**/api/folders/missing", _capture_then_hold_missing)
    page.goto(f"{live_server['url']}/browse")
    for _ in range(50):
        if held_cfg and held_missing:
            break
        page.wait_for_timeout(100)
    assert held_cfg, "/api/config was never requested"
    assert held_missing, "/api/folders/missing was never requested"

    # Init begins after the transition and installs [park] as the newer
    # baseline while the old poll response remains undelivered.
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (folder_ids[0],)
    )
    db.conn.commit()
    for route in held_cfg:
        route.continue_()
    page.unroute("**/api/config", _hold_all_cfg)
    page.wait_for_function(
        f"browseDatasetReady && _missingFoldersLastIds.length === 1 && "
        f"_missingFoldersLastIds[0] === {folder_ids[0]}",
        timeout=10000,
    )
    expect(page.locator(".grid-card")).to_have_count(2)

    page.evaluate(
        "window._folderHealthEvents = [];"
        "document.addEventListener('vireo:folder-health-changed', "
        "function(e) { window._folderHealthEvents.push(e.detail.source); });"
    )
    held_missing[0][0].fulfill(response=held_missing[0][1])
    page.wait_for_timeout(300)

    assert page.evaluate("_missingFoldersLastIds") == [folder_ids[0]]
    assert page.evaluate("window._folderHealthEvents") == []
    assert page.locator("#missingFoldersBanner").evaluate(
        "(el) => getComputedStyle(el).display"
    ) == "flex"
    expect(page.locator(".grid-card")).to_have_count(2)


def test_newer_init_server_snapshot_supersedes_delivered_old_poll(
    live_server, page, tmp_path
):
    """Server observation order wins when the old poll is delivered first."""
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()

    # Hold real fetch responses in page JavaScript. Playwright route handlers
    # are serialized, so trying to hold both responses at that layer deadlocks
    # the second request instead of producing the intended delivery ordering.
    page.add_init_script(
        """
        (() => {
          const originalFetch = window.fetch.bind(window);
          const controls = {
            configs: [],
            configsReleased: false,
            missing: null,
            init: null,
          };
          window._snapshotFetchControls = controls;
          window.fetch = function(input, options) {
            const url = typeof input === 'string' ? input : input.url;
            let kind = null;
            if (url.indexOf('/api/config') !== -1) kind = 'config';
            else if (url.indexOf('/api/folders/missing') !== -1) kind = 'missing';
            else if (url.indexOf('/api/browse/init') !== -1) kind = 'init';
            if (!kind) return originalFetch(input, options);
            return new Promise((resolve, reject) => {
              originalFetch(input, options).then(response => {
                const held = {response: response, release: () => resolve(response)};
                if (kind === 'config') {
                  if (controls.configsReleased) held.release();
                  else controls.configs.push(held);
                } else {
                  controls[kind] = held;
                }
              }, reject);
            });
          };
        })();
        """
    )
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function(
        "_snapshotFetchControls.configs.length > 0 && "
        "_snapshotFetchControls.missing !== null",
        timeout=5000,
    )

    # The poll has already observed version N with no missing folders. Move
    # the server to N+1, then let init read and hold that newer response.
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (folder_ids[0],)
    )
    db.conn.commit()
    page.evaluate(
        "_snapshotFetchControls.configsReleased = true;"
        "_snapshotFetchControls.configs.splice(0).forEach(item => item.release());"
    )
    page.wait_for_function(
        "_snapshotFetchControls.init !== null", timeout=5000
    )

    # Deliver the old poll before applying the already-read newer init. A
    # client-only request-generation scheme accepts this [] baseline; the
    # init response must still supersede it via its higher server version.
    page.evaluate("_snapshotFetchControls.missing.release()")
    page.wait_for_function(
        "_missingFoldersLastIds !== null && "
        "_missingFoldersLastIds.length === 0"
    )
    old_server_version = page.evaluate("_missingFoldersServerVersion")

    page.evaluate(
        "window._folderHealthEvents = [];"
        "document.addEventListener('vireo:folder-health-changed', "
        "function(e) { window._folderHealthEvents.push(e.detail.source); });"
    )
    page.evaluate("_snapshotFetchControls.init.release()")
    page.wait_for_function(
        f"browseDatasetReady && _missingFoldersLastIds.length === 1 && "
        f"_missingFoldersLastIds[0] === {folder_ids[0]}",
        timeout=10000,
    )

    assert page.evaluate("_missingFoldersServerVersion") > old_server_version
    assert page.evaluate("window._folderHealthEvents") == []
    assert page.locator("#missingFoldersBanner").evaluate(
        "(el) => getComputedStyle(el).display"
    ) == "flex"
    expect(page.locator(".grid-card")).to_have_count(2)


def test_older_init_version_reconciles_when_missing_ids_match(live_server, page):
    """A stale init version must refresh even when both missing sets are []."""
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("browseDatasetReady", timeout=5000)

    result = page.evaluate(
        "() => {"
        "  window._folderHealthEvents = [];"
        "  document.addEventListener('vireo:folder-health-changed', "
        "    function(e) { window._folderHealthEvents.push(e.detail.source); }, "
        "    {once: true});"
        "  _missingFoldersLastIds = [];"
        "  _missingFoldersServerVersion = 12;"
        "  const startVersion = _missingFoldersSnapshotVersion;"
        "  const stale = _reconcileMissingFoldersInitSnapshot("
        "    [], startVersion, 'same-ids-version-reconcile', 11);"
        "  return {stale: stale, events: window._folderHealthEvents.slice()};"
        "}"
    )

    assert result == {
        "stale": True,
        "events": ["same-ids-version-reconcile"],
    }
    page.evaluate("() => window._activeFolderHealthRefresh")


def test_bootstrap_defers_lock_release_when_init_rejects(
    live_server, page, tmp_path
):
    """Bootstrap must not release the load lock when /api/browse/init
    rejects while a concurrent health refresh owns it.

    Regression (Codex review r3686191138): the finally-region guard on
    ``healthChangedDuringInit`` used to be assigned only in the try's
    success path (after the init await). If a folder-health event fired
    during init AND init then rejected, the assignment was skipped and
    the guard treated the flag as false — clearing ``loading`` even
    though the health refresh's ``loadPhotos`` still held the mutex,
    letting the intersection observer fire a duplicate page-1 request
    against the same ``loadEpoch``.
    """
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'missing' WHERE id = ?",
        [
            (str(tmp_path / "gone-park"), folder_ids[0]),
            (str(tmp_path / "gone-yard"), folder_ids[1]),
        ],
    )
    db.conn.commit()

    # Fail the first init request with a 500 so bootstrap's catch runs;
    # allow subsequent /api/browse/init requests (there are none in normal
    # bootstrap flow, but keeps the route robust).
    aborted_init = []

    def _fail_first_init(route):
        if not aborted_init:
            aborted_init.append(route)
            route.fulfill(status=500, body='{"error": "simulated"}',
                          content_type="application/json")
        else:
            route.continue_()

    page.route("**/api/browse/init**", _fail_first_init)

    page.goto(f"{live_server['url']}/browse")
    for _ in range(50):
        if aborted_init:
            break
        page.wait_for_timeout(100)
    assert aborted_init, "init request was never issued"

    # Now restore the folders and dispatch the health-change event.
    # refreshBrowseAfterFolderHealthChange bumps folderHealthRefreshSeq,
    # reloads the sidebars, and runs resetAndLoad → loadPhotos.
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', "
        "{detail: {restored: [], wentMissing: [], source: 'test'}}))"
    )

    # Wait for the health refresh's loadPhotos to populate the grid.
    page.wait_for_function("photos.length === 5", timeout=15000)

    # Bootstrap's catch has already run (init rejected synchronously via
    # the 500). Its finally-region MUST have deferred to the concurrent
    # refresh — no duplicate cards, no double-fetch.
    expect(page.locator(".grid-card")).to_have_count(5)
    unique_photos = page.evaluate("new Set(photos.map(p => p.id)).size")
    assert unique_photos == page.evaluate("photos.length"), (
        "duplicate photos in grid — bootstrap's catch released the load "
        "lock even though the concurrent health refresh owned it"
    )


def test_check_health_null_baseline_ignores_cross_workspace_flip(
    live_server, page, tmp_path
):
    """The null-baseline fallback in loadMissingFolders() must gate on
    ``workspace_changed``, not on the global ``changed`` count.

    Regression (Codex review r3686191131): when the user opened the
    missing-folders modal before any /api/folders/missing poll or
    /api/browse/init seeded ``_missingFoldersLastIds``, the modal POST
    ran with ``prevIds === null`` and dispatched
    ``vireo:folder-health-changed`` whenever ``data.changed > 0`` — even
    when the transition was in another workspace. That would reset the
    active Browse grid, selection, and detail panel for a change that
    never touched the active dataset.
    """
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(park), folder_ids[0]),
    )
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(yard), folder_ids[1]),
    )

    # A folder linked only to Field Work whose disk path is gone — the
    # check-health scan will flip it to ``missing`` and bump the global
    # ``changed`` count without changing the active workspace's set.
    field_ws = db.conn.execute(
        "SELECT id FROM workspaces WHERE name = 'Field Work'"
    ).fetchone()["id"]
    other_folder_id = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(tmp_path / "gone-field-folder"), "field"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id, is_root) "
        "VALUES (?, ?, 1)",
        (field_ws, other_folder_id),
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse")
    # Wait for the grid to fully render.
    expect(page.locator(".grid-card")).to_have_count(5)

    # Force null-baseline: reset the snapshot AFTER load so the modal
    # POST hits the fallback branch. Then instrument the dispatch.
    page.evaluate(
        "_missingFoldersLastIds = null;"
        "window._folderHealthEvents = 0;"
        "document.addEventListener('vireo:folder-health-changed', "
        "  function() { window._folderHealthEvents++; });"
    )

    with page.expect_response(
        lambda response: response.url.endswith("/api/folders/check-health")
    ) as health_response:
        page.evaluate("openMissingFoldersModal()")

    # The server reports a global change (Field Work's folder flipped)
    # but the workspace-scoped diff is empty.
    body = health_response.value.json()
    assert body["changed"] == 1
    assert body["workspace_changed"] is False
    # The null-baseline fallback saw workspace_changed=false and did not
    # dispatch. The grid stays as-is; no reset, no scope loss.
    assert page.evaluate("window._folderHealthEvents") == 0
    expect(page.locator(".grid-card")).to_have_count(5)


def test_poll_defers_snapshot_update_to_in_flight_check_health_post(
    live_server, page, tmp_path
):
    """A GET /api/folders/missing whose response arrives while a
    check-health POST is in flight must not update the snapshot or
    dispatch vireo:folder-health-changed — the POST is the freshness
    authority for the post-mutation state.

    Regression (Codex review r3685627312): the previous fix used a
    single shared ``_missingFoldersObservationGen`` bumped by both GET
    and POST. A GET fired by ``closeMissingFoldersModal()`` after the
    modal's POST was still running would bump the gen, and the POST's
    equality check would then fail on completion — dropping the POST's
    result even though it's the request that actually mutated folder
    status. Give the POST its own mutation gen and an in-flight counter
    so GETs defer to it and only POSTs supersede POSTs.
    """
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'missing' WHERE id = ?",
        (str(tmp_path / "gone-park"), folder_ids[0]),
    )
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(yard), folder_ids[1]),
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse")
    # Wait for the initial /api/folders/missing poll to seed the snapshot
    # with park's id.
    page.wait_for_function(
        f"typeof _missingFoldersLastIds !== 'undefined' && "
        f"_missingFoldersLastIds !== null && "
        f"_missingFoldersLastIds.length === 1 && "
        f"_missingFoldersLastIds[0] === {folder_ids[0]}"
    )
    # Instrument event dispatches so we can count them.
    page.evaluate(
        "window._folderHealthEvents = [];"
        "document.addEventListener('vireo:folder-health-changed', "
        "  function(e) { window._folderHealthEvents.push(e.detail.source); });"
    )

    # Hold the check-health POST so a subsequent GET can race ahead.
    held_post = []

    def _hold_post(route):
        if route.request.method == "POST":
            held_post.append(route)
        else:
            route.continue_()

    page.route("**/api/folders/check-health", _hold_post)

    # Kick off the POST (loadMissingFolders → POST /api/folders/check-health).
    # Fire-and-forget: page.evaluate awaits any returned promise, and the
    # held POST would never resolve — so wrap the call so evaluate returns
    # undefined synchronously and lets us drive the release manually below.
    page.evaluate(
        "() => { window._pendingLoadMissing = loadMissingFolders(); }"
    )
    for _ in range(50):
        if held_post:
            break
        page.wait_for_timeout(100)
    assert held_post, "check-health POST was never issued"
    # In-flight counter must reflect the held POST.
    assert page.evaluate("_missingFoldersMutationInFlight") == 1

    # Restore park in the DB now, so the GET (issued next) observes the
    # post-restore state — worse than the pre-fix bug's exact ordering
    # but simpler to set up: even with a "fresher-looking" GET result,
    # the POST must still be applied as the authority. Under the old
    # single-gen scheme the GET would bump the gen and the POST's
    # completion check would fail, so the POST's dispatch would never
    # fire. With the fix, the GET is skipped while the POST is in flight.
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(park), folder_ids[0]),
    )
    db.conn.commit()

    events_before_get = page.evaluate("window._folderHealthEvents.length")
    snapshot_before_get = page.evaluate("[..._missingFoldersLastIds]")

    # Fire the racing GET and await its completion in-page — the GET's
    # URL (/api/folders/missing) isn't intercepted, so it returns fast.
    # By the time this evaluate resolves, the GET's response handler
    # has run and either updated state (bug) or deferred to the POST (fix).
    page.evaluate(
        "(async () => { await checkMissingFolders(); })()"
    )
    # Belt-and-suspenders: give any microtasks a chance to settle.
    page.wait_for_timeout(200)

    # The GET's response landed while the POST is still in flight — it
    # must have skipped snapshot update AND dispatch.
    assert (
        page.evaluate("[..._missingFoldersLastIds]") == snapshot_before_get
    ), "racing GET updated snapshot while POST was in flight"
    assert (
        page.evaluate("window._folderHealthEvents.length") == events_before_get
    ), "racing GET dispatched folder-health-changed while POST was in flight"
    # In-flight counter still shows the held POST.
    assert page.evaluate("_missingFoldersMutationInFlight") == 1

    # Now release the POST. It observes park as ok → dispatches
    # 'check-health' and updates the snapshot to [].
    held_post[0].continue_()
    page.wait_for_function(
        f"window._folderHealthEvents.length > {events_before_get}",
        timeout=10000,
    )
    events_after = page.evaluate("window._folderHealthEvents")
    assert "check-health" in events_after[events_before_get:], (
        "POST's dispatch was dropped even though it's the freshness authority"
    )
    assert page.evaluate("_missingFoldersLastIds") == [], (
        "POST's snapshot update was dropped even though it's the freshness authority"
    )
    # In-flight counter drops back to zero after POST settles.
    page.wait_for_function("_missingFoldersMutationInFlight === 0", timeout=5000)


def test_load_folders_generation_guard_prevents_stale_render(
    live_server, page, tmp_path
):
    """Two overlapping ``loadFolders()`` calls: the later one's response must
    win the folder-tree render even if the earlier one arrives last.

    Regression (Codex review r3686842772): ``refreshBrowseSidebarCounts()``
    and every mutation-triggered ``loadFolders()`` fire without any
    ``shouldRender`` guard. Without an internal generation counter shared
    across every caller, a slow pre-transition response can arrive after a
    guarded ``refreshBrowseAfterFolderHealthChange()`` has already
    repainted the tree with the freshly-restored folders — putting the
    just-cleared missing folder back into the sidebar and letting the user
    click it into an empty stale scope.
    """
    db = live_server["db"]
    folder_ids = live_server["data"]["folders"]

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible", timeout=5000)
    initial_count = page.evaluate(
        "document.querySelectorAll('#folderTree .tree-item').length"
    )
    assert initial_count >= 2, "test fixture must ship at least two folders"

    # Hold the FIRST post-bootstrap /api/folders response so we can fire a
    # newer loadFolders() while the older one is still in flight, then let
    # the older response come back last.
    held = []

    def _hold_first_folders(route):
        if route.request.method == "GET" and not held:
            held.append(route)
        else:
            route.continue_()

    # ``loadFolders()`` now fires ``/api/folders?with_workspace=1`` so the
    # tree and its workspace id come from one snapshot; a bare
    # ``**/api/folders`` glob no longer matches (Codex review r3799207269).
    page.route("**/api/folders?*", _hold_first_folders)

    # Kick off the older ("stale") call. Do NOT await the returned promise —
    # ``page.evaluate`` would otherwise block until the held response returns.
    page.evaluate("() => { window._staleLoad = loadFolders(); }")
    for _ in range(50):
        if held:
            break
        page.wait_for_timeout(100)
    assert held, "older /api/folders request was never issued"

    # Meanwhile, mark one folder missing so the second (newer) call reads a
    # shorter visible tree. Do not delete the row: fixture photos still
    # reference it and production health transitions preserve folder rows.
    # The newer call is unrouted, so its response returns immediately with
    # the mutated data.
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?",
        (folder_ids[-1],),
    )
    db.conn.commit()

    page.evaluate("() => { window._freshLoad = loadFolders(); }")
    # Wait for the newer render to repaint the tree with the shorter list.
    page.wait_for_function(
        f"document.querySelectorAll('#folderTree .tree-item').length "
        f"=== {initial_count - 1}",
        timeout=10000,
    )
    fresh_count = page.evaluate(
        "document.querySelectorAll('#folderTree .tree-item').length"
    )

    # Release the older, stale response. Under the pre-fix code the older
    # ``renderFolderTree(data)`` would clobber the tree back to its
    # pre-mutation shape; the generation guard must drop it silently.
    held[0].continue_()
    # Belt-and-suspenders: give the older response's handler a chance to run.
    page.wait_for_timeout(300)

    final_count = page.evaluate(
        "document.querySelectorAll('#folderTree .tree-item').length"
    )
    assert final_count == fresh_count, (
        f"stale loadFolders response repainted the tree "
        f"(expected {fresh_count} items, got {final_count})"
    )


def test_load_folders_renders_older_success_after_newer_failure(
    live_server, page
):
    """A failed superseder must not suppress the newest successful tree."""
    db = live_server["db"]
    folder_ids = live_server["data"]["folders"]
    page.goto(f"{live_server['url']}/browse")
    expect(page.locator("#folderTree .tree-item")).to_have_count(2)

    held = []
    request_count = 0

    def _hold_success_then_fail(route):
        nonlocal request_count
        request_count += 1
        if request_count == 1:
            held.append(route)
        elif request_count == 2:
            route.fulfill(status=500, body="fail")
        else:
            route.continue_()

    page.route("**/api/folders?*", _hold_success_then_fail)
    page.evaluate(
        "() => { window._olderSuccessfulFolderLoad = "
        "loadFolders({ shouldRender: () => true }); }"
    )
    for _ in range(50):
        if held:
            break
        page.wait_for_timeout(100)
    assert held, "older folder-tree request was never issued"

    # The held request will read the new tree when released. Start a newer
    # request first and make that superseder fail.
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?",
        (folder_ids[0],),
    )
    db.conn.commit()
    page.evaluate("() => { window._failedFolderLoad = loadFolders(); }")
    page.wait_for_timeout(200)
    held[0].continue_()

    page.wait_for_function(
        f"!document.querySelector("
        f"'#folderTree .tree-item[data-folder-id=\"{folder_ids[0]}\"]')",
        timeout=5000,
    )
    expect(page.locator("#folderTree .tree-item")).to_have_count(1)


def test_load_collections_renders_older_success_after_newer_failure(
    live_server, page
):
    """A failed superseder must not suppress the newest successful list."""
    db = live_server["db"]
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("!loading", timeout=5000)

    held = []
    request_count = 0

    def _hold_success_then_fail(route):
        nonlocal request_count
        request_count += 1
        if request_count == 1:
            held.append(route)
        elif request_count == 2:
            route.fulfill(status=500, body="fail")
        else:
            route.continue_()

    page.route("**/api/collections", _hold_success_then_fail)
    page.evaluate(
        "() => { window._olderSuccessfulCollectionLoad = "
        "loadCollections({ shouldRender: () => true }); }"
    )
    for _ in range(50):
        if held:
            break
        page.wait_for_timeout(100)
    assert held, "older collection-list request was never issued"

    # The held request reaches the server only after release, so it observes
    # this new collection. A later request starts first but fails; the older
    # success is therefore the newest usable response and must be rendered.
    collection_id = db.add_collection("Fallback collection", "[]")
    page.evaluate("() => { window._failedCollectionLoad = loadCollections(); }")
    page.wait_for_timeout(200)
    held[0].continue_()

    expect(
        page.locator(
            f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
        )
    ).to_have_count(1, timeout=5000)


def test_load_keywords_renders_older_success_after_newer_failure(
    live_server, page
):
    """A failed superseder must not suppress the newest successful tree."""
    db = live_server["db"]
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("!loading", timeout=5000)

    held = []
    request_count = 0

    def _hold_success_then_fail(route):
        nonlocal request_count
        request_count += 1
        if request_count == 1:
            held.append(route)
        elif request_count == 2:
            route.fulfill(status=500, body="fail")
        else:
            route.continue_()

    page.route("**/api/keywords", _hold_success_then_fail)
    page.evaluate(
        "() => { window._olderSuccessfulKeywordLoad = "
        "loadKeywords({ shouldRender: () => true }); }"
    )
    for _ in range(50):
        if held:
            break
        page.wait_for_timeout(100)
    assert held, "older keyword-tree request was never issued"

    keyword_name = "Fallback keyword"
    keyword_id = db.add_keyword(keyword_name)
    db.tag_photo(live_server["data"]["photos"][0], keyword_id)
    page.evaluate("() => { window._failedKeywordLoad = loadKeywords(); }")
    page.wait_for_timeout(200)
    held[0].continue_()

    expect(
        page.locator(f'#keywordTree .tree-item[data-keyword="{keyword_name}"]')
    ).to_have_count(1, timeout=5000)


def test_load_summary_renders_older_success_after_newer_failure(
    live_server, page
):
    """An identical-scope failed superseder must retain a usable summary."""
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("browseDatasetReady", timeout=5000)
    expect(page.locator("#summaryPhotoCount")).to_have_text("5", timeout=5000)

    held = []
    request_count = 0

    def _hold_success_then_fail(route):
        nonlocal request_count
        request_count += 1
        if request_count == 1:
            held.append(route)
        elif request_count == 2:
            route.fulfill(status=500, body="fail")
        else:
            route.continue_()

    page.route("**/api/browse/summary**", _hold_success_then_fail)
    page.evaluate(
        "() => {"
        "  document.getElementById('summaryPhotoCount').textContent = 'stale';"
        "  window._olderSuccessfulSummaryLoad = loadSummary();"
        "}"
    )
    for _ in range(50):
        if held:
            break
        page.wait_for_timeout(100)
    assert held, "older summary request was never issued"

    page.evaluate("() => { window._failedSummaryLoad = loadSummary(); }")
    page.wait_for_timeout(200)
    held[0].continue_()

    expect(page.locator("#summaryPhotoCount")).to_have_text("5", timeout=5000)


def test_load_calendar_renders_older_success_after_newer_failure(
    live_server, page
):
    """An identical-scope failed superseder must retain a usable calendar."""
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("browseDatasetReady", timeout=5000)
    expected_year = str(page.evaluate("calendarYear"))

    held = []
    request_count = 0

    def _hold_success_then_fail(route):
        nonlocal request_count
        request_count += 1
        if request_count == 1:
            held.append(route)
        elif request_count == 2:
            route.fulfill(status=500, body="fail")
        else:
            route.continue_()

    page.route("**/api/photos/calendar**", _hold_success_then_fail)
    page.evaluate(
        "() => {"
        "  document.getElementById('calYearLabel').textContent = 'stale';"
        "  window._olderSuccessfulCalendarLoad = loadCalendarData();"
        "}"
    )
    for _ in range(50):
        if held:
            break
        page.wait_for_timeout(100)
    assert held, "older calendar request was never issued"

    page.evaluate("() => { window._failedCalendarLoad = loadCalendarData(); }")
    page.wait_for_timeout(200)
    held[0].continue_()

    expect(page.locator("#calYearLabel")).to_have_text(expected_year, timeout=5000)


def test_health_refresh_awaits_config_before_loading_photos(
    live_server, page, tmp_path
):
    """A health event fired while /api/config is pending must not load
    photos with the hard-coded default ``perPage``.

    Regression (Codex review r3686912883): the health-refresh handler used
    to call ``resetAndLoad({ preserveCollection: true })`` unconditionally,
    which calls ``loadPhotos()`` against ``perPage = 50`` (the JS default).
    Bootstrap later applies the configured page size via
    ``applyBrowseConfig(await _cfgPromise)`` — its own init render is
    dropped by the health-generation guard, but the ``perPage`` variable
    it wrote is not. Subsequent pagination then computes offsets against
    the new size while page 1 was loaded with 50, silently skipping or
    duplicating photos across the boundary.
    """
    import config as cfg

    # Configure a non-default page size. Tiny value so the mismatch would
    # be dramatic if the fix regressed.
    saved = cfg.load()
    saved["photos_per_page"] = 3
    cfg.save(saved)

    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'missing' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()

    # Hold /api/config so bootstrap and refreshBrowseAfterFolderHealthChange
    # are both stuck awaiting ``_cfgPromise`` when the health event fires.
    held_cfg = []

    def _hold_first_cfg(route):
        if route.request.method == "GET" and not held_cfg:
            held_cfg.append(route)
        else:
            route.continue_()

    page.route("**/api/config", _hold_first_cfg)

    # Capture every photo-query request the client issues so we can inspect
    # the per_page value the health refresh's loadPhotos ended up sending.
    # The current Browse API sends pagination in a JSON POST body.
    photo_query_page_sizes = []

    def _capture_photo_query(req):
        if req.method == "POST" and req.url.endswith("/api/photos/query"):
            photo_query_page_sizes.append(req.post_data_json.get("per_page"))

    page.on("request", _capture_photo_query)

    page.goto(f"{live_server['url']}/browse")
    for _ in range(50):
        if held_cfg:
            break
        page.wait_for_timeout(100)
    assert held_cfg, "/api/config was never requested"

    # Restore folders on disk and dispatch the health event while /api/config
    # is still pending. Under the pre-fix code, refreshBrowseAfterFolderHealthChange
    # would immediately call resetAndLoad → loadPhotos using perPage=50.
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', "
        "{detail: {restored: [], wentMissing: [], source: 'test'}}))"
    )

    # Give the refresh handler a chance to reach its first await. With the
    # fix in place it awaits _cfgPromise; without the fix it would already
    # have fired /api/photos with per_page=50.
    page.wait_for_timeout(300)

    # Release /api/config so bootstrap and the refresh handler resume with
    # the configured perPage=3 applied.
    held_cfg[0].continue_()

    # Wait for the refresh to complete (page 1 fetched from restored folders).
    page.wait_for_function("photos.length > 0", timeout=10000)

    # Any photo-query request issued by the health refresh must have used
    # the configured per_page=3, never the hard-coded 50 that used to leak
    # through when the event fired before config resolved.
    assert photo_query_page_sizes, "health refresh never queried photos"
    assert all(size == 3 for size in photo_query_page_sizes), (
        "health-refresh loadPhotos used unconfigured perPage: "
        f"{photo_query_page_sizes}"
    )


def test_health_refresh_uses_its_folder_response_when_render_is_superseded(
    live_server, page, tmp_path
):
    """A superseded health-owned folder render must still clear a dead scope."""
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse?folder_id={folder_ids[0]}")
    expect(page.locator(".grid-card")).to_have_count(3)

    held = []

    def _hold_folder_loads(route):
        held.append(route)

    page.route("**/api/folders?*", _hold_folder_loads)
    park.rmdir()

    with page.expect_response(
        lambda response: response.url.endswith("/api/folders/check-health")
    ):
        page.evaluate("openMissingFoldersModal()")

    # The health event owns the first folder load. Start a second caller so
    # the shared generation suppresses the first load's DOM render.
    for _ in range(50):
        if held:
            break
        page.wait_for_timeout(100)
    assert len(held) == 1, "health refresh did not request the folder tree"
    page.evaluate("() => { window._winningFolderLoad = loadFolders(); }")
    for _ in range(50):
        if len(held) == 2:
            break
        page.wait_for_timeout(100)
    assert len(held) == 2, "competing folder load was not issued"

    # Release only the suppressed health-owned response. The competing render
    # is still pending, so the DOM remains stale; the refresh must use the
    # returned rows themselves to see that the active folder disappeared.
    held[0].continue_()
    page.wait_for_function("activeFolderId === null", timeout=10000)
    expect(page.locator(".grid-card")).to_have_count(2)

    held[1].continue_()
    page.wait_for_function(
        f"!document.querySelector("
        f"'#folderTree .tree-item[data-folder-id=\"{folder_ids[0]}\"]')",
        timeout=5000,
    )


def test_unrelated_folder_health_change_preserves_leaf_selection(
    live_server, page, tmp_path
):
    """A sibling folder transition must not reset a leaf-folder Browse view."""
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse?folder_id={folder_ids[0]}")
    expect(page.locator(".grid-card")).to_have_count(3)
    first = page.locator(".grid-card").first
    first.click()
    page.wait_for_function(
        "document.getElementById('detailContent').classList.contains('visible')",
        timeout=3000,
    )
    selected_id = page.evaluate("selectedPhotoId")
    load_epoch = page.evaluate("loadEpoch")

    # Only the unrelated yard folder changes health. The park-scoped grid is
    # identical, so the health refresh should update sidebars/counts without
    # rebuilding the grid or discarding the focused photo and detail panel.
    yard.rmdir()
    with page.expect_response(
        lambda response: response.url.endswith("/api/folders/check-health")
    ):
        page.evaluate("openMissingFoldersModal()")
    page.evaluate("() => window._activeFolderHealthRefresh")

    assert page.evaluate("loadEpoch") == load_epoch
    assert page.evaluate("selectedPhotoId") == selected_id
    assert page.evaluate(
        "document.getElementById('detailContent').classList.contains('visible')"
    )
    expect(page.locator(".grid-card")).to_have_count(3)


def test_folder_health_refresh_drops_selection_when_photo_goes_offline(
    live_server, page, tmp_path
):
    """An offline placeholder cannot retain the pre-refresh selection."""
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()
    collection_id = next(
        collection["id"]
        for collection in db.get_collections()
        if collection["name"] == "All Photos"
    )
    target_id = live_server["data"]["photos"][3]

    page.goto(f"{live_server['url']}/browse?collection_id={collection_id}")
    target = page.locator(f'.grid-card[data-id="{target_id}"]')
    target.wait_for(state="visible")
    target.click()
    page.wait_for_function("id => selectedPhotoId === id", arg=target_id)

    # Keep offline collection members in the next health-driven reload. The
    # selected yard photo then remains rendered, but only as a placeholder.
    page.evaluate("showOfflineCollectionPhotos = true")
    yard.rmdir()
    with page.expect_response(
        lambda response: response.url.endswith("/api/folders/check-health")
    ):
        page.evaluate("openMissingFoldersModal()")
    page.evaluate("() => window._activeFolderHealthRefresh")

    expect(target).to_have_class("grid-card offline")
    assert page.evaluate("selectedPhotoId") is None
    assert page.evaluate("selectedIndex") == -1
    assert page.evaluate("selectedPhotos.size") == 0
    expect(page.locator("#detailContent")).not_to_have_class("visible")


def test_health_refresh_skips_stale_reset_when_load_folders_fails(
    live_server, page, tmp_path
):
    """A transient ``/api/folders`` failure during a folder-health refresh
    must NOT commit to a stale-DOM membership decision, and Browse must
    recover automatically without waiting for the ten-minute background
    poll.

    Regression (Codex review r3687062925): ``loadFolders()`` swallowed its
    exception and resolved, so the refresh's ``Promise.all`` looked
    successful. The subsequent DOM check for ``activeFolderId`` then read
    the pre-transition tree, retained the just-missing folder scope, and
    reloaded that unavailable scope into an empty grid. Because the navbar
    advances ``_missingFoldersLastIds`` before dispatching the event, no
    later poll saw a transition to repair the view.
    """
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(park), folder_ids[0]),
    )
    db.conn.execute(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        (str(yard), folder_ids[1]),
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible", timeout=5000)
    expect(page.locator(".grid-card")).to_have_count(5)

    # Select park so the health refresh has a folder scope to potentially
    # mis-preserve. Once selected, only park's 3 photos are visible.
    page.evaluate(f"filterByFolder({folder_ids[0]})")
    expect(page.locator(".grid-card")).to_have_count(3)

    # Ensure the initial /api/folders/missing poll seeded the baseline so
    # the retry dispatched by the fix doesn't merely resurrect a null-diff.
    page.wait_for_function(
        "typeof _missingFoldersLastIds !== 'undefined' && "
        "_missingFoldersLastIds !== null"
    )

    # Server-side: park just went missing. The navbar's next check-health
    # POST will observe the transition and dispatch vireo:folder-health-
    # changed. Route /api/folders to fail ONCE so the first refresh has to
    # decide whether to reset the grid without fresh folder data.
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (folder_ids[0],)
    )
    db.conn.commit()

    fail_state = {"fired": False}

    def _fail_first_folders(route):
        if route.request.method == "GET" and not fail_state["fired"]:
            fail_state["fired"] = True
            route.fulfill(status=500, body="fail")
        else:
            route.continue_()

    page.route("**/api/folders?*", _fail_first_folders)

    # Trigger the health refresh directly. Using the event bypasses the
    # navbar poll timing so the test is deterministic; the fix under test
    # lives entirely on the browse.html listener side.
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', {"
        " detail: { source: 'test' } }))"
    )

    # The first refresh must NOT run resetAndLoad against the stale park
    # scope. Old behavior: the grid drops to 0 cards (park is missing).
    # New behavior: skip the reset entirely, so the grid stays populated
    # from the pre-event state until the retry fires.
    page.wait_for_timeout(500)
    interim_count = page.locator(".grid-card").count()
    assert interim_count > 0, (
        "health refresh reset the grid using stale folder data after "
        "loadFolders() failed (activeFolderId was preserved from the "
        "pre-transition DOM)"
    )

    # The scheduled retry (2s) re-dispatches the event; loadFolders now
    # succeeds against a park-less tree, membership check clears
    # activeFolderId, and the workspace-scoped reset lands on yard.
    page.wait_for_function(
        "activeFolderId === null",
        timeout=5000,
    )
    expect(page.locator(".grid-card")).to_have_count(2)


def test_health_refresh_retry_preserves_unrelated_transition_ids(
    live_server, page, tmp_path
):
    """A folder-tree retry must not reset an unaffected leaf-folder grid."""
    db = live_server["db"]
    park = tmp_path / "park"
    yard = tmp_path / "yard"
    park.mkdir()
    yard.mkdir()
    folder_ids = live_server["data"]["folders"]
    db.conn.executemany(
        "UPDATE folders SET path = ?, status = 'ok' WHERE id = ?",
        [(str(park), folder_ids[0]), (str(yard), folder_ids[1])],
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse?folder_id={folder_ids[0]}")
    expect(page.locator(".grid-card")).to_have_count(3)
    page.locator(".grid-card").first.click()
    page.wait_for_function(
        "document.getElementById('detailContent').classList.contains('visible')",
        timeout=3000,
    )
    selected_id = page.evaluate("selectedPhotoId")
    load_epoch = page.evaluate("loadEpoch")

    fail_state = {"fired": False}

    def _fail_first_folders(route):
        if route.request.method == "GET" and not fail_state["fired"]:
            fail_state["fired"] = True
            route.fulfill(status=500, body="fail")
        else:
            route.continue_()

    page.route("**/api/folders?*", _fail_first_folders)
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?",
        (folder_ids[1],),
    )
    db.conn.commit()
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', "
        f"{{detail: {{restored: [], wentMissing: [{folder_ids[1]}], "
        "source: 'test'}}))"
    )

    # The first folder load fails; the scheduled retry succeeds and removes
    # yard from the tree. It must carry the original wentMissing id so park
    # remains recognized as unaffected and its grid is not reset.
    page.wait_for_function(
        f"!document.querySelector("
        f"'#folderTree .tree-item[data-folder-id=\"{folder_ids[1]}\"]')",
        timeout=5000,
    )
    assert fail_state["fired"], "the first folder-tree request did not fail"
    assert page.evaluate("loadEpoch") == load_epoch
    assert page.evaluate("selectedPhotoId") == selected_id
    assert page.evaluate(
        "document.getElementById('detailContent').classList.contains('visible')"
    )
    expect(page.locator(".grid-card")).to_have_count(3)


def test_bounded_folder_retries_reconcile_on_next_normal_poll(live_server, page):
    """A capped retry burst stays dirty for the next normal health poll."""
    page.goto(f"{live_server['url']}/browse")
    expect(page.locator(".grid-card")).to_have_count(5)

    folder_requests = []
    fail_folders = {"value": True}

    def _fail_folders(route):
        folder_requests.append(route.request.url)
        if fail_folders["value"]:
            route.fulfill(status=500, body="fail")
        else:
            route.continue_()

    page.route("**/api/folders?*", _fail_folders)
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', {"
        "detail: {restored: [], wentMissing: [], source: 'test', "
        "refreshRetryAttempt: 3}}))"
    )
    page.evaluate("() => window._activeFolderHealthRefresh")

    # Attempt 3 is the cap: no fourth event should be scheduled two seconds
    # later, and therefore no second folder-tree request should appear.
    page.wait_for_timeout(2300)
    assert len(folder_requests) == 1
    assert page.evaluate("_missingFoldersReconciliationPending") is True

    # The navbar snapshot already advanced before the original transition,
    # so this poll sees unchanged missing-folder ids. It must still consume
    # the persistent dirty marker and refresh the folder tree successfully.
    fail_folders["value"] = False
    page.evaluate("checkMissingFolders()")
    page.evaluate("() => window._activeFolderHealthRefresh")
    assert page.evaluate("_missingFoldersReconciliationPending") is False
    assert len(folder_requests) == 2


def test_failed_health_grid_reload_reconciles_on_next_normal_poll(
    live_server, page
):
    """A transient first-page failure must remain pending for reconciliation."""
    page.goto(f"{live_server['url']}/browse")
    expect(page.locator(".grid-card")).to_have_count(5)
    page.wait_for_function(
        "typeof _missingFoldersLastIds !== 'undefined' && "
        "_missingFoldersLastIds !== null"
    )

    photo_requests = []
    fail_photos = {"value": True}

    def _fail_photos(route):
        photo_requests.append(route.request.url)
        if fail_photos["value"]:
            route.fulfill(status=500, body="fail")
        else:
            route.continue_()

    page.route("**/api/photos/query", _fail_photos)
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('vireo:folder-health-changed', {"
        "detail: {restored: [], wentMissing: [], source: 'test'}}))"
    )
    page.evaluate("() => window._activeFolderHealthRefresh")

    expect(page.locator(".grid-card")).to_have_count(0)
    assert len(photo_requests) == 1
    assert page.evaluate("_missingFoldersReconciliationPending") is True

    # The missing-folder IDs are unchanged, but the pending marker forces a
    # synthetic health event and retries the failed page-one grid load.
    fail_photos["value"] = False
    page.evaluate("checkMissingFolders()")
    page.evaluate("() => window._activeFolderHealthRefresh")

    expect(page.locator(".grid-card")).to_have_count(5)
    assert len(photo_requests) == 2
    assert page.evaluate("_missingFoldersReconciliationPending") is False


def test_wait_for_folder_health_refreshes_drains_replacements(live_server, page):
    """A deep-link waiter must not resume between refresh A and refresh B."""
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("browseDatasetReady", timeout=5000)

    page.evaluate(
        "() => {"
        "  window._refreshWaitDone = false;"
        "  _activeFolderHealthRefresh = new Promise(resolve => {"
        "    window._resolveRefreshA = resolve;"
        "  });"
        "  window._refreshWait = waitForFolderHealthRefreshesToSettle().then(() => {"
        "    window._refreshWaitDone = true;"
        "  });"
        "  _activeFolderHealthRefresh = new Promise(resolve => {"
        "    window._resolveRefreshB = resolve;"
        "  });"
        "  window._resolveRefreshA();"
        "}"
    )
    page.wait_for_timeout(100)
    assert page.evaluate("window._refreshWaitDone") is False

    page.evaluate("window._resolveRefreshB()")
    page.wait_for_function("window._refreshWaitDone === true", timeout=3000)


def test_missing_folders_recovery_skips_check_when_mutations_hang(
    live_server, page
):
    """When the mutation counter never returns to zero (a hung POST), the
    recovery poll must NOT fire ``checkMissingFolders()`` — its own
    in-flight guard would reject the GET, wasting a round-trip and
    masking the drop.

    Regression (Codex review r3686842771): the previous escape hatch
    (``attempts >= 100``) issued a stale ``checkMissingFolders()`` call
    anyway, guaranteed to be rejected by its ``_missingFoldersMutationInFlight
    > 0`` guard. The 10-minute background poll is the only remaining
    fallback for a genuinely hung POST; fire-and-drop instead of
    fire-and-be-rejected.
    """
    page.goto(f"{live_server['url']}/browse")
    # Wait for the initial poll to settle so the counter is at rest.
    page.wait_for_function("_missingFoldersMutationInFlight === 0", timeout=10000)

    # Count subsequent /api/folders/missing requests so we can prove none
    # were issued by the recovery timer.
    page.evaluate(
        "window._missingRequestCount = 0;"
        "const _origFetch = window.fetch;"
        "window.fetch = function(url, opts) {"
        "  if (typeof url === 'string' && url.indexOf('/api/folders/missing') !== -1) {"
        "    window._missingRequestCount++;"
        "  }"
        "  return _origFetch.apply(this, arguments);"
        "};"
    )

    # Pin the mutation counter above zero to simulate a POST that never
    # resolves. The recovery timer wakes up every 50ms for 100 attempts.
    page.evaluate("_missingFoldersMutationInFlight = 5;")
    page.evaluate("_scheduleMissingFoldersRecovery();")

    # Sleep past the escape-hatch window (100 × 50ms = 5s) with headroom
    # for scheduler jitter.
    page.wait_for_timeout(6500)

    # Escape hatch must have released the scheduled flag without firing a
    # request the mutation guard would immediately reject.
    assert page.evaluate("_missingFoldersRecoveryScheduled") is False, (
        "recovery flag was not cleared after escape-hatch window elapsed"
    )
    assert page.evaluate("window._missingRequestCount") == 0, (
        "recovery fired a stale /api/folders/missing GET the mutation guard "
        "would have rejected"
    )


def test_bootstrap_renders_grid_from_init_on_plain_load(live_server, page):
    """A plain /browse load must render grid cards from init's response.

    Regression: ``bootstrapBrowse()`` synchronously reads
    ``folderHealthRefreshSeq`` into ``bootstrapHealthSeq`` before its first
    await. When the paired ``var folderHealthRefreshSeq = 0;`` initialization
    lived further down the script (past the bootstrapBrowse() invocation),
    hoisting made the snapshot ``undefined``; the outer script's ``= 0``
    assignment then ran while bootstrap was awaiting /api/browse/init, and
    the post-await ``folderHealthRefreshSeq !== bootstrapHealthSeq`` check
    became ``0 !== undefined`` = true on every normal load. That skipped
    bootstrap's render block, leaving Browse empty until VireoFilter.init
    happened to trigger its own reload — which, without an active filter,
    it does not (Codex review r3686317674).
    """
    page.goto(f"{live_server['url']}/browse")
    # Bootstrap's own render must populate the grid — plain /browse has no
    # filter/collection deep-link fallback to hide the regression.
    page.locator(".grid-card").first.wait_for(state="visible", timeout=5000)
    expect(page.locator(".grid-card")).to_have_count(5)


def test_keyword_search_empty_state_and_clear(live_server, page):
    """A zero-result keyword search must not look like an empty library."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")

    search = page.locator(".vf-search input")
    expect(search).to_have_attribute("autocomplete", "off")
    expect(search).to_have_attribute("spellcheck", "false")

    with page.expect_response(lambda response: "/api/photos/query" in response.url):
        search.fill("definitely-no-such-photo")
        search.press("Enter")

    expect(page.locator("#emptyState")).to_be_visible()
    expect(page.locator("#welcomeState")).to_be_hidden()
    expect(page.locator("#emptyState")).to_contain_text("No photos match")

    with page.expect_response(lambda response: "/api/photos/query" in response.url):
        search.fill("")
        search.press("Enter")

    cards.first.wait_for(state="visible")
    expect(page.locator("#emptyState")).to_be_hidden()
    expect(page.locator("#welcomeState")).to_be_hidden()


def test_clearing_keyword_search_keeps_selected_photo_in_place(live_server, page):
    """Restoring filtered-out photos must not pull focus away from the selection."""
    url = live_server["url"]
    selected_id = live_server["data"]["photos"][3]
    page.goto(f"{url}/browse")

    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate("updateThumbSize(400)")

    page.evaluate("VireoFilter.quickSearch('American Robin')")
    page.wait_for_function("() => photos.length === 1")
    selected = page.locator(f'.grid-card[data-id="{selected_id}"]')
    selected.wait_for(state="visible")
    selected.click()

    top_before = page.evaluate(
        """(id) => {
          const card = document.querySelector(`.grid-card[data-id="${id}"]`);
          const container = document.getElementById('gridContainer');
          return card.getBoundingClientRect().top - container.getBoundingClientRect().top;
        }""",
        selected_id,
    )

    page.evaluate("VireoFilter.quickSearch('')")
    page.wait_for_function(
        "(id) => photos.length === 5 && selectedPhotoId === id",
        arg=selected_id,
    )
    page.wait_for_timeout(100)  # allow the anchor-restoration animation frame

    assert page.evaluate("selectedPhotos.size") == 0
    expect(selected).to_have_class("grid-card selected")
    top_after = page.evaluate(
        """(id) => {
          const card = document.querySelector(`.grid-card[data-id="${id}"]`);
          const container = document.getElementById('gridContainer');
          return card.getBoundingClientRect().top - container.getBoundingClientRect().top;
        }""",
        selected_id,
    )
    assert abs(top_after - top_before) < 4


def test_flag_quick_filters_show_picks_and_rejects(live_server, page):
    """Browse offers quick filters for picked and rejected photos in Filters."""
    url = live_server["url"]
    db = live_server["db"]
    photos = db.get_photos()
    pick_id = photos[0]["id"]
    reject_id = photos[1]["id"]
    db.update_photo_flag(pick_id, "flagged")
    db.update_photo_flag(reject_id, "rejected")

    page.goto(f"{url}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.click(".vf-filters-btn")
    pick_btn = page.locator('.vf-quick-flags [data-flag="flagged"]')
    reject_btn = page.locator('.vf-quick-flags [data-flag="rejected"]')
    expect(pick_btn).to_be_visible()
    expect(reject_btn).to_be_visible()

    pick_btn.click()
    expect(pick_btn).to_have_class("active")
    expect(page.locator(".grid-card")).to_have_count(1)
    assert page.locator(".grid-card").first.get_attribute("data-id") == str(pick_id)

    # Flags multi-select now: adding Rejected combines into "is one of".
    reject_btn.click()
    expect(page.locator(".grid-card")).to_have_count(2)

    pick_btn.click()
    expect(pick_btn).not_to_have_class("active")
    expect(page.locator(".grid-card")).to_have_count(1)
    assert page.locator(".grid-card").first.get_attribute("data-id") == str(reject_id)

    reject_btn.click()
    expect(page.locator(".grid-card")).to_have_count(5)


def test_toolbar_history_requeries_flag_filter_and_keeps_collection(live_server, page):
    db = live_server['db']
    ids = live_server['data']['photos'][:2]
    db.update_photo_flag(ids[1], 'rejected')
    collection_id = db.add_collection('Rejected history test', json.dumps([
        {'field': 'flag', 'op': 'is', 'value': 'rejected'},
    ]))
    page.goto(live_server['url'] + '/browse')
    page.locator('.grid-card').first.wait_for(state='visible')
    page.evaluate("id => setFlagFor(id, 'rejected')", ids[0])
    page.locator(f'.tree-item[data-collection-id="{collection_id}"]').click()
    expect(page.locator('.grid-card')).to_have_count(2)
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.locator('.grid-card')).to_have_count(1)
    expect(page.locator('.grid-card')).to_have_attribute('data-id', str(ids[1]))
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    assert page.evaluate('openedCollectionId') == collection_id
    page.locator('#historyRedoBtn').click()
    expect(page.locator('.grid-card')).to_have_count(2)
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    # Keyboard and toolbar share the query refresh, including collection scope.
    page.evaluate('(id) => { activeCollectionId = id; }', collection_id)
    page.evaluate('undoLast()')
    expect(page.locator('.grid-card')).to_have_count(1)
    assert page.evaluate('openedCollectionId') == collection_id
    assert page.evaluate('activeCollectionId') == collection_id


def test_toolbar_history_requeries_rating_sort(live_server, page):
    db = live_server['db']
    ids = live_server['data']['photos'][:2]
    db.update_photo_rating(ids[0], 2)
    db.update_photo_rating(ids[1], 1)
    page.goto(live_server['url'] + '/browse')
    page.locator('#sortSelect').select_option('rating')
    expect(page.locator('.grid-card').first).to_have_attribute('data-id', str(ids[0]))
    page.evaluate('id => setRatingFor(id, 5)', ids[1])
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    expect(page.locator('.grid-card').first).to_have_attribute('data-id', str(ids[0]))
    page.locator('#historyRedoBtn').click()
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    expect(page.locator('.grid-card').first).to_have_attribute('data-id', str(ids[1]))
