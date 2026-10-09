import json
import os
import sys
import time

import pytest
from testing.waits import synchronization_timeout


def test_api_darktable_status(app_and_db):
    """GET /api/darktable/status returns availability info."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/darktable/status')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'available' in data
    assert isinstance(data['available'], bool)
    assert 'bin' in data


def test_api_job_develop_requires_photo_ids(app_and_db):
    """POST /api/jobs/develop returns 400 without photo_ids."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/jobs/develop',
                       data=json.dumps({}),
                       content_type='application/json')
    assert resp.status_code == 400


def test_api_config_saves_darktable_settings(app_and_db):
    """POST /api/config saves darktable settings."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/config',
                       data=json.dumps({
                           "darktable_bin": "/usr/local/bin/darktable-cli",
                           "darktable_style": "Wildlife",
                           "darktable_output_format": "tiff",
                           "darktable_output_dir": "/output",
                           "darktable_auto_convert_dng": True,
                           "dng_converter_bin": "/Applications/Adobe DNG Converter.app/Contents/MacOS/Adobe DNG Converter",
                       }),
                       content_type='application/json')
    assert resp.status_code == 200

    resp2 = client.get('/api/config')
    cfg = resp2.get_json()
    assert cfg["darktable_bin"] == "/usr/local/bin/darktable-cli"
    assert cfg["darktable_style"] == "Wildlife"
    assert cfg["darktable_output_format"] == "tiff"
    assert cfg["darktable_output_dir"] == "/output"
    assert cfg["darktable_auto_convert_dng"] is True
    assert cfg["dng_converter_bin"] == "/Applications/Adobe DNG Converter.app/Contents/MacOS/Adobe DNG Converter"


def _poll_job(client, job_id, timeout_iters=50):
    """Poll /api/jobs/<id> until it reaches a terminal state or times out."""
    data = None
    for _ in range(timeout_iters):
        resp = client.get(f'/api/jobs/{job_id}')
        data = resp.get_json()
        if data['status'] in ('completed', 'failed', 'cancelled'):
            return data
        time.sleep(0.05)
    return data


def test_api_job_develop_all_failures_marks_job_failed(app_and_db, tmp_path, monkeypatch):
    """If every develop_photo call fails, the rollup status must be 'failed',
    not 'completed' (rollups with any failed item report failure)."""
    app, db = app_and_db
    client = app.test_client()

    # develop requires a configured/findable binary or we short-circuit with
    # a 400 before the job even starts. Monkeypatch find_darktable in the
    # develop module so the endpoint proceeds into the job.
    import develop as develop_mod
    fake_bin = str(tmp_path / "darktable-cli")
    with open(fake_bin, "w") as f:
        f.write("")
    os.chmod(fake_bin, 0o755)
    monkeypatch.setattr(develop_mod, "find_darktable", lambda _p: fake_bin)

    # Make every develop_photo call fail deterministically.
    monkeypatch.setattr(
        develop_mod,
        "develop_photo",
        lambda **kwargs: {
            "success": False,
            "output_path": kwargs.get("output_path", ""),
            "error": "fake failure",
        },
    )

    # Pick one photo from the fixture.
    photos = db.get_photos(per_page=1)
    assert photos, "fixture should provide at least one photo"
    pid = photos[0]['id']

    resp = client.post(
        '/api/jobs/develop',
        data=json.dumps({"photo_ids": [pid]}),
        content_type='application/json',
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    job_id = resp.get_json()['job_id']

    data = _poll_job(client, job_id)
    assert data is not None
    # Bug being fixed: used to be 'completed' with 0/1 developed.
    assert data['status'] == 'failed', f"expected failed, got {data['status']}: {data}"
    # Result counts must still be present so the UI can show them.
    result = data.get('result') or {}
    assert result.get('developed') == 0
    assert result.get('errors') == 1
    assert result.get('total') == 1
    # And the primary per-photo error should be surfaced in job['errors'].
    errs = data.get('errors') or []
    assert any('fake failure' in e for e in errs), f"expected fake failure in errors: {errs}"
    # Regression: the rollup failure raise must not synthesize a second,
    # non-matching error string that _run_job then appends on top of the
    # real per-photo failure (would inflate error_count to 2 for 1 photo).
    assert len(errs) == 1, f"expected exactly one error entry, got {len(errs)}: {errs}"


def test_api_job_develop_mixed_outcomes_marks_job_failed(app_and_db, tmp_path, monkeypatch):
    """If some photos succeed and some fail, the rollup status is still
    'failed' (any failure => failed, per the rollup rule)."""
    app, db = app_and_db
    client = app.test_client()

    import develop as develop_mod
    fake_bin = str(tmp_path / "darktable-cli")
    with open(fake_bin, "w") as f:
        f.write("")
    os.chmod(fake_bin, 0o755)
    monkeypatch.setattr(develop_mod, "find_darktable", lambda _p: fake_bin)

    # Alternate success/failure based on input filename.
    def flaky_develop(**kwargs):
        in_path = kwargs.get("input_path", "")
        if "bird1" in in_path:
            return {"success": True, "output_path": kwargs["output_path"], "error": None}
        return {
            "success": False,
            "output_path": kwargs["output_path"],
            "error": "fake failure on second photo",
        }

    monkeypatch.setattr(develop_mod, "develop_photo", flaky_develop)

    photos = db.get_photos(per_page=2)
    assert len(photos) >= 2, "fixture should provide at least two photos"
    pids = [photos[0]['id'], photos[1]['id']]

    resp = client.post(
        '/api/jobs/develop',
        data=json.dumps({"photo_ids": pids}),
        content_type='application/json',
    )
    assert resp.status_code == 200
    job_id = resp.get_json()['job_id']

    data = _poll_job(client, job_id)
    assert data is not None
    assert data['status'] == 'failed', f"expected failed, got {data['status']}: {data}"
    result = data.get('result') or {}
    assert result.get('developed') == 1
    assert result.get('errors') == 1
    assert result.get('total') == 2
    # Regression: only the actual per-photo failure should appear in the
    # errors list — no synthetic summary string tacked on by _run_job.
    errs = data.get('errors') or []
    assert len(errs) == 1, f"expected exactly one error entry, got {len(errs)}: {errs}"
    assert any('fake failure on second photo' in e for e in errs), errs


def test_api_darktable_status_reports_checked_paths(app_and_db):
    """The status route says where it looked, so a bare 'not found' can explain itself."""
    app, _ = app_and_db
    client = app.test_client()
    data = client.get('/api/darktable/status').get_json()

    assert 'checked_paths' in data
    assert isinstance(data['checked_paths'], list)
    assert all(isinstance(p, str) for p in data['checked_paths'])


def test_api_darktable_status_checked_paths_mentions_path_probe(app_and_db):
    """$PATH is the probe most likely to explain a miss, so it must be listed.

    find_darktable tries shutil.which() before any filesystem candidate;
    omitting it would make "we checked here" untrue by omission.
    """
    app, _ = app_and_db
    data = app.test_client().get('/api/darktable/status').get_json()

    assert any('PATH' in p for p in data['checked_paths'])


def test_api_darktable_status_checked_paths_never_empty(app_and_db, monkeypatch):
    """Never render an empty 'we checked here' list.

    darktable_search_paths() returns [] on a Linux box with no AppImage
    installed — precisely the user this feature targets.
    """
    import develop
    app, _ = app_and_db
    monkeypatch.setattr(develop, "darktable_search_paths", lambda: [])

    data = app.test_client().get('/api/darktable/status').get_json()

    assert data['checked_paths'], "checked_paths must never be empty"


def test_api_darktable_status_checked_paths_includes_detector_candidates(app_and_db, monkeypatch):
    """The real detector candidates reach the response, in priority order.

    Asserting the whole list rather than membership also pins ordering, which
    users read as "we tried these, in this sequence". Pinned to macOS so the
    tools dir is not appended and the expected list is exact on any host.
    """
    import develop
    app, _ = app_and_db
    monkeypatch.setattr(develop, "darktable_search_paths", lambda: ["/sentinel/darktable-cli"])
    monkeypatch.setattr(os, "name", "posix")
    monkeypatch.setattr(sys, "platform", "darwin")

    data = app.test_client().get('/api/darktable/status').get_json()

    assert data['checked_paths'] == ["$PATH (darktable-cli)", "/sentinel/darktable-cli"]


def test_api_darktable_status_checked_paths_names_linux_tools_dir(app_and_db, monkeypatch):
    """On Linux the list names the AppImage directory even when it's empty.

    darktable_search_paths() only reports AppImages that already exist, so a
    fresh Linux box contributes nothing; the route must still name the
    directory find_darktable looks in, since that is where a download lands.
    """
    import develop
    app, _ = app_and_db
    monkeypatch.setattr(develop, "darktable_search_paths", lambda: [])
    monkeypatch.setattr(os, "name", "posix")
    monkeypatch.setattr(sys, "platform", "linux")

    data = app.test_client().get('/api/darktable/status').get_json()

    # Use the same helper the route uses so path-separator conventions match on
    # Windows (os.path.join is bound at import time and ignores monkeypatched
    # os.name, so a hand-built "~/.vireo/tools/darktable" would drift here).
    assert develop.darktable_tools_dir() in data['checked_paths']


def test_api_darktable_status_checked_paths_omits_linux_tools_dir_on_macos(app_and_db, monkeypatch):
    """macOS never probes the Linux AppImage directory, so don't claim we did."""
    import develop
    app, _ = app_and_db
    monkeypatch.setattr(develop, "darktable_search_paths", lambda: [])
    monkeypatch.setattr(os, "name", "posix")
    monkeypatch.setattr(sys, "platform", "darwin")

    data = app.test_client().get('/api/darktable/status').get_json()

    assert develop.darktable_tools_dir() not in data['checked_paths']
    assert data['checked_paths'], "checked_paths must never be empty"


@pytest.fixture(autouse=True)
def _clear_release_cache():
    """The release cache is module-level state that would leak between tests.

    Without this, a test that stubs a failure would see the previous test's
    cached success and pass for the wrong reason.
    """
    import darktable_install
    darktable_install._release_cache.update(at=0.0, value=None)
    yield
    darktable_install._release_cache.update(at=0.0, value=None)


def _fake_release(version="5.6.0"):
    return {
        "version": version,
        "name": f"darktable-{version}-arm64.dmg",
        "size": 87094261,
        "url": "https://github.com/darktable-org/darktable/releases/download/x/d.dmg",
        "digest": "sha256:" + "a" * 64,
    }


def test_api_darktable_install_available_shape(app_and_db, monkeypatch):
    """The confirmation needs version, name, size, url and digest."""
    import darktable_install
    app, _ = app_and_db
    monkeypatch.setattr(darktable_install, "resolve_release", lambda: (_fake_release(), None))

    data = app.test_client().get('/api/darktable/install/available').get_json()
    assert data['available'] is True
    assert data['version'] == "5.6.0"
    assert data['name'] == "darktable-5.6.0-arm64.dmg"
    assert data['size'] == 87094261
    assert data['url'].startswith("https://github.com/darktable-org/darktable/releases/download/")
    assert data['digest'].startswith("sha256:")


def test_api_darktable_install_available_reports_network_failure_honestly(app_and_db, monkeypatch):
    """A user behind a proxy must not be told their platform is unsupported."""
    import darktable_install
    app, _ = app_and_db
    monkeypatch.setattr(darktable_install, "resolve_release",
                        lambda: (None, darktable_install.REASON_UNREACHABLE))

    resp = app.test_client().get('/api/darktable/install/available')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['available'] is False
    assert data['reason'] == darktable_install.REASON_UNREACHABLE
    assert 'platform' not in data['reason'].lower()


def test_api_darktable_install_available_handles_unsupported_platform(app_and_db, monkeypatch):
    import darktable_install
    app, _ = app_and_db
    monkeypatch.setattr(darktable_install, "resolve_release",
                        lambda: (None, darktable_install.REASON_NO_PLATFORM_BUILD))

    data = app.test_client().get('/api/darktable/install/available').get_json()
    assert data['available'] is False
    assert data['reason'] == darktable_install.REASON_NO_PLATFORM_BUILD


def test_api_darktable_install_available_distinguishes_no_usable_asset(app_and_db, monkeypatch):
    """"Release had no build we could use" is a third, distinct fact.

    Together with the two tests above this pins that the route relays whatever
    resolve_release said rather than collapsing every miss to one sentence.
    """
    import darktable_install
    app, _ = app_and_db
    monkeypatch.setattr(darktable_install, "resolve_release",
                        lambda: (None, darktable_install.REASON_NO_USABLE_ASSET))

    data = app.test_client().get('/api/darktable/install/available').get_json()
    assert data['available'] is False
    assert data['reason'] == darktable_install.REASON_NO_USABLE_ASSET


def test_api_darktable_install_available_survives_an_unexpected_raise(app_and_db, monkeypatch):
    """Belt and braces: resolve_release should never raise, but if it does the
    route must still 200 with a reason rather than 500 into a dead button."""
    import darktable_install
    app, _ = app_and_db

    def boom():
        raise OSError("network unreachable")
    monkeypatch.setattr(darktable_install, "resolve_release", boom)

    resp = app.test_client().get('/api/darktable/install/available')
    assert resp.status_code == 200
    assert resp.get_json()['available'] is False
    assert resp.get_json()['reason']


def test_api_darktable_install_available_caches_a_successful_lookup(app_and_db, monkeypatch):
    """Repeated Settings loads must not burn GitHub's 60 unauthenticated req/hr.

    Once that budget is gone every user on the IP gets the fallback link, so a
    second call within the TTL must not reach the API at all.
    """
    import darktable_install
    app, _ = app_and_db
    calls = []

    def counting():
        calls.append(1)
        return _fake_release(), None
    monkeypatch.setattr(darktable_install, "resolve_release", counting)

    client = app.test_client()
    first = client.get('/api/darktable/install/available').get_json()
    second = client.get('/api/darktable/install/available').get_json()

    assert first['available'] is True
    assert second == first
    assert len(calls) == 1, f"expected one API lookup, got {len(calls)}"


def test_api_darktable_install_available_does_not_cache_failures(app_and_db, monkeypatch):
    """A transient outage must not pin the fallback link for the whole TTL.

    Caching a failure would also freeze its reason string, so a user who fixed
    their network would keep being told GitHub is unreachable.
    """
    import darktable_install
    app, _ = app_and_db
    outcomes = [
        (None, darktable_install.REASON_UNREACHABLE),
        (_fake_release(), None),
    ]
    monkeypatch.setattr(darktable_install, "resolve_release", lambda: outcomes.pop(0))

    client = app.test_client()
    first = client.get('/api/darktable/install/available').get_json()
    second = client.get('/api/darktable/install/available').get_json()

    assert first['available'] is False
    assert first['reason'] == darktable_install.REASON_UNREACHABLE
    assert second['available'] is True, "a failure must not be cached over a later success"
    assert second['version'] == "5.6.0"


# --- POST /api/jobs/download-darktable -------------------------------------


def _stub_happy_path(monkeypatch, *, release=None, detail="ok",
                     hand_off_result=None, download=None):
    """Stub every darktable_install call the job makes. No network, ever."""
    import darktable_install
    release = release or _fake_release()
    monkeypatch.setattr(darktable_install, "resolve_release", lambda: (release, None))
    monkeypatch.setattr(darktable_install, "free_space_bytes", lambda p: 10 ** 12)
    monkeypatch.setattr(
        darktable_install, "download",
        download or (lambda *a, **k: ("/tmp/d.dmg", detail)),
    )
    monkeypatch.setattr(
        darktable_install, "hand_off",
        lambda p, **k: hand_off_result or {
            "action": "opened-installer", "location": p, "bin_path": None,
        },
    )
    monkeypatch.setattr(darktable_install, "is_quarantined", lambda p: False)
    return release


def test_api_job_download_darktable_returns_job_id(app_and_db, monkeypatch):
    import darktable_install
    app, _ = app_and_db
    monkeypatch.setattr(darktable_install, "resolve_release", lambda: ({
        "version": "5.6.0", "name": "d.dmg", "size": 100,
        "url": "https://github.com/darktable-org/darktable/releases/download/x/d.dmg",
        "digest": None,
    }, None))
    monkeypatch.setattr(darktable_install, "free_space_bytes", lambda p: 10 ** 12)
    monkeypatch.setattr(darktable_install, "download", lambda *a, **k: ("/tmp/d.dmg", "ok"))
    monkeypatch.setattr(darktable_install, "hand_off",
                        lambda p, **k: {"action": "opened-installer",
                                        "location": p, "bin_path": None})
    monkeypatch.setattr(darktable_install, "is_quarantined", lambda p: False)

    client = app.test_client()
    resp = client.post('/api/jobs/download-darktable')
    assert resp.status_code == 200
    job_id = resp.get_json()['job_id']

    # Wait for the job thread to finish INSIDE the test. Returning early lets
    # it run after monkeypatch teardown, where it would call the real download
    # and hit the network.
    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)
    assert job['status'] == 'completed'
    result = job['result']
    assert result['version'] == "5.6.0"
    assert result['downloaded_to'] == "/tmp/d.dmg"
    assert result['action'] == "opened-installer"


def test_api_job_download_darktable_refuses_without_disk_space(app_and_db, monkeypatch):
    """Refuse before downloading 87MB, and say what is needed vs free."""
    import darktable_install
    app, _ = app_and_db
    monkeypatch.setattr(darktable_install, "resolve_release", lambda: ({
        "version": "5.6.0", "name": "d.dmg", "size": 87094261,
        "url": "https://github.com/darktable-org/darktable/releases/download/x/d.dmg",
        "digest": None,
    }, None))
    monkeypatch.setattr(darktable_install, "free_space_bytes", lambda p: 1024)

    def must_not_download(*a, **k):
        raise AssertionError("preflight must refuse before spending bandwidth")
    monkeypatch.setattr(darktable_install, "download", must_not_download)

    resp = app.test_client().post('/api/jobs/download-darktable')
    assert resp.status_code == 400
    error = resp.get_json()['error']
    assert 'space' in error.lower()
    # Both halves of the fact the user acts on: how much is needed, and how
    # much they have. "Not enough disk space." alone tells them nothing.
    assert '166' in error, f"needed size (2x87MB) missing from {error!r}"
    assert 'free' in error.lower(), f"free space missing from {error!r}"


def test_api_job_download_darktable_space_check_reuses_completed_download(
        app_and_db, monkeypatch):
    """A cancelled-during-verify retry must not be blocked by the preflight.

    When Stop is pressed during verify_digest, download() has already renamed
    the .partial to the final destination.  A retry fast-paths straight to
    verify and spends zero download bytes — but the preflight would still
    demand 2x the asset size free, so a 178 MB download accepted with 400 MB
    free would leave 222 MB and then refuse the retry.  Count the reusable
    file against `needed` so the promised resume path actually works.
    """
    import darktable_install
    app, _ = app_and_db
    release = _fake_release()
    asset_size = release["size"]
    monkeypatch.setattr(darktable_install, "resolve_release", lambda: (release, None))

    # Simulate the cancelled-verify state: the artifact is already on disk
    # at the API-published size, ready for verify_digest to re-run.
    target = darktable_install.install_dir()
    os.makedirs(target, exist_ok=True)
    completed = os.path.join(target, release["name"])
    with open(completed, "wb") as f:
        f.truncate(asset_size)

    # Only unpacking headroom is available — one asset-size, not two.  Without
    # the reusable-file credit the preflight would reject this.
    monkeypatch.setattr(darktable_install, "free_space_bytes", lambda p: asset_size)

    def fake_download(asset, **kwargs):
        return completed, "ok"
    monkeypatch.setattr(darktable_install, "download", fake_download)
    monkeypatch.setattr(darktable_install, "hand_off",
                        lambda p, **k: {"action": "opened-installer",
                                        "location": p, "bin_path": None})
    monkeypatch.setattr(darktable_install, "is_quarantined", lambda p: False)

    resp = app.test_client().post('/api/jobs/download-darktable')
    assert resp.status_code == 200, resp.get_json()


def test_api_job_download_darktable_space_check_ignores_wrong_size_stray(
        app_and_db, monkeypatch):
    """A stray file of the *wrong* size must not credit the preflight.

    Only the API-published size counts — an unrelated file the user dropped
    into the tools directory could otherwise sneak past the check and leave
    the retry stranded on a full disk when download() decides it cannot
    reuse the bytes.
    """
    import darktable_install
    app, _ = app_and_db
    release = _fake_release()
    monkeypatch.setattr(darktable_install, "resolve_release", lambda: (release, None))

    target = darktable_install.install_dir()
    os.makedirs(target, exist_ok=True)
    stray = os.path.join(target, release["name"])
    with open(stray, "wb") as f:
        f.truncate(1024)  # wrong size — not a reusable download

    monkeypatch.setattr(darktable_install, "free_space_bytes", lambda p: 1024)

    def must_not_download(*a, **k):
        raise AssertionError("preflight must refuse when the stray is unusable")
    monkeypatch.setattr(darktable_install, "download", must_not_download)

    resp = app.test_client().post('/api/jobs/download-darktable')
    assert resp.status_code == 400
    assert 'space' in resp.get_json()['error'].lower()


def test_api_job_download_darktable_space_check_credits_resumable_partial(
        app_and_db, monkeypatch):
    """A retry after a mid-transfer cancel must not be rejected on disk space.

    ``_download_with_resume`` resumes from ``<dest>.partial`` — those bytes
    are already on disk, so the retry only needs the remaining transfer
    plus the unpack headroom.  For a 100 MB partial of a 178 MB asset the
    check should demand ~256 MB (178 to unpack + 78 still to transfer),
    not the unretried 356 MB.
    """
    import darktable_install
    app, _ = app_and_db
    release = _fake_release()
    asset_size = release["size"]
    partial_size = asset_size // 2
    monkeypatch.setattr(darktable_install, "resolve_release",
                        lambda: (release, None))

    target = darktable_install.install_dir()
    os.makedirs(target, exist_ok=True)
    partial_path = os.path.join(target, release["name"] + ".partial")
    with open(partial_path, "wb") as f:
        f.truncate(partial_size)

    # Free space equals what the retry actually needs: one full asset (unpack
    # headroom) plus the bytes still to transfer.  Without crediting the
    # partial the preflight demands 2x the asset size and rejects the retry.
    monkeypatch.setattr(darktable_install, "free_space_bytes",
                        lambda p: asset_size + (asset_size - partial_size))

    def fake_download(asset, **kwargs):
        return os.path.join(target, release["name"]), "ok"
    monkeypatch.setattr(darktable_install, "download", fake_download)
    monkeypatch.setattr(darktable_install, "hand_off",
                        lambda p, **k: {"action": "opened-installer",
                                        "location": p, "bin_path": None})
    monkeypatch.setattr(darktable_install, "is_quarantined", lambda p: False)

    resp = app.test_client().post('/api/jobs/download-darktable')
    assert resp.status_code == 200, resp.get_json()


def test_api_job_download_darktable_space_check_partial_credit_capped_at_asset_size(
        app_and_db, monkeypatch):
    """A ``.partial`` larger than the API-published size must not over-credit.

    A stale partial from a different release could otherwise let a retry
    slip past the preflight and then fill the disk mid-transfer.  Bound
    the credit at ``asset_size``.
    """
    import darktable_install
    app, _ = app_and_db
    release = _fake_release()
    asset_size = release["size"]
    monkeypatch.setattr(darktable_install, "resolve_release",
                        lambda: (release, None))

    target = darktable_install.install_dir()
    os.makedirs(target, exist_ok=True)
    partial_path = os.path.join(target, release["name"] + ".partial")
    # Twice the asset size — a stale file that would over-credit the check
    # if the cap did not exist.
    with open(partial_path, "wb") as f:
        f.truncate(asset_size * 2)

    # Free = asset_size - 1 byte.  With no cap the credit would be
    # 2*asset_size, so ``needed`` = 0 and the check would pass.  Capped at
    # asset_size, ``needed`` = asset_size and the check must refuse.
    monkeypatch.setattr(darktable_install, "free_space_bytes",
                        lambda p: asset_size - 1)

    def must_not_download(*a, **k):
        raise AssertionError("preflight must refuse when credit would over-count")
    monkeypatch.setattr(darktable_install, "download", must_not_download)

    resp = app.test_client().post('/api/jobs/download-darktable')
    assert resp.status_code == 400
    assert 'space' in resp.get_json()['error'].lower()


def test_api_job_download_darktable_400_when_the_target_dir_is_unusable(
        app_and_db, monkeypatch):
    """A read-only home must not 500 a button press.

    free_space_bytes creates the tools directory, so it raises when ~/.vireo
    is not writable. A 500 tells the user nothing they can act on.
    """
    import darktable_install
    app, _ = app_and_db
    monkeypatch.setattr(darktable_install, "resolve_release",
                        lambda: (_fake_release(), None))

    def denied(path):
        raise PermissionError(13, "Permission denied")
    monkeypatch.setattr(darktable_install, "free_space_bytes", denied)

    def must_not_download(*a, **k):
        raise AssertionError("must not download into an unusable directory")
    monkeypatch.setattr(darktable_install, "download", must_not_download)

    resp = app.test_client().post('/api/jobs/download-darktable')
    assert resp.status_code == 400
    error = resp.get_json()['error']
    assert 'Permission denied' in error, error
    assert 'darktable' in error, f"the message must name the directory: {error!r}"


def test_api_job_download_darktable_400_when_unavailable(app_and_db, monkeypatch):
    import darktable_install
    app, _ = app_and_db
    monkeypatch.setattr(darktable_install, "resolve_release",
                        lambda: (None, darktable_install.REASON_NO_PLATFORM_BUILD))

    resp = app.test_client().post('/api/jobs/download-darktable')
    assert resp.status_code == 400
    # The 400 body must carry the specific reason, not a generic message.
    assert resp.get_json()['error'] == darktable_install.REASON_NO_PLATFORM_BUILD


def test_api_job_download_darktable_400_says_unreachable_not_unsupported(
        app_and_db, monkeypatch):
    """A user behind a proxy must not be told their platform is unsupported.

    Paired with the test above, this pins that the route relays whichever
    reason resolve_release gave rather than collapsing both to one sentence.
    """
    import darktable_install
    app, _ = app_and_db
    monkeypatch.setattr(darktable_install, "resolve_release",
                        lambda: (None, darktable_install.REASON_UNREACHABLE))

    resp = app.test_client().post('/api/jobs/download-darktable')
    assert resp.status_code == 400
    assert resp.get_json()['error'] == darktable_install.REASON_UNREACHABLE


def test_api_job_download_darktable_400_when_resolve_raises(app_and_db, monkeypatch):
    """resolve_release is documented never to raise; if it does, no 500."""
    import darktable_install
    app, _ = app_and_db

    def boom():
        raise OSError("network unreachable")
    monkeypatch.setattr(darktable_install, "resolve_release", boom)

    resp = app.test_client().post('/api/jobs/download-darktable')
    assert resp.status_code == 400
    assert resp.get_json()['error'] == darktable_install.REASON_UNREACHABLE


def test_api_job_download_darktable_resolves_fresh_not_from_cache(app_and_db, monkeypatch):
    """The job re-resolves server-side so it never downloads a stale URL.

    /api/darktable/install/available caches for 10 minutes; if the job read
    that cache it could fetch a URL the user never saw confirmed, or one
    GitHub has since replaced.
    """
    import darktable_install
    app, _ = app_and_db

    stale = dict(_fake_release("5.0.0"),
                 url="https://github.com/darktable-org/darktable/releases/download/x/stale.dmg")
    fresh = dict(_fake_release("5.6.0"),
                 url="https://github.com/darktable-org/darktable/releases/download/x/fresh.dmg")
    darktable_install._release_cache.update(at=time.monotonic(), value=stale)

    captured = {}

    def fake_download(asset, **kwargs):
        captured['asset'] = asset
        return "/tmp/fresh.dmg", "ok"

    _stub_happy_path(monkeypatch, release=fresh, download=fake_download)

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)

    assert job['status'] == 'completed', job
    assert captured['asset']['url'].endswith("fresh.dmg"), captured['asset']
    assert job['result']['version'] == "5.6.0"


def test_api_job_download_darktable_reports_verification_detail_verbatim(
        app_and_db, monkeypatch):
    """download() returns the only sentence that says WHAT was checked.

    verify_digest returns ok=True both when the digest matched and when GitHub
    published no digest at all. Synthesizing "Verified" from that boolean would
    tell a user their unverifiable download was verified.
    """
    app, _ = app_and_db
    detail = ("GitHub published no digest for this asset, so its contents could not "
              "be verified.")
    _stub_happy_path(monkeypatch, detail=detail)

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)

    assert job['status'] == 'completed', job
    assert job['result']['verified'] == detail


def test_api_job_download_darktable_verifying_progress_carries_the_detail(
        app_and_db, monkeypatch):
    """The Verifying event must send total=0 and carry the detail.

    A non-zero total makes the Settings progress UI take its byte-count branch
    and render "Verifying: 0 of 0 MB" instead of what was actually checked.
    """
    app, _ = app_and_db
    detail = "SHA256 matches the digest GitHub published for this asset (abc…)."
    _stub_happy_path(monkeypatch, detail=detail)

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    from wait import wait_for_job_via_client
    wait_for_job_via_client(client, job_id)

    events = app._job_runner.get_events(job_id)
    verifying = [e['data'] for e in events
                 if e['type'] == 'progress' and e['data'].get('phase') == 'Verifying']
    assert verifying, f"no Verifying progress event in {events!r}"
    assert verifying[-1]['total'] == 0
    assert verifying[-1]['current_file'] == detail


def test_api_job_download_darktable_reports_byte_progress(app_and_db, monkeypatch):
    """byte_callback must reach the job's event stream, unclamped.

    Progress moving backwards is honest: when a server ignores Range the
    retry truncates the .partial and restarts from 0.
    """
    app, _ = app_and_db

    def fake_download(asset, byte_callback=None, should_cancel=None):
        byte_callback(1024, 87094261)
        byte_callback(4096, 87094261)
        byte_callback(0, 87094261)  # server ignored Range; restarted
        return "/tmp/d.dmg", "ok"

    _stub_happy_path(monkeypatch, download=fake_download)

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)
    assert job['status'] == 'completed', job

    events = app._job_runner.get_events(job_id)
    downloading = [e['data'] for e in events
                   if e['type'] == 'progress'
                   and e['data'].get('phase') == 'Downloading darktable']
    assert [d['current'] for d in downloading] == [1024, 4096, 0]
    assert all(d['total'] == 87094261 for d in downloading)
    assert all(d['current_file'] == "darktable-5.6.0-arm64.dmg" for d in downloading)


def test_api_job_download_darktable_writes_config_when_handoff_returns_bin(
        app_and_db, monkeypatch):
    """Linux: the AppImage we downloaded IS the binary, so record it.

    Without this the user installs darktable and Settings still says it is
    missing.
    """
    import config as cfg
    app, _ = app_and_db
    bin_path = "/tmp/darktable-5.6.0-x86_64.AppImage"
    _stub_happy_path(monkeypatch, hand_off_result={
        "action": "installed", "location": bin_path, "bin_path": bin_path,
    })

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)

    assert job['status'] == 'completed', job
    assert cfg.get("darktable_bin") == bin_path
    # And the result must say the write happened, so the UI can report it.
    assert job['result']['config_written'] is True
    assert job['result']['bin_path'] == bin_path


def test_api_job_download_darktable_config_write_stays_raw_not_pinned(
        app_and_db, monkeypatch):
    """The bin_path write must not pin every DEFAULTS value into config.json.

    Regression: the earlier path wrote via ``cfg.set()`` (load-modify-save on
    the merged config), which pinned the current default for every setting
    the user had never touched — silently blocking future default upgrades.
    Routing through ``_settings_write_lock`` + raw read-modify-write leaves
    the file minimal, the same way every other settings endpoint writes.
    """
    import config as cfg
    app, _ = app_and_db
    bin_path = "/tmp/darktable-5.6.0-x86_64.AppImage"
    _stub_happy_path(monkeypatch, hand_off_result={
        "action": "installed", "location": bin_path, "bin_path": bin_path,
    })

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    from wait import wait_for_job_via_client
    wait_for_job_via_client(client, job_id)

    # Read the on-disk file directly, not through cfg.load()/cfg.get(): those
    # deep-merge DEFAULTS in and would hide a pinned-defaults regression.
    with open(cfg.CONFIG_PATH) as f:
        raw = json.load(f)
    assert raw.get("darktable_bin") == bin_path
    # If cfg.set had been used, raw would carry every DEFAULTS key (e.g.
    # thumbnail_size, preview_max_size, DEFAULTS["pipeline"], ...).
    assert "thumbnail_size" not in raw, (
        f"config write pinned unrelated defaults into config.json: {sorted(raw.keys())}"
    )
    assert "pipeline" not in raw, (
        f"config write pinned the DEFAULTS pipeline block: {sorted(raw.keys())}"
    )


def test_api_job_download_darktable_config_write_preserves_other_user_settings(
        app_and_db, monkeypatch):
    """A user setting saved through /api/config must survive the download.

    Both writers use the raw read-modify-write pattern under
    ``_settings_write_lock``, so nothing on the on-disk file is lost when the
    download finishes right after a settings save.
    """
    import config as cfg
    app, _ = app_and_db
    client = app.test_client()

    # Persist a user setting the normal way — the settings endpoint uses
    # _settings_write_lock + raw read-modify-write.
    resp = client.post('/api/config',
                       data=json.dumps({"darktable_style": "Wildlife"}),
                       content_type='application/json')
    assert resp.status_code == 200

    bin_path = "/tmp/darktable-5.6.0-x86_64.AppImage"
    _stub_happy_path(monkeypatch, hand_off_result={
        "action": "installed", "location": bin_path, "bin_path": bin_path,
    })

    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    from wait import wait_for_job_via_client
    wait_for_job_via_client(client, job_id)

    # Both the new value and the pre-existing one must be on disk.
    with open(cfg.CONFIG_PATH) as f:
        raw = json.load(f)
    assert raw.get("darktable_bin") == bin_path
    assert raw.get("darktable_style") == "Wildlife"


def test_api_job_download_darktable_leaves_config_alone_without_bin(
        app_and_db, monkeypatch):
    """macOS/Windows hand off to an installer; where the app lands is unknown.

    Writing a guessed darktable_bin here would overwrite a path the user
    configured with one that does not exist.
    """
    import config as cfg
    app, _ = app_and_db
    cfg.set("darktable_bin", "/sentinel/darktable-cli")
    _stub_happy_path(monkeypatch)

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)

    assert job['status'] == 'completed', job
    assert cfg.get("darktable_bin") == "/sentinel/darktable-cli"
    assert job['result']['config_written'] is False


def test_api_job_download_darktable_cancel_ends_cancelled_not_failed(
        app_and_db, monkeypatch):
    """A user pressing Stop must not see a red "failed" job.

    The UI distinguishes the two, and a cancelled job carries no errors.
    """
    app, _ = app_and_db
    import taxonomy

    def fake_download(asset, byte_callback=None, should_cancel=None):
        deadline = time.monotonic() + 20
        while not should_cancel():
            if time.monotonic() > deadline:
                raise AssertionError("cancellation never reached the download")
            time.sleep(0.01)
        raise taxonomy.DownloadCancelled("cancelled")

    _stub_happy_path(monkeypatch, download=fake_download)

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    assert client.post(f'/api/jobs/{job_id}/cancel').status_code == 200

    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)
    assert job['status'] == 'cancelled', job
    assert not job['errors'], job['errors']


def test_api_job_download_darktable_handoff_failure_fails_the_job(app_and_db, monkeypatch):
    """hand_off raises after a good download; the message names the saved file.

    That path is the user's way forward, so it must survive into job errors
    rather than being swallowed into a generic failure.
    """
    import darktable_install
    app, _ = app_and_db
    _stub_happy_path(monkeypatch)

    def boom(path, **kwargs):
        raise RuntimeError(
            f"Downloaded to {path}, but macOS could not open it (exit code 1). "
            "Open it yourself to install darktable."
        )
    monkeypatch.setattr(darktable_install, "hand_off", boom)

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)

    assert job['status'] == 'failed', job
    assert any("/tmp/d.dmg" in e for e in job['errors']), job['errors']


def test_api_job_download_darktable_joins_a_running_job_instead_of_starting_a_second(
        app_and_db, monkeypatch):
    """Two clicks (two tabs, a refresh mid-download, or a double-click) must
    not start two workers writing the same .partial.  Both would race
    truncation, rename, verification, and deletion; the second cleanup could
    delete the first one's verified installer.  The second POST returns the
    already-running job so the client subscribes to it."""
    import threading

    app, _ = app_and_db

    started = threading.Event()
    can_finish = threading.Event()

    def slow_download(asset, byte_callback=None, should_cancel=None):
        started.set()
        # Block until the second POST has had its chance.  Any test-timeout
        # would surface here, not as a mystery hang.
        assert can_finish.wait(timeout=synchronization_timeout(10)), "test never released the download"
        return "/tmp/d.dmg", "ok"

    _stub_happy_path(monkeypatch, download=slow_download)

    client = app.test_client()
    first = client.post('/api/jobs/download-darktable').get_json()
    first_id = first['job_id']
    assert started.wait(timeout=synchronization_timeout(5)), "the first download never started"

    second = client.post('/api/jobs/download-darktable').get_json()

    # Same job — no new worker, no second .partial, no race.
    assert second['job_id'] == first_id
    assert second.get('joined_existing') is True

    # Let the (single) worker finish so nothing leaks past this test.
    can_finish.set()
    from wait import wait_for_job_via_client
    wait_for_job_via_client(client, first_id)


def test_api_job_download_darktable_rejects_a_new_run_only_while_active(
        app_and_db, monkeypatch):
    """The singleton guard has to release when the first job reaches a
    terminal state.  Otherwise a completed (or cancelled) download would pin
    the button forever."""
    app, _ = app_and_db
    _stub_happy_path(monkeypatch)

    client = app.test_client()
    first_id = client.post('/api/jobs/download-darktable').get_json()['job_id']
    from wait import wait_for_job_via_client
    wait_for_job_via_client(client, first_id)

    second = client.post('/api/jobs/download-darktable').get_json()
    assert second['job_id'] != first_id, (
        "after the first job finishes, a fresh POST must start a new job"
    )
    wait_for_job_via_client(client, second['job_id'])


def test_api_job_download_darktable_binds_to_the_confirmed_asset(app_and_db, monkeypatch):
    """If the release changes between the dialog opening and the POST landing,
    the server must not silently download the new artifact — the user
    confirmed a different one.  Code=darktable_asset_changed lets the client
    re-fetch and re-prompt with the new identity."""
    app, _ = app_and_db
    fresh = _fake_release("5.7.0")
    fresh["name"] = "darktable-5.7.0-arm64.dmg"

    def must_not_download(*a, **k):
        raise AssertionError("mismatched asset must not be downloaded")

    _stub_happy_path(monkeypatch, release=fresh, download=must_not_download)

    resp = app.test_client().post(
        '/api/jobs/download-darktable',
        data=json.dumps({
            "expected_version": "5.6.0",
            "expected_name": "darktable-5.6.0-arm64.dmg",
            "expected_digest": "sha256:" + "a" * 64,
        }),
        content_type='application/json',
    )

    assert resp.status_code == 409
    body = resp.get_json()
    assert body['code'] == 'darktable_asset_changed'
    # Must name both the confirmed and the current artifact so the user knows
    # what changed.
    assert "5.6.0" in body['error']
    assert "5.7.0" in body['error']


def test_api_job_download_darktable_binds_to_the_confirmed_size_for_digestless_assets(
        app_and_db, monkeypatch):
    """A release with no digest still needs a size check.

    If GitHub deletes and re-uploads an asset under the same tag and filename
    within the availability cache's TTL, name and version match but the bytes
    differ.  With no digest to compare, the name/version check alone would
    silently accept the swap.  ``expected_size`` closes that gap.
    """
    app, _ = app_and_db
    # A digestless release: this is the case the digest check cannot catch.
    fresh = _fake_release("5.6.0")
    fresh["digest"] = None
    fresh["size"] = 87094261 + 500_000  # Republished asset, same name+version

    def must_not_download(*a, **k):
        raise AssertionError("mismatched asset must not be downloaded")

    _stub_happy_path(monkeypatch, release=fresh, download=must_not_download)

    resp = app.test_client().post(
        '/api/jobs/download-darktable',
        data=json.dumps({
            "expected_version": "5.6.0",
            "expected_name": fresh["name"],
            "expected_digest": None,
            # Confirmation showed the original size — the republished asset
            # differs and must be rejected even though digest cannot arbitrate.
            "expected_size": 87094261,
        }),
        content_type='application/json',
    )
    assert resp.status_code == 409
    body = resp.get_json()
    assert body['code'] == 'darktable_asset_changed'


def test_api_job_download_darktable_409_refreshes_the_availability_cache(
        app_and_db, monkeypatch):
    """After a 409 the UI's Re-check must not hit the same stale asset again.

    The availability endpoint caches for 10 minutes.  Without the refresh, a
    Re-check between the POST that returned 409 and the TTL expiring would
    hand back the same cached name, the user would re-confirm the same
    identity, and the endpoint would 409 in a loop until the TTL elapsed.
    """
    import darktable_install
    app, _ = app_and_db

    stale = _fake_release("5.6.0")
    fresh = dict(_fake_release("5.7.0"))
    fresh["name"] = "darktable-5.7.0-arm64.dmg"

    # Seed the availability cache with the stale release, as if /install/available
    # ran once at dialog-open time and pinned 5.6.0.
    darktable_install._release_cache.update(at=time.monotonic(), value=stale)
    # The uncached resolver the POST handler calls returns the fresh release.
    monkeypatch.setattr(darktable_install, "resolve_release", lambda: (fresh, None))

    client = app.test_client()
    resp = client.post(
        '/api/jobs/download-darktable',
        data=json.dumps({
            "expected_version": stale["version"],
            "expected_name": stale["name"],
            "expected_digest": stale["digest"],
        }),
        content_type='application/json',
    )
    assert resp.status_code == 409
    assert resp.get_json()['code'] == 'darktable_asset_changed'

    # After the 409, /install/available must reflect the fresh release,
    # not the pre-POST cached one — otherwise the client's Re-check would
    # confirm the same stale asset and 409 again.
    data = client.get('/api/darktable/install/available').get_json()
    assert data['available'] is True
    assert data['name'] == "darktable-5.7.0-arm64.dmg"
    assert data['version'] == "5.7.0"


def test_api_job_download_darktable_proceeds_when_expected_matches(
        app_and_db, monkeypatch):
    """The confirmation binding must not break the normal flow: when the
    fresh resolution agrees with what the dialog showed, the download starts."""
    app, _ = app_and_db
    release = _fake_release("5.6.0")
    _stub_happy_path(monkeypatch, release=release)

    client = app.test_client()
    resp = client.post(
        '/api/jobs/download-darktable',
        data=json.dumps({
            "expected_version": release["version"],
            "expected_name": release["name"],
            "expected_digest": release["digest"],
        }),
        content_type='application/json',
    )
    assert resp.status_code == 200
    job_id = resp.get_json()['job_id']

    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)
    assert job['status'] == 'completed', job


def test_api_job_download_darktable_no_expected_still_works(app_and_db, monkeypatch):
    """A POST with no body (older client, curl, test harness) still works —
    the binding check only fires when the client actually sends expected_name."""
    app, _ = app_and_db
    _stub_happy_path(monkeypatch)

    client = app.test_client()
    # No content-type, no body: the JSON parse is silent (json_error's
    # get_json(silent=True) returns None) so the check is skipped.
    resp = client.post('/api/jobs/download-darktable')
    assert resp.status_code == 200

    from wait import wait_for_job_via_client
    wait_for_job_via_client(client, resp.get_json()['job_id'])


def test_api_job_download_darktable_cancellation_between_verify_and_handoff_does_not_hand_off(
        app_and_db, monkeypatch):
    """A Stop press after the digest matches but before hand_off runs must
    NOT open the installer (macOS/Windows) or chmod+install the AppImage
    (Linux).  jobs.py would still label the job cancelled but the side effect
    the user cancelled would already have happened."""
    import darktable_install
    app, _ = app_and_db

    def fake_download(asset, byte_callback=None, should_cancel=None):
        # Download and verify succeed; we simulate the user pressing Stop
        # right at this seam — the request the endpoint already routed to
        # jobs' cancel set will be observed after download() returns.
        return "/tmp/d.dmg", "ok"

    hand_off_calls = []

    def spy_hand_off(path, **kwargs):
        hand_off_calls.append(path)
        return {"action": "opened-installer", "location": path, "bin_path": None}

    _stub_happy_path(monkeypatch, download=fake_download,
                     hand_off_result={"action": "opened-installer",
                                      "location": "/tmp/d.dmg", "bin_path": None})
    monkeypatch.setattr(darktable_install, "hand_off", spy_hand_off)

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']

    # Cancel racing with the worker: request cancellation from the request
    # thread, and the check between download() and hand_off() sees it.
    app._job_runner.cancel_job(job_id)

    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)

    # This test can be flaky if download() runs entirely before cancel_job()
    # lands.  Confirm the state we care about explicitly.
    if job['status'] == 'cancelled':
        assert hand_off_calls == [], (
            "hand_off must not run after a Stop was observed"
        )


def test_api_job_download_darktable_handoff_phase_is_uncancellable(
        app_and_db, monkeypatch):
    """A Stop that arrives WHILE hand_off is executing must be rejected —
    otherwise cancellation "wins" the terminal-status race even though the
    installer has already been opened / the AppImage chmodded, and the user
    sees "cancelled" for a side effect that in fact committed.

    Enforced by wrapping hand_off in begin_uncancellable(): once inside,
    cancel_job() returns False and the run finishes as completed."""
    import threading

    import darktable_install
    app, _ = app_and_db

    in_handoff = threading.Event()
    can_finish_handoff = threading.Event()

    def slow_hand_off(path, **kwargs):
        # Signal that we are past the cancellation gate; hold long enough
        # for the test's cancel_job() call to land.
        in_handoff.set()
        assert can_finish_handoff.wait(timeout=synchronization_timeout(10)), "hand_off never released"
        return {"action": "opened-installer", "location": path, "bin_path": None}

    _stub_happy_path(monkeypatch)
    monkeypatch.setattr(darktable_install, "hand_off", slow_hand_off)

    client = app.test_client()
    job_id = client.post('/api/jobs/download-darktable').get_json()['job_id']

    # Wait until the worker is inside hand_off, then request cancellation.
    assert in_handoff.wait(timeout=synchronization_timeout(5)), "hand_off was never entered"
    cancel_accepted = app._job_runner.cancel_job(job_id)
    # begin_uncancellable() has already fired, so the cancel is a no-op.
    assert cancel_accepted is False, (
        "cancel_job during hand_off must be rejected — otherwise the terminal "
        "status would flip to cancelled while the side effect already committed"
    )

    # Let hand_off complete; the job must finish as 'completed', not
    # 'cancelled', because the side effect DID happen.
    can_finish_handoff.set()
    from wait import wait_for_job_via_client
    job = wait_for_job_via_client(client, job_id)
    assert job['status'] == 'completed', job
    assert job['result']['action'] == "opened-installer"


def test_api_job_download_darktable_rejects_a_join_for_a_different_artifact(
        app_and_db, monkeypatch):
    """An in-flight download of version X + a fresh POST confirming version
    Y must NOT join the old job — the user's dialog said Y and Y is what
    they must receive (or an error). Silently joining a mismatched running
    job would deliver X's bytes under Y's identity."""
    import threading
    app, _ = app_and_db

    running = threading.Event()
    release_download = threading.Event()

    def slow_download(asset, byte_callback=None, should_cancel=None):
        running.set()
        assert release_download.wait(timeout=synchronization_timeout(10)), "download never released"
        return "/tmp/d.dmg", "ok"

    # First job resolves to 5.6.0.
    first_release = _fake_release("5.6.0")
    _stub_happy_path(monkeypatch, release=first_release, download=slow_download)

    client = app.test_client()
    first_id = client.post(
        '/api/jobs/download-darktable',
        data=json.dumps({
            "expected_version": first_release["version"],
            "expected_name": first_release["name"],
            "expected_digest": first_release["digest"],
        }),
        content_type='application/json',
    ).get_json()['job_id']
    assert running.wait(timeout=synchronization_timeout(5)), "the first download never started"

    # Second POST confirms a DIFFERENT artifact (a newer release the tab saw
    # from a separate /install/available call).  Even though a singleton is
    # running, the endpoint must not join it — it would deliver 5.6.0's bytes.
    resp = client.post(
        '/api/jobs/download-darktable',
        data=json.dumps({
            "expected_version": "5.7.0",
            "expected_name": "darktable-5.7.0-arm64.dmg",
            "expected_digest": "sha256:" + "b" * 64,
        }),
        content_type='application/json',
    )
    assert resp.status_code == 409, resp.get_json()
    body = resp.get_json()
    assert body['code'] == 'darktable_asset_changed'
    # Message must name the version in flight and the version the user
    # confirmed so they know why they were bounced back.
    assert "5.6.0" in body['error']
    assert "5.7.0" in body['error']

    release_download.set()
    from wait import wait_for_job_via_client
    wait_for_job_via_client(client, first_id)


def test_api_job_download_darktable_second_join_matches_when_artifacts_align(
        app_and_db, monkeypatch):
    """A second POST for the SAME artifact still joins the running job —
    the mismatch guard must not accidentally reject compatible joins.
    (Two Settings tabs, a refresh, a double-click — all should share one
    download.)"""
    import threading
    app, _ = app_and_db

    running = threading.Event()
    release_download = threading.Event()

    def slow_download(asset, byte_callback=None, should_cancel=None):
        running.set()
        assert release_download.wait(timeout=synchronization_timeout(10)), "download never released"
        return "/tmp/d.dmg", "ok"

    release = _fake_release("5.6.0")
    _stub_happy_path(monkeypatch, release=release, download=slow_download)

    client = app.test_client()
    body = {
        "expected_version": release["version"],
        "expected_name": release["name"],
        "expected_digest": release["digest"],
    }
    first_id = client.post(
        '/api/jobs/download-darktable',
        data=json.dumps(body), content_type='application/json',
    ).get_json()['job_id']
    assert running.wait(timeout=synchronization_timeout(5))

    second = client.post(
        '/api/jobs/download-darktable',
        data=json.dumps(body), content_type='application/json',
    ).get_json()
    assert second['job_id'] == first_id, (
        "matching artifact identity must still join the running job"
    )
    assert second.get('joined_existing') is True

    release_download.set()
    from wait import wait_for_job_via_client
    wait_for_job_via_client(client, first_id)


def test_api_job_download_darktable_join_rejects_size_mismatch_for_digestless_asset(
        app_and_db, monkeypatch):
    """Two tabs, one in-flight digestless download, republished asset between
    them: the second POST confirms a different size for the same name+version,
    and must NOT silently join a worker downloading different bytes.

    Covers the join path specifically, which stores ``asset_size`` in the
    singleton config so ``_artifact_matches`` can arbitrate without a digest.
    """
    import threading
    app, _ = app_and_db

    running = threading.Event()
    release_download = threading.Event()

    def slow_download(asset, byte_callback=None, should_cancel=None):
        running.set()
        assert release_download.wait(timeout=synchronization_timeout(10)), "download never released"
        return "/tmp/d.dmg", "ok"

    first_release = _fake_release("5.6.0")
    first_release["digest"] = None
    _stub_happy_path(monkeypatch, release=first_release, download=slow_download)

    client = app.test_client()
    first_id = client.post(
        '/api/jobs/download-darktable',
        data=json.dumps({
            "expected_version": first_release["version"],
            "expected_name": first_release["name"],
            "expected_digest": None,
            "expected_size": first_release["size"],
        }),
        content_type='application/json',
    ).get_json()['job_id']
    assert running.wait(timeout=synchronization_timeout(5))

    # Same name+version, no digest, different size — the republished-asset
    # case.  Without the size guard this would silently return
    # joined_existing=True and hand back the first job's bytes.
    resp = client.post(
        '/api/jobs/download-darktable',
        data=json.dumps({
            "expected_version": first_release["version"],
            "expected_name": first_release["name"],
            "expected_digest": None,
            "expected_size": first_release["size"] + 500_000,
        }),
        content_type='application/json',
    )
    assert resp.status_code == 409, resp.get_json()
    assert resp.get_json()['code'] == 'darktable_asset_changed'

    release_download.set()
    from wait import wait_for_job_via_client
    wait_for_job_via_client(client, first_id)


def test_api_job_download_darktable_singleton_check_and_start_are_atomic(
        app_and_db, monkeypatch):
    """The earlier check-then-start pattern let two concurrent POSTs both
    see "no running job" and both call runner.start(), racing on the same
    ``.partial``. The fix is to check-and-start under a single lock
    acquisition inside JobRunner. Exercise it by hitting the endpoint from
    many threads at once and asserting exactly one worker was started."""
    import threading
    app, _ = app_and_db

    workers_started = []
    workers_lock = threading.Lock()
    release_download = threading.Event()

    def slow_download(asset, byte_callback=None, should_cancel=None):
        with workers_lock:
            workers_started.append(1)
        assert release_download.wait(timeout=synchronization_timeout(10)), "download never released"
        return "/tmp/d.dmg", "ok"

    _stub_happy_path(monkeypatch, download=slow_download)

    client = app.test_client()
    ids = []
    ids_lock = threading.Lock()
    barrier = threading.Barrier(8)

    def fire():
        barrier.wait()
        r = client.post('/api/jobs/download-darktable').get_json()
        with ids_lock:
            ids.append(r['job_id'])

    threads = [threading.Thread(target=fire) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(set(ids)) == 1, (
        f"all concurrent POSTs must return the same job id, got {set(ids)}"
    )
    # This is the load-bearing assertion: no matter how many callers slipped
    # between the old list_jobs() check and start(), only ONE download worker
    # ran. Two would race the same .partial and installer.
    release_download.set()
    from wait import wait_for_job_via_client
    wait_for_job_via_client(client, ids[0])
    assert len(workers_started) == 1, (
        f"exactly one download worker must run, got {len(workers_started)}"
    )
