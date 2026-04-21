import json
import os
import time


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
                       }),
                       content_type='application/json')
    assert resp.status_code == 200

    resp2 = client.get('/api/config')
    cfg = resp2.get_json()
    assert cfg["darktable_bin"] == "/usr/local/bin/darktable-cli"
    assert cfg["darktable_style"] == "Wildlife"
    assert cfg["darktable_output_format"] == "tiff"
    assert cfg["darktable_output_dir"] == "/output"


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
