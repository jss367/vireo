import json


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
