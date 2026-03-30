import json


def test_open_external_requires_photo_ids(app_and_db):
    """POST /api/photos/open-external returns 400 without photo_ids."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/photos/open-external',
                       data=json.dumps({}),
                       content_type='application/json')
    assert resp.status_code == 400


def test_open_external_returns_404_for_missing_photos(app_and_db):
    """POST /api/photos/open-external returns 404 for nonexistent photos."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [9999]}),
                       content_type='application/json')
    assert resp.status_code == 404


def test_open_external_success(app_and_db, monkeypatch):
    """POST /api/photos/open-external opens photos and returns count."""
    app, db = app_and_db
    client = app.test_client()

    launched = []
    def fake_popen(cmd, **kwargs):
        launched.append(cmd)

    import subprocess
    monkeypatch.setattr(subprocess, 'Popen', fake_popen)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['opened'] == 1
    assert len(launched) == 1


def test_open_external_uses_configured_editor(app_and_db, monkeypatch):
    """Uses external_editor from config when set."""
    app, db = app_and_db
    client = app.test_client()

    # Set configured editor
    client.post('/api/config',
                data=json.dumps({"external_editor": "/usr/bin/gimp"}),
                content_type='application/json')

    launched = []
    def fake_popen(cmd, **kwargs):
        launched.append(cmd)

    import subprocess
    monkeypatch.setattr(subprocess, 'Popen', fake_popen)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200
    assert launched[0][0] == '/usr/bin/gimp'


def test_config_saves_external_editor(app_and_db):
    """POST /api/config saves external_editor setting."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post('/api/config',
                       data=json.dumps({"external_editor": "/usr/bin/rawtherapee"}),
                       content_type='application/json')
    assert resp.status_code == 200
    resp2 = client.get('/api/config')
    cfg = resp2.get_json()
    assert cfg["external_editor"] == "/usr/bin/rawtherapee"
