import json
import subprocess
import sys


class _FakeCompleted:
    def __init__(self, returncode=0, stderr=""):
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = ""


def _patch_launchers(monkeypatch, run_returncode=0, run_stderr=""):
    """Capture both subprocess.Popen and subprocess.run invocations.

    Returns a list of recorded calls. Each entry is (kind, cmd) where kind is
    either 'popen' or 'run'.
    """
    launched = []

    def fake_popen(cmd, **kwargs):
        launched.append(('popen', cmd))

    def fake_run(cmd, **kwargs):
        launched.append(('run', cmd))
        return _FakeCompleted(returncode=run_returncode, stderr=run_stderr)

    monkeypatch.setattr(subprocess, 'Popen', fake_popen)
    monkeypatch.setattr(subprocess, 'run', fake_run)
    return launched


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
    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['opened'] == 1
    assert len(launched) == 1


def test_open_external_uses_configured_editor(app_and_db, monkeypatch):
    """Uses external_editor from config when set to a plain executable."""
    app, db = app_and_db
    client = app.test_client()

    client.post('/api/config',
                data=json.dumps({"external_editor": "/usr/bin/gimp"}),
                content_type='application/json')

    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200
    # Plain executable: launched via Popen, not `open -a`
    assert launched[0][1][0] == '/usr/bin/gimp'


def test_open_external_returns_500_on_launch_failure(app_and_db, monkeypatch):
    """POST /api/photos/open-external returns 500 when launcher raises."""
    app, _ = app_and_db
    client = app.test_client()

    def failing_popen(cmd, **kwargs):
        raise FileNotFoundError("No such file: /bad/editor")
    def failing_run(cmd, **kwargs):
        raise FileNotFoundError("No such file: /bad/editor")
    monkeypatch.setattr(subprocess, 'Popen', failing_popen)
    monkeypatch.setattr(subprocess, 'run', failing_run)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 500
    assert "error" in resp.get_json()


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


def test_open_external_resolves_app_bundle_inside_directory(app_and_db, monkeypatch, tmp_path):
    """A directory containing a .app bundle is resolved to that bundle.

    Adobe Lightroom Classic installs to /Applications/Adobe Lightroom Classic/
    (a folder) containing Adobe Lightroom Classic.app. Setting the folder as
    the editor must still launch the bundled app.
    """
    app, _ = app_and_db
    client = app.test_client()

    monkeypatch.setattr(sys, 'platform', 'darwin')

    # Build a fake app structure: <tmp>/MyEditor/MyEditor.app/
    container = tmp_path / "MyEditor"
    bundle = container / "MyEditor.app"
    bundle.mkdir(parents=True)

    client.post('/api/config',
                data=json.dumps({"external_editor": str(container)}),
                content_type='application/json')

    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200, resp.get_json()
    # Should be `open -a <bundle>` via subprocess.run
    assert launched[0][0] == 'run'
    assert launched[0][1][0] == 'open'
    assert launched[0][1][1] == '-a'
    assert launched[0][1][2] == str(bundle)


def test_open_external_app_bundle_path_uses_open_a(app_and_db, monkeypatch, tmp_path):
    """A direct .app path is launched with `open -a`."""
    app, _ = app_and_db
    client = app.test_client()

    monkeypatch.setattr(sys, 'platform', 'darwin')

    bundle = tmp_path / "Foo.app"
    bundle.mkdir()

    client.post('/api/config',
                data=json.dumps({"external_editor": str(bundle)}),
                content_type='application/json')

    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200
    assert launched[0][0] == 'run'
    assert launched[0][1][:3] == ['open', '-a', str(bundle)]


def test_open_external_surfaces_open_command_failure(app_and_db, monkeypatch, tmp_path):
    """Non-zero exit from `open` is reported as 500 with stderr in the body."""
    app, _ = app_and_db
    client = app.test_client()

    monkeypatch.setattr(sys, 'platform', 'darwin')

    bundle = tmp_path / "Broken.app"
    bundle.mkdir()

    client.post('/api/config',
                data=json.dumps({"external_editor": str(bundle)}),
                content_type='application/json')

    _patch_launchers(monkeypatch, run_returncode=1,
                     run_stderr="The application cannot be opened.")

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 500
    body = resp.get_json()
    assert "cannot be opened" in body["error"]


def test_open_external_directory_without_app_bundle_errors(app_and_db, monkeypatch, tmp_path):
    """Directory with no .app inside reports a clear error instead of execing it."""
    app, _ = app_and_db
    client = app.test_client()

    monkeypatch.setattr(sys, 'platform', 'darwin')

    container = tmp_path / "NotAnApp"
    container.mkdir()

    client.post('/api/config',
                data=json.dumps({"external_editor": str(container)}),
                content_type='application/json')

    _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 500
    body = resp.get_json()
    assert ".app" in body["error"]


def test_open_external_expands_user_in_editor_path(app_and_db, monkeypatch, tmp_path):
    """`~`-prefixed editor paths are expanded before use."""
    app, _ = app_and_db
    client = app.test_client()

    monkeypatch.setattr(sys, 'platform', 'darwin')
    monkeypatch.setenv('HOME', str(tmp_path))

    bundle_dir = tmp_path / "Apps"
    bundle = bundle_dir / "Tilde.app"
    bundle.mkdir(parents=True)

    client.post('/api/config',
                data=json.dumps({"external_editor": "~/Apps/Tilde.app"}),
                content_type='application/json')

    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200
    assert launched[0][1][2] == str(bundle)
