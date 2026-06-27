import json
import os
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
    client.post('/api/config',
                data=json.dumps({"external_editor": sys.executable}),
                content_type='application/json')
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


def test_open_external_hands_off_rendered_edit_recipe(client_with_photo, monkeypatch):
    """External editors receive a rendered derivative when Vireo edits exist."""
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    db.set_photo_edit_recipe(
        photo_id,
        {"rotation": 90, "flip": {"horizontal": True}},
    )
    client.post('/api/config',
                data=json.dumps({"external_editor": "/usr/bin/gimp"}),
                content_type='application/json')
    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [photo_id]}),
                       content_type='application/json')

    assert resp.status_code == 200
    opened_path = launched[0][1][1]
    assert os.path.basename(opened_path) == f"{photo_id}.jpg"
    assert os.path.basename(os.path.dirname(opened_path)) == "external-edits"
    with Image.open(opened_path) as img:
        assert img.size == (600, 800)


def test_open_external_edit_recipe_avoids_capped_working_copy(
    client_with_photo, monkeypatch,
):
    """Rotate/flip handoffs should render from full-res original when wc is small."""
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (400, 300), (10, 20, 30)).save(working_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    client.post('/api/config',
                data=json.dumps({"external_editor": "/usr/bin/gimp"}),
                content_type='application/json')
    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [photo_id]}),
                       content_type='application/json')

    assert resp.status_code == 200
    opened_path = launched[0][1][1]
    with Image.open(opened_path) as img:
        assert img.size == (600, 800)


def test_open_external_uses_working_copy_when_raw_source_missing(
    client_with_photo, monkeypatch,
):
    """RAW-first handoff should still work when the RAW volume is offline."""
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{photo_id}.jpg")
    Image.new("RGB", (800, 600), (10, 20, 30)).save(working_path, "JPEG")
    db.conn.execute(
        """UPDATE photos
           SET filename='offline.NEF', extension='.nef',
               working_copy_path=?,
               width=800, height=600
           WHERE id=?""",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    client.post('/api/config',
                data=json.dumps({"external_editor": "/usr/bin/gimp"}),
                content_type='application/json')
    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [photo_id]}),
                       content_type='application/json')

    assert resp.status_code == 200, resp.get_json()
    opened_path = launched[0][1][1]
    assert os.path.basename(os.path.dirname(opened_path)) == "external-edits"
    with Image.open(opened_path) as img:
        assert img.size == (600, 800)


def test_open_external_uses_companion_when_raw_source_missing(
    client_with_photo, monkeypatch,
):
    """Offline RAW+JPEG handoff should use a full-size sidecar if no wc exists."""
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    db.conn.execute(
        """UPDATE photos
           SET filename='offline.NEF', extension='.nef',
               working_copy_path=NULL,
               companion_path='test.jpg',
               width=800, height=600
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"rotation": 90})
    client.post('/api/config',
                data=json.dumps({"external_editor": "/usr/bin/gimp"}),
                content_type='application/json')
    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [photo_id]}),
                       content_type='application/json')

    assert resp.status_code == 200, resp.get_json()
    opened_path = launched[0][1][1]
    assert os.path.basename(os.path.dirname(opened_path)) == "external-edits"
    with Image.open(opened_path) as img:
        assert img.size == (600, 800)


def test_open_external_uses_companion_for_cropped_raw_source_missing(
    client_with_photo, monkeypatch,
):
    """Offline cropped RAW+JPEG handoff should use a satisfying sidecar."""
    from PIL import Image

    app, db, photo_id = client_with_photo
    client = app.test_client()
    db.conn.execute(
        """UPDATE photos
           SET filename='offline.NEF', extension='.nef',
               working_copy_path=NULL,
               companion_path='test.jpg',
               width=800, height=600
           WHERE id=?""",
        (photo_id,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(
        photo_id,
        {"crop": {"x": 0, "y": 0, "w": 0.5, "h": 1}},
    )
    client.post('/api/config',
                data=json.dumps({"external_editor": "/usr/bin/gimp"}),
                content_type='application/json')
    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [photo_id]}),
                       content_type='application/json')

    assert resp.status_code == 200, resp.get_json()
    opened_path = launched[0][1][1]
    assert os.path.basename(os.path.dirname(opened_path)) == "external-edits"
    with Image.open(opened_path) as img:
        assert img.size == (400, 600)


def test_open_external_falls_back_to_companion_when_raw_decode_fails(
    app_and_db, monkeypatch, tmp_path,
):
    """For edited RAW+JPEG pairs, the handoff render must fall back to the
    full-size companion JPEG when libraw can't decode the RAW. Without the
    fallback, Open External fails for any RAW variant the system can't decode
    even though a usable JPEG handoff sits right next to it.
    """
    import image_loader
    from PIL import Image

    app, db = app_and_db
    client = app.test_client()

    source_dir = tmp_path / "ext-raw"
    source_dir.mkdir()
    raw_path = source_dir / "bird.NEF"
    raw_path.write_bytes(b"unsupported raw")
    jpg_path = source_dir / "bird.JPG"
    Image.new("RGB", (1200, 800), (60, 120, 200)).save(
        str(jpg_path), "JPEG", quality=85,
    )

    fid = db.add_folder(str(source_dir), name="ext-raw")
    pid = db.add_photo(
        folder_id=fid, filename="bird.NEF", extension=".nef",
        file_size=raw_path.stat().st_size,
        file_mtime=raw_path.stat().st_mtime,
        width=1200, height=800,
    )
    db.conn.execute(
        "UPDATE photos SET companion_path=? WHERE id=?",
        ("bird.JPG", pid),
    )
    db.set_photo_edit_recipe(pid, {"adjustments": {"exposure": 0.5}})
    db.conn.commit()

    real_load_image = image_loader.load_image
    load_calls = []

    def fake_load_image(path, *args, **kwargs):
        load_calls.append(path)
        # Simulate libraw refusing the RAW variant.
        if path.lower().endswith(".nef"):
            return None
        return real_load_image(path, *args, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", fake_load_image)

    client.post(
        '/api/config',
        data=json.dumps({"external_editor": "/usr/bin/gimp"}),
        content_type='application/json',
    )
    launched = _patch_launchers(monkeypatch)

    resp = client.post(
        '/api/photos/open-external',
        data=json.dumps({"photo_ids": [pid]}),
        content_type='application/json',
    )
    assert resp.status_code == 200, resp.get_json()
    # RAW was tried first, then the companion fallback succeeded.
    assert any(p.lower().endswith(".nef") for p in load_calls), load_calls
    assert any(p.lower().endswith(".jpg") for p in load_calls), load_calls
    opened_path = launched[0][1][1]
    assert os.path.exists(opened_path)
    assert os.path.basename(os.path.dirname(opened_path)) == "external-edits"


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


def test_open_external_converts_nikon_he_nef_for_darktable(
        app_and_db, monkeypatch, tmp_path):
    """Opening a Nikon HE NEF in darktable sends a persistent DNG instead."""
    app, db = app_and_db
    client = app.test_client()

    source_dir = tmp_path / "photos"
    source_dir.mkdir()
    raw_path = source_dir / "bird.NEF"
    raw_path.write_bytes(b"nef")
    fid = db.add_folder(str(source_dir), name="photos")
    pid = db.add_photo(
        folder_id=fid,
        filename="bird.NEF",
        extension=".nef",
        file_size=raw_path.stat().st_size,
        file_mtime=raw_path.stat().st_mtime,
    )
    db.conn.execute(
        "UPDATE photos SET exif_data=? WHERE id=?",
        (json.dumps({"Nikon": {"NEFCompression": "High Efficiency*"}}), pid),
    )
    db.conn.commit()

    monkeypatch.setattr(sys, 'platform', 'darwin')
    bundle = tmp_path / "darktable.app"
    bundle.mkdir()

    client.post('/api/config',
                data=json.dumps({
                    "external_editors": [
                        {"name": "darktable", "path": str(bundle)},
                    ],
                    "dng_converter_bin": "/fake/dng-converter",
                }),
                content_type='application/json')

    import develop

    def fake_convert_to_dng(dng_converter_bin, input_path, output_dir):
        assert dng_converter_bin == "/fake/dng-converter"
        assert input_path == str(raw_path)
        os.makedirs(output_dir, exist_ok=True)
        dng_path = os.path.join(output_dir, "bird.dng")
        with open(dng_path, "wb") as f:
            f.write(b"dng")
        return {"success": True, "output_path": dng_path, "error": None}

    monkeypatch.setattr(develop, "convert_to_dng", fake_convert_to_dng)
    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [pid]}),
                       content_type='application/json')
    assert resp.status_code == 200, resp.get_json()
    assert launched[0][0] == 'run'
    assert launched[0][1][:3] == ['open', '-a', str(bundle)]
    opened_path = launched[0][1][-1]
    assert opened_path.endswith(os.path.join("external-dng", str(pid), "bird.dng"))
    assert os.path.exists(opened_path)
    assert str(raw_path) not in launched[0][1]


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


def test_open_external_directory_with_multiple_app_bundles_errors(app_and_db, monkeypatch, tmp_path):
    """Directory containing multiple .app bundles reports a clear error instead of
    silently picking the first one alphabetically."""
    app, _ = app_and_db
    client = app.test_client()

    monkeypatch.setattr(sys, 'platform', 'darwin')

    container = tmp_path / "AmbiguousApps"
    (container / "Alpha.app").mkdir(parents=True)
    (container / "Beta.app").mkdir()

    client.post('/api/config',
                data=json.dumps({"external_editor": str(container)}),
                content_type='application/json')

    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 500
    body = resp.get_json()
    assert "Multiple" in body["error"]
    assert "Alpha.app" in body["error"]
    assert "Beta.app" in body["error"]
    assert launched == []


def test_open_external_filters_malformed_legacy_editor(app_and_db, monkeypatch):
    """Non-string legacy `external_editor` is filtered out, not an error.

    /api/config doesn't type-validate writes, so the persisted value can be any
    JSON type. cfg.get_editors() rejects non-string entries, so the endpoint
    falls back cleanly to the OS default opener instead of erroring.
    """
    app, _ = app_and_db
    client = app.test_client()

    import config as cfg
    cfg.set("external_editor", 12345)

    monkeypatch.setattr(sys, 'platform', 'darwin')
    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200
    # Falls through to plain `open <files>` (no -a flag, no editor binary).
    assert launched[0][0] == 'run'
    assert launched[0][1][0] == 'open'
    assert '-a' not in launched[0][1]


def test_open_external_uses_first_editor_from_list(app_and_db, monkeypatch):
    """external_editors list takes precedence over legacy external_editor."""
    app, _ = app_and_db
    client = app.test_client()

    client.post('/api/config',
                data=json.dumps({
                    "external_editor": "/usr/bin/legacy",
                    "external_editors": [
                        {"name": "Lightroom", "path": "/usr/bin/lr"},
                        {"name": "Affinity", "path": "/usr/bin/affinity"},
                    ],
                }),
                content_type='application/json')

    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200
    # First editor wins when no editor_index is passed; legacy field is ignored.
    assert launched[0][1][0] == '/usr/bin/lr'


def test_open_external_editor_index_picks_specific_editor(app_and_db, monkeypatch):
    """editor_index in the request body picks that editor from the list."""
    app, _ = app_and_db
    client = app.test_client()

    client.post('/api/config',
                data=json.dumps({
                    "external_editors": [
                        {"name": "First", "path": "/usr/bin/first"},
                        {"name": "Second", "path": "/usr/bin/second"},
                        {"name": "Third", "path": "/usr/bin/third"},
                    ],
                }),
                content_type='application/json')

    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1], "editor_index": 2}),
                       content_type='application/json')
    assert resp.status_code == 200
    assert launched[0][1][0] == '/usr/bin/third'


def test_open_external_editor_index_out_of_range(app_and_db):
    """editor_index >= len(editors) returns 400 with a useful message."""
    app, _ = app_and_db
    client = app.test_client()

    client.post('/api/config',
                data=json.dumps({
                    "external_editors": [{"name": "Only", "path": "/usr/bin/only"}],
                }),
                content_type='application/json')

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1], "editor_index": 5}),
                       content_type='application/json')
    assert resp.status_code == 400
    assert "out of range" in resp.get_json()["error"]


def test_open_external_editor_index_with_no_editors(app_and_db):
    """editor_index passed with no editors configured returns 400, not OS default."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1], "editor_index": 0}),
                       content_type='application/json')
    assert resp.status_code == 400
    assert "No external editors configured" in resp.get_json()["error"]


def test_open_external_legacy_editor_synthesizes_when_list_empty(app_and_db, monkeypatch):
    """Empty external_editors + legacy external_editor still works (back-compat)."""
    app, _ = app_and_db
    client = app.test_client()

    client.post('/api/config',
                data=json.dumps({"external_editor": "/usr/bin/legacy"}),
                content_type='application/json')

    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200
    assert launched[0][1][0] == '/usr/bin/legacy'


def test_clearing_editor_list_does_not_resurrect_legacy_field(app_and_db, monkeypatch):
    """Saving an empty external_editors list must let the user reach OS default.

    Without this, a user who had `external_editor` set in the old single-string
    config, and then removes every editor from the new list in Settings, would
    keep getting the legacy value back via cfg.get_editors() — the new list is
    empty, so it synthesizes from the legacy field. The settings UI guards
    against this by clearing `external_editor` whenever it saves the new list.
    """
    app, _ = app_and_db
    client = app.test_client()

    # Pre-existing legacy value (simulates a config that pre-dates this PR).
    import config as cfg
    cfg.set("external_editor", "/usr/bin/legacy")

    # The settings UI's saveConfig posts both keys together — empty list and
    # empty legacy field — to complete the migration.
    client.post('/api/config',
                data=json.dumps({
                    "external_editors": [],
                    "external_editor": "",
                }),
                content_type='application/json')

    monkeypatch.setattr(sys, 'platform', 'darwin')
    launched = _patch_launchers(monkeypatch)

    resp = client.post('/api/photos/open-external',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 200
    # Should be the OS default opener (`open <file>`), not the legacy editor.
    assert launched[0][1][0] == 'open'
    assert '-a' not in launched[0][1]
    assert '/usr/bin/legacy' not in launched[0][1]


def test_get_editors_filters_malformed_entries(app_and_db):
    """cfg.get_editors() drops dicts missing a path or with non-string fields.

    The same filtering shape is mirrored in _navbar.html's getExternalEditors()
    so a hand-edited config.json (or any /api/config writer that doesn't
    validate the list shape) can't trip path.replace() and leave the JS
    cache as a permanently-rejected promise.
    """
    import config as cfg
    cfg.set("external_editors", [
        {"name": "Good", "path": "/usr/bin/good"},
        {"name": "NoPath"},
        {"path": ""},
        "not even a dict",
        {"name": "NumPath", "path": 12345},      # non-string path → drop
        {"name": 999, "path": "/usr/bin/numname"},  # non-string name → use basename
        {"name": "", "path": "/Applications/Foo.app"},
    ])
    editors = cfg.get_editors()
    assert [e["path"] for e in editors] == [
        "/usr/bin/good",
        "/usr/bin/numname",
        "/Applications/Foo.app",
    ]
    assert editors[0]["name"] == "Good"
    # Non-string name falls back to the basename of the path.
    assert editors[1]["name"] == "numname"
    # Empty name falls back to the basename of the path.
    assert editors[2]["name"] == "Foo.app"


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
    assert os.path.normpath(launched[0][1][2]) == os.path.normpath(str(bundle))
