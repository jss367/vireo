import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import Database


@pytest.fixture
def db(tmp_path):
    d = Database(str(tmp_path / "test.db"))
    ws_id = d.ensure_default_workspace()
    d.set_active_workspace(ws_id)
    return d


def test_inat_submissions_table_exists(db):
    """The inat_submissions table should exist after DB init."""
    row = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='inat_submissions'"
    ).fetchone()
    assert row is not None


def test_record_inat_submission(db):
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='bird.jpg', extension='.jpg',
                       file_size=1000, file_mtime=1.0, timestamp='2024-06-01T10:00:00')
    db.record_inat_submission(pid, 123456, "https://www.inaturalist.org/observations/123456")
    subs = db.get_inat_submissions([pid])
    assert len(subs) == 1
    assert subs[pid]['observation_id'] == 123456
    assert subs[pid]['observation_url'] == "https://www.inaturalist.org/observations/123456"


def test_get_inat_submissions_empty(db):
    subs = db.get_inat_submissions([999])
    assert subs == {}


def test_inat_submission_cascades_on_photo_delete(db):
    fid = db.add_folder('/photos', name='photos')
    pid = db.add_photo(folder_id=fid, filename='bird.jpg', extension='.jpg',
                       file_size=1000, file_mtime=1.0, timestamp='2024-06-01T10:00:00')
    db.record_inat_submission(pid, 111, "https://www.inaturalist.org/observations/111")
    db.conn.execute("DELETE FROM photos WHERE id = ?", (pid,))
    db.conn.commit()
    row = db.conn.execute("SELECT * FROM inat_submissions WHERE photo_id = ?", (pid,)).fetchone()
    assert row is None


from unittest.mock import MagicMock, patch


def test_validate_token_success():
    from inat import validate_token
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"login": "birder42"}
    with patch("inat.requests.get", return_value=mock_resp) as mock_get:
        result = validate_token("fake-token")
        assert result == {"login": "birder42"}
        mock_get.assert_called_once()


def test_validate_token_invalid():
    from inat import validate_token
    mock_resp = MagicMock()
    mock_resp.status_code = 401
    with patch("inat.requests.get", return_value=mock_resp):
        result = validate_token("bad-token")
        assert result is None


def test_create_observation_success():
    from inat import create_observation
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = [{"id": 99999, "uri": "https://www.inaturalist.org/observations/99999"}]
    with patch("inat.requests.post", return_value=mock_resp) as mock_post:
        obs = create_observation(
            token="fake-token",
            taxon_name="Cardinalis cardinalis",
            observed_on="2024-06-01",
            latitude=38.9,
            longitude=-77.0,
            description="Test obs",
            geoprivacy="open",
        )
        assert obs["id"] == 99999
        mock_post.assert_called_once()


def test_create_observation_auth_error():
    from inat import InatAuthError, create_observation
    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_resp.text = "Unauthorized"
    with patch("inat.requests.post", return_value=mock_resp), pytest.raises(InatAuthError):
        create_observation(token="bad", taxon_name="Test")


def test_upload_photo_success():
    from inat import upload_photo
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"id": 55555}
    with patch("inat.requests.post", return_value=mock_resp), patch("builtins.open", MagicMock()):
        result = upload_photo("fake-token", 99999, "/path/to/photo.jpg")
        assert result["id"] == 55555


def test_submit_observation_success():
    from inat import submit_observation
    with patch("inat.create_observation", return_value={"id": 88888, "uri": "https://www.inaturalist.org/observations/88888"}) as mock_create:
        with patch("inat.upload_photo", return_value={"id": 1}) as mock_upload:
            obs_id, obs_url = submit_observation(
                token="fake-token",
                photo_path="/path/to/photo.jpg",
                taxon_name="Cardinalis cardinalis",
            )
            assert obs_id == 88888
            assert obs_url == "https://www.inaturalist.org/observations/88888"
            mock_create.assert_called_once()
            mock_upload.assert_called_once_with("fake-token", 88888, "/path/to/photo.jpg")


from PIL import Image


@pytest.fixture
def app_and_db(tmp_path):
    """Create a test app with sample data for iNat tests."""
    import config as cfg
    from app import create_app

    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    d = Database(db_path)
    ws_id = d.ensure_default_workspace()
    d.set_active_workspace(ws_id)

    # Create a real photo file on disk so submit can read it
    photo_dir = str(tmp_path / "photos")
    os.makedirs(photo_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(photo_dir, 'bird.jpg'))

    fid = d.add_folder(photo_dir, name='photos')
    pid = d.add_photo(folder_id=fid, filename='bird.jpg', extension='.jpg',
                      file_size=1000, file_mtime=1.0, timestamp='2024-06-01T10:00:00')

    # Add a detection and prediction for the photo
    det_ids = d.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    d.add_prediction(det_ids[0], "Northern Cardinal", 0.95, "test-model",
                     taxonomy={"scientific_name": "Cardinalis cardinalis"})

    for p in [pid]:
        Image.new('RGB', (100, 100)).save(os.path.join(thumb_dir, f"{p}.jpg"))

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, d, pid


def test_api_inat_prepare_includes_submission_status(app_and_db):
    app, db, pid = app_and_db
    client = app.test_client()
    # Before submission
    resp = client.get(f'/api/inat/prepare/{pid}')
    data = resp.get_json()
    assert data['already_submitted'] is False

    # After submission
    db.record_inat_submission(pid, 999, "https://www.inaturalist.org/observations/999")
    resp = client.get(f'/api/inat/prepare/{pid}')
    data = resp.get_json()
    assert data['already_submitted'] is True
    assert data['existing_observation_url'] == "https://www.inaturalist.org/observations/999"


def test_api_inat_prepare_uses_assigned_location_coords(app_and_db):
    app, db, pid = app_and_db
    kid = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) "
        "VALUES (?, 'location', ?, ?)",
        ("Paris Airbnb", 48.8566, 2.3522),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, kid),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f'/api/inat/prepare/{pid}')
    assert resp.status_code == 200
    data = resp.get_json()

    assert data['latitude'] == 48.8566
    assert data['longitude'] == 2.3522
    assert "lat=48.8566" in data["upload_url"]
    assert "lng=2.3522" in data["upload_url"]


def test_api_inat_prepare_url_encodes_query_params(app_and_db):
    """The prefilled upload URL must be valid: a scientific name with a space
    (e.g. "Cardinalis cardinalis") has to be percent-encoded, otherwise the
    desktop opener plugin rejects the malformed URL and nothing opens."""
    from urllib.parse import parse_qs, urlparse

    app, _db, pid = app_and_db
    client = app.test_client()
    resp = client.get(f'/api/inat/prepare/{pid}')
    assert resp.status_code == 200
    data = resp.get_json()

    url = data["upload_url"]
    # No raw spaces anywhere in the URL.
    assert " " not in url
    # The taxon name still round-trips back to the real binomial.
    qs = parse_qs(urlparse(url).query)
    assert qs["taxon_name"] == ["Cardinalis cardinalis"]


def test_api_inat_prepare_marks_quick_mode_as_not_photo_upload(app_and_db):
    app, _db, pid = app_and_db
    client = app.test_client()
    resp = client.get(f'/api/inat/prepare/{pid}')
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["mode"] == "quick"
    assert data["direct_upload_enabled"] is False
    assert data["photo_upload_requires_token"] is True


def test_api_inat_prepare_marks_direct_mode_with_token(app_and_db):
    app, _db, pid = app_and_db
    import config as cfg
    cfg.save({"inat_token": "fake-token"})

    client = app.test_client()
    resp = client.get(f'/api/inat/prepare/{pid}')
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["mode"] == "direct"
    assert data["direct_upload_enabled"] is True
    assert data["photo_upload_requires_token"] is False


def _add_out_of_workspace_photo(db):
    active_ws = db._active_workspace_id
    base_dir = db.conn.execute("SELECT path FROM folders LIMIT 1").fetchone()["path"]
    other_dir = os.path.join(os.path.dirname(base_dir), "other-photos")
    os.makedirs(other_dir, exist_ok=True)
    Image.new('RGB', (100, 100)).save(os.path.join(other_dir, 'outside.jpg'))

    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    fid = db.add_folder(other_dir, name="other-photos")
    pid = db.add_photo(
        folder_id=fid,
        filename='outside.jpg',
        extension='.jpg',
        file_size=1000,
        file_mtime=1.0,
        timestamp='2024-06-02T10:00:00',
    )
    db.set_active_workspace(active_ws)
    return pid


def test_api_inat_prepare_rejects_out_of_workspace_photo(app_and_db):
    app, db, _ = app_and_db
    pid = _add_out_of_workspace_photo(db)

    client = app.test_client()
    resp = client.get(f'/api/inat/prepare/{pid}')

    assert resp.status_code == 403
    assert "active workspace" in resp.get_json()["error"]


def test_api_inat_submit_no_token(app_and_db):
    app, db, pid = app_and_db
    client = app.test_client()
    resp = client.post('/api/inat/submit', json={'photo_id': pid})
    assert resp.status_code == 400
    assert 'token' in resp.get_json()['error'].lower()


def test_api_inat_submit_success(app_and_db):
    app, db, pid = app_and_db
    import config as cfg
    cfg.save({"inat_token": "fake-token"})

    client = app.test_client()
    with patch("inat.submit_observation", return_value=(12345, "https://www.inaturalist.org/observations/12345")):
        resp = client.post('/api/inat/submit', json={'photo_id': pid})
    data = resp.get_json()
    assert data['observation_id'] == 12345
    assert data['observation_url'] == "https://www.inaturalist.org/observations/12345"

    # Verify recorded in DB
    subs = db.get_inat_submissions([pid])
    assert pid in subs


def test_api_inat_submit_uses_edited_render(app_and_db):
    app, db, pid = app_and_db
    import config as cfg
    cfg.save({"inat_token": "fake-token"})

    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id = p.folder_id WHERE p.id = ?",
        (pid,),
    ).fetchone()
    original_path = os.path.join(folder["path"], "bird.jpg")
    Image.new("RGB", (80, 40), (220, 30, 20)).save(original_path)
    db.set_photo_edit_recipe(pid, {"rotation": 90})

    client = app.test_client()
    with patch(
        "inat.submit_observation",
        return_value=(12345, "https://www.inaturalist.org/observations/12345"),
    ) as mock_submit:
        resp = client.post("/api/inat/submit", json={"photo_id": pid})

    assert resp.status_code == 200
    upload_path = mock_submit.call_args.kwargs["photo_path"]
    assert upload_path != original_path
    assert os.path.basename(os.path.dirname(upload_path)) == "inat-uploads"
    assert os.path.isfile(upload_path)
    with Image.open(upload_path) as img:
        assert img.size == (40, 80)


def test_api_inat_submit_uses_working_copy_when_raw_source_missing(app_and_db):
    app, db, pid = app_and_db
    import config as cfg
    cfg.save({"inat_token": "fake-token"})

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_rel = f"working/{pid}.jpg"
    Image.new("RGB", (80, 40), (220, 30, 20)).save(
        os.path.join(vireo_dir, wc_rel), "JPEG",
    )
    db.conn.execute(
        """UPDATE photos
           SET filename='offline.NEF', extension='.nef',
               working_copy_path=?,
               width=80, height=40
           WHERE id=?""",
        (wc_rel, pid),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(pid, {"rotation": 90})

    client = app.test_client()
    with patch(
        "inat.submit_observation",
        return_value=(12345, "https://www.inaturalist.org/observations/12345"),
    ) as mock_submit:
        resp = client.post("/api/inat/submit", json={"photo_id": pid})

    assert resp.status_code == 200
    upload_path = mock_submit.call_args.kwargs["photo_path"]
    assert os.path.basename(os.path.dirname(upload_path)) == "inat-uploads"
    with Image.open(upload_path) as img:
        assert img.size == (40, 80)


def test_api_inat_submit_uses_companion_when_raw_source_missing(app_and_db):
    app, db, pid = app_and_db
    import config as cfg
    cfg.save({"inat_token": "fake-token"})

    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id = p.folder_id WHERE p.id = ?",
        (pid,),
    ).fetchone()
    companion_path = os.path.join(folder["path"], "bird.jpg")
    Image.new("RGB", (80, 40), (220, 30, 20)).save(companion_path)
    db.conn.execute(
        """UPDATE photos
           SET filename='offline.NEF', extension='.nef',
               working_copy_path=NULL,
               companion_path='bird.jpg',
               width=80, height=40
           WHERE id=?""",
        (pid,),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(pid, {"rotation": 90})

    client = app.test_client()
    with patch(
        "inat.submit_observation",
        return_value=(12345, "https://www.inaturalist.org/observations/12345"),
    ) as mock_submit:
        resp = client.post("/api/inat/submit", json={"photo_id": pid})

    assert resp.status_code == 200, resp.get_json()
    upload_path = mock_submit.call_args.kwargs["photo_path"]
    assert os.path.basename(os.path.dirname(upload_path)) == "inat-uploads"
    with Image.open(upload_path) as img:
        assert img.size == (40, 80)


def test_inat_upload_handoff_meta_includes_math_version(app_and_db):
    """The iNat upload render reuses inat-uploads/<id>.jpg only when its
    cached metadata matches. That metadata must carry EDIT_MATH_VERSION so a
    math bump (which changes per-pixel output but not recipe/source/mtime)
    invalidates the stale render — otherwise unchanged recipes would keep
    submitting the old hard-clipped JPEG to iNaturalist after a deploy."""
    import json as _json

    from image_edits import EDIT_MATH_VERSION

    app, db, pid = app_and_db
    import config as cfg
    cfg.save({"inat_token": "fake-token"})

    folder = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id = p.folder_id WHERE p.id = ?",
        (pid,),
    ).fetchone()
    Image.new("RGB", (80, 40), (220, 30, 20)).save(
        os.path.join(folder["path"], "bird.jpg"),
    )
    db.set_photo_edit_recipe(pid, {"adjustments": {"exposure": 0.5}})

    client = app.test_client()
    with patch(
        "inat.submit_observation",
        return_value=(12345, "https://www.inaturalist.org/observations/12345"),
    ):
        resp = client.post("/api/inat/submit", json={"photo_id": pid})
    assert resp.status_code == 200

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    meta_path = os.path.join(vireo_dir, "inat-uploads", f"{pid}.json")
    assert os.path.exists(meta_path)
    with open(meta_path, encoding="utf-8") as f:
        meta = _json.load(f)
    assert meta.get("edit_math_version") == EDIT_MATH_VERSION


def test_api_inat_submit_uses_assigned_location_coords(app_and_db):
    app, db, pid = app_and_db
    import config as cfg
    cfg.save({"inat_token": "fake-token"})

    kid = db.conn.execute(
        "INSERT INTO keywords (name, type, latitude, longitude) "
        "VALUES (?, 'location', ?, ?)",
        ("Paris Airbnb", 48.8566, 2.3522),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, kid),
    )
    db.conn.commit()

    client = app.test_client()
    with patch("inat.submit_observation", return_value=(12345, "https://www.inaturalist.org/observations/12345")) as mock_submit:
        resp = client.post('/api/inat/submit', json={'photo_id': pid})

    assert resp.status_code == 200
    kwargs = mock_submit.call_args.kwargs
    assert kwargs["latitude"] == 48.8566
    assert kwargs["longitude"] == 2.3522


def test_api_inat_submit_rejects_out_of_workspace_photo(app_and_db):
    app, db, _ = app_and_db
    import config as cfg
    cfg.save({"inat_token": "fake-token"})
    pid = _add_out_of_workspace_photo(db)

    client = app.test_client()
    with patch("inat.submit_observation") as mock_submit:
        resp = client.post('/api/inat/submit', json={'photo_id': pid})

    assert resp.status_code == 403
    assert "active workspace" in resp.get_json()["error"]
    mock_submit.assert_not_called()


def test_api_inat_submit_batch(app_and_db):
    app, db, pid = app_and_db
    import config as cfg
    cfg.save({"inat_token": "fake-token"})

    client = app.test_client()
    with patch("inat.submit_observation", return_value=(11111, "https://www.inaturalist.org/observations/11111")):
        resp = client.post('/api/inat/submit-batch', json={
            'submissions': [{'photo_id': pid}]
        })
    data = resp.get_json()
    assert len(data['results']) == 1
    assert data['results'][0]['observation_id'] == 11111


def test_api_inat_submit_batch_rejects_out_of_workspace_photo(app_and_db):
    app, db, _ = app_and_db
    import config as cfg
    cfg.save({"inat_token": "fake-token"})
    pid = _add_out_of_workspace_photo(db)

    client = app.test_client()
    with patch("inat.submit_observation") as mock_submit:
        resp = client.post('/api/inat/submit-batch', json={
            'submissions': [{'photo_id': pid}]
        })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data['results'] == [{
        'photo_id': pid,
        'error': f"Photo {pid} does not belong to the active workspace",
    }]
    mock_submit.assert_not_called()


def test_api_inat_submissions_lookup(app_and_db):
    app, db, pid = app_and_db
    db.record_inat_submission(pid, 777, "https://www.inaturalist.org/observations/777")
    client = app.test_client()
    resp = client.get(f'/api/inat/submissions?photo_ids={pid}')
    data = resp.get_json()
    assert str(pid) in data or pid in data


def test_api_inat_validate_token(app_and_db):
    app, db, pid = app_and_db
    client = app.test_client()
    with patch("inat.validate_token", return_value={"login": "birder42"}):
        resp = client.post('/api/inat/validate-token', json={'token': 'fake'})
    data = resp.get_json()
    assert data['login'] == 'birder42'
