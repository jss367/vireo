import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
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


from unittest.mock import patch, MagicMock


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
    from inat import create_observation, InatAuthError
    mock_resp = MagicMock()
    mock_resp.status_code = 401
    mock_resp.text = "Unauthorized"
    with patch("inat.requests.post", return_value=mock_resp):
        with pytest.raises(InatAuthError):
            create_observation(token="bad", taxon_name="Test")


def test_upload_photo_success():
    from inat import upload_photo
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"id": 55555}
    with patch("inat.requests.post", return_value=mock_resp):
        with patch("builtins.open", MagicMock()):
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
