"""Behavior pins for export's catalog-wide camera-metadata reads.

``export.py`` reads each selected photo's raw ``exif_data`` and promoted
camera columns through ``db.photo_metadata``. The reads are catalog-wide, so
they must keep working with no active workspace; the structural tests at the
end keep the accessor a fresh repository built without the connection.
"""

import ast
import inspect
import textwrap

import pytest
from db import Database
from export import _get_photo_camera_data, _get_photo_exif_data
from repositories.photo_metadata import PhotoMetadataRepository
from services import photo_ingest

CAMERA = {
    "camera_make": "Canon", "camera_model": "R5", "lens": "RF 100-500",
    "focal_length": 500, "aperture": 7.1, "shutter_speed": "1/2000", "iso": 1600,
}


@pytest.fixture
def photos(db, tmp_path):
    folder = db.add_folder(str(tmp_path), name="photos")
    ids = [
        photo_ingest.add_photo(db, folder_id=folder, filename=name, extension=".jpg",
                     file_size=100, file_mtime=1.0)
        for name in ("a.jpg", "b.jpg")
    ]
    db.conn.execute(
        "UPDATE photos SET exif_data = ?, camera_make = ?, camera_model = ?, lens = ?, "
        "focal_length = ?, aperture = ?, shutter_speed = ?, iso = ? WHERE id = ?",
        ('{"Make": "Canon"}', *CAMERA.values(), ids[0]),
    )
    db.conn.commit()
    return ids


def test_exif_and_camera_data_by_ids(db, photos):
    pid, bare = photos

    assert db.photo_metadata.exif_data_by_ids([pid, bare, 999999]) == {
        pid: '{"Make": "Canon"}', bare: None,
    }
    assert db.photo_metadata.camera_data_by_ids([pid]) == {pid: {"id": pid, **CAMERA}}
    assert db.photo_metadata.exif_data_by_ids([]) == {}
    assert db.photo_metadata.camera_data_by_ids([]) == {}


def test_reads_chunk_at_999_ids(db, photos):
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        ids = [photos[0], *range(10**6, 10**6 + 999)]
        exif = db.photo_metadata.exif_data_by_ids(ids)
        camera = db.photo_metadata.camera_data_by_ids(ids)
    finally:
        db.conn.set_trace_callback(None)
    assert list(exif) == list(camera) == [photos[0]]
    assert len([s for s in statements if "SELECT id, exif_data FROM photos" in s]) == 2
    assert len([s for s in statements if "SELECT id, camera_make" in s]) == 2


def test_reads_need_no_active_workspace(db, photos):
    """Refactor guard: export's reads never resolved a workspace and must not start."""
    pid = photos[0]
    db.set_active_workspace(None)

    assert db.photo_metadata.exif_data_by_ids([pid]) == {pid: '{"Make": "Canon"}'}
    assert db.photo_metadata.camera_data_by_ids([pid]) == {pid: {"id": pid, **CAMERA}}
    assert _get_photo_exif_data(db, [pid]) == {pid: '{"Make": "Canon"}'}
    assert _get_photo_camera_data(db, [pid]) == {pid: {"id": pid, **CAMERA}}


def test_export_helpers_return_empty_without_a_connection(db, photos):
    """Closed databases (and fakes without ``conn``) read nothing."""
    assert _get_photo_exif_data(object(), photos) == {}
    assert _get_photo_camera_data(object(), photos) == {}
    assert _get_photo_exif_data(db, []) == {}
    db.close()
    assert _get_photo_exif_data(db, photos) == {}
    assert _get_photo_camera_data(db, photos) == {}


# -- structure ------------------------------------------------------------------


def test_photo_metadata_is_a_fresh_repository_on_the_connection_per_access(db):
    first, second = db.photo_metadata, db.photo_metadata
    assert isinstance(first, PhotoMetadataRepository)
    assert first is not second
    assert first.conn is db.conn


def test_photo_metadata_accessor_builds_through_the_factory_not_the_connection():
    accessor = Database.__dict__["photo_metadata"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_photo_metadata_repository" in attrs
    assert "conn" not in attrs
