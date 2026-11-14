"""The edit preview reports which file supplied its pixels.

The Photo Editor's footer reads this to tell the user whether an edit runs
on RAW sensor data or on a JPEG stand-in, so every case asserts the origin
of the pixels that were decoded, not the file that was selected first.
"""

import os

import numpy as np
import pytest
from PIL import Image

SESSION = "c" * 32


def _preview_and_receipt(app, photo_id):
    client = app.test_client()
    query = {"size": 512, "preview_session": SESSION, "preview_seq": "1"}
    preview = client.get(f"/photos/{photo_id}/edit-preview", query_string=query)
    assert preview.status_code == 200, preview.get_data(as_text=True)
    receipt = client.get(
        f"/photos/{photo_id}/edit-preview",
        query_string={**query, "source_status": "1"},
    )
    return preview, receipt.get_json()


def _folder_path(db, photo_id):
    return db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?",
        (photo_id,),
    ).fetchone()["path"]


def _make_raw_primary(db, photo_id, *, companion=None):
    """Turn the fixture's photo into ``shot.NEF`` (800x600) with a saved edit."""
    folder = _folder_path(db, photo_id)
    raw_path = os.path.join(folder, "shot.NEF")
    with open(raw_path, "wb") as handle:
        handle.write(b"not decodable without the patched loader")
    if companion:
        Image.new("RGB", (800, 600), (40, 90, 180)).save(
            os.path.join(folder, companion), "JPEG",
        )
    db.conn.execute(
        """UPDATE photos
           SET filename='shot.NEF', extension='.nef', companion_path=?,
               working_copy_path=NULL, width=800, height=600
           WHERE id=?""",
        (companion, photo_id),
    )
    db.conn.commit()
    db.set_photo_edit_recipe(photo_id, {"adjustments": {"exposure": 0.5}})
    db.commit()
    return raw_path


def _patch_raw_decode(monkeypatch, result):
    """Make every ``.nef`` decode return ``result()``; other files decode normally."""
    import image_loader

    real_load_image = image_loader.load_image

    def load_image(path, *args, **kwargs):
        if str(path).lower().endswith(".nef"):
            return result()
        return real_load_image(path, *args, **kwargs)

    monkeypatch.setattr(image_loader, "load_image", load_image)


def test_non_raw_original_reports_original(client_with_photo):
    app, _db, photo_id = client_with_photo

    preview, receipt = _preview_and_receipt(app, photo_id)

    assert preview.headers["X-Vireo-Preview-Origin"] == "original"
    assert preview.headers["X-Vireo-Preview-Source"] == "srgb"
    assert receipt == {"source": "srgb", "origin": "original", "original_is_raw": False}


def test_decoded_sensor_data_reports_raw(client_with_photo, monkeypatch):
    from float_image import FloatImage

    app, db, photo_id = client_with_photo
    _make_raw_primary(db, photo_id)
    _patch_raw_decode(
        monkeypatch, lambda: FloatImage(np.full((600, 800, 3), 0.2, dtype=np.float32)),
    )

    preview, receipt = _preview_and_receipt(app, photo_id)

    assert preview.headers["X-Vireo-Preview-Origin"] == "raw"
    assert preview.headers["X-Vireo-Preview-Source"] == "linear"
    assert receipt == {"source": "linear", "origin": "raw", "original_is_raw": True}


def test_undecodable_raw_reports_its_embedded_jpeg(client_with_photo, monkeypatch):
    """A RAW libraw cannot demosaic still loads, as the camera's embedded JPEG."""
    app, db, photo_id = client_with_photo
    _make_raw_primary(db, photo_id)
    _patch_raw_decode(monkeypatch, lambda: Image.new("RGB", (800, 600), (90, 90, 90)))

    preview, receipt = _preview_and_receipt(app, photo_id)

    assert preview.headers["X-Vireo-Preview-Origin"] == "embedded"
    assert receipt == {"source": "srgb", "origin": "embedded", "original_is_raw": True}


def test_companion_rescue_reports_companion(client_with_photo, monkeypatch):
    app, db, photo_id = client_with_photo
    _make_raw_primary(db, photo_id, companion="shot.jpg")
    _patch_raw_decode(monkeypatch, lambda: None)

    preview, receipt = _preview_and_receipt(app, photo_id)

    assert preview.headers["X-Vireo-Preview-Origin"] == "companion"
    assert receipt == {"source": "srgb", "origin": "companion", "original_is_raw": True}


def test_offline_raw_reports_working_copy(client_with_photo):
    from app_paths import media_root

    app, db, photo_id = client_with_photo
    raw_path = _make_raw_primary(db, photo_id)
    os.unlink(raw_path)
    working_dir = os.path.join(media_root(app.config["THUMB_CACHE_DIR"]), "working")
    os.makedirs(working_dir, exist_ok=True)
    Image.new("RGB", (800, 600), (180, 0, 0)).save(
        os.path.join(working_dir, f"{photo_id}.jpg"), "JPEG",
    )
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{photo_id}.jpg", photo_id),
    )
    db.conn.commit()

    preview, receipt = _preview_and_receipt(app, photo_id)

    assert preview.headers["X-Vireo-Preview-Origin"] == "working_copy"
    assert receipt == {"source": "srgb", "origin": "working_copy", "original_is_raw": True}


def test_expired_receipt_reports_no_origin(client_with_photo):
    app, _db, photo_id = client_with_photo

    receipt = app.test_client().get(
        f"/photos/{photo_id}/edit-preview",
        query_string={"preview_session": SESSION, "preview_seq": "9", "source_status": "1"},
    ).get_json()

    assert receipt == {"source": None, "origin": None, "original_is_raw": False}


@pytest.mark.parametrize("origin", ["embedded", "companion", "working_copy", "original"])
def test_only_raw_origin_is_linear(origin):
    from web.media_editing import _source_encoding

    assert _source_encoding("raw") == "linear"
    assert _source_encoding(origin) == "srgb"
