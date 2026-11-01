"""The cull job names the photos it could not fingerprint for scene grouping.

The job reported "N photos could not be fingerprinted for scene grouping and
were grouped on their own" without saying which photos, and logged why only at
debug level. Each such photo now appears in the result with its path and the
reason, the Jobs page lists them (capped, then "and N more"), and the reason
is logged at warning level with the exception.
"""

import logging
import os

from services import workspace_folders
from test_culling import _setup_culling_db

DECODER_ERROR = "decoder exploded"


def _corrupt(path):
    with open(path, "wb") as f:
        f.write(b"not an image")


def test_names_a_photo_whose_image_could_not_be_opened(tmp_path, caplog):
    from culling import analyze_for_culling

    db, photo_ids = _setup_culling_db(tmp_path)
    bad = tmp_path / "photos" / "bird3.jpg"
    _corrupt(bad)

    with caplog.at_level(logging.WARNING, logger="culling"):
        result = analyze_for_culling(db)

    assert result["photos_missing_phash"] == 1
    assert result["unfingerprinted_photos"] == [{
        "photo_id": photo_ids[3],
        "path": str(bad),
        "reason": "Vireo could not open the image",
    }]
    [record] = [r for r in caplog.records if "pHash" in r.getMessage()]
    assert record.levelno == logging.WARNING
    assert str(bad) in record.getMessage()


def test_names_a_photo_whose_hash_raised_and_logs_the_exception(tmp_path, monkeypatch, caplog):
    import culling
    from culling import analyze_for_culling

    db, photo_ids = _setup_culling_db(tmp_path)
    real_load = culling.load_image

    def load(path, *args, **kwargs):
        if path.endswith("bird2.jpg"):
            raise OSError(DECODER_ERROR)
        return real_load(path, *args, **kwargs)

    monkeypatch.setattr(culling, "load_image", load)

    with caplog.at_level(logging.WARNING, logger="culling"):
        result = analyze_for_culling(db)

    path = os.path.join(str(tmp_path / "photos"), "bird2.jpg")
    assert result["unfingerprinted_photos"] == [
        {"photo_id": photo_ids[2], "path": path, "reason": DECODER_ERROR},
    ]
    [record] = [r for r in caplog.records if "pHash" in r.getMessage()]
    assert record.levelno == logging.WARNING
    assert record.exc_info and str(record.exc_info[1]) == DECODER_ERROR
    assert path in record.getMessage()


def test_every_photo_fingerprinted_names_nothing(tmp_path):
    from culling import analyze_for_culling

    db, _photo_ids = _setup_culling_db(tmp_path)

    result = analyze_for_culling(db)

    assert result["photos_missing_phash"] == 0
    assert result["unfingerprinted_photos"] == []


def test_the_list_is_capped(tmp_path, monkeypatch):
    import culling
    from culling import analyze_for_culling

    monkeypatch.setattr(culling, "UNFINGERPRINTED_PHOTOS_LISTED", 2)
    db, _photo_ids = _setup_culling_db(tmp_path)
    for i in range(4):
        _corrupt(tmp_path / "photos" / f"bird{i}.jpg")

    result = analyze_for_culling(db)

    assert result["photos_missing_phash"] == 4
    assert [os.path.basename(p["path"]) for p in result["unfingerprinted_photos"]] == [
        "bird0.jpg", "bird1.jpg",
    ]


def test_jobs_page_lists_the_photos_then_and_n_more():
    from job_summaries import describe_result

    out = describe_result("cull", {
        "total_photos": 10, "suggested_keepers": 0, "suggested_rejects": 0,
        "photos_missing_phash": 3,
        "unfingerprinted_photos": [
            {"photo_id": 1, "path": "/p/a.NEF", "reason": "Vireo could not open the image"},
            {"photo_id": 2, "path": "/p/b.NEF", "reason": DECODER_ERROR},
        ],
    })

    assert out["details"] == [
        "3 photos could not be fingerprinted for scene grouping and were grouped on their own:",
        "/p/a.NEF: Vireo could not open the image",
        f"/p/b.NEF: {DECODER_ERROR}",
        "and 1 more",
    ]
    one = describe_result("cull", {
        "total_photos": 1, "photos_missing_phash": 1,
        "unfingerprinted_photos": [{"photo_id": 1, "path": "/p/a.NEF", "reason": DECODER_ERROR}],
    })
    assert one["details"] == [
        "1 photo could not be fingerprinted for scene grouping and was grouped on its own:",
        f"/p/a.NEF: {DECODER_ERROR}",
    ]


def test_cull_job_result_names_the_photo(app_and_db, tmp_path):
    from db import Database
    from PIL import Image
    from services import photo_ingest
    from wait import wait_for_job_via_client

    app, _db = app_and_db
    db = Database(app.config["DB_PATH"])
    folder = tmp_path / "cullphotos"
    folder.mkdir()
    Image.new("RGB", (64, 64), "blue").save(folder / "good.jpg")
    _corrupt(folder / "bad.jpg")
    fid = workspace_folders.add_folder(db, str(folder), name="cullphotos")
    for name in ("good.jpg", "bad.jpg"):
        pid = photo_ingest.add_photo(db, fid, name, ".jpg", 10, 1.0)
        det_ids = db.detections.save(pid, [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
        ], detector_model="MDV6")
        db.predictions.add(det_ids[0], "Robin", 0.95, "test-model")
        db.commit()
    db.conn.commit()
    client = app.test_client()

    job_id = client.post("/api/jobs/cull", json={}).get_json()["job_id"]
    job = wait_for_job_via_client(client, job_id, wait_for_history=True)

    result = job["result"]
    assert result["photos_missing_phash"] >= 1
    bad = [p for p in result["unfingerprinted_photos"] if p["path"].endswith("bad.jpg")]
    assert bad and bad[0]["reason"] == "Vireo could not open the image"
