"""Audit excursion detection, reviewed GPS precedence and restoring original evidence."""

import json
from datetime import datetime, timedelta

from services import photo_ingest, workspace_folders


def test_review_gps_excursion(app_and_db):
    app, db = app_and_db
    folder = workspace_folders.add_folder(db, "/gps/outing", name="outing")
    ids = []
    for idx, (sec, lat) in enumerate(
        [(0, 32), (100, 32.03), (101, 32.03), (106, 32), (120, 33), (130, 33), (1800, 32)]
    ):
        captured = datetime(2026, 1, 1) + timedelta(seconds=sec)
        pid = photo_ingest.add_photo(
            db,
            folder_id=folder,
            filename=f"DSC_{1000 + idx}.NEF",
            extension=".nef",
            file_size=5,
            file_mtime=0,
            timestamp=captured.isoformat(),
        )
        exif = {
            "EXIF": {"DateTimeOriginal": captured.strftime("%Y:%m:%d %H:%M:%S"), "Model": "Camera"},
            "Composite": {"GPSLatitude": lat, "GPSLongitude": -117},
        }
        db.conn.execute(
            "UPDATE photos SET exif_data=?, latitude=?, longitude=-117 WHERE id=?", (json.dumps(exif), lat, pid)
        )
        ids.append(pid)
    db.commit()
    client = app.test_client()
    url = "/api/audit/gps-jumps"
    finding, jump = client.get(url).get_json()
    assert jump["kind"] == "jump" and jump["before"] == "DSC_1003.NEF"
    assert finding["ids"] == ids[1:3]
    assert finding["return_seconds"] == 5
    assert client.post(url, json={"folder_id": folder, "key": "stale", "action": "apply"}).status_code == 409
    for _ in range(2):
        assert client.post(url, json={"folder_id": folder, "key": finding["key"], "action": "apply"}).status_code == 200
    assert client.get(url).get_json()[0]["reviewed"]
    reviewed = db.locations.get_effective(ids)
    assert reviewed[ids[1]]["source"] == "reviewed_gps"
    assert reviewed[ids[2]]["latitude"] == 32
    assert reviewed[ids[0]]["source"] == "exif"
    assert any(
        row["latitude"] == 32 and row["coord_source"] == "reviewed_gps" for row in db.locations.get_geolocated_photos()
    )
    assert client.post(url, json={"folder_id": folder, "key": finding["key"], "action": "restore"}).status_code == 200
    assert db.locations.get_effective(ids)[ids[1]]["latitude"] == 32.03
