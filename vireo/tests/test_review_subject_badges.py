"""Review badges on a photo with two subjects (issue #1298).

A redshank and an egret in one frame are two detections. Once the photo is
tagged Spotted Redshank, the egret's prediction is a second species, not a
disagreement. ``/api/predictions`` compares each prediction only with the
keywords its own detection is responsible for, and ``existing_species``
names only those, so the Review card never says the egret "disagrees with
Spotted Redshank".
"""

from services import photo_ingest


def _two_subject_photo(db, filename="redshank-egret.jpg"):
    folder_id = db.folders.tree()[0]["id"]
    photo_id = photo_ingest.add_photo(
        db, folder_id=folder_id, filename=filename, extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    redshank_det, egret_det = db.detections.save(
        photo_id,
        [{"box": {"x": x, "y": 0.1, "w": 0.3, "h": 0.3},
          "confidence": 0.9, "category": "animal"} for x in (0.1, 0.6)],
        detector_model="MDV6",
    )
    db.predictions.add(redshank_det, "Spotted Redshank", 0.9, "bioclip",
                      labels_fingerprint="fp1")
    # Classify compared this subject with every keyword on the photo, so the
    # stored snapshot says disagreement.
    db.predictions.add(egret_det, "Little Egret", 0.9, "bioclip",
                      category="disagreement", labels_fingerprint="fp1")
    db.tag_photo(photo_id, db.add_keyword("Spotted Redshank", is_species=True))
    return photo_id


def _row(client, photo_id, species):
    resp = client.get(f"/api/predictions?photo_ids={photo_id}")
    assert resp.status_code == 200, resp.get_data(as_text=True)
    return next(
        p for p in resp.get_json()["predictions"] if p["species"] == species
    )


def test_second_subject_is_new_not_a_disagreement(app_and_db):
    app, db = app_and_db
    photo_id = _two_subject_photo(db)

    egret = _row(app.test_client(), photo_id, "Little Egret")

    assert egret["category"] == "disagreement"  # the stale snapshot
    assert egret["effective_category"] == "new"
    assert "existing_species" not in egret


def test_disagreement_names_only_this_subjects_keywords(app_and_db):
    """A keyword no detection predicts could describe either bird, so the
    egret is still compared with it, but not with the redshank's keyword."""
    app, db = app_and_db
    photo_id = _two_subject_photo(db)
    db.tag_photo(photo_id, db.add_keyword("Grey Heron", is_species=True))

    egret = _row(app.test_client(), photo_id, "Little Egret")

    assert egret["effective_category"] == "conflict"
    assert egret["existing_species"] == ["Grey Heron"]
