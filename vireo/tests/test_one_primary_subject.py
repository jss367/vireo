"""One primary-subject rule for the lightbox, Highlights and the inspector.

Reproduces the 2026-11-07 hawk frame (DSC_8166): the detector kept a
full-height trunk strip at 0.47 beside the hawk at 0.37. The lightbox showed
the hawk while Highlights named the photo after the strip ("Common Leopard
Gecko"), and the Pipeline inspector drew only the hawk, so nothing on screen
explained the gecko.
"""

import json

from services import detection_store
from subjects import payload, resync_after_classification, sync_primary


def _folder(db, path="/hawk"):
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')", (path, path.strip("/")),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)", (db._ws_id(), fid),
    )
    return fid


def _regions(db, pid, *regions):
    """Save ``(box, confidence)`` regions as one detector run; their ids, in order."""
    return db.detections.save(
        pid, [{"box": dict(zip("xywh", box, strict=True)), "confidence": conf, "category": "animal"}
              for box, conf in regions],
        detector_model="megadetector-v6",
    )


def _classify(db, det, species, confidence, match, background=0.10):
    db.predictions.add(det, species, confidence, "BioCLIP-2.5")
    db.conn.execute(
        "INSERT INTO classifier_match_scores (detection_id, classifier_model, labels_fingerprint, "
        "max_match_score, background_score, score_kind) VALUES (?, 'BioCLIP-2.5', "
        "(SELECT labels_fingerprint FROM predictions WHERE detection_id = ? LIMIT 1), ?, ?, 'cosine')",
        (det, det, match, background),
    )
    db.commit()


def _analysis(db, det, quality):
    features = json.dumps({
        "subject_sharpness": 1.0, "subject_size": 0.1, "subject_clip_high": 0.0,
        "subject_clip_low": 0.0, "subject_y_median": 0.5,
    })
    db.conn.execute(
        "INSERT INTO detection_subjects (detection_id, source_key, crop, quality_score, exposure_ev, features) "
        "VALUES (?, 'k', '{}', ?, 0.0, ?)", (det, quality, features),
    )
    db.commit()


def _hawk_frame(db, fid, name="DSC_8166.jpg"):
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, ?, 0.8, 'none')",
        (fid, name),
    ).lastrowid
    hawk, strip, blur = _regions(
        db, pid, ((0.32, 0.26, 0.35, 0.74), 0.37), ((0.66, 0.0, 0.34, 1.0), 0.47),
        ((0.0, 0.0, 0.38, 1.0), 0.35),
    )
    _classify(db, hawk, "Red-shouldered Hawk", 1.0, match=0.494)
    _classify(db, strip, "Common Leopard Gecko", 0.92, match=0.445, background=0.20)
    _classify(db, blur, "Grace's Warbler", 0.47, match=0.386, background=0.25)
    return pid, hawk, strip, blur


def _buckets(client, fid):
    data = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0").get_json()
    return {b["species"]: [p["id"] for p in b["photos"]] for b in data["buckets"]}


def test_lightbox_and_highlights_pick_the_hawk_not_the_trunk_strip(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid, hawk, _strip, _blur = _hawk_frame(db, fid)

    assert detection_store.get_detections(db, pid)[0]["id"] == hawk
    assert payload(db, pid)["primary_detection_id"] == hawk
    buckets = _buckets(app.test_client(), fid)
    assert list(buckets) == ["Red-shouldered Hawk"]
    assert buckets["Red-shouldered Hawk"] == [pid]


def test_a_confident_strip_beside_the_subject_is_not_a_second_species(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, 'p.jpg', 0.8, 'none')", (fid,),
    ).lastrowid
    hawk, strip, bird = _regions(
        db, pid, ((0.45, 0.3, 0.3, 0.6), 0.58), ((0.0, 0.0, 0.39, 1.0), 0.61),
        ((0.8, 0.1, 0.1, 0.1), 0.77),
    )
    _classify(db, hawk, "Red-shouldered Hawk", 1.0, match=0.50)
    _classify(db, strip, "Violet-crowned Hummingbird", 0.9, match=0.44)
    _classify(db, bird, "Yellow-rumped Warbler", 0.95, match=0.47)

    buckets = _buckets(app.test_client(), fid)
    # The second real bird still opens its bucket; the strip does not.
    assert sorted(buckets) == ["Red-shouldered Hawk", "Yellow-rumped Warbler"]


def test_inspector_lists_every_region_and_what_each_view_does_with_it(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid, hawk, strip, blur = _hawk_frame(db, fid)

    data = app.test_client().get(f"/api/photos/{pid}/pipeline").get_json()

    regions = data["regions"]
    assert [r["detection_id"] for r in regions] == [hawk, strip, blur]
    assert [r["number"] for r in regions] == [1, 2, 3]
    assert [r["is_subject"] for r in regions] == [True, False, False]
    assert [r["spans_frame"] for r in regions] == [False, True, True]
    assert regions[0]["highlights_role"] == "bucket"
    assert regions[0]["highlights_species"] == "Red-shouldered Hawk"
    assert regions[1]["top_species"] == "Common Leopard Gecko"
    assert regions[1]["highlights_role"] == "below_floor"
    assert data["secondary_subject_floor"] == 0.5
    assert data["detection_box"]["x"] == regions[0]["box"]["x"]
    # Every prediction says which region it came from.
    assert {p["detection_id"] for p in data["predictions"]} == {hawk, strip, blur}


def test_inspector_names_a_spanning_second_region(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, 'p.jpg', 0.8, 'none')", (fid,),
    ).lastrowid
    hawk, strip = _regions(db, pid, ((0.45, 0.3, 0.3, 0.6), 0.58), ((0.0, 0.0, 0.39, 1.0), 0.61))
    _classify(db, hawk, "Red-shouldered Hawk", 1.0, match=0.50)
    _classify(db, strip, "Swinhoe's White-eye", 0.9, match=0.44)

    regions = app.test_client().get(f"/api/photos/{pid}/pipeline").get_json()["regions"]
    assert [(r["detection_id"], r["highlights_role"]) for r in regions] == [
        (hawk, "bucket"), (strip, "spans_frame"),
    ]


def test_classification_moves_the_projected_subject(db):
    fid = _folder(db)
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, 'p.jpg', NULL, 'none')", (fid,),
    ).lastrowid
    strip, hawk = _regions(db, pid, ((0.6, 0.1, 0.3, 0.5), 0.6), ((0.1, 0.1, 0.3, 0.5), 0.4))
    _analysis(db, strip, 0.55)
    _analysis(db, hawk, 0.68)
    # Analysis runs before classification: with no match scores yet the
    # detector's stronger box is the subject and its quality is projected.
    sync_primary(db, pid, min_conf=0.2)
    db.commit()
    assert db.subjects.primary_state(pid)["detection_id"] == strip
    _classify(db, hawk, "Red-shouldered Hawk", 1.0, match=0.49)
    _classify(db, strip, "Common Leopard Gecko", 0.9, match=0.44)

    resync_after_classification(db, [pid], 0.2)

    assert db.subjects.primary_state(pid)["detection_id"] == hawk
    photo = db.conn.execute("SELECT quality_score FROM photos WHERE id = ?", (pid,)).fetchone()
    assert photo["quality_score"] == 0.68
