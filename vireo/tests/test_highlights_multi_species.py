"""Highlights with photos that hold two species (issue #1298).

A photo with two detected subjects can carry two species. Highlights puts it
in each species' bucket, Confirm from a bucket adds that bucket's species
without touching the other, and Relabel from a bucket replaces only that
bucket's species.
"""

from highlights_payload import photo_highlight_entries
from services.edit_history import redo_last_undo, undo_last_edit


def _folder(db, path="/ms"):
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (path, path.strip("/")),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    return fid


def _photo(db, fid, name, quality=0.8):
    return db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, ?, ?, 'none')",
        (fid, name, quality),
    ).lastrowid


def _subjects(db, pid, count=2):
    return db.detections.save(
        pid,
        [{"box": {"x": 0.1 + 0.4 * i, "y": 0.1, "w": 0.3, "h": 0.3},
          "confidence": 0.9, "category": "animal"} for i in range(count)],
        detector_model="MDV6",
    )


def _pred(db, det, species, confidence):
    db.predictions.add(det, species, confidence, "m")
    db.commit()
    return db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det, species),
    ).fetchone()["id"]


def _keywords(db, pid):
    return {kw["name"] for kw in db.photo_keywords.get_for_photo(pid)}


def _buckets(client, fid, confirmation="all"):
    data = client.get(
        f"/api/highlights?folder_id={fid}&confirmation={confirmation}"
        "&confidence_threshold=0.5"
    ).get_json()
    return data, {b["species"]: b for b in data["buckets"]}


def _teal_photo_with_wigeon_subject(db, fid):
    """A photo tagged Green-winged Teal whose second subject reads as a wigeon."""
    pid = _photo(db, fid, "pair.jpg")
    db.tag_photo(pid, db.add_keyword("Green-winged Teal", is_species=True))
    db.commit()
    teal_det, wigeon_det = _subjects(db, pid)
    teal_pred = _pred(db, teal_det, "Green-winged Teal", 0.9)
    wigeon_pred = _pred(db, wigeon_det, "American Wigeon", 0.85)
    return pid, teal_pred, wigeon_pred


def test_photo_with_a_second_subject_joins_both_buckets(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid, _teal_pred, wigeon_pred = _teal_photo_with_wigeon_subject(db, fid)

    data, buckets = _buckets(app.test_client(), fid)

    teal = buckets["Green-winged Teal"]["photos"]
    wigeon = buckets["American Wigeon"]["photos"]
    assert [p["id"] for p in teal] == [pid]
    assert [p["id"] for p in wigeon] == [pid]
    assert teal[0]["has_accepted_species"] is True
    assert wigeon[0]["has_accepted_species"] is False
    assert wigeon[0]["is_confirmable_prediction"] is True
    assert wigeon[0]["prediction_id"] == wigeon_pred
    assert wigeon[0]["bucket_species"] == "American Wigeon"
    # Each entry says what the photo is already tagged with.
    assert wigeon[0]["confirmed_species_list"] == ["Green-winged Teal"]
    # ...and every bucket it is in, so the page can refresh each one.
    for entry in (teal[0], wigeon[0]):
        assert entry["bucket_species_list"] == [
            "Green-winged Teal", "American Wigeon",
        ]
    # One photo, even though it shows in two buckets.
    assert data["meta"]["eligible"] == 1


def test_confirmation_filter_splits_one_photo_by_bucket(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    _teal_photo_with_wigeon_subject(db, fid)
    client = app.test_client()

    _data, confirmed = _buckets(client, fid, "confirmed")
    _data, unconfirmed = _buckets(client, fid, "unconfirmed")

    assert set(confirmed) == {"Green-winged Teal"}
    assert set(unconfirmed) == {"American Wigeon"}


def test_photo_tagged_with_two_species_is_confirmed_in_both(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid = _photo(db, fid, "two-tags.jpg")
    db.tag_photo(pid, db.add_keyword("Green-winged Teal", is_species=True))
    db.tag_photo(pid, db.add_keyword("American Wigeon", is_species=True))
    db.commit()

    _data, buckets = _buckets(app.test_client(), fid)

    for species in ("Green-winged Teal", "American Wigeon"):
        photos = buckets[species]["photos"]
        assert [p["id"] for p in photos] == [pid]
        assert photos[0]["has_accepted_species"] is True
    assert photo_highlight_entries(db, pid) and {
        e["species"] for e in photo_highlight_entries(db, pid)
    } == {"Green-winged Teal", "American Wigeon"}


def test_untagged_photo_with_two_subjects_is_a_candidate_for_each(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid = _photo(db, fid, "untagged-pair.jpg")
    teal_det, wigeon_det = _subjects(db, pid)
    _pred(db, teal_det, "Green-winged Teal", 0.9)
    _pred(db, wigeon_det, "American Wigeon", 0.85)

    data, buckets = _buckets(app.test_client(), fid)

    assert [p["id"] for p in buckets["Green-winged Teal"]["photos"]] == [pid]
    assert [p["id"] for p in buckets["American Wigeon"]["photos"]] == [pid]
    assert data["unidentified"]["photo_count"] == 0


def test_keyword_the_model_disagrees_with_opens_no_prediction_bucket(app_and_db):
    """A user-corrected photo (keyword differs from its only subject's
    prediction) stays in its keyword's bucket alone, as before."""
    app, db = app_and_db
    fid = _folder(db)
    pid = _photo(db, fid, "corrected.jpg")
    db.tag_photo(pid, db.add_keyword("Green-winged Teal", is_species=True))
    db.commit()
    (det,) = _subjects(db, pid, count=1)
    _pred(db, det, "Mallard", 0.95)

    _data, buckets = _buckets(app.test_client(), fid)

    assert set(buckets) == {"Green-winged Teal"}


def test_confirm_from_a_bucket_adds_that_species_and_keeps_the_other(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid, teal_pred, wigeon_pred = _teal_photo_with_wigeon_subject(db, fid)
    client = app.test_client()

    resp = client.post(
        "/api/highlights/confirm",
        json={"photo_ids": [pid], "species": "American Wigeon"},
    )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["skipped"] == []
    assert [a["photo_id"] for a in body["affected"]] == [pid]
    assert _keywords(db, pid) >= {"Green-winged Teal", "American Wigeon"}
    ws = db._ws_id()
    assert db.predictions.get_review_status(wigeon_pred, ws) == "accepted"
    # The other subject's prediction is a different detection's decision.
    assert db.predictions.get_review_status(teal_pred, ws) == "pending"

    undone = undo_last_edit(db)
    assert undone["action_type"] == "prediction_accept"
    keywords = _keywords(db, pid)
    assert "Green-winged Teal" in keywords
    assert "American Wigeon" not in keywords


def test_confirm_from_a_bucket_skips_photos_already_tagged_with_it(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid, teal_pred, _wigeon_pred = _teal_photo_with_wigeon_subject(db, fid)

    resp = app.test_client().post(
        "/api/highlights/confirm",
        json={"photo_ids": [pid], "species": "Green-winged Teal"},
    )

    assert resp.get_json()["skipped"] == [
        {"photo_id": pid, "reason": "already_confirmed"},
    ]
    assert db.predictions.get_review_status(teal_pred, db._ws_id()) == "pending"


def test_confirm_from_a_bucket_skips_photos_with_no_such_subject(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid, _teal_pred, _wigeon_pred = _teal_photo_with_wigeon_subject(db, fid)

    resp = app.test_client().post(
        "/api/highlights/confirm",
        json={"photo_ids": [pid], "species": "Mallard"},
    )

    assert resp.get_json()["skipped"] == [
        {"photo_id": pid, "reason": "no_prediction"},
    ]
    assert "Mallard" not in _keywords(db, pid)


def test_confirm_rejects_a_blank_species(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid = _photo(db, fid, "blank.jpg")

    resp = app.test_client().post(
        "/api/highlights/confirm", json={"photo_ids": [pid], "species": " "},
    )

    assert resp.status_code == 400


def test_relabel_from_a_bucket_replaces_only_that_species(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid = _photo(db, fid, "relabel-pair.jpg")
    db.tag_photo(pid, db.add_keyword("Green-winged Teal", is_species=True))
    db.tag_photo(pid, db.add_keyword("American Wigeon", is_species=True))
    db.commit()
    teal_det, wigeon_det = _subjects(db, pid)
    teal_pred = _pred(db, teal_det, "Green-winged Teal", 0.9)
    wigeon_pred = _pred(db, wigeon_det, "American Wigeon", 0.85)

    resp = app.test_client().post("/api/highlights/relabel", json={
        "photo_ids": [pid],
        "species": "Eurasian Wigeon",
        "from_species": "American Wigeon",
    })

    assert resp.status_code == 200
    assert resp.get_json()["rejected_prediction_ids"] == [wigeon_pred]
    keywords = _keywords(db, pid)
    assert {"Green-winged Teal", "Eurasian Wigeon"} <= keywords
    assert "American Wigeon" not in keywords
    ws = db._ws_id()
    assert db.predictions.get_review_status(teal_pred, ws) == "pending"
    history = db.edit_history.list_recent(limit=1)[0]
    assert history["action_type"] == "species_replace"
    assert history["description"] == (
        'Replaced "American Wigeon" with "Eurasian Wigeon" on 1 photos'
    )

    undo_last_edit(db)
    keywords = _keywords(db, pid)
    assert {"Green-winged Teal", "American Wigeon"} <= keywords
    assert "Eurasian Wigeon" not in keywords
    assert db.predictions.get_review_status(wigeon_pred, ws) == "pending"

    redo_last_undo(db)
    keywords = _keywords(db, pid)
    assert {"Green-winged Teal", "Eurasian Wigeon"} <= keywords
    assert "American Wigeon" not in keywords


def test_relabel_candidate_onto_a_species_the_photo_has_survives_undo(app_and_db):
    """Relabelling the wigeon candidate as the teal the photo already carries
    must not lose the teal keyword when undone."""
    app, db = app_and_db
    fid = _folder(db)
    pid, _teal_pred, wigeon_pred = _teal_photo_with_wigeon_subject(db, fid)

    resp = app.test_client().post("/api/highlights/relabel", json={
        "photo_ids": [pid],
        "species": "Green-winged Teal",
        "from_species": "American Wigeon",
    })

    assert resp.status_code == 200
    assert resp.get_json()["rejected_prediction_ids"] == [wigeon_pred]
    assert "Green-winged Teal" in _keywords(db, pid)

    undo_last_edit(db)
    assert "Green-winged Teal" in _keywords(db, pid)
    assert db.predictions.get_review_status(wigeon_pred, db._ws_id()) == "pending"


def test_relabel_candidate_keeps_the_tagged_species(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid, teal_pred, wigeon_pred = _teal_photo_with_wigeon_subject(db, fid)

    resp = app.test_client().post("/api/highlights/relabel", json={
        "photo_ids": [pid],
        "species": "Eurasian Wigeon",
        "from_species": "American Wigeon",
    })

    assert resp.status_code == 200
    assert {"Green-winged Teal", "Eurasian Wigeon"} <= _keywords(db, pid)
    ws = db._ws_id()
    assert db.predictions.get_review_status(wigeon_pred, ws) == "rejected"
    assert db.predictions.get_review_status(teal_pred, ws) == "pending"

    undo_last_edit(db)
    keywords = _keywords(db, pid)
    assert "Green-winged Teal" in keywords
    assert "Eurasian Wigeon" not in keywords


def test_relabel_response_returns_canonical_stored_species(app_and_db):
    """Lightbox retargets its active bucket to the response's ``species``, so
    a request submitted in a non-canonical case still points the still-open
    controls at the bucket the server actually created."""
    app, db = app_and_db
    fid = _folder(db)
    pid, _teal_pred, _wigeon_pred = _teal_photo_with_wigeon_subject(db, fid)

    resp = app.test_client().post("/api/highlights/relabel", json={
        "photo_ids": [pid],
        "species": "eurasian wigeon",
        "from_species": "American Wigeon",
    })

    assert resp.status_code == 200
    payload = resp.get_json()
    kid = payload["keyword_id"]
    stored = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (kid,)
    ).fetchone()["name"]
    assert payload["species"] == stored
