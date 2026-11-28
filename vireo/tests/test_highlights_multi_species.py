"""Highlights with photos that hold two species (issue #1298).

A photo with two detected subjects can carry two species. Highlights puts it
in each species' bucket, Confirm from a bucket adds that bucket's species
without touching the other, and Relabel from a bucket replaces only that
bucket's species.
"""

from highlights_payload import photo_highlight_entries
from services import keyword_tagging
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
    keyword_tagging.tag_photo(db, pid, db.keywords.add("Green-winged Teal", is_species=True))
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
    keyword_tagging.tag_photo(db, pid, db.keywords.add("Green-winged Teal", is_species=True))
    keyword_tagging.tag_photo(db, pid, db.keywords.add("American Wigeon", is_species=True))
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
    keyword_tagging.tag_photo(db, pid, db.keywords.add("Green-winged Teal", is_species=True))
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
    keyword_tagging.tag_photo(db, pid, db.keywords.add("Green-winged Teal", is_species=True))
    keyword_tagging.tag_photo(db, pid, db.keywords.add("American Wigeon", is_species=True))
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


# -- which species a photo is a picture of (``species_presence``) -----------


def _tagged_photo(db, fid, name, species, birds, quality=0.8, flag="none"):
    """A photo tagged with ``species``, one detected region per
    ``(species, box, detector confidence)`` in ``birds``."""
    pid = _photo(db, fid, name, quality)
    db.conn.execute("UPDATE photos SET flag = ? WHERE id = ?", (flag, pid))
    for tag in species:
        keyword_tagging.tag_photo(db, pid, db.keywords.add(tag, is_species=True))
    db.commit()
    dets = db.detections.save(
        pid,
        [{"box": dict(zip("xywh", box, strict=True)), "confidence": conf,
          "category": "animal"} for _species, box, conf in birds],
        detector_model="MDV6",
    )
    for det, (bird, _box, _conf) in zip(dets, birds, strict=True):
        _pred(db, det, bird, 0.9)
    return pid


#: A big dunlin in the middle of the frame and a small plover in a corner.
_DUNLIN_WITH_PLOVER_BEHIND = [
    ("Dunlin", (0.3, 0.3, 0.4, 0.4), 0.9),
    ("Black-bellied Plover", (0.8, 0.1, 0.1, 0.1), 0.6),
]
_BOTH = ["Dunlin", "Black-bellied Plover"]


def test_a_pick_counts_only_for_the_species_the_photo_is_of(app_and_db):
    """A picked dunlin photo with a plover behind it leads Dunlin and sorts
    after the plover's own photos, without the pick's score bonus."""
    from services.species_choices import PhotoScope
    from species_best import SpeciesBestQuery, build_species_best_payload

    app, db = app_and_db
    fid = _folder(db)
    flock = _tagged_photo(
        db, fid, "flock.jpg", _BOTH, _DUNLIN_WITH_PLOVER_BEHIND,
        quality=0.9, flag="flagged",
    )
    portrait = _tagged_photo(
        db, fid, "plover.jpg", ["Black-bellied Plover"],
        [("Black-bellied Plover", (0.3, 0.3, 0.4, 0.4), 0.9)], quality=0.4,
    )

    _data, buckets = _buckets(app.test_client(), fid)

    dunlin, plover = buckets["Dunlin"], buckets["Black-bellied Plover"]
    assert [p["id"] for p in dunlin["photos"]] == [flock]
    assert dunlin["photos"][0]["species_presence"] == "subject"
    assert dunlin["best_decision"]["kind"] == "pick"
    assert dunlin["photos"][0]["highlight_score"] > dunlin["photos"][0]["highlight_base_score"]

    assert [p["id"] for p in plover["photos"]] == [portrait, flock]
    behind = plover["photos"][1]
    assert behind["species_presence"] == "in_frame"
    assert behind["subject_species_list"] == ["Dunlin"]
    assert behind["flag"] == "flagged"
    assert behind["highlight_score"] == behind["highlight_base_score"]
    assert behind["reasons"] == ["scored on another bird"]
    assert plover["best_decision"]["kind"] == "score"
    # The bucket ranks on its own photos' scores, not the dunlin photo's.
    assert plover["best_score"] == plover["photos"][0]["highlight_score"]

    best = build_species_best_payload(
        db, SpeciesBestQuery(scope=PhotoScope(folder_id=fid), per_species=5),
    )
    listed = {entry["species"]: entry["photos"] for entry in best["species"]}
    assert [p["id"] for p in listed["Black-bellied Plover"]] == [portrait, flock]
    assert listed["Black-bellied Plover"][1]["species_presence"] == "in_frame"
    assert listed["Black-bellied Plover"][1]["subject_species"] == ["Dunlin"]


def test_a_species_cropped_out_sorts_last_and_leads_only_with_nothing_else(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    cropped = _tagged_photo(
        db, fid, "cropped.jpg", _BOTH, _DUNLIN_WITH_PLOVER_BEHIND,
        quality=0.9, flag="flagged",
    )
    # The crop keeps the dunlin (center 0.5, 0.5) and drops the plover
    # (center 0.85, 0.15).
    db.set_photo_edit_recipe(
        cropped, {"crop": {"x": 0.25, "y": 0.25, "w": 0.5, "h": 0.5}},
    )
    uncropped = _tagged_photo(
        db, fid, "uncropped.jpg", _BOTH, _DUNLIN_WITH_PLOVER_BEHIND, quality=0.2,
    )
    db.commit()
    client = app.test_client()

    _data, buckets = _buckets(client, fid)

    plover = buckets["Black-bellied Plover"]
    assert [(p["id"], p["species_presence"]) for p in plover["photos"]] == [
        (uncropped, "in_frame"), (cropped, "outside_crop"),
    ]
    assert plover["best_decision"]["kind"] == "in_frame"
    assert plover["best_score"] is None
    assert [p["id"] for p in buckets["Dunlin"]["photos"]] == [cropped, uncropped]

    db.conn.execute("UPDATE photos SET flag = 'rejected' WHERE id = ?", (uncropped,))
    db.commit()
    _data, buckets = _buckets(client, fid)
    assert buckets["Black-bellied Plover"]["best_decision"]["kind"] == "outside_crop"


def test_a_chosen_highlight_leads_even_where_the_species_is_only_in_frame(app_and_db):
    """The rule orders what nobody chose; a highlight is the user's call."""
    app, db = app_and_db
    fid = _folder(db)
    flock = _tagged_photo(db, fid, "flock.jpg", _BOTH, _DUNLIN_WITH_PLOVER_BEHIND)
    portrait = _tagged_photo(
        db, fid, "plover.jpg", ["Black-bellied Plover"],
        [("Black-bellied Plover", (0.3, 0.3, 0.4, 0.4), 0.9)],
    )
    from services.species_choices import add_highlight
    add_highlight(db, "Black-bellied Plover", flock)
    db.commit()

    _data, buckets = _buckets(app.test_client(), fid)

    plover = buckets["Black-bellied Plover"]
    assert [p["id"] for p in plover["photos"]] == [flock, portrait]
    assert plover["best_decision"]["kind"] == "highlight"


def test_workspace_card_pages_share_rank_and_invalidate_after_edit(app_and_db, monkeypatch):
    import config
    import web.highlights as routes

    app, db = app_and_db
    client = app.test_client()
    fid = _folder(db)
    pid, _, _ = _teal_photo_with_wigeon_subject(db, fid)
    calls = []
    rank = routes.ranked_highlight_cards

    def count_rank(*args):
        calls.append(1)
        return rank(*args)

    monkeypatch.setattr(routes, "ranked_highlight_cards", count_rank)
    url = "/api/highlights?scope=workspace&view=cards"
    data = client.get(url).get_json()
    full = client.get("/api/highlights?scope=workspace").get_json()
    for bucket, expected in zip(data["buckets"], full["buckets"], strict=True):
        card = dict(zip(data["card_fields"], bucket["card_rows"][0], strict=True))
        assert card == {field: expected["photos"][0][field] for field in card}
        page = client.get("/api/highlights/bucket", query_string={
            "view": "cards", "scope": "workspace", "species": bucket["species"],
        }).get_json()
        assert page["photos"] == [card]
    found = client.get(url + "&species=wigeon").get_json()
    assert [b["species"] for b in found["buckets"]] == ["American Wigeon"]
    assert len(calls) == 1

    # The global detector floor lives outside SQLite. Raising it removes
    # the predicted wigeon membership and keeps the teal keyword.
    config.set("detector_confidence", 0.99)
    assert [b["species"] for b in client.get(url).get_json()["buckets"]] == ["Green-winged Teal"]
    assert len(calls) == 2
    db.conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (pid,))
    db.commit()
    assert client.get(url).get_json()["buckets"] == []
    assert len(calls) == 3
