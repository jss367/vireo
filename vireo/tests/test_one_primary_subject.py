"""One primary-subject rule for the lightbox, Highlights and the inspector.

Reproduces the 2026-11-07 hawk frame (DSC_8166): the detector kept a
full-height trunk strip at 0.47 beside the hawk at 0.37. The lightbox showed
the hawk while Highlights named the photo after the strip ("Common Leopard
Gecko"), and the Pipeline inspector drew only the hawk, so nothing on screen
explained the gecko.
"""

import json

import pytest
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


@pytest.mark.parametrize("primary_prediction", ["missing", "rejected"])
@pytest.mark.parametrize("secondary_confidence", [0.4, 0.8])
def test_unclassified_or_rejected_primary_does_not_promote_secondary(
    app_and_db, primary_prediction, secondary_confidence,
):
    from services import review_decisions

    app, db = app_and_db
    fid = _folder(db)
    pid, hawk, strip, _blur = _hawk_frame(db, fid)
    db.conn.execute(
        "UPDATE detections SET detector_confidence = ? WHERE id = ?",
        (secondary_confidence, strip),
    )
    prediction_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (hawk,),
    ).fetchone()["id"]
    if primary_prediction == "missing":
        db.conn.execute("DELETE FROM predictions WHERE id = ?", (prediction_id,))
    else:
        review_decisions.update_prediction_status(db, prediction_id, "rejected")
    db.commit()

    assert detection_store.get_detections(db, pid)[0]["id"] == hawk
    rows = db.predictions.top_unrejected_by_detection([pid])[pid]
    assert {row["detection_id"]: row["subject_rank"] for row in rows}[strip] > 1
    candidates = db.species_curation.get_highlights_candidates(fid)
    photo = next(row for row in candidates if row["id"] == pid)
    assert all(subject["subject_rank"] > 1 for subject in photo["subject_predictions"])
    assert _buckets(app.test_client(), fid) == {}


def _af_metadata(x, y):
    return json.dumps({"MakerNotes": {"AFCoordinatesAvailable": 1, "AFImageWidth": 1000,
                      "AFImageHeight": 1000, "AFAreaXPosition": x, "AFAreaYPosition": y,
                      "AFAreaWidth": 60, "AFAreaHeight": 60}})


def test_af_choice_and_miss_are_explained_in_both_views(app_and_db):
    from subjects import select_primary
    app, db = app_and_db
    pid, hawk, strip, _ = _hawk_frame(db, _folder(db))
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?", (_af_metadata(800, 500), pid))
    sync_primary(db, pid, min_conf=.2)
    db.commit()
    client = app.test_client()
    for endpoint in ("pipeline", "subjects"):
        result = client.get(f"/api/photos/{pid}/{endpoint}").get_json()
        assert result["primary_reason"] == "Contains the camera AF point"
        assert not result["possible_missed_subject"]
    assert payload(db, pid)["primary_detection_id"] == strip
    select_primary(db, pid, hawk)
    assert payload(db, pid)["primary_reason"] == "Your choice"
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?", (_af_metadata(500, 50), pid))
    sync_primary(db, pid)
    assert payload(db, pid)["possible_missed_subject"]


def test_sharpness_requires_a_clear_lead_over_the_other_box(db):
    fid = _folder(db)
    pid = db.conn.execute("INSERT INTO photos(folder_id,filename) VALUES (?, 'a.jpg')", (fid,)).lastrowid
    first, second = _regions(db, pid, ((.1,.1,.2,.2), .8), ((.6,.1,.2,.2), .7))
    _classify(db, first, "Coot", .9, .55)
    _classify(db, second, "Grebe", .8, .45)
    for did in (first, second):
        _analysis(db, did, .5)
    for sharp, expected, reason in [(1.8, first, "Species-label match"), (3., second, "Clearly sharper region")]:
        db.conn.execute("UPDATE detection_subjects SET features=json_set(features,'$.subject_sharpness',?) "
                        "WHERE detection_id=?", (sharp, second))
        sync_primary(db, pid)
        result = payload(db, pid)
        assert (result["primary_detection_id"], result["primary_reason"]) == (expected, reason)


def test_burst_overlap_breaks_weak_switch_and_af_explains_real_switch(db):
    fid = _folder(db)
    def frame(name, ts):
        return db.conn.execute("INSERT INTO photos(folder_id,filename,timestamp) VALUES (?,?,?)",
                               (fid,name,"2026-11-14T09:00:17." + ts)).lastrowid
    first = frame("DSC_1.NEF", "100")
    _regions(db, first, ((.1,.1,.2,.2), .7))
    sync_primary(db, first)
    second = frame("DSC_2.NEF", "200")
    same, other = _regions(db, second, ((.11,.1,.2,.2), .7), ((.6,.6,.2,.2), .8))
    _classify(db, other, "Coot", .9, .55)
    sync_primary(db, second)
    assert payload(db, second)["primary_detection_id"] == same
    assert payload(db, second)["primary_reason"] == "Overlaps the previous burst subject"
    db.conn.execute("UPDATE photos SET exif_data=? WHERE id=?", (_af_metadata(700,700), second))
    sync_primary(db, second)
    result = payload(db, second)
    assert result["primary_detection_id"] == other
    assert result["possible_subject_switch"]


@pytest.mark.parametrize("group", ["EXIF", "IFD0", "TIFF", "File", None])
def test_nikon_focus_area_maps_sensor_crop_and_orientation(group):
    from focus_point import focus_point
    meta = json.loads(_af_metadata(550, 400))
    meta["MakerNotes"]["CropHiSpeed"] = "2 1000 1000 500 500 250 250"
    if group:
        meta[group] = {"Orientation": 6}
    else:
        meta["Orientation"] = 6
    focus = focus_point(meta)
    assert tuple(focus) == pytest.approx((.7,.6,.12,.12))
    meta["MakerNotes"]["AFCoordinatesAvailable"] = 0
    assert focus_point(meta) is None
    assert focus_point("broken json") is None

def test_manual_choice_and_history_refresh_an_already_projected_burst(db):
    from services.subject_selections import replay_history, set_selection

    fid = _folder(db)
    frames = []
    for i, timestamp in enumerate(("2026-11-14T09:00:17.000", "2026-11-14T09:00:17.100",
                                   "2026-11-14T09:00:17.200", "2026-11-15T09:00:17.300")):
        pid = db.conn.execute("INSERT INTO photos(folder_id,filename,timestamp,burst_id) VALUES (?,?,?,?)",
                              (fid, f"bird{i}.NEF", timestamp, "reused-id")).lastrowid
        left, right = _regions(db, pid, ((.1,.1,.2,.2), .8), ((.6,.6,.2,.2), .7))
        sync_primary(db, pid)
        frames.append((pid, left, right))
    db.commit()
    pid, _, right = frames[0]
    assert db.subjects.previous_burst_subject(frames[-1][0]) is None
    set_selection(db, pid, {"detection_id": right})
    assert [db.subjects.primary_state(p)["detection_id"] for p, _, _ in frames] == [
        frames[0][2], frames[1][2], frames[2][2], frames[3][1]]
    with db.transaction():
        replay_history(db, [{"photo_id": pid, "old_value": json.dumps({"choice": None, "boxes": []})}], undo=True)
    assert all(db.subjects.primary_state(p)["detection_id"] == left for p, left, _ in frames)


def _consecutive_pair(db):
    fid = _folder(db)
    return tuple(
        db.conn.execute("INSERT INTO photos(folder_id,filename,timestamp) VALUES (?,?,?)",
                        (fid, f"bird{i}.NEF", f"2026-11-14T09:00:17.{i}00")).lastrowid
        for i in range(2)
    )


@pytest.mark.parametrize("rescued_successor", [False, True])
def test_successor_keeps_its_projection_floor_and_model(db, rescued_successor):
    first, second = _consecutive_pair(db)
    left, right = (.1, .1, .2, .2), (.6, .6, .2, .2)
    model = "megadetector-v6"
    normal, rescue = .2, .05
    _regions(db, first, (right, .8 if rescued_successor else .1))
    fallback, overlap = _regions(db, second, (left, .15 if rescued_successor else .8), (right, .09))
    if rescued_successor:
        # A stronger overlapping region from a different model is ineligible for rescue.
        db.detections.save(second, [{"box": dict(zip("xywh", (.61, .61, .2, .2), strict=True)),
                                     "confidence": .9, "category": "animal"}], detector_model="other")
    sync_primary(db, second, min_conf=rescue if rescued_successor else normal,
                 detector_model=model if rescued_successor else None)
    assert db.subjects.primary_state(second)["detection_id"] == fallback
    db.conn.execute("UPDATE photos SET mask_path='second.png' WHERE id=?", (second,))

    sync_primary(db, first, min_conf=normal if rescued_successor else rescue,
                 detector_model=None if rescued_successor else model)

    assert db.subjects.primary_state(second)["detection_id"] == (overlap if rescued_successor else fallback)
    db.commit()
    mask = db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (second,)).fetchone()[0]
    assert mask == (None if rescued_successor else "second.png")


@pytest.mark.parametrize("rollback", [False, True])
def test_busy_successor_refresh_follows_committed_manual_choice(db, monkeypatch, rollback):
    from threading import Event

    import subjects
    from pipeline_locks import acquire_photo_mask
    from services.subject_selections import set_selection

    first, second = _consecutive_pair(db)
    left, right = (.1, .1, .2, .2), (.6, .6, .2, .2)
    first_left, first_right = _regions(db, first, (left, .8), (right, .7))
    second_left, second_right = _regions(db, second, (left, .8), (right, .7))
    sync_primary(db, first)
    sync_primary(db, second)
    db.commit()
    finished = Event()
    refresh = subjects._refresh_when_available

    def observe_refresh(*args):
        try:
            refresh(*args)
        finally:
            finished.set()

    monkeypatch.setattr(subjects, "_refresh_when_available", observe_refresh)
    with acquire_photo_mask(second):
        if rollback:
            with pytest.raises(RuntimeError, match="cancel edit"), db.transaction():
                set_selection(db, first, {"detection_id": first_right})
                raise RuntimeError("cancel edit")
        else:
            assert set_selection(db, first, {"detection_id": first_right})["changed"]
        assert db.subjects.primary_state(first)["detection_id"] == (first_left if rollback else first_right)
        state = db.subjects.primary_state(second)
        assert state["detection_id"] == second_left
        assert state["refresh_pending"] == (0 if rollback else 1)
    assert finished.wait(timeout=5)
    state = db.subjects.primary_state(second)
    assert state["detection_id"] == (second_left if rollback else second_right)
    assert not state["refresh_pending"]


def test_legacy_successor_waits_for_its_own_policy(db):
    first, second = _consecutive_pair(db)
    left, right = (.1, .1, .2, .2), (.6, .6, .2, .2)
    _regions(db, first, (right, .9))
    fallback, overlap = _regions(db, second, (left, .15), (right, .09))
    db.subjects.set_primary_state(second, fallback)

    sync_primary(db, first, min_conf=.2)
    state = db.subjects.primary_state(second)
    assert (state["detection_id"], state["refresh_pending"]) == (fallback, 1)
    sync_primary(db, second, min_conf=.05, detector_model="megadetector-v6")
    db.commit()
    state = db.subjects.primary_state(second)
    assert (state["detection_id"], state["refresh_pending"]) == (overlap, 0)


def test_audit_compares_active_evidence_with_retained_region(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid, hawk, strip, _ = _hawk_frame(db, fid)
    db.subjects.set_primary_state(pid, strip)
    db.commit()
    client = app.test_client()
    report = client.get('/api/audit/subjects').get_json()
    assert report['findings'] == [dict(id=pid, filename='DSC_8166.jpg', detection_id=hawk,
                                      unnamed=0, stale_score=1, stale_mask=0)]
    assert [(r['species'], r['detection_id'], r['bucket_size']) for r in report['review']] == [
        ('Red-shouldered Hawk', hawk, 1)]
    assert client.get('/api/audit/subjects/counts').get_json() == dict(
        unnamed=0, stale_score=1, stale_mask=0)


def test_subject_audit_checks_only_active_mask_and_visible_workspace(app_and_db):
    app, db = app_and_db
    fid = _folder(db)
    pid, hawk, strip, _ = _hawk_frame(db, fid)
    db.conn.execute("DELETE FROM predictions WHERE detection_id=?", (hawk,))
    anchor = db.detections.save(pid, [{"box": dict(x=0, y=0, w=1, h=1),
                                      "confidence": 1, "category": "animal"}], detector_model="full-image")[0]
    db.predictions.add(anchor, "Whole frame guess", 0.9, "BioCLIP-2.5")
    db.conn.execute("""INSERT INTO photo_masks(photo_id,variant,path,created_at,detector_model,
                    prompt_x,prompt_y,prompt_w,prompt_h) SELECT photo_id,'test','test.png',1,
                    detector_model,box_x,box_y,box_w,box_h FROM detections WHERE id=?""", (strip,))
    db.commit()
    assert db.audit.subject_findings(.2)[0]['unnamed'] == 1
    assert db.audit.subject_findings(.2)[0]['stale_mask'] == 0
    db.conn.execute("UPDATE photos SET active_mask_variant='test' WHERE id=?", (pid,))
    assert db.audit.subject_findings(.2)[0]['stale_mask'] == 1
    db.conn.execute("DELETE FROM workspace_folders WHERE folder_id=?", (fid,))
    assert db.audit.subject_findings(.2) == []


def test_subject_audit_treats_legacy_score_without_state_as_stale(db):
    """A migrated catalog with a quality_score but no photo_subject_state
    row cannot prove which detection produced the score, so the audit must
    report it stale (matching ``SubjectsRepository.has_stale_primary_state``)."""
    fid = _folder(db)
    pid, _hawk, _strip, _ = _hawk_frame(db, fid)
    db.conn.execute("DELETE FROM photo_subject_state WHERE photo_id=?", (pid,))
    db.commit()
    assert db.audit.subject_findings(.2)[0]['stale_score'] == 1


def test_subject_audit_excludes_rejected_and_superseded_predictions(db):
    """Audit's 'unnamed' check reflects the same evidence Highlights uses:
    rejected or stale-fingerprint predictions no longer count as a
    classification, and a stale full-image prediction no longer manufactures
    a whole-frame fallback."""
    fid = _folder(db)
    pid, hawk, _strip, _ = _hawk_frame(db, fid)
    anchor = db.detections.save(pid, [{"box": dict(x=0, y=0, w=1, h=1),
                                      "confidence": 1, "category": "animal"}],
                                detector_model="full-image")[0]
    db.predictions.add(anchor, "Whole frame guess", 0.9, "BioCLIP-2.5")
    db.conn.execute(
        "INSERT INTO prediction_review(prediction_id, workspace_id, status, reviewed_at) "
        "SELECT id, ?, 'rejected', '2026-11-18' FROM predictions WHERE detection_id=?",
        (db._ws_id(), hawk),
    )
    db.commit()
    assert db.audit.subject_findings(.2)[0]['unnamed'] == 1
    db.conn.execute(
        "UPDATE predictions SET labels_fingerprint='stale' WHERE detection_id=?", (anchor,),
    )
    db.predictions.add(anchor, "Current guess", 0.9, "BioCLIP-2.5",
                       labels_fingerprint="current")
    db.commit()
    assert db.audit.subject_findings(.2)[0]['unnamed'] == 1
    db.predictions.add(hawk, "Red-shouldered Hawk", 1.0, "BioCLIP-2.5",
                       labels_fingerprint="current")
    db.commit()
    assert db.audit.subject_findings(.2)[0]['unnamed'] == 0


def test_subject_audit_reference_label_excludes_rejected_prediction(db):
    """A reference photo whose primary prediction is rejected in this
    workspace must not caption the crop with the rejected species."""
    from services.subject_audit import subject_audit

    fid = _folder(db)
    pid, hawk, _strip, _ = _hawk_frame(db, fid)
    db.collections.add_import("Subject audit references", [pid])
    db.commit()
    before = next(r for r in subject_audit(db)["references"] if r["id"] == pid)
    assert (before["detection_id"], before["species"]) == (hawk, "Red-shouldered Hawk")
    db.conn.execute(
        "INSERT INTO prediction_review(prediction_id, workspace_id, status, reviewed_at) "
        "SELECT id, ?, 'rejected', '2026-11-18' FROM predictions WHERE detection_id=?",
        (db._ws_id(), hawk),
    )
    db.commit()
    after = next(r for r in subject_audit(db)["references"] if r["id"] == pid)
    assert (after["detection_id"], after["species"]) == (hawk, None)


def test_subject_audit_reference_label_honors_saved_weak_rescue_floor(db):
    """A reference photo whose primary detection sits below the workspace
    detector floor but is kept by the saved per-photo weak-rescue floor must
    still caption the crop with its current unrejected prediction."""
    from services.subject_audit import subject_audit

    fid = _folder(db)
    pid = db.conn.execute(
        "INSERT INTO photos(folder_id,filename) VALUES (?,'weak-rescue.jpg')", (fid,),
    ).lastrowid
    det = db.detections.save(pid, [dict(
        box=dict(x=.1, y=.1, w=.3, h=.3), confidence=.1, category='animal',
    )], detector_model='megadetector-v6')[0]
    _classify(db, det, "Great Horned Owl", 1.0, match=.5)
    db.subjects.set_primary_state(pid, det, min_conf=.05, detector_model='megadetector-v6')
    db.collections.add_import("Subject audit references", [pid])
    db.commit()
    row = next(r for r in subject_audit(db)["references"] if r["id"] == pid)
    assert (row["detection_id"], row["species"]) == (det, "Great Horned Owl")
