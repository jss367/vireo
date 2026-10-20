"""Species intent must choose the pixels, not merely filter photo keywords."""
import io

import numpy as np
import pytest
from PIL import Image
from services import detection_store, photo_ingest
from subject_analysis import analyze_detection
from subject_ranking import SubjectComparison, parse_comparison, resolve_subject

TARGET = "Vermilion Flycatcher"
OTHER = "Sparrow"


@pytest.fixture
def comparison_library(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    folder = db.add_folder(str(tmp_path), name="Species comparison")
    photo_ids, detections = [], {}
    # The soft flycatcher and sharp sparrow are spatially separate. The next
    # two photographs have successively sharper flycatchers and softer sparrows.
    for index, (target_contrast, other_contrast) in enumerate(((0, 120), (45, 20), (100, 0))):
        pixels = np.full((160, 320, 3), 120, np.uint8)
        pattern = (np.indices((120, 100)).sum(axis=0) // 3 % 2) * 2 - 1
        for left, contrast in ((20, target_contrast), (200, other_contrast)):
            pixels[20:140, left:left + 100] = (120 + pattern * contrast)[..., None]
        path = tmp_path / f"DSC_{index:04d}.png"
        Image.fromarray(pixels).save(path)
        photo_id = photo_ingest.add_photo(db, folder_id=folder, filename=path.name, extension=".png",
                                         file_size=path.stat().st_size, file_mtime=path.stat().st_mtime)
        ids = detection_store.write_detection_batch(db, photo_id, "megadetector-v6", [
            {"box": {"x": .06, "y": .12, "w": .32, "h": .76}, "confidence": .9, "category": "animal"},
            {"box": {"x": .62, "y": .12, "w": .32, "h": .76}, "confidence": .95, "category": "animal"},
        ])
        for did, species in zip(ids, (TARGET, OTHER), strict=True):
            db.predictions.add(did, species, .95, "test", status="accepted")
        # Deliberately poison every legacy projection with the wrong bird.
        db.subjects.set_choice(photo_id, ids[1])
        db.subjects.set_primary_state(photo_id, ids[1])
        db.conn.execute("UPDATE photos SET subject_tenengrad=?, bg_tenengrad=1, eye_x=.8, eye_y=.5, eye_tenengrad=? WHERE id=?",
                        (100000 - index * 10000, 100000, photo_id))
        detections[photo_id] = ids
        photo_ids.append(photo_id)
    db.commit()

    def mask_for_box(image, box, variant):
        mask = np.zeros((image.height, image.width), bool)
        x, y, w, h = (box[k] for k in "xywh")
        mask[int(y * image.height):int((y + h) * image.height),
             int(x * image.width):int((x + w) * image.width)] = True
        return mask

    monkeypatch.setattr("masking.generate_mask", mask_for_box)
    return app, db, photo_ids, detections


def _analyze(db, photo_ids, comparison, config=None):
    effective = db.get_effective_config(config or {})
    for pid in photo_ids:
        photo = db.photos.get(pid)
        detection, _ = resolve_subject(db, pid, comparison)
        analyze_detection(db, photo, detection, effective, lambda: None)


def _pack(app, photo_ids, **context):
    response = app.test_client().post("/api/v1/photos/burst-evidence", json={"photo_ids": photo_ids, **context},
        headers={"X-Vireo-Token": app.config["API_TOKEN"]})
    assert response.status_code == 200, response.get_json()
    return response.get_json()


def test_sharp_other_species_never_promotes_soft_target(comparison_library):
    app, db, pids, detections = comparison_library
    import config as cfg
    _analyze(db, pids, SubjectComparison(TARGET), cfg.load())
    pack = _pack(app, pids, requested_species=TARGET)
    assert pack["scored"]
    assert pack["best_photo_id"] == pids[2], [(c["id"], c["sharpness"], c["reject_reasons"]) for c in pack["cards"]]
    soft = next(c for c in pack["cards"] if c["id"] == pids[0])
    assert soft["rank"] == 3 and soft["sharpness"] == 0
    assert soft["label"] == "REJECT" and soft["id"] in pack["suggested_reject_ids"]
    assert any(reason.startswith("out_of_focus") for reason in soft["reject_reasons"])
    for card in pack["cards"]:
        did = detections[card["id"]][0]
        assert card["subject"]["detection_id"] == did
        assert card["subject"]["selection"] == "confirmed"
        assert card["eye_x"] is None and card["eye_tenengrad"] is None
        assert all(f"detection_id={did}" in card[k] for k in ("crop_url", "focus_crop_url", "mask_url", "focus_crop_mask_url"))
        assert db.subjects.choice(card["id"])["detection_id"] == detections[card["id"]][1]


def test_missing_analysis_is_review_and_does_not_enter_percentiles(comparison_library):
    app, db, pids, _ = comparison_library
    pack = _pack(app, pids, requested_species=TARGET)
    assert not pack["scored"] and pack["best_photo_id"] is None
    assert pack["suggested_reject_ids"] == []
    assert pack["coverage"]["with_quality"] == 0
    assert all(c["rank"] is None and c["quality"] is None and not c["has_eye"] for c in pack["cards"])
    assert "subject_analysis_missing" in {r["code"] for r in pack["ambiguity"]["reasons"]}
    import config as cfg
    _analyze(db, pids[1:], SubjectComparison(TARGET), cfg.load())
    partial = _pack(app, pids, requested_species=TARGET)
    complete = _pack(app, pids[1:], requested_species=TARGET)
    assert [(c["id"], c["quality"]) for c in partial["cards"] if c["rank"]] == [(c["id"], c["quality"]) for c in complete["cards"]]
    assert next(c for c in partial["cards"] if c["id"] == pids[0])["rank"] is None


def test_same_species_subjects_require_explicit_choice(comparison_library):
    app, db, pids, ids = comparison_library
    db.predictions.add(ids[pids[0]][1], TARGET, .9, "another-model", status="accepted")
    # Remove its old accepted sparrow identity: now genuinely two target birds.
    row = db.conn.execute("SELECT id FROM predictions WHERE detection_id=? AND species=?", (ids[pids[0]][1], OTHER)).fetchone()
    db.update_prediction_status(row["id"], "rejected")
    pack = _pack(app, pids, requested_species=TARGET)
    card = next(c for c in pack["cards"] if c["id"] == pids[0])
    assert card["evidence_status"] == "multiple_matching_subjects" and card["subject"]["detection_id"] is None
    selected = _pack(app, pids, requested_species=TARGET, subject_overrides={str(pids[0]): ids[pids[0]][0]})
    card = next(c for c in selected["cards"] if c["id"] == pids[0])
    assert card["subject"]["detection_id"] == ids[pids[0]][0] and card["subject"]["selection"] == "selected"


def test_photo_keyword_and_pending_top_prediction_do_not_confirm_subject(comparison_library):
    app, db, pids, ids = comparison_library
    for row in db.conn.execute("SELECT id FROM predictions WHERE detection_id=?", (ids[pids[0]][0],)).fetchall():
        db.update_prediction_status(row["id"], "pending")
    keyword = db.add_keyword(TARGET, is_species=True)
    db.tag_photo(pids[0], keyword)
    pack = _pack(app, pids, requested_species=TARGET)
    card = next(c for c in pack["cards"] if c["id"] == pids[0])
    assert card["evidence_status"] == "subject_identity_uncertain"
    assert card["subject"]["detection_id"] is None and card["crop_url"] is None
    assert len(card["subject"]["candidates"]) == 2


def test_conflicting_override_is_review_and_foreign_detection_is_rejected(comparison_library):
    app, _, pids, ids = comparison_library
    pack = _pack(app, pids, requested_species=TARGET, subject_overrides={str(pids[0]): ids[pids[0]][1]})
    card = next(c for c in pack["cards"] if c["id"] == pids[0])
    assert card["evidence_status"] == "subject_identity_conflict" and card["rank"] is None
    response = app.test_client().post("/api/photos/burst-evidence", json={"photo_ids": pids,
        "subject_overrides": {str(pids[0]): ids[pids[1]][0]}})
    assert response.status_code == 400


def test_single_subject_frames_and_existing_decisions(comparison_library):
    app, db, pids, ids = comparison_library
    for pid in pids:
        detection_store.delete_detections_by_ids(db, [ids[pid][1]])
    db.conn.execute("UPDATE photos SET flag='flagged', rating=5 WHERE id=?", (pids[0],))
    db.conn.execute("UPDATE photos SET flag='rejected', rating=1 WHERE id=?", (pids[1],))
    db.commit()
    import config as cfg
    _analyze(db, pids, SubjectComparison(TARGET), cfg.load())
    pack = _pack(app, pids, requested_species=TARGET)
    assert all(c["subject"]["selection"] == "confirmed" and c["detection_count"] == 1 for c in pack["cards"])
    assert [(db.photos.get(pid)["flag"], db.photos.get(pid)["rating"]) for pid in pids[:2]] == [("flagged", 5), ("rejected", 1)]
    assert not set(pids[:2]) & set(pack["suggested_reject_ids"])


def test_selected_mask_focus_and_measurements_share_generation(comparison_library, monkeypatch):
    app, db, pids, ids = comparison_library
    import config as cfg
    _analyze(db, pids, SubjectComparison(TARGET), cfg.load())
    pack = _pack(app, pids, requested_species=TARGET)
    card = pack["cards"][0]
    # Observe geometry/selection independently of disk decode plumbing.
    monkeypatch.setattr("web.media._focus_source_image", lambda *args: (Image.new("RGB", (320, 160), "white"), "original"))
    client = app.test_client()
    mask = client.get(card["mask_url"])
    assert mask.status_code == 200
    pixels = np.asarray(Image.open(io.BytesIO(mask.data)))
    assert pixels[80, 70] == 255 and pixels[80, 240] == 0
    focus = client.get(card["focus_crop_mask_url"])
    assert focus.status_code == 200
    assert focus.headers["X-Vireo-Detection-Id"] == str(ids[card["id"]][0])
    assert focus.headers["X-Vireo-Focus-Basis"] == "subject"
    # Foreign detection ids and a different generation cannot borrow the primary mask.
    assert client.get(card["mask_url"].replace(card["subject"]["analysis_key"], "wrong")).status_code == 404
    assert client.get(card["focus_crop_url"].replace(f"detection_id={ids[card['id']][0]}", f"detection_id={ids[pids[0]][1]}&unused=1")).status_code == 404


def test_source_and_configuration_changes_invalidate_analysis(comparison_library):
    app, db, pids, _ = comparison_library
    import config as cfg
    _analyze(db, pids, SubjectComparison(TARGET), cfg.load())
    before = _pack(app, pids, requested_species=TARGET)
    old = next(c for c in before["cards"] if c["id"] == pids[0])
    from subject_ranking import source_path
    path = source_path(db, db.photos.get(pids[0]))
    Image.new("RGB", (320, 160), "black").save(path)
    after = _pack(app, pids, requested_species=TARGET)
    assert next(c for c in after["cards"] if c["id"] == pids[0])["evidence_status"] == "subject_analysis_missing"
    assert app.test_client().get(old["mask_url"]).status_code == 404
    from subject_ranking import cached_analysis
    photo = db.photos.get(pids[1])
    detection, _ = resolve_subject(db, pids[1], SubjectComparison(TARGET))
    effective = db.get_effective_config(cfg.load())
    changed = {**effective, "pipeline": {**effective.get("pipeline", {}), "eye_detect_enabled": True}}
    assert cached_analysis(db, photo, detection, changed)[1] is None


def test_eye_measurements_are_selected_and_mask_gated(comparison_library, monkeypatch):
    _, db, pids, _ = comparison_library
    import keypoints as kp
    import pipeline
    from subject_analysis import measure_eye
    detection, _ = resolve_subject(db, pids[0], SubjectComparison(TARGET))
    monkeypatch.setattr(pipeline, "_resolve_keypoint_model", lambda *args: "superanimal-bird")
    monkeypatch.setattr("subject_analysis.os.path.isfile", lambda *args: True)
    monkeypatch.setattr(kp, "detect_keypoints", lambda *args: [
        {"name": "left_eye", "x": 70, "y": 80, "conf": .9},
        {"name": "right_eye", "x": 240, "y": 80, "conf": .99}])
    image = Image.new("RGB", (320, 160), "white")
    mask = np.zeros((160, 320), bool)
    mask[:, :120] = True
    result = measure_eye(db, pids[0], detection, image, mask, {"pipeline": {"eye_detect_enabled": True}})
    assert result["eye_x"] == 70 / 320 and result["eye_conf"] == .9


def test_analysis_cancellation_and_deleted_detection_publish_nothing(comparison_library):
    _, db, pids, _ = comparison_library
    from web.background_jobs import JobCancelled
    photo = db.photos.get(pids[0])
    detection, _ = resolve_subject(db, pids[0], SubjectComparison(TARGET))
    def cancel():
        raise JobCancelled("cancelled")
    with pytest.raises(JobCancelled):
        analyze_detection(db, photo, detection, {}, cancel)
    assert db.conn.execute("SELECT COUNT(*) FROM subject_ranking_analysis").fetchone()[0] == 0
    detection_store.delete_detections_by_ids(db, [detection["id"]])
    db.subjects.save_ranking_analysis(detection["id"], "late", {}, b"png")
    assert db.conn.execute("SELECT COUNT(*) FROM subject_ranking_analysis").fetchone()[0] == 0


@pytest.mark.parametrize("body", [{"requested_species": ""}, {"requested_species": []},
    {"subject_overrides": []}, {"subject_overrides": {"1": True}}, {"subject_overrides": {"bad": 1}},
    {"subject_overrides": {"1": "2"}}, {"subject_overrides": "bad"}])
def test_comparison_validation(body):
    with pytest.raises(ValueError):
        parse_comparison(body)


def test_background_analysis_is_scoped_and_reports_unresolved_frames(comparison_library):
    app, db, pids, ids = comparison_library
    from vireo.tests.wait import wait_for_job_via_client
    # An unconfirmed bird must not silently analyze its sharp neighbour.
    row = db.conn.execute("SELECT id FROM predictions WHERE detection_id=?", (ids[pids[0]][0],)).fetchone()
    db.update_prediction_status(row["id"], "pending")
    client = app.test_client()
    launched = _pack(app, pids, requested_species=TARGET, analyze=True)
    job = wait_for_job_via_client(client, launched["job_id"], wait_for_history=True)
    assert job["status"] == "completed"
    assert job["result"]["analyzed"] == 2
    assert job["result"]["unresolved"] == [{"photo_id": pids[0], "reason": "subject_identity_uncertain"}]
    assert job["result"]["errors"] == []
    assert job["progress"]["current"] == 3
    pack = _pack(app, pids, requested_species=TARGET)
    assert pack["best_photo_id"] == pids[2]
    assert db.subjects.choice(pids[0])["detection_id"] == ids[pids[0]][1]


def test_background_analysis_surfaces_decode_errors(comparison_library, monkeypatch):
    app, _, pids, _ = comparison_library
    from vireo.tests.wait import wait_for_job_via_client
    monkeypatch.setattr("image_loader.load_image", lambda *args, **kwargs: None)
    launched = _pack(app, pids, requested_species=TARGET, analyze=True)
    job = wait_for_job_via_client(app.test_client(), launched["job_id"])
    assert len(job["result"]["errors"]) == len(pids)
    assert all("Could not decode" in e["error"] for e in job["result"]["errors"])
    assert not _pack(app, pids, requested_species=TARGET)["scored"]


def test_all_ranking_entrypoints_preserve_subject_context(comparison_library):
    app, db, pids, _ = comparison_library
    import config as cfg
    _analyze(db, pids, SubjectComparison(TARGET), cfg.load())
    client = app.test_client()
    for endpoint in ("best-batch", "burst-evidence"):
        get = client.get(f"/api/photos/{pids[0]}/{endpoint}", query_string={"requested_species": TARGET})
        post = client.post(f"/api/photos/{endpoint}", json={"photo_ids": pids, "requested_species": TARGET})
        assert get.status_code == post.status_code == 200
        assert get.get_json()["best_photo_id"] == post.get_json()["best_photo_id"] == pids[2]
        assert get.get_json()["comparison"] == post.get_json()["comparison"]


def test_classification_change_during_inference_does_not_publish_stale_eyes(comparison_library, monkeypatch):
    _, db, pids, ids = comparison_library
    photo = db.photos.get(pids[0])
    detection, _ = resolve_subject(db, pids[0], SubjectComparison(TARGET))
    def reclassify(*args):
        db.predictions.add(ids[pids[0]][0], "Another species", .99, "replacement", status="accepted")
        return {"eye_x": None, "eye_y": None, "eye_conf": None, "eye_tenengrad": None}
    monkeypatch.setattr("subject_analysis.measure_eye", reclassify)
    with pytest.raises(ValueError, match="inputs changed"):
        analyze_detection(db, photo, detection, {}, lambda: None)
    assert db.conn.execute("SELECT COUNT(*) FROM subject_ranking_analysis").fetchone()[0] == 0


def test_analysis_generations_are_immutable_and_cascade_with_detection(comparison_library):
    _, db, pids, ids = comparison_library
    did = ids[pids[0]][0]
    db.subjects.save_ranking_analysis(did, "generation", {"eye_x": .2}, b"first")
    db.subjects.save_ranking_analysis(did, "generation", {"eye_x": .8}, b"replacement")
    assert db.subjects.ranking_analysis(did, "generation") == {"features": {"eye_x": .2}, "mask_png": b"first"}
    detection_store.delete_detections_by_ids(db, [did])
    assert db.subjects.ranking_analysis(did, "generation") is None


def test_focus_refuses_source_replaced_while_decoding(comparison_library, monkeypatch):
    app, db, pids, _ = comparison_library
    import config as cfg
    from subject_ranking import source_path
    _analyze(db, pids, SubjectComparison(TARGET), cfg.load())
    card = _pack(app, pids, requested_species=TARGET)["cards"][0]
    def replacing_decoder(*args):
        Image.new("RGB", (320, 160), "black").save(source_path(db, db.photos.get(card["id"])))
        return Image.new("RGB", (320, 160), "black"), "original"
    monkeypatch.setattr("web.media._focus_source_image", replacing_decoder)
    response = app.test_client().get(card["focus_crop_mask_url"])
    assert response.status_code == 404
    assert "stale" in response.get_json()["error"]


@pytest.mark.parametrize("clean_candidate", [False, True])
def test_mixed_species_confirmations_block_automatic_subject_selection(comparison_library, clean_candidate):
    app, db, pids, ids = comparison_library
    # The right bird carries conflicting accepted identities, one of them the target.
    db.predictions.add(ids[pids[0]][1], TARGET, .9, "conflicting-model", status="accepted")
    if not clean_candidate:
        row = db.conn.execute("SELECT id FROM predictions WHERE detection_id=?", (ids[pids[0]][0],)).fetchone()
        db.update_prediction_status(row["id"], "pending")
    pack = _pack(app, pids, requested_species=TARGET)
    card = next(c for c in pack["cards"] if c["id"] == pids[0])
    assert card["evidence_status"] == "subject_identity_conflict"
    assert card["subject"]["detection_id"] is None
    assert card["rank"] is None and card["quality"] is None
    assert card["id"] not in pack["suggested_reject_ids"]
    assert "subject_identity_conflict" in {r["code"] for r in pack["ambiguity"]["reasons"]}
    # An explicit choice of the non-conflicting subject still resolves the intended animal.
    selected = _pack(app, pids, requested_species=TARGET, subject_overrides={str(pids[0]): ids[pids[0]][0]})
    card = next(c for c in selected["cards"] if c["id"] == pids[0])
    assert card["subject"]["detection_id"] == ids[pids[0]][0]


@pytest.mark.parametrize("taxon_id", ["0", "-1", "not-a-number", str(2**63), "999999999999999999999999"])
def test_invalid_explicit_taxon_ids_return_400_for_all_comparison_routes(comparison_library, taxon_id):
    app, _, pids, _ = comparison_library
    client = app.test_client()
    for endpoint in ("best-batch", "burst-evidence"):
        get = client.get(f"/api/photos/{pids[0]}/{endpoint}", query_string={"requested_species": "taxon:" + taxon_id})
        post = client.post(f"/api/photos/{endpoint}", json={"photo_ids": pids, "requested_species": "taxon:" + taxon_id})
        assert get.status_code == post.status_code == 400
        assert "taxon id" in get.get_json()["error"]
        assert "taxon id" in post.get_json()["error"]


def test_largest_supported_explicit_taxon_id_is_valid(comparison_library):
    app, _, pids, _ = comparison_library
    pack = _pack(app, pids, requested_species=f"taxon:{2**63 - 1}")
    assert not pack["scored"]
    assert all(c["subject"]["requested_species_key"] == f"taxon:{2**63 - 1}" for c in pack["cards"])


@pytest.mark.parametrize("selection", ["primary", "explicit"])
def test_subject_only_comparisons_report_conflicting_confirmations(comparison_library, selection):
    app, db, pids, ids = comparison_library
    # The default primary is on the right; the explicit choice below is on the left.
    did = ids[pids[0]][1 if selection == "primary" else 0]
    db.predictions.add(did, "Mule Deer", .9, "conflicting-mammal-model", status="accepted")
    context = {"compare_subjects": True}
    if selection == "explicit":
        context["subject_overrides"] = {str(pids[0]): did}
    pack = _pack(app, pids, **context)
    card = next(c for c in pack["cards"] if c["id"] == pids[0])
    assert card["evidence_status"] == "subject_identity_conflict"
    assert card["subject"]["detection_id"] is None
    assert card["subject"]["analysis_key"] is None
    assert card["rank"] is None and card["quality"] is None
    assert card["eye_tenengrad"] is None
    assert card["subject"]["requested_species_key"] is None


def test_background_analysis_continues_after_selected_detection_disappears(comparison_library, monkeypatch):
    app, _, pids, ids = comparison_library
    import subject_analysis

    from vireo.tests.wait import wait_for_job_via_client
    original = subject_analysis.resolve_subject
    removed = False
    def remove_before_resolving(db, photo_id, comparison):
        nonlocal removed
        if photo_id == pids[0] and not removed:
            # Request validation already succeeded; the worker's selection is now stale.
            detection_store.delete_detections_by_ids(db, [ids[photo_id][0]])
            db.commit()
            removed = True
        return original(db, photo_id, comparison)
    monkeypatch.setattr(subject_analysis, "resolve_subject", remove_before_resolving)
    launched = _pack(app, pids, requested_species=TARGET,
                    subject_overrides={str(pids[0]): ids[pids[0]][0]}, analyze=True)
    job = wait_for_job_via_client(app.test_client(), launched["job_id"], wait_for_history=True)
    assert job["status"] == "completed"
    assert job["result"]["analyzed"] == 2
    assert job["result"]["unresolved"] == []
    assert len(job["result"]["errors"]) == 1
    assert job["result"]["errors"][0]["photo_id"] == pids[0]
    assert "not a retained animal" in job["result"]["errors"][0]["error"]
    assert job["progress"]["current"] == len(pids)



def test_previously_returned_media_rejects_a_new_identity_conflict(comparison_library, monkeypatch):
    app, db, pids, _ = comparison_library
    import config as cfg
    _analyze(db, pids, SubjectComparison(TARGET), cfg.load())
    card = _pack(app, pids, requested_species=TARGET)["cards"][0]
    db.predictions.add(card["subject"]["detection_id"], "Mule Deer", .9, "conflicting-model", status="accepted")
    def never_decode(*args):
        raise AssertionError("Conflicted subject must be rejected before loading pixels")
    monkeypatch.setattr("web.media._focus_source_image", never_decode)
    client = app.test_client()
    for url in (card["focus_crop_url"], card["focus_crop_mask_url"], card["mask_url"]):
        response = client.get(url)
        assert response.status_code == 404
        assert "identity is unresolved" in response.get_json()["error"]



def test_publishing_a_new_analysis_prunes_only_that_detections_old_generations(comparison_library):
    _, db, pids, ids = comparison_library
    did, other = ids[pids[0]]
    db.subjects.save_ranking_analysis(other, "other-subject", {"eye_x": .8}, b"other")
    for index in range(5):
        key = f"generation-{index}"
        db.subjects.save_ranking_analysis(did, key, {"eye_x": index / 10}, b"latest")
        db.commit()
        assert db.conn.execute("SELECT COUNT(*) FROM subject_ranking_analysis WHERE detection_id=?", (did,)).fetchone()[0] == 1
        assert db.subjects.ranking_analysis(did, key)["features"]["eye_x"] == index / 10
        if index:
            assert db.subjects.ranking_analysis(did, f"generation-{index - 1}") is None
    assert db.subjects.ranking_analysis(other, "other-subject")["mask_png"] == b"other"


def test_new_analysis_insert_and_pruning_roll_back_together(comparison_library):
    _, db, pids, ids = comparison_library
    did = ids[pids[0]][0]
    db.subjects.save_ranking_analysis(did, "first", {"eye_x": .2}, b"first")
    db.commit()
    db.subjects.save_ranking_analysis(did, "second", {"eye_x": .8}, b"second")
    assert db.subjects.ranking_analysis(did, "first") is None
    db.rollback()
    assert db.subjects.ranking_analysis(did, "second") is None
    assert db.subjects.ranking_analysis(did, "first")["mask_png"] == b"first"

def test_explicit_subject_can_recover_a_weak_detection_without_changing_primary(comparison_library):
    app, db, pids, ids = comparison_library
    pid, did = pids[0], ids[pids[0]][0]
    db.conn.execute('UPDATE detections SET detector_confidence=.052 WHERE id=?', (did,))
    db.commit()
    automatic = _pack(app, pids, requested_species=TARGET)
    card = next(c for c in automatic['cards'] if c['id'] == pid)
    assert card['subject']['detection_id'] is None
    assert any(c['detection_id'] == did for c in card['subject']['candidates'])
    explicit = _pack(app, pids, requested_species=TARGET, subject_overrides={str(pid): did})
    card = next(c for c in explicit['cards'] if c['id'] == pid)
    assert card['subject']['detection_id'] == did
    assert card['evidence_status'] == 'subject_analysis_missing'
    assert db.subjects.choice(pid)['detection_id'] == ids[pid][1]
