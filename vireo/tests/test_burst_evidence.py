"""Burst evidence packs: chaining, coverage, ambiguity, and the routes that serve them."""

import os

from burst_evidence import (
    AMBIGUITY_CODES,
    DEFAULT_CLOSE_CALL_MARGIN,
    assess_ambiguity,
    coverage_counts,
    group_folder_bursts,
)
from PIL import Image
from services import photo_ingest


def _row(pid, filename, timestamp=None, extension=None, flag="none"):
    return {
        "id": pid,
        "filename": filename,
        "extension": extension,
        "timestamp": timestamp,
        "flag": flag,
        "rating": 0,
    }


# -- group_folder_bursts --------------------------------------------------------


def test_group_folder_bursts_chains_filename_sequences_and_splits_on_gaps():
    rows = [
        _row(1, "DSC_0001.NEF", "2024-03-01T12:00:00"),
        _row(2, "DSC_0002.NEF", "2024-03-01T12:00:01"),
        _row(3, "DSC_0003.NEF", "2024-03-01T12:00:02"),
        # Sequence gap of 3 > 2 starts a new burst.
        _row(4, "DSC_0006.NEF", "2024-03-01T12:00:03"),
        _row(5, "DSC_0007.NEF", "2024-03-01T12:00:04"),
        # Time gap of a minute splits even adjacent numbers.
        _row(6, "DSC_0008.NEF", "2024-03-01T12:01:30"),
        _row(7, "DSC_0009.NEF", "2024-03-01T12:01:31"),
        # A lone frame is not a burst.
        _row(8, "DSC_0020.NEF", "2024-03-01T13:00:00"),
    ]
    bursts = group_folder_bursts(rows)
    assert [[r["id"] for r in b] for b in bursts] == [[1, 2, 3], [4, 5], [6, 7]]


def test_group_folder_bursts_keeps_extensions_and_prefixes_apart():
    rows = [
        _row(1, "DSC_0001.NEF", "2024-03-01T12:00:00"),
        _row(2, "DSC_0001.JPG", "2024-03-01T12:00:00"),
        _row(3, "DSC_0002.NEF", "2024-03-01T12:00:01"),
        _row(4, "DSC_0002.JPG", "2024-03-01T12:00:01"),
        _row(5, "IMG_0003.NEF", "2024-03-01T12:00:02"),
        _row(6, "IMG_0004.NEF", "2024-03-01T12:00:03"),
    ]
    bursts = group_folder_bursts(rows)
    assert sorted([r["id"] for r in b] for b in bursts) == [[1, 3], [2, 4], [5, 6]]


def test_group_folder_bursts_without_timestamps_chains_by_sequence_only():
    rows = [_row(1, "DSC_0001.NEF"), _row(2, "DSC_0002.NEF"), _row(3, "DSC_0004.NEF")]
    assert [[r["id"] for r in b] for b in group_folder_bursts(rows)] == [[1, 2, 3]]


def test_group_folder_bursts_unnumbered_names_need_timestamps():
    rows = [
        _row(1, "alpha.jpg", "2024-03-01T12:00:00"),
        _row(2, "beta.jpg", "2024-03-01T12:00:03"),
        _row(3, "gamma.jpg", None),
        _row(4, "delta.jpg", None),
        _row(5, "zeta.jpg", "2024-03-01T15:00:00"),
    ]
    assert [[r["id"] for r in b] for b in group_folder_bursts(rows)] == [[1, 2]]


# -- coverage_counts -----------------------------------------------------------


def _evidence(pid, *, mask=0, quality=0, eye=0, dets=0, crop_complete=None, subject_size=None):
    return {
        "id": pid, "has_mask": mask, "has_quality": quality, "has_eye": eye,
        "detection_count": dets, "crop_complete": crop_complete, "subject_size": subject_size,
    }


def test_coverage_counts_each_feature_separately():
    rows = [
        _evidence(1, mask=1, quality=1, eye=1, dets=1),
        _evidence(2, mask=1, quality=1, dets=2),
        _evidence(3, dets=1),
        _evidence(4),
    ]
    assert coverage_counts(rows) == {
        "photos": 4, "with_detection": 3, "with_mask": 2, "with_quality": 2, "with_eye": 1,
    }


# -- assess_ambiguity ----------------------------------------------------------


def _card(pid, sharpness, *, label=None, focus_basis="subject", crop_complete=1.0, dets=1,
          eye_tenengrad=None):
    return {
        "id": pid, "filename": f"DSC_{pid:04d}.NEF", "sharpness": sharpness, "label": label,
        "eye_tenengrad": eye_tenengrad, "focus_basis": focus_basis,
        "crop_complete": crop_complete, "detection_count": dets,
    }


def _full_coverage(n):
    return {"photos": n, "with_detection": n, "with_mask": n, "with_quality": n, "with_eye": 0}


def test_assess_ambiguity_clear_burst_has_no_reasons():
    cards = [_card(1, 80), _card(2, 60), _card(3, 30, label="REJECT")]
    verdict = assess_ambiguity(cards, _full_coverage(3), eye_detect_enabled=False)
    assert verdict == {"needs_review": False, "reasons": []}


def test_assess_ambiguity_close_call_compares_raw_sharpness_of_non_rejected_cards():
    cards = [_card(1, 80), _card(2, 78), _card(3, 30, label="REJECT")]
    verdict = assess_ambiguity(cards, _full_coverage(3), eye_detect_enabled=False)
    assert [r["code"] for r in verdict["reasons"]] == ["close_call"]
    detail = verdict["reasons"][0]["detail"]
    assert "DSC_0001.NEF" in detail and "DSC_0002.NEF" in detail
    assert "subject sharpness" in detail and "80 vs 78" in detail
    # A rejected runner-up never makes a close call.
    cards = [_card(1, 80), _card(2, 79, label="REJECT")]
    assert assess_ambiguity(cards, _full_coverage(2), eye_detect_enabled=False)["needs_review"] is False
    # The margin is the caller's: 70 is 12.5% below 80.
    cards = [_card(1, 80), _card(2, 70)]
    assert assess_ambiguity(cards, _full_coverage(2), eye_detect_enabled=False)["needs_review"] is False
    assert assess_ambiguity(cards, _full_coverage(2), eye_detect_enabled=False, margin=0.2)["needs_review"]


def test_assess_ambiguity_close_call_uses_eye_sharpness_when_both_frames_have_it():
    cards = [
        _card(1, 90, focus_basis="eye", eye_tenengrad=40),
        _card(2, 60, focus_basis="eye", eye_tenengrad=39),
    ]
    verdict = assess_ambiguity(cards, _full_coverage(2), eye_detect_enabled=True)
    assert [r["code"] for r in verdict["reasons"]] == ["close_call"]
    assert "eye sharpness" in verdict["reasons"][0]["detail"]
    # One eye frame and one body frame fall back to subject sharpness.
    cards = [_card(1, 90, focus_basis="eye", eye_tenengrad=40), _card(2, 60, eye_tenengrad=39)]
    verdict = assess_ambiguity(cards, _full_coverage(2), eye_detect_enabled=False)
    assert verdict["needs_review"] is False


def test_assess_ambiguity_reports_unanalyzed_frames():
    cards = [_card(1, 80), _card(2, 50)]
    coverage = {"photos": 5, "with_detection": 5, "with_mask": 2, "with_quality": 2, "with_eye": 0}
    verdict = assess_ambiguity(cards, coverage, eye_detect_enabled=False)
    assert [r["code"] for r in verdict["reasons"]] == ["unanalyzed_frames"]
    assert "3 of 5" in verdict["reasons"][0]["detail"]


def test_assess_ambiguity_no_eye_only_when_eye_detection_is_on():
    cards = [_card(1, 80, focus_basis="subject"), _card(2, 50)]
    off = assess_ambiguity(cards, _full_coverage(2), eye_detect_enabled=False)
    assert off["needs_review"] is False
    on = assess_ambiguity(cards, _full_coverage(2), eye_detect_enabled=True)
    assert [r["code"] for r in on["reasons"]] == ["no_eye_located"]
    eye_cards = [_card(1, 80, focus_basis="eye", eye_tenengrad=30), _card(2, 50)]
    assert assess_ambiguity(eye_cards, _full_coverage(2), eye_detect_enabled=True)["needs_review"] is False


def test_assess_ambiguity_edge_and_multiple_subjects_look_at_the_best_frame():
    cards = [_card(1, 80, crop_complete=0.7, dets=3), _card(2, 50, crop_complete=0.5, dets=4)]
    verdict = assess_ambiguity(cards, _full_coverage(2), eye_detect_enabled=False)
    assert [r["code"] for r in verdict["reasons"]] == ["subject_touches_edge", "multiple_subjects"]
    assert "DSC_0001.NEF" in verdict["reasons"][0]["detail"]
    assert "3 animal detections" in verdict["reasons"][1]["detail"]


def test_assess_ambiguity_all_rejected():
    cards = [_card(1, 20, label="REJECT"), _card(2, 10, label="REJECT")]
    verdict = assess_ambiguity(cards, _full_coverage(2), eye_detect_enabled=False)
    assert "all_rejected" in [r["code"] for r in verdict["reasons"]]


def test_ambiguity_codes_are_the_documented_set():
    assert set(AMBIGUITY_CODES) == {
        "unanalyzed_frames", "close_call", "no_eye_located",
        "subject_touches_edge", "multiple_subjects", "all_rejected", "subject_identity_uncertain", "subject_identity_conflict",
        "multiple_matching_subjects", "subject_analysis_missing",
    }
    assert DEFAULT_CLOSE_CALL_MARGIN == 0.10


# -- routes --------------------------------------------------------------------


def _mark_quality(db, photo_id, sharpness, *, crop_complete=1.0, eye=None):
    db.conn.execute(
        """UPDATE photos
           SET mask_path = 'mask.png',
               subject_tenengrad = ?,
               bg_tenengrad = 5,
               crop_complete = ?,
               bg_separation = 0,
               subject_clip_high = 0,
               subject_clip_low = 0,
               subject_y_median = 115,
               subject_size = 0.05,
               noise_estimate = 1
           WHERE id = ?""",
        (sharpness, crop_complete, photo_id),
    )
    if eye is not None:
        db.conn.execute(
            "UPDATE photos SET eye_x = ?, eye_y = ?, eye_conf = 0.9, eye_tenengrad = ? WHERE id = ?",
            (eye[0], eye[1], sharpness, photo_id),
        )


def _seed_burst(db, folder_id, start, sharpness_by_frame, *, prefix="DSC_", ts_base="2024-03-01T12:00:"):
    pids = []
    for offset, sharp in enumerate(sharpness_by_frame):
        idx = start + offset
        pid = photo_ingest.add_photo(
            db, folder_id=folder_id,
            filename=f"{prefix}{idx:04d}.jpg",
            extension=".jpg",
            file_size=1000 + idx,
            file_mtime=float(idx),
            timestamp=f"{ts_base}{offset:02d}",
        )
        if sharp is not None:
            _mark_quality(db, pid, sharp)
        pids.append(pid)
    db.conn.commit()
    return pids


def _folder_id(db, path="/photos/2024"):
    return db.conn.execute("SELECT id FROM folders WHERE path = ?", (path,)).fetchone()["id"]


def test_burst_evidence_get_wraps_best_batch_with_coverage_and_urls(app_and_db):
    app, db = app_and_db
    folder_id = _folder_id(db)
    pids = _seed_burst(db, folder_id, 3069, [10, 80, 30])
    db.detections.save(pids[1], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0.6, "y": 0.6, "w": 0.2, "h": 0.2}, "confidence": 0.8, "category": "animal"},
    ], detector_model="MDV6")

    client = app.test_client()
    resp = client.get(f"/api/photos/{pids[1]}/burst-evidence")
    assert resp.status_code == 200
    pack = resp.get_json()

    assert pack["scope_method"] == "filename_sequence"
    assert pack["scored"] is True
    assert pack["photo_ids"] == pids
    assert pack["best_photo_id"] == pids[1]
    assert pack["coverage"] == {
        "photos": 3, "with_detection": 1, "with_mask": 3, "with_quality": 3, "with_eye": 0,
    }
    assert {key: pack["scoring"][key] for key in ("eye_detect_enabled", "close_call_margin", "detector_confidence")} == {
        "eye_detect_enabled": False, "close_call_margin": 0.10, "detector_confidence": 0.2,
    }
    # The best frame carries two animal boxes, so the scorer may have scored
    # the wrong one.
    assert [r["code"] for r in pack["ambiguity"]["reasons"]] == ["multiple_subjects"]
    assert pack["ambiguity"]["needs_review"] is True

    best = next(c for c in pack["cards"] if c["id"] == pids[1])
    assert best["role"] == "best"
    assert best["detection_count"] == 2
    assert best["has_mask"] is True and best["has_quality"] is True and best["has_eye"] is False
    assert best["crop_complete"] == 1.0
    assert best["reject_reasons"] == []
    assert best["focus_crop_url"] == f"/photos/{pids[1]}/focus-crop?size=512"
    assert best["focus_crop_mask_url"] == f"/photos/{pids[1]}/focus-crop?size=512&mask=1"
    assert best["thumbnail_url"] == f"/thumbnails/{pids[1]}.jpg"
    assert best["preview_url"] == f"/photos/{pids[1]}/preview?size=1920"
    assert best["crop_url"] == f"/photos/{pids[1]}/crop"
    worst = next(c for c in pack["cards"] if c["id"] == pids[0])
    assert worst["label"] == "REJECT"
    assert worst["reject_reasons"]


def test_burst_evidence_validates_margin_and_focus_size(app_and_db):
    app, db = app_and_db
    pids = _seed_burst(db, _folder_id(db), 3069, [10, 80])
    client = app.test_client()
    assert client.get(f"/api/photos/{pids[0]}/burst-evidence?margin=1.5").status_code == 400
    assert client.get(f"/api/photos/{pids[0]}/burst-evidence?focus_size=64").status_code == 400
    resp = client.get(f"/api/photos/{pids[0]}/burst-evidence?margin=0.2&focus_size=256")
    assert resp.status_code == 200
    pack = resp.get_json()
    assert pack["scoring"]["close_call_margin"] == 0.2
    assert pack["cards"][0]["focus_crop_url"].endswith("focus-crop?size=256")


def test_burst_evidence_unscored_burst_still_reports_coverage(app_and_db):
    app, db = app_and_db
    pids = _seed_burst(db, _folder_id(db), 3069, [None, None, None])
    client = app.test_client()
    resp = client.get(f"/api/photos/{pids[0]}/burst-evidence")
    assert resp.status_code == 200
    pack = resp.get_json()
    assert pack["scored"] is False
    assert "pipeline" in pack["unscored_reason"].lower()
    assert pack["best_photo_id"] is None
    assert pack["coverage"]["photos"] == 3 and pack["coverage"]["with_quality"] == 0
    assert [r["code"] for r in pack["ambiguity"]["reasons"]] == ["unanalyzed_frames"]
    assert [c["id"] for c in pack["cards"]] == pids
    assert pack["cards"][0]["focus_crop_mask_url"] is None


def test_burst_evidence_post_uses_the_best_batch_selection_contract(app_and_db):
    app, db = app_and_db
    pids = _seed_burst(db, _folder_id(db), 3069, [10, 80, 30])
    client = app.test_client()
    assert client.post("/api/photos/burst-evidence", json={}).status_code == 400
    assert client.post("/api/photos/burst-evidence", json={"photo_ids": [pids[0]]}).status_code == 400
    assert client.post("/api/photos/burst-evidence", json={"photo_ids": ["x", 1]}).status_code == 400
    resp = client.post("/api/photos/burst-evidence", json={"photo_ids": pids})
    assert resp.status_code == 200
    pack = resp.get_json()
    assert pack["scope_method"] == "selected_photos"
    assert pack["best_photo_id"] == pids[1]
    assert pack["coverage"]["photos"] == 3


def test_burst_evidence_photo_outside_workspace_is_not_found(app_and_db):
    app, db = app_and_db
    pids = _seed_burst(db, _folder_id(db), 3069, [10, 80])
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    db.conn.execute(
        "UPDATE workspaces SET last_opened_at = datetime('now', '+1 hour') WHERE id = ?", (other,)
    )
    db.conn.commit()
    client = app.test_client()
    assert client.get(f"/api/photos/{pids[0]}/burst-evidence").status_code == 404


def test_folder_bursts_lists_every_burst_with_folder_coverage(app_and_db):
    app, db = app_and_db
    folder_id = _folder_id(db)
    sharp = _seed_burst(db, folder_id, 3069, [10, 80, 30])
    close = _seed_burst(db, folder_id, 3100, [50, 49], ts_base="2024-03-01T13:00:")
    partial = _seed_burst(db, folder_id, 3200, [60, None], ts_base="2024-03-01T14:00:")
    db.photo_review.set_flag(sharp[0], "rejected")

    client = app.test_client()
    resp = client.get(f"/api/folders/{folder_id}/bursts")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["folder"]["id"] == folder_id
    # The fixture's own bird photos are not sequence-named and have no bursts.
    existing = data["photo_count"] - 7
    assert existing >= 0
    assert data["coverage"]["photos"] == data["photo_count"]
    assert data["coverage"]["with_quality"] == 6
    assert data["burst_count"] == 3
    assert data["photos_in_bursts"] == 7
    assert data["ambiguous_burst_count"] == 2
    by_seed = {b["seed_photo_id"]: b for b in data["bursts"]}

    clear = by_seed[sharp[0]]
    assert clear["photo_ids"] == sharp
    assert clear["filenames"] == ["DSC_3069.jpg", "DSC_3071.jpg"]
    assert clear["sequence_range"] == [3069, 3071]
    assert clear["flags"] == {"none": 2, "flagged": 0, "rejected": 1}
    assert clear["scored"] is True
    assert clear["best_photo_id"] == sharp[1]
    assert clear["ambiguity"] == {"needs_review": False, "reasons": []}

    tie = by_seed[close[0]]
    assert tie["scored"] is True
    assert [r["code"] for r in tie["ambiguity"]["reasons"]] == ["close_call"]

    # One analyzed frame is enough for the scorer to rank the burst (the
    # unanalyzed frame fails the no-mask rule), so the listing must say the
    # ranking covers half the frames rather than hide it.
    half = by_seed[partial[0]]
    assert half["scored"] is True
    assert half["best_photo_id"] == partial[0]
    assert half["coverage"] == {
        "photos": 2, "with_detection": 0, "with_mask": 1, "with_quality": 1, "with_eye": 0,
    }
    assert [r["code"] for r in half["ambiguity"]["reasons"]] == ["unanalyzed_frames"]


def test_folder_bursts_rejects_hidden_folders_bad_margin_and_oversized_folders(app_and_db, monkeypatch):
    app, db = app_and_db
    folder_id = _folder_id(db)
    client = app.test_client()
    assert client.get("/api/folders/999999/bursts").status_code == 404
    assert client.get(f"/api/folders/{folder_id}/bursts?margin=-1").status_code == 400

    hidden = db.add_folder("/elsewhere", name="elsewhere", link_to_workspace=False)
    assert client.get(f"/api/folders/{hidden}/bursts").status_code == 404

    import burst_evidence
    monkeypatch.setattr(burst_evidence, "MAX_FOLDER_BURST_PHOTOS", 1)
    resp = client.get(f"/api/folders/{folder_id}/bursts")
    assert resp.status_code == 400
    assert "up to 1" in resp.get_json()["error"]


def _working_copy(app, db, pid, size=(3000, 2000), color=(40, 90, 160)):
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    img = Image.new("RGB", size, color=color)
    # A white square at the centre marks the eye so the crop's content can be checked.
    cx, cy = size[0] // 2, size[1] // 2
    for x in range(cx - 10, cx + 10):
        for y in range(cy - 10, cy + 10):
            img.putpixel((x, y), (255, 255, 255))
    path = os.path.join(working_dir, f"{pid}.jpg")
    img.save(path, "JPEG", quality=95)
    db.conn.execute(
        "UPDATE photos SET working_copy_path=?, width=?, height=? WHERE id=?",
        (f"working/{pid}.jpg", size[0], size[1], pid),
    )
    db.conn.commit()
    return path


def test_focus_crop_centres_on_the_eye_at_native_scale(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    _working_copy(app, db, pid)
    db.conn.execute(
        "UPDATE photos SET eye_x = 0.5, eye_y = 0.5, eye_conf = 0.9, eye_tenengrad = 40 WHERE id = ?",
        (pid,),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/photos/{pid}/focus-crop?size=256&basis=eye")
    assert resp.status_code == 200
    assert resp.content_type == "image/jpeg"
    assert resp.headers["X-Vireo-Focus-Basis"] == "eye"
    assert resp.headers["X-Vireo-Focus-Center"] == "0.5000,0.5000"
    assert resp.headers["X-Vireo-Focus-Scale"] == "1"
    assert resp.headers["X-Vireo-Focus-Mask"] == "0"
    assert resp.headers["X-Vireo-Focus-Window"] == "1372,872,1628,1128"
    import io
    img = Image.open(io.BytesIO(resp.data))
    assert img.size == (256, 256)
    # Native pixels: the 20px eye marker is still 20px wide in the crop.
    r, g, b = img.getpixel((128, 128))
    assert r > 200 and g > 200 and b > 200
    r, g, b = img.getpixel((128 + 30, 128))
    assert b > r


def test_focus_crop_falls_back_to_subject_then_frame(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    _working_copy(app, db, pid)
    client = app.test_client()

    resp = client.get(f"/photos/{pid}/focus-crop?size=128")
    assert resp.status_code == 200
    assert resp.headers["X-Vireo-Focus-Basis"] == "frame"
    assert client.get(f"/photos/{pid}/focus-crop?basis=eye").status_code == 404
    assert client.get(f"/photos/{pid}/focus-crop?basis=subject").status_code == 404

    db.detections.save(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.2, "h": 0.2}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    resp = client.get(f"/photos/{pid}/focus-crop?size=128")
    assert resp.headers["X-Vireo-Focus-Basis"] == "subject"
    assert resp.headers["X-Vireo-Focus-Center"] == "0.2000,0.3000"
    # Window clamps to the image: 0.2*3000-64 = 536, 0.3*2000-64 = 536.
    assert resp.headers["X-Vireo-Focus-Window"] == "536,536,664,664"


def test_focus_crop_validates_size_and_basis_and_workspace(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    _working_copy(app, db, pid)
    client = app.test_client()
    assert client.get(f"/photos/{pid}/focus-crop?size=12").status_code == 400
    assert client.get(f"/photos/{pid}/focus-crop?size=4096").status_code == 400
    assert client.get(f"/photos/{pid}/focus-crop?basis=nose").status_code == 400
    assert client.get("/photos/999999/focus-crop").status_code == 404

    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    db.conn.execute(
        "UPDATE workspaces SET last_opened_at = datetime('now', '+1 hour') WHERE id = ?", (other,)
    )
    db.conn.commit()
    assert client.get(f"/photos/{pid}/focus-crop").status_code == 404


def test_focus_crop_shades_outside_the_mask(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    _working_copy(app, db, pid, color=(200, 200, 200))
    masks_dir = os.path.join(os.path.dirname(app.config["DB_PATH"]), "masks")
    os.makedirs(masks_dir, exist_ok=True)
    # Pipeline-resolution mask (1024 long side): subject is the right half.
    mask = Image.new("L", (1024, 683), 0)
    for x in range(512, 1024):
        for y in range(683):
            mask.putpixel((x, y), 255)
    mask_path = os.path.join(masks_dir, f"{pid}.png")
    mask.save(mask_path)
    db.conn.execute("UPDATE photos SET mask_path = ? WHERE id = ?", (mask_path, pid))
    db.conn.commit()

    client = app.test_client()
    resp = client.get(f"/photos/{pid}/focus-crop?size=256&mask=1")
    assert resp.status_code == 200
    assert resp.headers["X-Vireo-Focus-Mask"] == "1"
    import io
    img = Image.open(io.BytesIO(resp.data))
    left = img.getpixel((10, 128))[0]
    right = img.getpixel((245, 128))[0]
    assert right > 180
    assert left < 110

    # A mask path outside the masks directory is never read.
    db.conn.execute("UPDATE photos SET mask_path = ? WHERE id = ?", ("/etc/hosts", pid))
    db.conn.commit()
    resp = client.get(f"/photos/{pid}/focus-crop?size=256&mask=1")
    assert resp.status_code == 200
    assert resp.headers["X-Vireo-Focus-Mask"] == "0"


def test_focus_crop_falls_back_to_the_original_file(app_and_db, tmp_path):
    app, db = app_and_db
    photo = db.photos.list_page()[0]
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute("UPDATE folders SET path=? WHERE id=?", (str(photo_dir), photo["folder_id"]))
    db.conn.execute("UPDATE photos SET working_copy_path=NULL WHERE id=?", (photo["id"],))
    db.conn.commit()
    Image.new("RGB", (900, 600)).save(os.path.join(str(photo_dir), photo["filename"]), "JPEG")
    client = app.test_client()
    resp = client.get(f"/photos/{photo['id']}/focus-crop?size=1024")
    assert resp.status_code == 200
    # Smaller than the window: the whole image comes back, still at 1:1.
    assert resp.headers["X-Vireo-Focus-Window"] == "0,0,900,600"


def test_v1_aliases_cover_the_burst_endpoints(app_and_db):
    app, db = app_and_db
    views = app.view_functions
    assert views["v1_api_photo_burst_evidence"] is views["photos.api_photo_burst_evidence"]
    assert (
        views["v1_api_selected_photos_burst_evidence"]
        is views["photos.api_selected_photos_burst_evidence"]
    )
    assert views["v1_serve_focus_crop"] is views["media.serve_focus_crop"]
    assert views["v1_api_folder_bursts"] is views["folders.api_folder_bursts"]

    folder_id = _folder_id(db)
    pids = _seed_burst(db, folder_id, 3069, [10, 80])
    headers = {"X-Vireo-Token": app.config["API_TOKEN"]}
    client = app.test_client()
    assert client.get(f"/api/v1/photos/{pids[0]}/burst-evidence").status_code == 401
    assert client.get(f"/api/v1/photos/{pids[0]}/burst-evidence", headers=headers).status_code == 200
    assert client.get(f"/api/v1/folders/{folder_id}/bursts", headers=headers).status_code == 200
    _working_copy(app, db, pids[0])
    resp = client.get(f"/api/v1/photos/{pids[0]}/focus-crop?size=128", headers=headers)
    assert resp.status_code == 200
    assert resp.content_type == "image/jpeg"
    body = client.post(
        "/api/v1/photos/burst-evidence", json={"photo_ids": pids}, headers=headers,
    ).get_json()
    assert body["scope_method"] == "selected_photos"


def test_focus_crop_center_puts_the_window_where_the_reviewer_chose(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]["id"]
    _working_copy(app, db, pid)
    db.detections.save(pid, [
        {"box": {"x": 0.1, "y": 0.2, "w": 0.2, "h": 0.2}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    client = app.test_client()
    resp = client.get(f"/photos/{pid}/focus-crop?size=128&center=0.75,0.25")
    assert resp.status_code == 200
    assert resp.headers["X-Vireo-Focus-Basis"] == "point"
    assert resp.headers["X-Vireo-Focus-Center"] == "0.7500,0.2500"
    # 0.75*3000-64 = 2186, 0.25*2000-64 = 436.
    assert resp.headers["X-Vireo-Focus-Window"] == "2186,436,2314,564"
    for bad in ("abc", "0.5", "0.5,1.5", "nan,0.5", "0.1,0.2,0.3"):
        assert client.get(f"/photos/{pid}/focus-crop?center={bad}").status_code == 400, bad


def test_focus_crop_source_prefers_the_original_over_a_downsized_working_copy(app_and_db, tmp_path):
    app, db = app_and_db
    photo = db.photos.list_page()[0]
    pid = photo["id"]
    # The catalog knows the sensor size; the working copy is half of it.
    _working_copy(app, db, pid, size=(1500, 1000))
    db.conn.execute("UPDATE photos SET width = 3000, height = 2000 WHERE id = ?", (pid,))
    photo_dir = tmp_path / "originals"
    photo_dir.mkdir()
    db.conn.execute("UPDATE folders SET path = ? WHERE id = ?", (str(photo_dir), photo["folder_id"]))
    db.conn.commit()
    client = app.test_client()

    # No original on disk: auto keeps the working copy and says how small it is.
    resp = client.get(f"/photos/{pid}/focus-crop?size=128")
    assert resp.status_code == 200
    assert resp.headers["X-Vireo-Focus-Source"] == "working_copy"
    assert resp.headers["X-Vireo-Focus-Raster-Size"] == "1500,1000"
    assert resp.headers["X-Vireo-Focus-Original-Scale"] == "0.5000"
    assert client.get(f"/photos/{pid}/focus-crop?source=original").status_code == 404

    Image.new("RGB", (3000, 2000), color=(10, 200, 10)).save(
        os.path.join(str(photo_dir), photo["filename"]), "JPEG",
    )
    resp = client.get(f"/photos/{pid}/focus-crop?size=128")
    assert resp.headers["X-Vireo-Focus-Source"] == "original"
    assert resp.headers["X-Vireo-Focus-Raster-Size"] == "3000,2000"
    assert resp.headers["X-Vireo-Focus-Original-Scale"] == "1.0000"
    resp = client.get(f"/photos/{pid}/focus-crop?size=128&source=working")
    assert resp.headers["X-Vireo-Focus-Source"] == "working_copy"
    assert resp.headers["X-Vireo-Focus-Original-Scale"] == "0.5000"
    resp = client.get(f"/photos/{pid}/focus-crop?size=128&source=original")
    assert resp.headers["X-Vireo-Focus-Source"] == "original"
    assert client.get(f"/photos/{pid}/focus-crop?source=sensor").status_code == 400

    # A full-size working copy is left alone: nothing to gain from the original.
    _working_copy(app, db, pid, size=(3000, 2000))
    resp = client.get(f"/photos/{pid}/focus-crop?size=128")
    assert resp.headers["X-Vireo-Focus-Source"] == "working_copy"
    assert resp.headers["X-Vireo-Focus-Original-Scale"] == "1.0000"
