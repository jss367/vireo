"""Tests for the classification inventory feature.

Covers `Database.get_classification_inventory()` (per-pair aggregates over the
active workspace) and the `/api/workspace/classification-inventory` endpoint
that builds the cross-product, identifies stale rows, and computes medians.
"""
import json
import os

import pytest

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _add_detection(db, photo_id, model="megadetector-v6", conf=0.9, category="animal"):
    cur = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_model, detector_confidence, category, "
        "box_x, box_y, box_w, box_h) VALUES (?, ?, ?, ?, 0.1, 0.1, 0.5, 0.5)",
        (photo_id, model, conf, category),
    )
    db.conn.commit()
    return cur.lastrowid


def _record_run(db, detection_id, model_name, fingerprint, run_at=None):
    """Insert a classifier_runs row + a single prediction row.

    A real classify pass writes both. Tests should mirror that so coverage
    queries (which use classifier_runs) and median queries (which use
    predictions) line up.
    """
    if run_at:
        db.conn.execute(
            "INSERT INTO classifier_runs (detection_id, classifier_model, "
            "labels_fingerprint, run_at, prediction_count) VALUES (?, ?, ?, ?, 1)",
            (detection_id, model_name, fingerprint, run_at),
        )
    else:
        db.conn.execute(
            "INSERT INTO classifier_runs (detection_id, classifier_model, "
            "labels_fingerprint, prediction_count) VALUES (?, ?, ?, 1)",
            (detection_id, model_name, fingerprint),
        )
    db.conn.commit()


def _add_prediction(db, detection_id, model_name, fingerprint, species, confidence):
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence) VALUES (?, ?, ?, ?, ?)",
        (detection_id, model_name, fingerprint, species, confidence),
    )
    db.conn.commit()


def _seed_workspace(db, name="WS1", folder_path="/photos/ws1", n_photos=2):
    """Create a workspace with N photos in one folder. Returns (ws_id, photo_ids)."""
    ws_id = db.create_workspace(name) if hasattr(db, "create_workspace") else None
    if ws_id is None:
        cur = db.conn.execute("INSERT INTO workspaces (name) VALUES (?)", (name,))
        ws_id = cur.lastrowid
        db.conn.commit()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(folder_path, name=os.path.basename(folder_path))
    photo_ids = []
    for i in range(n_photos):
        pid = db.add_photo(
            folder_id=fid,
            filename=f"p{i}.jpg",
            extension=".jpg",
            file_size=1000 + i,
            file_mtime=float(i),
            timestamp=f"2026-01-{i+1:02d}T10:00:00",
        )
        photo_ids.append(pid)
    return ws_id, photo_ids


# ---------------------------------------------------------------------------
# DB-level tests: Database.get_classification_inventory()
# ---------------------------------------------------------------------------

def test_empty_workspace_returns_zero_pairs(db):
    ws_id, _ = _seed_workspace(db, n_photos=0)
    inv = db.get_classification_inventory(ws_id, min_conf=0.2)
    assert inv["total_real_detections"] == 0
    assert inv["pairs"] == []


def test_no_predictions_returns_zero_classified(db):
    ws_id, photos = _seed_workspace(db, n_photos=2)
    for p in photos:
        _add_detection(db, p)
    inv = db.get_classification_inventory(ws_id, min_conf=0.2)
    assert inv["total_real_detections"] == 2
    assert inv["pairs"] == []


def test_single_pair_fully_classified(db):
    ws_id, photos = _seed_workspace(db, n_photos=2)
    for p in photos:
        det = _add_detection(db, p)
        _record_run(db, det, "BioCLIP-2.5", "fp_birds")
        _add_prediction(db, det, "BioCLIP-2.5", "fp_birds", "Robin", 0.85)

    inv = db.get_classification_inventory(ws_id, min_conf=0.2)
    assert inv["total_real_detections"] == 2
    assert len(inv["pairs"]) == 1
    pair = inv["pairs"][0]
    assert pair["classifier_model"] == "BioCLIP-2.5"
    assert pair["labels_fingerprint"] == "fp_birds"
    assert pair["classified_dets"] == 2
    assert pair["photos_covered"] == 2
    assert pair["last_run"] is not None


def test_two_pairs_independent(db):
    ws_id, photos = _seed_workspace(db, n_photos=3)
    dets = [_add_detection(db, p) for p in photos]
    # All 3 detections classified by BioCLIP-2.5 with fp_birds
    for d in dets:
        _record_run(db, d, "BioCLIP-2.5", "fp_birds")
    # Only 1 detection classified by iNat21 with tol
    _record_run(db, dets[0], "iNat21 (EVA-02 Large)", "tol")

    inv = db.get_classification_inventory(ws_id, min_conf=0.2)
    pair_map = {(p["classifier_model"], p["labels_fingerprint"]): p for p in inv["pairs"]}
    assert pair_map[("BioCLIP-2.5", "fp_birds")]["classified_dets"] == 3
    assert pair_map[("iNat21 (EVA-02 Large)", "tol")]["classified_dets"] == 1


def test_below_threshold_excluded(db):
    ws_id, photos = _seed_workspace(db, n_photos=2)
    high = _add_detection(db, photos[0], conf=0.9)
    low = _add_detection(db, photos[1], conf=0.05)
    _record_run(db, high, "M", "fp")
    _record_run(db, low, "M", "fp")

    inv = db.get_classification_inventory(ws_id, min_conf=0.2)
    assert inv["total_real_detections"] == 1  # low excluded
    assert inv["pairs"][0]["classified_dets"] == 1


def test_full_image_detections_excluded(db):
    ws_id, photos = _seed_workspace(db, n_photos=2)
    real = _add_detection(db, photos[0], model="megadetector-v6")
    full = _add_detection(db, photos[1], model="full-image")
    _record_run(db, real, "M", "fp")
    _record_run(db, full, "M", "fp")

    inv = db.get_classification_inventory(ws_id, min_conf=0.2)
    assert inv["total_real_detections"] == 1
    assert inv["pairs"][0]["classified_dets"] == 1


def test_workspace_scoping_excludes_other_workspaces(db):
    ws1_id, photos1 = _seed_workspace(db, name="WS1", folder_path="/photos/ws1", n_photos=1)
    det1 = _add_detection(db, photos1[0])
    _record_run(db, det1, "M", "fp")

    ws2_id, photos2 = _seed_workspace(db, name="WS2", folder_path="/photos/ws2", n_photos=1)
    det2 = _add_detection(db, photos2[0])
    _record_run(db, det2, "M", "fp")

    # Inventory for ws1 should see only its own detection
    inv1 = db.get_classification_inventory(ws1_id, min_conf=0.2)
    assert inv1["total_real_detections"] == 1
    assert inv1["pairs"][0]["classified_dets"] == 1

    inv2 = db.get_classification_inventory(ws2_id, min_conf=0.2)
    assert inv2["total_real_detections"] == 1
    assert inv2["pairs"][0]["classified_dets"] == 1


def test_photos_covered_dedupes(db):
    ws_id, photos = _seed_workspace(db, n_photos=1)
    # 5 detections on one photo
    dets = [_add_detection(db, photos[0]) for _ in range(5)]
    for d in dets:
        _record_run(db, d, "M", "fp")

    inv = db.get_classification_inventory(ws_id, min_conf=0.2)
    pair = inv["pairs"][0]
    assert pair["classified_dets"] == 5
    assert pair["photos_covered"] == 1


def test_median_top1_confidence_sample(db):
    ws_id, photos = _seed_workspace(db, n_photos=10)
    # Each photo: one detection, one prediction with conf 0.9
    for p in photos:
        d = _add_detection(db, p)
        _record_run(db, d, "M", "fp")
        _add_prediction(db, d, "M", "fp", "X", 0.9)

    inv = db.get_classification_inventory(ws_id, min_conf=0.2)
    pair = inv["pairs"][0]
    # When all confidences are 0.9, median must be 0.9 regardless of sampling.
    assert pair["median_top1_conf"] == pytest.approx(0.9, abs=0.001)
    assert pair["median_sample_size"] >= 1


def test_median_top1_sample_is_unbiased_by_detection_id(db):
    """The capped median sample must not deterministically pick the oldest
    detection IDs. Detection IDs are auto-incremented chronologically, so
    a deterministic-by-ID sample under-represents recent runs and biases
    the median.

    Setup: 50 low-conf detections (older IDs) followed by 200 high-conf
    (newer IDs). Sample 50 per pair.
      - Deterministic-by-ID: picks the 50 oldest → all conf 0.1 →
        median 0.1.
      - Random: ~20% of population is low, so ~10 of 50 sampled are
        low → median 0.9.
    """
    ws_id, photos = _seed_workspace(db, n_photos=1)
    photo = photos[0]
    for _ in range(50):
        d = _add_detection(db, photo)
        _record_run(db, d, "M", "fp")
        _add_prediction(db, d, "M", "fp", "X", 0.1)
    for _ in range(200):
        d = _add_detection(db, photo)
        _record_run(db, d, "M", "fp")
        _add_prediction(db, d, "M", "fp", "X", 0.9)

    inv = db.get_classification_inventory(ws_id, min_conf=0.2,
                                           median_sample_per_pair=50)
    pair = inv["pairs"][0]
    # Random median is 0.9 with overwhelming probability; deterministic-by-ID
    # bias would pin it at 0.1.
    assert pair["median_top1_conf"] > 0.5, (
        f"median {pair['median_top1_conf']} indicates biased sampling"
    )


def test_median_top1_uses_max_per_detection(db):
    """A detection with multiple species predictions: median uses the top-1 (max conf)."""
    ws_id, photos = _seed_workspace(db, n_photos=1)
    d = _add_detection(db, photos[0])
    _record_run(db, d, "M", "fp")
    _add_prediction(db, d, "M", "fp", "Robin", 0.85)
    _add_prediction(db, d, "M", "fp", "Sparrow", 0.10)

    inv = db.get_classification_inventory(ws_id, min_conf=0.2)
    pair = inv["pairs"][0]
    assert pair["median_top1_conf"] == pytest.approx(0.85, abs=0.001)


# ---------------------------------------------------------------------------
# Endpoint-level tests: /api/workspace/classification-inventory
# ---------------------------------------------------------------------------

def _setup_labels_dir(tmp_path, monkeypatch, sets):
    """Write label files under a temp dir and patch labels.LABELS_DIR.

    sets: list of (name, [species]). Writes name.json + name.txt.
    """
    import labels as labels_mod
    labels_dir = os.path.join(str(tmp_path), ".vireo", "labels")
    os.makedirs(labels_dir, exist_ok=True)
    monkeypatch.setattr(labels_mod, "LABELS_DIR", labels_dir)
    paths = {}
    for name, species in sets:
        txt = os.path.join(labels_dir, f"{name}.txt")
        with open(txt, "w") as f:
            f.write("\n".join(species))
        meta = {
            "name": name,
            "place_name": name,
            "taxon_groups": [],
            "species_count": len(species),
            "labels_file": txt,
        }
        with open(os.path.join(labels_dir, f"{name}.json"), "w") as f:
            json.dump(meta, f)
        paths[name] = txt
    return paths


def test_endpoint_cross_product_includes_never_run(app_and_db, tmp_path, monkeypatch):
    from labels_fingerprint import compute_fingerprint  # noqa: F401
    app, db = app_and_db
    # Seed a single detection in workspace's first photo
    ws_id = db._active_workspace_id
    photo_row = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()
    _add_detection(db, photo_row["id"])

    # Two label sets on disk, no runs at all
    _setup_labels_dir(tmp_path, monkeypatch, [
        ("birds", ["Robin", "Sparrow"]),
        ("reptiles", ["Lizard"]),
    ])

    client = app.test_client()
    resp = client.get("/api/workspace/classification-inventory")
    assert resp.status_code == 200
    body = resp.get_json()

    # Cross-product: every (model × label-set) pair appears, each "never_run"
    assert body["total_real_detections"] == 1
    assert len(body["models"]) >= 1
    seen_pairs = []
    for m in body["models"]:
        for p in m["pairs"]:
            seen_pairs.append((m["name"], p["label_set"]))
            assert p["status"] in ("never_run", "complete", "partial", "stale")
    # There should be at least one (model, "birds") pair and one (model, "reptiles") pair
    label_sets_seen = {ls for _, ls in seen_pairs}
    assert "birds" in label_sets_seen
    assert "reptiles" in label_sets_seen


def test_endpoint_identifies_stale(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    photo_row = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()
    det = _add_detection(db, photo_row["id"])
    # A prior run on a fingerprint that does not match any current label file
    _record_run(db, det, "BioCLIP-2.5", "obsolete_fp_xyz")

    _setup_labels_dir(tmp_path, monkeypatch, [("birds", ["Robin"])])

    client = app.test_client()
    resp = client.get("/api/workspace/classification-inventory")
    body = resp.get_json()
    stale_fps = {s["fingerprint"] for s in body["stale"]}
    assert "obsolete_fp_xyz" in stale_fps


def test_endpoint_tol_only_for_supported_models(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    _setup_labels_dir(tmp_path, monkeypatch, [("birds", ["Robin"])])

    client = app.test_client()
    resp = client.get("/api/workspace/classification-inventory")
    body = resp.get_json()

    for m in body["models"]:
        tol_pairs = [p for p in m["pairs"] if p.get("is_tol")]
        if m["supports_tol"]:
            assert len(tol_pairs) == 1, f"{m['name']} should have one Tree of Life pair"
        else:
            assert tol_pairs == [], f"{m['name']} should not have a Tree of Life pair"


def test_endpoint_legacy_model_grouped(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    photo_row = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()
    det = _add_detection(db, photo_row["id"])
    # A run from a model not in the registry
    _record_run(db, det, "AncientModel-v0", "fp")

    _setup_labels_dir(tmp_path, monkeypatch, [("birds", ["Robin"])])

    client = app.test_client()
    resp = client.get("/api/workspace/classification-inventory")
    body = resp.get_json()
    legacy_models = [m for m in body["models"] if m.get("legacy")]
    legacy_names = {m["name"] for m in legacy_models}
    assert "AncientModel-v0" in legacy_names


def test_endpoint_grand_total(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    photo_row = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()
    det = _add_detection(db, photo_row["id"])
    _record_run(db, det, "BioCLIP-2.5", "fp_birds")
    _add_prediction(db, det, "BioCLIP-2.5", "fp_birds", "Robin", 0.9)

    _setup_labels_dir(tmp_path, monkeypatch, [("birds", ["Robin"])])

    client = app.test_client()
    resp = client.get("/api/workspace/classification-inventory")
    body = resp.get_json()
    gt = body["grand_total"]
    assert gt["classified_dets"] >= 1
    assert gt["total_predictions_rows"] >= 1


def test_endpoint_dedupes_label_files_with_same_fingerprint(
    app_and_db, tmp_path, monkeypatch,
):
    """Two label files with identical content (same fingerprint) must produce
    one inventory row per model — emitting both would double-count the shared
    db_pair stats in subtotals and grand totals.
    """
    app, db = app_and_db
    photo_row = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()
    det = _add_detection(db, photo_row["id"])

    # Two label files with identical species → identical fingerprint.
    _setup_labels_dir(tmp_path, monkeypatch, [
        ("birds", ["Robin", "Sparrow"]),
        ("birds_copy", ["Robin", "Sparrow"]),
    ])

    # Pre-record a run + prediction against the shared fingerprint so the
    # double-count would be observable in the totals.
    from labels_fingerprint import compute_fingerprint
    fp = compute_fingerprint(["Robin", "Sparrow"])
    _record_run(db, det, "BioCLIP-2.5", fp)
    _add_prediction(db, det, "BioCLIP-2.5", fp, "Robin", 0.9)

    client = app.test_client()
    resp = client.get("/api/workspace/classification-inventory")
    body = resp.get_json()

    # For each non-legacy model, the duplicate fingerprint should appear only
    # once across that model's label-set rows.
    for m in body["models"]:
        if m.get("legacy"):
            continue
        non_intrinsic = [p for p in m["pairs"] if not p.get("is_intrinsic")]
        fps = [p["fingerprint"] for p in non_intrinsic]
        assert len(fps) == len(set(fps)), (
            f"{m['name']} has duplicate fingerprints: {fps}"
        )

    # The grand total counts each detection at most once per (model, fp);
    # without dedup, classified_dets would be inflated by the duplicate row.
    for m in body["models"]:
        if m.get("legacy"):
            continue
        for p in m["pairs"]:
            if p["fingerprint"] == fp:
                assert p["classified_dets"] <= 1


def test_endpoint_merged_fingerprint_not_stale(app_and_db, tmp_path, monkeypatch):
    """A classify run with multiple label files merged produces a fingerprint
    of the union — that fingerprint matches no single ``.txt`` file but is
    still current as long as the source files exist and their content
    matches what was hashed. The labels_fingerprints sidecar records
    the (fingerprint, sources) pair; the inventory must consult it so
    merged-labels runs don't get marked stale.
    """
    from labels_fingerprint import compute_fingerprint
    app, db = app_and_db
    photo_row = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()
    det = _add_detection(db, photo_row["id"])

    paths = _setup_labels_dir(tmp_path, monkeypatch, [
        ("birds", ["Robin", "Sparrow"]),
        ("reptiles", ["Lizard"]),
    ])
    merged = sorted({"Robin", "Sparrow", "Lizard"})
    merged_fp = compute_fingerprint(merged)
    _record_run(db, det, "BioCLIP-2.5", merged_fp)
    _add_prediction(db, det, "BioCLIP-2.5", merged_fp, "Robin", 0.7)
    db.upsert_labels_fingerprint(
        fingerprint=merged_fp,
        display_name="birds.txt, reptiles.txt",
        sources=[paths["birds"], paths["reptiles"]],
        label_count=len(merged),
    )

    client = app.test_client()
    resp = client.get("/api/workspace/classification-inventory")
    body = resp.get_json()
    stale_fps = {s["fingerprint"] for s in body["stale"]}
    assert merged_fp not in stale_fps


def test_endpoint_stale_count_uses_prediction_rows(app_and_db, tmp_path, monkeypatch):
    """Stale rows are rendered with a "Predictions" column header in the UI,
    so ``stale_count`` must reflect prediction rows (which can be multiple
    per detection — one per species in a top-k result), not distinct
    detection IDs.
    """
    app, db = app_and_db
    photo_row = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()
    det = _add_detection(db, photo_row["id"])
    # Run on a fingerprint that does not match any current label file →
    # this becomes a stale entry. Three species predictions on the one
    # detection: prediction count = 3, distinct detection count = 1.
    _record_run(db, det, "BioCLIP-2.5", "obsolete_fp")
    _add_prediction(db, det, "BioCLIP-2.5", "obsolete_fp", "Robin", 0.7)
    _add_prediction(db, det, "BioCLIP-2.5", "obsolete_fp", "Sparrow", 0.2)
    _add_prediction(db, det, "BioCLIP-2.5", "obsolete_fp", "Wren", 0.1)

    _setup_labels_dir(tmp_path, monkeypatch, [("birds", ["Robin"])])

    client = app.test_client()
    resp = client.get("/api/workspace/classification-inventory")
    body = resp.get_json()
    stale = [s for s in body["stale"] if s["fingerprint"] == "obsolete_fp"]
    assert len(stale) == 1
    assert stale[0]["stale_count"] == 3
