"""Behavior pins for the stats domain of ``Database``.

The behavior tests exercise the dashboard, coverage and per-stage
pending/stale counters only through the public ``Database`` façade, so they
hold whether the SQL lives in ``db.py`` or in ``repositories/stats.py``.
They pin the exact counts over one seeded library that has an offline
folder, a folder linked only to another workspace, secondary and noise
detections, full-image anchors, stale and incomplete classifier runs,
masks under two SAM variants and eye keypoints, plus the scope clauses
(folder subtree, collection, dates, temp-table staging for large photo-id
scopes), the no-active-workspace errors, and the SQL each reader issues.
"""

import ast
import inspect
import json
import sqlite3
import textwrap

import config as cfg
import pytest
from db import Database

MODEL = "bioclip"
OTHER_MODEL = "other-model"
FP = "fpA"
OLD_FP = "fpOld"
MD = "megadetector-v6"
BOX = (0.1, 0.2, 0.3, 0.4)


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))


def _photo(db, folder_id, name, timestamp=None):
    return db.add_photo(
        folder_id=folder_id, filename=name, extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp=timestamp, width=1, height=1,
    )


def _det(db, photo_id, conf, *, model=MD, category="animal", box=BOX):
    cur = db.conn.execute(
        """INSERT INTO detections
             (photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (photo_id, model, *box, conf, category),
    )
    return cur.lastrowid


def _pred(db, det_id, species, conf, *, model=MODEL, fp=FP,
          taxonomy_class=None, created_at="2025-01-01 00:00:00"):
    cur = db.conn.execute(
        """INSERT INTO predictions
             (detection_id, classifier_model, labels_fingerprint, species,
              confidence, taxonomy_class, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (det_id, model, fp, species, conf, taxonomy_class, created_at),
    )
    return cur.lastrowid


def _run(db, det_id, *, model=MODEL, fp=FP, runtime="legacy",
         run_at="2025-01-01 00:00:00"):
    db.conn.execute(
        """INSERT INTO classifier_runs
             (detection_id, classifier_model, labels_fingerprint,
              runtime_fingerprint, run_at)
           VALUES (?, ?, ?, ?, ?)""",
        (det_id, model, fp, runtime, run_at),
    )


def _detector_run(db, photo_id, box_count, model=MD):
    db.conn.execute(
        "INSERT INTO detector_runs (photo_id, detector_model, box_count) "
        "VALUES (?, ?, ?)",
        (photo_id, model, box_count),
    )


def _mask(db, photo_id, variant, path, prompt=BOX, model=MD):
    db.conn.execute(
        """INSERT INTO photo_masks
             (photo_id, variant, path, created_at, detector_model,
              prompt_x, prompt_y, prompt_w, prompt_h)
           VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?)""",
        (photo_id, variant, path, model, *prompt),
    )


def _set(db, photo_id, **cols):
    assignments = ", ".join(f"{c} = ?" for c in cols)
    db.conn.execute(
        f"UPDATE photos SET {assignments} WHERE id = ?",
        (*cols.values(), photo_id),
    )


def seed(db):
    """Build the shared library. Returns a dict of ids."""
    ws = db._ws_id()
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    f_other = db.add_folder("/other", name="other")
    db.set_active_workspace(ws)
    f_root = db.add_folder("/lib", name="lib")
    f_child = db.add_folder("/lib/a", name="a", parent_id=f_root)
    f_empty = db.add_folder("/lib/empty", name="empty", parent_id=f_root)
    f_off = db.add_folder("/offline", name="offline")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (f_off,))

    p1 = _photo(db, f_root, "p1.jpg", "2024-05-01 07:30:00")
    p2 = _photo(db, f_root, "p2.jpg", "2024-05-20 18:05:00")
    p3 = _photo(db, f_child, "p3.jpg", "2024-06-02 12:00:00")
    p4 = _photo(db, f_off, "p4.jpg", "2023-12-31 23:59:59")
    p5 = _photo(db, f_other, "p5.jpg", "2024-05-02 09:00:00")
    p6 = _photo(db, f_root, "p6.jpg")

    _set(db, p1, rating=5, flag="flagged", quality_score=0.83,
         thumb_path="/t/p1.jpg", latitude=1.0, longitude=2.0,
         exif_data="{}", phash="ab", file_hash="h1", mask_path="/m/p1.png",
         active_mask_variant="sam2-small", subject_tenengrad=1.0,
         eye_x=0.5, eye_tenengrad=2.0, eye_kp_fingerprint=None, burst_id="b1")
    _set(db, p2, rating=0, file_hash="dup", quality_score=0.25)
    _set(db, p3, rating=4, file_hash="dup")
    _set(db, p4, rating=5, mask_path="/m/p4.png",
         active_mask_variant="sam2-small", thumb_path="/t/p4.jpg")
    _set(db, p5, rating=5, mask_path="/m/p5.png")

    d1 = _det(db, p1, 0.9)
    d1b = _det(db, p1, 0.5, box=(0.6, 0.6, 0.1, 0.1))
    d1f = _det(db, p1, 1.0, model="full-image")
    d1p = _det(db, p1, 0.95, category="person")
    d2 = _det(db, p2, 0.1)
    d2f = _det(db, p2, 1.0, model="full-image")
    d3f = _det(db, p3, 1.0, model="full-image")
    d4 = _det(db, p4, 0.8, box=(0.5, 0.5, 0.2, 0.2))
    d5 = _det(db, p5, 0.9)
    _detector_run(db, p1, 3)
    _detector_run(db, p2, 1)
    _detector_run(db, p3, 0)
    _detector_run(db, p6, 2)

    _run(db, d1, run_at="2025-02-01 00:00:00")
    _run(db, d1, model=OTHER_MODEL, fp="fpX")
    _run(db, d1b, fp=OLD_FP)
    _run(db, d4, runtime="incomplete")
    _run(db, d2f, fp=OLD_FP)
    _run(db, d5)

    robin = _pred(db, d1, "Robin", 0.9, taxonomy_class="Aves")
    _pred(db, d1, "Thrush", 0.05)
    _pred(db, d1, "Robin", 0.4, fp=OLD_FP, created_at="2020-01-01 00:00:00")
    _pred(db, d1, "Robin", 0.7, model=OTHER_MODEL, fp="fpX")
    _pred(db, d1b, "Sparrow", 0.6, fp=OLD_FP)
    _pred(db, d1b, "Finch", 0.3, fp=OLD_FP)
    fox = _pred(db, d4, "Fox", 0.8, taxonomy_class="Mammalia")
    _pred(db, d5, "Bear", 0.9)
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'accepted')", (robin, ws),
    )
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'rejected')", (fox, other_ws),
    )

    _mask(db, p1, "sam2-small", "/m/p1.png")
    _mask(db, p1, "sam2-large", "")
    _mask(db, p4, "sam2-small", "/m/p4.png", prompt=(0.0, 0.0, 1.0, 1.0))

    for pid, size in ((p1, 1920), (p3, 1920), (p2, 640)):
        db.conn.execute(
            "INSERT INTO preview_cache (photo_id, size, bytes, last_access_at) "
            "VALUES (?, ?, 1, 0)", (pid, size),
        )
    for pid, wid in ((p1, ws), (p3, ws), (p1, other_ws)):
        db.conn.execute(
            "INSERT INTO pending_changes (photo_id, change_type, value, workspace_id) "
            "VALUES (?, 'rating', '5', ?)", (pid, wid),
        )
    db.conn.commit()

    robin_kw = db.add_keyword("Robin", is_species=True)
    park = db.add_keyword("Park")
    db.tag_photo(p1, robin_kw)
    db.tag_photo(p1, park)
    db.tag_photo(p3, park)
    db.tag_photo(p4, park)

    best = db.add_collection(
        "Best", json.dumps([{"field": "rating", "op": ">=", "value": 5}]),
    )
    return {
        "ws": ws, "other_ws": other_ws,
        "f_root": f_root, "f_child": f_child, "f_empty": f_empty,
        "f_off": f_off, "f_other": f_other,
        "p1": p1, "p2": p2, "p3": p3, "p4": p4, "p5": p5, "p6": p6,
        "d1": d1, "d1b": d1b, "d1f": d1f, "d1p": d1p, "d2": d2, "d2f": d2f,
        "d3f": d3f, "d4": d4, "d5": d5,
        "best": best,
    }


@pytest.fixture
def lib(db):
    return seed(db)


def _trace(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


def _coverage_keys(**nonzero):
    keys = (
        "timestamp", "exif", "gps", "file_hash", "phash", "thumbnail",
        "working_copy", "mask", "subject_sharpness", "bg_sharpness", "eye",
        "quality", "dino_embedding", "label_embedding", "burst", "rating",
    )
    return {k: nonzero.get(k, 0) for k in keys}


# -- coverage ------------------------------------------------------------------


def test_coverage_stats_counts_accessible_workspace_photos(db, lib):
    # p4 sits in an offline folder and p5 in a folder linked only to the
    # other workspace, so the total is p1, p2, p3 and p6. ``detected`` and
    # ``classified`` count any detection at the floor, full-image included.
    assert db.get_coverage_stats() == {
        "total": 4,
        **_coverage_keys(
            timestamp=3, exif=1, gps=1, file_hash=3, phash=1, thumbnail=1,
            mask=1, subject_sharpness=1, eye=1, quality=2, burst=1, rating=2,
        ),
        "detected": 3,
        "classified": 1,
    }


def test_coverage_stats_counts_label_embeddings_and_honors_the_floor(db, lib):
    db.conn.execute(
        "INSERT INTO photo_embeddings (photo_id, model, embedding) "
        "VALUES (?, 'clip', x'00')", (lib["p2"],),
    )
    db.conn.commit()
    stats = db.get_coverage_stats()
    assert stats["label_embedding"] == 1
    db.update_workspace(lib["ws"], config_overrides={"detector_confidence": 0.95})
    stats = db.get_coverage_stats()
    # Only full-image anchors (confidence 1.0) clear 0.95.
    assert (stats["detected"], stats["classified"]) == (3, 0)


def test_coverage_stats_scopes(db, lib):
    by_child = db.get_coverage_stats(folder_id=lib["f_child"])
    assert (by_child["total"], by_child["detected"]) == (1, 1)
    by_root = db.get_coverage_stats(folder_id=lib["f_root"])
    assert (by_root["total"], by_root["detected"]) == (4, 3)
    # The collection (rating >= 5) holds p1, offline p4 and other-ws p5;
    # the outer accessible-folder join drops p4.
    by_collection = db.get_coverage_stats(collection_id=lib["best"])
    assert (by_collection["total"], by_collection["classified"]) == (1, 1)
    # A date-only ``date_to`` is inclusive of that whole day (p2 at 18:05).
    by_date = db.get_coverage_stats(date_from="2024-05-01", date_to="2024-05-20")
    assert (by_date["total"], by_date["timestamp"]) == (2, 2)
    assert db.get_coverage_stats(date_to="2024-05-19")["total"] == 1


def test_scope_rejects_unlinked_folder_and_unknown_collection(db, lib):
    with pytest.raises(ValueError, match="folder not found in active workspace"):
        db.get_coverage_stats(folder_id=lib["f_other"])
    with pytest.raises(ValueError, match="collection not found in active workspace"):
        db.get_coverage_stats(collection_id=9999)
    with pytest.raises(ValueError, match="folder not found in active workspace"):
        db.get_dashboard_stats(folder_id=lib["f_other"])


def test_dashboard_scope_clause_shape(db, lib):
    assert db._dashboard_scope_clause() == ("", [])
    sql, params = db._dashboard_scope_clause(
        folder_id=lib["f_root"], date_from="2024-01-01", date_to="2024-12-31",
        table_alias="x",
    )
    assert sql == (
        " AND x.folder_id IN (?,?,?)"
        " AND x.timestamp >= ? AND x.timestamp <= ?"
    )
    assert sorted(params[:3]) == sorted(
        [lib["f_root"], lib["f_child"], lib["f_empty"]]
    )
    assert params[3] == "2024-01-01"
    assert params[4] > "2024-12-31"  # widened to the end of the day
    sql, params = db._dashboard_scope_clause(collection_id=lib["best"])
    assert sql.startswith(" AND p.id IN (SELECT DISTINCT p.id FROM photos p ")


def test_dashboard_scope_clause_needs_no_workspace_until_folder_scoped(db, lib):
    db.set_active_workspace(None)
    assert db._dashboard_scope_clause(date_from="2024-01-01") == (
        " AND p.timestamp >= ?", ["2024-01-01"],
    )
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db._dashboard_scope_clause(folder_id=lib["f_root"])


def test_folder_coverage_stats_rows(db, lib):
    rows = db.get_folder_coverage_stats()
    # Offline and other-workspace folders are left out; the empty folder
    # stays as 0 / 0. Rows are ordered by path and do not roll up children.
    assert [r["path"] for r in rows] == ["/lib", "/lib/a", "/lib/empty"]
    root, child, empty = rows
    assert root == {
        "folder_id": lib["f_root"], "path": "/lib", "name": "lib", "total": 3,
        **_coverage_keys(
            timestamp=2, exif=1, gps=1, file_hash=2, phash=1, thumbnail=1,
            mask=1, subject_sharpness=1, eye=1, quality=2, burst=1, rating=1,
        ),
        "detected": 2, "classified": 1,
    }
    assert (child["total"], child["detected"], child["classified"]) == (1, 1, 0)
    assert child["rating"] == 1 and child["file_hash"] == 1
    assert empty == {
        "folder_id": lib["f_empty"], "path": "/lib/empty", "name": "empty",
        "total": 0, **_coverage_keys(), "detected": 0, "classified": 0,
    }


def test_folder_coverage_stats_scopes(db, lib):
    rows = db.get_folder_coverage_stats(folder_id=lib["f_child"])
    assert [(r["path"], r["total"]) for r in rows] == [("/lib/a", 1)]
    # Date scope sits on the LEFT JOIN: every folder row survives at 0.
    rows = db.get_folder_coverage_stats(date_from="2024-06-01")
    assert [(r["path"], r["total"], r["detected"]) for r in rows] == [
        ("/lib", 0, 0), ("/lib/a", 1, 1), ("/lib/empty", 0, 0),
    ]
    rows = db.get_folder_coverage_stats(collection_id=lib["best"])
    assert [(r["path"], r["total"], r["classified"]) for r in rows] == [
        ("/lib", 1, 1), ("/lib/a", 0, 0), ("/lib/empty", 0, 0),
    ]
    with pytest.raises(ValueError, match="folder not found in active workspace"):
        db.get_folder_coverage_stats(folder_id=lib["f_other"])


def test_folder_scope_composes_subtree_through_the_facade(db, lib, monkeypatch):
    calls = []
    monkeypatch.setattr(
        db, "get_folder_subtree_ids", lambda fid: calls.append(fid) or [fid],
    )
    with pytest.raises(ValueError):
        db.get_folder_coverage_stats(folder_id=lib["f_other"])
    assert calls == []
    rows = db.get_folder_coverage_stats(folder_id=lib["f_root"])
    # Façade-patched subtree: only the root itself is enumerated, and both
    # the full scope clause and the folder filter asked for it.
    assert calls == [lib["f_root"], lib["f_root"]]
    assert [r["path"] for r in rows] == ["/lib"]
    assert db.get_dashboard_stats(folder_id=lib["f_root"])["total_photos"] == 3


# -- dashboard -------------------------------------------------------------------


def test_dashboard_stats_whole_workspace(db, lib):
    stats = db.get_dashboard_stats()
    park, robin = stats["top_keywords"]
    assert stats == {
        "top_keywords": [
            {"id": park["id"], "name": "Park", "is_species": 0,
             "identity": park["identity"], "photo_count": 3},
            {"id": robin["id"], "name": "Robin", "is_species": 1,
             "identity": robin["identity"], "photo_count": 1},
        ],
        "photos_by_month": [
            {"month": "2023-12", "count": 1},
            {"month": "2024-05", "count": 2},
            {"month": "2024-06", "count": 1},
        ],
        "rating_distribution": [
            {"rating": 0, "count": 2}, {"rating": 4, "count": 1},
            {"rating": 5, "count": 2},
        ],
        "flag_distribution": [
            {"flag": "flagged", "count": 1}, {"flag": "none", "count": 4},
        ],
        # Latest fingerprint per (detection, model) at the floor: d1's three
        # current rows, d1b's two fpOld rows and d4's Fox (reviewed only in
        # the other workspace, so still pending here).
        "prediction_status": [
            {"status": "accepted", "count": 1}, {"status": "pending", "count": 5},
        ],
        "classified_count": 2,
        "photos_by_hour": [
            {"hour": 7, "count": 1}, {"hour": 12, "count": 1},
            {"hour": 18, "count": 1}, {"hour": 23, "count": 1},
        ],
        "quality_distribution": [
            {"bucket": -1, "count": 3}, {"bucket": 2, "count": 1},
            {"bucket": 8, "count": 1},
        ],
        "detected_count": 4,
        "total_photos": 5,
        "accessible_photos": 4,
        "missing_folder_count": 1,
        "folder_count": 3,
        "keyword_count": 2,
        "pending_changes": 2,
        "attention": {
            "unclassified": 3,
            "missing_location": 3,
            "missing_previews": 2,
            "preview_size": 1920,
            "preview_enabled": True,
            "pending_sync": 2,
            "duplicate_groups": 1,
        },
    }


def test_dashboard_stats_with_previews_disabled(db, lib):
    db.update_workspace(lib["ws"], config_overrides={"preview_max_size": 0})
    statements = _trace(db)
    attention = db.get_dashboard_stats()["attention"]
    assert attention["missing_previews"] == 0
    assert attention["preview_size"] == 0
    assert attention["preview_enabled"] is False
    assert not any("preview_cache" in s for s in statements)


def test_dashboard_stats_scoped(db, lib):
    stats = db.get_dashboard_stats(folder_id=lib["f_child"])
    assert stats["total_photos"] == 1
    assert stats["top_keywords"][0]["name"] == "Park"
    assert stats["attention"]["duplicate_groups"] == 0
    stats = db.get_dashboard_stats(collection_id=lib["best"])
    # Dashboard collection scope keeps the offline photo in totals.
    assert (stats["total_photos"], stats["accessible_photos"]) == (2, 1)
    assert stats["classified_count"] == 2
    assert stats["attention"]["unclassified"] == 0
    stats = db.get_dashboard_stats(date_from="2024-05-01", date_to="2024-05-31")
    assert stats["photos_by_month"] == [{"month": "2024-05", "count": 2}]
    assert stats["pending_changes"] == 1


# -- pipeline-plan counters --------------------------------------------------------


def test_detection_counts(db, lib):
    assert db.count_real_detections_in_scope() == {
        "photos_with_dets": 2, "total_dets": 3,
    }
    assert db.count_primary_detections_in_scope() == {
        "photos_with_dets": 2, "total_dets": 2,
    }
    assert db.count_real_detections_in_scope(min_conf=0.05) == {
        "photos_with_dets": 3, "total_dets": 4,
    }
    assert db.count_primary_detections_in_scope(photo_ids={lib["p1"]}) == {
        "photos_with_dets": 1, "total_dets": 1,
    }
    assert db.count_real_detections_in_scope(photo_ids=[]) == {
        "photos_with_dets": 0, "total_dets": 0,
    }
    assert db.count_primary_detections_in_scope(min_conf=0.99) == {
        "photos_with_dets": 0, "total_dets": 0,
    }
    db.update_workspace(lib["ws"], config_overrides={"detector_confidence": 0.6})
    assert db.count_real_detections_in_scope()["total_dets"] == 2


def test_classify_pending_and_stale(db, lib):
    # d1 has a current run; d1b only a stale one; d4's current run is
    # 'incomplete', so it is still pending but not stale.
    assert db.count_classify_pending_pairs(MODEL, FP) == 2
    assert db.count_primary_classify_pending_pairs(MODEL, FP) == 1
    assert db.count_classify_stale(MODEL, FP) == 1
    assert db.count_primary_classify_stale(MODEL, FP) == 0
    assert db.count_classify_pending_pairs(MODEL, "new-fp") == 3
    assert db.count_classify_stale(MODEL, "new-fp") == 3
    assert db.count_primary_classify_stale(MODEL, "new-fp") == 2
    assert db.count_classify_pending_pairs(
        MODEL, FP, photo_ids=[lib["p4"]], min_conf=0.85,
    ) == 0
    assert db.count_primary_classify_pending_pairs(
        MODEL, FP, photo_ids=[lib["p4"]],
    ) == 1
    assert db.count_primary_classify_stale(
        MODEL, "new-fp", photo_ids=[], min_conf=0.1,
    ) == 0


def test_full_image_fallback_counts(db, lib):
    # p3 ran with box_count 0; p2's only real box is noise below 0.2; p6's
    # run claims boxes that are gone, so it is not a consistent empty run.
    assert db.count_full_image_fallback_photos() == 1
    assert db.count_full_image_fallback_photos(min_conf=0.2) == 2
    assert db.count_full_image_fallback_photos(detector_model="other") == 0
    assert db.count_full_image_classify_pending_pairs(MODEL, FP) == 1
    assert db.count_full_image_classify_pending_pairs(MODEL, FP, min_conf=0.2) == 2
    assert db.count_full_image_classify_pending_pairs(
        MODEL, OLD_FP, min_conf=0.2,
    ) == 1
    assert db.count_full_image_classify_stale(MODEL, FP) == 0
    assert db.count_full_image_classify_stale(MODEL, FP, min_conf=0.2) == 1
    assert db.count_full_image_classify_stale(
        MODEL, FP, photo_ids=[lib["p3"]], min_conf=0.2,
    ) == 0
    # A fallback photo with no full-image anchor yet is pending, not stale.
    db.conn.execute("DELETE FROM detections WHERE id = ?", (lib["d3f"],))
    db.conn.commit()
    assert db.count_full_image_classify_pending_pairs(MODEL, FP) == 1
    assert db.count_full_image_classify_stale(MODEL, "x") == 0


def test_mask_counts(db, lib):
    assert db.count_photos_pending_masks() == {"eligible": 2, "pending": 0}
    assert db.count_photos_pending_masks(sam2_variant="sam2-small") == {
        "eligible": 2, "pending": 0,
    }
    # p1's sam2-large row has an empty path; p4 has no sam2-large row.
    assert db.count_photos_pending_masks(sam2_variant="sam2-large") == {
        "eligible": 2, "pending": 2,
    }
    _set(db, lib["p1"], mask_path=None)
    db.conn.commit()
    # The mask stage does not filter on category: the 0.95 person box counts.
    assert db.count_photos_pending_masks(photo_ids=[lib["p1"]], min_conf=0.99) == {
        "eligible": 0, "pending": 0,
    }
    assert db.count_photos_pending_masks(photo_ids=[lib["p1"]]) == {
        "eligible": 1, "pending": 1,
    }


def test_thumbnail_and_preview_counts(db, lib):
    # Offline p4 counts: these read the catalog, not the disk.
    assert db.count_photos_missing_thumb() == {"eligible": 5, "pending": 3}
    assert db.count_photos_missing_preview(1920) == {"eligible": 5, "pending": 3}
    assert db.count_photos_missing_preview(640) == {"eligible": 5, "pending": 4}
    assert db.count_photos_missing_thumb_or_preview(1920) == {
        "eligible": 5, "pending": 4,
    }
    assert db.count_photos_missing_thumb(photo_ids=[]) == {
        "eligible": 0, "pending": 0,
    }
    assert db.count_photos_missing_thumb_or_preview(
        1920, photo_ids=[lib["p1"], lib["p4"]],
    ) == {"eligible": 2, "pending": 1}


def test_extract_stale(db, lib):
    # p4's sam2-small prompt no longer matches its primary detection.
    assert db.count_extract_stale("sam2-small") == 1
    assert db.count_extract_stale("sam2-large") == 0
    assert db.count_extract_stale("sam2-small", detector_confidence=0.85) == 0
    assert db.count_extract_stale("sam2-small", photo_ids=[lib["p1"]]) == 0
    db.update_workspace(lib["ws"], config_overrides={"detector_confidence": 0.85})
    assert db.count_extract_stale("sam2-small") == 0


def test_eye_keypoint_counts(db, lib):
    assert db.count_eye_keypoint_eligible() == 1
    assert db.count_eye_keypoint_stale() == 1
    assert db.count_eye_keypoint_attemptable(0.5) == 1
    assert db.count_eye_keypoint_attemptable(0.95) == 0
    assert db.count_eye_keypoint_eligible(photo_ids=[lib["p4"]]) == 0
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    _set(db, lib["p1"], eye_kp_fingerprint=EYE_KP_FINGERPRINT_VERSION)
    db.conn.commit()
    assert db.count_eye_keypoint_stale() == 0
    _set(db, lib["p1"], eye_kp_fingerprint="v0")
    db.conn.commit()
    assert db.count_eye_keypoint_stale() == 1
    db.update_workspace(lib["ws"], config_overrides={"detector_confidence": 0.95})
    assert db.count_eye_keypoint_eligible() == 0
    assert db.count_eye_keypoint_stale() == 0
    assert db.count_eye_keypoint_attemptable(0.0) == 0


# -- classification inventory ------------------------------------------------------


def _pairs(inventory):
    return sorted(
        (p["classifier_model"], p["labels_fingerprint"], p["classified_dets"],
         p["photos_covered"], p["last_run"], p["predictions_count"],
         p["median_top1_conf"], p["median_sample_size"])
        for p in inventory["pairs"]
    )


def test_classification_inventory(db, lib):
    inventory = db.get_classification_inventory(lib["ws"])
    assert inventory["total_real_detections"] == 4
    assert inventory["total_predictions_rows"] == 7
    assert _pairs(inventory) == [
        (MODEL, FP, 2, 2, "2025-02-01 00:00:00", 3, pytest.approx(0.85), 2),
        (MODEL, OLD_FP, 1, 1, "2025-01-01 00:00:00", 3, pytest.approx(0.5), 2),
        (OTHER_MODEL, "fpX", 1, 1, "2025-01-01 00:00:00", 1, 0.7, 1),
    ]
    other = db.get_classification_inventory(lib["other_ws"], min_conf=0.2)
    assert other["total_real_detections"] == 1
    assert _pairs(other) == [(MODEL, FP, 1, 1, "2025-01-01 00:00:00", 1, 0.9, 1)]
    capped = db.get_classification_inventory(lib["ws"], median_sample_per_pair=1)
    assert {p[-1] for p in _pairs(capped)} == {1}


def test_classification_inventory_pair_without_predictions(db, lib):
    # A classifier run with no prediction rows still lists its pair, with
    # no median.
    db.conn.execute(
        "INSERT INTO classifier_runs (detection_id, classifier_model, "
        "labels_fingerprint) VALUES (?, 'empty-model', 'fpE')", (lib["d4"],),
    )
    db.conn.commit()
    inventory = db.get_classification_inventory(lib["ws"], min_conf=0.2)
    empty = [p for p in inventory["pairs"] if p["classifier_model"] == "empty-model"]
    assert empty == [{
        "classifier_model": "empty-model", "labels_fingerprint": "fpE",
        "classified_dets": 1, "photos_covered": 1,
        "last_run": empty[0]["last_run"], "predictions_count": 0,
        "median_top1_conf": None, "median_sample_size": 0,
    }]


def test_classification_inventory_reads_the_target_workspace_floor(db, lib):
    db.update_workspace(lib["other_ws"], config_overrides={"detector_confidence": 0.95})
    db.set_active_workspace(None)
    other = db.get_classification_inventory(lib["other_ws"])
    assert other["total_real_detections"] == 0
    assert db._active_workspace_id is None
    db.set_active_workspace(lib["ws"])
    assert db.get_classification_inventory(lib["ws"])["total_real_detections"] == 4
    assert db._active_workspace_id == lib["ws"]


def test_classification_inventory_restores_workspace_when_config_fails(
    db, lib, monkeypatch,
):
    seen = []

    def boom(self, global_config):
        seen.append(self._active_workspace_id)
        raise RuntimeError("config broke")

    monkeypatch.setattr(Database, "get_effective_config", boom)
    with pytest.raises(RuntimeError, match="config broke"):
        db.get_classification_inventory(lib["other_ws"])
    assert seen == [lib["other_ws"]]
    assert db._active_workspace_id == lib["ws"]


def test_classification_inventory_needs_no_active_workspace(db, lib):
    db.set_active_workspace(None)
    inventory = db.get_classification_inventory(lib["ws"], min_conf=0.2)
    assert inventory["total_real_detections"] == 4


def test_sampled_top1_medians(db, lib):
    assert db._sampled_top1_medians(lib["ws"], 0.2, 100) == {
        (MODEL, FP): (pytest.approx(0.85), 2),
        (MODEL, OLD_FP): (0.5, 2),
        (OTHER_MODEL, "fpX"): (0.7, 1),
    }
    capped = db._sampled_top1_medians(lib["ws"], 0.2, 1)
    assert {v[1] for v in capped.values()} == {1}
    assert db._sampled_top1_medians(lib["ws"], 0.99, 100) == {}
    db.conn.execute("UPDATE predictions SET confidence = NULL")
    assert db._sampled_top1_medians(lib["ws"], 0.2, 100) == {}
    db.conn.rollback()


# -- scope staging -------------------------------------------------------------------


def test_scope_clause_shapes(db):
    assert db._scope_clause(None) == ("", [])
    assert db._scope_clause([]) == (" AND p.id IN (NULL)", [])
    assert db._scope_clause([3, 1], table_alias="x") == (" AND x.id IN (?,?)", [3, 1])
    assert db._scope_clause(range(1, 900)) == (
        " AND p.id IN (SELECT id FROM scope_ids)", [],
    )
    assert db.conn.execute("SELECT COUNT(*) FROM temp.scope_ids").fetchone()[0] == 899


def test_large_scope_counts_match_inline_scope(db, lib):
    small = [lib["p1"], lib["p2"], lib["p3"], lib["p4"], lib["p6"]]
    big = list(range(1, 900))
    statements = _trace(db)
    assert db.count_real_detections_in_scope(photo_ids=big) == \
        db.count_real_detections_in_scope(photo_ids=small)
    assert "SAVEPOINT stage_read_scope" in statements
    assert any("IN (SELECT id FROM scope_ids)" in s for s in statements)
    assert db.count_photos_missing_thumb_or_preview(1920, photo_ids=big) == \
        db.count_photos_missing_thumb_or_preview(1920, photo_ids=small)
    assert db.count_eye_keypoint_attemptable(0.5, photo_ids=big) == 1
    assert db.count_full_image_classify_stale(
        MODEL, FP, photo_ids=big, min_conf=0.2,
    ) == 1
    assert not db.conn.in_transaction


def test_stage_scope_ids_rejects_unknown_table(db):
    statements = _trace(db)
    with pytest.raises(ValueError, match="Unknown scope table"):
        db._stage_scope_ids("photos", [1])
    assert statements == []


def test_stage_scope_ids_replaces_and_supports_both_tables(db):
    db._stage_scope_ids("missing_subtree_ids", [1, 2, 2])
    db._stage_scope_ids("missing_subtree_ids", [5])
    rows = db.conn.execute("SELECT id FROM temp.missing_subtree_ids").fetchall()
    assert [r[0] for r in rows] == [5]
    assert not db.conn.in_transaction


def test_stage_scope_ids_rolls_back_a_failed_stage(db, lib):
    db._stage_scope_ids("scope_ids", [1, 2])

    def ids():
        yield 3
        raise KeyError("boom")

    db.conn.execute("UPDATE photos SET rating = 3 WHERE id = ?", (lib["p2"],))
    with pytest.raises(KeyError):
        db._stage_scope_ids("scope_ids", ids())
    # The failed stage is undone to the savepoint; the caller's own
    # uncommitted write and transaction survive.
    rows = db.conn.execute("SELECT id FROM temp.scope_ids ORDER BY id").fetchall()
    assert [r[0] for r in rows] == [1, 2]
    assert db.conn.in_transaction
    db.conn.rollback()
    assert db.get_photo(lib["p2"])["rating"] == 0


# -- workspace requirement, transactions, and query shape ------------------------------


SCOPED_READERS = [
    ("get_coverage_stats", ()),
    ("get_folder_coverage_stats", ()),
    ("get_dashboard_stats", ()),
    ("count_real_detections_in_scope", ()),
    ("count_primary_detections_in_scope", ()),
    ("count_classify_pending_pairs", (MODEL, FP)),
    ("count_primary_classify_pending_pairs", (MODEL, FP)),
    ("count_classify_stale", (MODEL, FP)),
    ("count_primary_classify_stale", (MODEL, FP)),
    ("count_full_image_fallback_photos", ()),
    ("count_full_image_classify_pending_pairs", (MODEL, FP)),
    ("count_full_image_classify_stale", (MODEL, FP)),
    ("count_photos_pending_masks", ()),
    ("count_photos_missing_thumb", ()),
    ("count_photos_missing_preview", (1920,)),
    ("count_photos_missing_thumb_or_preview", (1920,)),
    ("count_extract_stale", ("sam2-small",)),
    ("count_eye_keypoint_eligible", ()),
    ("count_eye_keypoint_stale", ()),
    ("count_eye_keypoint_attemptable", (0.5,)),
]


@pytest.mark.parametrize("name,args", SCOPED_READERS)
def test_scoped_reader_requires_active_workspace(db, lib, name, args):
    db.set_active_workspace(None)
    statements = _trace(db)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        getattr(db, name)(*args)
    assert statements == []


@pytest.mark.parametrize("name,args", SCOPED_READERS)
def test_scoped_reader_leaves_no_transaction_open(db, lib, name, args):
    kwargs = {}
    if "photo_ids" in inspect.signature(getattr(Database, name)).parameters:
        kwargs["photo_ids"] = range(1, 900)  # staged through the temp table
    db.conn.commit()
    getattr(db, name)(*args, **kwargs)
    assert not db.conn.in_transaction
    # A second connection can take the write lock straight away.
    other = sqlite3.connect(db._db_path, timeout=0)
    try:
        other.execute("UPDATE photos SET rating = 1 WHERE id = ?", (lib["p6"],))
    finally:
        other.rollback()
        other.close()


QUERY_COUNTS = [
    # (method, args, kwargs, statements the reader itself issues)
    ("get_coverage_stats", (), {}, 3),
    ("get_folder_coverage_stats", (), {}, 3),
    ("get_dashboard_stats", (), {}, 15),
    ("count_real_detections_in_scope", (), {"min_conf": 0.2}, 1),
    ("count_classify_stale", (MODEL, FP), {"min_conf": 0.2}, 1),
    ("count_full_image_classify_pending_pairs", (MODEL, FP), {}, 1),
    ("count_photos_pending_masks", (), {"min_conf": 0.2, "sam2_variant": "x"}, 1),
    ("count_photos_missing_preview", (1920,), {}, 1),
    ("count_extract_stale", ("sam2-small",), {"detector_confidence": 0.2}, 1),
]


@pytest.mark.parametrize("name,args,kwargs,expected", QUERY_COUNTS)
def test_reader_query_count(db, lib, monkeypatch, name, args, kwargs, expected):
    # Freeze the config lookup so only the reader's own SQL is traced.
    effective = db.get_effective_config(cfg.load())
    monkeypatch.setattr(db, "get_effective_config", lambda _cfg: effective)
    statements = _trace(db)
    getattr(db, name)(*args, **kwargs)
    assert len(statements) == expected, statements


def test_classification_inventory_query_count(db, lib):
    statements = _trace(db)
    db.get_classification_inventory(lib["ws"], min_conf=0.2)
    assert len(statements) == 4
    assert "ORDER BY RANDOM()" in statements[-1]


# -- structure: the SQL lives in StatsRepository ---------------------------------
#
# The scope composition (``_scope_clause`` / ``_dashboard_scope_clause``) and the
# workspace-effective config lookups stay on the façade; only the SQL moved.


STATS_METHODS = [
    "_dashboard_scope_clause",
    "get_coverage_stats",
    "get_folder_coverage_stats",
    "_stage_scope_ids",
    "count_real_detections_in_scope",
    "count_primary_detections_in_scope",
    "count_classify_pending_pairs",
    "count_primary_classify_pending_pairs",
    "count_classify_stale",
    "count_primary_classify_stale",
    "count_full_image_fallback_photos",
    "count_full_image_classify_pending_pairs",
    "count_full_image_classify_stale",
    "get_classification_inventory",
    "_sampled_top1_medians",
    "count_photos_pending_masks",
    "count_photos_missing_thumb",
    "count_photos_missing_preview",
    "count_photos_missing_thumb_or_preview",
    "count_extract_stale",
    "count_eye_keypoint_eligible",
    "count_eye_keypoint_stale",
    "count_eye_keypoint_attemptable",
    "get_dashboard_stats",
]


@pytest.mark.parametrize("name", STATS_METHODS)
def test_stats_method_delegates_to_repository(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    fn = ast.parse(source).body[0]
    attrs = {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to StatsRepository"
    )
    assert "_stats_repository" in attrs, (
        f"Database.{name} no longer delegates to StatsRepository"
    )
