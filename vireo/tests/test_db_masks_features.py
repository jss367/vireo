"""Behavior pins for the masks/features domain of ``Database``.

The tests exercise the SAM-mask, pipeline-feature and embedding operations
through the ``db.masks_features`` accessor and the coordinated ``Database``
methods that stay on the façade; the structural tests at the end pin the
accessor's shape and keep the old forwarding wrappers gone. They cover mask rows and
variant activation, the storage-cleanup deletes and their masks-directory
containment check, the per-variant coverage and rerun warning, the
pipeline-feature and raw-analysis writers, the mask and eye-keypoint stage
selectors, and the DINOv2 / per-model embedding stores.
"""

import ast
import contextlib
import inspect
import json
import os
import sqlite3
import textwrap

import config as cfg
import db as db_module
import pytest
from db import Database
from repositories.masks_features import MasksFeaturesRepository

BOX = (0.1, 0.2, 0.3, 0.4)
OTHER_BOX = (0.5, 0.5, 0.2, 0.2)


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))


@contextlib.contextmanager
def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _folder(db, path="/photos"):
    return db.add_folder(path, name=os.path.basename(path))


def _photo(db, name, folder_id=None):
    fid = folder_id if folder_id is not None else _folder(db)
    return db.add_photo(
        folder_id=fid, filename=name, extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp=None, width=40, height=30,
    )


def _det(db, photo_id, detector_model="megadetector-v6", conf=0.9, box=BOX,
         category="animal"):
    cur = db.conn.execute(
        """INSERT INTO detections
             (photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (photo_id, detector_model, *box, conf, category),
    )
    db.conn.commit()
    return cur.lastrowid


def _mask(db, photo_id, variant="sam2-small", path=None,
          detector_model="megadetector-v6", box=BOX, **features):
    db.masks_features.upsert_mask(
        photo_id, variant, path or f"/nowhere/{photo_id}_{variant}.png",
        detector_model, *box, **features,
    )


def _row(db, photo_id):
    return db.conn.execute(
        "SELECT * FROM photos WHERE id=?", (photo_id,),
    ).fetchone()


def _set_floor(db, value):
    db.update_workspace(
        db._ws_id(), config_overrides={"detector_confidence": value},
    )


def _masks_dir(db):
    d = os.path.join(os.path.dirname(db._db_path), "masks")
    os.makedirs(d, exist_ok=True)
    return d


def _mask_file(db, name):
    path = os.path.join(_masks_dir(db), name)
    with open(path, "wb") as f:
        f.write(b"x" * 10)
    return path


def _other_workspace_photo(db, name="foreign.jpg"):
    home = db._ws_id()
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    pid = _photo(db, name, folder_id=_folder(db, "/elsewhere"))
    db.set_active_workspace(home)
    return pid


def _commit_recorder(monkeypatch):
    calls = []
    real = db_module.commit_with_retry

    def recording(conn, *args, **kwargs):
        calls.append(conn)
        return real(conn, *args, **kwargs)

    monkeypatch.setattr(db_module, "commit_with_retry", recording)
    return calls


# -- mask rows ----------------------------------------------------------------


def test_get_photo_mask_returns_dict_or_none(db):
    pid = _photo(db, "a.jpg")
    assert db.masks_features.get_mask(pid, "sam2-small") is None
    _mask(db, pid, subject_size=0.25)
    row = db.masks_features.get_mask(pid, "sam2-small")
    assert isinstance(row, dict)
    assert row["photo_id"] == pid
    assert row["variant"] == "sam2-small"
    assert row["subject_size"] == 0.25
    assert (row["prompt_x"], row["prompt_y"], row["prompt_w"],
            row["prompt_h"]) == BOX


def test_list_masks_for_photo_orders_newest_first(db):
    pid = _photo(db, "a.jpg")
    assert db.masks_features.list_masks_for_photo(pid) == []
    _mask(db, pid, "old")
    _mask(db, pid, "new")
    db.conn.execute(
        "UPDATE photo_masks SET created_at = CASE variant "
        "WHEN 'old' THEN 100 ELSE 200 END"
    )
    db.conn.commit()
    rows = db.masks_features.list_masks_for_photo(pid)
    assert [r["variant"] for r in rows] == ["new", "old"]
    assert all(isinstance(r, dict) for r in rows)


def test_upsert_photo_mask_inserts_replaces_and_commits(db, monkeypatch):
    pid = _photo(db, "a.jpg")
    calls = _commit_recorder(monkeypatch)
    _mask(db, pid, path="/m/a.png", subject_size=0.1, subject_tenengrad=1.0,
          bg_tenengrad=2.0, crop_complete=1.0, quality_input_recipe="r1",
          subject_clip_high=0.01, subject_clip_low=0.02, subject_y_median=0.5,
          bg_separation=3.0, phash_crop="abc", noise_estimate=0.7)
    assert calls == [db.conn]
    with _reader(db) as reader:
        row = reader.execute("SELECT * FROM photo_masks").fetchone()
    assert row["path"] == "/m/a.png"
    assert isinstance(row["created_at"], int)
    assert row["quality_input_recipe"] == "r1"
    assert row["phash_crop"] == "abc"
    assert row["noise_estimate"] == 0.7

    _mask(db, pid, path="/m/b.png", detector_model="other", box=OTHER_BOX)
    rows = db.masks_features.list_masks_for_photo(pid)
    assert len(rows) == 1
    assert rows[0]["path"] == "/m/b.png"
    assert rows[0]["detector_model"] == "other"
    assert rows[0]["prompt_x"] == OTHER_BOX[0]
    # Every feature column is replaced, not merged.
    assert rows[0]["subject_size"] is None
    assert rows[0]["phash_crop"] is None


def test_upsert_photo_mask_without_commit_leaves_transaction_open(db):
    pid = _photo(db, "a.jpg")
    _mask(db, pid, _commit=False)
    assert db.conn.in_transaction
    with _reader(db) as reader:
        assert reader.execute("SELECT COUNT(*) FROM photo_masks").fetchone()[0] == 0
    db.conn.commit()


# -- set_active_mask_variant --------------------------------------------------


def test_set_active_mask_variant_missing_row_raises(db):
    pid = _photo(db, "a.jpg")
    with pytest.raises(ValueError, match=rf"No photo_masks row for photo {pid} variant 'nope'"):
        db.set_active_mask_variant(pid, "nope")


def test_set_active_mask_variant_denormalizes_and_commits(db, monkeypatch):
    pid = _photo(db, "a.jpg")
    _det(db, pid)
    _mask(db, pid, path="/m/a.png", subject_size=0.3, subject_tenengrad=1.5,
          bg_tenengrad=0.5, crop_complete=1.0, quality_input_recipe="rec",
          subject_clip_high=0.1, subject_clip_low=0.2, subject_y_median=0.4,
          bg_separation=2.5, phash_crop="ff", noise_estimate=0.05)
    calls = _commit_recorder(monkeypatch)
    db.set_active_mask_variant(pid, "sam2-small")
    assert calls == [db.conn]
    with _reader(db) as reader:
        p = reader.execute("SELECT * FROM photos WHERE id=?", (pid,)).fetchone()
    assert p["mask_path"] == "/m/a.png"
    assert p["active_mask_variant"] == "sam2-small"
    assert p["subject_size"] == 0.3
    assert p["subject_tenengrad"] == 1.5
    assert p["bg_tenengrad"] == 0.5
    assert p["crop_complete"] == 1.0
    assert p["quality_input_recipe"] == "rec"
    assert p["subject_clip_high"] == 0.1
    assert p["subject_clip_low"] == 0.2
    assert p["subject_y_median"] == 0.4
    assert p["bg_separation"] == 2.5
    assert p["phash_crop"] == "ff"
    assert p["noise_estimate"] == 0.05


def test_active_mask_variant_and_mask_path_reads(db):
    pid = _photo(db, "a.jpg")
    assert db.masks_features.active_variant(pid) is None
    assert db.masks_features.photo_mask_path(pid) is None
    assert db.masks_features.active_variant(99999) is None
    assert db.masks_features.photo_mask_path(99999) is None
    _det(db, pid)
    _mask(db, pid, path="/m/a.png")
    db.set_active_mask_variant(pid, "sam2-small")
    assert db.masks_features.active_variant(pid) == "sam2-small"
    assert db.masks_features.photo_mask_path(pid) == "/m/a.png"


def test_set_active_mask_variant_without_commit(db):
    pid = _photo(db, "a.jpg")
    _det(db, pid)
    _mask(db, pid)
    db.set_active_mask_variant(pid, "sam2-small", _commit=False)
    assert db.conn.in_transaction
    with _reader(db) as reader:
        row = reader.execute(
            "SELECT active_mask_variant FROM photos WHERE id=?", (pid,),
        ).fetchone()
    assert row["active_mask_variant"] is None
    db.conn.commit()


@pytest.mark.parametrize("mask_kwargs", [
    {"box": OTHER_BOX},
    {"detector_model": "other-det"},
])
def test_set_active_mask_variant_rejects_mask_of_another_subject(db, mask_kwargs):
    pid = _photo(db, "a.jpg")
    _det(db, pid)
    _mask(db, pid, **mask_kwargs)
    with pytest.raises(ValueError, match="belongs to another subject"):
        db.set_active_mask_variant(pid, "sam2-small")
    assert _row(db, pid)["active_mask_variant"] is None


def test_set_active_mask_variant_uses_primary_ordering(db):
    """The top-ordered detection is the primary; a secondary's mask is rejected."""
    pid = _photo(db, "a.jpg")
    _det(db, pid, conf=0.95, box=BOX)
    _det(db, pid, conf=0.5, box=OTHER_BOX)
    _mask(db, pid, "primary", box=BOX)
    _mask(db, pid, "secondary", box=OTHER_BOX)
    with pytest.raises(ValueError, match="another subject"):
        db.set_active_mask_variant(pid, "secondary")
    db.set_active_mask_variant(pid, "primary")
    assert _row(db, pid)["active_mask_variant"] == "primary"


def test_set_active_mask_variant_accepts_pre_detection_migration_row(db):
    pid = _photo(db, "a.jpg")
    _mask(db, pid, "unknown", path="/m/legacy.png")
    db.set_active_mask_variant(pid, "unknown")
    p = _row(db, pid)
    assert p["active_mask_variant"] == "unknown"
    assert p["mask_path"] == "/m/legacy.png"


def test_set_active_mask_variant_rejects_when_only_detector_run_exists(db):
    pid = _photo(db, "a.jpg")
    db.model_runs.record_detector_run(pid, "megadetector-v6", 0)
    _mask(db, pid)
    with pytest.raises(ValueError, match="another subject"):
        db.set_active_mask_variant(pid, "sam2-small")


def test_set_active_mask_variant_reads_floor_from_workspace_config(db):
    pid = _photo(db, "a.jpg")
    _det(db, pid, conf=0.5)
    _mask(db, pid)
    _set_floor(db, 0.6)
    with pytest.raises(ValueError, match="another subject"):
        db.set_active_mask_variant(pid, "sam2-small")
    _set_floor(db, 0.4)
    db.set_active_mask_variant(pid, "sam2-small")
    assert _row(db, pid)["active_mask_variant"] == "sam2-small"


def test_set_active_mask_variant_weak_rescue_lowers_floor_for_megadetector(db):
    pid = _photo(db, "a.jpg")
    _det(db, pid, conf=0.1)
    _mask(db, pid)
    with pytest.raises(ValueError, match="another subject"):
        db.set_active_mask_variant(pid, "sam2-small")
    db.set_active_mask_variant(pid, "sam2-small", weak_rescue_min_conf=0.05)
    assert _row(db, pid)["active_mask_variant"] == "sam2-small"


def test_set_active_mask_variant_weak_rescue_only_considers_megadetector(db):
    pid = _photo(db, "a.jpg")
    _det(db, pid, detector_model="yolo", conf=0.9)
    _mask(db, pid, detector_model="yolo")
    # Without rescue the yolo box is the primary and matches.
    db.set_active_mask_variant(pid, "sam2-small", _commit=False)
    db.conn.rollback()
    with pytest.raises(ValueError, match="another subject"):
        db.set_active_mask_variant(pid, "sam2-small", weak_rescue_min_conf=0.05)


# -- masks directory containment ----------------------------------------------


def test_masks_dir_real(db, tmp_path):
    assert db._masks_dir_real() == os.path.realpath(str(tmp_path / "masks"))
    db._db_path = ":memory:"
    assert db._masks_dir_real() is None
    db._db_path = "bare.db"
    assert db._masks_dir_real() is None


def test_safe_remove_mask_file_removes_only_inside_masks_dir(db, tmp_path):
    inside = _mask_file(db, "a.png")
    outside = tmp_path / "outside.png"
    outside.write_bytes(b"x")
    db._safe_remove_mask_file(None)
    db._safe_remove_mask_file("")
    db._safe_remove_mask_file(str(outside))
    assert outside.exists()
    # The masks directory itself is inside the root but is not a file.
    db._safe_remove_mask_file(_masks_dir(db))
    assert os.path.isdir(_masks_dir(db))
    db._safe_remove_mask_file(os.path.join(_masks_dir(db), "missing.png"))
    db._safe_remove_mask_file(inside)
    assert not os.path.exists(inside)


def test_safe_remove_mask_file_refuses_without_masks_dir(db):
    inside = _mask_file(db, "a.png")
    db._db_path = ":memory:"
    db._safe_remove_mask_file(inside)
    assert os.path.exists(inside)


def test_safe_remove_mask_file_tolerates_os_errors(db, monkeypatch):
    inside = _mask_file(db, "a.png")
    unresolvable = os.path.join(_masks_dir(db), "bad.png")
    real_realpath = os.path.realpath

    def realpath(p, *args, **kwargs):
        if p == unresolvable:
            raise OSError("cannot resolve")
        return real_realpath(p, *args, **kwargs)

    monkeypatch.setattr(os.path, "realpath", realpath)
    db._safe_remove_mask_file(unresolvable)

    def boom(path):
        raise OSError("busy")

    monkeypatch.setattr(os, "remove", boom)
    db._safe_remove_mask_file(inside)
    assert os.path.exists(inside)


# -- cleanup deletes -----------------------------------------------------------


def test_delete_masks_for_variant_refuses_active_variant(db):
    pid = _photo(db, "a.jpg")
    _det(db, pid)
    _mask(db, pid)
    db.set_active_mask_variant(pid, "sam2-small")
    with pytest.raises(ValueError, match=r"Variant 'sam2-small' is active for 1 photo\(s\)"):
        db.masks_features.delete_for_variant("sam2-small")
    assert db.masks_features.get_mask(pid, "sam2-small") is not None


def test_delete_masks_for_variant_removes_rows_and_contained_files(db, tmp_path, monkeypatch):
    fid = _folder(db)
    a, b, c = (_photo(db, n, fid) for n in ("a.jpg", "b.jpg", "c.jpg"))
    fa = _mask_file(db, "a.png")
    outside = tmp_path / "b.png"
    outside.write_bytes(b"x")
    _mask(db, a, "large", path=fa)
    _mask(db, b, "large", path=str(outside))
    _mask(db, c, "small")
    calls = _commit_recorder(monkeypatch)
    assert db.masks_features.delete_for_variant("large") == 2
    assert calls == [db.conn]
    assert not os.path.exists(fa)
    assert outside.exists()
    with _reader(db) as reader:
        variants = [r[0] for r in reader.execute("SELECT variant FROM photo_masks")]
    assert variants == ["small"]
    assert db.masks_features.delete_for_variant("absent") == 0


def test_delete_inactive_masks(db, monkeypatch):
    fid = _folder(db)
    a, b = _photo(db, "a.jpg", fid), _photo(db, "b.jpg", fid)
    fa_old = _mask_file(db, "a_old.png")
    fa_new = _mask_file(db, "a_new.png")
    _mask(db, a, "new", path=fa_new)
    _mask(db, a, "old", path=fa_old)
    db.set_active_mask_variant(a, "new")
    # b has masks but no active variant: never touched.
    _mask(db, b, "x")
    _mask(db, b, "y")
    calls = _commit_recorder(monkeypatch)
    assert db.masks_features.delete_inactive() == 1
    assert calls == [db.conn]
    assert not os.path.exists(fa_old)
    assert os.path.exists(fa_new)
    with _reader(db) as reader:
        rows = sorted(tuple(r) for r in reader.execute(
            "SELECT photo_id, variant FROM photo_masks"))
    assert rows == [(a, "new"), (b, "x"), (b, "y")]
    assert db.masks_features.delete_inactive() == 0


def test_find_stale_masks(db):
    fid = _folder(db)
    a, b, c = (_photo(db, n, fid) for n in ("a.jpg", "b.jpg", "c.jpg"))
    _det(db, a, conf=0.9, box=BOX)
    _det(db, a, conf=0.4, box=OTHER_BOX)
    _mask(db, a, "fresh", box=BOX)
    _mask(db, a, "secondary", box=OTHER_BOX)
    _det(db, b, conf=0.3, box=BOX)
    _mask(db, b, "weak", box=BOX)
    _mask(db, c, "orphan")  # no detections at all
    # Full-image rows never count as the primary.
    _det(db, c, detector_model="full-image", conf=1.0, box=BOX)

    stale = db.masks_features.find_stale()
    keys = {(s["photo_id"], s["variant"]) for s in stale}
    assert keys == {(a, "secondary"), (c, "orphan")}
    assert set(stale[0]) == {
        "photo_id", "variant", "path", "detector_model",
        "prompt_x", "prompt_y", "prompt_w", "prompt_h",
    }
    # A floor above b's only box hides it, so b's mask is stale too.
    keys = {(s["photo_id"], s["variant"])
            for s in db.masks_features.find_stale(detector_confidence=0.5)}
    assert keys == {(a, "secondary"), (b, "weak"), (c, "orphan")}


def test_delete_stale_masks_skips_active_and_forwards_confidence(db, monkeypatch):
    fid = _folder(db)
    a, b = _photo(db, "a.jpg", fid), _photo(db, "b.jpg", fid)
    _det(db, a, conf=0.3)
    stale_file = _mask_file(db, "a.png")
    _mask(db, a, "weak", path=stale_file)
    _mask(db, b, "legacy")
    db.set_active_mask_variant(b, "legacy")  # no detections → allowed

    seen = []
    real_find = MasksFeaturesRepository.find_stale

    def spy(self, detector_confidence=None):
        seen.append(detector_confidence)
        return real_find(self, detector_confidence=detector_confidence)

    monkeypatch.setattr(MasksFeaturesRepository, "find_stale", spy)
    calls = _commit_recorder(monkeypatch)
    assert db.delete_stale_masks() == 0
    assert calls == [db.conn]  # commits even when nothing was deleted
    assert db.delete_stale_masks(detector_confidence=0.5) == 1
    assert seen == [None, 0.5]
    assert not os.path.exists(stale_file)
    with _reader(db) as reader:
        rows = [tuple(r) for r in reader.execute(
            "SELECT photo_id, variant FROM photo_masks")]
    assert rows == [(b, "legacy")]


# -- coverage and summaries ---------------------------------------------------


def test_mask_variant_coverage_is_workspace_scoped(db):
    fid = _folder(db)
    a, b = _photo(db, "a.jpg", fid), _photo(db, "b.jpg", fid)
    foreign = _other_workspace_photo(db)
    _mask(db, a, "large")
    _mask(db, a, "small")
    _mask(db, b, "small")
    db.set_active_mask_variant(b, "small")
    _mask(db, foreign, "tiny")
    _mask(db, foreign, "small")
    assert db.masks_features.variant_coverage() == [
        {"variant": "large", "count": 1, "active_count": 0},
        {"variant": "small", "count": 2, "active_count": 1},
    ]
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.masks_features.variant_coverage()


def test_get_workspace_photo_ids_with_mask_variant_is_workspace_scoped(db):
    fid = _folder(db)
    a, b, c = (_photo(db, n, fid) for n in ("a.jpg", "b.jpg", "c.jpg"))
    foreign = _other_workspace_photo(db)
    _mask(db, a, "large")
    _mask(db, b, "large")
    _mask(db, b, "small")
    _mask(db, c, "small")
    _mask(db, foreign, "large")
    assert sorted(db.masks_features.workspace_photo_ids_with_variant("large")) == [a, b]
    assert db.masks_features.workspace_photo_ids_with_variant("tiny") == []
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.masks_features.workspace_photo_ids_with_variant("large")


def test_get_photo_pipeline_features_reads_the_feature_columns(db):
    foreign = _other_workspace_photo(db)
    db.masks_features.update_pipeline_features(
        foreign, subject_tenengrad=4.5, bg_tenengrad=1.5, subject_y_median=0.2,
    )
    row = db.masks_features.pipeline_feature_row(foreign)
    assert list(row.keys()) == [
        "id", "filename", "timestamp", "width", "height",
        "mask_path", "subject_tenengrad", "bg_tenengrad",
        "crop_complete", "bg_separation",
        "subject_clip_high", "subject_clip_low", "subject_y_median",
        "phash_crop", "subject_size",
    ]
    assert (row["id"], row["filename"]) == (foreign, "foreign.jpg")
    assert (row["subject_tenengrad"], row["bg_tenengrad"], row["subject_y_median"]) == (4.5, 1.5, 0.2)
    db.set_active_workspace(None)  # catalog-wide
    assert db.masks_features.pipeline_feature_row(foreign)["id"] == foreign
    assert db.masks_features.pipeline_feature_row(999999) is None


def test_mask_variant_coverage_empty(db):
    assert db.masks_features.variant_coverage() == []


def test_mask_variants_summary_counts_bytes_across_catalog(db, monkeypatch):
    fid = _folder(db)
    a, b = _photo(db, "a.jpg", fid), _photo(db, "b.jpg", fid)
    foreign = _other_workspace_photo(db)
    fa = _mask_file(db, "a.png")
    _mask(db, a, "large", path=fa)
    _mask(db, b, "large", path="/does/not/exist.png")
    _mask(db, foreign, "small", path=_mask_file(db, "f.png"))
    db.set_active_mask_variant(foreign, "small")
    assert db.masks_features.variants_summary() == [
        {"variant": "large", "count": 2, "active_count": 0, "bytes": 10},
        {"variant": "small", "count": 1, "active_count": 1, "bytes": 10},
    ]

    def boom(path):
        raise OSError("gone")

    monkeypatch.setattr(os.path, "getsize", boom)
    assert [s["bytes"] for s in db.masks_features.variants_summary()] == [0, 0]


def test_mask_variants_summary_empty(db):
    assert db.masks_features.variants_summary() == []


# -- sam_variant_rerun_warning -------------------------------------------------


def _sam_fixture(db, n_photos=4, selected=0, alternate=4, conf=0.9):
    fid = _folder(db)
    pids = [_photo(db, f"p{i}.jpg", fid) for i in range(n_photos)]
    for i, pid in enumerate(pids):
        _det(db, pid, conf=conf)
        if i < alternate:
            _mask(db, pid, "sam2-large", path=f"/m/{pid}_l.png")
        if i < selected:
            _mask(db, pid, "sam2-small", path=f"/m/{pid}_s.png")
        if i < max(selected, alternate):
            db.masks_features.update_pipeline_features(pid, mask_path=f"/m/{pid}.png")
    return pids


@pytest.mark.parametrize("variant", [None, "", "unknown"])
def test_sam_variant_rerun_warning_skips_without_variant_before_workspace(db, variant):
    db.set_active_workspace(None)
    assert db.sam_variant_rerun_warning(variant) is None


def test_sam_variant_rerun_warning_no_targets(db):
    assert db.sam_variant_rerun_warning("sam2-small") is None


def test_sam_variant_rerun_warning_reports_high_alternate(db):
    _sam_fixture(db, n_photos=4, selected=1, alternate=4)
    warning = db.sam_variant_rerun_warning("sam2-small")
    assert warning == {
        "code": "sam_variant_rerun",
        "selected_variant": "sam2-small",
        "selected_count": 1,
        "selected_ratio": 0.25,
        "alternate_variant": "sam2-large",
        "alternate_count": 4,
        "alternate_ratio": 1.0,
        "target_count": 4,
        "message": (
            "sam2-small has masks for 1 of 4 target photos, while sam2-large "
            "already has masks for 4. Starting will rerun SAM for the "
            "selected variant."
        ),
    }


def test_sam_variant_rerun_warning_thresholds(db):
    _sam_fixture(db, n_photos=4, selected=2, alternate=3)
    # 2/4 selected is above the default 25% cap.
    assert db.sam_variant_rerun_warning("sam2-small") is None
    # With a higher cap, the 75% alternate misses the default 80% floor ...
    assert db.sam_variant_rerun_warning("sam2-small", selected_max_ratio=0.5) is None
    # ... and clears a lower floor.
    warning = db.sam_variant_rerun_warning(
        "sam2-small", selected_max_ratio=0.5, alternate_min_ratio=0.75,
    )
    assert warning["alternate_count"] == 3
    assert warning["selected_ratio"] == 0.5


def test_sam_variant_rerun_warning_without_alternates(db):
    _sam_fixture(db, n_photos=2, selected=0, alternate=0)
    assert db.sam_variant_rerun_warning("sam2-small") is None


def test_sam_variant_rerun_warning_min_conf_and_scope(db):
    pids = _sam_fixture(db, n_photos=4, selected=0, alternate=4, conf=0.3)
    # Workspace floor 0.2 by default: all four photos are targets.
    assert db.sam_variant_rerun_warning("sam2-small")["target_count"] == 4
    _set_floor(db, 0.5)
    assert db.sam_variant_rerun_warning("sam2-small") is None
    assert db.sam_variant_rerun_warning("sam2-small", min_conf=0.1)["target_count"] == 4
    scoped = db.sam_variant_rerun_warning(
        "sam2-small", photo_ids=pids[:2], min_conf=0.1,
    )
    assert scoped["target_count"] == 2
    assert db.sam_variant_rerun_warning("sam2-small", photo_ids=[], min_conf=0.1) is None


def test_sam_variant_rerun_warning_ignores_unknown_and_empty_paths(db):
    fid = _folder(db)
    pids = [_photo(db, f"p{i}.jpg", fid) for i in range(2)]
    for pid in pids:
        _det(db, pid)
        _mask(db, pid, "unknown")
        db.masks_features.update_pipeline_features(pid, mask_path="/m/x.png")
    db.conn.execute("UPDATE photo_masks SET path='', variant='sam2-large' "
                    "WHERE photo_id=?", (pids[0],))
    db.conn.commit()
    assert db.sam_variant_rerun_warning("sam2-small") is None


# -- pipeline features and raw analysis ---------------------------------------


def test_save_subject_raw_analysis_upserts_and_commits(db, monkeypatch):
    pid = _photo(db, "a.jpg")
    det = _det(db, pid)
    calls = _commit_recorder(monkeypatch)
    db.masks_features.save_subject_raw_analysis(det, {"recipe": "r1", "ev": 1.5})
    assert calls == [db.conn]
    with _reader(db) as reader:
        row = reader.execute("SELECT * FROM subject_raw_analysis").fetchone()
    assert row["detection_id"] == det
    assert row["recipe"] == "r1"
    assert json.loads(row["report_json"]) == {"recipe": "r1", "ev": 1.5}
    assert isinstance(row["created_at"], int)

    db.masks_features.save_subject_raw_analysis(det, {"recipe": "r2"}, _commit=False)
    assert db.conn.in_transaction
    db.conn.commit()
    rows = db.conn.execute("SELECT recipe FROM subject_raw_analysis").fetchall()
    assert [r["recipe"] for r in rows] == ["r2"]


def test_save_subject_raw_analysis_rejects_nan(db):
    pid = _photo(db, "a.jpg")
    det = _det(db, pid)
    with pytest.raises(ValueError):
        db.masks_features.save_subject_raw_analysis(det, {"recipe": "r", "v": float("nan")})


def test_update_photo_pipeline_features_writes_only_provided_columns(db, monkeypatch):
    pid = _photo(db, "a.jpg")
    db.masks_features.update_pipeline_features(
        pid, mask_path="/m/a.png", subject_tenengrad=1.0, bg_tenengrad=2.0,
        crop_complete=1.0, bg_separation=3.0, subject_clip_high=0.1,
        subject_clip_low=0.2, subject_y_median=0.3, phash_crop="ab",
        noise_estimate=0.4, eye_x=5.0, eye_y=6.0, eye_conf=0.7,
        eye_tenengrad=8.0, eye_kp_fingerprint="v1", quality_input_recipe="q",
    )
    with _reader(db) as reader:
        p = reader.execute("SELECT * FROM photos WHERE id=?", (pid,)).fetchone()
    assert (p["mask_path"], p["subject_tenengrad"], p["bg_tenengrad"],
            p["crop_complete"], p["bg_separation"], p["subject_clip_high"],
            p["subject_clip_low"], p["subject_y_median"], p["phash_crop"],
            p["noise_estimate"], p["eye_x"], p["eye_y"], p["eye_conf"],
            p["eye_tenengrad"], p["eye_kp_fingerprint"],
            p["quality_input_recipe"]) == (
        "/m/a.png", 1.0, 2.0, 1.0, 3.0, 0.1, 0.2, 0.3, "ab", 0.4,
        5.0, 6.0, 0.7, 8.0, "v1", "q",
    )

    statements = []
    db.conn.set_trace_callback(statements.append)
    db.masks_features.update_pipeline_features(pid, eye_x=None, noise_estimate=0.9)
    db.conn.set_trace_callback(None)
    updates = [s for s in statements if s.startswith("UPDATE photos")]
    assert updates == [f"UPDATE photos SET noise_estimate=0.9, eye_x=NULL WHERE id={pid}"]
    p = _row(db, pid)
    assert p["eye_x"] is None
    assert p["noise_estimate"] == 0.9
    assert p["mask_path"] == "/m/a.png"


def test_update_photo_pipeline_features_noop_without_columns(db, monkeypatch):
    pid = _photo(db, "a.jpg")
    calls = _commit_recorder(monkeypatch)
    statements = []
    db.conn.set_trace_callback(statements.append)
    db.masks_features.update_pipeline_features(pid)
    db.conn.set_trace_callback(None)
    assert statements == []
    assert calls == []


def test_update_photo_pipeline_features_commit_flag(db, monkeypatch):
    pid = _photo(db, "a.jpg")
    calls = _commit_recorder(monkeypatch)
    db.masks_features.update_pipeline_features(pid, mask_path="/m/a.png", _commit=False)
    assert calls == []
    assert db.conn.in_transaction
    db.conn.rollback()
    db.masks_features.update_pipeline_features(pid, mask_path="/m/b.png")
    assert calls == [db.conn]
    with _reader(db) as reader:
        assert reader.execute(
            "SELECT mask_path FROM photos WHERE id=?", (pid,),
        ).fetchone()[0] == "/m/b.png"


# -- get_photos_missing_masks --------------------------------------------------


def test_get_photos_missing_masks_whole_workspace(db):
    fid = _folder(db)
    a, b, c = (_photo(db, n, fid) for n in ("a.jpg", "b.jpg", "c.jpg"))
    _det(db, a, conf=0.5, box=OTHER_BOX)
    _det(db, a, conf=0.9, box=BOX)
    _det(db, b, conf=0.1)  # below the default 0.2 floor
    _det(db, c, conf=0.9)
    db.masks_features.update_pipeline_features(c, mask_path="/m/c.png")
    foreign = _other_workspace_photo(db)
    _det(db, foreign, conf=0.9)

    rows = db.get_photos_missing_masks()
    assert rows == [{
        "id": a, "folder_id": fid, "filename": "a.jpg",
        "detection_box": json.dumps({"x": BOX[0], "y": BOX[1],
                                     "w": BOX[2], "h": BOX[3]}),
        "detection_conf": 0.9,
    }]
    _set_floor(db, 0.95)
    assert db.get_photos_missing_masks() == []


def test_get_photos_missing_masks_folder_filter_stays_workspace_scoped(db):
    f1, f2 = _folder(db, "/one"), _folder(db, "/two")
    a, b = _photo(db, "a.jpg", f1), _photo(db, "b.jpg", f2)
    _det(db, a)
    _det(db, b)
    foreign = _other_workspace_photo(db)
    _det(db, foreign)
    foreign_folder = _row(db, foreign)["folder_id"]

    assert [r["id"] for r in db.get_photos_missing_masks(folder_ids=[f2])] == [b]
    assert [r["id"] for r in db.get_photos_missing_masks(
        folder_ids=[f1, f2, foreign_folder])] == [a, b]
    # An empty list means "no filter", not "no folders".
    assert [r["id"] for r in db.get_photos_missing_masks(folder_ids=[])] == [a, b]


# -- workspace_mask_candidate_detections ----------------------------------------


def test_workspace_mask_candidate_detections_rows_and_order(db):
    fid = _folder(db)
    a, b, c = (_photo(db, n, fid) for n in ("a.jpg", "b.jpg", "c.jpg"))
    _det(db, a, conf=0.5, box=OTHER_BOX)
    _det(db, a, conf=0.9, box=BOX)
    _det(db, a, detector_model="full-image", conf=1.0)
    _det(db, b, conf=0.1)  # below the floor the caller passes
    _det(db, c, conf=0.3)
    # Already masked photos stay: the job checks the cache per photo.
    db.masks_features.update_pipeline_features(c, mask_path="/m/c.png")
    foreign = _other_workspace_photo(db)
    _det(db, foreign, conf=0.9)

    rows = db.masks_features.workspace_mask_candidate_detections(0.2)
    got = [(r["id"], r["detector_confidence"]) for r in rows]
    # Photo id first, then the primary ordering (confidence among animals).
    assert got == [(a, 0.9), (a, 0.5), (c, 0.3)]
    first = dict(rows[0])
    assert first == {
        "id": a, "folder_id": fid, "filename": "a.jpg",
        "detector_model": "megadetector-v6",
        "box_x": BOX[0], "box_y": BOX[1], "box_w": BOX[2], "box_h": BOX[3],
        "detector_confidence": 0.9,
    }
    assert [r["id"] for r in db.masks_features.workspace_mask_candidate_detections(0.6)] == [a]


def test_workspace_mask_candidate_detections_needs_a_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.masks_features.workspace_mask_candidate_detections(0.2)


# -- photo_mask_state -----------------------------------------------------------


def test_get_photo_mask_state_reads_the_three_columns(db):
    pid = _photo(db, "a.jpg")
    state = db.masks_features.photo_mask_state(pid)
    assert dict(state) == {
        "active_mask_variant": None,
        "dino_embedding_variant": None,
        "quality_input_recipe": None,
    }
    _det(db, pid)
    _mask(db, pid, path="/m/a.png")
    db.set_active_mask_variant(pid, "sam2-small")
    db.masks_features.update_embeddings(pid, dino_subject_embedding=b"s",
                               dino_global_embedding=b"g", variant="vit-b14")
    db.masks_features.update_pipeline_features(pid, quality_input_recipe="r1")
    assert dict(db.masks_features.photo_mask_state(pid)) == {
        "active_mask_variant": "sam2-small",
        "dino_embedding_variant": "vit-b14",
        "quality_input_recipe": "r1",
    }
    assert db.masks_features.photo_mask_state(99999) is None


# -- list_photos_for_eye_keypoint_stage ----------------------------------------


def _eye_ready(db, name, fid, conf=0.9, box=BOX, taxonomy=None, species="Robin",
               species_conf=0.8, model="BioCLIP"):
    pid = _photo(db, name, fid)
    det = _det(db, pid, conf=conf, box=box)
    db.add_prediction(det, species=species, confidence=species_conf,
                      model=model, taxonomy=taxonomy)
    _mask(db, pid, box=box, path=f"/m/{pid}.png")
    db.set_active_mask_variant(pid, "sam2-small")
    return pid, det


def test_list_photos_for_eye_keypoint_stage_returns_eligible_rows(db):
    fid = _folder(db)
    a, det_a = _eye_ready(db, "a.jpg", fid,
                          taxonomy={"class": "Aves", "scientific_name": "Turdus"})
    rows = db.list_photos_for_eye_keypoint_stage()
    assert rows == [{
        "id": a, "folder_id": fid, "filename": "a.jpg",
        "width": 40, "height": 30, "mask_path": f"/m/{a}.png",
        "detection_id": det_a,
        "box_x": BOX[0], "box_y": BOX[1], "box_w": BOX[2], "box_h": BOX[3],
        "species_conf": 0.8, "taxonomy_class": "Aves",
        "scientific_name": "Turdus", "species": "Robin",
    }]


def test_list_photos_for_eye_keypoint_stage_filters(db):
    import pipeline

    fid = _folder(db)
    ready, _ = _eye_ready(db, "ready.jpg", fid)
    stamped, _ = _eye_ready(db, "stamped.jpg", fid)
    db.masks_features.update_pipeline_features(
        stamped, eye_kp_fingerprint=pipeline.EYE_KP_FINGERPRINT_VERSION,
    )
    old_stamp, _ = _eye_ready(db, "old.jpg", fid)
    db.masks_features.update_pipeline_features(old_stamp, eye_kp_fingerprint="v0")
    stale, _ = _eye_ready(db, "stale.jpg", fid)
    # A new, stronger detection makes the active mask's prompt stale.
    _det(db, stale, conf=0.99, box=OTHER_BOX)
    unmasked = _photo(db, "unmasked.jpg", fid)
    det = _det(db, unmasked)
    db.add_prediction(det, species="Robin", confidence=0.5, model="BioCLIP")
    no_pred = _photo(db, "nopred.jpg", fid)
    _det(db, no_pred)
    _mask(db, no_pred)
    db.set_active_mask_variant(no_pred, "sam2-small")

    ids = [r["id"] for r in db.list_photos_for_eye_keypoint_stage()]
    assert ids == [ready, old_stamp]
    assert db.list_photos_for_eye_keypoint_stage(photo_ids=[]) == []
    assert [r["id"] for r in db.list_photos_for_eye_keypoint_stage(
        photo_ids=iter([old_stamp, stamped]))] == [old_stamp]

    _set_floor(db, 0.95)
    assert db.list_photos_for_eye_keypoint_stage() == []


def test_list_photos_for_eye_keypoint_stage_prefers_routable_predictions(db):
    fid = _folder(db)
    pid, det = _eye_ready(db, "a.jpg", fid, species="Bird", species_conf=0.95)
    db.add_prediction(det, species="Robin", confidence=0.4, model="BioCLIP",
                      taxonomy={"scientific_name": "Turdus migratorius"})
    rows = db.list_photos_for_eye_keypoint_stage()
    assert len(rows) == 1
    assert rows[0]["species"] == "Robin"
    assert rows[0]["scientific_name"] == "Turdus migratorius"


def test_list_photos_for_eye_keypoint_stage_uses_latest_label_set(db):
    fid = _folder(db)
    pid, det = _eye_ready(db, "a.jpg", fid, species="Old", species_conf=0.99)
    db.conn.execute(
        "UPDATE predictions SET created_at='2000-01-01' WHERE detection_id=?",
        (det,),
    )
    db.conn.commit()
    db.add_prediction(det, species="New", confidence=0.1, model="BioCLIP",
                      labels_fingerprint="fp-new")
    rows = db.list_photos_for_eye_keypoint_stage()
    assert [r["species"] for r in rows] == ["New"]


def test_list_photos_for_eye_keypoint_stage_is_workspace_scoped(db):
    fid = _folder(db)
    _eye_ready(db, "a.jpg", fid)
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    assert db.list_photos_for_eye_keypoint_stage() == []


# -- embeddings ----------------------------------------------------------------


def test_update_photo_embeddings_stores_blobs_and_commits(db, monkeypatch):
    pid = _photo(db, "a.jpg")
    calls = _commit_recorder(monkeypatch)
    db.masks_features.update_embeddings(pid, b"\x01\x02", b"\x03", variant="vit-b14")
    assert calls == [db.conn]
    with _reader(db) as reader:
        p = reader.execute("SELECT * FROM photos WHERE id=?", (pid,)).fetchone()
    assert (p["dino_subject_embedding"], p["dino_global_embedding"],
            p["dino_embedding_variant"]) == (b"\x01\x02", b"\x03", "vit-b14")

    db.masks_features.update_embeddings(pid, _commit=False)
    assert calls == [db.conn]
    assert db.conn.in_transaction
    db.conn.commit()
    p = _row(db, pid)
    assert (p["dino_subject_embedding"], p["dino_global_embedding"],
            p["dino_embedding_variant"]) == (None, None, None)


def test_photo_embedding_roundtrip_and_upsert(db):
    pid = _photo(db, "a.jpg")
    assert db.masks_features.get_embedding(pid, "bioclip") is None
    db.upsert_photo_embedding(pid, "bioclip", b"one")
    assert not db.conn.in_transaction
    with _reader(db) as reader:
        assert reader.execute(
            "SELECT embedding FROM photo_embeddings").fetchone()[0] == b"one"
    db.upsert_photo_embedding(pid, "bioclip", b"two")
    db.upsert_photo_embedding(pid, "bioclip", b"v2", variant="v2")
    assert db.masks_features.get_embedding(pid, "bioclip") == b"two"
    assert db.masks_features.get_embedding(pid, "bioclip", variant="v2") == b"v2"
    assert db.masks_features.get_embedding(pid, "other") is None
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_embeddings").fetchone()[0] == 2


def test_upsert_photo_embedding_verify_workspace(db):
    mine = _photo(db, "a.jpg")
    foreign = _other_workspace_photo(db)
    # Unverified writes accept any photo (background jobs pre-scope).
    db.upsert_photo_embedding(foreign, "m", b"x")
    with pytest.raises(ValueError, match=f"Photo {foreign} does not belong"):
        db.upsert_photo_embedding(foreign, "m", b"y", verify_workspace=True)
    assert db.masks_features.get_embedding(foreign, "m") == b"x"
    db.upsert_photo_embedding(mine, "m", b"z", verify_workspace=True)
    assert db.masks_features.get_embedding(mine, "m") == b"z"


def test_get_photos_with_embedding_scope_and_offline_folders(db):
    online, offline = _folder(db, "/online"), _folder(db, "/offline")
    a, b = _photo(db, "a.jpg", online), _photo(db, "b.jpg", offline)
    c = _photo(db, "c.jpg", online)
    foreign = _other_workspace_photo(db)
    db.conn.execute("UPDATE folders SET status='missing' WHERE id=?", (offline,))
    db.conn.execute("UPDATE folders SET status='partial' WHERE id=?",
                    (_row(db, c)["folder_id"],))
    db.conn.commit()
    for pid in (a, b, c, foreign):
        db.upsert_photo_embedding(pid, "m", bytes([pid]))
    db.upsert_photo_embedding(a, "m", b"v", variant="v2")
    db.upsert_photo_embedding(a, "n", b"n")

    assert sorted(db.masks_features.photos_with_embedding("m")) == [
        (a, bytes([a])), (c, bytes([c]))]
    assert sorted(db.masks_features.photos_with_embedding(
        "m", include_offline_folders=True)) == [
        (a, bytes([a])), (b, bytes([b])), (c, bytes([c]))]
    assert db.masks_features.photos_with_embedding("m", variant="v2") == [(a, b"v")]
    assert db.masks_features.photos_with_embedding("m", photo_ids=[c, foreign]) == [
        (c, bytes([c]))]
    assert db.masks_features.photos_with_embedding("m", photo_ids=[]) == []
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.masks_features.photos_with_embedding("m", photo_ids=[])


def test_get_photos_with_embedding_chunks_photo_ids(db):
    fid = _folder(db)
    a = _photo(db, "a.jpg", fid)
    db.upsert_photo_embedding(a, "m", b"x")
    statements = []
    db.conn.set_trace_callback(statements.append)
    ids = list(range(100000, 101850)) + [a]
    assert db.masks_features.photos_with_embedding("m", photo_ids=ids) == [(a, b"x")]
    db.conn.set_trace_callback(None)
    selects = [s for s in statements if "FROM photo_embeddings" in s]
    assert len(selects) == 3  # 900 + 900 + 51


# -- workspace scoping --------------------------------------------------------


def _trace(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


def test_scoped_selectors_raise_before_any_sql_without_a_workspace(db):
    """The workspace-scoped selectors need a workspace, exactly where the wrappers did.

    The workspace is resolved lazily, so reaching ``db.masks_features`` with
    none active is fine; each scoped call raises ``RuntimeError`` before
    touching a table, as the wrapper's eagerly scoped repository did.
    """
    db.set_active_workspace(None)
    repo = db.masks_features
    statements = _trace(db)
    try:
        for call in (
            repo.variant_coverage,
            lambda: repo.workspace_photo_ids_with_variant("large"),
            lambda: repo.workspace_mask_candidate_detections(0.2),
            lambda: repo.photos_with_embedding("m"),
            # The empty-ids early return still comes after the resolve.
            lambda: repo.photos_with_embedding("m", photo_ids=[]),
            lambda: repo.photos_missing_masks(None, 0.2),
            lambda: repo.list_photos_for_eye_keypoint_stage(
                0.2, "", [], eye_kp_fingerprint_version="v1",
            ),
            lambda: repo.sam_variant_rerun_warning("large", 0.2, "", []),
        ):
            with pytest.raises(RuntimeError, match="No active workspace"):
                call()
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []


def test_photo_keyed_operations_need_no_workspace(db):
    pid = _photo(db, "a.jpg", _folder(db))
    db.set_active_workspace(None)
    repo = db.masks_features
    repo.upsert_mask(pid, "sam2-small", "/m/a.png", "megadetector-v6", *BOX)
    assert repo.get_mask(pid, "sam2-small")["path"] == "/m/a.png"
    assert [m["variant"] for m in repo.list_masks_for_photo(pid)] == ["sam2-small"]
    repo.update_pipeline_features(pid, mask_path="/m/a.png")
    assert repo.photo_mask_path(pid) == "/m/a.png"
    assert repo.pipeline_feature_row(pid)["mask_path"] == "/m/a.png"
    repo.update_embeddings(pid, b"s", b"g", variant="vit-b14")
    assert repo.get_embedding(pid, "m") is None
    assert [s["variant"] for s in repo.variants_summary()] == ["sam2-small"]
    assert len(repo.find_stale()) == 1  # no detection backs the mask


@pytest.mark.parametrize("call", [
    # ``min_conf`` given: this one reads the config before resolving, as it
    # always did, but still raises before staging its large photo scope.
    lambda db: db.sam_variant_rerun_warning("large", photo_ids=list(range(1, 2000)), min_conf=0.2),
    lambda db: db.get_photos_missing_masks(),
    lambda db: db.list_photos_for_eye_keypoint_stage(photo_ids=[]),
], ids=["sam_variant_rerun_warning", "get_photos_missing_masks", "list_photos_for_eye_keypoint_stage"])
def test_coordinated_selectors_raise_before_reading_config_or_scope(db, monkeypatch, call):
    """The façade selectors still resolve the workspace first.

    Before the lazy resolver they built an eagerly scoped repository before
    staging the photo scope (and, except for the rerun warning, before
    reading the detector floor), so with no workspace they raised first;
    they still do. An empty eye-stage scope still raises instead of
    returning ``[]``.
    """
    def no_config_read():
        raise AssertionError("read the config before resolving the workspace")

    db.set_active_workspace(None)
    monkeypatch.setattr(cfg, "load", no_config_read)
    statements = _trace(db)
    try:
        with pytest.raises(RuntimeError, match="No active workspace"):
            call(db)
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []


def test_scoped_selectors_read_the_workspace_active_at_the_call(db):
    """A fresh repository per access, and each scoped call reads the current workspace."""
    home = db._ws_id()
    mine = _photo(db, "a.jpg", _folder(db))
    foreign = _other_workspace_photo(db)
    _mask(db, mine, "large")
    _mask(db, foreign, "large")
    assert db.masks_features.workspace_photo_ids_with_variant("large") == [mine]
    other = next(w["id"] for w in db.get_workspaces() if w["id"] != home)
    db.set_active_workspace(other)
    assert db.masks_features.workspace_photo_ids_with_variant("large") == [foreign]
    # Even a repository held across the switch reads the workspace per call.
    repo = db.masks_features
    db.set_active_workspace(home)
    assert repo.workspace_photo_ids_with_variant("large") == [mine]


# -- structure ----------------------------------------------------------------

_REMOVED_MASKS_FEATURES_WRAPPERS = (
    "get_photo_mask",
    "list_masks_for_photo",
    "get_workspace_photo_ids_with_mask_variant",
    "get_photo_pipeline_features",
    "get_active_mask_variant",
    "get_photo_mask_path",
    "get_photo_mask_state",
    "delete_masks_for_variant",
    "delete_inactive_masks",
    "find_stale_masks",
    "mask_variant_coverage",
    "mask_variants_summary",
    "upsert_photo_mask",
    "save_subject_raw_analysis",
    "update_photo_pipeline_features",
    "get_workspace_mask_candidate_detections",
    "update_photo_embeddings",
    "get_photo_embedding",
    "get_photos_with_embedding",
)

# Coordinated methods that stay on ``Database``: each reads the workspace
# config, the photo scope or the workspace guard around its repository calls.
_COORDINATED_MASKS_FEATURES_METHODS = (
    "set_active_mask_variant",
    "delete_stale_masks",
    "sam_variant_rerun_warning",
    "get_photos_missing_masks",
    "list_photos_for_eye_keypoint_stage",
    "upsert_photo_embedding",
)


def _self_attrs(fn_obj):
    source = textwrap.dedent(inspect.getsource(fn_obj))
    fn = ast.parse(source).body[0]
    return {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }


def test_masks_features_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.masks_features`` builds a new repository each time, never a cached one.

    The workspace is passed as ``Database._ws_id`` itself, uncalled, so
    building the repository resolves nothing; the cleanup deletes get the
    façade's contained file delete, and ``commit_with_retry`` is the module
    function as it is at access time.
    """
    first, second = db.masks_features, db.masks_features
    assert isinstance(first, MasksFeaturesRepository)
    assert first is not second
    assert first.conn is db.conn
    assert first.workspace_id_fn == db._ws_id
    assert first.remove_file == db._safe_remove_mask_file
    assert first.commit_with_retry is db_module.commit_with_retry


def test_accessor_writes_reach_a_patched_commit_with_retry(db, monkeypatch):
    pid = _photo(db, "a.jpg")
    db.masks_features.get_mask(pid, "sam2-small")  # an access before the patch
    calls = _commit_recorder(monkeypatch)
    db.masks_features.update_pipeline_features(pid, mask_path="/m/a.png")
    assert calls == [db.conn]


def test_masks_features_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.masks_features``; Database keeps no aliases."""
    for name in _REMOVED_MASKS_FEATURES_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.masks_features"
    accessor = Database.__dict__["masks_features"]
    assert isinstance(accessor, property)
    attrs = _self_attrs(accessor.fget)
    assert "_masks_features_repository" in attrs
    assert "conn" not in attrs


@pytest.mark.parametrize("name", _COORDINATED_MASKS_FEATURES_METHODS)
def test_coordinated_masks_features_method_delegates_to_repository(name):
    attrs = _self_attrs(getattr(Database, name))
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to MasksFeaturesRepository"
    )
    assert "_masks_features_repository" in attrs, (
        f"Database.{name} no longer delegates to MasksFeaturesRepository"
    )


@pytest.mark.parametrize("name", ["_masks_dir_real", "_safe_remove_mask_file"])
def test_mask_file_helpers_hold_no_sql(name):
    """The containment check reads ``_db_path`` state and stays on ``Database``."""
    assert "conn" not in _self_attrs(getattr(Database, name))


def test_cleanup_deletes_remove_files_through_the_facade():
    """Every cleanup delete gets ``_safe_remove_mask_file`` from the factory."""
    assert "_safe_remove_mask_file" in _self_attrs(Database._masks_features_repository)


@pytest.mark.parametrize("name", [
    "set_active_mask_variant", "sam_variant_rerun_warning",
    "get_photos_missing_masks", "list_photos_for_eye_keypoint_stage",
])
def test_detector_floor_reads_config_through_the_facade(name):
    assert "get_effective_config" in _self_attrs(getattr(Database, name))


@pytest.mark.parametrize("name", [
    "sam_variant_rerun_warning", "list_photos_for_eye_keypoint_stage",
])
def test_photo_scope_clause_stays_on_the_facade(name):
    assert "_scope_clause" in _self_attrs(getattr(Database, name))


def test_upsert_photo_embedding_guards_through_the_facade():
    assert "_verify_photo_in_workspace" in _self_attrs(Database.upsert_photo_embedding)


def test_repository_imports_no_db_code():
    import repositories.masks_features as module

    tree = ast.parse(inspect.getsource(module))
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree) if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module.split(".")[0]
        for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    assert "db" not in imported
    assert "pipeline" not in imported
    assert "config" not in imported
