"""Behavior pins for the detections domain of ``Database``.

The tests exercise the detection and miss methods only through the public
``Database`` façade, so they hold whether the SQL lives in ``db.py`` or in
``repositories/detections.py``. They cover the content-addressed detection
upsert and its stale-row sweep, the atomic detection batch (runtime
retirement, the review pin, rollback), the threshold-at-read-time readers,
clears and id-based deletes, and the workspace-scoped misses queue.
"""

import ast
import inspect
import json
import sqlite3
import textwrap

import config as cfg
import db as db_module
import pytest
from db import Database
from detection_id import detection_id

MODEL = "megadetector-v6"


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _folder(db, path="/photos"):
    return db.add_folder(path, name=path.strip("/") or "root")


def _photo(db, name="a.jpg", folder_id=None, timestamp=None):
    fid = folder_id if folder_id is not None else _folder(db)
    return db.add_photo(
        folder_id=fid, filename=name, extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp=timestamp, width=1, height=1,
    )


def _bulk_photos(db, folder_id, n, **columns):
    """Insert ``n`` photos with raw SQL (fast) and return their ids."""
    names = ["folder_id", "filename", "extension", "file_size", "file_mtime",
             *columns]
    placeholders = ",".join("?" for _ in names)
    ids = []
    for i in range(n):
        cur = db.conn.execute(
            f"INSERT INTO photos ({','.join(names)}) VALUES ({placeholders})",
            (folder_id, f"bulk{i}.jpg", ".jpg", 1, 1.0, *columns.values()),
        )
        ids.append(cur.lastrowid)
    db.conn.commit()
    return ids


def _box(x=0.1, y=0.1, w=0.2, h=0.2):
    return {"x": x, "y": y, "w": w, "h": h}


def _d(conf=0.9, category="animal", **box):
    det = {"box": _box(**box), "confidence": conf}
    if category is not None:
        det["category"] = category
    return det


def _raw_det(db, photo_id, conf=0.9, model=MODEL, category="animal",
             runtime_fingerprint="legacy", x=0.0):
    cur = db.conn.execute(
        """INSERT INTO detections
             (photo_id, detector_model, runtime_fingerprint,
              box_x, box_y, box_w, box_h, detector_confidence, category)
           VALUES (?, ?, ?, ?, 0.0, 1.0, 1.0, ?, ?)""",
        (photo_id, model, runtime_fingerprint, x, conf, category),
    )
    db.conn.commit()
    return cur.lastrowid


def _det_rows(db, photo_id):
    return db.conn.execute(
        """SELECT id, detector_model, runtime_fingerprint, box_x,
                  detector_confidence, category
           FROM detections WHERE photo_id = ? ORDER BY id""",
        (photo_id,),
    ).fetchall()


def _run_row(db, photo_id, model=MODEL):
    return db.conn.execute(
        """SELECT runtime_fingerprint, input_fingerprint, box_count
           FROM detector_runs WHERE photo_id = ? AND detector_model = ?""",
        (photo_id, model),
    ).fetchone()


def _commit_recorder(monkeypatch):
    calls = []
    real = db_module.commit_with_retry

    def recording(conn, *args, **kwargs):
        calls.append(conn)
        return real(conn, *args, **kwargs)

    monkeypatch.setattr(db_module, "commit_with_retry", recording)
    return calls


def _trace(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


def _config_recorder(monkeypatch, db, effective):
    calls = []

    def fake(base):
        calls.append(base)
        return dict(effective)

    monkeypatch.setattr(db, "get_effective_config", fake)
    return calls


# -- save_detections / _upsert_detection_rows --------------------------------


def test_save_detections_requires_a_detector_model(db):
    pid = _photo(db)
    with pytest.raises(ValueError, match="detector_model is required"):
        db.save_detections(pid, [_d()], None)
    assert _det_rows(db, pid) == []


def test_save_detections_writes_content_addressed_rows_and_commits(db, monkeypatch):
    pid = _photo(db)
    commits = _commit_recorder(monkeypatch)
    ids = db.save_detections(
        pid,
        [_d(0.8, x=0.1), _d(0.7, category=None, x=0.5), _d(0.6, "person", x=0.7)],
        MODEL,
        runtime_fingerprint="rt-1",
    )
    assert ids == [
        detection_id(pid, MODEL, (0.1, 0.1, 0.2, 0.2), "animal"),
        detection_id(pid, MODEL, (0.5, 0.1, 0.2, 0.2), "animal"),
        detection_id(pid, MODEL, (0.7, 0.1, 0.2, 0.2), "person"),
    ]
    assert commits == [db.conn]
    assert not db.conn.in_transaction
    with _reader(db) as reader:
        rows = reader.execute(
            """SELECT id, runtime_fingerprint, detector_confidence, category
               FROM detections WHERE photo_id = ?""", (pid,),
        ).fetchall()
    assert {r["id"]: (r["runtime_fingerprint"], r["detector_confidence"],
                      r["category"]) for r in rows} == {
        ids[0]: ("rt-1", 0.8, "animal"),
        ids[1]: ("rt-1", 0.7, "animal"),
        ids[2]: ("rt-1", 0.6, "person"),
    }
    # save_detections never touches detector_runs.
    assert _run_row(db, pid) is None


def test_save_detections_defaults_runtime_fingerprint_to_legacy(db):
    pid = _photo(db)
    db.save_detections(pid, [_d()], MODEL)
    assert [r["runtime_fingerprint"] for r in _det_rows(db, pid)] == ["legacy"]


def test_duplicate_boxes_keep_highest_confidence_then_latest(db):
    pid = _photo(db)
    ids = db.save_detections(
        pid,
        [
            _d(0.5, x=0.1),   # A, first seen
            _d(0.3, x=0.4),   # B
            _d(0.9, x=0.1),   # A again, higher: wins
            _d(0.3, x=0.4),   # B again, equal: later index wins (same values)
            _d(0.2, x=0.1),   # A again, lower: ignored
        ],
        MODEL,
    )
    a = detection_id(pid, MODEL, (0.1, 0.1, 0.2, 0.2), "animal")
    b = detection_id(pid, MODEL, (0.4, 0.1, 0.2, 0.2), "animal")
    assert ids == [a, b]
    confs = {r["id"]: r["detector_confidence"] for r in _det_rows(db, pid)}
    assert confs == {a: 0.9, b: 0.3}


def test_resave_upserts_in_place_and_keeps_predictions(db):
    pid = _photo(db)
    (det_id,) = db.save_detections(pid, [_d(0.5)], MODEL)
    db.add_prediction(det_id, species="Robin", confidence=0.9, model="clf")
    statements = _trace(db)
    assert db.save_detections(pid, [_d(0.75)], MODEL) == [det_id]
    db.conn.set_trace_callback(None)
    assert not any("INSERT OR REPLACE" in s for s in statements)
    assert [r["detector_confidence"] for r in _det_rows(db, pid)] == [0.75]
    assert db.conn.execute(
        "SELECT COUNT(*) FROM predictions WHERE detection_id = ?", (det_id,),
    ).fetchone()[0] == 1


def test_resave_retires_only_same_model_same_runtime_stale_rows(db):
    pid = _photo(db)
    keep, stale = db.save_detections(
        pid, [_d(0.9, x=0.1), _d(0.8, x=0.3)], MODEL, runtime_fingerprint="rt",
    )
    other_runtime = _raw_det(db, pid, runtime_fingerprint="other", x=0.9)
    other_model = _raw_det(db, pid, model="other-det", runtime_fingerprint="rt")

    assert db.save_detections(
        pid, [_d(0.9, x=0.1)], MODEL, runtime_fingerprint="rt",
    ) == [keep]
    ids = {r["id"] for r in _det_rows(db, pid)}
    assert ids == {keep, other_runtime, other_model}
    assert stale not in ids


def test_save_empty_list_clears_same_runtime_rows(db):
    pid = _photo(db)
    db.save_detections(pid, [_d(x=0.1), _d(x=0.2)], MODEL)
    assert db.save_detections(pid, [], MODEL) == []
    assert _det_rows(db, pid) == []


def test_upsert_detection_rows_leaves_the_transaction_open(db):
    pid = _photo(db)
    ids = db._upsert_detection_rows(pid, MODEL, [_d(x=0.1)], "rt")
    assert db.conn.in_transaction
    with _reader(db) as reader:
        assert reader.execute(
            "SELECT COUNT(*) FROM detections WHERE photo_id = ?", (pid,),
        ).fetchone()[0] == 0
    db.conn.rollback()
    assert ids == [detection_id(pid, MODEL, (0.1, 0.1, 0.2, 0.2), "animal")]
    assert _det_rows(db, pid) == []


def test_stale_sweep_deletes_in_chunks_of_500(db):
    pid = _photo(db)
    for i in range(1001):
        _raw_det(db, pid, x=i / 10000)
    statements = _trace(db)
    db.save_detections(pid, [], MODEL)
    db.conn.set_trace_callback(None)
    # Cascade sub-programs re-report the statement; count distinct ones.
    deletes = list(dict.fromkeys(
        s for s in statements if s.startswith("DELETE FROM detections")))
    assert [s.count(",") + 1 for s in deletes] == [500, 500, 1]
    assert _det_rows(db, pid) == []


# -- write_detection_batch ---------------------------------------------------


def test_write_detection_batch_requires_a_detector_model(db):
    pid = _photo(db)
    with pytest.raises(ValueError, match="detector_model is required"):
        db.write_detection_batch(pid, None, [_d()])


def test_write_detection_batch_records_run_and_commits_once(db, monkeypatch):
    pid = _photo(db)
    commits = _commit_recorder(monkeypatch)
    ids = db.write_detection_batch(
        pid, MODEL, [_d(0.9, x=0.1), _d(0.8, x=0.2)],
        runtime_fingerprint="rt", input_fingerprint="in",
    )
    assert commits == [db.conn]
    assert not db.conn.in_transaction
    with _reader(db) as reader:
        run = reader.execute(
            """SELECT runtime_fingerprint, input_fingerprint, box_count
               FROM detector_runs WHERE photo_id = ?""", (pid,),
        ).fetchone()
        det_ids = [r["id"] for r in reader.execute(
            "SELECT id FROM detections WHERE photo_id = ?", (pid,),
        )]
    assert tuple(run) == ("rt", "in", 2)
    assert sorted(det_ids) == sorted(ids)


def test_write_detection_batch_empty_scene_records_zero_boxes(db):
    pid = _photo(db)
    db.write_detection_batch(pid, MODEL, [_d()])
    assert db.write_detection_batch(pid, MODEL, []) == []
    assert _det_rows(db, pid) == []
    assert tuple(_run_row(db, pid)) == ("legacy", None, 0)


def test_write_detection_batch_same_identity_keeps_other_runtime_rows(db):
    pid = _photo(db)
    db.write_detection_batch(pid, MODEL, [_d(x=0.1)],
                             runtime_fingerprint="rt", input_fingerprint="in")
    leftover = _raw_det(db, pid, runtime_fingerprint="old", x=0.9)
    # input_fingerprint None on the new call never counts as a change.
    ids = db.write_detection_batch(pid, MODEL, [_d(x=0.2)],
                                   runtime_fingerprint="rt")
    assert {r["id"] for r in _det_rows(db, pid)} == {ids[0], leftover}
    assert tuple(_run_row(db, pid)) == ("rt", None, 1)


@pytest.mark.parametrize("change", [
    {"runtime_fingerprint": "rt-2", "input_fingerprint": "in"},
    {"runtime_fingerprint": "rt", "input_fingerprint": "in-2"},
])
def test_identity_change_retires_every_row_for_the_model(db, change):
    pid = _photo(db)
    db.write_detection_batch(pid, MODEL, [_d(x=0.1)],
                             runtime_fingerprint="rt", input_fingerprint="in")
    leftover = _raw_det(db, pid, runtime_fingerprint="old", x=0.9)
    other_model = _raw_det(db, pid, model="other-det")
    ids = db.write_detection_batch(pid, MODEL, [_d(x=0.2)], **change)
    assert {r["id"] for r in _det_rows(db, pid)} == {ids[0], other_model}
    assert leftover not in ids
    assert tuple(_run_row(db, pid)) == (
        change["runtime_fingerprint"], change["input_fingerprint"], 1,
    )


def test_pinned_identity_change_returns_existing_ids_untouched(db, monkeypatch):
    pid = _photo(db)
    db.write_detection_batch(pid, MODEL, [_d(0.5, x=0.1), _d(0.9, x=0.2)],
                             runtime_fingerprint="rt")
    tie = _raw_det(db, pid, conf=0.5, runtime_fingerprint="rt", x=0.3)
    calls = []

    def pinned(photo_id, detector_model):
        calls.append((photo_id, detector_model))
        return True

    monkeypatch.setattr(db, "detector_run_is_pinned", pinned)
    before = [tuple(r) for r in _det_rows(db, pid)]
    ids = db.write_detection_batch(pid, MODEL, [_d(0.7, x=0.6)],
                                   runtime_fingerprint="rt-2")
    assert calls == [(pid, MODEL)]
    by_conf = sorted(
        before, key=lambda r: (-r[4], r[0]),
    )
    assert ids == [r[0] for r in by_conf]
    assert tie in ids
    assert [tuple(r) for r in _det_rows(db, pid)] == before
    assert _run_row(db, pid)["runtime_fingerprint"] == "rt"


def test_real_manual_review_pins_the_detector_run(db):
    pid = _photo(db)
    (det_id,) = db.write_detection_batch(pid, MODEL, [_d()],
                                         runtime_fingerprint="rt")
    db.add_prediction(det_id, species="Robin", confidence=0.9, model="clf")
    pred_id = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (det_id,),
    ).fetchone()["id"]
    db.set_review_status(pred_id, db._ws_id(), "accepted")
    assert db.write_detection_batch(pid, MODEL, [_d(x=0.5)],
                                    runtime_fingerprint="rt-2") == [det_id]
    assert db.conn.execute(
        "SELECT COUNT(*) FROM predictions WHERE detection_id = ?", (det_id,),
    ).fetchone()[0] == 1


def test_force_runtime_replace_skips_the_pin_check(db, monkeypatch):
    pid = _photo(db)
    db.write_detection_batch(pid, MODEL, [_d(x=0.1)], runtime_fingerprint="rt")

    def boom(*args):
        raise AssertionError("pin check must be skipped")

    monkeypatch.setattr(db, "detector_run_is_pinned", boom)
    ids = db.write_detection_batch(pid, MODEL, [_d(x=0.5)],
                                   runtime_fingerprint="rt-2",
                                   force_runtime_replace=True)
    assert [r["id"] for r in _det_rows(db, pid)] == ids
    # Unchanged identity never consults the pin either.
    db.write_detection_batch(pid, MODEL, [_d(x=0.5)], runtime_fingerprint="rt-2")


def test_first_run_never_consults_the_pin(db, monkeypatch):
    pid = _photo(db)
    monkeypatch.setattr(db, "detector_run_is_pinned",
                        lambda *a: pytest.fail("no previous run"))
    assert len(db.write_detection_batch(pid, MODEL, [_d()])) == 1


def test_force_replace_with_no_run_record_retires_stray_rows(db):
    pid = _photo(db)
    # A stray row with a different runtime and no ``detector_runs`` entry:
    # a torn state ``save_detections`` (which never touches ``detector_runs``)
    # or a legacy write can leave behind. Same-model, different box: the new
    # write's same-runtime sweep would not touch it.
    stray = _raw_det(db, pid, runtime_fingerprint="old-rt", x=0.9)
    other_model = _raw_det(db, pid, model="other-det",
                           runtime_fingerprint="old-rt")
    ids = db.write_detection_batch(pid, MODEL, [_d(x=0.1)],
                                   runtime_fingerprint="rt-new",
                                   force_runtime_replace=True)
    remaining = {r["id"] for r in _det_rows(db, pid)}
    assert remaining == {ids[0], other_model}
    assert stray not in remaining
    # ``detector_runs`` now records the new authoritative run.
    assert _run_row(db, pid)["runtime_fingerprint"] == "rt-new"


def test_no_force_replace_with_no_run_record_keeps_stray_rows(db):
    pid = _photo(db)
    stray = _raw_det(db, pid, runtime_fingerprint="old-rt", x=0.9)
    ids = db.write_detection_batch(pid, MODEL, [_d(x=0.1)],
                                   runtime_fingerprint="rt-new")
    # Non-forced writes keep unrelated-runtime rows: the pre-existing
    # non-reclassify behavior for save_detections stays intact.
    remaining = {r["id"] for r in _det_rows(db, pid)}
    assert remaining == {ids[0], stray}


def test_write_detection_batch_rolls_back_on_failure(db):
    pid = _photo(db)
    first = db.write_detection_batch(pid, MODEL, [_d(x=0.1)],
                                     runtime_fingerprint="rt")
    db.conn.execute(
        """CREATE TEMP TRIGGER fail_run BEFORE UPDATE ON detector_runs
           BEGIN SELECT RAISE(ABORT, 'run write failed'); END""",
    )
    with pytest.raises(sqlite3.IntegrityError, match="run write failed"):
        db.write_detection_batch(pid, MODEL, [_d(x=0.5)],
                                 runtime_fingerprint="rt-2")
    assert not db.conn.in_transaction
    assert [r["id"] for r in _det_rows(db, pid)] == first
    assert _run_row(db, pid)["runtime_fingerprint"] == "rt"


# -- readers -----------------------------------------------------------------


def test_get_detections_filters_at_read_time_and_orders_primary_first(db):
    pid = _photo(db)
    low = _raw_det(db, pid, conf=0.1)
    person = _raw_det(db, pid, conf=0.95, category="person")
    full = _raw_det(db, pid, conf=0.99, model="full-image")
    mid = _raw_det(db, pid, conf=0.6)
    high = _raw_det(db, pid, conf=0.8)
    rows = db.get_detections(pid, min_conf=0.5)
    assert isinstance(rows[0], sqlite3.Row)
    assert [r["id"] for r in rows] == [high, mid, full, person]
    assert [r["id"] for r in db.get_detections(pid, min_conf=0)] == [
        high, mid, low, full, person,
    ]
    assert [r["id"] for r in db.get_detections(
        pid, min_conf=0, detector_model="full-image")] == [full]


def test_get_detections_manual_choice_then_quality_beat_confidence(db):
    pid = _photo(db)
    a = _raw_det(db, pid, conf=0.9)
    b = _raw_det(db, pid, conf=0.5)
    c = _raw_det(db, pid, conf=0.4)
    db.conn.execute(
        """INSERT INTO detection_subjects
             (detection_id, source_key, crop, quality_score, exposure_ev, features)
           VALUES (?, 'k', '{}', 0.99, 0.0, '{}')""", (c,),
    )
    db.conn.commit()
    assert [r["id"] for r in db.get_detections(pid, min_conf=0)] == [c, a, b]
    db.conn.execute(
        "INSERT INTO photo_subject_choices (photo_id, detection_id) VALUES (?, ?)",
        (pid, b),
    )
    db.conn.commit()
    assert [r["id"] for r in db.get_detections(pid, min_conf=0)] == [b, c, a]


def test_get_detections_default_floor_comes_from_workspace_config(db, monkeypatch):
    pid = _photo(db)
    _raw_det(db, pid, conf=0.3)
    keep = _raw_det(db, pid, conf=0.7)
    calls = _config_recorder(monkeypatch, db, {"detector_confidence": 0.5})
    assert [r["id"] for r in db.get_detections(pid)] == [keep]
    assert len(calls) == 1
    _config_recorder(monkeypatch, db, {})
    assert len(db.get_detections(pid)) == 2  # default floor 0.2


def test_get_detections_for_photos_shape_and_omissions(db):
    fid = _folder(db)
    p1, p2, p3 = (_photo(db, n, fid) for n in ("1.jpg", "2.jpg", "3.jpg"))
    a = _raw_det(db, p1, conf=0.6)
    b = _raw_det(db, p1, conf=0.9, model="other-det", category="animal", x=0.5)
    _raw_det(db, p2, conf=0.1)
    result = db.get_detections_for_photos([p1, p2, p3], min_conf=0.5)
    assert result == {p1: [
        {"id": b, "x": 0.5, "y": 0.0, "w": 1.0, "h": 1.0, "confidence": 0.9,
         "category": "animal", "detector_model": "other-det"},
        {"id": a, "x": 0.0, "y": 0.0, "w": 1.0, "h": 1.0, "confidence": 0.6,
         "category": "animal", "detector_model": MODEL},
    ]}
    assert [d["id"] for d in db.get_detections_for_photos(
        [p1], min_conf=0, detector_model=MODEL)[p1]] == [a]


def test_get_detections_for_photos_empty_input_skips_config(db, monkeypatch):
    monkeypatch.setattr(db, "get_effective_config",
                        lambda base: pytest.fail("config read"))
    assert db.get_detections_for_photos([]) == {}
    assert db.get_detections_for_photos(()) == {}


def test_get_detections_for_photos_default_floor(db, monkeypatch):
    pid = _photo(db)
    _raw_det(db, pid, conf=0.3)
    calls = _config_recorder(monkeypatch, db, {"detector_confidence": 0.5})
    assert db.get_detections_for_photos([pid]) == {}
    assert len(calls) == 1
    _config_recorder(monkeypatch, db, {})
    assert len(db.get_detections_for_photos([pid])[pid]) == 1


def test_get_detections_for_photos_dedups_across_chunks(db):
    fid = _folder(db)
    target = _photo(db, "t.jpg", fid)
    det = _raw_det(db, target)
    fillers = _bulk_photos(db, fid, db_module._SQLITE_PARAM_CHUNK_SIZE)
    statements = _trace(db)
    result = db.get_detections_for_photos([target, *fillers, target], min_conf=0)
    db.conn.set_trace_callback(None)
    selects = [s for s in statements if "FROM detections" in s]
    assert len(selects) == 2
    assert [d["id"] for d in result[target]] == [det]
    assert list(result) == [target]


def test_get_predictions_for_detection_filters_and_orders(db, monkeypatch):
    pid = _photo(db)
    (det_id,) = db.save_detections(pid, [_d()], MODEL)
    for species, conf, model, fp in [
        ("Robin", 0.4, "clf-a", "fp1"),
        ("Wren", 0.9, "clf-a", "fp1"),
        ("Jay", 0.7, "clf-b", "fp1"),
        ("Crow", 0.8, "clf-a", "fp2"),
    ]:
        db.add_prediction(det_id, species=species, confidence=conf,
                          model=model, labels_fingerprint=fp)

    def species(rows):
        return [r["species"] for r in rows]

    assert species(db.get_predictions_for_detection(
        det_id, min_classifier_conf=0)) == ["Wren", "Crow", "Jay", "Robin"]
    assert species(db.get_predictions_for_detection(
        det_id, min_classifier_conf=0.5, classifier_model="clf-a",
    )) == ["Wren", "Crow"]
    assert species(db.get_predictions_for_detection(
        det_id, min_classifier_conf=0, classifier_model="clf-a",
        labels_fingerprint="fp1",
    )) == ["Wren", "Robin"]
    calls = _config_recorder(monkeypatch, db, {"classifier_confidence": 0.75})
    assert species(db.get_predictions_for_detection(det_id)) == ["Wren", "Crow"]
    assert len(calls) == 1
    _config_recorder(monkeypatch, db, {})
    assert len(db.get_predictions_for_detection(det_id)) == 4


# -- clears and deletes ------------------------------------------------------


def test_clear_detections_for_every_model(db):
    fid = _folder(db)
    pid, other = _photo(db, "a.jpg", fid), _photo(db, "b.jpg", fid)
    db.write_detection_batch(pid, MODEL, [_d()])
    db.write_detection_batch(pid, "other-det", [_d()])
    db.write_detection_batch(other, MODEL, [_d()])
    db.clear_detections(pid)
    assert not db.conn.in_transaction
    with _reader(db) as reader:
        assert reader.execute(
            "SELECT COUNT(*) FROM detections WHERE photo_id = ?", (pid,),
        ).fetchone()[0] == 0
        assert reader.execute(
            "SELECT COUNT(*) FROM detector_runs WHERE photo_id = ?", (pid,),
        ).fetchone()[0] == 0
    assert len(_det_rows(db, other)) == 1
    assert _run_row(db, other) is not None


def test_clear_detections_for_one_model(db):
    pid = _photo(db)
    db.write_detection_batch(pid, MODEL, [_d()])
    (kept,) = db.write_detection_batch(pid, "other-det", [_d()])
    db.clear_detections(pid, detector_model=MODEL)
    assert not db.conn.in_transaction
    assert [r["id"] for r in _det_rows(db, pid)] == [kept]
    assert _run_row(db, pid) is None
    assert _run_row(db, pid, "other-det") is not None


def test_get_existing_detection_photo_ids_delegates(db, monkeypatch):
    calls = []

    def fake(detector_model):
        calls.append(detector_model)
        return {1, 2}

    monkeypatch.setattr(db, "get_detector_run_photo_ids", fake)
    assert db.get_existing_detection_photo_ids() == {1, 2}
    assert db.get_existing_detection_photo_ids("other") == {1, 2}
    assert calls == ["megadetector-v6", "other"]


def test_get_detection_ids_for_photos(db):
    fid = _folder(db)
    p1, p2, p3 = (_photo(db, n, fid) for n in ("1.jpg", "2.jpg", "3.jpg"))
    a = _raw_det(db, p1, conf=0.01)
    b = _raw_det(db, p1, model="other-det")
    c = _raw_det(db, p2)
    assert db.get_detection_ids_for_photos([]) == {}
    assert db.get_detection_ids_for_photos(
        iter([p1, p2, p3])) == {p1: {a, b}, p2: {c}}


def test_get_detection_ids_for_photos_chunks_by_900(db):
    fid = _folder(db)
    pids = _bulk_photos(db, fid, 901)
    det = _raw_det(db, pids[-1])
    statements = _trace(db)
    assert db.get_detection_ids_for_photos(pids) == {pids[-1]: {det}}
    db.conn.set_trace_callback(None)
    selects = [s for s in statements if "FROM detections" in s]
    assert [s.count(",") + 1 for s in selects] == [900 + 1, 1 + 1]


def test_delete_detections_by_ids_empty_is_a_no_op(db):
    statements = _trace(db)
    assert db.delete_detections_by_ids([]) is None
    db.conn.set_trace_callback(None)
    assert statements == []


def test_delete_detections_by_ids_cascades_and_commits(db):
    pid = _photo(db)
    a, b = db.save_detections(pid, [_d(x=0.1), _d(x=0.2)], MODEL)
    db.add_prediction(a, species="Robin", confidence=0.9, model="clf")
    db.delete_detections_by_ids({a})
    assert not db.conn.in_transaction
    with _reader(db) as reader:
        assert [r["id"] for r in reader.execute(
            "SELECT id FROM detections WHERE photo_id = ?", (pid,),
        )] == [b]
        assert reader.execute(
            "SELECT COUNT(*) FROM predictions WHERE detection_id = ?", (a,),
        ).fetchone()[0] == 0


def test_delete_detections_by_ids_resyncs_subject_state(db):
    fid = _folder(db)
    moved, cleared = _photo(db, "m.jpg", fid), _photo(db, "c.jpg", fid)
    top = _raw_det(db, moved, conf=0.9)
    nxt = _raw_det(db, moved, conf=0.8)
    only = _raw_det(db, cleared, conf=0.9)
    db.conn.executemany(
        "INSERT INTO photo_subject_state (photo_id, detection_id) VALUES (?, ?)",
        [(moved, top), (cleared, only)],
    )
    db.conn.execute("UPDATE photos SET quality_score = 0.5 WHERE id = ?",
                    (cleared,))
    db.conn.commit()
    db.delete_detections_by_ids([top, only])
    assert not db.conn.in_transaction
    with _reader(db) as reader:
        state = dict(reader.execute(
            "SELECT photo_id, detection_id FROM photo_subject_state",
        ).fetchall())
        quality = reader.execute(
            "SELECT quality_score FROM photos WHERE id = ?", (cleared,),
        ).fetchone()[0]
    assert state == {moved: nxt}
    assert quality is None


def test_delete_detections_by_ids_chunks_by_900(db):
    pid = _photo(db)
    ids = [_raw_det(db, pid, x=i / 10000) for i in range(901)]
    statements = _trace(db)
    db.delete_detections_by_ids(ids)
    db.conn.set_trace_callback(None)
    # Cascade sub-programs re-report the statement; count distinct ones.
    deletes = list(dict.fromkeys(
        s for s in statements if s.startswith("DELETE FROM detections")))
    assert [s.count(",") + 1 for s in deletes] == [900, 1]
    assert _det_rows(db, pid) == []


# -- misses ------------------------------------------------------------------


def _miss(db, pid, **cols):
    sets = ", ".join(f"{k} = ?" for k in cols)
    db.conn.execute(f"UPDATE photos SET {sets} WHERE id = ?",
                    (*cols.values(), pid))
    db.conn.commit()


def test_list_misses_categories_order_and_exclusions(db):
    fid = _folder(db)
    ns = _photo(db, "ns.jpg", fid, timestamp="2024-01-01T00:00:00")
    clip = _photo(db, "clip.jpg", fid, timestamp="2024-01-03T00:00:00")
    oof = _photo(db, "oof.jpg", fid, timestamp="2024-01-02T00:00:00")
    rejected = _photo(db, "rej.jpg", fid)
    null_flag = _photo(db, "null.jpg", fid, timestamp="2023-01-01T00:00:00")
    _photo(db, "clean.jpg", fid)
    _miss(db, ns, miss_no_subject=1)
    _miss(db, clip, miss_clipped=1)
    _miss(db, oof, miss_oof=1)
    _miss(db, rejected, miss_clipped=1, flag="rejected")
    _miss(db, null_flag, miss_oof=1, flag=None)

    assert [m["id"] for m in db.list_misses()] == [clip, oof, ns, null_flag]
    assert [m["id"] for m in db.list_misses("no_subject")] == [ns]
    assert [m["id"] for m in db.list_misses("clipped")] == [clip]
    assert [m["id"] for m in db.list_misses("oof")] == [oof, null_flag]
    with pytest.raises(KeyError):
        db.list_misses("blurry")
    row = db.list_misses("clipped")[0]
    assert set(row) == {
        "id", "folder_id", "filename", "companion_path", "timestamp",
        "burst_id", "subject_size", "crop_complete", "subject_tenengrad",
        "bg_tenengrad", "miss_no_subject", "miss_clipped", "miss_oof",
        "miss_computed_at", "flag", "raw_detection_conf",
        "detector_confidence_threshold", "detection_box", "detection_conf",
    }


def test_list_misses_since_and_photo_scope(db):
    fid = _folder(db)
    old, new = _photo(db, "old.jpg", fid), _photo(db, "new.jpg", fid)
    _miss(db, old, miss_clipped=1, miss_computed_at="2024-01-01")
    _miss(db, new, miss_clipped=1, miss_computed_at="2024-02-01")
    assert [m["id"] for m in db.list_misses(since="2024-01-15")] == [new]
    assert [m["id"] for m in db.list_misses("clipped", since="2024-01-15")] == [new]
    assert db.list_misses(photo_ids=[]) == []
    assert [m["id"] for m in db.list_misses(photo_ids={old})] == [old]
    assert db.list_misses(since="2024-01-15", photo_ids=[old]) == []


def test_list_misses_large_photo_scope_is_staged(db):
    fid = _folder(db)
    pids = _bulk_photos(db, fid, db_module._SQLITE_PARAM_CHUNK_SIZE + 1,
                        miss_oof=1)
    got = db.list_misses(photo_ids=pids[1:])
    assert {m["id"] for m in got} == set(pids[1:])


def test_list_misses_is_scoped_to_the_active_workspace(db):
    fid = _folder(db)
    mine = _photo(db, "mine.jpg", fid)
    _miss(db, mine, miss_clipped=1)
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    assert db.list_misses() == []
    assert db.bulk_reject_miss_category("clipped") == []


def test_list_misses_requires_an_active_workspace_before_category(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.list_misses("blurry")


def test_list_misses_empty_skips_config(db, monkeypatch):
    _photo(db)
    monkeypatch.setattr(db, "get_effective_config",
                        lambda base: pytest.fail("config read"))
    assert db.list_misses() == []


def test_list_misses_attaches_primary_and_raw_detections(db, monkeypatch):
    fid = _folder(db)
    calls = _config_recorder(monkeypatch, db, {"detector_confidence": 0.5})
    strong = _photo(db, "s.jpg", fid, timestamp="2024-03-01")
    weak = _photo(db, "w.jpg", fid, timestamp="2024-02-01")
    none = _photo(db, "n.jpg", fid, timestamp="2024-01-01")
    for pid in (strong, weak, none):
        _miss(db, pid, miss_clipped=1)
    _raw_det(db, strong, conf=0.6, x=0.1)
    _raw_det(db, strong, conf=0.8, x=0.2)
    _raw_det(db, strong, conf=0.99, category="person")
    _raw_det(db, strong, conf=0.97, model="full-image")
    _raw_det(db, weak, conf=0.3)
    _raw_det(db, weak, conf=0.4, x=0.3)
    _raw_det(db, none, conf=0.9, category="vehicle")

    misses = {m["id"]: m for m in db.list_misses()}
    assert len(calls) == 1
    s, w, n = misses[strong], misses[weak], misses[none]
    assert s["detection_conf"] == 0.8
    assert json.loads(s["detection_box"]) == {"x": 0.2, "y": 0.0, "w": 1.0, "h": 1.0}
    assert s["raw_detection_conf"] == 0.8
    assert w["detection_box"] is None and w["detection_conf"] is None
    assert w["raw_detection_conf"] == 0.4
    assert n["raw_detection_conf"] is None and n["detection_conf"] is None
    assert {m["detector_confidence_threshold"] for m in misses.values()} == {0.5}


def test_list_misses_default_floor_and_detection_chunks(db):
    fid = _folder(db)
    pids = _bulk_photos(db, fid, 501, miss_oof=1)
    _raw_det(db, pids[0], conf=0.25)
    _raw_det(db, pids[-1], conf=0.15)
    statements = _trace(db)
    misses = {m["id"]: m for m in db.list_misses()}
    db.conn.set_trace_callback(None)
    det_selects = [s for s in statements if "FROM detections" in s
                   and "photo_id IN" in s]
    assert len(det_selects) == 2
    assert misses[pids[0]]["detection_conf"] == 0.25
    assert misses[pids[-1]]["detection_conf"] is None
    assert misses[pids[-1]]["raw_detection_conf"] == 0.15
    assert misses[pids[0]]["detector_confidence_threshold"] == 0.2


def test_clear_miss_flag_zeroes_one_column_and_commits(db):
    pid = _photo(db)
    _miss(db, pid, miss_no_subject=1, miss_clipped=1, miss_oof=1)
    db.clear_miss_flag(pid, "oof")
    db.clear_miss_flag(pid, "no_subject")
    assert not db.conn.in_transaction
    with _reader(db) as reader:
        row = reader.execute(
            "SELECT miss_no_subject, miss_clipped, miss_oof FROM photos WHERE id = ?",
            (pid,),
        ).fetchone()
    assert tuple(row) == (0, 1, 0)
    with pytest.raises(KeyError):
        db.clear_miss_flag(pid, "blurry")


def test_clear_miss_flag_rejects_other_workspace_photos_first(db):
    pid = _photo(db)
    _miss(db, pid, miss_clipped=1)
    db.set_active_workspace(db.create_workspace("Other"))
    with pytest.raises(ValueError, match="does not belong to the active workspace"):
        db.clear_miss_flag(pid, "blurry")
    assert db.conn.execute(
        "SELECT miss_clipped FROM photos WHERE id = ?", (pid,),
    ).fetchone()[0] == 1


def test_bulk_reject_returns_old_values_and_commits(db):
    fid = _folder(db)
    a, b, c, d = (_photo(db, n, fid) for n in ("a.jpg", "b.jpg", "c.jpg", "d.jpg"))
    _miss(db, a, miss_clipped=1)
    _miss(db, b, miss_clipped=1, flag=None)
    _miss(db, c, miss_clipped=1, flag="rejected")
    _miss(db, d, miss_oof=1, flag="flagged")
    affected = db.bulk_reject_miss_category("clipped")
    assert sorted(affected, key=lambda r: r["photo_id"]) == [
        {"photo_id": a, "old_value": "none"},
        {"photo_id": b, "old_value": None},
    ]
    assert not db.conn.in_transaction
    with _reader(db) as reader:
        flags = dict(reader.execute("SELECT id, flag FROM photos").fetchall())
    assert flags == {a: "rejected", b: "rejected", c: "rejected", d: "flagged"}


def test_bulk_reject_since_and_scope(db):
    fid = _folder(db)
    old, new, other = (_photo(db, n, fid) for n in ("o.jpg", "n.jpg", "x.jpg"))
    _miss(db, old, miss_oof=1, miss_computed_at="2024-01-01")
    _miss(db, new, miss_oof=1, miss_computed_at="2024-02-01")
    _miss(db, other, miss_oof=1, miss_computed_at="2024-02-01")
    assert db.bulk_reject_miss_category("oof", photo_ids=[]) == []
    assert db.bulk_reject_miss_category(
        "oof", since="2024-01-15", photo_ids=[new, old],
    ) == [{"photo_id": new, "old_value": "none"}]
    assert db.bulk_reject_miss_category("oof", since="2024-01-15") == [
        {"photo_id": other, "old_value": "none"},
    ]
    assert db.bulk_reject_miss_category("oof") == [
        {"photo_id": old, "old_value": "none"},
    ]


def test_bulk_reject_validates_category_before_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(KeyError):
        db.bulk_reject_miss_category("blurry")
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.bulk_reject_miss_category("oof")


def test_bulk_reject_updates_in_chunks_of_500(db):
    fid = _folder(db)
    pids = _bulk_photos(db, fid, 501, miss_no_subject=1)
    statements = _trace(db)
    affected = db.bulk_reject_miss_category("no_subject")
    db.conn.set_trace_callback(None)
    # Distinct texts, in order: the trace callback re-reports the parent
    # statement each time the per-row duplicate_rejections trigger fires.
    updates = list(dict.fromkeys(
        s for s in statements if s.startswith("UPDATE photos SET flag")
    ))
    assert [s.count(",") + 1 for s in updates] == [500, 1]
    assert {a["photo_id"] for a in affected} == set(pids)


# -- structure ---------------------------------------------------------------

_DELEGATING_DETECTION_METHODS = (
    "save_detections",
    "_upsert_detection_rows",
    "write_detection_batch",
    "get_detections",
    "get_detections_for_photos",
    "get_predictions_for_detection",
    "clear_detections",
    "list_misses",
    "clear_miss_flag",
    "bulk_reject_miss_category",
    "get_detection_ids_for_photos",
    "delete_detections_by_ids",
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


@pytest.mark.parametrize("name", _DELEGATING_DETECTION_METHODS)
def test_detection_method_delegates_to_repository(name):
    attrs = _self_attrs(getattr(Database, name))
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to DetectionsRepository"
    )
    assert "_detections_repository" in attrs, (
        f"Database.{name} no longer delegates to DetectionsRepository"
    )


def test_existing_detection_photo_ids_composes_through_the_facade():
    attrs = _self_attrs(Database.get_existing_detection_photo_ids)
    assert "conn" not in attrs
    assert "get_detector_run_photo_ids" in attrs


def test_pin_check_stays_on_the_facade():
    """``detector_run_is_pinned`` belongs to the model-runs domain."""
    assert "detector_run_is_pinned" in _self_attrs(Database.write_detection_batch)


@pytest.mark.parametrize("name", [
    "get_detections", "get_detections_for_photos",
    "get_predictions_for_detection", "list_misses",
])
def test_floors_read_config_through_the_facade(name):
    assert "get_effective_config" in _self_attrs(getattr(Database, name))


@pytest.mark.parametrize("name", ["list_misses", "bulk_reject_miss_category"])
def test_misses_scope_through_the_facade(name):
    attrs = _self_attrs(getattr(Database, name))
    assert {"_ws_id", "_scope_clause"} <= attrs


def test_repository_imports_no_db_code():
    import repositories.detections as module

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
