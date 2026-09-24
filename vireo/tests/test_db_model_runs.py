"""Behavior pins for the model-runs domain of ``Database``.

The behavior tests exercise the model-run methods only through the public
``Database`` façade, so they hold whether the SQL lives in ``db.py`` or in
``repositories/model_runs.py``; the structural tests at the end keep it in
the repository. They cover detector runs (recording, global stats, the
review pin, the cached-run skip set), classifier runs and their runtime
gates, match scores, the classify preflight's cache-hit and
unclassifiable sets, and the labels-fingerprint sidecar.
"""

import ast
import inspect
import json
import sqlite3
import textwrap

import config as cfg
import db as db_module
import pytest
from db import AUTO_MATCH_REVIEW_MARKER, Database

MODEL = "BioCLIP-2.5"


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _folder(db):
    return db.add_folder("/photos", name="photos")


def _photo(db, name, folder_id=None):
    fid = folder_id if folder_id is not None else _folder(db)
    return db.add_photo(
        folder_id=fid, filename=name, extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp=None, width=1, height=1,
    )


def _det(db, photo_id, detector_model="test-det", conf=0.9,
         category="animal", runtime_fingerprint="legacy"):
    cur = db.conn.execute(
        """INSERT INTO detections
             (photo_id, detector_model, runtime_fingerprint,
              box_x, box_y, box_w, box_h, detector_confidence, category)
           VALUES (?, ?, ?, 0.0, 0.0, 1.0, 1.0, ?, ?)""",
        (photo_id, detector_model, runtime_fingerprint, conf, category),
    )
    db.conn.commit()
    return cur.lastrowid


def _pred(db, detection_id, fingerprint, species="Robin", model=MODEL,
          confidence=0.9):
    db.add_prediction(
        detection_id, species=species, confidence=confidence, model=model,
        labels_fingerprint=fingerprint,
    )
    return db.conn.execute(
        """SELECT id FROM predictions
            WHERE detection_id = ? AND classifier_model = ?
              AND labels_fingerprint = ? AND species = ?""",
        (detection_id, model, fingerprint, species),
    ).fetchone()["id"]


def _usable(db, detection_id, fingerprint="fp-a", model=MODEL, **run_kwargs):
    db.record_classifier_run(
        detection_id, model, fingerprint, prediction_count=1, **run_kwargs,
    )
    return _pred(db, detection_id, fingerprint, model=model)


def _review(db, prediction_id, status, individual=None):
    db.set_review_status(prediction_id, db._ws_id(), status,
                         individual=individual)


def _set_floor(db, value):
    db.update_workspace(
        db._ws_id(), config_overrides={"detector_confidence": value},
    )


# -- detector runs ------------------------------------------------------------


def test_record_detector_run_inserts_upserts_and_commits(db):
    pid = _photo(db, "a.jpg")
    db.record_detector_run(pid, "megadetector-v6", 2)

    reader = _reader(db)
    row = reader.execute(
        "SELECT * FROM detector_runs WHERE photo_id = ?", (pid,),
    ).fetchone()
    assert row["detector_model"] == "megadetector-v6"
    assert row["runtime_fingerprint"] == "legacy"
    assert row["input_fingerprint"] is None
    assert row["box_count"] == 2
    assert row["run_at"] is not None

    db.record_detector_run(
        pid, "megadetector-v6", 0,
        runtime_fingerprint="rt-2", input_fingerprint="in-2",
    )
    rows = reader.execute(
        "SELECT * FROM detector_runs WHERE photo_id = ?", (pid,),
    ).fetchall()
    assert len(rows) == 1
    assert (rows[0]["runtime_fingerprint"], rows[0]["input_fingerprint"],
            rows[0]["box_count"]) == ("rt-2", "in-2", 0)
    assert not db.conn.in_transaction
    reader.close()


def test_get_global_detection_stats_counts_distinct_photos_and_models(db):
    assert db.get_global_detection_stats() == {
        "photo_count": 0, "model_count": 0,
    }
    fid = _folder(db)
    p1 = _photo(db, "a.jpg", fid)
    p2 = _photo(db, "b.jpg", fid)
    db.record_detector_run(p1, "megadetector-v6", 1)
    db.record_detector_run(p1, "megadetector-v5", 0)
    db.record_detector_run(p2, "megadetector-v6", 3)
    assert db.get_global_detection_stats() == {
        "photo_count": 2, "model_count": 2,
    }


def test_detector_run_is_pinned_only_by_real_manual_reviews(db):
    pid = _photo(db, "a.jpg")
    assert db.detector_run_is_pinned(pid, "megadetector-v6") is False

    det = _det(db, pid, "megadetector-v6")
    pred = _pred(db, det, "fp-a")
    assert db.detector_run_is_pinned(pid, "megadetector-v6") is False

    _review(db, pred, "pending")
    assert db.detector_run_is_pinned(pid, "megadetector-v6") is False

    _review(db, pred, "accepted", individual=AUTO_MATCH_REVIEW_MARKER)
    assert db.detector_run_is_pinned(pid, "megadetector-v6") is False

    db.conn.execute(
        "UPDATE prediction_review SET individual = NULL WHERE prediction_id = ?",
        (pred,),
    )
    db.conn.commit()
    assert db.detector_run_is_pinned(pid, "megadetector-v6") is True
    assert db.detector_run_is_pinned(pid, "megadetector-v5") is False

    _review(db, pred, "rejected", individual="Robin")
    assert db.detector_run_is_pinned(pid, "megadetector-v6") is True


def test_get_detector_run_photo_ids_excludes_torn_runs(db):
    fid = _folder(db)
    empty = _photo(db, "empty.jpg", fid)
    torn = _photo(db, "torn.jpg", fid)
    ok = _photo(db, "ok.jpg", fid)
    other = _photo(db, "other.jpg", fid)
    db.record_detector_run(empty, "megadetector-v6", 0)
    db.record_detector_run(torn, "megadetector-v6", 2)
    db.record_detector_run(ok, "megadetector-v6", 1)
    _det(db, ok, "megadetector-v6")
    db.record_detector_run(other, "megadetector-v5", 0)
    # A detection under a different model does not make the torn row whole.
    _det(db, torn, "megadetector-v5")

    result = db.get_detector_run_photo_ids("megadetector-v6")
    assert isinstance(result, set)
    assert result == {empty, ok}
    assert db.get_detector_run_photo_ids("missing-model") == set()


def test_get_detector_run_photo_ids_runtime_filter(db):
    fid = _folder(db)
    match = _photo(db, "match.jpg", fid)
    legacy = _photo(db, "legacy.jpg", fid)
    stale = _photo(db, "stale.jpg", fid)
    reviewed = _photo(db, "reviewed.jpg", fid)
    auto = _photo(db, "auto.jpg", fid)
    pending = _photo(db, "pending.jpg", fid)

    db.record_detector_run(match, "megadetector-v6", 0,
                           runtime_fingerprint="rt-new")
    db.record_detector_run(legacy, "megadetector-v6", 0)
    db.record_detector_run(stale, "megadetector-v6", 0,
                           runtime_fingerprint="rt-old")
    for pid, status, individual in (
        (reviewed, "accepted", None),
        (auto, "accepted", AUTO_MATCH_REVIEW_MARKER),
        (pending, "pending", None),
    ):
        db.record_detector_run(pid, "megadetector-v6", 1,
                               runtime_fingerprint="rt-old")
        det = _det(db, pid, "megadetector-v6")
        _review(db, _pred(db, det, "fp-a"), status, individual=individual)

    assert db.get_detector_run_photo_ids("megadetector-v6") == {
        match, legacy, stale, reviewed, auto, pending,
    }
    assert db.get_detector_run_photo_ids(
        "megadetector-v6", runtime_fingerprint="rt-new",
    ) == {match, legacy, reviewed}


# -- classifier runs ----------------------------------------------------------


def test_record_classifier_run_inserts_upserts_and_commits_with_retry(
    db, monkeypatch,
):
    commits = []
    real = db_module.commit_with_retry

    def recording(conn, *args, **kwargs):
        commits.append(conn)
        return real(conn, *args, **kwargs)

    det = _det(db, _photo(db, "a.jpg"))
    monkeypatch.setattr(db_module, "commit_with_retry", recording)
    db.record_classifier_run(det, MODEL, "fp-a", 3)
    assert commits == [db.conn]

    reader = _reader(db)
    row = reader.execute(
        "SELECT * FROM classifier_runs WHERE detection_id = ?", (det,),
    ).fetchone()
    assert dict(row) | {"run_at": None} == {
        "detection_id": det, "classifier_model": MODEL,
        "labels_fingerprint": "fp-a", "labels_fingerprint_full": None,
        "runtime_fingerprint": "legacy", "input_recipe": None,
        "input_fingerprint": None, "run_at": None, "prediction_count": 3,
    }

    db.record_classifier_run(
        det, MODEL, "fp-a", 5, labels_fingerprint_full="full-a",
        runtime_fingerprint="rt", input_fingerprint="in",
        input_recipe="raw",
    )
    rows = reader.execute(
        "SELECT * FROM classifier_runs WHERE detection_id = ?", (det,),
    ).fetchall()
    assert len(rows) == 1
    assert (rows[0]["labels_fingerprint_full"], rows[0]["runtime_fingerprint"],
            rows[0]["input_fingerprint"], rows[0]["prediction_count"],
            rows[0]["input_recipe"]) == ("full-a", "rt", "in", 5, "raw")
    db.record_classifier_run(det, MODEL, "fp-b", 1)
    assert reader.execute(
        "SELECT COUNT(*) FROM classifier_runs WHERE detection_id = ?", (det,),
    ).fetchone()[0] == 2
    reader.close()


def test_record_classifier_match_score_upserts_and_commits_with_retry(
    db, monkeypatch,
):
    commits = []
    real = db_module.commit_with_retry

    def recording(conn, *args, **kwargs):
        commits.append(conn)
        return real(conn, *args, **kwargs)

    det = _det(db, _photo(db, "a.jpg"))
    monkeypatch.setattr(db_module, "commit_with_retry", recording)
    db.record_classifier_match_score(det, MODEL, "fp-a", 0.4)
    assert commits == [db.conn]

    reader = _reader(db)
    row = reader.execute(
        "SELECT * FROM classifier_match_scores WHERE detection_id = ?", (det,),
    ).fetchone()
    assert row["max_match_score"] == 0.4
    assert (row["match_margin"], row["top_species"], row["label_count"],
            row["score_kind"]) == (None, None, None, None)

    db.record_classifier_match_score(
        det, MODEL, "fp-a", 0.7, match_margin=0.1, top_species="Robin",
        label_count=12, score_kind="cosine",
    )
    rows = reader.execute(
        "SELECT * FROM classifier_match_scores WHERE detection_id = ?", (det,),
    ).fetchall()
    assert len(rows) == 1
    assert (rows[0]["max_match_score"], rows[0]["match_margin"],
            rows[0]["top_species"], rows[0]["label_count"],
            rows[0]["score_kind"]) == (0.7, 0.1, "Robin", 12, "cosine")
    reader.close()


def test_has_classifier_match_score_matches_the_exact_key(db):
    det = _det(db, _photo(db, "a.jpg"))
    assert db.has_classifier_match_score(det, MODEL, "fp-a") is False
    db.record_classifier_match_score(det, MODEL, "fp-a", 0.2)
    assert db.has_classifier_match_score(det, MODEL, "fp-a") is True
    assert db.has_classifier_match_score(det, MODEL, "fp-b") is False
    assert db.has_classifier_match_score(det, "other", "fp-a") is False
    assert db.has_classifier_match_score(det + 1, MODEL, "fp-a") is False


def test_get_unscored_current_prediction_runs(db):
    fid = _folder(db)
    pid = _photo(db, "a.jpg", fid)
    other = _photo(db, "b.jpg", fid)
    assert db.get_unscored_current_prediction_runs(pid) == []

    det = _det(db, pid, "megadetector-v6")
    _pred(db, det, "fp-old")
    _pred(db, det, "fp-new", species="Wren")
    # A score recorded only for the superseded list does not cover the
    # current one.
    db.record_classifier_match_score(det, MODEL, "fp-old", 0.8)
    other_det = _det(db, other)
    _pred(db, other_det, "fp-new")

    rows = db.get_unscored_current_prediction_runs(pid)
    assert rows == [{
        "detection_id": det, "classifier_model": MODEL,
        "detector_model": "megadetector-v6", "labels_fingerprint": "fp-new",
    }]
    assert isinstance(rows[0], dict)

    db.record_classifier_match_score(det, MODEL, "fp-new", 0.3)
    assert db.get_unscored_current_prediction_runs(pid) == []


def test_get_match_scores_for_photo_orders_and_stamps_is_current(db):
    fid = _folder(db)
    pid = _photo(db, "a.jpg", fid)
    assert db.get_match_scores_for_photo(pid) == []

    det = _det(db, pid, "megadetector-v6", conf=0.05)
    _pred(db, det, "fp-old")
    _pred(db, det, "fp-new", species="Wren")
    db.record_classifier_match_score(det, MODEL, "fp-old", 0.9)
    db.record_classifier_match_score(det, MODEL, "fp-new", 0.3)

    # No predictions at all: the latest match-score row is current.
    bare = _det(db, pid, "megadetector-v6", conf=0.5)
    db.record_classifier_match_score(bare, MODEL, "fp-x", 0.5)
    db.record_classifier_match_score(bare, MODEL, "fp-y", 0.1)
    db.conn.execute(
        """UPDATE classifier_match_scores SET run_at = ?
            WHERE detection_id = ? AND labels_fingerprint = ?""",
        ("2020-01-01 00:00:00", bare, "fp-x"),
    )
    db.conn.commit()
    other_det = _det(db, _photo(db, "b.jpg", fid))
    db.record_classifier_match_score(other_det, MODEL, "fp-a", 1.0)

    rows = db.get_match_scores_for_photo(pid)
    assert [(r["detection_id"], r["labels_fingerprint"], r["is_current"])
            for r in rows] == [
        (det, "fp-old", 0),
        (bare, "fp-x", 0),
        (det, "fp-new", 1),
        (bare, "fp-y", 1),
    ]
    assert rows[0]["detector_confidence"] == 0.05
    assert rows[0]["detector_model"] == "megadetector-v6"
    assert rows[0]["classifier_model"] == MODEL
    assert isinstance(rows[0], dict)


def test_get_match_scores_is_current_breaks_run_at_ties_by_rowid(db):
    det = _det(db, _photo(db, "a.jpg"))
    db.record_classifier_match_score(det, MODEL, "fp-x", 0.5)
    db.record_classifier_match_score(det, MODEL, "fp-y", 0.4)
    db.conn.execute(
        "UPDATE classifier_match_scores SET run_at = '2020-01-01 00:00:00'",
    )
    db.conn.commit()
    rows = db.get_match_scores_for_photo(
        db.conn.execute(
            "SELECT photo_id FROM detections WHERE id = ?", (det,),
        ).fetchone()[0],
    )
    assert {r["labels_fingerprint"]: r["is_current"] for r in rows} == {
        "fp-x": 0, "fp-y": 1,
    }


def test_get_classifier_run_keys_filters_recipe_incomplete_and_runtime(db):
    det = _det(db, _photo(db, "a.jpg"))
    assert db.get_classifier_run_keys(det) == set()

    db.record_classifier_run(det, MODEL, "fp-current", 1,
                             runtime_fingerprint="rt-new")
    db.record_classifier_run(det, MODEL, "fp-legacy", 1)
    db.record_classifier_run(det, MODEL, "fp-stale", 1,
                             runtime_fingerprint="rt-old")
    db.record_classifier_run(det, MODEL, "fp-reviewed", 1,
                             runtime_fingerprint="rt-old")
    db.record_classifier_run(det, MODEL, "fp-auto", 1,
                             runtime_fingerprint="rt-old")
    db.record_classifier_run(det, MODEL, "fp-raw", 1,
                             runtime_fingerprint="rt-new", input_recipe="raw")
    db.record_classifier_run(det, MODEL, "fp-incomplete", 1,
                             runtime_fingerprint="incomplete")
    _review(db, _pred(db, det, "fp-reviewed"), "rejected")
    _review(db, _pred(db, det, "fp-auto"), "accepted",
            individual=AUTO_MATCH_REVIEW_MARKER)

    assert db.get_classifier_run_keys(det) == {
        (MODEL, "fp-current"), (MODEL, "fp-legacy"), (MODEL, "fp-stale"),
        (MODEL, "fp-reviewed"), (MODEL, "fp-auto"),
    }
    assert db.get_classifier_run_keys(det, runtime_fingerprint="rt-new") == {
        (MODEL, "fp-current"), (MODEL, "fp-legacy"), (MODEL, "fp-reviewed"),
    }


def test_get_classifier_run_key_gate_partitions_rows(db):
    det = _det(db, _photo(db, "a.jpg"))
    assert db.get_classifier_run_key_gate(det, None) == (set(), set())
    assert db.get_classifier_run_key_gate(det, "rt-new") == (set(), set())

    db.record_classifier_run(det, MODEL, "fp-current", 1,
                             runtime_fingerprint="rt-new")
    db.record_classifier_run(det, MODEL, "fp-legacy", 1)
    db.record_classifier_run(det, MODEL, "fp-stale", 1,
                             runtime_fingerprint="rt-old")
    db.record_classifier_run(det, MODEL, "fp-reviewed", 1,
                             runtime_fingerprint="rt-old")
    db.record_classifier_run(det, MODEL, "fp-auto", 1,
                             runtime_fingerprint="rt-old")
    db.record_classifier_run(det, MODEL, "fp-raw", 1,
                             runtime_fingerprint="rt-new", input_recipe="raw")
    db.record_classifier_run(det, MODEL, "fp-incomplete", 1,
                             runtime_fingerprint="incomplete")
    db.record_classifier_run(det, MODEL, "fp-raw-reviewed", 1,
                             input_recipe="raw")
    _review(db, _pred(db, det, "fp-reviewed"), "accepted", individual="Robin")
    _review(db, _pred(db, det, "fp-auto"), "accepted",
            individual=AUTO_MATCH_REVIEW_MARKER)
    _review(db, _pred(db, det, "fp-raw-reviewed"), "accepted")

    accepted, rejected = db.get_classifier_run_key_gate(det, "rt-new")
    assert accepted == {
        (MODEL, "fp-current"), (MODEL, "fp-legacy"), (MODEL, "fp-reviewed"),
    }
    assert rejected == {
        (MODEL, "fp-stale"), (MODEL, "fp-auto"), (MODEL, "fp-raw"),
        (MODEL, "fp-incomplete"), (MODEL, "fp-raw-reviewed"),
    }
    # Mirrors the runtime-filtered key set.
    assert accepted == db.get_classifier_run_keys(
        det, runtime_fingerprint="rt-new",
    )
    assert db.get_classifier_run_key_gate(det, None) == (set(), set())


# -- classify preflight: cache hits -------------------------------------------


def test_cache_hits_and_unclassifiable_return_early_without_config(
    db, monkeypatch,
):
    def boom(self, *_args, **_kwargs):
        raise AssertionError("config must not be read for an empty id list")

    monkeypatch.setattr(Database, "get_effective_config", boom)
    for empty in ([], (), set(), None):
        assert db.get_classifier_run_cache_hits(empty, MODEL, "fp-a") == set()
        assert db.count_classifier_runs(empty, MODEL, "fp-a") == 0
        assert db.get_unclassifiable_photos(empty) == set()


def test_cache_hits_read_the_workspace_detector_floor(db):
    fid = _folder(db)
    pid = _photo(db, "a.jpg", fid)
    cached = _det(db, pid, conf=0.9)
    _usable(db, cached)
    _det(db, pid, conf=0.3)  # uncached, qualifies only under the default floor

    assert db.get_classifier_run_cache_hits([pid], MODEL, "fp-a") == set()
    _set_floor(db, 0.5)
    assert db.get_classifier_run_cache_hits([pid], MODEL, "fp-a") == {pid}
    assert db.count_classifier_runs([pid], MODEL, "fp-a") == 1


def test_cache_hits_ignore_negative_confidence_predictions_and_other_keys(db):
    fid = _folder(db)
    neg = _photo(db, "neg.jpg", fid)
    other_model = _photo(db, "model.jpg", fid)
    other_fp = _photo(db, "fp.jpg", fid)

    d = _det(db, neg)
    db.record_classifier_run(d, MODEL, "fp-a", 1)
    _pred(db, d, "fp-a", confidence=-1)
    _usable(db, _det(db, other_model), model="other")
    _usable(db, _det(db, other_fp), fingerprint="fp-b")

    assert db.get_classifier_run_cache_hits(
        [neg, other_model, other_fp], MODEL, "fp-a",
    ) == set()


def test_cache_hits_chunk_large_id_lists(db):
    fid = _folder(db)
    normal = _photo(db, "normal.jpg", fid)
    weak = _photo(db, "weak.jpg", fid)
    anchor_photo = _photo(db, "anchor.jpg", fid)
    _usable(db, _det(db, normal))
    _usable(db, _det(db, weak, "megadetector-v6", conf=0.1))
    _usable(db, _det(db, anchor_photo, "full-image", conf=0))

    filler = list(range(10_000, 10_000 + 1_200))
    ids = filler + [normal, weak, anchor_photo]
    hits = db.get_classifier_run_cache_hits(
        ids, MODEL, "fp-a",
        contextual_weak_photo_ids={weak}, weak_confidence=0.05,
    )
    assert hits == {normal, weak, anchor_photo}


def test_cache_hits_fresh_candidates_skip_ineligible_detections(db):
    fid = _folder(db)
    pid = _photo(db, "a.jpg", fid)
    cached = _det(db, pid, "megadetector-v6", conf=0.9)
    _usable(db, cached)
    uncached_full = _det(db, pid, "full-image", conf=0)
    uncached_person = _det(db, pid, "megadetector-v6", conf=0.9,
                           category="person")
    uncached_bad = _det(db, pid, "megadetector-v6", conf=0.9)
    uncached_low = _det(db, pid, "megadetector-v6", conf=0.05)

    fresh = {pid: [
        {"id": cached, "detector_model": "megadetector-v6",
         "confidence": 0.9, "category": "animal"},
        {"id": uncached_full, "detector_model": "full-image",
         "confidence": 1.0},
        {"id": uncached_person, "detector_model": "megadetector-v6",
         "confidence": 0.9, "category": "person"},
        {"id": uncached_bad, "detector_model": "megadetector-v6",
         "confidence": "not-a-number"},
        {"id": uncached_bad, "detector_model": "megadetector-v6",
         "confidence": [0.9]},
        {"id": uncached_low, "detector_model": "megadetector-v6",
         "detector_confidence": 0.05},
        {"detector_model": "megadetector-v6", "confidence": 0.9},  # no id
    ]}
    assert db.get_classifier_run_cache_hits(
        [pid], MODEL, "fp-a",
        fresh_detections_by_photo=fresh, fresh_processed_photo_ids={pid},
    ) == {pid}
    # Without the fresh map the DB fallback sees the uncached rows.
    assert db.get_classifier_run_cache_hits([pid], MODEL, "fp-a") == set()


def test_cache_hits_fresh_contextual_weak_uses_top_megadetector_box(db):
    fid = _folder(db)
    top_cached = _photo(db, "top.jpg", fid)
    low_cached = _photo(db, "low.jpg", fid)

    def _weak_pair(pid, cache_top):
        top = _det(db, pid, "megadetector-v6", conf=0.15)
        low = _det(db, pid, "megadetector-v6", conf=0.1)
        foreign = _det(db, pid, "megadetector-v5", conf=0.9)
        _usable(db, top if cache_top else low)
        _usable(db, foreign)
        return [
            {"id": low, "detector_model": "megadetector-v6",
             "confidence": 0.1},
            {"id": foreign, "detector_model": "megadetector-v5",
             "confidence": 0.9},
            {"id": top, "detector_model": "megadetector-v6",
             "confidence": 0.15},
        ]

    fresh = {
        top_cached: _weak_pair(top_cached, True),
        low_cached: _weak_pair(low_cached, False),
    }
    hits = db.get_classifier_run_cache_hits(
        [top_cached, low_cached], MODEL, "fp-a",
        contextual_weak_photo_ids={top_cached, low_cached},
        weak_confidence=0.05,
        fresh_detections_by_photo=fresh,
        fresh_processed_photo_ids={top_cached, low_cached},
    )
    assert hits == {top_cached}


def test_cache_hits_fresh_weak_photo_without_candidates_uses_db(db):
    fid = _folder(db)
    pid = _photo(db, "a.jpg", fid)
    _usable(db, _det(db, pid, "megadetector-v6", conf=0.1))
    assert db.get_classifier_run_cache_hits(
        [pid], MODEL, "fp-a",
        contextual_weak_photo_ids=[pid], weak_confidence=0.05,
        fresh_detections_by_photo={pid: []},
        fresh_processed_photo_ids=[pid],
    ) == {pid}


def test_cache_hits_fresh_full_image_anchor_unless_confident_non_animal(db):
    fid = _folder(db)
    noise = _photo(db, "noise.jpg", fid)
    person = _photo(db, "person.jpg", fid)
    no_anchor = _photo(db, "no-anchor.jpg", fid)
    uncached_anchor = _photo(db, "uncached.jpg", fid)
    for pid in (noise, person):
        _usable(db, _det(db, pid, "full-image", conf=0))
    _det(db, uncached_anchor, "full-image", conf=0)

    fresh = {
        noise: [
            {"id": 1, "detector_model": "full-image", "confidence": 1.0,
             "category": "person"},
            {"detector_model": "megadetector-v6", "confidence": "bad",
             "category": "person"},
            {"detector_model": "megadetector-v6", "confidence": 0.1,
             "category": "vehicle"},
        ],
        person: [
            {"detector_model": "megadetector-v6",
             "detector_confidence": 0.9, "category": "person"},
        ],
        no_anchor: [],
        uncached_anchor: [],
    }
    ids = [noise, person, no_anchor, uncached_anchor]
    assert db.get_classifier_run_cache_hits(
        ids, MODEL, "fp-a",
        fresh_detections_by_photo=fresh, fresh_processed_photo_ids=set(ids),
    ) == {noise}


def test_cache_hits_fresh_map_is_ignored_without_processed_ids(db):
    fid = _folder(db)
    pid = _photo(db, "a.jpg", fid)
    stale = _det(db, pid, "megadetector-v5", conf=0.9)
    fresh_id = _det(db, pid, "megadetector-v6", conf=0.9)
    _usable(db, fresh_id)
    fresh = {pid: [{"id": fresh_id, "detector_model": "megadetector-v6",
                    "confidence": 0.9}]}
    assert stale
    assert db.get_classifier_run_cache_hits(
        [pid], MODEL, "fp-a", fresh_detections_by_photo=fresh,
    ) == set()
    assert db.get_classifier_run_cache_hits(
        [pid], MODEL, "fp-a", fresh_processed_photo_ids={pid},
    ) == set()


def test_cache_hits_db_full_image_anchor_requires_consistent_detector_run(db):
    fid = _folder(db)
    consistent = _photo(db, "consistent.jpg", fid)
    torn = _photo(db, "torn.jpg", fid)
    empty_run = _photo(db, "empty.jpg", fid)
    blocked = _photo(db, "blocked.jpg", fid)
    for pid in (consistent, torn, empty_run, blocked):
        _usable(db, _det(db, pid, "full-image", conf=0))
    # consistent: a detector run with a surviving (below-floor) box.
    db.record_detector_run(consistent, "megadetector-v6", 1)
    _det(db, consistent, "megadetector-v6", conf=0.05)
    # torn: box_count > 0 but no megadetector-v6 rows remain.
    db.record_detector_run(torn, "megadetector-v6", 2)
    db.record_detector_run(empty_run, "megadetector-v6", 0)
    # blocked: a confident non-animal box blocks the fallback.
    _det(db, blocked, "megadetector-v6", conf=0.9, category="vehicle")

    assert db.get_classifier_run_cache_hits(
        [consistent, torn, empty_run, blocked], MODEL, "fp-a",
    ) == {consistent, empty_run}


def test_cache_hits_runtime_map_applies_to_every_branch(db):
    fid = _folder(db)
    normal = _photo(db, "normal.jpg", fid)
    weak = _photo(db, "weak.jpg", fid)
    anchor = _photo(db, "anchor.jpg", fid)
    fresh_photo = _photo(db, "fresh.jpg", fid)
    raw = _photo(db, "raw.jpg", fid)
    incomplete = _photo(db, "incomplete.jpg", fid)

    _usable(db, _det(db, normal, runtime_fingerprint="rt-det"),
            runtime_fingerprint="cls-old")
    _usable(db, _det(db, weak, "megadetector-v6", conf=0.1,
                     runtime_fingerprint="rt-det"),
            runtime_fingerprint="cls-old")
    _usable(db, _det(db, anchor, "full-image", conf=0,
                     runtime_fingerprint="rt-det"),
            runtime_fingerprint="cls-old")
    fresh_det = _det(db, fresh_photo, "megadetector-v6",
                     runtime_fingerprint="rt-det")
    _usable(db, fresh_det, runtime_fingerprint="cls-old")
    _usable(db, _det(db, raw), input_recipe="raw")
    _usable(db, _det(db, incomplete), runtime_fingerprint="incomplete")

    kwargs = dict(
        contextual_weak_photo_ids={weak}, weak_confidence=0.05,
        fresh_detections_by_photo={fresh_photo: [
            {"id": fresh_det, "detector_model": "megadetector-v6",
             "confidence": 0.9},
        ]},
        fresh_processed_photo_ids={fresh_photo},
    )
    ids = [normal, weak, anchor, fresh_photo, raw, incomplete]
    assert db.get_classifier_run_cache_hits(
        ids, MODEL, "fp-a", **kwargs,
    ) == {normal, weak, anchor, fresh_photo}
    assert db.get_classifier_run_cache_hits(
        ids, MODEL, "fp-a",
        expected_classifier_runtime_by_detector_runtime={},
        **kwargs,
    ) == {normal, weak, anchor, fresh_photo}
    assert db.get_classifier_run_cache_hits(
        ids, MODEL, "fp-a",
        expected_classifier_runtime_by_detector_runtime={"rt-det": "cls-new"},
        **kwargs,
    ) == set()
    assert db.get_classifier_run_cache_hits(
        ids, MODEL, "fp-a",
        expected_classifier_runtime_by_detector_runtime={
            "rt-det": "cls-old", "other-rt": None,
        },
        **kwargs,
    ) == {normal, weak, anchor, fresh_photo}
    assert db.count_classifier_runs(
        ids, MODEL, "fp-a",
        expected_classifier_runtime_by_detector_runtime={"rt-det": None},
        **kwargs,
    ) == 4


# -- classify preflight: unclassifiable ---------------------------------------


def test_unclassifiable_db_path(db):
    fid = _folder(db)
    person_only = _photo(db, "person.jpg", fid)
    with_animal = _photo(db, "animal.jpg", fid)
    low_person = _photo(db, "low.jpg", fid)
    full_image_only = _photo(db, "full.jpg", fid)
    weak_foreign = _photo(db, "weak-foreign.jpg", fid)
    weak_mdv6 = _photo(db, "weak-mdv6.jpg", fid)

    _det(db, person_only, conf=0.9, category="person")
    _det(db, with_animal, conf=0.9, category="vehicle")
    _det(db, with_animal, conf=0.9)
    _det(db, low_person, conf=0.1, category="person")
    _det(db, full_image_only, "full-image", conf=1.0, category="person")
    for pid in (weak_foreign, weak_mdv6):
        _det(db, pid, conf=0.9, category="person")
    _det(db, weak_foreign, "megadetector-v5", conf=0.9)
    _det(db, weak_mdv6, "megadetector-v6", conf=0.1)

    filler = list(range(10_000, 10_000 + 1_100))
    result = db.get_unclassifiable_photos(
        filler + [person_only, with_animal, low_person, full_image_only,
                  weak_foreign, weak_mdv6],
        contextual_weak_photo_ids={weak_foreign, weak_mdv6},
        weak_confidence=0.05,
    )
    assert result == {person_only, weak_foreign}

    _set_floor(db, 0.95)
    assert db.get_unclassifiable_photos([person_only]) == set()


def test_unclassifiable_fresh_path_without_processed_ids(db):
    fid = _folder(db)
    person = _photo(db, "person.jpg", fid)
    animal = _photo(db, "animal.jpg", fid)
    missing = _photo(db, "missing.jpg", fid)
    # DB rows are ignored on this path.
    _det(db, missing, conf=0.9, category="person")

    fresh = {
        person: [
            {"category": "animal", "confidence": "bad"},
            {"category": "animal", "confidence": 0.05},
            {"detector_model": "full-image", "category": "person",
             "confidence": 1.0},
            {"category": "person", "confidence": None},
            {"category": "person", "confidence": [1]},
            {"category": "person", "detector_confidence": 0.9},
        ],
        animal: [
            {"confidence": 0.9},
            {"category": "person", "confidence": 0.9},
        ],
    }
    assert db.get_unclassifiable_photos(
        [person, animal, missing], fresh_detections_by_photo=fresh,
    ) == {person}


def test_unclassifiable_fresh_path_with_processed_ids(db):
    fid = _folder(db)
    person = _photo(db, "person.jpg", fid)
    no_person = _photo(db, "no-person.jpg", fid)
    unprocessed = _photo(db, "unprocessed.jpg", fid)
    weak_fresh = _photo(db, "weak-fresh.jpg", fid)
    weak_db = _photo(db, "weak-db.jpg", fid)

    _det(db, unprocessed, conf=0.9, category="person")
    _det(db, weak_db, conf=0.9, category="person")
    _det(db, weak_db, "megadetector-v5", conf=0.9)

    fresh = {
        person: [{"category": "person", "confidence": 0.9}],
        no_person: [{"detector_model": "full-image", "category": "person",
                     "confidence": 1.0}],
        weak_fresh: [
            {"detector_model": "megadetector-v6", "confidence": 0.1},
            {"category": "person", "confidence": 0.9},
        ],
        weak_db: [
            {"detector_model": "megadetector-v5", "confidence": 0.9},
            {"detector_model": "megadetector-v6", "confidence": "bad"},
            {"category": "person", "confidence": 0.9},
        ],
    }
    assert db.get_unclassifiable_photos(
        [person, no_person, unprocessed, weak_fresh, weak_db],
        contextual_weak_photo_ids=[weak_fresh, weak_db],
        weak_confidence=0.05,
        fresh_detections_by_photo=fresh,
        fresh_processed_photo_ids=[person, no_person, weak_fresh, weak_db],
    ) == {person, unprocessed, weak_db}


# -- labels fingerprints ------------------------------------------------------


def test_labels_fingerprint_upsert_round_trip_and_commit(db):
    assert db.get_labels_fingerprints() == []
    db.upsert_labels_fingerprint("fp-a", "Birds", ["/a.txt"], 10,
                                 full_fingerprint="full-a")
    reader = _reader(db)
    row = reader.execute(
        "SELECT * FROM labels_fingerprints WHERE fingerprint = 'fp-a'",
    ).fetchone()
    assert row["sources_json"] == json.dumps(["/a.txt"])
    assert not db.conn.in_transaction

    # A later write without the full digest keeps the stored one.
    db.upsert_labels_fingerprint("fp-a", "Birds 2", ["/a.txt", "/b.txt"], 11)
    db.upsert_labels_fingerprint("fp-b", None, None, None)
    rows = sorted(db.get_labels_fingerprints(), key=lambda r: r["fingerprint"])
    assert rows == [
        {"fingerprint": "fp-a", "full_fingerprint": "full-a",
         "display_name": "Birds 2", "sources": ["/a.txt", "/b.txt"],
         "label_count": 11},
        {"fingerprint": "fp-b", "full_fingerprint": None,
         "display_name": None, "sources": [], "label_count": None},
    ]
    assert reader.execute(
        "SELECT sources_json FROM labels_fingerprints WHERE fingerprint = 'fp-b'",
    ).fetchone()[0] == "[]"
    db.upsert_labels_fingerprint("fp-a", "Birds 3", [], 1,
                                 full_fingerprint="full-a2")
    assert [r["full_fingerprint"] for r in db.get_labels_fingerprints()
            if r["fingerprint"] == "fp-a"] == ["full-a2"]
    reader.close()


@pytest.mark.parametrize("raw, expected", [
    (None, []),
    ("", []),
    ("{", []),
    ('["x"]', ["x"]),
    ('{"a": 1}', {"a": 1}),
])
def test_get_labels_fingerprints_tolerates_malformed_sources(db, raw, expected):
    db.conn.execute(
        "INSERT INTO labels_fingerprints (fingerprint, sources_json) VALUES (?, ?)",
        ("fp", raw),
    )
    db.conn.commit()
    assert db.get_labels_fingerprints()[0]["sources"] == expected


# -- structure ----------------------------------------------------------------

_DELEGATING_MODEL_RUN_METHODS = (
    "record_detector_run",
    "get_global_detection_stats",
    "detector_run_is_pinned",
    "get_detector_run_photo_ids",
    "record_classifier_run",
    "record_classifier_match_score",
    "has_classifier_match_score",
    "get_unscored_current_prediction_runs",
    "get_match_scores_for_photo",
    "get_classifier_run_keys",
    "get_classifier_run_key_gate",
    "get_classifier_run_cache_hits",
    "get_unclassifiable_photos",
    "get_labels_fingerprints",
    "upsert_labels_fingerprint",
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


def test_count_classifier_runs_composes_through_the_facade():
    attrs = _self_attrs(Database.count_classifier_runs)
    assert "conn" not in attrs
    assert "get_classifier_run_cache_hits" in attrs


@pytest.mark.parametrize("name", [
    "get_classifier_run_cache_hits", "get_unclassifiable_photos",
])
def test_preflight_reads_config_through_the_facade(name):
    """The workspace floor comes from ``Database.get_effective_config``."""
    assert "get_effective_config" in _self_attrs(getattr(Database, name))
