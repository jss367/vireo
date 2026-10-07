"""Behavior pins for the predictions domain of ``Database``.

The tests go through the public ``Database`` façade only, so they hold
whether the SQL lives in ``db.py`` or in ``repositories/predictions.py``.
They pin prediction storage (normalize-on-write, INSERT OR IGNORE re-use,
fingerprint / match-score backfill, refreshed outputs), the per-workspace
review state (absence == pending, preserved manual decisions, the auto-match
marker), the ``_commit=False`` nested-transaction seams of the review
mutators, commit visibility and ``commit_with_retry`` routing, chunking,
workspace scoping and lazy active-workspace resolution, and that
composition (``get_effective_config``, ``_build_query_from_rules``,
``accept_prediction``) still routes through the façade so monkeypatches take
effect.
"""

import ast
import contextlib
import inspect
import sqlite3
import textwrap

import pytest
from db import AUTO_MATCH_REVIEW_MARKER, Database


def _visible(db, sql, params=()):
    """Rows as a second connection sees them (i.e. committed)."""
    with contextlib.closing(sqlite3.connect(db._db_path)) as conn:
        return [tuple(r) for r in conn.execute(sql, params).fetchall()]


def _trace(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


def _review(db, pred_id, ws=None):
    ws = db._active_workspace_id if ws is None else ws
    row = db.conn.execute(
        "SELECT status, individual, group_id, vote_count, total_votes "
        "FROM prediction_review WHERE prediction_id = ? AND workspace_id = ?",
        (pred_id, ws),
    ).fetchone()
    return tuple(row) if row else None


def _det(db, pid, conf=0.9, model="md", x=0.1):
    return db.save_detections(
        pid,
        [{"box": {"x": x, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": conf,
          "category": "animal"}],
        model,
    )[0]


def _pred_id(db, det, species, model="m1", fp="fp1"):
    return db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND classifier_model = ? "
        "AND labels_fingerprint = ? AND species IS ?",
        (det, model, fp, species),
    ).fetchone()[0]


@pytest.fixture
def cat(db, tmp_path, monkeypatch):
    import config as cfg

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    ws = db._active_workspace_id
    fid = db.add_folder("/pred", name="pred")
    other_folder = db.add_folder("/elsewhere", name="elsewhere", link_to_workspace=False)
    photos = [db.add_photo(fid, f"p{i}.jpg", ".jpg", 10 + i, float(i)) for i in range(4)]
    outside = db.add_photo(other_folder, "out.jpg", ".jpg", 99, 9.0)
    return {"ws": ws, "fid": fid, "p": photos, "outside": outside}


# -- add_prediction ------------------------------------------------------------------------


def test_add_prediction_requires_detection(db, cat):
    with pytest.raises(ValueError, match="non-null detection_id"):
        db.add_prediction(None, "Robin", 0.5, "m1")


def test_add_prediction_stores_normalized_output_and_commits(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(
        det, "Swinhoe’s White-eye", 0.7, "m1", category="match",
        taxonomy={"kingdom": "K", "phylum": "P", "class": "C", "order": "O",
                  "family": "F", "genus": "G", "scientific_name": "S", "taxon_id": 7},
        labels_fingerprint="fp1", labels_fingerprint_full="full", match_score=3.5,
    )
    assert not db.conn.in_transaction
    assert _visible(
        db,
        "SELECT species, confidence, category, taxonomy_kingdom, taxonomy_genus, "
        "scientific_name, source_taxon_id, match_score, labels_fingerprint_full "
        "FROM predictions",
    ) == [("Swinhoe's White-eye", 0.7, "match", "K", "G", "S", 7, 3.5, "full")]
    # Pending default: no review row (absence == pending).
    assert db.conn.execute("SELECT COUNT(*) FROM prediction_review").fetchone()[0] == 0
    # A label that normalizes to empty keeps its raw spelling.
    db.add_prediction(det, "’", 0.1, "m1", labels_fingerprint="fp1")
    assert _pred_id(db, det, "’")


def test_add_prediction_pending_needs_no_workspace(db, cat):
    det = _det(db, cat["p"][0])
    db.set_active_workspace(None)
    db.add_prediction(det, "Robin", 0.5, "m1")
    with pytest.raises(RuntimeError):
        db.add_prediction(det, "Robin", 0.5, "m1", status="accepted")


def test_add_prediction_reuse_backfills_without_overwriting(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="fp1")
    db.add_prediction(det, "Robin", 0.9, "m1", labels_fingerprint="fp1",
                      labels_fingerprint_full="full-1", match_score=1.0)
    db.add_prediction(det, "Robin", 0.9, "m1", labels_fingerprint="fp1",
                      labels_fingerprint_full="full-2", match_score=2.0)
    row = db.conn.execute(
        "SELECT confidence, labels_fingerprint_full, match_score FROM predictions"
    ).fetchone()
    assert tuple(row) == (0.5, "full-1", 1.0)
    db.add_prediction(det, "Robin", 0.9, "m1", labels_fingerprint="fp1",
                      match_score=4.0, from_fresh_inference=True)
    assert db.conn.execute("SELECT match_score FROM predictions").fetchone()[0] == 4.0
    assert db.conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 1


def test_add_prediction_refresh_output_keeps_id_and_clears_alternative(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="fp1",
                      status="alternative")
    pid = _pred_id(db, det, "Robin")
    db.add_prediction(det, "Robin", 0.8, "m1", category="change",
                      taxonomy={"genus": "Turdus"}, labels_fingerprint="fp1",
                      refresh_output=True, group_id="g", vote_count=2, total_votes=3,
                      individual='{"Robin": 2}')
    assert _pred_id(db, det, "Robin") == pid
    row = db.conn.execute(
        "SELECT confidence, category, taxonomy_genus FROM predictions WHERE id = ?",
        (pid,),
    ).fetchone()
    assert tuple(row) == (0.8, "change", "Turdus")
    assert _review(db, pid) == ("pending", '{"Robin": 2}', "g", 2, 3)
    # A refresh with no burst clears the old metadata (plain overwrite).
    db.add_prediction(det, "Robin", 0.8, "m1", labels_fingerprint="fp1",
                      refresh_output=True)
    assert _review(db, pid) == ("pending", None, None, None, None)


def test_add_prediction_review_state_coalesces(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1", group_id="g1", vote_count=3,
                      total_votes=4, individual='{"Robin": 3}')
    pid = _pred_id(db, det, "Robin", fp="legacy")
    assert _review(db, pid) == ("pending", '{"Robin": 3}', "g1", 3, 4)
    db.add_prediction(det, "Robin", 0.5, "m1", status="accepted")
    assert _review(db, pid) == ("accepted", '{"Robin": 3}', "g1", 3, 4)
    assert not db.conn.in_transaction


@pytest.mark.parametrize("status", ["accepted", "rejected"])
def test_add_prediction_preserves_manual_decisions(db, cat, status):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1", status=status)
    pid = _pred_id(db, det, "Robin", fp="legacy")
    db.add_prediction(det, "Robin", 0.5, "m1", status="pending", group_id="g",
                      preserve_manual_review=True)
    assert _review(db, pid)[:3] == (status, None, None)
    assert not db.conn.in_transaction
    # refresh_output keeps the decision but recomputes burst membership,
    # dropping the auto-match marker.
    db.add_prediction(det, "Robin", 0.5, "m1", refresh_output=True, group_id="g2",
                      vote_count=1, total_votes=1, individual=AUTO_MATCH_REVIEW_MARKER)
    assert _review(db, pid) == (status, None, "g2", 1, 1)


def test_add_prediction_overrides_auto_match_review(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1", status="accepted",
                      individual=AUTO_MATCH_REVIEW_MARKER)
    pid = _pred_id(db, det, "Robin", fp="legacy")
    db.add_prediction(det, "Robin", 0.5, "m1", status="pending", group_id="g",
                      preserve_manual_review=True)
    assert _review(db, pid)[:3] == ("pending", AUTO_MATCH_REVIEW_MARKER, "g")


def test_retain_prediction_candidates(db, cat):
    det = _det(db, cat["p"][0])
    for sp in ("Robin", "Swinhoe's White-eye", "Old"):
        db.add_prediction(det, sp, 0.5, "m1", labels_fingerprint="fp1")
    db.add_prediction(det, "Other fp", 0.5, "m1", labels_fingerprint="fp2")
    db.retain_prediction_candidates(det, "m1", "fp1",
                                    ["Robin", "Swinhoe’s White-eye"])
    assert not db.conn.in_transaction
    assert sorted(r[0] for r in _visible(db, "SELECT species FROM predictions")) == [
        "Other fp", "Robin", "Swinhoe's White-eye",
    ]


# -- reconcile / clear ---------------------------------------------------------------------


def test_reconcile_match_review_state(db, cat, monkeypatch):
    import db as dbmod

    commits = []
    real = dbmod.commit_with_retry
    monkeypatch.setattr(dbmod, "commit_with_retry",
                        lambda conn, *a, **k: commits.append(conn) or real(conn, *a, **k))
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1", category="match", status="accepted",
                      individual=AUTO_MATCH_REVIEW_MARKER, labels_fingerprint="fp1")
    pid = _pred_id(db, det, "Robin")
    commits.clear()
    db.reconcile_match_review_state(det, "m1", "fp1", "Missing", "new")
    assert commits == []
    db.reconcile_match_review_state(det, "m1", "fp1", "Robin", "match")
    assert _review(db, pid)[0] == "accepted"
    assert len(commits) == 1
    db.reconcile_match_review_state(det, "m1", "fp1", "Robin", "match", auto_accept=False)
    assert _review(db, pid) is None
    db.add_prediction(det, "Robin", 0.5, "m1", status="accepted",
                      individual=AUTO_MATCH_REVIEW_MARKER, labels_fingerprint="fp1")
    db.reconcile_match_review_state(det, "m1", "fp1", "Robin", "new")
    assert _review(db, pid) is None
    assert db.conn.execute("SELECT category FROM predictions WHERE id = ?",
                           (pid,)).fetchone()[0] == "new"
    assert not db.conn.in_transaction


def test_reconcile_keeps_manual_accept_and_normalizes_species(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Say's phoebe", 0.5, "m1", category="match",
                      status="accepted", labels_fingerprint="fp1")
    pid = _pred_id(db, det, "Say's phoebe")
    db.reconcile_match_review_state(det, "m1", "fp1", "Say’s phoebe", "change")
    assert _review(db, pid)[0] == "accepted"
    assert db.conn.execute("SELECT category FROM predictions").fetchone()[0] == "change"


def _seed_runs(db, det, model="m1", fp="fp1"):
    db.conn.execute(
        "INSERT INTO classifier_runs (detection_id, classifier_model, labels_fingerprint) "
        "VALUES (?, ?, ?)", (det, model, fp))
    db.conn.execute(
        "INSERT INTO classifier_match_scores (detection_id, classifier_model, "
        "labels_fingerprint) VALUES (?, ?, ?)", (det, model, fp))


def test_clear_predictions_filters_and_run_keys(db, cat):
    p0, p1 = cat["p"][:2]
    d0, d1 = _det(db, p0), _det(db, p1)
    dout = _det(db, cat["outside"])
    for det in (d0, d1, dout):
        db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="fp1")
        db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="fp2")
        db.add_prediction(det, "Robin", 0.5, "m2", labels_fingerprint="fp1")
        _seed_runs(db, det)
        _seed_runs(db, det, fp="fp2")
    db.conn.commit()
    db.clear_predictions(model="m1", labels_fingerprint="fp1", collection_photo_ids=[p0])
    assert not db.conn.in_transaction
    left = _visible(db, "SELECT detection_id, classifier_model, labels_fingerprint "
                        "FROM predictions ORDER BY 1, 2, 3")
    assert (d0, "m1", "fp1") not in left
    assert (d1, "m1", "fp1") in left and (dout, "m1", "fp1") in left
    runs = _visible(db, "SELECT detection_id, labels_fingerprint FROM classifier_runs")
    assert (d0, "fp1") not in runs and (d0, "fp2") in runs
    scores = _visible(db, "SELECT detection_id, labels_fingerprint FROM classifier_match_scores")
    assert (d0, "fp1") not in scores and (d0, "fp2") in scores
    # clear_run_keys=False keeps the runs but still drops the match scores.
    db.clear_predictions(model="m2", clear_run_keys=False)
    assert not db.conn.in_transaction
    assert (d1, "m2") not in _visible(db, "SELECT detection_id, classifier_model FROM predictions")
    assert (d1, "fp1") in _visible(db, "SELECT detection_id, labels_fingerprint FROM classifier_runs")
    # No filters: every workspace-visible row goes, the other folder's stay.
    db.clear_predictions()
    left = _visible(db, "SELECT DISTINCT detection_id FROM predictions")
    assert left == [(dout,)]
    assert _visible(db, "SELECT DISTINCT detection_id FROM classifier_runs") == [(dout,)]


def test_clear_predictions_chunks_photo_ids(db, cat):
    p0 = cat["p"][0]
    db.add_prediction(_det(db, p0), "Robin", 0.5, "m1")
    statements = _trace(db)
    db.clear_predictions(collection_photo_ids=list(range(10_000, 10_800)) + [p0])
    db.conn.set_trace_callback(None)
    distinct = list(dict.fromkeys(s.strip() for s in statements))
    assert len([s for s in distinct if s.startswith("DELETE FROM predictions")]) == 2
    assert len([s for s in distinct if s.startswith("DELETE FROM classifier_runs")]) == 2
    assert db.conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0


# -- reads ---------------------------------------------------------------------------------


def test_prediction_states(db, cat, monkeypatch):
    p0, p1, p2, p3 = cat["p"]
    assert db.get_prediction_states([]) == {}
    d0 = _det(db, p0, conf=0.9)
    _det(db, p1, conf=0.05)
    db.conn.execute(
        "INSERT INTO detector_runs (photo_id, detector_model) VALUES (?, 'md'), (?, 'md')",
        (p0, p1))
    db.conn.execute(
        "INSERT INTO classifier_runs (detection_id, classifier_model, labels_fingerprint) "
        "VALUES (?, 'm1', 'fp1')", (d0,))
    seen = []
    real = db.get_effective_config
    monkeypatch.setattr(db, "get_effective_config",
                        lambda c: seen.append(1) or dict(real(c), classifier_confidence=0.3))
    states = db.get_prediction_states([p0, p1, p2, p0])
    assert seen == [1]
    assert list(states) == [p0, p1, p2]
    assert states[p0] == {"detector_ran": True, "detection_count": 1,
                          "classifier_ran": True, "threshold": 0.3}
    assert states[p1] == {"detector_ran": True, "detection_count": 0,
                          "classifier_ran": False, "threshold": 0.3}
    assert states[p2]["detector_ran"] is False


def test_get_predictions_filters(db, cat, monkeypatch):
    p0, p1 = cat["p"][:2]
    d0, d1 = _det(db, p0), _det(db, p1)
    db.add_prediction(d0, "Robin", 0.5, "m1", labels_fingerprint="fp1")
    db.add_prediction(d0, "Sparrow", 0.9, "m2", labels_fingerprint="fp1",
                      status="accepted")
    db.add_prediction(d1, "Jay", 0.7, "m1", labels_fingerprint="fp1")
    db.add_prediction(_det(db, cat["outside"]), "Hidden", 0.99, "m1")
    rows = db.get_predictions()
    assert [r["species"] for r in rows] == ["Sparrow", "Jay", "Robin"]
    assert rows[0]["status"] == "accepted" and rows[0]["model"] == "m2"
    assert [r["species"] for r in db.get_predictions(model="m1")] == ["Jay", "Robin"]
    assert [r["species"] for r in db.get_predictions(status="pending")] == ["Jay", "Robin"]
    assert [r["species"] for r in db.get_predictions(photo_ids=[p1, p1])] == ["Jay"]


def test_get_predictions_latest_fingerprint_only(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Old", 0.9, "m1", labels_fingerprint="old")
    db.add_prediction(det, "New", 0.2, "m1", labels_fingerprint="new")
    db.conn.execute("UPDATE predictions SET created_at = '2030-01-01' WHERE species = 'New'")
    assert [r["species"] for r in db.get_predictions()] == ["New"]


def test_get_predictions_chunks_and_resorts(db, cat):
    p0, p1 = cat["p"][:2]
    db.add_prediction(_det(db, p0), "Low", 0.2, "m1")
    db.add_prediction(_det(db, p1), "High", 0.8, "m1")
    db.conn.execute("UPDATE predictions SET confidence = NULL WHERE species = 'Low'")
    ids = [p0] + list(range(10_000, 10_800)) + [p1]
    statements = _trace(db)
    rows = db.get_predictions(photo_ids=ids)
    db.conn.set_trace_callback(None)
    assert [r["species"] for r in rows] == ["High", "Low"]
    assert len([s for s in dict.fromkeys(statements) if "FROM predictions pr" in s]) == 2


def test_get_predictions_rules_route_through_facade(db, cat, monkeypatch):
    p0, p1 = cat["p"][:2]
    db.add_prediction(_det(db, p0), "Robin", 0.5, "m1")
    db.add_prediction(_det(db, p1), "Jay", 0.9, "m1")
    calls = []
    real_build = db._build_query_from_rules
    monkeypatch.setattr(db, "_build_query_from_rules",
                        lambda rules, **kw: calls.append(("build", rules, kw))
                        or real_build(rules, **kw))
    real_relax = db._relax_negated_prediction_leaves
    monkeypatch.setattr(db, "_relax_negated_prediction_leaves",
                        lambda rules: calls.append(("relax", rules)) or real_relax(rules))
    real_filter = db._filter_prediction_rows_by_rules
    monkeypatch.setattr(db, "_filter_prediction_rows_by_rules",
                        lambda rows, rules: calls.append(("filter", len(rows)))
                        or real_filter(rows, rules))
    rules = [{"field": "prediction_confidence", "op": ">=", "value": 0.8}]
    rows = db.get_predictions(rules=rules)
    assert [r["species"] for r in rows] == ["Jay"]
    assert [c[0] for c in calls] == ["relax", "build", "filter"]
    assert calls[1][2] == {"row_scoped": True}


def test_prediction_reads_require_workspace(db):
    db.set_active_workspace(None)
    for call in (
        lambda: db.get_predictions(),
        lambda: db.get_group_predictions("g"),
        lambda: db.get_existing_prediction_photo_ids("m1"),
        lambda: db.get_top_prediction_for_photo(1),
        lambda: db.get_prediction_for_photo(1, "m1"),
        lambda: db.update_prediction_status(1, "accepted"),
        lambda: db.update_predictions_status_by_photo(1, "accepted"),
        lambda: db.ungroup_prediction(1),
        lambda: db.clear_predictions(),
        lambda: db.clear_prediction_group_info(1, "m1"),
        lambda: db.update_prediction_group_info(1, "m1", "g", 1, 1, None),
        lambda: db.accept_subject_species(1),
        lambda: db.reconcile_match_review_state(1, "m1", "fp", "x", "new"),
    ):
        with pytest.raises(RuntimeError):
            call()
    # Empty inputs return before resolving the workspace.
    assert db.get_prediction_states([]) == {}
    assert db.get_top_prediction_confidences([]) == {}


def test_group_predictions_with_alternatives(db, cat):
    p0, p1 = cat["p"][:2]
    d0, d1 = _det(db, p0), _det(db, p1)
    assert db.get_group_predictions("g1") == []
    db.add_prediction(d0, "Robin", 0.9, "m1", group_id="g1", labels_fingerprint="fp1")
    db.add_prediction(d0, "Thrush", 0.3, "m1", status="alternative", labels_fingerprint="fp1")
    db.add_prediction(d0, "Stale", 0.2, "m1", status="alternative", labels_fingerprint="fp0")
    db.add_prediction(d1, "Robin", 0.8, "m1", group_id="g1", labels_fingerprint="fp1")
    db.conn.execute("UPDATE photos SET quality_score = ? WHERE id = ?", (0.9, p1))
    db.conn.execute("UPDATE photos SET quality_score = ? WHERE id = ?", (0.1, p0))
    rows = db.get_group_predictions("g1")
    assert [r["photo_id"] for r in rows] == [p1, p0]
    assert rows[1]["alternatives"] == [{"species": "Thrush", "confidence": 0.3}]
    assert rows[0]["alternatives"] == []


def test_existing_prediction_photo_ids(db, cat):
    p0, p1 = cat["p"][:2]
    db.add_prediction(_det(db, p0), "Robin", 0.5, "m1", labels_fingerprint="fp1")
    db.add_prediction(_det(db, p1), "Robin", 0.5, "m1", labels_fingerprint="fp2")
    db.add_prediction(_det(db, cat["outside"]), "Robin", 0.5, "m1", labels_fingerprint="fp1")
    assert db.get_existing_prediction_photo_ids("m1") == {p0, p1}
    assert db.get_existing_prediction_photo_ids("m1", "fp1") == {p0}
    assert db.get_existing_prediction_photo_ids("m2") == set()


def test_top_prediction_for_photo(db, cat):
    p0 = cat["p"][0]
    high = _det(db, p0, conf=0.9, x=0.1, model="md-a")
    low = _det(db, p0, conf=0.1, x=0.6, model="md-b")
    db.add_prediction(high, "Robin", 0.6, "m1", taxonomy={"scientific_name": "T. m."})
    db.add_prediction(low, "Hawk", 0.95, "m1")
    top = db.get_top_prediction_for_photo(p0)
    # Provenance rides along so readers can tell a guessed binomial from evidence.
    assert tuple(top) == ("Hawk", None, 0.95, low, "m1", "legacy", None)
    top = db.get_top_prediction_for_photo(p0, min_detector_confidence=0.5)
    assert tuple(top) == ("Robin", "T. m.", 0.6, high, "m1", "legacy", None)
    assert db.get_top_prediction_for_photo(cat["p"][1]) is None


def test_top_prediction_confidences(db, cat, monkeypatch):
    p0, p1 = cat["p"][:2]
    db.add_prediction(_det(db, p0), "Robin", 0.6, "m1")
    calls = []
    real = db._top_prediction_confidence_params
    monkeypatch.setattr(db, "_top_prediction_confidence_params",
                        lambda: calls.append(1) or real())
    got = db.get_top_prediction_confidences([p0, p1])
    assert calls == [1]
    assert got == {p0: pytest.approx(0.6)}
    statements = _trace(db)
    db.get_top_prediction_confidences([p0] + list(range(10_000, 10_800)))
    db.conn.set_trace_callback(None)
    assert len([s for s in dict.fromkeys(statements) if "p.id IN (" in s]) == 2


def test_top_unrejected_predictions_by_photo(db, cat):
    p0, p1, p2, p3 = cat["p"]
    det = _det(db, p0)
    db.add_prediction(det, "Robin", 0.99, "m1", labels_fingerprint="old")
    db.add_prediction(det, "Jay", 0.7, "m1", labels_fingerprint="new")
    db.add_prediction(det, "Wren", 0.8, "m1", labels_fingerprint="new")
    jay = _pred_id(db, det, "Jay", fp="new")
    wren = _pred_id(db, det, "Wren", fp="new")
    db.update_prediction_status(wren, "rejected")
    db.set_review_status(jay, cat["ws"], "reviewed", group_id="g")
    # Another workspace's rejection does not hide a row here.
    other = db.create_workspace("Other")
    db.set_review_status(jay, other, "rejected")

    tie = _det(db, p1)
    db.add_prediction(tie, "Hawk", 0.5, "m1")
    db.add_prediction(tie, "Kite", 0.5, "m1")
    kite = _pred_id(db, tie, "Kite", fp="legacy")

    only_rejected = _det(db, p2)
    db.add_prediction(only_rejected, "Owl", 0.9, "m1")
    db.update_prediction_status(_pred_id(db, only_rejected, "Owl", fp="legacy"), "rejected")

    # Not filtered by workspace visibility.
    db.add_prediction(_det(db, cat["outside"]), "Crow", 0.4, "m1")

    got = db.get_top_unrejected_predictions_by_photo([p0, p1, p2, p3, cat["outside"]])
    assert set(got) == {p0, p1, cat["outside"]}
    assert dict(got[p0]) == {
        "photo_id": p0, "id": jay, "species": "Jay", "confidence": 0.7,
        "classifier_model": "m1", "group_id": "g", "status": "reviewed",
    }
    # Equal confidence: the higher id wins.
    assert got[p1]["id"] == kite
    assert (got[p1]["status"], got[p1]["group_id"]) == ("pending", None)
    assert got[cat["outside"]]["species"] == "Crow"

    statements = _trace(db)
    padded = list(range(10_000, 10_800)) + [p1]
    assert set(db.get_top_unrejected_predictions_by_photo(padded)) == {p1}
    db.conn.set_trace_callback(None)
    assert len([s for s in dict.fromkeys(statements) if "d.photo_id IN" in s]) == 2


def test_top_unrejected_predictions_by_photo_workspace_resolution(db, cat):
    db.set_active_workspace(None)
    assert db.get_top_unrejected_predictions_by_photo([]) == {}
    with pytest.raises(RuntimeError):
        db.get_top_unrejected_predictions_by_photo([cat["p"][0]])


def test_prediction_for_photo(db, cat):
    p0 = cat["p"][0]
    det = _det(db, p0)
    db.add_prediction(det, "Robin", 0.6, "m1", labels_fingerprint="fp1")
    assert tuple(db.get_prediction_for_photo(p0, "m1")) == ("Robin", 0.6, det)
    assert tuple(db.get_prediction_for_photo(p0, "m1", "fp1")) == ("Robin", 0.6, det)
    assert db.get_prediction_for_photo(p0, "m1", "fp2") is None
    assert db.get_prediction_for_photo(cat["outside"], "m1") is None


def test_inspector_predictions_for_photo(db, cat):
    p0 = cat["p"][0]
    ws = cat["ws"]
    high = _det(db, p0, conf=0.9, x=0.1, model="md-a")
    low = _det(db, p0, conf=0.1, x=0.6, model="md-b")
    full = _det(db, p0, conf=1.0, model="full-image")
    db.add_prediction(high, "Old Robin", 0.99, "m1", labels_fingerprint="fp-old")
    db.add_prediction(high, "Robin", 0.6, "m1", labels_fingerprint="fp-new",
                      taxonomy={"scientific_name": "T. m."})
    db.add_prediction(high, "Wren", 0.7, "m2", labels_fingerprint="fp-x")
    db.add_prediction(low, "Hawk", 0.95, "m1", labels_fingerprint="fp-new")
    db.add_prediction(full, "Heron", 0.8, "m1", labels_fingerprint="fp-new")
    robin = _pred_id(db, high, "Robin", fp="fp-new")
    db.set_review_status(robin, ws, "accepted", individual="Bob", group_id="g1")
    other_ws = db.create_workspace("Other")
    wren = _pred_id(db, high, "Wren", model="m2", fp="fp-x")
    db.set_review_status(wren, other_ws, "rejected")

    rows = db.get_inspector_predictions_for_photo(p0, 0.5)
    # Latest label set per (detection, model), real detections at or above
    # the floor, best first; review state comes from the active workspace.
    assert [(r["species"], r["model"], r["status"]) for r in rows] == [
        ("Wren", "m2", "pending"), ("Robin", "m1", "accepted"),
    ]
    robin_row = rows[1]
    assert (robin_row["individual"], robin_row["group_id"]) == ("Bob", "g1")
    assert (robin_row["scientific_name"], robin_row["labels_fingerprint"]) == ("T. m.", "fp-new")
    assert robin_row["detector_confidence"] == 0.9
    assert rows[0]["vote_count"] is None

    assert [r["species"] for r in db.get_inspector_predictions_for_photo(p0, 0.0)] == [
        "Hawk", "Wren", "Robin",
    ]
    db.set_active_workspace(None)
    assert {r["status"] for r in db.get_inspector_predictions_for_photo(p0, 0.5)} == {"pending"}
    assert db.get_inspector_predictions_for_photo(cat["p"][1], 0.0) == []


# -- review mutators and their _commit seams ----------------------------------------------


def test_update_prediction_status_commit_flag(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1")
    pid = _pred_id(db, det, "Robin", fp="legacy")
    db.update_prediction_status(pid, "rejected", _commit=False)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM prediction_review") == [(0,)]
    db.conn.commit()
    db.update_prediction_status(pid, "accepted")
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT status FROM prediction_review") == [("accepted",)]


def test_update_predictions_status_by_photo_commit_flag(db, cat):
    p0 = cat["p"][0]
    d0 = _det(db, p0, x=0.1, model="md-a")
    d1 = _det(db, p0, x=0.6, model="md-b")
    db.add_prediction(d0, "Robin", 0.5, "m1")
    db.add_prediction(d1, "Jay", 0.5, "m1", status="accepted")
    db.update_predictions_status_by_photo(p0, "rejected", _commit=False)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT status FROM prediction_review") == [("accepted",)]
    db.conn.commit()
    db.update_predictions_status_by_photo(p0, "reviewed")
    assert not db.conn.in_transaction
    assert sorted(_visible(db, "SELECT status FROM prediction_review")) == [
        ("reviewed",), ("reviewed",)]


def test_ungroup_prediction_commit_flag(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1", group_id="g1")
    pid = _pred_id(db, det, "Robin", fp="legacy")
    db.ungroup_prediction(pid, _commit=False)
    assert db.conn.in_transaction
    assert _visible(db, "SELECT group_id FROM prediction_review") == [("g1",)]
    db.conn.rollback()
    db.ungroup_prediction(pid)
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT group_id FROM prediction_review") == [(None,)]


def test_review_status_get_and_set(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1")
    pid = _pred_id(db, det, "Robin", fp="legacy")
    other = db.create_workspace("Other")
    assert db.get_review_status(pid, cat["ws"]) == "pending"
    db.set_review_status(pid, other, "accepted", individual="i", group_id="g")
    assert not db.conn.in_transaction
    db.set_review_status(pid, other, "rejected")
    assert db.get_review_status(pid, other) == "rejected"
    assert _review(db, pid, other) == ("rejected", "i", "g", None, None)
    assert db.get_review_status(pid, cat["ws"]) == "pending"


@pytest.mark.parametrize("fp", [None, "fp1"])
def test_group_info_update_and_clear(db, cat, monkeypatch, fp):
    import db as dbmod

    commits = []
    real = dbmod.commit_with_retry
    monkeypatch.setattr(dbmod, "commit_with_retry",
                        lambda conn, *a, **k: commits.append(1) or real(conn, *a, **k))
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.8, "m1", labels_fingerprint="fp1")
    db.add_prediction(det, "Thrush", 0.9, "m1", labels_fingerprint="fp1",
                      status="alternative")
    primary = _pred_id(db, det, "Robin")
    commits.clear()
    db.update_prediction_group_info(det, "m1", "g1", 2, 3, '{"Robin": 2}',
                                    labels_fingerprint=fp)
    assert commits == [1]
    assert not db.conn.in_transaction
    assert _review(db, primary) == ("pending", '{"Robin": 2}', "g1", 2, 3)
    db.clear_prediction_group_info(det, "m1", labels_fingerprint=fp)
    assert commits == [1, 1]
    assert _review(db, primary) == ("pending", None, None, None, None)
    # No matching primary: nothing written, nothing committed.
    db.update_prediction_group_info(det, "m9", "g", 1, 1, None, labels_fingerprint=fp)
    db.clear_prediction_group_info(det, "m9", labels_fingerprint=fp)
    assert commits == [1, 1]


def test_clear_group_info_never_inserts(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.8, "m1")
    db.clear_prediction_group_info(det, "m1")
    assert db.conn.execute("SELECT COUNT(*) FROM prediction_review").fetchone()[0] == 0


# -- legacy burst repair -------------------------------------------------------------------


# -- accepts -------------------------------------------------------------------------------


def test_accept_prediction_commit_flag_and_rollback(db, cat, monkeypatch):
    p0 = cat["p"][0]
    det = _det(db, p0)
    db.add_prediction(det, "Robin", 0.9, "m1")
    pid = _pred_id(db, det, "Robin", fp="legacy")
    db.conn.commit()
    result = db.accept_prediction(pid, _commit=False)
    assert result["accepted_prediction_ids"] == [pid]
    assert db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM photo_keywords") == [(0,)]
    db.conn.rollback()

    def boom(*a, **k):
        raise RuntimeError("tag failed")

    monkeypatch.setattr(db, "tag_photo", boom)
    with pytest.raises(RuntimeError, match="tag failed"):
        db.accept_prediction(pid)
    assert not db.conn.in_transaction
    assert _visible(db, "SELECT COUNT(*) FROM prediction_review") == [(0,)]


def test_accept_subject_species(db, cat, monkeypatch):
    p0 = cat["p"][0]
    det = _det(db, p0)
    db.add_prediction(det, "Robin", 0.9, "m1", labels_fingerprint="fp1")
    db.add_prediction(det, "robin ", 0.7, "m2", labels_fingerprint="fp1")
    db.add_prediction(det, "Robin", 0.6, "m3", labels_fingerprint="fp1",
                      status="rejected")
    db.add_prediction(det, "Jay", 0.5, "m1", labels_fingerprint="fp1")
    top = _pred_id(db, det, "Robin")
    calls = []
    real = db.accept_prediction
    monkeypatch.setattr(db, "accept_prediction",
                        lambda pid, **kw: calls.append((pid, kw)) or real(pid, **kw))
    result = db.accept_subject_species(top, _commit=False)
    assert db.conn.in_transaction
    assert [c[1] for c in calls] == [{"photo_ids": [p0], "_commit": False}] * 2
    assert result["photo_id"] == p0
    assert result["prediction_ids"][0] == top
    assert len(result["prediction_ids"]) == 2
    db.conn.commit()
    assert db.accept_subject_species(999_999) is None
    # A prediction on a photo outside the workspace is not a target.
    out_det = _det(db, cat["outside"])
    db.add_prediction(out_det, "Robin", 0.9, "m1")
    assert db.accept_subject_species(_pred_id(db, out_det, "Robin", fp="legacy")) is None


def test_accept_subject_species_rolls_back(db, cat, monkeypatch):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.9, "m1")
    pid = _pred_id(db, det, "Robin", fp="legacy")
    db.conn.commit()

    def boom(*a, **k):
        db.conn.execute("INSERT INTO db_meta (key, value) VALUES ('probe', '1')")
        raise RuntimeError("accept failed")

    monkeypatch.setattr(db, "accept_prediction", boom)
    with pytest.raises(RuntimeError, match="accept failed"):
        db.accept_subject_species(pid)
    assert not db.conn.in_transaction
    assert db.get_meta("probe") is None
    with pytest.raises(RuntimeError, match="accept failed"):
        db.accept_subject_species(pid, _commit=False)
    assert db.conn.in_transaction
    db.conn.rollback()


def test_accept_subject_species_no_accept(db, cat, monkeypatch):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.9, "m1")
    pid = _pred_id(db, det, "Robin", fp="legacy")
    monkeypatch.setattr(db, "accept_prediction",
                        lambda pid, **kw: {"accepted_prediction_ids": [], "affected": []})
    assert db.accept_subject_species(pid) is None
    assert not db.conn.in_transaction


def test_decided_statuses_are_pinned():
    assert Database.DECIDED_PREDICTION_STATUSES == ("accepted", "rejected", "reviewed")


# -- prediction-decision reads ------------------------------------------------------------


def _scope_preds(db, photo_id, species, model="m1", fp="fp1", x=0.1):
    """One detection carrying a prediction per species in one label set."""
    det = _det(db, photo_id, x=x)
    ids = []
    for name in species:
        db.add_prediction(det, name, 0.5, model, labels_fingerprint=fp)
        ids.append(_pred_id(db, det, name, model=model, fp=fp))
    return det, ids


def test_decision_row_and_status(db, cat):
    det, (pid,) = _scope_preds(db, cat["p"][0], ["Robin"])
    other = db.create_workspace("Other")
    db.set_review_status(pid, other, "accepted", group_id="other-g")

    row = db.get_prediction_decision_row(pid)
    assert dict(row) == {
        "id": pid, "species": "Robin", "detection_id": det, "model": "m1",
        "labels_fingerprint": "fp1", "photo_id": cat["p"][0],
        "group_id": None, "status": "pending",
    }
    assert db.get_prediction_status(pid) == "pending"

    db.set_review_status(pid, cat["ws"], "rejected", group_id="g")
    row = db.get_prediction_decision_row(pid)
    assert (row["status"], row["group_id"]) == ("rejected", "g")
    assert db.get_prediction_status(pid) == "rejected"

    assert db.get_prediction_decision_row(987_654) is None
    assert db.get_prediction_status(987_654) is None
    assert not db.conn.in_transaction


def test_predictions_by_id_keeps_superseded_rows_and_votes(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="old")
    db.add_prediction(det, "Robin", 0.6, "m1", labels_fingerprint="new",
                      group_id="g", individual="Robin")
    old = _pred_id(db, det, "Robin", fp="old")
    new = _pred_id(db, det, "Robin", fp="new")
    rows = {r["id"]: r for r in db.get_predictions_by_id([old, new, 987_654])}
    assert set(rows) == {old, new}
    assert rows[new]["photo_id"] == cat["p"][0]
    assert rows[new]["model"] == "m1"
    assert (rows[new]["group_id"], rows[new]["individual"]) == ("g", "Robin")
    assert (rows[old]["group_id"], rows[old]["individual"]) == (None, None)
    assert db.get_predictions_by_id([]) == []


def test_decided_and_superseded_prediction_ids(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="old")
    db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="new")
    db.add_prediction(det, "Jay", 0.5, "m2", labels_fingerprint="old")
    old = _pred_id(db, det, "Robin", fp="old")
    new = _pred_id(db, det, "Robin", fp="new")
    jay = _pred_id(db, det, "Jay", model="m2", fp="old")
    # m2 has only the one label set, so its row is current.
    assert db.get_superseded_prediction_ids([old, new, jay]) == {old}

    db.update_prediction_status(old, "accepted")
    db.update_prediction_status(new, "alternative")
    db.update_prediction_status(jay, "reviewed")
    other = db.create_workspace("Other")
    db.set_review_status(new, other, "rejected")
    assert db.get_decided_prediction_ids([old, new, jay]) == {old, jay}
    assert db.get_decided_prediction_ids([]) == set()
    assert db.get_superseded_prediction_ids([]) == set()

    # Chunked under SQLite's variable limit.
    statements = _trace(db)
    padded = list(range(1_000_000, 1_000_900)) + [old]
    assert db.get_decided_prediction_ids(padded) == {old}
    assert db.get_superseded_prediction_ids(padded) == {old}
    db.conn.set_trace_callback(None)
    assert len([s for s in dict.fromkeys(statements)
                if "FROM prediction_review" in s]) == 2


def test_out_of_workspace_prediction_ids(db, cat):
    _, (inside,) = _scope_preds(db, cat["p"][0], ["Robin"])
    _, (outside,) = _scope_preds(db, cat["outside"], ["Robin"])
    assert db.get_out_of_workspace_prediction_ids([inside, outside]) == {outside}
    assert db.get_out_of_workspace_prediction_ids([]) == set()


def test_prediction_photo_ids_in_id_order_and_stops_early(db, cat):
    _, (a,) = _scope_preds(db, cat["p"][1], ["Robin"])
    _, (b,) = _scope_preds(db, cat["p"][0], ["Jay"])
    got = db.get_prediction_photo_ids([b, 987_654, a])
    assert list(got.items()) == [(a, cat["p"][1]), (b, cat["p"][0])]

    statements = _trace(db)
    payload = [a, b] + list(range(1_000_000, 1_000_900))
    assert db.get_prediction_photo_ids(payload) == {a: cat["p"][1], b: cat["p"][0]}
    assert len([s for s in dict.fromkeys(statements) if "pr.id IN" in s]) == 2
    statements.clear()
    # Two photos in the first chunk already exceed the bound: stop there.
    assert len(db.get_prediction_photo_ids(payload, stop_after_photos=1)) == 2
    db.conn.set_trace_callback(None)
    assert len([s for s in dict.fromkeys(statements) if "pr.id IN" in s]) == 1


def test_non_alternative_predictions_for_photos(db, cat):
    p0, p1 = cat["p"][0], cat["p"][1]
    _, (lead, runner_up) = _scope_preds(db, p0, ["Robin", "Jay"])
    _, (plain,) = _scope_preds(db, p1, ["Wren"])
    db.update_prediction_status(runner_up, "alternative")
    db.set_review_status(lead, cat["ws"], "pending", group_id="g")
    rows = db.get_non_alternative_predictions_for_photos([p1, p0])
    assert [(r["id"], r["photo_id"], bool(r["in_group"])) for r in rows] == [
        (lead, p0, True), (plain, p1, False),
    ]


def test_scope_review_statuses(db, cat):
    det, (pick, sib, unreviewed) = _scope_preds(
        db, cat["p"][0], ["Robin", "Jay", "Wren"],
    )
    db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="fp2")
    other_set = _pred_id(db, det, "Robin", fp="fp2")
    db.update_prediction_status(pick, "rejected")
    db.update_prediction_status(sib, "accepted")
    db.update_prediction_status(other_set, "accepted")
    rows = db.get_scope_review_statuses([pick])
    assert sorted((r["pick_id"], r["prediction_id"], r["status"]) for r in rows) == [
        (pick, pick, "rejected"), (pick, sib, "accepted"),
    ]
    assert unreviewed not in {r["prediction_id"] for r in rows}


def test_prediction_scope_and_sibling_reads(db, cat):
    det, (lead, alt, decided, pending) = _scope_preds(
        db, cat["p"][0], ["Robin", "Jay", "Wren", "Crow"],
    )
    db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="fp2")
    other_set = _pred_id(db, det, "Robin", fp="fp2")
    db.update_prediction_status(alt, "alternative")
    db.update_prediction_status(decided, "rejected")
    db.update_prediction_status(other_set, "alternative")

    scope = db.get_prediction_scope(lead)
    assert tuple(scope) == (det, "m1", "fp1")
    assert db.get_prediction_scope(987_654) is None

    assert sorted(db.get_open_scope_sibling_ids(*scope, [lead])) == sorted(
        [alt, pending]
    )
    assert db.get_open_scope_sibling_ids(*scope, [lead, alt]) == [pending]
    assert db.get_alternative_sibling_ids(*scope, lead) == [alt]
    assert db.get_alternative_sibling_ids(*scope, alt) == []


def test_prediction_statuses_with_supersession(db, cat):
    det = _det(db, cat["p"][0])
    db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="old")
    db.add_prediction(det, "Robin", 0.5, "m1", labels_fingerprint="new")
    old = _pred_id(db, det, "Robin", fp="old")
    new = _pred_id(db, det, "Robin", fp="new")
    db.update_prediction_status(new, "accepted")
    rows = {
        r["prediction_id"]: (r["photo_id"], r["status"], bool(r["is_superseded"]))
        for r in db.get_prediction_statuses_with_supersession([old, new])
    }
    assert rows == {
        old: (cat["p"][0], "pending", True),
        new: (cat["p"][0], "accepted", False),
    }


def test_burst_group_members(db, cat):
    ws = cat["ws"]
    _, (lead,) = _scope_preds(db, cat["p"][0], ["Robin"])
    _, (member,) = _scope_preds(db, cat["p"][1], ["Robin"])
    _, (decided,) = _scope_preds(db, cat["p"][2], ["Robin"])
    _, (other_model,) = _scope_preds(db, cat["p"][3], ["Robin"], model="m2")
    _, (hidden,) = _scope_preds(db, cat["outside"], ["Robin"])
    db.set_review_status(lead, ws, "pending", group_id="g")
    db.set_review_status(member, ws, "pending", group_id="g")
    db.set_review_status(decided, ws, "rejected", group_id="g")
    db.set_review_status(other_model, ws, "pending", group_id="g")
    db.set_review_status(hidden, ws, "pending", group_id="g")
    rows = db.get_burst_group_members("g", "m1", lead)
    assert [(r["id"], r["photo_id"], r["status"]) for r in rows] == [
        (member, cat["p"][1], "pending"), (decided, cat["p"][2], "rejected"),
    ]


# -- structure ----------------------------------------------------------------------------


_DELEGATING_PREDICTION_METHODS = (
    "add_prediction",
    "retain_prediction_candidates",
    "reconcile_match_review_state",
    "clear_predictions",
    "get_prediction_states",
    "get_predictions",
    "update_prediction_status",
    "get_group_predictions",
    "update_predictions_status_by_photo",
    "ungroup_prediction",
    "get_existing_prediction_photo_ids",
    "get_top_prediction_for_photo",
    "get_top_prediction_confidences",
    "get_top_unrejected_predictions_by_photo",
    "get_prediction_for_photo",
    "clear_prediction_group_info",
    "update_prediction_group_info",
    "accept_subject_species",
    "get_review_status",
    "set_review_status",
    "get_prediction_decision_row",
    "get_prediction_status",
    "get_predictions_by_id",
    "get_decided_prediction_ids",
    "get_superseded_prediction_ids",
    "get_out_of_workspace_prediction_ids",
    "get_prediction_photo_ids",
    "get_non_alternative_predictions_for_photos",
    "get_scope_review_statuses",
    "get_prediction_scope",
    "get_open_scope_sibling_ids",
    "get_alternative_sibling_ids",
    "get_prediction_statuses_with_supersession",
    "get_burst_group_members",
    "get_inspector_predictions_for_photo",
)

# ``test_route_contract`` looks these up by name on ``Database``.
_PREDICTION_MUTATORS = (
    "accept_prediction",
    "accept_subject_species",
    "update_prediction_status",
    "update_predictions_status_by_photo",
    "ungroup_prediction",
    "set_review_status",
)


def _self_attrs(fn):
    source = textwrap.dedent(inspect.getsource(fn))
    node = ast.parse(source).body[0]
    return {
        n.attr
        for n in ast.walk(node)
        if isinstance(n, ast.Attribute)
        and isinstance(n.value, ast.Name)
        and n.value.id == "self"
    }


@pytest.mark.parametrize("name", _DELEGATING_PREDICTION_METHODS)
def test_prediction_method_delegates_to_repository(name):
    attrs = _self_attrs(getattr(Database, name))
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to PredictionRepository"
    )
    assert "_prediction_repository" in attrs, (
        f"Database.{name} no longer delegates to PredictionRepository"
    )


@pytest.mark.parametrize("name", _PREDICTION_MUTATORS)
def test_prediction_mutators_keep_their_database_names(name):
    assert callable(getattr(Database, name, None))


def test_accept_prediction_delegates_to_keyword_provenance_repository():
    """It tags through ``tag_photo`` mid-transaction, so it lives with the
    provenance writers rather than in ``PredictionRepository``."""
    attrs = _self_attrs(Database.accept_prediction)
    assert "conn" not in attrs
    assert "_keyword_provenance_repository" in attrs
    assert "_prediction_repository" not in attrs


def test_prediction_repository_never_references_keyword_writers():
    import repositories.predictions as module

    tree = ast.parse(inspect.getsource(module))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            names.add(node.value)
    assert not names & {"tag_photo", "untag_photo", "_merge_keyword_into",
                        "retire_builtin_wildlife_genre", "link_keyword_to_place"}


def test_prediction_repository_routes_facade_calls_through_database(db):
    repo = db._prediction_repository()
    from repositories.predictions import FACADE_METHODS

    for name in FACADE_METHODS:
        assert getattr(repo, name) == getattr(db, name), name


def test_prediction_repository_never_hands_itself_out_as_the_database():
    import repositories.predictions as module

    tree = ast.parse(inspect.getsource(module))
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    bare = [
        node.lineno for node in ast.walk(tree)
        if isinstance(node, ast.Name) and node.id == "self"
        and not (isinstance(parents.get(node), ast.Attribute)
                 and parents[node].value is node)
        and not isinstance(parents.get(node), ast.arguments)
    ]
    assert len(bare) == 1  # ``setattr(self, ...)`` in ``__init__``


def test_prediction_facade_signatures_unchanged():
    sig = {n: str(inspect.signature(getattr(Database, n)))
           for n in _DELEGATING_PREDICTION_METHODS}
    assert sig["add_prediction"] == (
        "(self, detection_id, species, confidence, model, category='new', "
        "status='pending', group_id=None, vote_count=None, total_votes=None, "
        "individual=None, taxonomy=None, labels_fingerprint='legacy', "
        "labels_fingerprint_full=None, preserve_manual_review=False, "
        "match_score=None, from_fresh_inference=False, refresh_output=False)"
    )
    assert sig["clear_predictions"] == (
        "(self, model=None, collection_photo_ids=None, labels_fingerprint=None, "
        "clear_run_keys=True)"
    )
    assert sig["update_prediction_status"] == (
        "(self, prediction_id, status, _commit=True)"
    )
    assert sig["update_predictions_status_by_photo"] == (
        "(self, photo_id, status, _commit=True)"
    )
    assert sig["ungroup_prediction"] == "(self, prediction_id, _commit=True)"
    assert sig["accept_subject_species"] == "(self, prediction_id, _commit=True)"
    assert sig["set_review_status"] == (
        "(self, prediction_id, workspace_id, status, individual=None, group_id=None)"
    )
    assert sig["get_predictions"] == (
        "(self, photo_ids=None, model=None, status=None, rules=None)"
    )
