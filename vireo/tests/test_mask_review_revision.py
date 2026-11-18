"""Conditional mask reviews preserve intervening decisions, including ABA changes."""
import sqlite3

import pytest
from canonical_schema import SCHEMA_VERSION
from test_subject_ranking import TARGET, _analyze, _pack
from test_subject_ranking import comparison_library as _comparison_library

comparison_library = _comparison_library


@pytest.mark.parametrize("first,later", [("valid", "invalid"), ("invalid", "valid")])
def test_delayed_opposing_review_cannot_overwrite_a_newer_decision(comparison_library, first, later):
    import config as cfg
    from subject_ranking import SubjectComparison

    app, db, pids, _ = comparison_library
    pid = pids[0]
    _analyze(db, [pid], SubjectComparison(TARGET), cfg.load())
    card = _pack(app, [pid], requested_species=TARGET)["cards"][0]
    binding = {"detection_id": card["subject"]["detection_id"], "analysis_key": card["subject"]["analysis_key"],
               "mask_digest": card["mask_review"]["mask_digest"], "expected_revision": 0}
    url = f"/api/photos/{pid}/mask-review"
    earlier_tab, later_tab = app.test_client(), app.test_client()
    # Both tabs read revision zero. The later click's request arrives first.
    assert later_tab.put(url, json={**binding, "verdict": later}).status_code == 200
    before = db.subjects.mask_reviews(pid)
    assert earlier_tab.put(url, json={**binding, "verdict": first}).status_code == 409
    assert db.subjects.mask_reviews(pid) == before
    assert before[0]["verdict"] == later and before[0]["revision"] == 1
    # Changing away and back must not make the old revision usable again.
    assert later_tab.put(url, json={**binding, "expected_revision": 1, "verdict": first}).status_code == 200
    assert later_tab.put(url, json={**binding, "expected_revision": 2, "verdict": later}).status_code == 200
    assert earlier_tab.put(url, json={**binding, "expected_revision": 1, "verdict": first}).status_code == 409
    row, = db.subjects.mask_reviews(pid)
    assert row["verdict"] == later and row["revision"] == 3
    for missing in (None, True, -1, "3", 2**100):
        assert earlier_tab.put(url, json={**binding, "expected_revision": missing, "verdict": first}).status_code == 400
    assert db.subjects.mask_reviews(pid) == [row]


def test_existing_mask_reviews_keep_verdict_and_bytes_during_revision_upgrade():
    import schema

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE photo_masks (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE subject_mask_reviews (id INTEGER PRIMARY KEY, verdict TEXT, mask_png BLOB)")
        conn.execute("INSERT INTO subject_mask_reviews VALUES (1, 'invalid', ?)", (b"fictional PNG",))
        conn.execute("PRAGMA user_version=55")
        conn.commit()
        schema._apply_pending(conn)
        schema._apply_pending(conn)
        assert conn.execute("SELECT verdict, mask_png, revision FROM subject_mask_reviews").fetchone() == (
            "invalid", b"fictional PNG", 1)
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    finally:
        conn.close()


@pytest.mark.parametrize("history", ["main55", "reviews55", "reviews56"])
def test_divergent_version_55_and_56_schemas_converge_without_losing_evidence(history):
    import schema

    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE photo_masks (id INTEGER PRIMARY KEY)")
        if history == "main55":
            conn.execute("ALTER TABLE photo_masks ADD COLUMN comparison_inputs TEXT")
            conn.execute("INSERT INTO photo_masks VALUES (1, 'retained primary proof')")
        else:
            conn.execute("CREATE TABLE subject_mask_reviews (id INTEGER PRIMARY KEY, verdict TEXT, mask_png BLOB)")
            conn.execute("INSERT INTO subject_mask_reviews VALUES (1, 'invalid', ?)", (b"synthetic reviewed bytes",))
            if history == "reviews56":
                conn.execute("ALTER TABLE subject_mask_reviews ADD COLUMN revision INTEGER NOT NULL DEFAULT 1")
                conn.execute("UPDATE subject_mask_reviews SET revision=7")
        conn.execute(f"PRAGMA user_version={56 if history == 'reviews56' else 55}")
        conn.commit()
        schema._apply_pending(conn)
        schema._apply_pending(conn)
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert "comparison_inputs" in {r[1] for r in conn.execute("PRAGMA table_info(photo_masks)")}
        assert "revision" in {r[1] for r in conn.execute("PRAGMA table_info(subject_mask_reviews)")}
        if history == "main55":
            assert conn.execute("SELECT comparison_inputs FROM photo_masks").fetchone() == ("retained primary proof",)
        else:
            assert conn.execute("SELECT verdict, mask_png, revision FROM subject_mask_reviews").fetchone() == (
                "invalid", b"synthetic reviewed bytes", 7 if history == "reviews56" else 1)
    finally:
        conn.close()


def test_review_revisions_are_independent_for_different_masks(comparison_library):
    import config as cfg
    from subject_ranking import SubjectComparison

    app, db, pids, _ = comparison_library
    _analyze(db, pids[:2], SubjectComparison(TARGET), cfg.load())
    cards = _pack(app, pids[:2], requested_species=TARGET)["cards"]
    for card, verdict in zip(cards, ("valid", "invalid"), strict=True):
        assert card["mask_review"]["revision"] == 0
        response = app.test_client().put(f'/api/photos/{card["id"]}/mask-review', json={
            "detection_id": card["subject"]["detection_id"], "analysis_key": card["subject"]["analysis_key"],
            "mask_digest": card["mask_review"]["mask_digest"], "verdict": verdict, "expected_revision": 0,
        })
        assert response.status_code == 200
        assert response.json == {"verdict": verdict, "revision": 1}
