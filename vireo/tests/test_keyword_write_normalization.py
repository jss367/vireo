"""Write-side keyword and species-name normalization.

``add_prediction`` and the keyword writers store every name in
``normalize_keyword_display()`` form, so runtime code never guards against
stored typographic variants.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import Database  # noqa: E402


def _make_db(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos", name="photos")
    p1 = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    p2 = db.add_photo(
        folder_id=fid, filename="b.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    return db, ws_id, p1, p2


def test_add_prediction_folds_curly_apostrophe_species(tmp_path):
    """`Database.add_prediction` is the shared choke point for the classify
    job's storage helpers (`_store_pending_detection_prediction`,
    `_store_match_prediction`). Neither production path runs
    `normalize_keyword_display` before calling here, so a bundled label
    that spells `Swinhoe’s White-eye` with U+2019 would still land in
    predictions.species as-is and fail to match its accepted
    `Swinhoe's white-eye` keyword row (both exact and COLLATE NOCASE joins
    are quote-preserving). Folding inside `add_prediction` closes that
    hole once for every caller."""
    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = db.conn.execute(
            "INSERT INTO detections (photo_id, category, detector_confidence) "
            "VALUES (?, 'animal', 0.99)",
            (p1,),
        ).lastrowid
        db.conn.commit()

        db.add_prediction(
            detection_id=det,
            species="Swinhoe’s White-eye",
            confidence=0.8,
            model="m1",
            labels_fingerprint="fp1",
        )

        stored = [
            r["species"] for r in db.conn.execute(
                "SELECT species FROM predictions"
            ).fetchall()
        ]
        assert stored == ["Swinhoe's White-eye"]
    finally:
        db.close()


def test_add_prediction_folds_review_row_to_normalized_species(tmp_path):
    """When add_prediction folds species, the workspace-scoped
    prediction_review row must land on the *folded* prediction row's id,
    not on a stray original-spelling row (which would never exist because
    the fold applies before insert). Verifies the id lookup after fold."""
    db, ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = db.conn.execute(
            "INSERT INTO detections (photo_id, category, detector_confidence) "
            "VALUES (?, 'animal', 0.99)",
            (p1,),
        ).lastrowid
        db.conn.commit()

        db.add_prediction(
            detection_id=det,
            species="Swinhoe’s White-eye",
            confidence=0.8,
            model="m1",
            status="accepted",
            labels_fingerprint="fp1",
        )

        row = db.conn.execute(
            "SELECT p.id AS pid, p.species, pr.status "
            "FROM predictions p "
            "LEFT JOIN prediction_review pr "
            "  ON pr.prediction_id = p.id AND pr.workspace_id = ? "
            "WHERE p.detection_id = ?",
            (ws_id, det),
        ).fetchone()
        assert row is not None
        assert row["species"] == "Swinhoe's White-eye"
        assert row["status"] == "accepted"
    finally:
        db.close()


def test_prime_symbol_not_folded_in_keywords(tmp_path):
    """U+2032 PRIME is the semantic prime symbol used for feet and
    arcminutes. Keywords like `10′ waterfall` must not be silently
    rewritten to `10' waterfall` by the apostrophe fold — the fold table
    intentionally excludes U+2032 so measurement notation survives the
    display/storage normalization applied on every keyword write."""
    from keyword_normalization import normalize_keyword_display

    assert normalize_keyword_display("10′ waterfall") == "10′ waterfall"
    # Middle-of-string preservation matters most, but a lone prime as an
    # edge char is stripped by _EDGE_QUOTES (measurement notation almost
    # never appears at the boundary of a keyword name — the edge behavior
    # is unchanged from before the fold table existed).

    db, _ws_id, p1, _p2 = _make_db(tmp_path)
    try:
        det = db.conn.execute(
            "INSERT INTO detections (photo_id, category, detector_confidence) "
            "VALUES (?, 'animal', 0.99)",
            (p1,),
        ).lastrowid
        db.conn.commit()

        # A prediction whose species carries a legitimate prime stays intact
        # after add_prediction normalizes on write.
        db.add_prediction(
            detection_id=det,
            species="10′ waterfall bird",
            confidence=0.5,
            model="m1",
            labels_fingerprint="fp1",
        )
        stored = db.conn.execute(
            "SELECT species FROM predictions WHERE detection_id = ?", (det,)
        ).fetchone()["species"]
        assert stored == "10′ waterfall bird"
    finally:
        db.close()
